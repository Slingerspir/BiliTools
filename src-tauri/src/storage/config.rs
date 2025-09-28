use anyhow::{anyhow, Result};
use sea_query::{
    ColumnDef, Iden, OnConflict, Query, SqliteQueryBuilder, Table, TableCreateStatement,
};
use sea_query_binder::SqlxBinder;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use specta::Type;
use sqlx::Row;
use std::{path::PathBuf, sync::Arc};
use tauri::Manager;

use super::db::{get_db, TableSpec};
use crate::shared::{get_app_handle, Theme, WindowEffect, CONFIG};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Type)]
#[serde(rename_all = "camelCase")]
pub enum CacheKey {
    Log,
    Temp,
    Webview,
    Database,
}

#[derive(Clone, Debug, Serialize, Deserialize, Type)]
pub struct Settings {
    pub add_metadata: bool,
    pub auto_check_update: bool,
    pub auto_download: bool,
    pub block_pcdn: bool,
    pub check_update: bool,
    pub clipboard: bool,
    pub convert: SettingsConvert,
    pub default: SettingsDefault,
    pub down_dir: PathBuf,
    pub drag_search: bool,
    pub format: SettingsFormat,
    pub language: String,
    pub max_conc: usize,
    pub notify: bool,
    pub temp_dir: PathBuf,
    pub theme: Theme,
    pub window_effect: WindowEffect,
    pub organize: SettingsOrganize,
    pub proxy: SettingsProxy,
}

impl Settings {
    pub fn get_cache(&self, key: &CacheKey) -> Result<PathBuf> {
        let app = get_app_handle();
        let path = app.path();
        match key {
            CacheKey::Log => Ok(path.app_log_dir()?),
            CacheKey::Temp => Ok(self.temp_dir()),
            CacheKey::Webview => Ok(match std::env::consts::OS {
                "macos" => path.app_cache_dir()?.join("../WebKit/BiliTools/WebsiteData"),
                "linux" => path.app_cache_dir()?.join("bilitools"),
                _ => path.app_local_data_dir()?.join("EBWebView"), // windows
            }),
            CacheKey::Database => Ok(path.app_data_dir()?.join("Storage")),
        }
    }

    pub fn temp_dir(&self) -> PathBuf {
        self.temp_dir.join("com.btjawa.bilitools")
    }
}

// 其他结构体定义保持不变...

#[derive(Iden)]
pub enum Config {
    Table,
    Name,
    Value,
}

pub struct ConfigTable;

impl TableSpec for ConfigTable {
    const NAME: &'static str = "config";
    const LATEST: i32 = 1;
    
    fn create_stmt() -> TableCreateStatement {
        Table::create()
            .table(Config::Table)
            .if_not_exists() // 优化点1：防止表已存在时报错
            .col(ColumnDef::new(Config::Name).text().not_null().primary_key())
            .col(ColumnDef::new(Config::Value).text().not_null())
            .to_owned()
    }
}

pub fn read() -> Arc<Settings> {
    CONFIG.load_full()
}

pub async fn load() -> Result<()> {
    let pool = get_db().await?; // 优化点2：提前获取数据库连接
    
    // 优化点3：简化查询构建
    let (sql, values) = Query::select()
        .columns([Config::Name, Config::Value])
        .from(Config::Table)
        .build_sqlx(SqliteQueryBuilder);

    let rows = sqlx::query_with(&sql, values).fetch_all(&pool).await?;
    let mut local = serde_json::Map::new();
    
    // 优化点4：简化行处理
    for r in rows {
        let name: String = r.try_get("name")?;
        let value_str: String = r.try_get("value")?;
        let value: Value = serde_json::from_str(&value_str)?;
        local.insert(name, value);
    }

    let default_config = read();
    let map = serde_json::to_value(default_config)?
        .as_object()
        .ok_or(anyhow!("Failed to read config"))?
        .clone();

    // 优化点5：简化配置合并逻辑
    for (key, default_value) in map {
        match local.entry(key.clone()) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(default_value.clone());
                insert(&key, &default_value).await?;
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                if let (Value::Object(default_obj), Value::Object(local_obj)) = 
                    (&default_value, entry.get_mut())
                {
                    for (sub_key, sub_value) in default_obj {
                        if !local_obj.contains_key(sub_key) {
                            local_obj.insert(sub_key.clone(), sub_value.clone());
                        }
                    }
                    insert(&key, &Value::Object(local_obj.clone())).await?;
                }
            }
        }
    }

    CONFIG.store(Arc::new(serde_json::from_value(Value::Object(local))?));
    Ok(())
}

pub async fn insert(name: &str, value: &Value) -> Result<()> {
    let pool = get_db().await?;
    let value_str = serde_json::to_string(value)?;
    
    // 优化点6：简化插入查询构建
    let (sql, values) = Query::insert()
        .into_table(Config::Table)
        .columns([Config::Name, Config::Value])
        .values_panic([name.into(), value_str.into()])
        .on_conflict(
            OnConflict::column(Config::Name)
                .update_column(Config::Value)
                .to_owned(),
        )
        .build_sqlx(SqliteQueryBuilder);

    sqlx::query_with(&sql, values).execute(&pool).await?;
    Ok(())
}

pub async fn write(settings: serde_json::Map<String, Value>) -> Result<()> {
    let mut current_config = serde_json::to_value(read())?;
    let config_keys = current_config
        .as_object()
        .map(|v| v.keys().cloned().collect::<Vec<_>>())
        .ok_or(anyhow!("Failed to read config"))?;

    // 优化点7：使用更高效的方式处理设置更新
    let valid_settings = settings.into_iter()
        .filter(|(k, _)| config_keys.contains(k))
        .collect::<Vec<_>>();

    if valid_settings.is_empty() {
        return Ok(());
    }

    let config_obj = current_config
        .as_object_mut()
        .ok_or(anyhow!("Failed to get mutable config"))?;

    for (key, value) in valid_settings {
        insert(&key, &value).await?;
        config_obj.insert(key, value);
    }

    CONFIG.store(Arc::new(serde_json::from_value(current_config)?));

    #[cfg(debug_assertions)]
    log::info!("CONFIG: \n{}", serde_json::to_string_pretty(&read())?);
    
    Ok(())
}
