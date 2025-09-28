use anyhow::{anyhow, Result};
use sea_query::{
    Alias, ColumnDef, Expr, Iden, OnConflict, Query, SqliteQueryBuilder, Table,
    TableCreateStatement,
};
use sea_query_binder::SqlxBinder;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
    Row, SqlitePool, Transaction,
};
use std::{
    future::Future,
    path::PathBuf,
    str::FromStr,
    sync::OnceLock,
    time::Duration,
};
use tokio::fs;

use crate::shared::{get_ts, DATABASE_URL, STORAGE_PATH};

// 使用 OnceLock 替代 RwLock+Option 组合
static DB: OnceLock<SqlitePool> = OnceLock::new();

#[derive(Iden)]
enum Meta {
    Table,
    Name,
    Version,
}

pub trait TableSpec: Send + Sync + 'static {
    const NAME: &'static str;
    const LATEST: i32;
    
    fn create_stmt() -> TableCreateStatement;
    
    async fn check_latest() -> Result<()> {
        init_meta().await?;
        let pool = get_db()?;
        let cur = get_version(Self::NAME).await?;
        
        if cur != Self::LATEST {
            let mut tx = pool.begin().await?;
            let ts = get_ts(true);
            let old_table_name = format!("{}_{}", Self::NAME, ts);
            
            // 重命名旧表
            let rename_sql = Table::rename()
                .table(Alias::new(Self::NAME), Alias::new(&old_table_name))
                .to_string(SqliteQueryBuilder);
            sqlx::query(&rename_sql).execute(&mut *tx).await.ok();
            
            // 创建新表
            let create_sql = Self::create_stmt().to_string(SqliteQueryBuilder);
            sqlx::query(&create_sql).execute(&mut *tx).await?;
            
            // 尝试迁移数据
            Self::migrate_data(&old_table_name, &mut tx).await?;
            
            tx.commit().await?;
            set_version(Self::NAME, Self::LATEST).await?;
        }
        Ok(())
    }
    
    // 默认空实现，可被子表覆盖实现具体迁移逻辑
    async fn migrate_data(old_table: &str, tx: &mut Transaction<'_, Sqlite>) -> Result<()> {
        Ok(())
    }
}

pub async fn init_db() -> Result<()> {
    let opts = SqliteConnectOptions::from_str(&DATABASE_URL)?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .foreign_keys(true)
        .busy_timeout(Duration::from_secs(3));

    let pool = SqlitePoolOptions::new()
        .max_connections(6)
        .min_connections(1)
        .connect_with(opts)
        .await?;

    DB.set(pool).map_err(|_| anyhow!("Database already initialized"))?;
    Ok(())
}

pub fn get_db() -> Result<&'static SqlitePool> {
    DB.get().ok_or(anyhow!("Database not initialized"))
}

pub async fn close_db() -> Result<()> {
    if let Some(pool) = DB.get() {
        pool.close().await;
    }
    Ok(())
}

pub async fn init_meta() -> Result<()> {
    let sql = Table::create()
        .table(Meta::Table)
        .if_not_exists()
        .col(ColumnDef::new(Meta::Name).text().not_null().primary_key())
        .col(
            ColumnDef::new(Meta::Version)
                .integer()
                .not_null()
                .default(0),
        )
        .to_string(SqliteQueryBuilder);

    let pool = get_db()?;
    sqlx::query(&sql).execute(pool).await?;
    Ok(())
}

pub async fn get_version(name: &str) -> Result<i32> {
    let (sql, values) = Query::select()
        .column(Meta::Version)
        .from(Meta::Table)
        .cond_where(Expr::col(Meta::Name).eq(name))
        .build_sqlx(SqliteQueryBuilder);

    let pool = get_db()?;
    if let Some(row) = sqlx::query_with(&sql, values).fetch_optional(pool).await? {
        Ok(row.try_get::<i32, _>("version")?)
    } else {
        Ok(0)
    }
}

pub async fn set_version(name: &str, value: i32) -> Result<()> {
    let (sql, values) = Query::insert()
        .into_table(Meta::Table)
        .columns([Meta::Name, Meta::Version])
        .values_panic([name.into(), value.into()])
        .on_conflict(
            OnConflict::column(Meta::Name)
                .update_column(Meta::Version)
                .to_owned(),
        )
        .build_sqlx(SqliteQueryBuilder);

    let pool = get_db()?;
    sqlx::query_with(&sql, values).execute(pool).await?;
    Ok(())
}

pub async fn import(input: PathBuf) -> Result<()> {
    // 备份当前数据库
    let backup_path = STORAGE_PATH.with_extension("bak");
    if fs::metadata(&*STORAGE_PATH).await.is_ok() {
        fs::copy(&*STORAGE_PATH, &backup_path).await?;
    }
    
    // 关闭当前连接
    close_db().await?;
    
    // 替换数据库文件
    let target = STORAGE_PATH.to_string_lossy();
    let _ = fs::remove_file(&*target).await;
    let _ = fs::remove_file(&format!("{target}-wal")).await;
    let _ = fs::remove_file(&format!("{target}-shm")).await;
    fs::copy(&input, &*target).await?;
    
    // 重新初始化
    init_db().await?;
    Ok(())
}

pub async fn export(output: PathBuf) -> Result<()> {
    let pool = get_db()?;
    let mut conn = pool.acquire().await?;
    
    // 确保所有数据写入磁盘
    sqlx::query("PRAGMA wal_checkpoint(FULL);")
        .execute(&mut *conn)
        .await?;
    
    // 执行VACUUM导出
    let output_str = output.to_string_lossy().replace('\'', "''");
    sqlx::query(&format!("VACUUM INTO '{output_str}';"))
        .execute(&mut *conn)
        .await?;
    
    Ok(())
}
