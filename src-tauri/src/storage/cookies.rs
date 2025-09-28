use anyhow::{anyhow, Context, Result};
use regex::Regex;
use sea_query::{
    ColumnDef, Expr, Iden, OnConflict, Query, SqliteQueryBuilder, Table, TableCreateStatement,
};
use sea_query_binder::SqlxBinder;
use serde::{Deserialize, Serialize};
use sqlx::Row;
use std::collections::BTreeMap;
use time::{macros::format_description, PrimitiveDateTime};

use crate::storage::db::{get_db, TableSpec};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CookieRow {
    pub name: String,
    pub value: String,
    pub path: Option<String>,
    pub domain: Option<String>,
    pub expires: Option<i64>,
    pub httponly: bool,
    pub secure: bool,
}

#[derive(Iden)]
pub enum Cookies {
    Table,
    Name,
    Value,
    Path,
    Domain,
    Expires,
    Httponly,
    Secure,
}

pub struct CookiesTable;

impl TableSpec for CookiesTable {
    const NAME: &'static str = "cookies";
    const LATEST: i32 = 1;
    
    fn create_stmt() -> TableCreateStatement {
        Table::create()
            .table(Cookies::Table)
            .if_not_exists() // 优化点1：防止表已存在时报错
            .col(
                ColumnDef::new(Cookies::Name)
                    .text()
                    .not_null()
                    .primary_key(),
            )
            .col(ColumnDef::new(Cookies::Value).text().not_null())
            .col(ColumnDef::new(Cookies::Path).text().null())
            .col(ColumnDef::new(Cookies::Domain).text().null())
            .col(ColumnDef::new(Cookies::Expires).integer().null())
            .col(ColumnDef::new(Cookies::Httponly).boolean().not_null())
            .col(ColumnDef::new(Cookies::Secure).boolean().not_null())
            .to_owned()
    }
}

pub async fn load() -> Result<BTreeMap<String, String>> {
    // 优化点2：提前获取数据库连接
    let pool = get_db().await?;
    
    // 优化点3：简化查询构建
    let (sql, values) = Query::select()
        .columns([Cookies::Name, Cookies::Value])
        .from(Cookies::Table)
        .build_sqlx(SqliteQueryBuilder);

    let rows = sqlx::query_with(&sql, values).fetch_all(&pool).await?;
    let mut result = BTreeMap::new();
    
    // 优化点4：简化行处理
    for r in rows {
        result.insert(r.try_get("name")?, r.try_get("value")?);
    }
    Ok(result)
}

pub async fn insert(cookie: String) -> Result<()> {
    // 优化点5：预编译正则表达式（假设多次调用insert）
    lazy_static::lazy_static! {
        static ref RE_NAME_VALUE: Regex = Regex::new(r"^([^=]+)=([^;]+)").unwrap();
        static ref RE_ATTRIBUTE: Regex = Regex::new(r"(?i)\b(path|domain|expires|httponly|secure)\b(?:=([^;]*))?").unwrap();
    }

    let captures = RE_NAME_VALUE
        .captures(&cookie)
        .context(anyhow!("Invalid Cookie"))?;
    
    // 优化点6：简化名称和值提取
    let name = captures.get(1)
        .ok_or(anyhow!("Failed to get name from cookie"))?
        .as_str()
        .trim()
        .to_string();
    
    let value = captures.get(2)
        .ok_or(anyhow!("Failed to get value from cookie"))?
        .as_str()
        .trim()
        .to_string();

    let mut row = CookieRow {
        name,
        value,
        path: None,
        domain: None,
        expires: None,
        httponly: false,
        secure: false,
    };

    // 优化点7：简化属性处理
    for cap in RE_ATTRIBUTE.captures_iter(&cookie) {
        let key = cap.get(1).map_or("", |m| m.as_str()).to_lowercase();
        let value = cap.get(2).map_or("", |m| m.as_str().trim());
        
        match key.as_str() {
            "path" => row.path = Some(value.to_string()),
            "domain" => row.domain = Some(value.to_string()),
            "expires" => {
                let fmt = format_description!(
                    "[weekday repr:short], [day] [month repr:short] [year] [hour]:[minute]:[second] GMT"
                );
                row.expires = Some(
                    PrimitiveDateTime::parse(value, &fmt)?
                        .assume_utc()
                        .unix_timestamp()
                );
            }
            "httponly" => row.httponly = true,
            "secure" => row.secure = true,
            _ => (),
        }
    }

    // 优化点8：简化SQL构建
    let (sql, values) = Query::insert()
        .into_table(Cookies::Table)
        .columns([
            Cookies::Name, Cookies::Value, Cookies::Path,
            Cookies::Domain, Cookies::Expires, Cookies::Httponly,
            Cookies::Secure,
        ])
        .values_panic([
            row.name.into(),
            row.value.into(),
            row.path.into(),
            row.domain.into(),
            row.expires.into(),
            row.httponly.into(),
            row.secure.into(),
        ])
        .on_conflict(
            OnConflict::column(Cookies::Name)
                .update_columns([
                    Cookies::Value, Cookies::Path, Cookies::Domain,
                    Cookies::Expires, Cookies::Httponly, Cookies::Secure,
                ])
                .to_owned(),
        )
        .build_sqlx(SqliteQueryBuilder);

    let pool = get_db().await?;
    sqlx::query_with(&sql, values).execute(&pool).await?;
    Ok(())
}

pub async fn delete(name: String) -> Result<()> {
    // 优化点9：简化删除操作
    let pool = get_db().await?;
    let (sql, values) = Query::delete()
        .from_table(Cookies::Table)
        .cond_where(Expr::col(Cookies::Name).eq(name))
        .build_sqlx(SqliteQueryBuilder);

    sqlx::query_with(&sql, values).execute(&pool).await?;
    Ok(())
}
