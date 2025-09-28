use anyhow::Result;
use sea_query::{
    ColumnDef, Expr, Iden, OnConflict, Query, SqliteQueryBuilder, Table, TableCreateStatement,
};
use sea_query_binder::SqlxBinder;
use sqlx::Row;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{
    queue::{
        runtime::TASK_MANAGER,
        types::{Task, TaskState},
    },
    shared::get_ts,
};

use super::db::{get_db, TableSpec};

#[derive(Iden)]
pub enum Archive {
    Table,
    Name,
    Value,
    UpdatedAt,
}

pub struct ArchiveTable;

impl TableSpec for ArchiveTable {
    const NAME: &'static str = "archive";
    const LATEST: i32 = 2;
    
    fn create_stmt() -> TableCreateStatement {
        Table::create()
            .table(Archive::Table)
            .if_not_exists()  // 优化点1：添加if_not_exists避免表已存在时报错
            .col(
                ColumnDef::new(Archive::Name)
                    .text()
                    .not_null()
                    .primary_key(),
            )
            .col(ColumnDef::new(Archive::Value).text().not_null())
            .col(ColumnDef::new(Archive::UpdatedAt).integer().not_null())
            .to_owned()
    }
}

pub async fn load() -> Result<()> {
    // 优化点2：提前获取数据库连接
    let pool = get_db().await?;
    
    let (sql, values) = Query::select()
        .columns([Archive::Value])
        .from(Archive::Table)
        .build_sqlx(SqliteQueryBuilder);

    let rows = sqlx::query_with(&sql, values).fetch_all(&pool).await?;
    
    // 优化点3：单次写操作清空任务列表
    let mut tasks = TASK_MANAGER.tasks.write().await;
    tasks.clear();
    
    // 优化点4：预先分配内存空间
    if !rows.is_empty() {
        tasks.reserve(rows.len());
    }
    
    for r in rows {
        // 优化点5：简化值获取
        let value_str: String = r.try_get("value")?;
        let mut task: Task = serde_json::from_str(&value_str)?;
        
        // 优化点6：简化状态转换逻辑
        if task.state == TaskState::Active {
            task.state = TaskState::Paused;
        }
        
        let id = task.id.clone();
        tasks.insert(id, Arc::new(RwLock::new(task)));
    }
    
    Ok(())
}

pub async fn upsert(task: &Task) -> Result<()> {
    let pool = get_db().await?;
    let now = get_ts(true);
    let name = task.id.clone();
    let value = serde_json::to_string(task)?;
    
    // 优化点7：简化查询构建
    let (sql, values) = Query::insert()
        .into_table(Archive::Table)
        .columns([Archive::Name, Archive::Value, Archive::UpdatedAt])
        .values_panic([name.into(), value.into(), now.into()])
        .on_conflict(
            OnConflict::column(Archive::Name)
                .update_columns([Archive::Value, Archive::UpdatedAt])
                .to_owned(),
        )
        .build_sqlx(SqliteQueryBuilder);

    sqlx::query_with(&sql, values).execute(&pool).await?;
    Ok(())
}

pub async fn delete(name: &str) -> Result<()> {
    // 优化点8：简化删除操作
    let pool = get_db().await?;
    let (sql, values) = Query::delete()
        .from_table(Archive::Table)
        .cond_where(Expr::col(Archive::Name).eq(name))
        .build_sqlx(SqliteQueryBuilder);

    sqlx::query_with(&sql, values).execute(&pool).await?;
    Ok(())
}
