pub mod commands;
pub mod errors;
pub mod services;
pub mod shared;
pub mod storage;

use commands::*;
use tauri::Manager;
use tauri_specta::{collect_commands, collect_events, Builder};

#[cfg(debug_assertions)]
use tauri_plugin_log::fern::colors::{Color, ColoredLevelConfig};

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() -> Result<(), Box<dyn std::error::Error>> {
    // 设置全局 panic 处理
    std::panic::set_hook(Box::new(|e| {
        let bt = std::backtrace::Backtrace::force_capture();
        log::error!("Panicked: {e}");
        log::error!("Backtrace:\n{bt:?}");
    }));

    // 1. 命令和事件注册优化
    let builder = Builder::<tauri::Wry>::new()
        .commands(collect_commands![
            // 分组命令，提高可读性
            // 基础功能
            meta, init, set_window, config_write, 
            open_cache, get_size, clean_cache,
            db_import, db_export, export_data,
            
            // 登录相关
            stop_login, exit, sms_login, pwd_login,
            switch_cookie, scan_login, refresh_cookie,
            
            // 队列相关
            submit_task, process_queue, open_folder,
            ctrl_event, update_max_conc, update_select
        ])
        .events(collect_events![
            shared::HeadersData,
            shared::ProcessError,
            queue::runtime::QueueEvent
        ]);

    // 2. 开发环境专用配置
    #[cfg(debug_assertions)]
    {
        // 生成 TypeScript 类型定义
        builder
            .export(
                specta_typescript::Typescript::default()
                    .bigint(specta_typescript::BigIntExportBehavior::Number)
                    .header("// @ts-nocheck"),
                "../src/services/backend.ts",
            )
            .expect("Failed to export typescript bindings");
    }

    // 3. 日志系统配置优化
    let log_builder = {
        let builder = tauri_plugin_log::Builder::new()
            .timezone_strategy(tauri_plugin_log::TimezoneStrategy::UseLocal)
            .rotation_strategy(tauri_plugin_log::RotationStrategy::KeepAll)
            .level(log::LevelFilter::Info)
            .level_for("sqlx::query", log::LevelFilter::Warn);

        #[cfg(debug_assertions)]
        let builder = builder.with_colors(
            ColoredLevelConfig::new()
                .error(Color::Red)
                .warn(Color::Yellow)
                .info(Color::Green)
                .debug(Color::Blue)
                .trace(Color::Magenta),
        );

        builder
    };

    // 4. Tauri 应用构建
    tauri::Builder::default()
        // 核心插件
        .plugin(log_builder.build())
        .plugin(tauri_plugin_clipboard_manager::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_http::init())
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_os::init())
        .plugin(tauri_plugin_process::init())
        .plugin(tauri_plugin_shell::init())
        // 单实例处理
        .plugin(tauri_plugin_single_instance::init(|app, _, _| {
            app.get_webview_window("main")
                .expect("No main window found")
                .set_focus()
                .expect("Failed to focus window");
        }))
        // 自动更新
        .plugin(tauri_plugin_updater::Builder::new().build())
        // 命令处理器
        .invoke_handler(builder.invoke_handler())
        // 应用初始化
        .setup(move |app| {
            // 记录版本信息
            log::info!("BiliTools v{}", app.package_info().version);
            
            // 挂载事件处理器
            builder.mount_events(app);
            
            // 设置全局应用句柄
            shared::APP_HANDLE.set(app.app_handle().clone())?;
            
            // 开发工具
            #[cfg(debug_assertions)]
            if let Some(window) = app.get_webview_window("main") {
                window.open_devtools();
            }
            
            // 异步初始化
            tauri::async_runtime::spawn(async move {
                storage::init().await?;
                services::init().await?;
                Ok::<(), crate::TauriError>(())
            });
            
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("Failed to run BiliTools");
    
    Ok(())
}
