mod core;

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use core::{CoreRuntime, CoreStatus, DesktopBootstrap};
use tauri::State;
use tracing_subscriber::EnvFilter;

#[tauri::command]
async fn desktop_bootstrap(runtime: State<'_, CoreRuntime>) -> Result<DesktopBootstrap, String> {
    runtime.bootstrap().await
}

#[tauri::command]
fn desktop_status(runtime: State<'_, CoreRuntime>) -> CoreStatus {
    runtime.status()
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    init_tracing();
    let runtime = CoreRuntime::new();
    let setup_runtime = runtime.clone();
    let exit_runtime = runtime.clone();
    let shutdown_started = Arc::new(AtomicBool::new(false));
    let allow_exit = Arc::new(AtomicBool::new(false));
    let app = tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_shell::init())
        .manage(runtime)
        .setup(move |app| {
            setup_runtime
                .launch(app.handle())
                .map_err(std::io::Error::other)?;
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![desktop_bootstrap, desktop_status])
        .build(tauri::generate_context!())
        .expect("failed to build NorthBridge desktop application");

    app.run(move |app, event| match event {
        tauri::RunEvent::ExitRequested { code, api, .. } if !allow_exit.load(Ordering::SeqCst) => {
            api.prevent_exit();
            if !shutdown_started.swap(true, Ordering::SeqCst) {
                let runtime = exit_runtime.clone();
                let app = app.clone();
                let allow_exit = allow_exit.clone();
                tauri::async_runtime::spawn(async move {
                    runtime.shutdown().await;
                    allow_exit.store(true, Ordering::SeqCst);
                    app.exit(code.unwrap_or_default());
                });
            }
        }
        tauri::RunEvent::Exit => exit_runtime.force_shutdown(),
        _ => {}
    });
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("northbridge=info,tauri=info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .compact()
        .try_init();
}
