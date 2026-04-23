mod app;
mod models;
mod mqtt;
mod protocol;
mod store;

use app::MeshBcTesterApp;

fn main() -> eframe::Result {
    let native_options = eframe::NativeOptions::default();
    eframe::run_native(
        "蓝牙Mesh BC灯测试工具",
        native_options,
        Box::new(|cc| Ok(Box::new(MeshBcTesterApp::new(cc)))),
    )
}
