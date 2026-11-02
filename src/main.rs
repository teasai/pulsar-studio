#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use pulsar_studio::app::PulsarStudio;

fn load_icon() -> Option<egui::IconData> {
    let bytes = include_bytes!("../assets/app.png");
    let img = image::load_from_memory(bytes).ok()?.to_rgba8();
    let (width, height) = img.dimensions();
    Some(egui::IconData {
        rgba: img.into_raw(),
        width,
        height,
    })
}

fn main() -> eframe::Result<()> {
    let mut viewport = egui::ViewportBuilder::default()
        .with_inner_size([1360.0, 860.0])
        .with_min_inner_size([960.0, 600.0])
        .with_title("Pulsar Studio");
    if let Some(icon) = load_icon() {
        viewport = viewport.with_icon(icon);
    }
    let options = eframe::NativeOptions {
        viewport,
        ..Default::default()
    };
    eframe::run_native(
        "Pulsar Studio",
        options,
        Box::new(|cc| Ok(Box::new(PulsarStudio::new(cc)))),
    )
}
