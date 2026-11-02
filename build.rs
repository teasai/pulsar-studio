fn main() {
    #[cfg(target_os = "windows")]
    {
        if std::path::Path::new("assets/app.ico").exists() {
            let mut res = winresource::WindowsResource::new();
            res.set_icon("assets/app.ico");
            if let Err(e) = res.compile() {
                eprintln!("[build] embedding icon failed: {e}");
            }
        }
    }
    println!("cargo:rerun-if-changed=assets/app.ico");
}
