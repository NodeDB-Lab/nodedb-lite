fn main() {
    let crate_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();

    // Create include directory.
    let _ = std::fs::create_dir_all(format!("{crate_dir}/include"));

    let config = cbindgen::Config::from_file("cbindgen.toml").unwrap_or_default();

    match cbindgen::Builder::new()
        .with_crate(&crate_dir)
        .with_config(config)
        .generate()
    {
        Ok(bindings) => {
            bindings.write_to_file(format!("{crate_dir}/include/nodedb_lite.h"));
        }
        Err(e) => {
            eprintln!("cargo:warning=cbindgen failed to generate C bindings: {e}");
        }
    }
}
