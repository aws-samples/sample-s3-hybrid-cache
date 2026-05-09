use chrono::Utc;

fn main() {
    // Use semantic version from Cargo.toml
    let version = env!("CARGO_PKG_VERSION");

    // Generate compilation timestamp for additional build info
    let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string();

    // Set the version as the primary version string
    println!("cargo:rustc-env=BUILD_VERSION={}", version);

    // Set the compilation timestamp as additional build info
    println!("cargo:rustc-env=BUILD_TIMESTAMP={}", timestamp);
}
