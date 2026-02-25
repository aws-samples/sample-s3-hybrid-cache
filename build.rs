use std::process::Command;

fn main() {
    // Use semantic version from Cargo.toml
    let version = env!("CARGO_PKG_VERSION");

    // Generate compilation timestamp for additional build info
    let output = Command::new("date")
        .args(["+%Y-%m-%d %H:%M:%S UTC"])
        .env("TZ", "UTC")
        .output()
        .expect("Failed to execute date command");

    let timestamp = String::from_utf8(output.stdout)
        .expect("Invalid UTF-8 from date command")
        .trim()
        .to_string();

    // Set the version as the primary version string
    println!("cargo:rustc-env=BUILD_VERSION={}", version);

    // Set the compilation timestamp as additional build info
    println!("cargo:rustc-env=BUILD_TIMESTAMP={}", timestamp);

    // Also set a fallback if date command fails
    if timestamp.is_empty() {
        println!("cargo:rustc-env=BUILD_TIMESTAMP=Unknown");
    }
}
