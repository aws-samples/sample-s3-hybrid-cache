use chrono::Utc;

fn main() {
    // Without any rerun-if-changed line Cargo reruns this script when any file in the package
    // changes, including logs under tmp/, and every rerun recompiles the crate. The version
    // comes from Cargo.toml, which Cargo tracks itself. `src` is listed so BUILD_TIMESTAMP still
    // moves whenever the code the binary is built from changes.
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src");

    // Use semantic version from Cargo.toml
    let version = env!("CARGO_PKG_VERSION");

    // Generate compilation timestamp for additional build info
    let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string();

    // Set the version as the primary version string
    println!("cargo:rustc-env=BUILD_VERSION={}", version);

    // Set the compilation timestamp as additional build info
    println!("cargo:rustc-env=BUILD_TIMESTAMP={}", timestamp);
}
