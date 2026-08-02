use std::{env, fs, path::PathBuf};

fn main() {
    embed_sidecar_metadata();
    tauri_build::build();
}

fn embed_sidecar_metadata() {
    let target = env::var("TARGET").expect("Cargo must provide TARGET to build scripts");
    let manifest_directory = PathBuf::from(
        env::var_os("CARGO_MANIFEST_DIR").expect("Cargo must provide CARGO_MANIFEST_DIR"),
    )
    .join("binaries");

    embed_manifest(
        &manifest_directory.join(format!("agentgateway-{target}.manifest.json")),
        "agentgateway",
        Some("northbridge-loopback-v1"),
        "NORTHBRIDGE_BUNDLED_AGENTGATEWAY_SHA256",
        Some("NORTHBRIDGE_BUNDLED_AGENTGATEWAY_PROFILE"),
    );
    embed_manifest(
        &manifest_directory.join(format!("mistralrs-{target}.manifest.json")),
        "mistral.rs",
        Some("loopback-cli-no-agent-tools-v1"),
        "NORTHBRIDGE_BUNDLED_MISTRALRS_SHA256",
        None,
    );
}

fn embed_manifest(
    path: &std::path::Path,
    expected_component: &str,
    expected_profile: Option<&str>,
    hash_environment_name: &str,
    profile_environment_name: Option<&str>,
) {
    println!("cargo:rerun-if-changed={}", path.display());
    let contents = match fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            println!(
                "cargo:warning={} sidecar manifest is not staged; local AI will remain unavailable",
                expected_component
            );
            return;
        }
        Err(error) => panic!("failed to read {}: {error}", path.display()),
    };
    let manifest: serde_json::Value = serde_json::from_str(&contents)
        .unwrap_or_else(|error| panic!("invalid sidecar manifest {}: {error}", path.display()));
    let component = required_string(&manifest, "component", path);
    assert_eq!(
        component,
        expected_component,
        "sidecar component mismatch in {}",
        path.display()
    );
    let hash = required_string(&manifest, "binarySha256", path);
    assert!(
        hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "invalid binarySha256 in {}",
        path.display()
    );
    println!("cargo:rustc-env={hash_environment_name}={hash}");

    if let Some(expected_profile) = expected_profile {
        let profile = required_string(&manifest, "securityProfile", path);
        assert_eq!(
            profile,
            expected_profile,
            "sidecar security profile mismatch in {}",
            path.display()
        );
        if let Some(profile_environment_name) = profile_environment_name {
            println!("cargo:rustc-env={profile_environment_name}={profile}");
        }
    }
}

fn required_string<'a>(
    manifest: &'a serde_json::Value,
    field: &str,
    path: &std::path::Path,
) -> &'a str {
    manifest[field]
        .as_str()
        .unwrap_or_else(|| panic!("missing string field {field} in {}", path.display()))
}
