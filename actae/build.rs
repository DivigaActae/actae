// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    // Commit identifier for `actae --version`. Prefers the GIT_SHA env var
    // (set by the release workflow for reproducible builds); falls back to
    // `git rev-parse` in a checkout.
    println!("cargo:rerun-if-env-changed=GIT_SHA");
    let git_sha = env::var("GIT_SHA")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| {
            std::process::Command::new("git")
                .args(["rev-parse", "--short=12", "HEAD"])
                .output()
                .ok()
                .filter(|o| o.status.success())
                .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        })
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=ACTAE_GIT_SHA={git_sha}");

    // License-signing public key: release builds MUST set
    // ACTAE_LICENSE_PUBKEY to the production key (base64url of the 32-byte
    // Ed25519 public key, as printed by `python -m actae_saas admin
    // license-key generate`). When unset (dev/test builds) the binary
    // falls back to the shared test public key so the conformance stub can
    // sign responses. The value is validated here so a release build can
    // never silently ship the test key.
    println!("cargo:rerun-if-env-changed=ACTAE_LICENSE_PUBKEY");
    if let Ok(raw) = env::var("ACTAE_LICENSE_PUBKEY") {
        let trimmed = raw.trim().trim_end_matches('=');
        use base64::engine::general_purpose::URL_SAFE_NO_PAD;
        use base64::Engine;
        let bytes = URL_SAFE_NO_PAD
            .decode(trimmed)
            .unwrap_or_else(|e| panic!("ACTAE_LICENSE_PUBKEY is not valid base64url: {e}"));
        assert_eq!(
            bytes.len(),
            32,
            "ACTAE_LICENSE_PUBKEY must decode to exactly 32 bytes (got {})",
            bytes.len()
        );
        println!("cargo:rustc-env=ACTAE_LICENSE_PUBKEY_B64={trimmed}");
    }

    // Embedded PostgreSQL jar: with the `pg-embedded` feature the zonky jar
    // (a ZIP containing an XZ-compressed tar of the PG runtime) is baked into
    // the binary via ACTAE_PG_EMBED_JAR. Production builds REQUIRE it — a
    // release without the PG bytes is a broken distribution, so we fail fast.
    // Default/dev builds omit the bytes and managed storage downloads instead.
    if env::var("CARGO_FEATURE_PG_EMBEDDED").is_ok() {
        println!("cargo:rerun-if-env-changed=ACTAE_PG_EMBED_JAR");
        let jar = env::var("ACTAE_PG_EMBED_JAR").unwrap_or_else(|_| {
            panic!(
                "ACTAE_PG_EMBED_JAR must point at the zonky PostgreSQL jar \
                 (io/zonky/test/postgres/embedded-postgres-binaries-<platform>-<version>.jar) \
                 when building with the `pg-embedded` feature"
            )
        });
        let src = PathBuf::from(jar);
        let meta = fs::metadata(&src)
            .unwrap_or_else(|e| panic!("ACTAE_PG_EMBED_JAR {src:?} is not readable: {e}"));
        assert!(meta.is_file(), "ACTAE_PG_EMBED_JAR {src:?} is not a file");
        let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
        let embedded_dir = out_dir.join("pg-embedded");
        fs::create_dir_all(&embedded_dir).expect("failed to create pg-embedded OUT_DIR");
        let dest = embedded_dir.join("postgresql.jar");
        fs::copy(&src, &dest)
            .unwrap_or_else(|e| panic!("failed to copy PG jar {src:?} -> {dest:?}: {e}"));
        println!("cargo:rustc-cfg=pg_embedded");
        println!("cargo:rustc-env=ACTAE_PG_EMBED_JAR_PATH={}", dest.display());
        // The zonky classifier of the embedded package (e.g. `linux-amd64`),
        // from ACTAE_PG_EMBED_PLATFORM or parsed from the jar filename. The
        // runtime resolver compares it against the CURRENT platform: on a
        // musl/Alpine host it differs, so managed storage downloads the
        // platform-matched package instead of extracting the glibc one.
        let platform = env::var("ACTAE_PG_EMBED_PLATFORM")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .or_else(|| {
                let fname = src
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("embedded-postgres-binaries-<platform>-18.2.0.jar");
                fname
                    .strip_prefix("embedded-postgres-binaries-")
                    .and_then(|rest| rest.split_once("-18.2.0.jar"))
                    .map(|(p, _)| p.to_string())
            })
            .unwrap_or_else(|| {
                panic!(
                    "cannot determine the embedded zonky platform: set ACTAE_PG_EMBED_PLATFORM \
                 or use a jar named embedded-postgres-binaries-<platform>-18.2.0.jar"
                )
            });
        println!("cargo:rerun-if-env-changed=ACTAE_PG_EMBED_PLATFORM");
        println!("cargo:rustc-env=ACTAE_PG_EMBED_PLATFORM={platform}");
        println!(
            "cargo:warning=embedded PostgreSQL jar ({platform}) baked into the binary ({} bytes)",
            meta.len()
        );
    }

    // Only run when mimalloc feature is enabled
    if env::var("CARGO_FEATURE_ALLOCATOR").is_ok() {
        println!("cargo:rustc-cfg=feature=\"mimalloc_enabled\"");

        // Set mimalloc features based on enabled features
        if env::var("CARGO_FEATURE_ALLOCATOR_SECURE").is_ok() {
            println!("cargo:rustc-cfg=feature=\"mimalloc_secure\"");
            println!("cargo:rustc-env=MIMALLOC_SECURE=1");
        }

        if env::var("CARGO_FEATURE_ALLOCATOR_STATS").is_ok() {
            println!("cargo:rustc-cfg=feature=\"mimalloc_stats\"");

            println!("cargo:rustc-env=MIMALLOC_SHOW_STATS=1");
        }

        // Platform-specific configuration
        let target_os = env::var("CARGO_CFG_TARGET_OS").unwrap();
        let target_arch = env::var("CARGO_CFG_TARGET_ARCH").unwrap();

        match target_os.as_str() {
            "linux" => {
                println!("cargo:rustc-link-lib=pthread");
                if target_arch == "x86_64" {
                    // Enable specific optimizations for x86_64 Linux
                    println!("cargo:rustc-env=MIMALLOC_USE_COMPOUND_TLS=1");
                }
            }
            "macos" => {
                println!("cargo:rustc-link-lib=System");
                // macOS specific optimizations
                println!("cargo:rustc-env=MIMALLOC_USE_COMPOUND_TLS=1");
            }
            "windows" => {
                // Windows specific configuration
                if target_arch == "x86_64" {
                    println!("cargo:rustc-env=MIMALLOC_USE_COMPOUND_TLS=1");
                }
            }
            _ => {
                // Default configuration for other platforms
            }
        }

        // Performance optimizations
        println!("cargo:rustc-env=MIMALLOC_LARGE_OS_PAGES=1");

        // Print some helpful information
        println!("cargo:warning=mimalloc allocator enabled for target: {target_os}-{target_arch}");

        if env::var("CARGO_FEATURE_ALLOCATOR_SECURE").is_ok() {
            println!("cargo:warning=mimalloc secure mode enabled");
        }

        if env::var("CARGO_FEATURE_ALLOCATOR_STATS").is_ok() {
            println!("cargo:warning=mimalloc statistics collection enabled");
        }
    }
}
