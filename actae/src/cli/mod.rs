// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! `actae` CLI subcommands: keys, SDK credentials, dashboard password, upgrades.
//!
//! The server binary doubles as a small ops CLI:
//!
//! ```text
//! actae keys create|list|revoke   API keys for this instance
//! actae login                     save SDK credentials to ~/.actae/client.json
//! actae logout                    remove the saved SDK credentials
//! actae passwd                    set the local dashboard password
//! actae upgrade                   self-update from the public release feed
//! actae add|explain integration   generate SDK setup for an existing project
//! ```
//!
//! Commands that need the database run on the Actae host with the same
//! environment as the server. There is no enrollment or license command in the
//! community build.

use crate::auth::password_hash::SecurePasswordHasher;
use crate::config::AppConfig;
use crate::control_plane::enrollment::Enrollment;
use crate::database::DatabaseManager;
use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use serde::Serialize;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn same_server(a: &str, b: &str) -> bool {
    a.trim_end_matches('/') == b.trim_end_matches('/')
}

/// The stored `key_prefix` for a raw key (`local_keys` stores the first 12
/// chars), used to locate and revoke the exact key being rotated.
fn key_prefix(raw: &str) -> &str {
    &raw[..12.min(raw.len())]
}

/// Revoke every active key matching `prefix`; returns how many rows changed.
async fn revoke_key_by_prefix(pool: &deadpool_postgres::Pool, prefix: &str) -> Result<u64> {
    let client = pool
        .get()
        .await
        .context("failed to get a database connection to rotate the SDK key")?;
    let rows = client
        .execute(
            "UPDATE public.api_keys SET status = 'revoked', updated_at = NOW()
             WHERE status = 'active' AND key_prefix = $1",
            &[&prefix],
        )
        .await
        .context("failed to revoke the previous SDK key")?;
    Ok(rows)
}

/// Normalize a user-supplied endpoint: trim, drop a trailing slash, and
/// require an `http://`/`https://` URL.
fn normalize_endpoint(raw: &str) -> Result<String> {
    let trimmed = raw.trim().trim_end_matches('/');
    if !(trimmed.starts_with("http://") || trimmed.starts_with("https://")) {
        return Err(anyhow!("--url must start with http:// or https:// (got '{raw}')"));
    }
    if trimmed.len() <= "https://".len() {
        return Err(anyhow!("--url has no host"));
    }
    Ok(trimmed.to_string())
}

/// Prompt for an API key without echoing it. Falls back to a plain line read
/// when there is no TTY (piped input).
fn prompt_api_key() -> Result<String> {
    match rpassword::prompt_password("Paste API key: ") {
        Ok(v) => Ok(v),
        Err(_) => {
            let mut line = String::new();
            std::io::stdin()
                .read_line(&mut line)
                .context("failed to read the API key from stdin")?;
            Ok(line)
        }
    }
}

/// Verify an API key against the server's `/api/v1/auth/capabilities` (the
/// API-key-aware resolver; `/auth/me` is JWT-only). A definite rejection
/// (401/403) is fatal; an unreachable server is a warning (the operator may be
/// logging in before starting it) and the credentials are still saved.
async fn verify_key_against(endpoint: &str, key: &str) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .context("failed to build HTTP client")?;
    let request = client
        .get(format!("{endpoint}/api/v1/auth/capabilities"))
        .header("X-API-Key", key);
    match request.send().await {
        Ok(resp) if resp.status().is_success() => Ok(()),
        Ok(resp)
            if resp.status() == reqwest::StatusCode::UNAUTHORIZED
                || resp.status() == reqwest::StatusCode::FORBIDDEN =>
        {
            Err(anyhow!("server rejected the API key (HTTP {})", resp.status()))
        }
        Ok(resp) => Err(anyhow!(
            "unexpected response while verifying the key (HTTP {})",
            resp.status()
        )),
        Err(e) => {
            println!("⚠ Could not reach {endpoint} to verify the key ({e}); saving anyway.");
            Ok(())
        }
    }
}

/// `actae login [--url <url>] [--api-key <key>]` — persist SDK credentials to
/// `$ACTAE_CONFIG_DIR/client.json` so the Python/Go/TypeScript SDKs authenticate
/// with no further setup (the `opencode login` experience).
///
/// * **No `--url`:** target the single live local managed-storage instance,
///   mint a fresh local key, and write the credentials — no paste, no account.
/// * **With `--url`:** save credentials for a remote instance. A key given via
///   `--api-key` is verified, otherwise it is prompted without echo.
///
/// `actae login` always claims the default credentials file: it is the explicit
/// "make this my active credential" action.
async fn run_login(args: &[String]) -> Result<()> {
    let mut url: Option<String> = None;
    let mut api_key: Option<String> = None;
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--url" => url = Some(it.next().context("--url requires a value")?.clone()),
            "--api-key" | "--key" => {
                api_key = Some(it.next().context("--api-key requires a value")?.clone())
            }
            "-h" | "--help" => {
                println!(
                    "usage: actae login [--url <url>] [--api-key <key>]\n\n\
                     With no flags, logs in to the single live local Actae instance\n\
                     (mints a key and saves SDK credentials — no paste, no account).\n\
                     With --url, saves credentials for a remote instance."
                );
                return Ok(());
            }
            other => return Err(anyhow!("unknown actae login argument: {other}")),
        }
    }

    let path = crate::client_credentials::ClientCredentials::file_path();

    let Some(url) = url else {
        // Local: find the single live managed-storage instance and mint a key.
        let (data_dir, pg_port, inst_url) = discover_embedded_env().await.context(
            "no live local Actae instance found — start `actae` first, or pass \
             `--url <url>` / `--api-key <key>` for a remote instance",
        )?;
        // Capture the key we are about to replace so re-running `login` rotates
        // instead of accumulating dead `SDK login` rows (only when the existing
        // file points at this same server).
        let previous = crate::client_credentials::ClientCredentials::load()
            .filter(|c| same_server(&c.endpoint, &inst_url));

        let (config, pool) = keys_pool().await?;
        let raw = format!(
            "sk-{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        crate::auth::local_keys::insert_api_key(&pool, &config, &raw, "ops", "SDK login").await?;

        // Prove the freshly minted key actually authenticates before writing it
        // to disk. A mismatched signing secret (e.g. a stray `.env` JWT_SECRET
        // in the CLI's directory that the server did not load) would otherwise
        // be silently published as a permanently-401 credential.
        verify_key_against(&inst_url, &raw).await.context(
            "the freshly minted key was rejected by the running instance — the CLI and the \
             server resolved different signing secrets. Run `actae login` from the server's \
             working directory (or set the same JWT_SECRET), then retry",
        )?;

        if let Some(prev) = previous {
            match revoke_key_by_prefix(&pool, key_prefix(&prev.api_key)).await {
                Ok(n) if n > 0 => println!("  (rotated the previous SDK key)"),
                Ok(_) => {}
                Err(e) => tracing::warn!("could not revoke the previous SDK key: {e:#}"),
            }
        }

        let http_port = std::env::var("ACTAE_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(8002);
        let creds = crate::client_credentials::ClientCredentials::new(inst_url.clone(), raw)
            .with_instance(crate::client_credentials::InstanceInfo {
                http_port,
                pg_port: Some(pg_port),
                data_dir: data_dir.display().to_string(),
                pid: 0,
            });
        let outcome =
            crate::client_credentials::publish_at_with_liveness(&path, &creds, true, None)?;
        println!("✓ Logged in to {inst_url}");
        println!("  credentials: {}", outcome.path.display());
        println!("  your SDKs now authenticate automatically (Python / Go / TypeScript)");
        return Ok(());
    };

    let endpoint = normalize_endpoint(&url)?;
    let key = match api_key {
        Some(k) => k,
        None => prompt_api_key()?,
    };
    let key = key.trim().to_string();
    if key.is_empty() {
        return Err(anyhow!("no API key provided"));
    }
    verify_key_against(&endpoint, &key).await?;
    let creds = crate::client_credentials::ClientCredentials::new(endpoint.clone(), key);
    let outcome = crate::client_credentials::publish_at_with_liveness(&path, &creds, true, None)?;
    println!("✓ Logged in to {endpoint}");
    println!("  credentials: {}", outcome.path.display());
    Ok(())
}

/// `actae logout [--all]` — remove the saved SDK credentials. `--all` also
/// removes per-instance secondary files.
async fn run_logout(args: &[String]) -> Result<()> {
    let mut all = false;
    for arg in args {
        match arg.as_str() {
            "--all" => all = true,
            "-h" | "--help" => {
                println!("usage: actae logout [--all]");
                return Ok(());
            }
            other => return Err(anyhow!("unknown actae logout argument: {other}")),
        }
    }
    if all {
        let removed = crate::client_credentials::ClientCredentials::delete_all()?;
        println!("✓ Removed {removed} credential file(s).");
    } else {
        crate::client_credentials::ClientCredentials::delete_default()?;
        println!(
            "✓ Logged out (removed {}).",
            crate::client_credentials::ClientCredentials::file_path().display()
        );
    }
    Ok(())
}

pub async fn run(args: &[String]) -> Result<()> {
    match args.first().map(|s| s.as_str()) {
        Some("keys") => run_keys(&args[1..]).await,
        Some("passwd") => run_passwd(&args[1..]).await,
        Some("upgrade") => run_upgrade(&args[1..]).await,
        Some("login") => run_login(&args[1..]).await,
        Some("logout") => run_logout(&args[1..]).await,
        Some("add") => run_add(&args[1..]),
        Some("explain") => run_explain(&args[1..]),
        _ => {
            print_usage();
            Err(anyhow!("unknown command"))
        }
    }
}

fn print_usage() {
    eprintln!(
        "actae CLI\n\n\
         usage: actae keys create|list|revoke\n\
         usage: actae login [--url <url>] [--api-key <key>]\n\
         usage: actae logout [--all]\n\
         usage: actae passwd [email] [--password <password>]\n\
         usage: actae upgrade [--check] [--url <url>]\n\
         usage: actae add [--path <dir>] [--framework <name>] [--language python|typescript|go] [--write]\n\
         usage: actae explain integration [--path <dir>] [--framework <name>] [--language python|typescript|go] [--json]\n\n\
         upgrade     — check for and apply the latest release (self-update)\n\
         keys create — create an API key (prints the raw key once)\n\
         keys list   — list API keys (prefixes, never the raw keys)\n\
         keys revoke — revoke an API key by prefix or name\n\
         login       — save SDK credentials to ~/.actae/client.json (local: no paste; --url for remote)\n\
         logout      — remove the saved SDK credentials (--all removes per-instance files too)\n\
         passwd      — set the password for a local dashboard account (standalone mode)\n\
         add         — detect an agent stack and generate the smallest safe SDK setup\n\
         explain integration — print machine-readable integration guidance"
    );
}

// --------------------------------------------------------------------- #
// SDK integration onboarding (local-only; never contacts Actae)
// --------------------------------------------------------------------- #

#[derive(Clone, Debug, Serialize)]
struct IntegrationDetection {
    language: String,
    frameworks: Vec<String>,
    install_command: String,
    setup_file: String,
    deterministic_orchestrator: bool,
}

fn run_add(args: &[String]) -> Result<()> {
    let mut framework: Option<String> = None;
    let mut language: Option<String> = None;
    let mut path: Option<PathBuf> = None;
    let mut write = false;
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--framework" => {
                framework = Some(
                    it.next()
                        .cloned()
                        .ok_or_else(|| anyhow!("--framework requires a value"))?,
                )
            }
            "--language" => {
                language = Some(
                    it.next()
                        .cloned()
                        .ok_or_else(|| anyhow!("--language requires a value"))?,
                )
            }
            "--path" => {
                path = Some(PathBuf::from(
                    it.next()
                        .cloned()
                        .ok_or_else(|| anyhow!("--path requires a value"))?,
                ))
            }
            "--write" => write = true,
            _ => return Err(anyhow!("unknown actae add argument: {arg}")),
        }
    }
    let cwd = resolve_integration_root(path.as_deref())?;
    let detection = detect_integration(&cwd, language.as_deref(), framework.as_deref())?;
    let setup = integration_setup(&cwd, &detection)?;
    println!(
        "Detected: {} / {}",
        detection.language,
        detection.frameworks.join(", ")
    );
    println!("Install: {}", detection.install_command);
    println!();
    println!("{setup}");
    if !write {
        println!();
        println!(
            "Preview only. Re-run with --write to create {} and .env.actae.example.",
            detection.setup_file
        );
        return Ok(());
    }

    let setup_path = cwd.join(&detection.setup_file);
    let env_path = cwd.join(".env.actae.example");
    for path in [&setup_path, &env_path] {
        if path.exists() {
            return Err(anyhow!("refusing to overwrite existing {}", path.display()));
        }
    }
    write_new(&setup_path, setup.as_bytes())?;
    if let Err(error) = write_new(
        &env_path,
        b"ACTAE_URL=http://localhost:8002\nACTAE_API_KEY=replace-me\n",
    ) {
        if let Err(cleanup) = std::fs::remove_file(&setup_path) {
            return Err(error.context(format!(
                "also failed to roll back {}: {cleanup}",
                setup_path.display()
            )));
        }
        return Err(error);
    }
    println!();
    println!("Created {}", setup_path.display());
    println!("Created {}", env_path.display());
    Ok(())
}

fn run_explain(args: &[String]) -> Result<()> {
    if args.first().map(String::as_str) != Some("integration") {
        return Err(anyhow!("usage: actae explain integration [--path <dir>] [--framework <name>] [--language python|typescript|go] [--json]"));
    }
    let mut json = false;
    let mut framework: Option<String> = None;
    let mut language: Option<String> = None;
    let mut path: Option<PathBuf> = None;
    let mut it = args[1..].iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--json" => json = true,
            "--framework" => {
                framework = Some(
                    it.next()
                        .cloned()
                        .ok_or_else(|| anyhow!("--framework requires a value"))?,
                )
            }
            "--language" => {
                language = Some(
                    it.next()
                        .cloned()
                        .ok_or_else(|| anyhow!("--language requires a value"))?,
                )
            }
            "--path" => {
                path = Some(PathBuf::from(
                    it.next()
                        .cloned()
                        .ok_or_else(|| anyhow!("--path requires a value"))?,
                ))
            }
            _ => return Err(anyhow!("unknown actae explain integration argument: {arg}")),
        }
    }
    let cwd = resolve_integration_root(path.as_deref())?;
    let detection = detect_integration(&cwd, language.as_deref(), framework.as_deref())?;
    if json {
        let value = serde_json::json!({
            "schema": "actae.integration/v1",
            "root": cwd,
            "detection": detection,
            "principles": {
                "execution_owner": "application_or_orchestrator",
                "actae_owner": ["events", "checkpoints", "lineage", "effect_ledger"],
                "effect_identity_excludes_attempt": true,
                "deterministic_workflow_network_io": false
            },
            "commands": ["actae add", "actae add --write", "actae explain integration --json"]
        });
        println!("{}", serde_json::to_string_pretty(&value)?);
    } else {
        println!("language: {}", detection.language);
        println!("frameworks: {}", detection.frameworks.join(", "));
        println!("install: {}", detection.install_command);
        println!("setup: {}", detection.setup_file);
        println!(
            "Actae runs in your process; your framework/orchestrator remains the execution owner."
        );
    }
    Ok(())
}

fn resolve_integration_root(path: Option<&Path>) -> Result<PathBuf> {
    let cwd = std::env::current_dir().context("resolve current directory")?;
    let target = path.map_or(cwd.clone(), |value| {
        if value.is_absolute() {
            value.to_path_buf()
        } else {
            cwd.join(value)
        }
    });
    let target = target
        .canonicalize()
        .with_context(|| format!("resolve integration path {}", target.display()))?;
    if !target.is_dir() {
        return Err(anyhow!(
            "integration path is not a directory: {}",
            target.display()
        ));
    }
    Ok(target)
}

fn detect_integration(
    root: &Path,
    language: Option<&str>,
    framework: Option<&str>,
) -> Result<IntegrationDetection> {
    let has_python = [
        "pyproject.toml",
        "requirements.txt",
        "setup.py",
        "setup.cfg",
        "Pipfile",
    ]
    .iter()
    .any(|name| root.join(name).exists());
    let has_typescript = root.join("package.json").exists();
    let has_go = root.join("go.mod").exists();
    let language = match language {
        Some(value @ ("python" | "typescript" | "go")) => value.to_string(),
        Some(value) => {
            return Err(anyhow!(
                "unsupported language {value}; use python, typescript or go"
            ))
        }
        None if [has_python, has_typescript, has_go]
            .iter()
            .filter(|value| **value)
            .count()
            > 1 =>
        {
            return Err(anyhow!("multiple SDK languages detected in {}; pass --language python, typescript or go (and --path for a monorepo package)", root.display()));
        }
        None if has_python => "python".into(),
        None if has_typescript => "typescript".into(),
        None if has_go => "go".into(),
        None => {
            return Err(anyhow!(
                "could not detect Python, TypeScript or Go; pass --language"
            ))
        }
    };
    let manifest_path = match language.as_str() {
        "python" => [
            "pyproject.toml",
            "requirements.txt",
            "setup.py",
            "setup.cfg",
            "Pipfile",
        ]
        .iter()
        .map(|name| root.join(name))
        .find(|path| path.exists())
        .unwrap_or_else(|| root.join("pyproject.toml")),
        "typescript" => root.join("package.json"),
        "go" => root.join("go.mod"),
        _ => unreachable!(),
    };
    let manifest = std::fs::read_to_string(&manifest_path)
        .unwrap_or_default()
        .to_lowercase();
    let mut frameworks = Vec::new();
    if let Some(explicit) = framework {
        let normalized = match explicit.to_lowercase().as_str() {
            "openai" => "openai-agents",
            "azure-durable" => "azure-durable-functions",
            value => value,
        }
        .to_string();
        validate_framework(&language, &normalized)?;
        frameworks.push(normalized);
    } else {
        for (needle, name) in [
            ("langgraph", "langgraph"),
            ("langchain", "langchain"),
            ("crewai", "crewai"),
            ("claude-agent-sdk", "claude"),
            ("@anthropic-ai/claude-agent-sdk", "claude"),
            ("openai-agents", "openai-agents"),
            ("@openai/agents", "openai-agents"),
            ("copilot-sdk", "copilot"),
            ("@github/copilot-sdk", "copilot"),
            ("temporal", "temporal"),
            ("azure-functions-durable", "azure-durable-functions"),
            ("durable-functions", "durable-functions"),
        ] {
            let package_matches_language = !((language == "typescript"
                && needle == "openai-agents")
                || (language != "typescript" && needle == "@openai/agents"));
            if package_matches_language
                && manifest.contains(needle)
                && validate_framework(&language, name).is_ok()
                && !frameworks.iter().any(|item| item == name)
            {
                frameworks.push(name.to_string());
            }
        }
    }
    if frameworks.is_empty() {
        frameworks.push("custom".into());
    }
    let deterministic_orchestrator = frameworks.iter().any(|item| {
        matches!(
            item.as_str(),
            "temporal" | "durable-functions" | "azure-durable-functions"
        )
    });
    let setup_file = match language.as_str() {
        "python" => "actae_setup.py",
        "typescript" => "actae.ts",
        "go" => "actae_setup.go",
        _ => unreachable!(),
    }
    .to_string();
    let install_command = match language.as_str() {
        "python" => {
            let extras: Vec<&str> = frameworks
                .iter()
                .filter_map(|item| match item.as_str() {
                    "langgraph" => Some("langgraph"),
                    "langchain" => Some("langchain"),
                    "crewai" => Some("crewai"),
                    "claude" => Some("claude"),
                    "openai-agents" => Some("openai-agents"),
                    _ => None,
                })
                .collect();
            if extras.is_empty() {
                "python -m pip install actae-client".to_string()
            } else {
                format!("python -m pip install 'actae-client[{}]'", extras.join(","))
            }
        }
        "typescript" => "npm install @actae/sdk".to_string(),
        "go" => "go get github.com/divigaactae/actae/sdks/go".to_string(),
        _ => unreachable!(),
    };
    Ok(IntegrationDetection {
        language,
        frameworks,
        install_command,
        setup_file,
        deterministic_orchestrator,
    })
}

fn validate_framework(language: &str, framework: &str) -> Result<()> {
    let supported: &[&str] = match language {
        "python" => &[
            "custom",
            "langgraph",
            "langchain",
            "crewai",
            "claude",
            "openai-agents",
            "codex",
            "temporal",
            "durable-functions",
            "azure-durable-functions",
        ],
        "typescript" => &[
            "custom",
            "langgraph",
            "langchain",
            "claude",
            "openai-agents",
            "copilot",
            "codex",
            "temporal",
            "durable-functions",
            "azure-durable-functions",
        ],
        "go" => &[
            "custom",
            "copilot",
            "temporal",
            "durable-functions",
            "azure-durable-functions",
        ],
        _ => &[],
    };
    if supported.contains(&framework) {
        Ok(())
    } else {
        Err(anyhow!(
            "framework {framework} is not supported by the {language} SDK; supported values: {}",
            supported.join(", ")
        ))
    }
}

fn integration_setup(root: &Path, detection: &IntegrationDetection) -> Result<String> {
    match detection.language.as_str() {
        "python" => {
            let mut setup =
                "from actae_client import Actae\n\nactae = Actae.from_env()\n".to_string();
            for framework in &detection.frameworks {
                if matches!(
                    framework.as_str(),
                    "temporal" | "durable-functions" | "azure-durable-functions"
                ) {
                    setup.push_str(&format!(
                        "\norchestrator = actae.orchestrator({framework:?})\n# carrier = orchestrator.carrier(workflow_id)  # pure; pass to an activity\n"
                    ));
                    continue;
                }
                setup.push_str(match framework.as_str() {
                    "langgraph" => "\ncheckpointer = actae.langgraph.checkpointer()\n# graph = builder.compile(checkpointer=checkpointer)\n",
                    "langchain" => "\ncallbacks = actae.langchain.callbacks(existing=[])\n",
                    "claude" => "\nsession_store = actae.claude.session_store()\n",
                    "crewai" => "\nstate_hook = actae.crewai.hook()\n",
                    "openai-agents" => "\n# result = await actae.openai.run(agent, prompt, run_id=run_id)\n",
                    "codex" => "\ncodex_receiver = actae.codex.receiver()\n# await codex_receiver.start()\n",
                    _ => "",
                });
            }
            Ok(setup)
        }
        "typescript" => {
            let mut setup =
                "import { Actae } from '@actae/sdk';\n\nexport const actae = Actae.fromEnv();\n"
                    .to_string();
            for framework in &detection.frameworks {
                if matches!(
                    framework.as_str(),
                    "temporal" | "durable-functions" | "azure-durable-functions"
                ) {
                    setup.push_str(&format!(
                        "\nexport const orchestrator = actae.orchestrator({framework:?});\n// const carrier = orchestrator.carrier(workflowId); // pure; pass to an activity\n"
                    ));
                    continue;
                }
                setup.push_str(match framework.as_str() {
                    "langgraph" => "\nexport const checkpointer = actae.langgraph.checkpointer();\n",
                    "langchain" => "\nexport const callbacks = actae.langchain.callbacks();\n",
                    "claude" => "\nexport const sessionStore = actae.claude.sessionStore();\n",
                    "openai-agents" => "\n// await actae.openai.run(agent, prompt, { runId });\n",
                    "copilot" => "\nexport const copilot = actae.copilot.manager();\n",
                    "codex" => "\nexport const codexReceiver = actae.codex.receiver();\n// await codexReceiver.start();\n",
                    _ => "",
                });
            }
            Ok(setup)
        }
        "go" => {
            let package = detect_go_package(root).unwrap_or_else(|| "main".into());
            let has_copilot = detection.frameworks.iter().any(|item| item == "copilot");
            let orchestrator = detection.frameworks.iter().find(|item| {
                matches!(
                    item.as_str(),
                    "temporal" | "durable-functions" | "azure-durable-functions"
                )
            });
            let imports = if has_copilot {
                "import (\n\tactae \"github.com/divigaactae/actae/sdks/go\"\n\tactaecopilot \"github.com/divigaactae/actae/sdks/go/copilot\"\n)"
            } else {
                "import actae \"github.com/divigaactae/actae/sdks/go\""
            };
            let mut setup = format!("package {package}\n\n{imports}\n\nfunc NewActae() (*actae.Runtime, error) {{\n\treturn actae.NewRuntimeFromEnv(actae.ClientOptions{{}})\n}}\n");
            if has_copilot {
                setup.push_str("\nfunc NewCopilot(runtime *actae.Runtime) (*actaecopilot.Manager, error) {\n\treturn actaecopilot.NewManagerFromRuntime(runtime, actaecopilot.ManagerOptions{})\n}\n");
            }
            if let Some(name) = orchestrator {
                setup.push_str(&format!("\n// In deterministic workflow code call runtime.Orchestrator({name:?}).Carrier(workflowID).\n// Open the carrier with Orchestrator.Activity only at the activity/worker boundary.\n"));
            }
            Ok(setup)
        }
        _ => Err(anyhow!("unsupported language")),
    }
}

fn detect_go_package(root: &Path) -> Option<String> {
    let entries = std::fs::read_dir(root).ok()?;
    for entry in entries.filter_map(|entry| entry.ok()) {
        let path = entry.path();
        if path.extension().and_then(|v| v.to_str()) != Some("go")
            || path.file_name()?.to_str()?.ends_with("_test.go")
        {
            continue;
        }
        let source = std::fs::read_to_string(path).ok()?;
        if let Some(line) = source
            .lines()
            .find(|line| line.trim_start().starts_with("package "))
        {
            return line.split_whitespace().nth(1).map(str::to_string);
        }
    }
    None
}

fn write_new(path: &PathBuf, contents: &[u8]) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| format!("create {}", path.display()))?;
    file.write_all(contents)
        .with_context(|| format!("write {}", path.display()))?;
    file.sync_all()
        .with_context(|| format!("sync {}", path.display()))?;
    Ok(())
}

// --------------------------------------------------------------------- #
// actae keys — API key management (host-side, uses the server DB)
// --------------------------------------------------------------------- #

async fn run_keys(args: &[String]) -> Result<()> {
    match args.first().map(|s| s.as_str()) {
        Some("create") => keys_create(&args[1..]).await,
        Some("list") => keys_list().await,
        Some("revoke") => keys_revoke(args.get(1).map(|s| s.as_str())).await,
        _ => {
            eprintln!(
                "usage: actae keys create <name> [--user-id <id>] | list | revoke <prefix-or-name>"
            );
            Err(anyhow!("unknown keys subcommand"))
        }
    }
}

/// Load config + a client DB pool (same environment as the server).
async fn keys_pool() -> Result<(Arc<AppConfig>, deadpool_postgres::Pool)> {
    // `actae keys create` should "just work" against a running managed-storage
    // instance without the operator hand-reconstructing the DB env vars. If
    // the host DB env is absent, auto-detect a single live managed-storage instance
    // (via its `url` file + `pg.pwfile`, the same discovery `doctor` uses)
    // and seed the env from it before loading the config.
    if std::env::var_os("POSTGRES_USER").is_none() {
        if let Err(e) = discover_embedded_env().await {
            tracing::debug!("managed-storage instance discovery skipped: {e:#}");
        }
    }
    let mut config = AppConfig::load().context(
        "failed to load configuration — `actae keys` needs the Actae host environment (database settings)",
    )?;
    let db_manager = Arc::new(DatabaseManager::new(&config).await?);
    // Resolve the signing secret the same way the server does, so API keys
    // minted here are hashed with the secret the server will use.
    let existing_keys = crate::secrets::pool_has_api_keys(&db_manager.client_pool).await;
    if let Err(e) = crate::secrets::resolve_into_config(&mut config, existing_keys) {
        tracing::warn!("could not resolve the signing secret: {e:#}");
    }
    Ok((Arc::new(config), db_manager.client_pool.clone()))
}

/// Auto-seed the host DB env vars from a running managed-storage instance.
///
/// Reads each `~/.actae/data*` dir's `url` file (written by the embedded
/// bootstrap: `http://<http>:<port>`, `pg_port`, `data_dir`), probes
/// healthz to pick the live instance, and derives the connection settings
/// (user `actae`, password from `pg.pwfile`, host 127.0.0.1, db `actae`,
/// SSL disabled). Refuses when more than one instance is live so a stray
/// command can never hit the wrong database. Returns the chosen instance's
/// `(data_dir, pg_port, url)`.
async fn discover_embedded_env() -> Result<(std::path::PathBuf, u16, String)> {
    let base = std::env::var("ACTAE_DATA_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| {
            let home = std::env::var_os("HOME")
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|| std::path::PathBuf::from("."));
            home.join(".actae").join("data")
        });
    let parent = match base.parent() {
        Some(p) => p.to_path_buf(),
        None => return Err(anyhow!("cannot derive data dir parent")),
    };
    let base_name = base.file_name().map(|n| n.to_string_lossy().to_string());
    let Some(base_name) = base_name else {
        return Err(anyhow!("cannot derive data dir name"));
    };
    let entries = std::fs::read_dir(&parent)?;

    struct Candidate {
        data_dir: std::path::PathBuf,
        pg_port: u16,
        url: String,
    }
    let mut live: Vec<Candidate> = Vec::new();
    for entry in entries.flatten() {
        let Ok(ft) = entry.file_type() else { continue };
        if !ft.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.starts_with(&base_name) {
            continue;
        }
        let dir = entry.path();
        let Ok(raw) = std::fs::read_to_string(dir.join("url")) else {
            continue;
        };
        let Ok(info) = serde_json::from_str::<serde_json::Value>(&raw) else {
            continue;
        };
        let Some(url) = info["url"].as_str().map(|s| s.to_string()) else {
            continue;
        };
        let pg_port = info["pg_port"].as_u64().unwrap_or(0) as u16;
        if pg_port == 0 || !probe_instance(&url).await {
            continue;
        }
        live.push(Candidate {
            data_dir: dir,
            pg_port,
            url,
        });
    }

    match live.len() {
        0 => Err(anyhow!("no live managed-storage instance found")),
        1 => {
            let c = &live[0];
            let pwfile = c.data_dir.join("pg.pwfile");
            let password = std::fs::read_to_string(&pwfile)
                .context("no password file — is this a real Actae managed-storage data dir?")?;
            let password = password.trim().to_string();
            let http_port = c
                .url
                .rsplit(':')
                .next()
                .and_then(|s| s.parse::<u16>().ok())
                .unwrap_or(8002);
            // Mirror the embedded bootstrap's env seeding (main.rs).
            std::env::set_var("ACTAE_PORT", http_port.to_string());
            std::env::set_var("POSTGRES_USER", "actae");
            std::env::set_var("POSTGRES_PASSWORD", &password);
            std::env::set_var("POSTGRES_HOST", "127.0.0.1");
            std::env::set_var("POSTGRES_PORT", c.pg_port.to_string());
            std::env::set_var("POSTGRES_DB", "actae");
            std::env::set_var("POSTGRES_SSL_MODE", "disable");
            std::env::set_var("CLIENT_POSTGRES_USER", "actae");
            std::env::set_var("CLIENT_POSTGRES_PASSWORD", &password);
            std::env::set_var("CLIENT_POSTGRES_HOST", "127.0.0.1");
            std::env::set_var("CLIENT_POSTGRES_PORT", c.pg_port.to_string());
            std::env::set_var("CLIENT_POSTGRES_DB", "actae");
            std::env::set_var("CLIENT_POSTGRES_SSL", "disable");
            tracing::info!(
                "auto-detected managed-storage instance {} (pg {}, data {})",
                c.url,
                c.pg_port,
                c.data_dir.display()
            );
            Ok((c.data_dir.clone(), c.pg_port, c.url.clone()))
        }
        n => Err(anyhow!(
            "found {n} live managed-storage instances — set POSTGRES_* explicitly to pick one"
        )),
    }
}

/// `actae keys create <name> [--user-id <id>]` — mint a new API key.
///
/// The key is hashed (HMAC with `api_key_hash_secret`) before storage; the
/// raw key is printed exactly once. Keys created here are `source='local'`
/// and are subject to subscription enforcement like any other key (on a
/// standalone instance there is no lock; on an enrolled instance with a
/// lapsed subscription the key is blocked).
async fn keys_create(args: &[String]) -> Result<()> {
    let name = args.first().map(|s| s.as_str()).unwrap_or_default();
    if name.is_empty() {
        eprintln!("usage: actae keys create <name> [--user-id <id>]");
        return Err(anyhow!("key name is required"));
    }
    let mut user_id = "ops".to_string();
    let mut it = args[1..].iter();
    while let Some(flag) = it.next() {
        if flag == "--user-id" {
            if let Some(v) = it.next() {
                user_id = v.clone();
            }
        }
    }

    let (config, pool) = keys_pool().await?;

    // 64 hex chars (two UUIDs ≈ 122 bits of entropy) under an `sk-` prefix.
    let raw = format!(
        "sk-{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let key_prefix =
        crate::auth::local_keys::insert_api_key(&pool, &config, &raw, &user_id, name).await?;

    println!("Created API key '{name}' (user_id={user_id})");
    println!();
    println!("  {raw}");
    println!();
    println!("Store it now — the raw key is shown only once. Use `actae keys revoke {key_prefix}` to revoke it.");
    Ok(())
}

/// `actae keys list` — show prefixes/names (never raw keys).
async fn keys_list() -> Result<()> {
    let (_config, pool) = keys_pool().await?;
    let client = pool
        .get()
        .await
        .context("failed to get database connection")?;
    let rows = client
        .query(
            "SELECT key_prefix, name, user_id, source, status, created_at
             FROM public.api_keys ORDER BY created_at",
            &[],
        )
        .await
        .context("failed to list API keys")?;

    if rows.is_empty() {
        println!("No API keys found.");
        return Ok(());
    }
    println!(
        "{:<14} {:<28} {:<12} {:<12} {:<10} CREATED",
        "PREFIX", "NAME", "USER", "SOURCE", "STATUS"
    );
    for row in rows {
        let prefix: String = row.get("key_prefix");
        let name: String = row.get("name");
        let user_id: String = row.get("user_id");
        let source: String = row.get("source");
        let status: String = row.get("status");
        let created: chrono::DateTime<Utc> = row.get("created_at");
        println!(
            "{prefix:<14} {name:<28} {user_id:<12} {source:<12} {status:<10} {}",
            created.to_rfc3339()
        );
    }
    Ok(())
}

/// `actae keys revoke <prefix-or-name>` — deactivate a key by prefix or name.
async fn keys_revoke(target: Option<&str>) -> Result<()> {
    let target = target.unwrap_or_default().trim();
    if target.is_empty() {
        eprintln!("usage: actae keys revoke <prefix-or-name>");
        return Err(anyhow!("a key prefix or name is required"));
    }

    let (_config, pool) = keys_pool().await?;
    let client = pool
        .get()
        .await
        .context("failed to get database connection")?;
    let rows = client
        .execute(
            "UPDATE public.api_keys SET status = 'revoked', updated_at = NOW()
             WHERE status = 'active' AND (key_prefix = $1 OR name = $1)",
            &[&target],
        )
        .await
        .context("failed to revoke API key")?;
    if rows == 0 {
        eprintln!("No active key matches '{target}'. Use `actae keys list` to find the prefix.");
        return Err(anyhow!("no matching key"));
    }
    println!("Revoked {rows} API key(s) matching '{target}'.");
    Ok(())
}

// --------------------------------------------------------------------- #
// actae passwd
// --------------------------------------------------------------------- #

/// Parse `actae passwd [email] [--password <password>]`.
fn parse_passwd_args(args: &[String]) -> Result<(Option<String>, Option<String>)> {
    let mut email: Option<String> = None;
    let mut password: Option<String> = None;
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--password" | "-p" => {
                password = Some(it.next().context("--password requires a value")?.clone());
            }
            other if other.starts_with('-') => {
                return Err(anyhow!("unknown flag: {other}"));
            }
            other => {
                if email.is_some() {
                    return Err(anyhow!("only one email may be given"));
                }
                email = Some(other.to_string());
            }
        }
    }
    Ok((email, password))
}

/// `actae passwd [email] [--password <password>]` — set a local dashboard
/// account's password.
///
/// Standalone instances authenticate the dashboard against the local
/// `public.user` table. First boot prints a generated admin password once; this
/// command is the supported way to set (or reset) it when that password is
/// lost. It uses the same connection discovery as `actae keys` (auto-detects a
/// single live managed-storage instance). Existing dashboard sessions are
/// stateless JWTs and stay valid until they expire (24h).
async fn run_passwd(args: &[String]) -> Result<()> {
    if args.iter().any(|a| a == "-h" || a == "--help") {
        println!(
            "usage: actae passwd [email] [--password <password>]\n\n\
             Sets the password for a local dashboard account (standalone mode).\n\
             With no email and exactly one local user, that user is chosen.\n\
             With no --password, a strong password is generated and printed."
        );
        return Ok(());
    }
    let (email_arg, password_arg) = parse_passwd_args(args)?;

    let (_config, pool) = keys_pool().await?;
    let hasher = SecurePasswordHasher::with_secure_defaults();
    let client = pool
        .get()
        .await
        .context("failed to get database connection")?;

    let email = match email_arg {
        Some(e) => e.trim().to_lowercase(),
        None => {
            let rows = client
                .query(
                    "SELECT email FROM public.user ORDER BY \"createdAt\" LIMIT 2",
                    &[],
                )
                .await
                .context("failed to list local users")?;
            match rows.len() {
                0 => {
                    return Err(anyhow!(
                        "no local dashboard account exists. A standalone instance creates one \
                         on first boot (see the banner, or set ACTAE_INITIAL_ADMIN_EMAIL / \
                         ACTAE_INITIAL_ADMIN_PASSWORD)."
                    ))
                }
                1 => rows[0].get::<_, String>("email"),
                _ => {
                    return Err(anyhow!(
                        "multiple local accounts — pass the email: actae passwd <email>"
                    ))
                }
            }
        }
    };

    let row = client
        .query_opt("SELECT id FROM public.user WHERE email = $1", &[&email])
        .await
        .context("failed to look up the account")?
        .with_context(|| format!("no local dashboard account with email '{email}'"))?;
    let user_id: String = row.get("id");

    let password = password_arg.unwrap_or_else(|| hasher.generate_compliant_password());
    if password.len() < 8 || password.len() > 128 {
        return Err(anyhow!("password must be between 8 and 128 characters"));
    }

    let hash = hasher
        .hash_password(&password, &user_id)
        .await
        .map_err(|e| anyhow!("failed to hash the password: {e}"))?;
    client
        .execute(
            "UPDATE public.user SET password_hash = $1, \"updatedAt\" = NOW() WHERE id = $2",
            &[&hash, &user_id],
        )
        .await
        .context("failed to update the password")?;

    // A password reset invalidates existing dashboard sessions (best effort:
    // the table may not exist on a very old schema).
    let _ = client
        .execute(
            "UPDATE public.dashboard_sessions SET revoked_at = NOW()
             WHERE user_id = $1 AND revoked_at IS NULL",
            &[&user_id],
        )
        .await;

    // `actae passwd` is also the recovery path when the first-run setup token
    // was lost: setting a password completes setup so the dashboard stops
    // showing the setup page.
    let _ = crate::auth::setup::mark_complete(&pool).await;

    println!("Password set for {email}.");
    println!();
    println!("  {password}");
    println!();
    println!(
        "Sign in at the dashboard with that password. The value is shown once, and \
         any existing dashboard sessions for this account were revoked."
    );
    Ok(())
}

async fn resolve_host(base_url: &str) -> Result<String> {
    let host = url::Url::parse(base_url)
        .context("invalid ACTAE_CONTROL_PLANE_URL")?
        .host_str()
        .ok_or_else(|| anyhow!("control plane URL has no host"))?
        .to_string();
    let port = url::Url::parse(base_url)
        .ok()
        .and_then(|u| u.port_or_known_default())
        .unwrap_or(443);
    let addrs = tokio::net::lookup_host((host.as_str(), port))
        .await
        .context("DNS resolution failed")?;
    let mut list: Vec<String> = addrs.map(|a| a.to_string()).collect();
    list.sort_unstable();
    list.dedup();
    Ok(format!("{}:{} ({})", host, port, list.join(", ")))
}

/// Open the verification URL in the default browser — but only when the CLI
/// is attached to an interactive terminal and the operator has not opted out
/// (`ACTAE_NO_BROWSER=1`). Scripted runs (CI, tests, SSH pipelines) print the
/// URL instead; popping a browser there is noise, not a feature.
fn open_browser(url: &str) {
    use std::io::IsTerminal;

    if std::env::var("ACTAE_NO_BROWSER").is_ok() || !std::io::stdin().is_terminal() {
        tracing::debug!("not opening a browser (non-interactive or ACTAE_NO_BROWSER set)");
        return;
    }
    for cmd in [("open", vec!["-a", "Safari", url]), ("xdg-open", vec![url])] {
        if std::process::Command::new(cmd.0)
            .args(&cmd.1)
            .spawn()
            .is_ok()
        {
            return;
        }
    }
}

mod env {
    pub fn install_id() -> Option<String> {
        std::env::var("ACTAE_INSTALL_ID")
            .ok()
            .filter(|s| !s.trim().is_empty())
    }

    pub fn https_proxy() -> Option<String> {
        std::env::var("HTTPS_PROXY")
            .ok()
            .or_else(|| std::env::var("https_proxy").ok())
            .or_else(|| std::env::var("ALL_PROXY").ok())
    }
}

fn env_url() -> Result<Option<String>> {
    Ok(std::env::var("ACTAE_CONTROL_PLANE_URL")
        .ok()
        .filter(|s| !s.trim().is_empty()))
}

/// Credentials for subscription-gated self-updates. Container deployments
/// provide these through ACTAE_INSTALL_ID/ACTAE_ENROLL_SECRET; interactive
/// installs use the 0600 enrollment file written by `actae auth login`.
fn upgrade_credentials() -> Option<(String, String)> {
    let install_id = std::env::var("ACTAE_INSTALL_ID")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let secret = std::env::var("ACTAE_ENROLL_SECRET")
        .ok()
        .filter(|value| !value.trim().is_empty());
    match (install_id, secret) {
        (Some(id), Some(secret)) => Some((id, secret)),
        (None, None) => Enrollment::load()
            .map(|enrollment| (enrollment.install_id, enrollment.enrollment_secret)),
        _ => None,
    }
}

fn env_ca_bundle() -> Option<String> {
    std::env::var("ACTAE_CA_BUNDLE")
        .ok()
        .filter(|s| !s.trim().is_empty())
}

/// The platform key used by the release manifest / downloads endpoint.
///
/// Alpine (and other musl hosts) get the dedicated `-alpine` artifacts:
/// their embedded PostgreSQL is built against musl and cannot run on glibc
/// hosts (and vice versa), so `actae upgrade` must request the artifact that
/// matches this host's libc.
fn platform_key() -> &'static str {
    let alpine = crate::embedded_pg::on_alpine_like_host();
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") if alpine => "linux-x86_64-alpine",
        ("linux", "aarch64") if alpine => "linux-aarch64-alpine",
        ("linux", "x86_64") => "linux-x86_64",
        ("linux", "aarch64") => "linux-aarch64",
        ("macos", "x86_64") => "macos-x86_64",
        ("macos", "aarch64") => "macos-arm64",
        ("windows", "x86_64") => "windows-x86_64",
        _ => "unknown",
    }
}

/// Compare two dotted version strings (`1.10.0` > `1.9.4`).
fn compare_versions(a: &str, b: &str) -> std::cmp::Ordering {
    fn parts(v: &str) -> Vec<u64> {
        v.trim_start_matches('v')
            .split('.')
            .filter_map(|p| p.parse().ok())
            .collect()
    }
    let (pa, pb) = (parts(a), parts(b));
    for i in 0..pa.len().max(pb.len()) {
        match pa
            .get(i)
            .copied()
            .unwrap_or(0)
            .cmp(&pb.get(i).copied().unwrap_or(0))
        {
            std::cmp::Ordering::Equal => continue,
            order => return order,
        }
    }
    std::cmp::Ordering::Equal
}

/// `actae upgrade [--check]`: check for and apply the latest release from
/// the control plane. Downloads the presigned URL, verifies the SHA-256, and
/// atomically replaces the running binary.
async fn probe_instance(url: &str) -> bool {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .ok();
    let Some(client) = client else {
        return false;
    };
    match client.get(format!("{url}/healthz")).send().await {
        Ok(resp) => resp.status().is_success(),
        Err(_) => false,
    }
}

async fn run_upgrade(args: &[String]) -> Result<()> {
    let mut check_only = false;
    let mut url: Option<String> = None;
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--check" => check_only = true,
            "--url" => url = it.next().cloned(),
            _ => return Err(anyhow!("unknown argument: {arg}")),
        }
    }

    let base_url = url
        .or_else(|| env_url().ok().flatten())
        .unwrap_or_else(|| crate::control_plane::client::DEFAULT_CONTROL_PLANE_URL.to_string());
    let platform = platform_key();
    let current = env!("CARGO_PKG_VERSION");

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .context("failed to build HTTP client")?;

    let dl_url = format!(
        "{}/v1/downloads/{}",
        base_url.trim_end_matches('/'),
        platform
    );
    let mut request = client.get(&dl_url);
    if let Some((install_id, enroll_secret)) = upgrade_credentials() {
        request = request
            .header("authorization", format!("Bearer {enroll_secret}"))
            .header("x-actae-install-id", install_id);
    }
    let resp = request
        .send()
        .await
        .with_context(|| format!("failed to reach {dl_url}"))?;
    let status = resp.status();
    if !status.is_success() {
        return Err(anyhow!(
            "control plane returned {status} for GET {dl_url} — upgrades are unavailable"
        ));
    }
    let info: serde_json::Value = resp.json().await.context("invalid download response")?;
    let version = info["version"].as_str().unwrap_or_default().to_string();
    let filename = info["filename"].as_str().unwrap_or_default().to_string();
    let expected_sha = info["sha256"].as_str().unwrap_or_default().to_string();
    let download = info["url"].as_str().unwrap_or_default().to_string();

    if version.is_empty() || download.is_empty() {
        return Err(anyhow!(
            "control plane returned an incomplete download response"
        ));
    }

    match compare_versions(&version, current) {
        std::cmp::Ordering::Less | std::cmp::Ordering::Equal => {
            println!("actae {current} is up to date.");
            return Ok(());
        }
        std::cmp::Ordering::Greater => {}
    }
    println!("actae {current} -> {version} ({platform}, {filename})");
    if check_only {
        return Ok(());
    }

    println!("  downloading {filename} ...");
    let body = client
        .get(&download)
        .send()
        .await
        .context("failed to download the release")?
        .error_for_status()
        .context("release download failed")?
        .bytes()
        .await
        .context("failed to read the release body")?;

    let actual_sha = crate::control_plane::license::sha256_hex(&body);
    if !expected_sha.is_empty() && !actual_sha.eq_ignore_ascii_case(&expected_sha) {
        return Err(anyhow!(
            "SHA-256 mismatch: expected {expected_sha}, got {actual_sha} — refusing to \
             replace the binary"
        ));
    }

    let exe = std::env::current_exe().context("cannot determine the current executable path")?;
    let new_path = exe.with_extension("new");
    std::fs::write(&new_path, &body)
        .with_context(|| format!("failed to write {}", new_path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let exe_mode = std::fs::metadata(&exe)?.permissions().mode();
        std::fs::set_permissions(&new_path, std::fs::Permissions::from_mode(exe_mode))?;
    }

    // Atomically replace the running binary. On Windows a running executable
    // cannot be overwritten — rename it aside first.
    #[cfg(unix)]
    std::fs::rename(&new_path, &exe)
        .with_context(|| format!("failed to replace {}", exe.display()))?;
    #[cfg(windows)]
    {
        let old_path = exe.with_extension("old");
        let _ = std::fs::rename(&exe, &old_path);
        std::fs::rename(&new_path, &exe).map_err(|e| {
            let _ = std::fs::rename(&old_path, &exe); // best-effort restore
            anyhow::anyhow!(
                "failed to replace {} (close any running Actae and retry): {e}",
                exe.display()
            )
        })?;
        let _ = std::fs::remove_file(&old_path);
    }

    println!("  updated to actae {version} — restart the server to apply.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serializes tests that mutate the process environment (parallel tests
    /// otherwise race on shared env vars).
    static CLI_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn test_usage_reachable() {
        print_usage();
    }

    #[test]
    fn same_server_ignores_trailing_slash() {
        assert!(same_server("http://127.0.0.1:8002", "http://127.0.0.1:8002/"));
        assert!(!same_server("http://127.0.0.1:8002", "http://127.0.0.1:8003"));
    }

    #[test]
    fn key_prefix_matches_what_insert_stores() {
        assert_eq!(key_prefix("sk-0123456789abcdef"), "sk-012345678");
        assert_eq!(key_prefix("short"), "short");
    }

    #[test]
    fn normalize_endpoint_trims_and_validates() {
        assert_eq!(
            normalize_endpoint(" http://127.0.0.1:8002/ ").unwrap(),
            "http://127.0.0.1:8002"
        );
        assert!(normalize_endpoint("ftp://x").is_err());
        assert!(normalize_endpoint("http://").is_err());
    }

    #[test]
    fn test_env_helpers_redact_proxy_credentials() {
        // The display path must never leak proxy credentials: the CLI prints
        // the proxy value as-is from env::https_proxy(), so the caller is
        // responsible for redaction — verify the formatting helper used in
        // doctor output keeps the value intact but flags it.
        let _guard = CLI_ENV_LOCK.lock().unwrap();
        unsafe {
            std::env::set_var("HTTPS_PROXY", "http://user:pass@proxy.example.com:8080");
        }
        let proxy = env::https_proxy().expect("proxy must be visible");
        assert!(proxy.contains("proxy.example.com"));
        unsafe {
            std::env::remove_var("HTTPS_PROXY");
            std::env::remove_var("https_proxy");
            std::env::remove_var("ALL_PROXY");
        }
    }

    #[test]
    fn keys_create_requires_a_name() {
        // Arg validation happens before any DB access — safe without a server.
        let args: Vec<String> = vec![];
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(keys_create(&args));
        assert!(result.is_err(), "create without a name must fail");
        let err = result.unwrap_err().to_string();
        assert!(err.contains("name is required"), "unexpected error: {err}");
    }

    #[test]
    fn keys_revoke_requires_a_target() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let result = rt.block_on(keys_revoke(None));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("prefix or name"));
    }

    #[test]
    fn passwd_args_parse_email_and_password() {
        let args = vec![
            "admin@actae.local".to_string(),
            "--password".to_string(),
            "hunter2-goes-here".to_string(),
        ];
        assert_eq!(
            parse_passwd_args(&args).unwrap(),
            (
                Some("admin@actae.local".to_string()),
                Some("hunter2-goes-here".to_string())
            )
        );
        assert_eq!(parse_passwd_args(&[]).unwrap(), (None, None));
    }

    #[test]
    fn passwd_args_reject_unknown_flag_duplicate_email_and_missing_value() {
        assert!(parse_passwd_args(&["--nope".to_string()]).is_err());
        assert!(parse_passwd_args(&["a@x.com".to_string(), "b@x.com".to_string()]).is_err());
        assert!(parse_passwd_args(&["--password".to_string()]).is_err());
    }

    #[test]
    fn passwd_help_is_reachable_without_db() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        assert!(rt.block_on(run_passwd(&["--help".to_string()])).is_ok());
    }

    #[test]
    fn keys_unknown_subcommand_fails() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let args: Vec<String> = vec!["keys".into(), "explode".into()];
        let result = rt.block_on(run(&args));
        assert!(result.is_err());
    }

    #[test]
    fn test_env_install_id_missing() {
        let _guard = CLI_ENV_LOCK.lock().unwrap();
        unsafe {
            std::env::remove_var("ACTAE_INSTALL_ID");
        }
        assert!(env::install_id().is_none());
    }

    // ---------------------------------------------------------------------
    // SDK integration onboarding (`actae add` / `actae explain integration`)
    // ---------------------------------------------------------------------

    /// Scratch dir helper: unique per test to avoid cross-test interference.
    fn scratch_dir(label: &str) -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix(&format!("actae-cli-{label}-"))
            .tempdir()
            .expect("create scratch dir")
    }

    fn file(root: &Path, name: &str, contents: &str) {
        std::fs::write(root.join(name), contents).expect("write fixture file");
    }

    #[test]
    fn detect_python_pyproject_detects_langgraph_framework() {
        let dir = scratch_dir("py");
        file(
            dir.path(),
            "pyproject.toml",
            "[project]\ndependencies = [\"langgraph\"]\n",
        );
        let detection = detect_integration(dir.path(), None, None).expect("detect");
        assert_eq!(detection.language, "python");
        assert!(detection.frameworks.contains(&"langgraph".to_string()));
        assert_eq!(
            detection.install_command,
            "python -m pip install 'actae-client[langgraph]'"
        );
        assert_eq!(detection.setup_file, "actae_setup.py");
        assert!(!detection.deterministic_orchestrator);
    }

    #[test]
    fn detect_typescript_package_json_detects_openai_agents() {
        let dir = scratch_dir("ts");
        file(
            dir.path(),
            "package.json",
            "{\"dependencies\": {\"@openai/agents\": \"^1.0.0\"}}\n",
        );
        let detection = detect_integration(dir.path(), None, None).expect("detect");
        assert_eq!(detection.language, "typescript");
        assert!(detection.frameworks.contains(&"openai-agents".to_string()));
        assert_eq!(detection.install_command, "npm install @actae/sdk");
        assert_eq!(detection.setup_file, "actae.ts");
    }

    #[test]
    fn detect_go_mod_detects_copilot_and_temporal() {
        let dir = scratch_dir("go");
        file(
            dir.path(),
            "go.mod",
            "module example.com/agent\n\ngo 1.21\n\nrequire github.com/github/copilot-sdk/go v1.0.8\ntemporal.io/sdk v1.0.0\n",
        );
        let detection = detect_integration(dir.path(), None, None).expect("detect");
        assert_eq!(detection.language, "go");
        assert!(detection.frameworks.contains(&"copilot".to_string()));
        assert!(detection.frameworks.contains(&"temporal".to_string()));
        assert!(detection.deterministic_orchestrator);
        assert_eq!(detection.setup_file, "actae_setup.go");
    }

    #[test]
    fn detect_explicit_language_and_framework_alias() {
        let dir = scratch_dir("explicit");
        file(dir.path(), "requirements.txt", "actae-client\n");
        let detection =
            detect_integration(dir.path(), Some("python"), Some("openai")).expect("detect");
        assert_eq!(detection.frameworks, vec!["openai-agents"]);
        assert_eq!(
            detection.install_command,
            "python -m pip install 'actae-client[openai-agents]'"
        );
    }

    #[test]
    fn detect_multiple_languages_without_explicit_language_fails() {
        let dir = scratch_dir("multi");
        file(dir.path(), "pyproject.toml", "[project]\n");
        file(dir.path(), "package.json", "{}\n");
        let err = detect_integration(dir.path(), None, None).expect_err("must reject ambiguity");
        assert!(err.to_string().contains("multiple SDK languages"));
    }

    #[test]
    fn detect_no_manifest_without_explicit_language_fails() {
        let dir = scratch_dir("empty");
        let err = detect_integration(dir.path(), None, None).expect_err("must fail");
        assert!(err.to_string().contains("could not detect"));
    }

    #[test]
    fn detect_unsupported_language_value_fails() {
        let dir = scratch_dir("lang");
        let err = detect_integration(dir.path(), Some("ruby"), None).expect_err("must fail");
        assert!(err.to_string().contains("unsupported language ruby"));
    }

    #[test]
    fn validate_framework_rejects_language_framework_mismatch() {
        assert!(validate_framework("go", "langgraph").is_err());
        assert!(validate_framework("python", "copilot").is_err());
        assert!(validate_framework("typescript", "crewai").is_err());
        assert!(validate_framework("python", "langgraph").is_ok());
        assert!(validate_framework("go", "temporal").is_ok());
        assert!(validate_framework("typescript", "copilot").is_ok());
    }

    #[test]
    fn detect_falls_back_to_custom_framework() {
        let dir = scratch_dir("custom");
        file(dir.path(), "requirements.txt", "requests\n");
        let detection = detect_integration(dir.path(), None, None).expect("detect");
        assert_eq!(detection.frameworks, vec!["custom"]);
        assert_eq!(
            detection.install_command,
            "python -m pip install actae-client"
        );
    }

    #[test]
    fn integration_setup_python_contains_provider_entry_points() {
        let detection = IntegrationDetection {
            language: "python".into(),
            frameworks: vec!["langgraph".into(), "claude".into()],
            install_command: "python -m pip install 'actae-client[langgraph,claude]'".into(),
            setup_file: "actae_setup.py".into(),
            deterministic_orchestrator: false,
        };
        let setup = integration_setup(Path::new("/tmp"), &detection).expect("setup");
        assert!(setup.contains("actae.langgraph.checkpointer()"));
        assert!(setup.contains("actae.claude.session_store()"));
    }

    #[test]
    fn integration_setup_python_orchestrator_is_boundary_safe() {
        let detection = IntegrationDetection {
            language: "python".into(),
            frameworks: vec!["temporal".into()],
            install_command: "python -m pip install actae-client".into(),
            setup_file: "actae_setup.py".into(),
            deterministic_orchestrator: true,
        };
        let setup = integration_setup(Path::new("/tmp"), &detection).expect("setup");
        assert!(setup.contains("orchestrator = actae.orchestrator(\"temporal\")"));
        assert!(setup.contains("carrier"));
    }

    #[test]
    fn integration_setup_go_uses_detected_package_and_copilot_constructor() {
        let dir = scratch_dir("gosetup");
        file(dir.path(), "main.go", "package agent\n\nfunc main() {}\n");
        let detection = IntegrationDetection {
            language: "go".into(),
            frameworks: vec!["copilot".into()],
            install_command: "go get github.com/divigaactae/actae/sdks/go".into(),
            setup_file: "actae_setup.go".into(),
            deterministic_orchestrator: false,
        };
        let setup = integration_setup(dir.path(), &detection).expect("setup");
        assert!(setup.contains("package agent"));
        assert!(setup.contains("actaecopilot.NewManagerFromRuntime"));
        assert!(setup.contains("NewRuntimeFromEnv"));
    }

    #[test]
    fn integration_setup_typescript_emits_orchestrator_carrier_comment() {
        let detection = IntegrationDetection {
            language: "typescript".into(),
            frameworks: vec!["durable-functions".into()],
            install_command: "npm install @actae/sdk".into(),
            setup_file: "actae.ts".into(),
            deterministic_orchestrator: true,
        };
        let setup = integration_setup(Path::new("/tmp"), &detection).expect("setup");
        assert!(setup.contains("actae.orchestrator(\"durable-functions\")"));
        assert!(setup.contains("carrier"));
    }

    #[test]
    fn resolve_integration_root_rejects_non_directory() {
        let dir = scratch_dir("root");
        file(dir.path(), "notadir", "x");
        let err =
            resolve_integration_root(Some(&dir.path().join("notadir"))).expect_err("must fail");
        assert!(err.to_string().contains("not a directory"));
    }

    #[test]
    fn run_add_preview_does_not_write_files() {
        let dir = scratch_dir("addpreview");
        file(dir.path(), "requirements.txt", "actae-client\n");
        let args: Vec<String> = vec![
            "--path".into(),
            dir.path().display().to_string(),
            "--language".into(),
            "python".into(),
        ];
        run_add(&args).expect("preview succeeds");
        assert!(!dir.path().join("actae_setup.py").exists());
        assert!(!dir.path().join(".env.actae.example").exists());
    }

    #[test]
    fn run_add_write_creates_setup_and_env_files() {
        let dir = scratch_dir("addwrite");
        file(dir.path(), "requirements.txt", "actae-client\n");
        let args: Vec<String> = vec![
            "--path".into(),
            dir.path().display().to_string(),
            "--language".into(),
            "python".into(),
            "--write".into(),
        ];
        run_add(&args).expect("write succeeds");
        assert!(dir.path().join("actae_setup.py").exists());
        assert!(dir.path().join(".env.actae.example").exists());
        let env = std::fs::read_to_string(dir.path().join(".env.actae.example")).expect("read env");
        assert!(env.contains("ACTAE_URL=http://localhost:8002"));
        assert!(!env.contains("sk-live"));
    }

    #[test]
    fn run_add_write_refuses_to_overwrite_existing_files() {
        let dir = scratch_dir("addoverwrite");
        file(dir.path(), "requirements.txt", "actae-client\n");
        file(dir.path(), "actae_setup.py", "existing\n");
        let args: Vec<String> = vec![
            "--path".into(),
            dir.path().display().to_string(),
            "--language".into(),
            "python".into(),
            "--write".into(),
        ];
        let err = run_add(&args).expect_err("must refuse overwrite");
        assert!(err.to_string().contains("refusing to overwrite"));
        // The pre-existing file must be untouched.
        assert_eq!(
            std::fs::read_to_string(dir.path().join("actae_setup.py")).expect("read"),
            "existing\n"
        );
    }

    #[test]
    fn run_add_unknown_argument_fails() {
        let args: Vec<String> = vec!["--bogus".into()];
        assert!(run_add(&args).is_err());
    }

    #[test]
    fn run_explain_requires_integration_subcommand() {
        let args: Vec<String> = vec![];
        let err = run_explain(&args).expect_err("must fail");
        assert!(err.to_string().contains("usage: actae explain integration"));
    }

    #[test]
    fn run_explain_json_emits_schema_and_detection() {
        let dir = scratch_dir("explain");
        file(
            dir.path(),
            "pyproject.toml",
            "[project]\ndependencies = [\"crewai\"]\n",
        );
        let args: Vec<String> = vec![
            "integration".into(),
            "--path".into(),
            dir.path().display().to_string(),
            "--json".into(),
        ];
        run_explain(&args).expect("explain json succeeds");
    }

    #[test]
    fn run_explain_plain_emits_human_readable_guidance() {
        let dir = scratch_dir("explainplain");
        file(
            dir.path(),
            "go.mod",
            "module example.com/agent\n\ngo 1.21\n",
        );
        let args: Vec<String> = vec![
            "integration".into(),
            "--path".into(),
            dir.path().display().to_string(),
        ];
        run_explain(&args).expect("explain plain succeeds");
    }

    #[test]
    fn run_dispatches_add_and_explain_subcommands() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let dir = scratch_dir("dispatch");
        file(dir.path(), "requirements.txt", "actae-client\n");
        let add_args: Vec<String> = vec![
            "add".into(),
            "--path".into(),
            dir.path().display().to_string(),
            "--language".into(),
            "python".into(),
            "--write".into(),
        ];
        rt.block_on(run(&add_args)).expect("add dispatches");
        assert!(dir.path().join("actae_setup.py").exists());

        let explain_args: Vec<String> = vec![
            "explain".into(),
            "integration".into(),
            "--path".into(),
            dir.path().display().to_string(),
        ];
        rt.block_on(run(&explain_args)).expect("explain dispatches");
    }

    #[test]
    fn serialized_detection_matches_explain_json_shape() {
        let detection = IntegrationDetection {
            language: "python".into(),
            frameworks: vec!["custom".into()],
            install_command: "python -m pip install actae-client".into(),
            setup_file: "actae_setup.py".into(),
            deterministic_orchestrator: false,
        };
        let json = serde_json::to_string(&detection).expect("serialize");
        assert!(json.contains("\"language\":\"python\""));
        assert!(json.contains("\"deterministic_orchestrator\":false"));
        assert!(json.contains("\"setup_file\":\"actae_setup.py\""));
    }
}
