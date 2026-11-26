// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Top-level server CLI arguments (the flags that precede/select server
//! startup — everything else is a subcommand handled in `crate::cli`).
//!
//! ```text
//! actae [--config <file.toml>]
//!       [--database-url <dsn>] [--client-database-url <dsn>]
//!       [--host <addr>] [--port <n>] [--health-port <n>]
//! ```
//!
//! A bare `actae` runs the self-contained managed PostgreSQL. `--database-url`
//! (or `DATABASE_URL` / `POSTGRES_*`) selects an external database.

use anyhow::{anyhow, Result};
use std::path::PathBuf;

/// Parsed top-level server arguments.
#[derive(Debug, Default, Clone)]
pub struct ServerArgs {
    pub config_file: Option<PathBuf>,
    pub database_url: Option<String>,
    pub client_database_url: Option<String>,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub health_port: Option<u16>,
    pub help: bool,
    pub version: bool,
}

/// Parse the process arguments (excluding argv[0]).
///
/// Unknown flags fail loudly instead of being silently ignored — a typo like
/// `--databse-url` must never silently fall back to managed storage.
pub fn parse_server_args(args: &[String]) -> Result<ServerArgs> {
    let mut out = ServerArgs::default();
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        let (flag, inline_value) = split_flag(arg);
        match flag {
            "--config" | "-c" => {
                out.config_file = Some(PathBuf::from(take_value(flag, inline_value, &mut it)?));
            }
            "--database-url" | "--db-url" => {
                out.database_url = Some(validate_dsn(
                    flag,
                    &take_value(flag, inline_value, &mut it)?,
                )?);
            }
            "--client-database-url" => {
                out.client_database_url = Some(validate_dsn(
                    flag,
                    &take_value(flag, inline_value, &mut it)?,
                )?);
            }
            "--host" => out.host = Some(take_value(flag, inline_value, &mut it)?),
            "--port" => {
                out.port = Some(parse_port(flag, &take_value(flag, inline_value, &mut it)?)?)
            }
            "--health-port" => {
                out.health_port = Some(parse_port(flag, &take_value(flag, inline_value, &mut it)?)?)
            }
            "--help" | "-h" => out.help = true,
            "--version" | "-V" => out.version = true,
            "--embedded" => {
                return Err(anyhow!(
                    "`--embedded` was removed: the managed PostgreSQL is now the default when no \
                     external database is configured. Run `actae` with no flags, or point at an \
                     existing database with `--database-url <dsn>` / `DATABASE_URL`."
                ));
            }
            other if other.starts_with('-') => {
                return Err(anyhow!(
                    "unknown argument `{other}` — run `actae --help` for usage"
                ));
            }
            other => {
                return Err(anyhow!(
                    "unexpected argument `{other}` — run `actae --help` for usage"
                ));
            }
        }
    }
    Ok(out)
}

/// Split `--flag=value` into `("--flag", Some("value"))`; otherwise the value
/// is `None` (and read from the next argument).
fn split_flag(arg: &str) -> (&str, Option<&str>) {
    match arg.split_once('=') {
        Some((flag, value)) => (flag, Some(value)),
        None => (arg, None),
    }
}

fn take_value(
    flag: &str,
    inline: Option<&str>,
    it: &mut std::slice::Iter<'_, String>,
) -> Result<String> {
    let value = match inline {
        Some(v) => v.to_string(),
        None => it
            .next()
            .cloned()
            .ok_or_else(|| anyhow!("{flag} requires a value"))?,
    };
    if value.trim().is_empty() {
        return Err(anyhow!("{flag} requires a non-empty value"));
    }
    Ok(value)
}

fn parse_port(flag: &str, value: &str) -> Result<u16> {
    value
        .parse::<u16>()
        .map_err(|_| anyhow!("{flag} must be a valid port number (1-65535), got `{value}`"))
}

/// Reject a value that is not a PostgreSQL connection URL, so a typo fails at
/// argument parsing with a clear message instead of deep inside the pool.
fn validate_dsn(flag: &str, value: &str) -> Result<String> {
    let lower = value.to_ascii_lowercase();
    if lower.starts_with("postgres://") || lower.starts_with("postgresql://") {
        Ok(value.to_string())
    } else {
        Err(anyhow!(
            "{flag} must be a postgres:// or postgresql:// connection URL"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parses_space_and_equals_forms() {
        let a = parse_server_args(&args(&[
            "--config",
            "/etc/actae.toml",
            "--database-url=postgresql://u:p@h/db",
            "--port",
            "9000",
        ]))
        .unwrap();
        assert_eq!(a.config_file.unwrap(), PathBuf::from("/etc/actae.toml"));
        assert_eq!(a.database_url.as_deref(), Some("postgresql://u:p@h/db"));
        assert_eq!(a.port, Some(9000));
    }

    #[test]
    fn empty_bare_is_valid() {
        let a = parse_server_args(&[]).unwrap();
        assert!(a.config_file.is_none() && a.database_url.is_none());
    }

    #[test]
    fn rejects_embedded_with_migration_message() {
        let err = parse_server_args(&args(&["--embedded"])).unwrap_err();
        assert!(err.to_string().contains("removed"));
    }

    #[test]
    fn rejects_unknown_and_missing_values() {
        assert!(parse_server_args(&args(&["--nope"])).is_err());
        assert!(parse_server_args(&args(&["--port"])).is_err());
        assert!(parse_server_args(&args(&["--port", "abc"])).is_err());
    }

    #[test]
    fn rejects_non_postgres_dsn() {
        assert!(parse_server_args(&args(&["--database-url", "mysql://u@h/db"])).is_err());
        assert!(parse_server_args(&args(&["--database-url", "postgres://u@h/db"])).is_ok());
        assert!(parse_server_args(&args(&["--database-url=postgresql://u@h/db"])).is_ok());
    }
}
