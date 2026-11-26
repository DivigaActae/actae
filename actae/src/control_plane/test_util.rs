// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Shared test utilities for the control plane module.

/// A dead connection pool: connections are never served, which is fine for
/// tests that never touch the database (lock evaluation, parsing, etc.).
pub fn dead_pool() -> deadpool_postgres::Pool {
    let mut cfg = deadpool_postgres::Config::new();
    cfg.host = Some("127.0.0.1".to_string());
    cfg.port = Some(1);
    cfg.user = Some("actae".to_string());
    cfg.dbname = Some("actae".to_string());
    cfg.create_pool(
        Some(deadpool_postgres::Runtime::Tokio1),
        tokio_postgres::NoTls,
    )
    .expect("pool creation must succeed")
}
