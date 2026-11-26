// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

use thiserror::Error;

#[derive(Debug, Error)]
pub enum DatabaseError {
    #[error("Connection failed: {0}")]
    ConnectionError(#[from] deadpool_postgres::PoolError),

    #[error("PostgreSQL error: {0}")]
    PostgresError(#[from] tokio_postgres::Error),

    #[error("Entity not found with id {0}")]
    NotFound(String),

    #[error("Failed to build connection pool: {0}")]
    PoolBuildError(#[from] deadpool_postgres::BuildError),

    #[error("JSON serialization/deserialization error: {0}")]
    JsonError(String),

    #[error("Query error: {0}")]
    QueryError(String),

    #[error("An unknown database error occurred")]
    Unknown,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_error_display() {
        let err = DatabaseError::ConnectionError(deadpool_postgres::PoolError::Closed);
        assert!(err.to_string().contains("Connection failed"));
    }

    #[test]
    fn test_not_found_display() {
        let err = DatabaseError::NotFound("user_123".into());
        assert_eq!(err.to_string(), "Entity not found with id user_123");
    }

    #[test]
    fn test_json_error_display() {
        let err = DatabaseError::JsonError("invalid json".into());
        assert!(err.to_string().contains("JSON serialization"));
    }

    #[test]
    fn test_query_error_display() {
        let err = DatabaseError::QueryError("syntax error".into());
        assert!(err.to_string().contains("Query error"));
    }

    #[test]
    fn test_unknown_display() {
        let err = DatabaseError::Unknown;
        assert_eq!(err.to_string(), "An unknown database error occurred");
    }

    #[test]
    fn test_pool_build_error_display() {
        let err = DatabaseError::PoolBuildError(deadpool_postgres::BuildError::NoRuntimeSpecified);
        assert!(err.to_string().contains("Failed to build connection pool"));
    }

    #[test]
    fn test_postgres_error_display() {
        let pg_err = tokio_postgres::Error::__private_api_timeout();
        let err = DatabaseError::PostgresError(pg_err);
        assert!(err.to_string().contains("PostgreSQL error"));
    }

    #[test]
    fn test_from_pool_error() {
        let pool_err = deadpool_postgres::PoolError::Closed;
        let db_err: DatabaseError = pool_err.into();
        assert!(matches!(db_err, DatabaseError::ConnectionError(_)));
    }

    #[test]
    fn test_from_build_error() {
        let build_err = deadpool_postgres::BuildError::NoRuntimeSpecified;
        let db_err: DatabaseError = build_err.into();
        assert!(matches!(db_err, DatabaseError::PoolBuildError(_)));
    }

    #[test]
    fn test_debug_format() {
        let err = DatabaseError::NotFound("test_id".into());
        let debug = format!("{err:?}");
        assert!(debug.contains("NotFound"));
        assert!(debug.contains("test_id"));
    }

    #[test]
    fn test_error_source() {
        use std::error::Error;
        let pool_err = deadpool_postgres::PoolError::Closed;
        let db_err = DatabaseError::ConnectionError(pool_err);
        let source = db_err.source();
        assert!(source.is_some());
    }
}
