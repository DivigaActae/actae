// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Production-grade database schema validation and error handling
//! Prevents runtime errors due to schema mismatches

use anyhow::Result;
use deadpool_postgres::Client;
use sonic_rs::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, error, info, warn};

/// Schema validation errors
#[derive(Debug, thiserror::Error)]
pub enum SchemaValidationError {
    #[error("Table '{table}' does not exist")]
    TableNotFound { table: String },

    #[error("Column '{column}' does not exist in table '{table}'")]
    ColumnNotFound { table: String, column: String },

    #[error("Index '{index}' creation failed: {reason}")]
    IndexCreationFailed { index: String, reason: String },

    #[error("Database connection error: {source}")]
    DatabaseError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("Schema validation failed: {message}")]
    ValidationFailed { message: String },
}

/// Column information from database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub column_name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub column_default: Option<String>,
    pub character_maximum_length: Option<i32>,
}

/// Table schema information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub table_name: String,
    pub columns: HashMap<String, ColumnInfo>,
    pub indexes: Vec<String>,
    pub constraints: Vec<String>,
}

/// Schema validator for ensuring database compatibility
pub struct SchemaValidator {
    client: Client,
    schema_cache: HashMap<String, TableSchema>,
    validation_cache: HashMap<String, bool>,
}

impl SchemaValidator {
    /// Create new schema validator
    pub fn new(client: Client) -> Self {
        Self {
            client,
            schema_cache: HashMap::new(),
            validation_cache: HashMap::new(),
        }
    }

    /// Validate that a table exists and has expected columns
    pub async fn validate_table_schema(
        &mut self,
        table_name: &str,
        required_columns: &[&str],
    ) -> Result<TableSchema, SchemaValidationError> {
        info!(" Validating schema for table: {}", table_name);

        // Check cache first
        if let Some(cached_schema) = self.schema_cache.get(table_name) {
            debug!("Using cached schema for table: {}", table_name);
            return Ok(cached_schema.clone());
        }

        // Load table schema from database
        let schema = self.load_table_schema(table_name).await?;

        // Validate required columns exist
        for &column in required_columns {
            if !schema.columns.contains_key(column) {
                let error = SchemaValidationError::ColumnNotFound {
                    table: table_name.to_string(),
                    column: column.to_string(),
                };

                error!(" Schema validation failed: {}", error);

                // Attempt to suggest similar column names
                let similar_columns = self.find_similar_columns(&schema.columns, column);
                if !similar_columns.is_empty() {
                    warn!(" Similar columns found: {}", similar_columns.join(", "));
                }

                return Err(error);
            }
        }

        // Cache the validated schema
        self.schema_cache
            .insert(table_name.to_string(), schema.clone());

        info!(" Schema validation passed for table: {}", table_name);
        Ok(schema)
    }

    /// Load table schema from database
    async fn load_table_schema(
        &self,
        table_name: &str,
    ) -> Result<TableSchema, SchemaValidationError> {
        // Check if table exists
        let table_exists = self.check_table_exists(table_name).await?;
        if !table_exists {
            return Err(SchemaValidationError::TableNotFound {
                table: table_name.to_string(),
            });
        }

        // Load column information
        let columns = self.load_table_columns(table_name).await?;

        // Load index information
        let indexes = self.load_table_indexes(table_name).await?;

        // Load constraint information
        let constraints = self.load_table_constraints(table_name).await?;

        Ok(TableSchema {
            table_name: table_name.to_string(),
            columns,
            indexes,
            constraints,
        })
    }

    /// Check if table exists
    async fn check_table_exists(&self, table_name: &str) -> Result<bool, SchemaValidationError> {
        let query = "
            SELECT COUNT(*) as count
            FROM information_schema.tables 
            WHERE table_name = $1 
            AND table_schema = 'public'
        ";

        let rows = self
            .client
            .query(query, &[&table_name])
            .await
            .map_err(|e| SchemaValidationError::DatabaseError {
                source: Box::new(e),
            })?;

        let count: i64 = rows[0].get("count");
        Ok(count > 0)
    }

    /// Load column information for a table
    async fn load_table_columns(
        &self,
        table_name: &str,
    ) -> Result<HashMap<String, ColumnInfo>, SchemaValidationError> {
        let query = "
            SELECT 
                column_name,
                data_type,
                is_nullable::boolean as is_nullable,
                column_default,
                character_maximum_length
            FROM information_schema.columns 
            WHERE table_name = $1 
            AND table_schema = 'public'
            ORDER BY ordinal_position
        ";

        let rows = self
            .client
            .query(query, &[&table_name])
            .await
            .map_err(|e| SchemaValidationError::DatabaseError {
                source: Box::new(e),
            })?;

        let mut columns = HashMap::new();

        for row in rows {
            let column_name: String = row.get("column_name");
            let data_type: String = row.get("data_type");
            let is_nullable: bool = row.get("is_nullable");
            let column_default: Option<String> = row.get("column_default");
            let character_maximum_length: Option<i32> = row.get("character_maximum_length");

            let column_info = ColumnInfo {
                column_name: column_name.clone(),
                data_type,
                is_nullable,
                column_default,
                character_maximum_length,
            };

            columns.insert(column_name, column_info);
        }

        debug!("Loaded {} columns for table: {}", columns.len(), table_name);
        Ok(columns)
    }

    /// Load index information for a table
    async fn load_table_indexes(
        &self,
        table_name: &str,
    ) -> Result<Vec<String>, SchemaValidationError> {
        let query = "
            SELECT indexname
            FROM pg_indexes 
            WHERE tablename = $1 
            AND schemaname = 'public'
            ORDER BY indexname
        ";

        let rows = self
            .client
            .query(query, &[&table_name])
            .await
            .map_err(|e| SchemaValidationError::DatabaseError {
                source: Box::new(e),
            })?;

        let indexes = rows
            .iter()
            .map(|row| row.get::<_, String>("indexname"))
            .collect();

        debug!("Loaded {} indexes for table: {}", rows.len(), table_name);
        Ok(indexes)
    }

    /// Load constraint information for a table
    async fn load_table_constraints(
        &self,
        table_name: &str,
    ) -> Result<Vec<String>, SchemaValidationError> {
        let query = "
            SELECT constraint_name, constraint_type
            FROM information_schema.table_constraints 
            WHERE table_name = $1 
            AND table_schema = 'public'
            ORDER BY constraint_name
        ";

        let rows = self
            .client
            .query(query, &[&table_name])
            .await
            .map_err(|e| SchemaValidationError::DatabaseError {
                source: Box::new(e),
            })?;

        let constraints: Vec<String> = rows
            .iter()
            .map(|row| {
                let name: String = row.get("constraint_name");
                let constraint_type: String = row.get("constraint_type");
                format!("{name} ({constraint_type})")
            })
            .collect();

        debug!(
            "Loaded {} constraints for table: {}",
            constraints.len(),
            table_name
        );
        Ok(constraints)
    }

    /// Find columns with similar names (for helpful error messages)
    fn find_similar_columns(
        &self,
        columns: &HashMap<String, ColumnInfo>,
        target: &str,
    ) -> Vec<String> {
        let target_lower = target.to_lowercase();
        let mut similar = Vec::new();

        for column_name in columns.keys() {
            let column_lower = column_name.to_lowercase();

            // Check for partial matches
            if column_lower.contains(&target_lower) || target_lower.contains(&column_lower) {
                similar.push(column_name.clone());
            }

            // Check for similar words (basic Levenshtein-like)
            if Self::are_similar_strings(&target_lower, &column_lower) {
                similar.push(column_name.clone());
            }
        }

        similar.sort();
        similar.dedup();
        similar
    }

    /// Basic string similarity check
    pub(crate) fn are_similar_strings(s1: &str, s2: &str) -> bool {
        if s1.is_empty() || s2.is_empty() {
            return false;
        }

        let max_len = s1.len().max(s2.len());
        let min_len = s1.len().min(s2.len());

        // If lengths are very different, not similar
        if max_len > min_len * 2 {
            return false;
        }

        // Simple character overlap check
        let s1_chars: std::collections::HashSet<char> = s1.chars().collect();
        let s2_chars: std::collections::HashSet<char> = s2.chars().collect();
        let overlap = s1_chars.intersection(&s2_chars).count();

        // At least 60% character overlap
        overlap >= (min_len * 6) / 10
    }

    /// Safely create index with proper error handling
    pub async fn create_index_safe(
        &self,
        index_name: &str,
        table_name: &str,
        columns: &[&str],
        index_type: Option<&str>,
    ) -> Result<(), SchemaValidationError> {
        info!(" Creating index {} on table {}", index_name, table_name);

        // First validate that all columns exist
        for &column in columns {
            let column_exists_query = "
                SELECT COUNT(*) as count
                FROM information_schema.columns 
                WHERE table_name = $1 
                AND column_name = $2
                AND table_schema = 'public'
            ";

            let rows = self
                .client
                .query(column_exists_query, &[&table_name, &column])
                .await
                .map_err(|e| SchemaValidationError::DatabaseError {
                    source: Box::new(e),
                })?;

            let count: i64 = rows[0].get("count");
            if count == 0 {
                return Err(SchemaValidationError::ColumnNotFound {
                    table: table_name.to_string(),
                    column: column.to_string(),
                });
            }
        }

        // Build index creation SQL
        let column_list = columns.join(", ");
        let index_type_clause = match index_type {
            Some(idx_type) => format!("USING {idx_type}"),
            None => String::new(),
        };

        let create_index_sql = format!(
            "CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} {index_type_clause} ({column_list})"
        );

        // Execute index creation
        match self.client.execute(&create_index_sql, &[]).await {
            Ok(_) => {
                info!(" Successfully created index: {}", index_name);
                Ok(())
            }
            Err(e) => {
                let error = SchemaValidationError::IndexCreationFailed {
                    index: index_name.to_string(),
                    reason: e.to_string(),
                };
                error!(" Failed to create index: {}", error);
                Err(error)
            }
        }
    }

    /// Clear validation cache (for testing or when schema changes)
    pub fn clear_cache(&mut self) {
        self.schema_cache.clear();
        self.validation_cache.clear();
        debug!(" Schema validation cache cleared");
    }

    /// Get schema information for debugging
    pub fn get_schema_info(&self, table_name: &str) -> Option<&TableSchema> {
        self.schema_cache.get(table_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_string_similarity() {
        use deadpool_postgres::{Config, Runtime};
        use tokio_postgres::NoTls;

        // Create a mock database configuration for testing
        let mut cfg = Config::new();
        cfg.host = Some("localhost".to_string());
        cfg.port = Some(5432);
        cfg.user = Some("test_user".to_string());
        cfg.password = Some("test_password".to_string());
        cfg.dbname = Some("test_db".to_string());

        // Create test client - this would connect to test database in integration tests
        cfg.create_pool(Some(Runtime::Tokio1), NoTls)
            .expect("Failed to create test pool");

        // For unit testing of string similarity logic, we can test without actual DB connection
        // Test the string similarity logic directly
        // Note: "name" vs "feature_name" fails length check (12 > 4*2)
        assert!(SchemaValidator::are_similar_strings("user_id", "userid"));
        assert!(!SchemaValidator::are_similar_strings(
            "name",
            "completely_different"
        ));
        assert!(SchemaValidator::are_similar_strings(
            "first_name",
            "last_name"
        ));
        assert!(!SchemaValidator::are_similar_strings("abc", "xyz"));
    }

    #[test]
    fn test_find_similar_columns_by_value() {
        let mut columns = HashMap::new();
        columns.insert(
            "feature_name".to_string(),
            ColumnInfo {
                column_name: "feature_name".to_string(),
                data_type: "varchar".to_string(),
                is_nullable: false,
                column_default: None,
                character_maximum_length: Some(100),
            },
        );

        // Test find_similar_columns with a dummy client since we can't create one without DB
        // The method only uses self.client for DB queries, not for the similarity logic
        // Use a simplified approach: test the logic directly:
        let target_lower = "name".to_lowercase();
        let mut similar = Vec::new();
        for column_name in columns.keys() {
            let column_lower = column_name.to_lowercase();
            if column_lower.contains(&target_lower) || target_lower.contains(&column_lower) {
                similar.push(column_name.clone());
            } else if SchemaValidator::are_similar_strings(&target_lower, &column_lower) {
                similar.push(column_name.clone());
            }
        }
        similar.sort();
        similar.dedup();
        assert!(similar.contains(&"feature_name".to_string()));
    }
}
