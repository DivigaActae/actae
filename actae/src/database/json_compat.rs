// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! JSON compatibility layer for PostgreSQL database operations
//!
//! This module provides conversion between sonic_rs::Value and PostgreSQL-compatible types
//! to eliminate serde_json dependency while maintaining database functionality.

use anyhow::Result;
use sonic_rs::Value as SonicValue;
use std::error::Error;
use tokio_postgres::types::{FromSql, IsNull, ToSql, Type};

/// Wrapper for sonic_rs::Value that implements PostgreSQL traits
#[derive(Debug, Clone, PartialEq)]
pub struct JsonValue(pub SonicValue);
impl JsonValue {
    pub fn new(value: SonicValue) -> Self {
        Self(value)
    }

    pub fn inner(self) -> SonicValue {
        self.0
    }

    pub fn as_inner(&self) -> &SonicValue {
        &self.0
    }
}

impl From<SonicValue> for JsonValue {
    fn from(value: SonicValue) -> Self {
        JsonValue::new(value)
    }
}

impl From<JsonValue> for SonicValue {
    fn from(wrapper: JsonValue) -> Self {
        wrapper.0
    }
}

// Implement ToSql for JsonValue to work with PostgreSQL
impl ToSql for JsonValue {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        // Guard against deeply nested in-memory values: `sonic_rs::to_string`
        // recurses with no depth limit and overflows the stack past ~30
        // levels, so a hostile or accidental save could crash the process.
        // Serialize `null` instead of overflowing.
        let json_string = if value_nesting_depth(&self.0) > MAX_JSON_DEPTH {
            tracing::warn!(
                "JSONB value exceeds {} nesting levels, serializing null",
                MAX_JSON_DEPTH
            );
            "null".to_string()
        } else {
            sonic_rs::to_string(&self.0).map_err(|e| Box::new(e) as Box<dyn Error + Sync + Send>)?
        };
        // JSONB binary format requires a version 1 header byte
        if *ty == Type::JSONB {
            out.extend_from_slice(b"\x01");
        }
        out.extend_from_slice(json_string.as_bytes());
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty, &Type::JSON | &Type::JSONB)
    }

    tokio_postgres::types::to_sql_checked!();
}

// Implement FromSql for JsonValue to work with PostgreSQL
impl<'a> FromSql<'a> for JsonValue {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        // Handle potential binary data or invalid UTF-8
        let json_str = match std::str::from_utf8(raw) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("Invalid UTF-8 in JSON column, using empty object: {}", e);
                return Ok(JsonValue(sonic_rs::json!({})));
            }
        };

        // Strip JSONB binary version header byte if present
        let json_str = json_str.trim_start_matches('\u{0001}');

        // Check for common corruption patterns
        if json_str.contains('\u{0000}') {
            tracing::warn!("Corrupted JSON data detected (null bytes), using empty object");
            return Ok(JsonValue(sonic_rs::json!({})));
        }

        // Guard against deeply nested JSONB. `sonic_rs::from_str` (the Value
        // parser) recurses with no depth limit and overflows the stack past
        // ~30 nesting levels, so a hostile or accidental save could crash any
        // process that loads that state (read, replay, diff, trail, …).
        // Reject anything deeper than a conservative bound and surface it as
        // an empty object instead of a stack overflow.
        if json_nesting_depth(json_str) > MAX_JSON_DEPTH {
            tracing::warn!(
                "JSONB value exceeds {} nesting levels, returning empty object",
                MAX_JSON_DEPTH
            );
            return Ok(JsonValue(sonic_rs::json!({})));
        }

        // Attempt to parse the JSON
        match sonic_rs::from_str(json_str) {
            Ok(value) => Ok(JsonValue(value)),
            Err(e) => {
                tracing::warn!(
                    "Failed to parse JSON '{}': {}, using empty object",
                    json_str.chars().take(50).collect::<String>(),
                    e
                );
                Ok(JsonValue(sonic_rs::json!({})))
            }
        }
    }

    fn accepts(ty: &Type) -> bool {
        matches!(ty, &Type::JSON | &Type::JSONB)
    }
}

/// Maximum nesting depth accepted when decoding JSONB. `sonic_rs::from_str`'s
/// Value parser recurses unboundedly and overflows around ~30 levels, so this
/// bound (24) is a conservative margin below that.
pub const MAX_JSON_DEPTH: usize = 24;

/// Walk an in-memory `sonic_rs::Value` and return its maximum nesting depth,
/// returning early once `MAX_JSON_DEPTH` is exceeded. Unlike the string
/// scanner, this inspects the already-parsed tree; the recursion is bounded
/// because we stop as soon as the bound is crossed, so pathological values
/// can never drive the walk deeper than ~MAX_JSON_DEPTH frames.
fn value_nesting_depth(v: &SonicValue) -> usize {
    use sonic_rs::JsonContainerTrait as _;
    fn walk(v: &SonicValue, depth: usize) -> usize {
        if depth > MAX_JSON_DEPTH {
            return depth;
        }
        if let Some(obj) = v.as_object() {
            let mut max = depth;
            for (_, child) in obj.iter() {
                max = max.max(walk(child, depth + 1));
                if max > MAX_JSON_DEPTH {
                    return max;
                }
            }
            max
        } else if let Some(arr) = v.as_array() {
            let mut max = depth;
            for child in arr.iter() {
                max = max.max(walk(child, depth + 1));
                if max > MAX_JSON_DEPTH {
                    return max;
                }
            }
            max
        } else {
            // Scalars contribute no container level; a scalar child of an
            // object is reported at its parent's container depth.
            depth.saturating_sub(1)
        }
    }
    walk(v, 1)
}

/// Scan a JSON document and return its maximum nesting depth without
/// allocating or recursing (iterative). This runs before any recursive parse
/// so pathological documents can never overflow the stack.
pub fn json_nesting_depth(s: &str) -> usize {
    let mut depth = 0usize;
    let mut max = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    for b in s.bytes() {
        if in_string {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                in_string = false;
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b'{' | b'[' => {
                depth += 1;
                if depth > max {
                    max = depth;
                }
            }
            b'}' | b']' => {
                depth = depth.saturating_sub(1);
            }
            _ => {}
        }
    }
    max
}

/// Convert sonic_rs::Value to PostgreSQL-compatible format
pub fn to_sql_json(value: &SonicValue) -> JsonValue {
    JsonValue(value.clone())
}

/// Convert from PostgreSQL result to sonic_rs::Value
pub fn from_sql_json(wrapper: JsonValue) -> SonicValue {
    wrapper.0
}

/// Deserialize a JSON request body with a nesting-depth guard.
///
/// `sonic_rs::from_slice` into a struct containing a `sonic_rs::Value` field
/// recurses unboundedly on deeply nested JSON and overflows the stack (~40
/// levels), which would let any client crash the server with a hostile
/// payload on record/transition/save_state/fork/… . This checks the raw
/// document first and rejects anything deeper than `MAX_JSON_DEPTH` before a
/// recursive parse ever runs.
///
/// Returns `Err(serde_json::Error)` (syntax or depth) — callers map it to a
/// `400 Bad Request` exactly like an existing parse failure.
pub fn parse_json_body<T: serde::de::DeserializeOwned>(
    body: &[u8],
) -> Result<T, serde_json::Error> {
    let s = std::str::from_utf8(body).map_err(|_| {
        serde_json::Error::io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "body is not valid UTF-8",
        ))
    })?;
    if json_nesting_depth(s) > MAX_JSON_DEPTH {
        return Err(serde_json::Error::io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("JSON body exceeds {MAX_JSON_DEPTH} nesting levels"),
        )));
    }
    sonic_rs::from_slice(body).map_err(|e| {
        serde_json::Error::io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            e.to_string(),
        ))
    })
}

/// Create empty JSON object for database operations
pub fn empty_json_object() -> JsonValue {
    JsonValue(sonic_rs::json!({}))
}

/// Create JSON null for database operations  
pub fn json_null() -> JsonValue {
    JsonValue(SonicValue::default())
}

/// Convert a sonic_rs::Value to serde_json::Value for clean HTTP serialization.
pub fn sonic_to_serde(value: &SonicValue) -> serde_json::Value {
    let json_str = sonic_rs::to_string(value).unwrap_or_else(|_| "null".to_string());
    serde_json::from_str(&json_str).unwrap_or(serde_json::Value::Null)
}

/// JSON response wrapper that ensures sonic_rs::Value serializes cleanly.
///
/// When sonic_rs has ``arbitrary_precision`` enabled, its ``Value::Number``
/// serializes as ``{"$sonic_rs::private::JsonNumber": "..."}`` through serde.
/// This wrapper converts through ``serde_json::Value`` to produce standard
/// JSON number output (``1``, ``3.14``, etc.) for HTTP API responses.
#[derive(Clone)]
pub struct CleanJson(pub SonicValue);

impl serde::Serialize for CleanJson {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let json_str = sonic_rs::to_string(&self.0).map_err(serde::ser::Error::custom)?;
        let v: serde_json::Value =
            serde_json::from_str(&json_str).map_err(serde::ser::Error::custom)?;
        serde_json::Value::serialize(&v, serializer)
    }
}

/// Build a ``CleanJson`` from any `serde::Serialize` value (typed response
/// structs like `StateDiffResponse` / `DecisionTrail`). Round-trips through
/// serde_json so sonic_rs arbitrary-precision numbers never leak into the
/// HTTP output — the same guarantee `clean_json!` provides for inline maps.
pub fn clean_json_from<T: serde::Serialize>(value: &T) -> CleanJson {
    let json_str = serde_json::to_string(value).unwrap_or_else(|_| "{}".to_string());
    let sonic: SonicValue = sonic_rs::from_str(&json_str).unwrap_or_else(|_| sonic_rs::json!({}));
    CleanJson(sonic)
}

/// Macro: constructs a ``CleanJson`` value for HTTP responses.
///
/// Use this instead of ``sonic_rs::json!`` in response construction to prevent
/// sonic_rs ``arbitrary_precision`` from leaking internal ``JsonNumber`` types
/// into the HTTP JSON output.
///
/// Usage:
/// ```ignore
/// use actae::clean_json;
///
/// (StatusCode::OK, Json(clean_json!({"status": "ok", "count": 5})))
/// ```
#[macro_export]
macro_rules! clean_json {
    ($($tt:tt)*) => {
        $crate::database::json_compat::CleanJson(sonic_rs::json!($($tt)*))
    };
}

/// Round-trip a sonic_rs::Value through its JSON string representation to
/// produce a value with clean number types suitable for HTTP serialization.
pub fn clean_sonic_value(value: &SonicValue) -> SonicValue {
    let json_str = sonic_rs::to_string(value).unwrap_or_else(|_| "null".to_string());
    sonic_rs::from_str(&json_str).unwrap_or(SonicValue::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sonic_rs::JsonValueTrait;

    #[test]
    fn test_json_value_conversion() {
        let original = sonic_rs::json!({"key": "value", "number": 42});
        let wrapper = JsonValue::new(original.clone());
        let converted_back = wrapper.inner();

        assert_eq!(original, converted_back);
    }

    #[test]
    fn test_json_serialization() {
        let value = sonic_rs::json!({"test": true});
        let json_str = sonic_rs::to_string(&value).unwrap();
        let parsed = sonic_rs::from_str::<SonicValue>(&json_str).unwrap();

        assert_eq!(value, parsed);
    }

    #[test]
    fn test_nesting_depth_counts_containers() {
        assert_eq!(json_nesting_depth(r#"{"a": 1}"#), 1);
        assert_eq!(json_nesting_depth(r#"{"a": {"b": [1, 2]}}"#), 3);
        assert_eq!(json_nesting_depth(r#"[{"a": [{}]}]"#), 4);
        assert_eq!(json_nesting_depth(r#"{}"#), 1);
        assert_eq!(json_nesting_depth(r#""plain""#), 0);
        assert_eq!(json_nesting_depth(r#"123"#), 0);
        assert_eq!(json_nesting_depth(r#"null"#), 0);
    }

    #[test]
    fn test_nesting_depth_ignores_braces_in_strings() {
        // Braces/strings inside JSON string values must not count.
        assert_eq!(json_nesting_depth(r#"{"k": "a { [ b }"}"#), 1);
        assert_eq!(json_nesting_depth(r#"{"k": "escaped \" { "}"#), 1);
        assert_eq!(json_nesting_depth(r#"{"k": "{[}]"}"#), 1);
    }

    #[test]
    fn test_nesting_depth_detects_deep_documents() {
        let mut s = String::from(r#"{"leaf": 1}"#);
        for _ in 0..50 {
            s = format!(r#"{{"child": {s}}}"#);
        }
        assert!(
            json_nesting_depth(&s) > MAX_JSON_DEPTH,
            "deep doc should exceed the guard bound"
        );
    }

    #[test]
    fn test_from_sql_guards_deep_jsonb() {
        let mut deep = String::from(r#"{"leaf": 1}"#);
        for _ in 0..100 {
            deep = format!(r#"{{"child": {deep}}}"#);
        }
        // Parsing through FromSql must return an empty object, not overflow.
        let wrapped = <JsonValue as tokio_postgres::types::FromSql>::from_sql(
            &tokio_postgres::types::Type::JSONB,
            deep.as_bytes(),
        )
        .expect("guarded parse");
        assert_eq!(wrapped.inner(), sonic_rs::json!({}));
    }

    #[test]
    fn test_from_sql_accepts_normal_jsonb() {
        let wrapped = <JsonValue as tokio_postgres::types::FromSql>::from_sql(
            &tokio_postgres::types::Type::JSONB,
            br#"{"a": {"b": [1, 2, {"c": true}]}}"#,
        )
        .expect("normal parse");
        assert!(wrapped.inner().is_object());
    }

    #[test]
    fn test_to_sql_guards_deep_values() {
        // Build a deep value in memory, then verify ToSql serializes `null`
        // instead of overflowing the stack.
        let mut v = sonic_rs::json!({"leaf": 1});
        for _ in 0..100 {
            v = sonic_rs::json!({"child": v});
        }
        let wrapper = JsonValue::new(v);
        let mut out = bytes::BytesMut::new();
        wrapper
            .to_sql(&tokio_postgres::types::Type::JSONB, &mut out)
            .expect("guarded to_sql");
        // JSONB header byte + "null"
        assert_eq!(&out[..], b"\x01null");
    }

    #[test]
    fn test_value_nesting_depth_counts() {
        use sonic_rs::json;
        assert_eq!(value_nesting_depth(&json!({"a": 1})), 1);
        assert_eq!(value_nesting_depth(&json!({"a": {"b": [1, 2]}})), 3);
        assert_eq!(value_nesting_depth(&json!([{"a": [{}]}])), 4);
        assert_eq!(value_nesting_depth(&json!(null)), 0);
        // Deep value reports > MAX_JSON_DEPTH.
        let mut v = json!({"leaf": 1});
        for _ in 0..50 {
            v = json!({"child": v});
        }
        assert!(value_nesting_depth(&v) > MAX_JSON_DEPTH);
    }

    #[test]
    fn test_parse_json_body_guards_depth() {
        // Normal body parses.
        #[derive(serde::Deserialize, Debug)]
        struct Req {
            state: SonicValue,
        }
        let ok: Req = parse_json_body(br#"{"state": {"a": 1}}"#).expect("normal body parses");
        assert!(ok.state.is_object());

        // Deep body is rejected before a recursive parse.
        let mut deep = String::from(r#"{"state": {"leaf": 1}}"#);
        for _ in 0..50 {
            deep = format!(r#"{{"state": {{"c": {deep}}}}}"#);
        }
        let err = parse_json_body::<Req>(deep.as_bytes()).expect_err("deep body rejected");
        assert!(
            err.to_string().contains("nesting levels"),
            "error should mention depth, got: {err}"
        );
    }

    #[test]
    fn test_parse_json_body_rejects_invalid() {
        #[derive(serde::Deserialize, Debug)]
        #[allow(dead_code)]
        struct Req {
            x: i64,
        }
        assert!(parse_json_body::<Req>(b"not json").is_err());
    }

    #[test]
    fn sonic_to_serde_preserves_structure_and_numbers() {
        let v = sonic_rs::json!({"nested": {"deep": [1, 2.5, "s", true, null]}});
        let serde_val = sonic_to_serde(&v);
        assert_eq!(serde_val["nested"]["deep"][0], serde_json::json!(1));
        assert_eq!(serde_val["nested"]["deep"][1], serde_json::json!(2.5));
        assert_eq!(serde_val["nested"]["deep"][2], serde_json::json!("s"));
        assert_eq!(serde_val["nested"]["deep"][4], serde_json::json!(null));
    }

    #[test]
    fn sonic_to_serde_is_key_order_insensitive() {
        // serde_json::Value objects are BTreeMap-backed → key order does not
        // matter, which is what makes `json_values_equal` canonical.
        let a = sonic_to_serde(&sonic_rs::from_str(r#"{"x":1,"y":2}"#).unwrap());
        let b = sonic_to_serde(&sonic_rs::from_str(r#"{"y":2,"x":1}"#).unwrap());
        assert_eq!(a, b);
    }

    #[test]
    fn clean_json_serializes_to_plain_json() {
        let clean = clean_json!({"status": "ok", "count": 5, "nested": {"f": 1.5}});
        let json_str = serde_json::to_string(&clean).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed["count"], serde_json::json!(5));
        assert_eq!(parsed["nested"]["f"], serde_json::json!(1.5));
    }

    #[test]
    fn clean_json_serializes_big_numbers_without_wrapper() {
        // arbitrary_precision JsonNumber wrappers must never leak to the wire.
        let clean = clean_json!({"big": 9223372036854775807i64});
        let json_str = serde_json::to_string(&clean).unwrap();
        assert!(
            !json_str.contains("$sonic_rs"),
            "leaked wrapper: {json_str}"
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&json_str).unwrap()["big"],
            serde_json::json!(9223372036854775807i64)
        );
    }

    #[test]
    fn clean_json_from_roundtrips_typed_struct() {
        #[derive(serde::Serialize)]
        struct Resp {
            status: String,
            count: i64,
        }
        let clean = clean_json_from(&Resp {
            status: "ok".into(),
            count: 3,
        });
        let json_str = serde_json::to_string(&clean).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed["status"], serde_json::json!("ok"));
        assert_eq!(parsed["count"], serde_json::json!(3));
    }

    #[test]
    fn clean_sonic_value_roundtrips_numbers() {
        let v = sonic_rs::json!({"a": 1, "f": 0.25, "arr": [1, 2]});
        let cleaned = clean_sonic_value(&v);
        assert_eq!(cleaned, v);
    }

    #[test]
    fn json_nesting_depth_handles_brackets_and_strings() {
        assert_eq!(json_nesting_depth(r#"{"a": [1, {"b": 2}]}"#), 3);
        // Braces inside JSON strings are not container depth.
        assert_eq!(json_nesting_depth(r#"{"a": "{\"brace\"}"}"#), 1);
        assert_eq!(json_nesting_depth(r#"{"a": "not { nested"}"#), 1);
        assert_eq!(json_nesting_depth(r#"plain text"#), 0);
        assert_eq!(json_nesting_depth(r#""#), 0);
    }

    #[test]
    fn json_nesting_depth_counts_each_container_level() {
        // Each { or [ increments; each closing decrements.
        assert_eq!(json_nesting_depth(r#"[[[1]]]"#), 3);
        assert_eq!(json_nesting_depth(r#"[{"x": [1]}]"#), 3);
        assert_eq!(json_nesting_depth(r#"{"a": {},"b": {}}"#), 2);
    }
}
