// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

//! Dashboard static-file serving.
//!
//! The dashboard is a Next.js static export (`dashboard/out`). Production
//! builds (`dashboard-embed` feature) bake the files into the binary and
//! serve them from memory — the self-contained single-file distribution. The
//! filesystem is used for dev checkouts and for custom dashboards via
//! `ACTAE_DASHBOARD_DIR`.
//!
//! Resolution order (first match wins):
//!
//! 1. `ACTAE_DASHBOARD_DIR` (explicit override / custom dashboard).
//! 2. Embedded files (only when built with the `dashboard-embed` feature).
//! 3. `../dashboard/out` relative to the crate (dev checkout convenience).
//! 4. `/app/dashboard` (legacy docker mount).
//! 5. A 404 fallback with a loud startup warning (no dashboard available).

use axum::handler::HandlerWithoutStateExt;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Router;
use tower_http::services::ServeDir;
use tracing::warn;

#[cfg(feature = "dashboard-embed")]
use axum::body::Body;
#[cfg(feature = "dashboard-embed")]
use axum::http::{header, Uri};
#[cfg(feature = "dashboard-embed")]
use percent_encoding::percent_decode_str;

/// Embedded dashboard tree (`dashboard-embed` feature).
#[cfg(feature = "dashboard-embed")]
static DASHBOARD: include_dir::Dir<'_> =
    include_dir::include_dir!("$CARGO_MANIFEST_DIR/../dashboard/out");

/// Build the router that serves the dashboard as the fallback for any
/// non-API path. The API layer mounts its own routes above this fallback.
pub fn fallback_router() -> Router {
    if let Ok(dir) = std::env::var("ACTAE_DASHBOARD_DIR").map(|s| s.trim().to_string()) {
        if !dir.is_empty() {
            warn!("Serving dashboard from ACTAE_DASHBOARD_DIR={dir}");
            return Router::new().fallback_service(ServeDir::new(dir));
        }
    }

    #[cfg(feature = "dashboard-embed")]
    {
        return Router::new().fallback_service(embedded_handler.into_service());
    }

    #[cfg(not(feature = "dashboard-embed"))]
    {
        let dev_dir = std::path::Path::new("../dashboard/out");
        if dev_dir.is_dir() {
            warn!(
                "Serving dashboard from {} (build with `npm run build` in dashboard/ \
                 if missing; the dashboard-embed feature embeds it into the binary)",
                dev_dir.display()
            );
            return Router::new().fallback_service(ServeDir::new(dev_dir));
        }
        let docker_dir = std::path::Path::new("/app/dashboard");
        if docker_dir.is_dir() {
            return Router::new().fallback_service(ServeDir::new(docker_dir));
        }
        warn!(
            "No dashboard found: set ACTAE_DASHBOARD_DIR, build with the dashboard-embed \
             feature, or place dashboard/out next to the binary — the API still works, \
             the dashboard UI returns 404"
        );
        Router::new().fallback_service(handler_404.into_service())
    }
}

/// Serve an embedded file, mirroring ServeDir semantics: an exact path, or
/// `<path>/index.html` for a request that resolves to a directory.
#[cfg(feature = "dashboard-embed")]
async fn embedded_handler(uri: Uri) -> Response {
    let raw = uri.path().trim_start_matches('/');
    let path = match percent_decode_str(raw).decode_utf8() {
        Ok(p) => p,
        Err(_) => return StatusCode::BAD_REQUEST.into_response(),
    };
    // Normalize: drop a trailing slash so `/flow/` resolves like `/flow`;
    // the root path (`/`) maps to `index.html`.
    let path = path.trim_matches('/');

    let file = DASHBOARD.get_file(path).or_else(|| {
        let candidate = if path.is_empty() {
            "index.html".to_string()
        } else {
            format!("{path}/index.html")
        };
        DASHBOARD.get_file(&candidate)
    });

    let Some(file) = file else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let content_type = mime_guess::from_path(file.path())
        .first_or_octet_stream()
        .essence_str()
        .to_string();

    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff");
    // Files under `_next/static` are content-hashed (immutable); the rest
    // (HTML etc.) must be revalidated.
    if path.starts_with("_next/static") {
        builder = builder.header(header::CACHE_CONTROL, "public, max-age=31536000, immutable");
    } else {
        builder = builder.header(header::CACHE_CONTROL, "no-cache");
    }
    builder
        .body(Body::from(file.contents().to_vec()))
        .expect("static response builder is infallible")
}

/// Trivial 404 fallback when no dashboard source exists.
#[cfg(not(feature = "dashboard-embed"))]
async fn handler_404() -> Response {
    StatusCode::NOT_FOUND.into_response()
}
