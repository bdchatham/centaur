//! Story: `GET /api/version` is a public route returning exactly
//! `{"version", "git_commit"}` as non-empty strings, with `version` equal to the
//! crate's package version, even while the app state is not yet ready.

use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use centaur_api_server::{ApiAuthConfig, AppState, build_router_with_app_state};
use serde_json::Value;
use tower::ServiceExt;

async fn get_version() -> (StatusCode, Value) {
    let app = build_router_with_app_state(AppState::unready(ApiAuthConfig::testing("test-secret")));
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/version")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, body)
}

#[tokio::test]
async fn version_http_unauthenticated_get_returns_200_when_unready() {
    let (status, _) = get_version().await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn version_http_reports_non_empty_version_and_git_commit() {
    let (status, body) = get_version().await;
    assert_eq!(status, StatusCode::OK);
    for key in ["version", "git_commit"] {
        let value = body[key]
            .as_str()
            .unwrap_or_else(|| panic!("{key} must be a string in {body}"));
        assert!(!value.is_empty(), "{key} must be non-empty");
    }
}

#[tokio::test]
async fn version_http_version_matches_package_version() {
    let (status, body) = get_version().await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["version"], env!("CARGO_PKG_VERSION"));
}

#[tokio::test]
async fn version_http_body_has_exactly_version_and_git_commit_keys() {
    let (status, body) = get_version().await;
    assert_eq!(status, StatusCode::OK);
    let mut keys: Vec<&str> = body
        .as_object()
        .expect("body must be a JSON object")
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(keys, ["git_commit", "version"]);
}
