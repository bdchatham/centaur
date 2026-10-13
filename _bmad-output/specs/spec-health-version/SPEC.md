---
id: SPEC-health-version
companions: []
sources: []
---

> **Canonical contract.** This SPEC and the files in `companions:` are the complete, preservation-validated contract for what to build, test, and validate. Source documents listed in frontmatter are for traceability — consult them only if you need narrative rationale or prose color this contract intentionally omits.

# Report the api-rs version from the health endpoint

## Why

An operator or probe hitting api-rs's `GET /healthz` can confirm the service is up but not which build is answering. Exposing the crate version in the same response closes that gap cheaply. This is a throwaway smoke-test brief, so scope stays minimal.

## Capabilities

- **CAP-1**
  - **intent:** A caller of `GET /healthz` can read the api-rs crate version from the JSON response.
  - **success:** A Rust test in `centaur-api-server` calls `/healthz`, decodes the body, and asserts the top-level `version` is a string equal to `env!("CARGO_PKG_VERSION")` of `centaur-api-server`.
- **CAP-2**
  - **intent:** Existing `/healthz` behavior is unchanged by the addition.
  - **success:** The existing healthz tests in `centaur-api-server/src/lib.rs` pass unmodified, and a test asserts `version` is present both with and without a bearer `Authorization` header while `ok` stays `true`.

## Constraints

- The version is compiled in (`CARGO_PKG_VERSION`); no runtime file, env var, or network lookup, so `/healthz` stays dependency-free and answers before the runtime is ready.
- The change is additive: existing keys (`ok`, `slack_client_jwt`) keep their names and types, and `/healthz` stays unauthenticated.

## Non-goals

- Git SHA or other build metadata.
- Changes to `/readyz`, a separate version endpoint, or chart/probe configuration.
- Bumping or unifying crate versions across the workspace.

## Success signal

`cargo test -p centaur-api-server` in `services/api-rs` passes with a test showing `GET /healthz` returns `{"ok": true, "version": "<crate version>"}`.

## Assumptions

- "The api-rs crate version" means the version of `centaur-api-server`, the crate serving `/healthz`; the workspace has no crate named `api-rs` and no shared version.
- The field is a top-level string named `version`, in semver form as written in `Cargo.toml`.

## Open Questions

- Should `version` instead be a new workspace-wide version? Today every crate is `0.1.0`, so values coincide.
- Is `version` required on every `/healthz` response, including the one carrying `slack_client_jwt` diagnostics (assumed yes)?
