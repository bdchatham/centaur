---
id: SPEC-api-version-endpoint
companions: []
sources: []
---

> **Canonical contract.** This SPEC and the files in `companions:` are the complete, preservation-validated contract for what to build, test, and validate. Source documents listed in frontmatter are for traceability — consult them only if you need narrative rationale or prose color this contract intentionally omits.

# API version endpoint

## Why

A pain to solve: an operator or deploy tool cannot ask a running `centaur-api-server` which build it is. `GET /healthz` says only that the process is up. A `GET /api/version` endpoint reports the build's cargo package version and git commit so a deploy can be matched to the code.

## Capabilities

- **CAP-1**
  - **intent:** A client can ask the running server which build it is.
  - **success:** `GET /api/version` returns 200 with a JSON object whose `version` and `git_commit` are non-empty strings; `version` equals the crate's cargo package version.
- **CAP-2**
  - **intent:** The reported build identity is fixed when the binary is built.
  - **success:** Both values are embedded at compile time (no runtime git call or file read); a build where no commit can be resolved reports `git_commit` as `"unknown"` rather than failing or returning an empty string.
- **CAP-3**
  - **intent:** The endpoint is usable by probes and deploy tooling like the health checks.
  - **success:** `GET /api/version` with no `Authorization` header returns 200, including when the app state is not yet ready.

## Constraints

- Follow the health-check convention: the route is registered on the unauthenticated router beside `/healthz`, `/readyz` and `/metrics` in `routes.rs`; no `route_access` capability entry.
- Response body is exactly `{"version": ..., "git_commit": ...}`; no `ok` envelope, no other keys.
- `version` comes from `CARGO_PKG_VERSION`. `git_commit` comes from a new `build.rs` for `centaur-api-server`, resolving in order: env `CENTAUR_GIT_COMMIT`, `git rev-parse HEAD`, `"unknown"`; the build script re-runs when that env var or the git HEAD changes.
- The Docker build context has no `.git`, so `services/api-rs/Dockerfile` takes a `CENTAUR_GIT_COMMIT` build arg into the builder stage, and the Justfile `_build-api-rs` recipe passes the current commit.
- Additive only: existing routes and their tests keep their behavior; the public-route list test may be extended.
- Proof is by a new Rust test file under `services/api-rs/crates/centaur-api-server/tests/` using only existing public items (`build_router_with_app_state`, `AppState::unready`, `ApiAuthConfig`), no database, so it compiles before the change and fails with a 404 on the missing route. Existing tests may be edited where the design needs it; the CI oracle flags such edits for human review.

## Non-goals

- Build timestamp, rustc version, dirty-tree flag, or any field beyond `version` and `git_commit`.
- Changing `/healthz` or `/readyz` bodies.
- Edits to `.github/workflows/` (image publishing).
- `packages/api-client` types or UI.
- Authorization policy or a new capability.

## Success signal

A Rust test builds the router with an unready app state, sends an unauthenticated `GET /api/version`, and sees 200 with `version` and `git_commit` both non-empty strings, `version` matching the package version.

## Assumptions

- The route is public like `/healthz` (build info is not sensitive; probes need no credentials).
- `git_commit` is the full 40-character SHA when resolved from git or the env var.
- `"unknown"` is an acceptable fallback so source tarball or Docker builds without a commit still compile.

## Open Questions

- Should `.github/workflows/publish-images.yml` pass the commit as a `CENTAUR_GIT_COMMIT` build arg for the api-rs image? Until it does, published images report `"unknown"` unless built via the Justfile.
- Should the endpoint require authentication instead of being public?
- Should `git_commit` be a short SHA?
