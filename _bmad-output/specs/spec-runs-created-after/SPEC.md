---
id: SPEC-runs-created-after
companions: []
sources: []
---

> **Canonical contract.** This SPEC and the files in `companions:` are the complete, preservation-validated contract for what to build, test, and validate. Source documents listed in frontmatter are for traceability — consult them only if you need narrative rationale or prose color this contract intentionally omits.

# Filter workflow runs by creation time

## Why

A pain to solve: `GET /api/workflows/runs` can filter by `workflow_name` and `state` but not by time, so a client that polls for new work must re-fetch recent runs and diff them itself. Clients need to ask for only the runs created after a point in time.

## Capabilities

- **CAP-1**
  - **intent:** A client can list only the workflow runs created after a given time.
  - **success:** `GET /api/workflows/runs?created_after=<RFC 3339 time>` returns only runs whose `created_at` is strictly after that instant, newest first as today; a run created exactly at the bound is excluded, and offsets compare as instants (`10:00:00+02:00` equals `08:00:00Z`).
- **CAP-2**
  - **intent:** The time filter composes with the existing filters.
  - **success:** With `limit`, `workflow_name` and/or `state` also given, the response holds only runs matching all of them, and `limit` counts matching runs: with more than `limit` runs after the bound, the newest `limit` of them are returned, across all queues.
- **CAP-3**
  - **intent:** A client that sends an unusable time learns what is expected.
  - **success:** A `created_after` that is not a valid RFC 3339 timestamp (garbage, empty, date-only, no offset) returns HTTP 400 whose message names `created_after` and RFC 3339; a request without `created_after` returns what it returns today.

## Constraints

- The bound is applied in SQL per queue (workflow, slack-live, etl, etl-backfill) before `limit`, against the same `created_at` the response reports; trimming a fetched page in Rust could return fewer than `limit` matches while more exist.
- The route parses `created_after` before looking up the workflow runtime, so an invalid value is a 400 regardless of runtime availability.
- Additive only: response shape, ordering, and default limit are unchanged; no migration.
- Proof goes through the HTTP route only: new tests in new files under `tests/` of `services/api-rs/crates/centaur-api-server` call `GET /api/workflows/runs` on a router built with `build_router_with_session_and_workflow_runtime`, and use no type or signature this work adds, so they compile before the change and fail because `created_after` is ignored. Helper types and the `WorkflowRuntime::list_runs_by_state` shape are implementation choices behind those tests; existing tests are edited only if the chosen design requires it, and the CI oracle flags such edits.
- Database-backed tests skip without a database URL; the invalid-value 400 tests need none and carry the red-then-green proof there.

## Non-goals

- `created_before`, ranges, or time filters on `updated_at`, `started_at`, `completed_at`.
- Cursor pagination or ordering changes.
- New indexes or schema migration.
- Updating `packages/api-client` or docs pages.

## Success signal

HTTP-level Rust tests seed runs at known creation times across several queues and show `?created_after=T` returns exactly the runs created strictly after T (excluding one created at T), newest first, alone and with `limit`, `workflow_name` and `state`; invalid values yield a 400 naming `created_after` and RFC 3339.

## Assumptions

- Valid means accepted by the `time` crate's RFC 3339 parser: a full date-time with `Z` or an offset. A literal `+` in an offset must be percent-encoded in the URL, as for any query string.
- An empty `?created_after=` is invalid (400), as for `?state=`, not absent.
- A bound in the future returns an empty `runs` list with HTTP 200.

## Open Questions

- Does sub-microsecond precision in `created_after` matter? Postgres `timestamptz` holds microseconds; the bound is passed through as given (assumed: no special handling).
- Should `packages/api-client` types and `docs/pages/extend/workflows*.mdx` document the parameter in this work (assumed: no)?
