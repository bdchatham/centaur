---
id: SPEC-runs-created-before
companions: []
sources: []
---

> **Canonical contract.** This SPEC and the files in `companions:` are the complete, preservation-validated contract for what to build, test, and validate. Source documents listed in frontmatter are for traceability — consult them only if you need narrative rationale or prose color this contract intentionally omits.

# Filter workflow runs created before a time

## Why

A pain to solve: `GET /api/workflows/runs` can filter by `workflow_name`, `state` and `created_after`, but a client cannot ask for runs older than a point in time, so it cannot page backwards through history or bound a window. Clients need `created_before`, the counterpart of `created_after`.

## Capabilities

- **CAP-1**
  - **intent:** A client can list only the workflow runs created before a given time.
  - **success:** `GET /api/workflows/runs?created_before=<RFC 3339 time>` returns only runs whose `created_at` is strictly earlier than that instant, newest first as today; a run created exactly at the bound is excluded, and offsets compare as instants (`10:00:00+02:00` equals `08:00:00Z`). A bound earlier than every run returns HTTP 200 with an empty `runs` list.
- **CAP-2**
  - **intent:** The bound composes with the existing filters, `created_after` included.
  - **success:** With `limit`, `workflow_name`, `state` and/or `created_after` also given, the response holds only runs matching all of them, and `limit` counts matching runs: with more than `limit` runs before the bound, the newest `limit` of them are returned, across all queues. With both time bounds, only runs strictly between them are returned.
- **CAP-3**
  - **intent:** A client that sends an unusable time learns what is expected.
  - **success:** A `created_before` that is not a valid RFC 3339 timestamp (garbage, empty, date-only, no offset) returns HTTP 400 whose message names `created_before` and RFC 3339; a request without `created_before` returns every run it returns today.

## Constraints

- The bound is applied in SQL per queue (workflow, slack-live, etl, etl-backfill) before `limit`, against the same `created_at` the response reports and in the same place as `created_after`; trimming a fetched page in Rust could return fewer than `limit` matches while more exist.
- The route parses `created_before` before looking up the workflow runtime, as it does `created_after`, so an invalid value is a 400 regardless of runtime availability, and stays a 400 when other filters are valid.
- Additive only: response shape, ordering, default limit, and `created_after` behaviour are unchanged; no migration.
- Proof goes through the HTTP route only: a new file under `tests/` of `services/api-rs/crates/centaur-api-server` (mirroring `runs_created_after_http.rs`) calls `GET /api/workflows/runs` on a router built with `build_router_with_session_and_workflow_runtime` and uses no type or signature this work adds, so it compiles before the change and fails because `created_before` is ignored. Test helpers may be copied or extracted into a shared tests module, and the `WorkflowRuntime::list_runs_by_state` shape (another positional argument or a filter struct) is an implementation choice; existing tests are edited only if the chosen design requires it, and the CI oracle flags such edits.
- Database-backed tests skip without a database URL; the invalid-value 400 tests need none and carry the red-then-green proof there.

## Non-goals

- A single range parameter, or time filters on `updated_at`, `started_at`, `completed_at`.
- Cursor pagination or ordering changes.
- New indexes or schema migration.
- Changing `created_after` behaviour.
- Updating `packages/api-client` or docs pages.

## Success signal

HTTP-level Rust tests seed runs at known creation times across several queues and show `?created_before=T` returns exactly the runs created strictly before T (excluding one created at T), newest first, alone and with `limit`, `workflow_name`, `state` and `created_after`; a bound earlier than every run gives an empty list; omitting it returns every run; invalid values yield a 400 naming `created_before` and RFC 3339.

## Assumptions

- Valid means accepted by the `time` crate's RFC 3339 parser: a full date-time with `Z` or an offset. A literal `+` in an offset must be percent-encoded in the URL, as for any query string.
- An empty `?created_before=` is invalid (400), as for `created_after`, not absent.
- `created_after` at or after `created_before` returns an empty `runs` list with HTTP 200, with no special validation.

## Open Questions

- Does sub-microsecond precision in `created_before` matter? Postgres `timestamptz` holds microseconds; the bound is passed through as given (assumed: no special handling).
- Should `packages/api-client` types and `docs/pages/extend/workflows*.mdx` document the parameter in this work (assumed: no)?
- Should an inverted window (`created_after` >= `created_before`) be rejected with 400 instead of returning an empty list (assumed: empty 200)?
