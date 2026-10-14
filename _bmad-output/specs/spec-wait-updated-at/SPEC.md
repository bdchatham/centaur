---
id: SPEC-wait-updated-at
companions: []
sources: []
---

> **Canonical contract.** This SPEC and the files in `companions:` are the complete, preservation-validated contract for what to build, test, and validate. Source documents listed in frontmatter are for traceability — consult them only if you need narrative rationale or prose color this contract intentionally omits.

# Workflow runs waiting on an event list with a finite updated_at

## Why

A pain to solve: a workflow run that waits on an event with no timeout is stored by Absurd with `available_at = 'infinity'`. The api-rs workflow-runs queries compute `updated_at = greatest(enqueue_at, coalesce(available_at, enqueue_at))`, which yields `infinity`; `workflow_run_from_row` cannot decode it into `OffsetDateTime`, so `GET /api/workflows/runs` fails for every run while one such run exists, and `GET /api/workflows/runs/{run_id}` fails for that run.

## Capabilities

- **CAP-1**
  - **intent:** A client can fetch a run that is waiting on an event with no timeout.
  - **success:** `get_run` and `GET /api/workflows/runs/{run_id}` return that run with status `sleeping` and a finite RFC 3339 `updated_at`.
- **CAP-2**
  - **intent:** A waiting run does not break listing of other runs.
  - **success:** With a no-timeout waiting run present, `list_runs` and `GET /api/workflows/runs` return 200 including the waiting run and the other runs, for all four workflow queues (workflow, slack-live, etl, etl-backfill).
- **CAP-3**
  - **intent:** A waiting run's `updated_at` reflects its last real change, not the scheduler's sentinel.
  - **success:** For a waiting run, `updated_at` equals the later of `enqueue_at` and the sleeping attempt's `started_at` (or its `created_at` when never started), and is identical from list and get.

## Constraints

- Runs with a finite `available_at` keep today's `updated_at` value exactly; only the non-finite case changes.
- The fix is read-side, in the list and get queries in `centaur-workflows` for all four queues. Absurd's stored `available_at = 'infinity'` and its migrations stay as they are.
- `WorkflowRun` field names, types, and non-null RFC 3339 `updated_at` are unchanged; no new fields.
- Proof is by new Rust tests in a new file under `services/api-rs/crates/centaur-api-server/tests/`, seeding Absurd rows directly as `run_timestamps.rs` does; database-backed tests skip without a database URL.
- Each new test must compile before the change and fail only because of it (500 / decode error), so it calls only existing items (`WorkflowRuntime::list_runs`/`get_run`, the routes via `build_router_with_session_and_workflow_runtime`) and asserts on their output. Existing tests may be edited where the design needs it; the CI oracle flags such edits for human review.

## Non-goals

- Changing how Absurd stores waits, or the `await_event` migrations.
- Changing `updated_at` for runs with a finite `available_at`, including timed waits and retry backoff.
- Adding fields, or updating `packages/api-client` types or any UI.

## Success signal

Rust tests show that with a no-timeout waiting run seeded alongside pending and completed runs, the list endpoint returns 200 with all of them and the get endpoint returns the waiting run, each with a finite `updated_at` equal to the specified value.

## Assumptions

- Postgres `infinity` is the only non-finite `available_at` that occurs.
- The attempt's `started_at` (else `created_at`) is an acceptable "last real change" for a waiting run.

## Open Questions

- Should a waiting run's `updated_at` be the moment the wait began (the Absurd waits table `created_at`) rather than the attempt's start or creation?
- Should a finite future `available_at` (timed wait, retry backoff) also stop surfacing as `updated_at`, since it is a future time?
- Do `packages/api-client` or the UI need checks for waiting runs?
