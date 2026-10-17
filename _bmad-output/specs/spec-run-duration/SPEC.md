---
id: SPEC-run-duration
companions: []
sources: []
---

> **Canonical contract.** This SPEC and the files in `companions:` are the complete, preservation-validated contract for what to build, test, and validate. Source documents listed in frontmatter are for traceability — consult them only if you need narrative rationale or prose color this contract intentionally omits.

# Workflow run duration endpoint

## Why

An opportunity to capture: clients of the api-rs workflow-runs API that only need how long a run took must fetch `GET /api/workflows/runs/{run_id}` and compute the difference of `started_at` and `completed_at` themselves, receiving input, result and failure payloads they do not need. A small per-run duration endpoint answers that one question.

## Capabilities

- **CAP-1**
  - **intent:** A client can fetch the total duration of a workflow run that has ended.
  - **success:** `GET /api/workflows/runs/{run_id}/duration` for a completed or failed run returns 200 with the run's duration in whole milliseconds, equal to the full run's `completed_at` minus `started_at` (sub-millisecond remainder truncated), for runs on all four queues (workflow, slack-live, etl, etl-backfill). A run retried before it ended measures from its first start to its final end.
- **CAP-2**
  - **intent:** A client can tell that a run has no duration yet.
  - **success:** For a pending, running or sleeping run the endpoint returns 200 with the duration `null`, on all four queues.
- **CAP-3**
  - **intent:** A client learns that a run does not exist.
  - **success:** `GET /api/workflows/runs/{run_id}/duration` for an unknown run returns 404 with the same error body as the full run endpoint.

## Constraints

- Additive only: existing routes, `WorkflowRun`, and the list and get responses keep their shape and semantics.
- The duration derives from the same `started_at` (task first start) and `completed_at` (set only once the run is completed, failed or cancelled) that `GET /api/workflows/runs/{run_id}` reports; it is not computed from separate timestamps. The body carries the duration and the `ok` flag only, none of the run's other fields.
- The new route needs a `route_access` entry mapped to `Capability::WorkflowsRead`, like the sibling GET run routes; without one every caller gets 403.
- Values come from durable Absurd state in Postgres; no process-local source of truth and no migration.
- Proof is by new Rust test files under `services/api-rs/crates/centaur-api-server/tests/`. Each test compiles before the change and fails because the route is missing, so it uses only existing items (`build_router_with_session_and_workflow_runtime`, a console token, direct seeding of the Absurd queue tables) and asserts on HTTP status and JSON. Database-backed tests skip without a database URL. Existing tests may be edited where the design needs it; the CI oracle flags such edits for human review.

## Non-goals

- Durations for steps or attempts, or a queue-wait (created to first start) figure.
- A duration for a run still in progress (elapsed so far).
- A list endpoint or a `fields` selector on existing routes.
- Changing `started_at`, `completed_at` or `status` semantics.
- `packages/api-client` types or any UI.
- Caching.

## Success signal

Rust tests show that, for runs in each state and queue, the endpoint returns the whole-millisecond difference of the full run's `completed_at` and `started_at` for ended runs and `null` for runs that have not ended, and that an unknown run returns 404.

## Assumptions

- The body follows the sibling routes' envelope with the key `duration_ms`: `{"ok": true, "duration_ms": <integer or null>}`.
- A run that ended without ever starting (cancelled while pending) has no first start, so its duration is `null`.
- Milliseconds are truncated, not rounded.
- A malformed (non-UUID) run id behaves as it does on the full endpoint; this work does not special-case it.

## Open Questions

- Envelope and key: is `{"ok": true, "duration_ms": N}` (assumed) wanted, or a bare number or other key?
- Should a run cancelled before it ever started report `null` (assumed), `0`, or the time from creation to cancellation?
- `get_run` casts the id to uuid, so a malformed run id likely yields a 500 on the full endpoint today; should the duration route answer 404 or 400 for it?
