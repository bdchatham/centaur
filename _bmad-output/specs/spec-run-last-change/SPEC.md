---
id: SPEC-run-last-change
companions: []
sources: []
---

> **Canonical contract.** This SPEC and the files in `companions:` are the complete, preservation-validated contract for what to build, test, and validate. Source documents listed in frontmatter are for traceability — consult them only if you need narrative rationale or prose color this contract intentionally omits.

# Workflow run `updated_at` is the last real change

## Why

A pain to solve: clients of the api-rs workflow-runs API (`GET /api/workflows/runs`, `GET /api/workflows/runs/{run_id}`) read `updated_at` as "when this run last changed", but it is derived from scheduling times. A finished run reports when its last attempt started, not when it finished; a run cancelled while sleeping reports its future wake time. Prior work fixed sleeping and infinite-wait runs; terminal and pending runs remain wrong.

## Capabilities

- **CAP-1**
  - **intent:** A client sees when a completed or failed run finished as its `updated_at`.
  - **success:** For a completed run `updated_at` equals the last attempt's completion time; for a failed run, its failure time, never the attempt's start.
- **CAP-2**
  - **intent:** A client sees when a run was cancelled as its `updated_at`, including a run cancelled while sleeping.
  - **success:** A cancelled run (never started, cancelled mid-attempt, or cancelled while sleeping with a future wake time) reports its cancellation time, not its wake time.
- **CAP-3**
  - **intent:** No run reports a future `updated_at`.
  - **success:** A pending run whose `available_at` is in the future reports the later of its `enqueue_at` and its attempt row's creation time; sleeping, running, and terminal runs never exceed the present.
- **CAP-4**
  - **intent:** Every surface agrees on `updated_at`.
  - **success:** For the same run, `list_runs`, `get_run`, and both routes return equal `updated_at`, for runs from all four workflow queues (workflow, slack-live, etl, etl-backfill).

## Constraints

- Values come from durable Absurd task/run columns in Postgres (`completed_at`, `failed_at`, `cancelled_at`, `started_at`, `available_at`, `created_at`, `enqueue_at`); no process-local state and no migration.
- `updated_at` for a finished run uses the same source as the run's existing `completed_at` field, floored at `enqueue_at`, so the two are equal for finished runs.
- Running runs keep claim time (Absurd sets `available_at = started_at` at claim); sleeping and infinite-wait runs keep their existing rule (`greatest(enqueue_at, coalesce(started_at, created_at))`).
- Pending runs keep `available_at` when it is not in the future (a woken run's wake time or the enqueue time); otherwise they use the rule in CAP-3. `now()` clamping is out: `updated_at` must not drift between reads.
- `list_runs` and `get_run` compute `updated_at` in two duplicated SQL queries (`centaur-workflows/src/lib.rs`); both change together so surfaces cannot diverge.
- Field names, types, RFC 3339 format, `created_at`, `started_at`, `completed_at`, and list ordering (`created_at` desc) stay unchanged.
- Existing tests may change where the design needs it: `completed_run` and `pending_run` expectations in `centaur-api-server/tests/sleeping_updated_at.rs` and `wait_updated_at.rs` pin the old behavior; the CI oracle flags such edits for human review.
- Proof is by new Rust test files under `services/api-rs/crates/*/tests/` that seed Absurd tables and read back through existing items (`WorkflowRuntime::list_runs`/`get_run`, the routes via `build_router_with_session_and_workflow_runtime`); each fails before its story's change on a wrong value, not a missing item. Database-backed tests skip without a database URL.

## Non-goals

- Changing list ordering, `created_at`, `started_at`, or `completed_at`.
- Adding fields, per-attempt history, or schema migrations.
- Updating `packages/api-client` types or any UI.
- Changing sleeping or infinite-wait `updated_at` rules.

## Success signal

Rust tests show completed, failed, and cancelled runs (including one cancelled while sleeping) return their finish time as `updated_at`, and a pending run with a far-future `available_at` returns no future time, identically from list and get on every queue.

## Assumptions

- A cancelled run's last real change is the cancellation request time (`cancelled_at`), even if the attempt was still winding down.
- No repo consumer of workflow-run `updated_at` (client types, UI) needs changing.

## Open Questions

- Pending runs with a future `available_at` (scheduled start or retry delay): is the later of `enqueue_at` and the attempt row's `created_at` right, or should a retry-pending run report the previous attempt's failure time?
- Should `list_runs` ordering follow `updated_at` instead of `created_at`? The brief does not ask; left unchanged.
- Should `packages/api-client` types or docs describing `updated_at` be updated?
