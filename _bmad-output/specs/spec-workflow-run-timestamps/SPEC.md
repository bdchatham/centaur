---
id: SPEC-workflow-run-timestamps
companions: []
sources: []
---

> **Canonical contract.** This SPEC and the files in `companions:` are the complete, preservation-validated contract for what to build, test, and validate. Source documents listed in frontmatter are for traceability — consult them only if you need narrative rationale or prose color this contract intentionally omits.

# Workflow run start and completion timestamps

## Why

API clients (operators and tooling reading the api-rs workflow-runs API) cannot show how long a workflow run took: `WorkflowRun` exposes only `created_at` and `updated_at`, and `updated_at` is an approximation (`greatest(enqueue_at, coalesce(available_at, enqueue_at))`), not a start or finish time. The underlying Absurd tables already record start and completion times; they are not surfaced.

## Capabilities

- **CAP-1**
  - **intent:** A client reading any workflow run can see `started_at` and `completed_at` as explicit fields.
  - **success:** Serializing a `WorkflowRun` always emits both keys, as RFC3339 strings (same format as `created_at`) or JSON `null`; a Rust unit test asserts both forms.
- **CAP-2**
  - **intent:** A client can tell when a run began executing and when it finished, with null meaning "not yet".
  - **success:** A queued run reports `started_at: null` and `completed_at: null`; a running run reports `started_at` set and `completed_at: null`; a completed run reports both set with `completed_at >= started_at`. Verified by Rust tests of the row-to-run mapping and query output.
- **CAP-3**
  - **intent:** Every workflow-runs endpoint that returns a run exposes the fields, regardless of which workflow queue the run lives in.
  - **success:** `GET /api/workflows/runs` items and `GET /api/workflows/runs/{run_id}` `run` both contain `started_at` and `completed_at`, for runs from all four workflow queues; a Rust test at the route or runtime level asserts it.

## Constraints

- Change is additive: existing `WorkflowRun` fields, including `updated_at`'s derivation, and the response envelopes (`{ok, runs}`, `{ok, run}`) stay unchanged.
- Values come from stored Absurd timestamps, never from `updated_at` or read-time clock, so repeated reads of an unchanged run return identical values.
- Proof is by Rust tests in `services/api-rs`; DB-backed tests in this repo skip without a test database URL, so the mapping and serialization logic must be testable without Postgres.

## Non-goals

- A computed duration field.
- Changing `updated_at`, sorting, or filtering by the new fields.
- Changes to `CreateWorkflowRunResponse`, the TypeScript api-client, or any UI.
- A schema migration (the Absurd columns already exist) unless they prove insufficient.

## Success signal

- A client calling the list or get endpoint for a finished workflow run computes its duration as `completed_at - started_at`, and sees `null` for either field on a run that has not reached that point.

## Assumptions

- The observed API is the Rust `WorkflowRun` JSON in `services/api-rs/crates/centaur-workflows/src/lib.rs`, served by `centaur-api-server` routes; field type is `Option<OffsetDateTime>` serialized RFC3339.
- `completed_at` stays null while a run is non-terminal (retrying, sleeping, awaiting an event).

## Open Questions

- On a retried run, is `started_at` the task's `first_started_at` (first attempt began) or the latest attempt's `r.started_at`?
- For failed and cancelled runs, is `completed_at` set (from `r.failed_at` / `t.cancelled_at`), or only for successful completion?
- Do the TypeScript api-client run types (`packages/api-client`) need the new fields? Left out of scope pending confirmation.
