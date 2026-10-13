---
id: SPEC-run-timestamps
companions: []
sources: []
---

> **Canonical contract.** This SPEC and the files in `companions:` are the complete, preservation-validated contract for what to build, test, and validate. Source documents listed in frontmatter are for traceability — consult them only if you need narrative rationale or prose color this contract intentionally omits.

# Workflow run start and completion timestamps

## Why

A pain to solve: clients of the api-rs workflow-runs API (`GET /api/workflows/runs`, `GET /api/workflows/runs/{run_id}`) cannot tell when a run began executing or finished. `updated_at` is derived as `greatest(enqueue_at, available_at)`, so durations have to be guessed from it. Exposing the real timestamps lets clients show run durations.

## Capabilities

- **CAP-1**
  - **intent:** A client can see when each workflow run started executing.
  - **success:** Every run object returned by the API has `started_at`: an RFC 3339 timestamp once execution has begun, `null` before.
- **CAP-2**
  - **intent:** A client can see when each workflow run finished.
  - **success:** Every run object has `completed_at`: an RFC 3339 timestamp once the run is terminal, `null` until then.
- **CAP-3**
  - **intent:** The two fields behave identically wherever a run is returned.
  - **success:** For the same run, list and get return equal `started_at`/`completed_at`, for runs from all four workflow queues (workflow, slack-live, etl, etl-backfill).

## Constraints

- Additive only: existing run fields, including `updated_at`, keep their names, types, and semantics.
- The new fields are always serialized, as `null` when unset (never omitted), in RFC 3339 like `created_at`.
- Values come from durable Absurd task/run state in Postgres (columns `first_started_at`, `started_at`, `completed_at`, `failed_at`, `cancelled_at` already exist); no process-local source of truth, and no migration unless those columns prove insufficient.
- `started_at` is the first time any attempt began executing, so a retried run keeps its original start; `null` while the run has never been claimed.
- `WorkflowRun` keeps its fields, so every existing `WorkflowRun { .. }` literal compiles unchanged. The timestamps ride on a new response type that `#[serde(flatten)]`s `WorkflowRun` and adds `started_at`/`completed_at`; the list and get routes return that type.
- No existing test is edited. Proof is by new Rust tests in new files under the `tests/` directory of each touched `services/api-rs` crate; database-backed tests skip without a database URL.

## Non-goals

- Changing `updated_at` semantics.
- Adding duration fields, per-attempt history, or aggregate metrics.
- Schema migration or backfill of existing rows.
- Updating `packages/api-client` types or any UI.

## Success signal

Rust tests show a pending run returns both fields `null`, a running run returns `started_at` set and `completed_at` `null`, and completed, failed, and cancelled runs return `completed_at` set, identically from the list and get endpoints.

## Assumptions

- `completed_at` is set when the run reaches a terminal state (completed, failed, cancelled) and is `null` otherwise, including while a failed attempt awaits retry.
- A run cancelled before it ever started has `started_at` null and `completed_at` set.
- TypeScript client and other consumers are out of scope.

## Open Questions

- Retries: is `started_at` the first attempt's start (assumed) or the latest attempt's? Should `completed_at` cover the whole task or only the last attempt?
- Cancelled runs: should `completed_at` equal the cancellation time (`cancelled_at`), or stay `null` because the run did not finish?
- Do clients tolerate `completed_at` set while `started_at` is null (cancelled before start)?
- Should `packages/api-client` types be updated as part of this work?
