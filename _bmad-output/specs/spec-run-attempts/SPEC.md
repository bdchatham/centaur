---
id: SPEC-run-attempts
companions: []
sources: []
---

> **Canonical contract.** This SPEC and the files in `companions:` are the complete, preservation-validated contract for what to build, test, and validate. Source documents listed in frontmatter are for traceability — consult them only if you need narrative rationale or prose color this contract intentionally omits.

# Workflow run attempts endpoint

## Why

A pain to solve: a client of the api-rs workflow-runs API cannot see how often a run was retried. `GET /api/workflows/runs/{run_id}` reports only an `attempts` count and the whole run, including input, result and failure payloads. An attempt-history endpoint lists each attempt with its number, timing, outcome and, for a failed attempt, its failure message, oldest first, without any other payload. The `retry-report` brief reuses these entries unchanged, so they carry the failure message and come from a shared reader.

## Capabilities

- **CAP-1**
  - **intent:** A client can fetch the attempt history of one workflow run.
  - **success:** `GET /api/workflows/runs/{run_id}/attempts` for an existing run returns 200 with a list in which each entry holds exactly `attempt`, `started_at`, `ended_at`, `outcome` and `error`, one entry per attempt of that run's task, for runs on all four queues (workflow, slack-live, etl, etl-backfill).
- **CAP-2**
  - **intent:** The history reads oldest first.
  - **success:** Entries are ordered by `attempt` ascending, where `attempt` is the attempt's own number; a run retried twice lists attempts 1, 2, 3 whatever order the rows were stored in.
- **CAP-3**
  - **intent:** A client can tell what became of each attempt.
  - **success:** `outcome` is `succeeded` for a completed attempt, `failed` for a failed one, `cancelled` for a cancelled one, and `running` for an attempt that has not ended (pending, running or sleeping), whose `ended_at` is `null`; `started_at` and `ended_at` are RFC 3339 strings or `null`.
- **CAP-4**
  - **intent:** A client can see retries without fetching inputs, results or failure payloads.
  - **success:** No entry contains the run's input or result, nor any failure payload content other than `error`, nor run or task ids: a run seeded with distinctive values in those fields, and a failure payload with other members besides `message`, never shows them anywhere in the response body.
- **CAP-5**
  - **intent:** A client learns that a run does not exist.
  - **success:** `GET /api/workflows/runs/{run_id}/attempts` for an unknown run returns 404 with `ok` false and the same error body as the full run endpoint.
- **CAP-6**
  - **intent:** A client can tell why an attempt failed.
  - **success:** `error` is the string `message` member of the attempt's own `failure_reason` when `outcome` is `failed`, and `null` for every other outcome and for a failed attempt whose `failure_reason` is null or has no string `message`; a run retried after failures lists each failed attempt's own message.
- **CAP-7**
  - **intent:** Another route can return the same attempt entries without redefining them.
  - **success:** One public reader on the workflow runtime returns a run's entries, oldest first, in an entry type that serializes to exactly the five keys, and answers an unknown run with the not-found error `get_run` raises; the attempts route returns that reader's entries unchanged, and a test calls the reader directly.

## Constraints

- Additive only: existing routes, `WorkflowRun`, and the list and get responses keep their shape and semantics; no migration.
- Attempts are the rows of the run's task in `absurd.r_<queue>`, found on the queue that holds `{run_id}` as `get_run` does; `attempt` is the row's `attempt` column. The history is every attempt of that task, whichever attempt's run id is given.
- `started_at` is the row's `started_at`. `ended_at` is the row's `completed_at` or `failed_at`; for a cancelled attempt, which `absurd.cancel_task` marks without setting any run timestamp, it is the task's `cancelled_at`, as the run's `completed_at` is derived in `get_run_for_queue`.
- `error` is read from the attempt's own `absurd.r_<queue>.failure_reason` (jsonb), only its string `message` member, only when the attempt `failed`. An entry carries none of the run's input or result, no other part of the failure, and no run or task id.
- The entry shape is fixed for sharing: the reader's entry type serializes to exactly `attempt`, `started_at`, `ended_at`, `outcome`, `error`; the route adds nothing beyond the `{ok, attempts}` envelope, and a reusing route may not add fields.
- The new route needs a `route_access` entry mapped to `Capability::WorkflowsRead`, like the sibling GET run routes; without one every caller is denied.
- Values come from durable Absurd state in Postgres; no process-local source of truth.
- Proof is by new Rust test files under `services/api-rs/crates/centaur-api-server/tests/`. Each test fails before the change, because the route is missing or behaves differently, or fails to compile because it calls the public reader the change adds; apart from that reader it uses only existing items (`build_router_with_session_and_workflow_runtime`, a console token, direct seeding of the Absurd `t_<queue>` and `r_<queue>` tables as `run_timestamps.rs` does) and asserts on HTTP status and JSON. Database-backed tests skip without a database URL. Existing tests may be edited where the design needs it; the CI oracle flags such edits for human review.

## Non-goals

- Inputs, results, failure payloads other than a failed attempt's `message`, claim or worker details, or step data in the response.
- Pagination, filters, or a `fields` selector.
- Attempt history on the full run or list responses.
- `packages/api-client` types or any UI.
- Caching.
- Special handling of a malformed (non-UUID) run id.

## Success signal

Rust tests show that, for runs on each queue, the attempts endpoint lists exactly one entry per attempt with the five keys, oldest first, with the right outcome, `ended_at` and `error` for succeeded, failed, cancelled and running attempts, with no payload content beyond a failed attempt's message, that the public reader returns the same entries, and that an unknown run returns 404.

## Assumptions

- The body follows the sibling routes' envelope: `{"ok": true, "attempts": [...]}`.
- `{run_id}` is a run row id as on the full endpoint; any attempt's run id returns the whole history of its task.
- A failed attempt with no string `message` in its `failure_reason` reports `error` null, since the brief gives no other fallback.
- An attempt queued for retry but not yet started, or sleeping, reports `running` with `ended_at` null (and `started_at` null if it never started), since the brief names no fifth outcome.

## Open Questions

- Should a queued-but-unstarted or sleeping attempt be reported as `running` (specified), or left out until it first starts?
- Given an earlier attempt's run id, should the endpoint list all attempts of the task (specified) or only those up to that attempt, as `spec-run-steps` does for checkpoints?
- Envelope: `{"ok": true, "attempts": [...]}` (assumed) or a bare list?
- An attempt cancelled while queued has no run timestamps, so its `ended_at` is the task's `cancelled_at` and its `started_at` is null; is that acceptable?
- `get_run` casts the id to uuid, so a malformed run id likely yields a 500 on the full endpoint today; should `/attempts` answer 404 or 400 for it?
- Should `error` be passed through verbatim (specified) or truncated or sanitised, since a failure message could carry sensitive text?
