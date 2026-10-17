---
id: SPEC-run-events
companions: []
sources: []
---

> **Canonical contract.** This SPEC and the files in `companions:` are the complete, preservation-validated contract for what to build, test, and validate. Source documents listed in frontmatter are for traceability — consult them only if you need narrative rationale or prose color this contract intentionally omits.

# Workflow run events endpoint

## Why

A pain to solve: a client of the api-rs workflow-runs API that wants a run's history at a glance has to fetch the whole run, including input, result and failure payloads, and infer from `attempts`, `started_at` and `completed_at` what happened and when. A lifecycle-event endpoint lists each event of one run as a kind and a timestamp, oldest first, with nothing else.

## Capabilities

- **CAP-1**
  - **intent:** A client can fetch the lifecycle events of one workflow run.
  - **success:** `GET /api/workflows/runs/{run_id}/events` for an existing run returns 200 with `{"ok": true, "events": [...]}` in which each entry holds exactly `kind` and `at`, for runs on all four queues (workflow, slack-live, etl, etl-backfill).
- **CAP-2**
  - **intent:** The history reads oldest first.
  - **success:** Entries are ordered by `at` ascending, ties broken by lifecycle order (created, started, retried, then the terminal event); a run with every event lists them in that order whatever order the rows were stored in.
- **CAP-3**
  - **intent:** A client can see when a run was created and when it began executing.
  - **success:** Every run has a `created` event; a run that has been claimed has a `started` event, one that has not has none; `kind` is the lowercase string and `at` an RFC 3339 string.
- **CAP-4**
  - **intent:** A client can see each retry.
  - **success:** A run whose task has attempts 1..n lists n-1 `retried` events, one per attempt after the first; a failed attempt that was retried yields `retried`, not `failed`.
- **CAP-5**
  - **intent:** A client can see how and when a run ended.
  - **success:** A completed run has a `completed` event, a failed run a `failed` event and a cancelled run a `cancelled` event, each at the run's `completed_at`; a run that has not ended has none of the three, and no run has more than one.
- **CAP-6**
  - **intent:** A client gets the history without inputs, results or failure payloads.
  - **success:** A run seeded with distinctive values in input, result and failure payload, and with distinctive run and task ids, never shows them anywhere in the response body, and no entry has an attempt number or any key other than `kind` and `at`.
- **CAP-7**
  - **intent:** A client learns that a run does not exist.
  - **success:** `GET /api/workflows/runs/{run_id}/events` for an unknown run returns 404 with `ok` false and the same error body as the full run endpoint.
- **CAP-8**
  - **intent:** Another route can return the same events without redefining them.
  - **success:** One public reader on the workflow runtime returns a run's events, oldest first, in an event type that serializes to exactly `kind` and `at`, and answers an unknown run with the not-found error `get_run` raises; the route returns that reader's events unchanged, and a test calls the reader directly.

## Constraints

- Additive only: existing routes, `WorkflowRun`, and the list and get responses keep their shape and semantics; no migration.
- Events are derived from durable Absurd state in Postgres, never from a process-local source and never from a new table. `created` is the task's `enqueue_at`; `started` is its `first_started_at` when set; `retried` is the `created_at` of each `absurd.r_<queue>` row of the task with `attempt` above 1, which `absurd.fail_run` inserts when it records the failure; `completed`, `failed` and `cancelled` carry the run's `completed_at` as `get_run_for_queue` derives it (the last attempt's `completed_at` or `failed_at`, else the task's `cancelled_at`), and only when the task's state is `completed`, `failed` or `cancelled` respectively.
- The history is that of the whole task, found on the queue that holds `{run_id}` as `get_run` does, whichever attempt's run id is given.
- The entry shape is fixed for sharing: the reader's event type serializes to exactly `kind` and `at`, with `kind` one of `created`, `started`, `retried`, `completed`, `failed`, `cancelled`; the route adds nothing beyond the `{ok, events}` envelope, and a reusing route may not add fields.
- The new route needs a `route_access` entry mapped to `Capability::WorkflowsRead`, like the sibling GET run routes; without one every caller is denied.
- Proof is by new Rust test files under `services/api-rs/crates/centaur-api-server/tests/`. Each test fails before its story's change, because the route is missing or the events are missing, or fails to compile because it calls the public reader the change adds; apart from that reader it uses only existing items (`build_router_with_session_and_workflow_runtime`, a console token, direct seeding of the Absurd `t_<queue>` and `r_<queue>` tables as `run_timestamps.rs` does) and asserts on HTTP status and JSON. Database-backed tests skip without a database URL. Existing tests may be edited where the design needs it; the CI oracle flags such edits for human review.

## Non-goals

- Inputs, results, failure payloads, failure messages, attempt numbers, run or task ids, or worker details in the response.
- Pagination, filters, or a `fields` selector.
- Events on the full run or list responses.
- Sleeping, waiting or other intermediate states as events.
- A persisted event log or any schema migration.
- `packages/api-client` types or any UI.

## Success signal

Rust tests show that, for runs on each queue, the events endpoint lists exactly the events of a run's life (created; started; one retried per retry; and completed, failed or cancelled once it has ended) as `kind` and `at` only, oldest first, with no payload content, that the public reader returns the same events, and that an unknown run returns 404.

## Assumptions

- The body follows the sibling routes' envelope: `{"ok": true, "events": [...]}`.
- `{run_id}` is a run row id as on the full endpoint; any attempt's run id returns the whole history of its task.
- A `retried` event is timed at the retry row's `created_at`, when the retry was scheduled, not when the retry attempt began.
- A run cancelled before it started lists `created` and `cancelled` only; a run that failed without retries lists `created`, `started`, `failed`.
- An event whose timestamp is null in storage is omitted rather than serialized with a null `at`.

## Open Questions

- Should `retried` be timed when the retry is scheduled (specified) or when the retry attempt first starts?
- Given an earlier attempt's run id, should the endpoint list all events of the task (specified) or only those up to that attempt?
- Envelope: `{"ok": true, "events": [...]}` (assumed) or a bare list?
- A run cancelled while a retry was pending lists `retried` then `cancelled`; is that the intended reading?
- `get_run` casts the id to uuid, so a malformed run id likely yields a 500 on the full endpoint today; should `/events` answer 404 or 400 for it?
