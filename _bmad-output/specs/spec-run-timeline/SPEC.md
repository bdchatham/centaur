---
id: SPEC-run-timeline
companions: []
sources: []
---

> **Canonical contract.** This SPEC and the files in `companions:` are the complete, preservation-validated contract for what to build, test, and validate. Source documents listed in frontmatter are for traceability — consult them only if you need narrative rationale or prose color this contract intentionally omits.

# Workflow run timeline endpoint

## Why

A pain to solve: an on-call engineer looking at a retried workflow run can list its lifecycle events, but not see where the run lost time: how long it waited, ran, or sat between retries. A timeline endpoint groups a run's lifecycle events by attempt and states the time between consecutive events, so the slow attempt or the long wait before a retry shows at a glance. It builds on the lifecycle-events endpoint (`spec-run-events`) and reuses its event entries unchanged.

## Capabilities

- **CAP-1**
  - **intent:** A client can fetch the timeline of one workflow run.
  - **success:** `GET /api/workflows/runs/{run_id}/timeline` for an existing run returns 200 with `{"ok": true, "attempts": [...]}`, for runs on all four queues (workflow, slack-live, etl, etl-backfill); for an unknown run it returns 404 with `ok` false and the same error body as the full run endpoint.
- **CAP-2**
  - **intent:** The timeline is grouped by attempt.
  - **success:** `attempts` holds one group per distinct `attempt` value among the run's events, ascending, each group `{"attempt": n, "events": [...]}` (plus the CAP-3 fields) with `events` holding exactly the events whose `attempt` is `n`, in the events reader's order; a run retried twice has groups 1, 2 and 3, and no event is in two groups or in none.
- **CAP-3**
  - **intent:** A client can see how long the run spent between events, per attempt.
  - **success:** Each group has `intervals`, one `{"from": <kind>, "to": <kind>, "millis": <integer>}` per consecutive pair of the run's events in reader order (`from` the earlier event's `kind`, `to` the later one's, `millis` the difference of their `at` values in whole milliseconds), placed in the group of the later event, so the wait between a failed attempt and its retry sits in the retried attempt's group; each group has `duration_millis`, the sum of its intervals' `millis`; the first group's first event has no interval.
- **CAP-4**
  - **intent:** The timeline reuses the lifecycle-events entries unchanged.
  - **success:** For any seeded run, the concatenation of the groups' `events` equals the `events` array of `GET /api/workflows/runs/{run_id}/events` for the same run, entry for entry and key for key; the timeline never adds, drops or renames a key of an entry.
- **CAP-5**
  - **intent:** Another route can return the same timeline without redefining it.
  - **success:** One public reader on the workflow runtime returns a run's timeline in a type that serializes to the `attempts` array above, built from the events reader's entries, and answers an unknown run with the not-found error `get_run` raises; the route returns that reader's value unchanged inside `{ok, attempts}`, and a test calls the reader directly.

## Constraints

- Dependency: the events reader (`spec-run-events`) gives every entry exactly `kind`, `at` and `attempt`, `attempt` being a positive integer, the number of the attempt the event belongs to. This brief relies on `attempt` and must not add fields to the event entries itself; the timeline cannot be built until that reader exists.
- Attempts are never inferred by counting `retried` events; the grouping key is the entry's own `attempt`.
- Additive only: existing routes, `WorkflowRun`, the list and get responses and the events endpoint keep their shape and semantics; no migration.
- The timeline is computed from the events reader's output on each request: no process-local source of truth, no new table, no second query for event times.
- The route needs a `route_access` entry mapped to `Capability::WorkflowsRead`, like the sibling GET run routes; without one every caller is denied.
- The timeline shows event kinds, timestamps, attempt numbers and durations only: never inputs, results, failure payloads, failure messages, or run and task ids.
- Proof is by new Rust test files under `services/api-rs/crates/centaur-api-server/tests/`. Each test fails before its story's change, because the route is missing or the fields are missing, or fails to compile because it calls the public reader the change adds; apart from that reader it uses only existing items (`build_router_with_session_and_workflow_runtime`, a console token, direct seeding of the Absurd `t_<queue>` and `r_<queue>` tables as `run_timestamps.rs` does) and asserts on HTTP status and JSON. Database-backed tests skip without a database URL. Existing tests may be edited where the design needs it; the CI oracle flags such edits for human review.

## Non-goals

- Changing, extending or reshaping the event entries, or the events endpoint itself.
- Attempt-level outcomes, failure messages or step data (the attempts and steps endpoints own those).
- Percentiles, aggregation across runs, thresholds or alerting on slow attempts.
- Pagination, filters, or a `fields` selector.
- A persisted timeline or any schema migration.
- `packages/api-client` types or any UI.

## Success signal

Rust tests show that, for runs on each queue, the timeline endpoint returns the run's events grouped by attempt, byte-identical to the events endpoint's entries, with the right millisecond interval between each pair of consecutive events (including the wait before a retry) and per-attempt totals, that the public reader returns the same timeline, and that an unknown run returns 404.

## Assumptions

- The body follows the sibling routes' envelope: `{"ok": true, "attempts": [...]}`.
- `{run_id}` is a run row id as on the full endpoint; any attempt's run id returns the whole timeline of its task.
- Per `spec-run-events`, a `retried` event carries the attempt it starts (k at least 2), `created` and `started` carry 1 and a terminal event carries the task's last attempt; a run cancelled while a retry is pending has its `retried` and `cancelled` events in the same group.
- `millis` is never negative because the events reader orders events by `at`; a group of a single event with no predecessor in the run has an empty `intervals` and `duration_millis` 0.

## Open Questions

- Envelope: `{"ok": true, "attempts": [...]}` (assumed) or a bare list?
- `get_run` casts the id to uuid, so a malformed run id likely yields a 500 on the full endpoint today; should `/timeline` answer 404 or 400 for it?
