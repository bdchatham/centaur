---
id: SPEC-retry-report
companions: []
sources: []
---

> **Canonical contract.** This SPEC and the files in `companions:` are the complete, preservation-validated contract for what to build, test, and validate. Source documents listed in frontmatter are for traceability — consult them only if you need narrative rationale or prose color this contract intentionally omits.

# Workflow run retry report endpoint

## Why

A pain to solve: an on-call engineer looking at a retried workflow run cannot tell whether the retries hit the same error. `GET /api/workflows/runs/{run_id}` shows only the last failure, and the planned attempts endpoint (`spec-run-attempts`) shows outcomes without reasons. A retry-report endpoint lists every attempt of the run with why it failed, so identical or differing errors are visible at a glance.

## Capabilities

- **CAP-1**
  - **intent:** An on-call engineer can fetch the retry report of one workflow run.
  - **success:** `GET /api/workflows/runs/{run_id}/retry-report` for an existing run returns 200 with `attempts`, one entry per attempt of that run's task, oldest first, on all four queues (workflow, slack-live, etl, etl-backfill). Each entry equals, key for key and value for value, the entry `GET /api/workflows/runs/{run_id}/attempts` returns for the same attempt.
- **CAP-2**
  - **intent:** The engineer can see why each attempt failed and compare attempts.
  - **success:** The `error` of a failed attempt's entry is that attempt's failure message; the `error` of every other entry is `null`. A run seeded with two attempts failing with the same message and a third failing with a different one shows equal `error` on the first two and a different `error` on the third.
- **CAP-3**
  - **intent:** The engineer learns that a run does not exist.
  - **success:** `GET /api/workflows/runs/{run_id}/retry-report` for an unknown run returns 404 with `ok` false and the same error body as the full run endpoint.
- **CAP-4**
  - **intent:** The report exposes failure messages without leaking run data.
  - **success:** The body holds nothing beyond `ok` and the entries; a run seeded with distinctive input, result and failure-detail values (failure members other than the message) never shows them in the response body.

## Constraints

- Entries are reused unchanged from the attempt-history endpoint's brief (`bafyreibwwwhnsgqofa6lcxtma4h6tvkjzopvu26kbjp3mfm27qzqhqryme`, spec `spec-run-attempts`): this spec defines no entry field and adds none to the attempt history. The report needs each entry to carry `error` (the attempt's failure message, null unless it failed), which that spec's exact four-key entry (`attempt`, `started_at`, `ended_at`, `outcome`; its CAP-1, CAP-4 and story 1) rules out, so that spec must carry `error` itself.
- The report and `/attempts` build entries from one shared reader, so they cannot drift; no second query path over `absurd.r_<queue>`.
- `{run_id}` is looked up as `get_run` does, across the four queues; any attempt's run id returns the whole history of its task.
- The new route needs a `route_access` entry mapped to `Capability::WorkflowsRead`, like the sibling GET run routes; without one every caller is denied.
- Values come from durable Absurd state in Postgres (`failure_reason` of `absurd.r_<queue>`); no process-local source of truth.
- Additive only: existing routes, `WorkflowRun`, and the list and get responses keep their shape; no migration.
- Proof is by new Rust test files under `services/api-rs/crates/centaur-api-server/tests/`, HTTP requests and JSON assertions only, using existing items (`build_router_with_session_and_workflow_runtime`, a console token, direct seeding of the Absurd `t_<queue>` and `r_<queue>` tables as `run_timestamps.rs` does). Each test fails before the change because the route is missing; database-backed tests skip without a database URL. Existing tests may be edited where the design needs it; the CI oracle flags such edits for human review.

## Non-goals

- Adding any field to the attempt history, or changing its entries, from this brief.
- Run input, result or failure payloads beyond the failure message; claim, worker or step data.
- A report-level summary, verdict, grouping or diff of errors.
- Pagination, filters, or a `fields` selector.
- Report data on the full run or list responses.
- `packages/api-client` types or any UI.
- Special handling of a malformed (non-UUID) run id.

## Success signal

Rust tests show that, for a run retried twice on each queue, the retry-report lists each attempt oldest first with the same entries as `/attempts`, with `error` equal to the failure message on failed attempts and null otherwise, with no payload content, and that an unknown run returns 404.

## Assumptions

- The body follows the sibling routes' envelope: `{"ok": true, "attempts": [...]}`.
- `error` is the string `message` member of the attempt's `failure_reason` (the SDK writes failures as `{name, message}`); it is `null` if the attempt did not fail or its failure has no string message.
- A run that was never retried still returns 200 with its single entry; only an unknown run is 404.
- All attempts are listed, not only failed ones, so the final outcome stays visible beside the earlier errors.

## Open Questions

- Should the report add a summary (for example a `same_error` boolean) so the engineer need not compare entries, or is the entry list enough (specified)?
- Should `error` be the failure's `message` only (specified) or include its `name` (for example `TimeoutError: ...`)?
- Should a very long failure message be truncated?
- `get_run` casts the id to uuid, so a malformed run id likely yields a 500 today; should `/retry-report` answer 404 or 400 for it?
- The attempts spec and its story 1 currently forbid an `error` key; this spec depends on that spec being revised to carry it (challenge raised).
