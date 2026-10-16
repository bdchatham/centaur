---
id: SPEC-run-steps
companions: []
sources: []
---

> **Canonical contract.** This SPEC and the files in `companions:` are the complete, preservation-validated contract for what to build, test, and validate. Source documents listed in frontmatter are for traceability — consult them only if you need narrative rationale or prose color this contract intentionally omits.

# Workflow run steps endpoint

## Why

A pain to solve: a client debugging a workflow run in the api-rs workflow-runs API can see its status but not which steps it got through. `GET /api/workflows/runs/{run_id}` returns the whole run, including input, result and failure payloads, which a debugger does not want to fetch just to see progress. A step-history endpoint lists the steps a run completed, oldest first, without any step payloads.

## Capabilities

- **CAP-1**
  - **intent:** A client can fetch the step history of one workflow run.
  - **success:** `GET /api/workflows/runs/{run_id}/steps` for an existing run returns 200 with a list in which each entry holds exactly `name`, `state`, `started_at` and `completed_at`, one entry per committed step of that run, for runs on all four queues (workflow, slack-live, etl, etl-backfill).
- **CAP-2**
  - **intent:** The history reads in execution order.
  - **success:** Entries are ordered oldest first by `completed_at`, ties broken by `name` ascending; a run whose steps were committed out of name order is listed by commit time.
- **CAP-3**
  - **intent:** A client can debug a run without fetching step inputs or outputs.
  - **success:** No entry contains the checkpoint payload: a step whose stored payload is a distinctive value never shows that value anywhere in the response body.
- **CAP-4**
  - **intent:** A client learns that a run does not exist, and that a run with no steps has none.
  - **success:** `GET /api/workflows/runs/{run_id}/steps` for an unknown run returns 404 with the same error body as the full run endpoint; for an existing run with no committed steps it returns 200 with an empty list.

## Constraints

- Additive only: existing routes, `WorkflowRun`, and the list and get responses keep their shape and semantics; no migration (the Absurd checkpoint table `absurd.c_<queue>` already exists).
- Steps are the committed rows of the run's task in `absurd.c_<queue>`, the table the Absurd SDK reads to resume a run, restricted as `absurd.get_task_checkpoint_states` does: owner attempt not later than the requested run's attempt. `name` is the stored `checkpoint_name` (a repeated step name carries the SDK's `#N` suffix), `state` is the row's `status` string (not the payload), `completed_at` is the row's `updated_at` in RFC 3339 like the other run timestamps.
- `started_at` is always present and `null`: no step start time is stored, and recording one needs a migration and SDK change this brief does not ask for.
- An entry carries none of the checkpoint payload, run or task ids, input, result or failure.
- The new route needs a `route_access` entry mapped to `Capability::WorkflowsRead`, like the sibling GET run routes; without one every caller is denied.
- Values come from durable Absurd state in Postgres; no process-local source of truth.
- Proof is by new Rust test files under `services/api-rs/crates/centaur-api-server/tests/`. Each test compiles before the change and fails because the route is missing, so it uses only existing items (`build_router_with_session_and_workflow_runtime`, a console token, direct seeding of the Absurd queue tables including `c_<queue>`) and asserts on HTTP status and JSON. Database-backed tests skip without a database URL. Existing tests may be edited where the design needs it; the CI oracle flags such edits for human review.

## Non-goals

- Step payloads, inputs or outputs in the response.
- Real `started_at` values, in-progress or pending steps, and sleep or await-event wait rows (`absurd.w_<queue>`).
- Pagination, filters, or a `fields` selector.
- Steps on the full run or list responses.
- `packages/api-client` types or any UI.
- Caching.
- Special handling of a malformed (non-UUID) run id.

## Success signal

Rust tests show that, for runs on each queue, the steps endpoint lists exactly the committed steps with the four keys, oldest first, with no payload content, that a retried run sees only checkpoints up to its own attempt, that a run without steps returns an empty list, and that an unknown run returns 404.

## Assumptions

- The body follows the sibling routes' envelope: `{"ok": true, "steps": [...]}`.
- `{run_id}` is a run row id as on the full endpoint; the history is that run's task's checkpoints visible to that run (earlier-attempt checkpoints included), so an earlier attempt's run lists fewer steps than the latest run.
- Every listed step is complete: the SDK writes a checkpoint only when a step completes, so no running-step entry exists.
- `state` is therefore the checkpoint `status` (today always `committed`), not a richer step lifecycle.
- Sleep and await-event steps persist checkpoints too and are listed like any step.

## Open Questions

- `started_at`: no step start time is stored, so every entry returns `null`. Should the field be dropped, derived (for example the previous step's `completed_at`), or backed by a new column recorded when a step begins (migration plus `absurd-sdk` change)?
- `state`: does "state" mean the checkpoint `status` (`committed`) as specified, or a richer lifecycle (pending, running, completed) that would also list unfinished steps?
- Envelope: `{"ok": true, "steps": [...]}` (assumed) or a bare list?
- Should sleep and await-event checkpoints (for example `$awaitEvent:<event>`) be listed as ordinary steps, as specified, or distinguished or hidden?
- Should a retried run's history include earlier attempts' checkpoints (specified, matching the SDK's visibility rule) or only those owned by the requested run?
- `get_run` casts the id to uuid, so a malformed run id likely yields a 500 on the full endpoint today; should `/steps` answer 404 or 400 for it?
