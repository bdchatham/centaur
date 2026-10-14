---
id: SPEC-sleeping-updated-at
companions: []
sources: []
---

> **Canonical contract.** This SPEC and the files in `companions:` are the complete, preservation-validated contract for what to build, test, and validate. Source documents listed in frontmatter are for traceability — consult them only if you need narrative rationale or prose color this contract intentionally omits.

# Workflow run updated_at never reports a future wake time

## Why

A pain to solve: the api-rs workflow-runs queries (`GET /api/workflows/runs`, `GET /api/workflows/runs/{run_id}`) compute `updated_at = greatest(enqueue_at, available_at)`. For a run that sleeps, waits on an event with a timeout, or backs off before a retry, Absurd stores the wake or retry time in `available_at`, so clients see runs "updated" hours from now and sort them wrongly. `updated_at` should say when the run last changed.

## Capabilities

- **CAP-1**
  - **intent:** A client can fetch a sleeping run (sleep, timed event wait, or retry backoff) and see when it last changed rather than when it will wake.
  - **success:** `get_run` and `GET /api/workflows/runs/{run_id}` return a run in state `sleeping` with a finite future `available_at` and `updated_at` equal to the later of `enqueue_at` and the run's `started_at` (or its `created_at` when never started).
- **CAP-2**
  - **intent:** Listing reports the same last-change time as fetching.
  - **success:** For the same sleeping run, `list_runs` and `GET /api/workflows/runs` return an `updated_at` equal to the get value, for all four queues (workflow, slack-live, etl, etl-backfill).
- **CAP-3**
  - **intent:** Wake times no longer distort ordering by `updated_at`.
  - **success:** With a sleeping run whose wake time is far in the future seeded beside other runs, its `updated_at` equals its seeded last-change time, never precedes its `enqueue_at`, and sorts among the other runs by that time.

## Constraints

- Runs not in state `sleeping` keep today's `updated_at` value exactly (pending, running, completed, failed, cancelled).
- The fix is read-side, in the list and get queries in `centaur-workflows` for all four queues. Absurd's stored `available_at` and its migrations stay as they are.
- `WorkflowRun` field names, types, and non-null RFC 3339 `updated_at` are unchanged; no new fields; a non-finite `available_at` stays handled.
- A sleeping run's last real change is `greatest(enqueue_at, coalesce(run.started_at, run.created_at))`: the claim time of the attempt that went to sleep, or, for a retry backoff, the creation time of the new run (the failure time). It generalizes the existing no-timeout (`infinity`) branch.
- Proof is by new Rust tests in a new file under `services/api-rs/crates/centaur-api-server/tests/`, seeding Absurd rows directly as `wait_updated_at.rs` does; database-backed tests skip without a database URL.
- Each new test must compile before the change and fail only because of it (wrong `updated_at`), so it calls only existing items (`WorkflowRuntime::list_runs`/`get_run`, the routes via `build_router_with_session_and_workflow_runtime`) and asserts on their output. Existing tests may be edited where the design needs it; the CI oracle flags such edits for human review.

## Non-goals

- Changing how Absurd stores sleeps, waits, or retries.
- Changing `updated_at` for pending, running, completed, failed, or cancelled runs.
- Adding fields, or updating `packages/api-client` types or any UI.

## Success signal

Rust tests show that sleeping runs seeded with a far-future finite `available_at` (a sleep or timed wait started earlier, and a retry-backoff run) return, from both list and get on every queue, an `updated_at` equal to the specified last-change time, while pending and completed runs seeded beside them return the same values as before.

## Assumptions

- Absurd never produces a `pending` run with a future `available_at` (a retry due in the future is `sleeping`), so the synthetic 2099 pending seed in `wait_updated_at.rs` is out of scope and stays valid.
- The attempt's `started_at` is an acceptable proxy for when a sleep or timed wait began; Absurd does not record that moment on the run row.

## Open Questions

- A run cancelled while sleeping keeps its wake time in `available_at` and still reports it as `updated_at` (a future time). Should cancelled runs report `cancelled_at` instead?
- Should a waiting run's `updated_at` be the moment the wait began (the Absurd waits table `created_at`, event waits only) rather than the attempt's start?
- Should completed and failed runs report their completion time rather than claim time (today's `available_at`)?
- Do `packages/api-client` or the UI need changes for how they sort or show `updated_at`?
