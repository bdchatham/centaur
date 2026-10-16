---
id: SPEC-runs-by-state
companions: []
sources: []
---

> **Canonical contract.** This SPEC and the files in `companions:` are the complete, preservation-validated contract for what to build, test, and validate. Source documents listed in frontmatter are for traceability — consult them only if you need narrative rationale or prose color this contract intentionally omits.

# Filter workflow runs by state

## Why

A pain to solve: the only filter on `GET /api/workflows/runs` is `workflow_name`, so a client asking "is any run still live?" must page through every run. Operators and clients need to ask for runs in one state directly.

## Capabilities

- **CAP-1**
  - **intent:** A client can list only the workflow runs in a given state.
  - **success:** `GET /api/workflows/runs?state=<state>` returns only runs whose `status` equals `<state>`, for each of `pending`, `running`, `sleeping`, `completed`, `failed`, `cancelled`.
- **CAP-2**
  - **intent:** The state filter composes with the existing filters.
  - **success:** With `limit` and/or `workflow_name` also given, the response holds only runs matching all of them, and `limit` counts matching runs: a live run is returned even when more than `limit` newer runs in other states exist.
- **CAP-3**
  - **intent:** A client that sends an invalid state learns the valid ones.
  - **success:** An unknown `state` returns HTTP 400 whose message names all six valid states; a request without `state` returns what it returns today.

## Constraints

- The filter runs in SQL, per queue (workflow, slack-live, etl, etl-backfill), before `limit`; filtering a fetched page in Rust would hide live runs behind newer finished ones.
- Valid states are exactly the six Absurd task states already returned as `WorkflowRun.status`; matching is exact, lowercase, case-sensitive.
- State is a typed `WorkflowRunState` in `centaur-workflows` (parse from string, canonical name, list of all states); `WorkflowRuntime::list_runs` takes `Option<WorkflowRunState>` as a third parameter. The eight existing `list_runs(100, Some(..))` call sites in `centaur-api-server/tests` change to pass `None`; the CI oracle flags such edits for human review.
- The route parses `state` before looking up the workflow runtime, so an unknown state is a 400 naming the valid states regardless of runtime availability.
- Additive only: response shape, ordering, and default limit are unchanged; no migration.
- Proof is by new Rust tests in new files under `tests/` of each touched `services/api-rs` crate; database-backed tests skip without a database URL, while the unknown-state 400 and state parsing tests need none.
- Each new test must fail before its story's change (or not compile yet) and pass after it.

## Non-goals

- Multiple states in one request, negation, or state filters on other routes.
- Cursor pagination or ordering changes.
- New indexes or schema migration.
- Updating `packages/api-client` or docs pages.

## Success signal

Rust tests seed runs in all six states and show `?state=X` returns exactly the runs in state X, alone and with `limit` and `workflow_name`; a live run behind many newer finished runs is still returned with a small `limit`; an unknown state yields a 400 naming the valid states.

## Assumptions

- State matching is case-sensitive.
- A run's state is Absurd's task state exactly as `status` reports it today.

## Open Questions

- Should an empty `?state=` count as absent or as an unknown state (assumed: unknown, 400)?
- Should `packages/api-client` types and `docs/pages/extend/workflows*.mdx` document the new parameter in this work (assumed: no)?
- Should `Running` (any other casing) be accepted?
