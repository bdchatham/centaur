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
  - **success:** An unknown `state` (including empty or wrongly cased) returns HTTP 400 whose message names all six valid states; a request without `state` returns what it returns today.

## Constraints

- The filter runs in SQL, per queue (workflow, slack-live, etl, etl-backfill), before `limit`; filtering a fetched page in Rust would hide live runs behind newer finished ones.
- Valid states are exactly the six Absurd task states already returned as `WorkflowRun.status`; matching is exact, lowercase, case-sensitive.
- The route parses `state` before looking up the workflow runtime, so an unknown state is a 400 naming the valid states regardless of runtime availability.
- Additive only: response shape, ordering, and default limit are unchanged; no migration.
- Proof goes through the HTTP route only: new tests in new files under `tests/` of `services/api-rs/crates/centaur-api-server` call `GET /api/workflows/runs` on a router built with `build_router_with_session_and_workflow_runtime`, and use no type or signature this work adds, so they compile before the change and fail because `state` is ignored. The state type and the `WorkflowRuntime::list_runs` shape are implementation choices behind those tests; existing tests are edited only if the chosen design requires it, and the CI oracle flags such edits.
- Database-backed tests skip without a database URL; the unknown, empty, and wrongly cased state 400 tests need none and carry the red-then-green proof there.

## Non-goals

- Multiple states in one request, negation, or state filters on other routes.
- Cursor pagination or ordering changes.
- New indexes or schema migration.
- Updating `packages/api-client` or docs pages.

## Success signal

HTTP-level Rust tests seed runs in all six states and show `?state=X` returns exactly the runs in state X, alone and with `limit` and `workflow_name`; a live run behind many newer finished runs is still returned with a small `limit`; an unknown state yields a 400 naming the valid states.

## Assumptions

- State matching is case-sensitive: `Running` is an unknown state and returns 400.
- A run's state is Absurd's task state exactly as `status` reports it today.

## Open Questions

- Should an empty `?state=` count as absent or as an unknown state (assumed: unknown, 400)?
- Should `packages/api-client` types and `docs/pages/extend/workflows*.mdx` document the new parameter in this work (assumed: no)?
