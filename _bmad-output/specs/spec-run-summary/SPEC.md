---
id: SPEC-run-summary
companions: []
sources: []
---

> **Canonical contract.** This SPEC and the files in `companions:` are the complete, preservation-validated contract for what to build, test, and validate. Source documents listed in frontmatter are for traceability — consult them only if you need narrative rationale or prose color this contract intentionally omits.

# Workflow run summary endpoint

## Why

An opportunity to capture: clients of the api-rs workflow-runs API that only need a run's headline (which workflow, what status, how many attempts, when created and last changed) must fetch `GET /api/workflows/runs/{run_id}` and receive the whole run, including input, result and failure payloads. A small per-run summary endpoint serves them without that bulk.

## Capabilities

- **CAP-1**
  - **intent:** A client can fetch a small summary of one workflow run.
  - **success:** `GET /api/workflows/runs/{run_id}/summary` for an existing run returns 200 with a summary object holding exactly `run_id`, `workflow_name`, `status`, `attempts`, `created_at` and `updated_at`.
- **CAP-2**
  - **intent:** The summary tells the same story as the full run.
  - **success:** For the same run, each summary field equals the same field of `GET /api/workflows/runs/{run_id}`, for runs from all four queues (workflow, slack-live, etl, etl-backfill) and in every run state.
- **CAP-3**
  - **intent:** A client learns that a run does not exist.
  - **success:** `GET /api/workflows/runs/{run_id}/summary` for an unknown run returns 404 with the same error body as the full run endpoint.

## Constraints

- Additive only: existing routes, `WorkflowRun`, and the list and get responses keep their shape and semantics.
- Summary values are identical to the full run's (same `status` string, same `updated_at` derivation, RFC 3339 like `created_at`); they are not re-derived separately. The summary carries none of `input`, `result`, `failure`, task ids, `started_at` or `completed_at`.
- The new route needs a `route_access` entry mapped to `Capability::WorkflowsRead`, like the sibling GET run routes; without one every caller gets 403.
- Values come from durable Absurd state in Postgres; no process-local source of truth and no migration.
- Proof is by new Rust test files under `services/api-rs/crates/centaur-api-server/tests/`. Each test compiles before the change and fails because the route is missing, so it uses only existing items (`build_router_with_session_and_workflow_runtime`, a console token, direct seeding of the Absurd queue tables) and asserts on HTTP status and JSON. Database-backed tests skip without a database URL. Existing tests may be edited where the design needs it; the CI oracle flags such edits for human review.

## Non-goals

- Steps, input, result, failure, `started_at` or `completed_at` in the summary.
- A list-of-summaries endpoint or a `fields` selector on existing routes.
- Changing `status` or `updated_at` semantics.
- `packages/api-client` types or any UI.
- Caching.

## Success signal

Rust tests show that, for runs in each state and queue, the summary endpoint returns exactly the six fields with values equal to the full run endpoint's, and that an unknown run returns 404.

## Assumptions

- The body follows the sibling routes' envelope: `{"ok": true, "summary": {...}}`.
- The attempt count is the full run's `attempts` value, exposed under the key `attempts`.
- `status` is the raw Absurd state string the full run reports.
- A malformed (non-UUID) run id behaves as it does on the full endpoint; this work does not special-case it.

## Open Questions

- Envelope: should the body be `{"ok": true, "summary": {...}}` (assumed, matches sibling routes) or the bare summary object?
- Is "attempt count" the task-level `attempts` the full run reports, under the key `attempts`?
- `get_run` casts the id to uuid, so a malformed run id likely yields a 500 on the full endpoint today; should the summary answer 404 or 400 for it?
