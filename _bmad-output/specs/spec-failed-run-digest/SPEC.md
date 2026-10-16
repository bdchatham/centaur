---
id: SPEC-failed-run-digest
companions: []
sources: []
---

> **Canonical contract.** This SPEC and the files in `companions:` are the complete, preservation-validated contract for what to build, test, and validate. Source documents listed in frontmatter are for traceability — consult them only if you need narrative rationale or prose color this contract intentionally omits.

# Failed workflow run digest endpoint

## Why

A pain to solve: an on-call engineer who wants to know which workflow runs failed, and why, must list runs, filter by state and fetch each failed run in full. One endpoint returning the most recent failed runs as run summaries answers it at a glance. It builds on the run-summary brief (`bafyreih3phjysdb7zttv4ut35jy2an6uctkpkdutnqer2pcdmmjv7apuni`).

## Capabilities

- **CAP-1**
  - **intent:** An on-call engineer can fetch the most recent failed runs in one call.
  - **success:** `GET /api/workflows/runs/failed-digest` returns 200 with only runs whose `status` is `failed`, from all four queues (workflow, slack-live, etl, etl-backfill), newest first; a failed run is returned even when more than `limit` newer runs in other states exist.
- **CAP-2**
  - **intent:** The caller bounds the digest size.
  - **success:** Without `limit` at most 20 entries are returned; `?limit=N` returns at most N; a `limit` above 100 returns at most 100.
- **CAP-3**
  - **intent:** Each digest entry is that run's summary, and says why the run failed.
  - **success:** Every entry equals, field for field, the body's summary from `GET /api/workflows/runs/{run_id}/summary` for the same run, including `last_error`, with no field added or dropped by the digest.

## Constraints

- Each entry is the run-summary brief's summary unchanged. The digest adds no summary field; `last_error` (the run's failure message, null unless it failed) must be provided by the summary spec itself. The summary spec currently pins exactly six fields without `last_error`, so this spec depends on it and challenges that point.
- The summary route and type do not exist in `services/api-rs` yet; the digest is delivered on top of the summary work and reuses its single summary builder rather than re-deriving fields.
- The `failed` filter runs in SQL per queue before `limit`, reusing the `list_runs_by_state` path in `centaur-workflows` (queues merged, sorted by `created_at` then `task_id` descending); filtering a fetched page in Rust would hide failed runs behind newer live ones.
- The static route `/api/workflows/runs/failed-digest` coexists with `/api/workflows/runs/{run_id}`, and needs a `route_access` entry mapped to `Capability::WorkflowsRead`; without it every caller gets 403.
- Additive only: existing routes and responses keep their shape. Values come from durable Absurd state in Postgres; no migration.
- Proof is by new Rust test files under `services/api-rs/crates/centaur-api-server/tests/` that only issue HTTP requests against a router built with `build_router_with_session_and_workflow_runtime`, seeding the Absurd queue tables directly. They compile before the change and fail because the route is missing or `limit` is ignored. Database-backed tests skip without a database URL. Existing tests may be edited where the design needs it; the CI oracle flags such edits for human review.
- Every story sets `spec_checkpoint: true`; a human reviews each story before work starts.

## Non-goals

- Adding any field to the summary, including `last_error`, from this spec.
- Filters other than `limit` (workflow name, time window), cursors or pagination.
- Digest entries beyond the summary: steps, input, result, failure payload.
- Caching, `packages/api-client` types, or any UI.

## Success signal

HTTP-level Rust tests seed failed and non-failed runs over all four queues and show the digest returns only failed runs, newest first, 20 by default and at most 100, each equal to its summary-endpoint body.

## Assumptions

- The body follows the list route's envelope: `{"ok": true, "runs": [summary, ...]}`.
- "Newest" means `created_at` descending, `task_id` descending as tie-break, as on the list route.
- A numeric `limit` outside 1..=100 is clamped into that range; a non-integer `limit` is rejected by query parsing with 400.
- The digest is proven only once the summary route and `last_error` exist.

## Open Questions

- Should "most recent" order by failure time (`updated_at`) rather than `created_at`?
- Should `limit=0` or a negative `limit` be a 400 rather than clamped to 1?
- How is `last_error` derived from the stored failure payload (message string or serialized payload)? The summary spec owns this.
- The summary spec states the summary has exactly six fields and does not mention `last_error`; it must change before this spec's CAP-3 can hold.
