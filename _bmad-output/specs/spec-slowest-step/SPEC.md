---
id: SPEC-slowest-step
companions: []
sources: []
---

> **Canonical contract.** This SPEC and the files in `companions:` are the complete, preservation-validated contract for what to build, test, and validate. Source documents listed in frontmatter are for traceability — consult them only if you need narrative rationale or prose color this contract intentionally omits.

# Workflow run slowest-step endpoint

## Why

A pain to solve: an on-call engineer looking at a slow workflow run in the api-rs workflow-runs API cannot see where it spent its time without fetching the step history and comparing durations by hand. A slowest-step endpoint names the run's longest step and its duration.

## Capabilities

- **CAP-1**
  - **intent:** A client can fetch the slowest step of one workflow run.
  - **success:** `GET /api/workflows/runs/{run_id}/slowest-step` for an existing run returns 200 with `{"ok": true, "slowest_step": {"name", "duration_ms"}}`, where the entry is the one with the largest `duration_ms` among the run's step-history entries, for runs on all four queues (workflow, slack-live, etl, etl-backfill).
- **CAP-2**
  - **intent:** The choice is deterministic and ignores unfinished steps.
  - **success:** On equal `duration_ms` the entry earlier in step-history order wins; an entry whose `duration_ms` is null is never chosen.
- **CAP-3**
  - **intent:** A client learns that a run does not exist, and that a run with nothing to rank has no slowest step.
  - **success:** An unknown run returns 404 with the same error body as the full run endpoint; an existing run with no entry carrying a `duration_ms` returns 200 with `{"ok": true, "slowest_step": null}`.
- **CAP-4**
  - **intent:** The slowest step and the step history never disagree.
  - **success:** The returned `name` and `duration_ms` equal those of an entry in the same run's `GET .../steps` response, and that endpoint's entries are produced by the code the new route reuses, unchanged.

## Constraints

- Builds on the run-steps brief (`bafyreicwhes6micz7ic5z44zdktwnxvzonqw6rlz2mivuiufryul7hkjve`): reuse its step entries unchanged, and add no field to the step history in this brief.
- Prerequisite, owned by the run-steps spec: each step entry carries a real `duration_ms` (the step's run time in milliseconds, null while it runs). Today's run-steps spec forbids this: exactly four keys, `started_at` always null, no migration or SDK change, and `absurd.c_<queue>` stores no step start time. Delivery is blocked until the run-steps spec is revised and implemented to supply it.
- The response holds only `name` and `duration_ms` of the chosen entry; no payload, run or task ids, or other entry fields.
- Additive only: existing routes and their shapes keep their semantics; no migration in this brief.
- The new route needs a `route_access` entry mapped to `Capability::WorkflowsRead`, like the sibling GET run routes; without one every caller is denied.
- Values come from durable Absurd state in Postgres; no process-local source of truth.
- Proof is by new Rust test files under `services/api-rs/crates/centaur-api-server/tests/`; each fails (or does not compile) before its story's change. Database-backed tests skip without a database URL. Existing tests may be edited where the design needs it; the CI oracle flags such edits for human review.

## Non-goals

- Adding `duration_ms` or any other field to the step history in this brief.
- Recording step start times, a migration, or an `absurd-sdk` change in this brief.
- Top-N steps, percentiles, aggregates across runs, or a total run duration.
- Pagination, filters, or a `fields` selector.
- `packages/api-client` types or any UI.
- Caching.
- Special handling of a malformed (non-UUID) run id.

## Success signal

Rust tests show that, for runs on each queue, the endpoint returns the step with the largest `duration_ms` as `{name, duration_ms}` taken from the unchanged step entries, breaks ties by history order, skips null durations, returns `slowest_step: null` for a run without durations, and returns 404 for an unknown run.

## Assumptions

- The body follows the sibling routes' envelope: `{"ok": true, "slowest_step": {...}}`.
- `{run_id}` is a run row id as on the full endpoint; the entries ranked are those the steps endpoint returns for that run.
- `duration_ms` is a non-negative integer number of milliseconds.

## Open Questions

- `duration_ms` source: no step start time is stored, so a real duration needs a start time recorded when a step begins (migration plus `absurd-sdk` change) and exposed on step entries by the run-steps spec. Which brief owns that work? The run-steps spec is challenged on this.
- Should a run with no ranked step return 200 with `slowest_step: null` (assumed) or 404?
- Tie-break: earlier step in history order (assumed) or alphabetical by name?
- Should a step with null `duration_ms` (still running) ever be reported as the slowest? Specified: no.
