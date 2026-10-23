---
id: SPEC-stable-run-paging
companions:
  - git:bdchatham/specs@bb844c65010db6b188aea5eeec57562254a9610e:_bmad-output/specs/spec-stable-workflow-run-paging/SPEC.md
sources: []
---

> **Canonical contract.** This SPEC and the files in `companions:` are the complete, preservation-validated contract for what to build, test, and validate. Source documents listed in frontmatter are for traceability — consult them only if you need narrative rationale or prose color this contract intentionally omits.

# Stable cursor paging of workflow runs

## Why

A pain to solve: mana reads `GET /api/workflows/runs` to see Centaur's workflows, and the list stops at the newest 1000 runs (`limit` clamp). Runs can share a `created_at`, so walking back with `created_before` skips or repeats the runs at the boundary. api-rs needs a stable way to continue the list. This is the api-rs half of the parent spec (the companion, which holds the contract with mana and is not edited here); mana's half is built against it.

## Capabilities

- **CAP-1**
  - **intent:** A client that sends no `cursor` gets today's list, plus a continuation token.
  - **success:** Without `cursor`, filters (`workflow_name`, `state`, `created_after`, `created_before`), `limit` (default 50, clamped to 1..1000), ordering (`created_at` descending, then `task_id` descending, merged across the queues `workflow`, `slack-live`, `etl`, `etl-backfill`) and every run object are exactly as today; the body gains `next_cursor` and nothing else changes.
- **CAP-2**
  - **intent:** A client learns from every response whether more matching runs exist.
  - **success:** Every list response has the key `next_cursor`: a string when at least one matching run lies past the page, `null` when the page is the last. A page holding exactly the remaining runs (including a total that is an exact multiple of `limit`, and an empty result) has `null`.
- **CAP-3**
  - **intent:** A client can fetch the next page from a `next_cursor`, without skips or repeats.
  - **success:** With `cursor` set to a previous response's `next_cursor` (same filters and `limit`), the response is the next `limit` matching runs strictly after the position the cursor names, in the same total order. Runs sharing one `created_at` split across a page boundary, or sitting in different queues, appear once each; a walk over more than 1000 runs returns every matching run exactly once, in order.
- **CAP-4**
  - **intent:** A walk is unaffected by runs created while it is in progress.
  - **success:** The cursor names a position, not an offset: runs created after page 1 was read (they sort before it) never change a later page. Filters are held across pages because the client re-sends them; the cursor carries only the position, and a cursor combines with `created_after`/`created_before` by intersection.
- **CAP-5**
  - **intent:** A client that sends a `cursor` api-rs did not issue learns so.
  - **success:** A `cursor` that cannot be decoded (garbage, empty, wrong shape, a task id that is not a UUID, a timestamp out of range) returns HTTP 400 whose message names `cursor`, in the style of `parse_created_bound`; never an empty page and never a restart from the newest run.

## Constraints

- The position is applied in SQL in each queue's query (`list_runs_for_queue`) as a keyset predicate on `(t.enqueue_at, t.task_id)` before `limit`, and each queue reads `limit + 1` rows; reading everything and slicing in Rust is out. The `limit + 1` merged rows decide `next_cursor`, whose position is the last returned run.
- The cursor holds the stored `enqueue_at` at microsecond precision exactly (an integer, not text rounded to a coarser unit) plus the `task_id`, so a boundary run is neither lost nor repeated. It uses only URL-unreserved characters, so it can be sent in a query string unescaped.
- The Rust merge order (`created_at`, then `task_id` as canonical lowercase hyphenated UUID text, both descending) must agree with the SQL order (`enqueue_at`, then `task_id` as `uuid`, descending), or pages straddling a tie diverge.
- The route decodes `cursor` before looking up the workflow runtime, as it does `created_after`, so a bad cursor is a 400 regardless of runtime availability, and decoding rejects anything that SQL would otherwise fail on (a non-UUID id must not become a 500).
- No migration; `WorkflowRuntime::list_runs(limit, workflow_name)` keeps its signature (eight existing tests call it). The shape of `list_runs_by_state` and its page result is a design choice; existing tests change only where `next_cursor` requires it, and every such edit is reviewed in the PR diff.
- Proof goes through the HTTP route of a router built with `build_router_with_session_and_workflow_runtime`, as `runs_created_before_http.rs` does, seeding Absurd tables in `services/api-rs/crates/centaur-api-server/tests/` and run by the repo's `make test`. Database-backed tests skip without a database URL; tests that need none (bad cursors) carry the red proof there. Assertions compare values (`next_cursor` equals `null`, the ids in order), so each test fails before the change on a wrong value, not a missing item.

## Non-goals

- Changing run objects, ordering, the `limit` clamp or the filters.
- Offset or page-number paging, a total count.
- Paging any other endpoint, or `GET /api/workflows/runs/{run_id}`.
- Recording filters in the cursor, or keeping a cursor valid across deploys.
- New indexes or a schema migration.
- `packages/api-client` types and docs pages.

## Success signal

HTTP-level Rust tests seed more than 1000 runs, ties at one `created_at` across a page boundary and across queues, and runs inserted between pages; a walk following `next_cursor` with fixed filters and `limit` returns every matching run once in `created_at`/`task_id` descending order and ends on `null` (also when the total is a multiple of `limit`); no `cursor` returns today's first page plus `next_cursor`; a bad cursor is a 400.

## Assumptions

- Absurd sets `enqueue_at` once at task creation and never moves it, so `created_at` is a stable sort key across a walk, and it is never infinite.
- The deliveries for the three stories are merged together: between stories 1 and 2 a client following `next_cursor` would receive page 1 again.

## Open Questions

- Should a present but empty `cursor=` be a 400 (assumed, as for an empty `created_before`) rather than the same as absent?
- A cursor naming a position older than every matching run: empty page with `next_cursor: null` (assumed), not a 400?
- No index on `enqueue_at` exists in the Absurd tables, and no migration is allowed: the per-queue sort stays as costly as today for deep pages. Accepted?
