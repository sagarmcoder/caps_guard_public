# MVP Guard Roadmap (Canonical Tracker)

Status legend:
- `[x]` done
- `[ ]` pending
- `(~)` in progress / partial

Date baseline:
- Branch: `feature/langgraph-orchestration`
- Current checkpoint includes: Phase 3.5 HITL + state-header prep for Phase 4

## 1. North Star
- Build CAPS as a reusable enterprise orchestration core where:
  - execution is deterministic and policy-guarded
  - side effects are auditable and reviewable
  - environment rules are declarative (manifest), not hardcoded in engine logic

## 2. Product Wedge (MVP)
- Guarded Tool Execution + Decision Trace
- Narrow product surface, full-loop demo:
  - keep end-to-end CAPS flow for demos
  - productize policy guard + trace boundary at tool execution

## 3. Current State (Completed)
### Core pipeline + reliability
- [x] Action-parse -> reconcile -> task_graph -> verify -> compile -> runtime flow stable
- [x] Deterministic compile hardening:
  - DAG/toposort
  - cycle guard fallback
  - condition-token integrity checks
- [x] Runtime safety:
  - policy gate in runtime path
  - per-step typed outcomes + retries + error mapping
  - idempotency keys for send actions

### LangGraph + persistence
- [x] Phase 1 wrapper with parity to direct path
- [x] Phase 2A SQLite checkpointer
- [x] Phase 2B state/history inspection by thread
- [x] Phase 2C checkpoint recovery regression coverage

### HITL and runtime gating
- [x] Phase 3A pause before side effects
- [x] Phase 3B approve/reject/resume flow
- [x] Phase 3.5 review timing fix:
  - execute safe steps first
  - compute actionable sinks from runtime state
  - review only actionable sinks
  - resolve recipient before review for reachable branches

### State architecture prep (pre-Phase 4)
- [x] Added tiered state buckets:
  - `hot_state`
  - `warm_cache`
  - `cold_refs`
- [x] Added computed `state_header`
- [x] Added `memory_digest` field contract in `warm_cache`
- [x] State/history inspector empty-thread safety

### Regression gates
- [x] Standard flow regression suite
- [x] Cycle guard regression
- [x] LangGraph checkpoint regression
- [x] HITL approve/reject/actionable-sink regression

## 4. Short-Term Wins (MVP Shipping Scope)
### Phase 3.8 - Manifest Foundation (Before Phase 4)
- [x] Add `src/manifest.json` (default v1)
- [x] Add `src/core/manifest_loader.py` (load + validate + normalize)
- [x] Manifest v1 schema fields:
  - [x] `manifest_id`
  - [x] `manifest_version`
  - [x] `tool_registry`
  - [x] `tool_registry[].side_effect_class` (`READ | WRITE | IRREVERSIBLE`)
  - [x] (optional) `tool_registry[].data_sensitivity` (`PUBLIC | INTERNAL | CONFIDENTIAL`)
  - [x] `sink_tools`
  - [x] `review_policies`
  - [x] `constraint_flags`
- [x] Pass manifest context into execution graph state
- [x] Add manifest summary to `state_header`:
  - [x] `manifest_id`
  - [x] `manifest_version`
  - [x] `active_policy_flags`
  - [x] `sink_tools` (or compact equivalent)
- [x] Replace hardcoded sink/review checks with manifest-aware policy checks

### MVP policy decision contract
- [x] Canonical decision outcomes:
  - [x] `ALLOW`
  - [x] `REVIEW_REQUIRED`
  - [x] `BLOCK`
- [x] Enforce precedence:
  - [x] `DENY > REVIEW > ALLOW`
- [x] Canonical reason codes (stable taxonomy):
  - [x] `TOOL_DENYLISTED`
  - [x] `SINK_REQUIRES_REVIEW`
  - [x] `ARGS_FORBIDDEN_PATTERN`
  - [x] `TOOL_ALLOWLISTED`
  - [x] `TOOL_UNKNOWN`
  - [x] `TOOL_NOT_ALLOWED`
  - [x] `POLICY_CONFLICT_RESOLVED`
  - [x] `REVIEW_POLICY_MATCHED`
  - [x] `EXECUTION_ERROR`
  - [x] Keep reason code set bounded (target: 10-15 codes max)
- [x] Decision payload fields:
  - [x] `decision`
  - [x] `reason_code`
  - [x] `rule_id`
  - [x] `manifest_id`
  - [x] `manifest_version`
  - [x] `trace_id`
  - [x] Stable decision-event schema (do not break once published):
    - [x] `trace_id`
    - [x] `step_id`
    - [x] `tool_name`
    - [x] `decision`
    - [x] `reason_code`
    - [x] `rule_id` (nullable)
    - [x] `manifest_id`
    - [x] `manifest_version`
    - [x] `timestamp_ms`

### MVP CLI surface
- [x] `caps guard execute --manifest m.json --prompt "..."`
- [x] `caps guard execute --manifest m.json --plan plan.json`
- [x] `caps guard check --manifest m.json --tool <tool> --args <args.json>` (non-executing trust checker)
- [x] Output controls:
  - [x] `--output-dir <path>`
  - [x] `--format json|text`

### MVP trace outputs
- [x] Mandatory canonical `trace.json`
- [x] `trace_graph.json` artifact (deterministic nodes/edges for visualization)
- [x] Single-line summary output for human readability
- [ ] Optional demo renderer (`trace.svg`/`trace.html`)
- [x] Trace compactness contract:
  - [x] Canonical event types only: `decision`, `review_resume`, `tool_call`, `tool_result`, `final_summary`
  - [~] Stable event ordering for deterministic diffs
  - [x] Keep event payloads compact; avoid raw verbose blobs unless explicitly requested
- [x] Trace usability gate:
  - [x] Include topline summary + counts (`allow/review/block`, tool calls, failures)
  - [x] Trace remains human-auditable without scanning every event

### MVP Hardening (Pre-Live Run; from strict feedback)
- [x] A1. Exercise guard `execute` with real guarded tool steps (highest priority)
  - [x] Demo plan must include at least one READ tool step (expected `ALLOW`)
  - [x] Demo plan must include at least one WRITE/sink step (expected `REVIEW_REQUIRED`)
  - [x] Trace must include boundary events: `tool_call`, `tool_result` (and `tool_error` when failure path is tested)
  - [x] Add regression for this mixed READ+WRITE guarded path
- [x] B1. Fix reason code for non-tool steps
  - [x] For `tool_name = null`, stop emitting `TOOL_ALLOWLISTED`
  - [x] Emit explicit non-tool reason code: `NON_TOOL_STEP` (or `NO_TOOL_REQUIRED`)
  - [x] Add regression assertion for non-tool reason code semantics
- [x] C1. Add `trace_id` in `guard check` mode
  - [x] Generate deterministic `trace_id` (e.g., `trace_check_<id>`) in `check` responses
  - [x] Keep optional minimal check trace artifact as deferred/non-blocking
- [x] D1. Make `final_summary` payload audit-grade
  - [x] Include: `decision_counts`, `tool_call_count`, `tool_result_count`, `tool_error_count`, `event_count`
  - [x] Optional: include compact `reviewed_tools[]` and `blocked_tools[]`
  - [x] Add regression assertion for final summary schema
- [x] E1. Lock deterministic trace event ordering
  - [x] Define canonical append order by execution sequence
  - [x] Add deterministic tie-break when timestamps match (`step_id`, `event_type_order`)
  - [x] Add regression that compares stable ordered event sequence
- [x] F1. Tighten rule binding determinism
  - [x] Ensure fallback decisions use explicit synthetic rule ids (`DEFAULT_DENY`, `DEFAULT_ALLOW`)
  - [x] For multi-rule matches, persist precedence resolution marker (`POLICY_CONFLICT_RESOLVED`) with winner
  - [x] Add regression assertions for conflict resolution metadata

### Policy Trust Test Pack (MVP Gate)
- [x] Add dedicated policy precedence tests:
  - [x] deny beats allow
  - [x] deny beats review
  - [x] review beats allow
  - [x] unknown tool blocks
  - [x] sink tool triggers review
  - [x] forbidden args block
- [x] Rule binding determinism tests:
  - [x] every decision maps to a deterministic `rule_id` when possible
  - [x] fallback decisions map to explicit synthetic rules (e.g. `DEFAULT_DENY`, `DEFAULT_ALLOW`)
  - [x] no ambiguous multi-rule outputs without precedence resolution marker

### MVP Scope Guardrail
- [ ] Keep MVP state contract minimal:
  - [ ] if it does not affect `execute_safe` decisions or trace emission, it is not a ship blocker
- [x] For MVP, `state_header` must at minimum expose:
  - [x] `manifest_id`
  - [x] `manifest_version`
  - [x] `active_policy_flags`
  - [x] `trace_id` (or trace handle)

## 5. Phase 4 (After 3.8) - Semantic Supervisor
- [ ] Add supervisor node with strict structured outputs:
  - [ ] `APPROVE`
  - [ ] `REVIEW_REQUIRED`
  - [ ] `REJECT:<reason_code>`
- [ ] Supervisor reads:
  - [ ] `state_header`
  - [ ] manifest summary
  - [ ] minimal `warm_cache` context
- [ ] Keep deterministic authority in verifier/compiler/runtime guardrails
- [ ] Add supervisor path regressions

## 6. Long-Term Wins (Platformization)
### Enterprise portability
- [ ] Multi-manifest tenant support + versioned rollout strategy
- [ ] Policy testing harness and fixture packs per domain
- [ ] Guard API for external orchestrators (after CAPS-native CLI stabilizes)

### Adapter/bindings maturity
- [ ] Real `identity_api` integration
- [ ] Real `messaging_api` integration
- [ ] Real `email_api` integration
- [ ] Real `calendar_api` integration

### Advanced control + memory
- [ ] Memory digest refinement for cross-turn references
- [ ] Supervisor deep-dive mode (read cold history on demand)
- [ ] Predicate logic/branch model upgrades beyond current condition patterns

## 7. Explicit Deferrals (Not MVP)
- [ ] No full custom DSL for policies (keep declarative v1 small)
- [ ] No auto-generated adapter bindings from schemas
- [ ] No generic `caps guard run -- python ...` wrapper in first MVP cut
- [ ] No broad storage micro-optimization unless measured bottlenecks appear

## 8. Definition of MVP Done
- [x] Manifest-backed policy guard active at execution boundary
- [x] Deterministic verdict + reason code for every guarded decision
- [x] Canonical `trace.json` emitted for runs
- [x] `guard check` command works deterministically without execution
- [x] Full-loop CAPS demo shows safe gating + review + traceability
- [x] Regression suite remains green with new manifest-aware tests

## 9. MVP Demo Script (Required)
- [x] Provide deterministic demo command sequence using manifest-backed guard:
  - [x] `python scripts/caps_guard.py execute --manifest src/manifest.json --prompt "If weather is below 100C in Toronto, text Jacob I am not coming to university today." --thread-id demo1 --sqlite-path .caps_demo.sqlite --output-dir /tmp/section9_block`
  - [x] `python scripts/caps_guard.py execute --manifest src/manifest.json --resume-review approve --thread-id demo1 --sqlite-path .caps_demo.sqlite --output-dir /tmp/section9_approve`
- [x] Demo acceptance sequence:
  - [x] read tool allowed without review (`weather_api`)
  - [x] decision branch visible in trace
  - [x] sink tool flagged for review (`messaging_api`)
  - [x] approve path executes
  - [x] trace renders cleanly (`trace.json`, `trace_graph.json`)

## 10. Item 2 Expansion (Trust-First Sequence)
Objective:
- Add the highest-value policy proof points without widening scope or introducing DSL/config bloat.
- Preserve the same wedge: guarded execution + deterministic decisions + auditable trace.

Sequence rule:
- Execute in strict order: `A -> B -> E -> C -> D`.
- `D` is MVP+1 unless launch/demo requires it.

### 10.1 Slice A (Now): Side-effect Class Proof + IRREVERSIBLE Proof
- [x] Item 2A: prove `side_effect_class=WRITE` can trigger `REVIEW_REQUIRED` even when tool is not in `sink_tools`
- [x] Item 2B: add one `IRREVERSIBLE` tool path with deterministic blocked behavior
- [x] Add one compact manifest/demo profile for this proof (no adapter expansion required)
- [x] Add regression assertions:
  - [x] WRITE/non-sink path yields `REVIEW_REQUIRED` with deterministic `reason_code` + `rule_id` (`REVIEW_WRITE_CLASS`)
  - [x] IRREVERSIBLE path yields deterministic `BLOCK` with deterministic `reason_code` + `rule_id` (`BLOCK_IRREVERSIBLE`)
  - [x] trace includes decision + final_summary with expected counts

### 10.2 Slice B (Now): Argument-Level Security Proof
- [x] Item 2C: deterministic `ARGS_FORBIDDEN_PATTERN` block scenario
- [x] Add one guard demo command for blocked argument pattern
  - [x] `python scripts/caps_guard.py check --manifest src/manifest_args_demo.json --tool weather_api --args-json '{"query":"drop table users"}'`
  - [x] `python scripts/caps_guard.py execute --manifest src/manifest_args_demo.json --plan /tmp/plan_args_block.json`
- [x] Add regression assertions in `check` and/or `execute` path:
  - [x] `decision=BLOCK`
  - [x] `reason_code=ARGS_FORBIDDEN_PATTERN`
  - [x] deterministic `rule_id`

### 10.3 Slice E (Now): F1 Closure as Explicit Gate
- [x] Item 2F: force `DEFAULT_DENY` path in regression (unknown/unregistered tool)
- [x] Add/keep explicit multi-match precedence regression:
  - [x] emits `POLICY_CONFLICT_RESOLVED`
  - [x] winner fields are deterministic (`winning_decision`, `winning_rule_id`, etc.)

### 10.4 Slice C (Now): execution_result_count Contract Clarity
- [x] Item 2D: document exact semantics of `execution_result_count` in README
- [x] Keep field name backward-compatible; optional alias only if necessary
- [x] Add one short note in demo docs so reviewers can reconcile blocked vs approve runs quickly

### 10.5 Slice D (MVP+1): Minimal Environment-Aware Rule
- [ ] Item 2E: add `env` to manifest context/state_header (`dev|prod`)
- [ ] Add one minimal env-aware policy:
  - [ ] `prod + IRREVERSIBLE => BLOCK` (preferred)
  - [ ] or `prod + WRITE => REVIEW_REQUIRED`
- [ ] Add one demo + one regression only (no tenant/service matrix)
“It can deterministically block irreversible actions, and env-aware blocking like prod + IRREVERSIBLE => BLOCK is the next minimal policy extension.” ( this slice makes this possible)

### 10.6 Scope Guardrails (Do Not Expand)
- [ ] No policy DSL work in this slice
- [ ] No multi-dimension env policy matrix (tenant/service/region)
- [ ] No adapter plumbing expansion beyond stubs needed for deterministic demos
- [ ] Keep reason-code taxonomy bounded; prefer existing reason codes
