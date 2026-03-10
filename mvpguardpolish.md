# MVP Guard Polish (v0.1 Release Gate)

## No-BS Verdict
This checklist is correct and worth enforcing before release.

Recommended scope discipline:
- Ship v0.1 with Guard + Trace + HITL + Manifest policies.
- Do **not** include Slice D (env-aware rules) in v0.1 unless it is fully implemented and regression-covered.

## 1. Freeze Core Scope
Release claim for v0.1:
- Guardrails + audit-grade traces for tool-calling AI workflows.

Must-have capabilities:
- Deterministic `ALLOW | REVIEW_REQUIRED | BLOCK`
- HITL pause/resume
- `trace.json` + `trace_graph.json`
- Manifest-driven policy decisions
- CLI workflow (`caps_guard.py`)

Out of scope for v0.1:
- Env-aware policy matrix
- Hosted mode
- Broader platformization work

## 2. README Release Standard
README must let a new user understand the tool in under 2 minutes.

Required section order:
1. One-line product description
2. Problem solved
3. Core concepts
4. Install
5. Quickstart
6. End-to-end demo flow
7. Trace schema overview
8. Current limitations
9. Roadmap / next

Core concepts must include:
- Tool execution boundary
- Policy decisions
- HITL review flow
- Trace artifacts

Required demo flow in README:
- Prompt run that pauses on sink
- Resume with approve
- Inspect `trace.json`
- Inspect `trace_graph.json`

Required blocked demo in README:
- `ARGS_FORBIDDEN_PATTERN` or `IRREVERSIBLE` block

## 3. Example Manifests + Examples
Minimum examples required before ship:
- Primary profile (current messaging/tool-calling flow)
- Policy proof profile (`WRITE` non-sink -> `REVIEW_REQUIRED`, `IRREVERSIBLE` -> `BLOCK`)
- Argument-block profile (`ARGS_FORBIDDEN_PATTERN`)

Target files:
- `src/manifest_demo.json` (or promote `src/manifest.json` as demo profile and document clearly)
- `src/manifest_side_effect_demo.json`
- `src/manifest_args_demo.json`
- Sample plan/prompt snippets used in docs

Note:
- Do not block v0.1 on fully building all future profiles.

## 4. Freeze Guard Contract in Docs
Public contract to freeze for v0.1:
- Decisions: `ALLOW`, `REVIEW_REQUIRED`, `BLOCK`
- Canonical reason-code set (bounded)
- Trace event types:
  - `decision`
  - `tool_call`
  - `tool_result`
  - `review_resume`
  - `final_summary`
- Top-level trace provenance fields:
  - `trace_id`
  - `run_id` (compat alias)
  - `current_run_id`
  - `artifact_run_id`
  - `run_ids`
- `execution_result_count` meaning (single clear sentence)
- Schema stability statement for v0.1

## 5. Final Proof Pack (Release Gate)
Required commands:
```bash
python -m py_compile src/core/policy_engine.py src/core/manifest_loader.py scripts/regression_suite.py scripts/caps_guard.py src/core/langgraph_flow.py src/core/execution_runtime.py src/main.py src/core/mcp.py
python scripts/regression_suite.py --guard-only
python scripts/regression_suite.py --hitl-only
```

Required demo runs:
- Pause on sink
- Resume approve
- Blocked arg pattern
- WRITE non-sink review
- IRREVERSIBLE block

Required checks per relevant demo:
- Correct decision
- Correct reason code
- Correct rule id
- Expected event sequence
- `trace.json` and `trace_graph.json` emitted
- Summary counts consistent

## 6. Packaging for Real Use (v0.1 minimum)
Before ship:
- Clean install path
- Clear CLI entrypoint in docs
- Dockerfile added
- Docker demo run validated
- Local setup instructions simplified

Good enough for v0.1:
- GitHub release
- Install instructions
- Docker support
- Sample manifests
- Sample commands

Not required for v0.1:
- Helm
- Hosted deployment
- Enterprise packaging

## 7. Limitations Section (Required)
State explicitly:
- Works for current supported tool/step model
- New tools require manifest + adapter coverage
- Env-aware policy rules are not in v0.1 (unless Slice D is completed)
- Hosted review workflows are not part of v0.1

## 8. Section 9 Validation Notes (Latest Run)
Validated on end-to-end Section 9 run with SQLite checkpoint + resume:
- Prompt run pauses correctly on sink (`review_required=true`, `review_decision=null`).
- Blocked trace summary is consistent (`pending_review=true`, `paused_for_review=true`).
- Blocked state digest is truthful (approval required), no premature "message sent" text.
- Resume run emits `review_resume` and completes sink execution after approval.
- `trace_id` remains stable across pause/resume for the same workflow thread.
- Timing profile is expected for demo:
  - prompt path is heavy (LLM + tools + policy gate),
  - resume path is fast (continuation only).

Accepted non-blockers for v0.1:
- `artifact_run_id` semantics are still slightly less clear than `current_run_id + run_ids`.
- `execution_result_count` meaning is non-obvious unless documented clearly.
- `trace_graph.json` is an execution-path graph (sequential), not a full decision tree.

## 9. Future Path (Post-v0.1)
### Slice D: Environment-Aware Policy (Production Hardening)
Target:
- Add minimal environment context (`env=dev|prod`) into manifest context/state header.
- Add one deterministic rule with regression coverage, for example:
  - `prod + IRREVERSIBLE => BLOCK`, or
  - `prod + WRITE => REVIEW_REQUIRED`.

Guardrails:
- Keep exactly one env dimension in first pass.
- No DSL work, no tenant/service matrix in Slice D.
- Ship with one demo and one regression before expanding.

### Trace Graph Evolution: Sequential Path -> Branch-Aware Tree
Target:
- Keep current `trace_graph.json` for v0.1 compatibility.
- Add a branch-aware graph artifact in a new versioned format (for example `trace_graph_v2.json`).
- Represent condition splits and branch outcomes explicitly (`condition_true`, `condition_false`, `review_gate` edges).

Guardrails:
- Do not break existing `trace_graph.json` consumers.
- Keep node/event IDs deterministic for diffability.
- Validate edge semantics with regression tests, not demo-only checks.

### Renderer Recommendation (Worth Doing)
Verdict:
- Yes, a lightweight graph renderer is worth adding after v0.1 as a high-leverage DX improvement.

Pragmatic scope:
- Start with static HTML renderer from `trace_graph.json` / `trace_graph_v2.json`.
- Keep it offline and artifact-based (no service dependency).
- Add one README screenshot/GIF + one command (`render-trace`) to improve first-use experience.

## Release Rule
If any item in sections 1-5 is incomplete, do not tag v0.1.
