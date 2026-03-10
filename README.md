# CAPS Guard

Guardrails + audit-grade traces for tool-calling AI workflows.

## What Problem It Solves
AI workflows can make side-effect calls (message/email/calendar/etc.) without clear policy visibility.
CAPS Guard enforces deterministic policy decisions at the tool boundary and emits trace artifacts that explain exactly what happened and why.

## Core Concepts
### Tool Execution Boundary
- Every tool step is evaluated before execution.
- Decision outcomes are deterministic: `ALLOW`, `REVIEW_REQUIRED`, `BLOCK`.

### Policy Decisions
- Decisions are manifest-driven (`src/manifest*.json`), not hardcoded in runtime flow.
- Precedence is deterministic (`BLOCK > REVIEW_REQUIRED > ALLOW`).
- Decision payload includes `reason_code` and `rule_id` for auditability.

### HITL Review
- If policy returns `REVIEW_REQUIRED` for an actionable sink step, execution pauses.
- Resume path uses explicit human decision (`approve` or `reject`).

### Trace Artifacts
- `trace.json`: canonical event log for decisions/tool calls/results/final summary.
- `trace_graph.json`: deterministic nodes/edges execution-path view derived from `trace.json`.

## Install
Requirements:
- Python 3.10+
- Ollama running locally (for prompt/langgraph paths)
- Pulled local model (default from `src/config.py`)

Setup:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Quickstart
Policy check without execution:
```bash
python scripts/caps_guard.py check \
  --manifest src/manifest_demo.json \
  --tool messaging_api \
  --args-json '{"message":"hello"}' \
  --output-dir /tmp/guard_check_demo
```

Plan execution (no prompt parsing needed):
```bash
python scripts/caps_guard.py execute \
  --manifest src/manifest_demo.json \
  --plan examples/plan_rw_demo.json \
  --output-dir /tmp/guard_execute_demo
```

## Example Manifest Profiles
Use these profiles to validate the three v0.1 policy proofs:

- `src/manifest_demo.json`: primary profile (alias of default policy profile used for demos).
- `src/manifest_side_effect_demo.json`: side-effect class policy proof:
  - `WRITE` non-sink -> `REVIEW_REQUIRED` (`rule_id=REVIEW_WRITE_CLASS`)
  - `IRREVERSIBLE` -> `BLOCK` (`rule_id=BLOCK_IRREVERSIBLE`)
- `src/manifest_args_demo.json`: argument-level block proof:
  - forbidden args -> `BLOCK` (`reason_code=ARGS_FORBIDDEN_PATTERN`)

Side-effect class proof commands:
```bash
python scripts/caps_guard.py check \
  --manifest src/manifest_side_effect_demo.json \
  --tool messaging_api \
  --args-json '{"message":"hi"}' \
  --output-dir /tmp/sidefx_check_write

python scripts/caps_guard.py check \
  --manifest src/manifest_side_effect_demo.json \
  --tool calendar_api \
  --args-json '{"title":"deploy"}' \
  --output-dir /tmp/sidefx_check_irrev
```

Argument-block proof command:
```bash
python scripts/caps_guard.py check \
  --manifest src/manifest_args_demo.json \
  --tool weather_api \
  --args-json '{"query":"drop table users"}' \
  --output-dir /tmp/args_demo_check
```

## End-to-End Demo Flow
Use this exact flow for v0.1 demo (pause on sink, then approve):

```bash
rm -f .caps_guard_demo.sqlite
rm -rf /tmp/section9_block /tmp/section9_approve

python scripts/caps_guard.py execute \
  --manifest src/manifest_demo.json \
  --prompt "If weather is below 100C in Toronto, text Jacob I am not coming to university today." \
  --thread-id demo1 \
  --sqlite-path .caps_guard_demo.sqlite \
  --output-dir /tmp/section9_block \
  > /tmp/section9_block_stdout.json

python scripts/caps_guard.py execute \
  --manifest src/manifest_demo.json \
  --resume-review approve \
  --thread-id demo1 \
  --sqlite-path .caps_guard_demo.sqlite \
  --output-dir /tmp/section9_approve \
  > /tmp/section9_approve_stdout.json
```

Inspect artifacts:
```bash
cat /tmp/section9_block/trace.json
cat /tmp/section9_approve/trace.json
cat /tmp/section9_approve/trace_graph.json
```

Expected behavior:
- First run pauses before sink execution (`pending_review=true`, `paused_for_review=true`).
- Resume run emits `review_resume` and completes sink execution.
- `trace_id` remains stable across pause/resume for the same thread.

Blocked demo (`ARGS_FORBIDDEN_PATTERN`):
```bash
python scripts/caps_guard.py check \
  --manifest src/manifest_args_demo.json \
  --tool weather_api \
  --args-json '{"query":"drop table users"}' \
  --output-dir /tmp/args_demo_check
```

## Trace Schema Overview
Decision outcomes (v0.1):
- `ALLOW`
- `REVIEW_REQUIRED`
- `BLOCK`

Canonical reason codes (v0.1, bounded set):
- `TOOL_ALLOWLISTED`
- `NON_TOOL_STEP`
- `TOOL_DENYLISTED`
- `SINK_REQUIRES_REVIEW`
- `ARGS_FORBIDDEN_PATTERN`
- `TOOL_UNKNOWN`
- `TOOL_NOT_ALLOWED`
- `POLICY_CONFLICT_RESOLVED`
- `REVIEW_POLICY_MATCHED`
- `EXECUTION_ERROR`

Canonical event types in `trace.json`:
- `decision`
- `tool_call`
- `tool_result`
- `review_resume`
- `final_summary`

`decision` event payload fields:
- `decision`
- `reason_code`
- `rule_id`
- `manifest_id`
- `manifest_version`
- `timestamp_ms`
- `precedence_resolved` (optional)
- `resolution_reason_code` (optional)
- `winning_decision` (optional)
- `winning_reason_code` (optional)
- `winning_rule_id` (optional)

Top-level trace provenance fields:
- `trace_id`: workflow trace lineage id.
- `run_id`: backward-compatible alias of `current_run_id`.
- `current_run_id`: latest run id present in events.
- `artifact_run_id`: CLI invocation id that wrote this artifact.
- `run_ids`: ordered unique run ids seen in `events[*].run_id`.

`final_summary.payload` includes:
- `decision_counts`
- `tool_call_count`
- `tool_result_count`
- `tool_error_count`
- `event_count`
- `reviewed_tools`
- `blocked_tools`
- `sink_step_count`
- `execution_result_count`

`execution_result_count` contract:
- It is phase-local to the current artifact summary (not a global cross-run counter).
- For a blocked/pending-review artifact, it reflects safe-phase results.
- For an approve/resume artifact, it reflects resumed sink-phase results.
- Use `trace.json.events` + `run_ids` for cross-run accounting.

Schema stability statement (v0.1):
- `trace.json` event contract and top-level provenance fields are treated as stable for v0.1.
- New fields may be added, but existing documented fields are not intended to be renamed/removed in v0.1.

## Current Limitations
- Works for the current supported tool/step model.
- New tools require manifest policy coverage and adapter coverage.
- `trace_graph.json` is an execution-path graph (sequential nodes/edges), not a full branch tree.
- Env-aware policy matrix (`dev/stage/prod`) is post-v0.1.
- Hosted review workflows are out of scope for v0.1.

## Roadmap / What’s Next
- Post-v0.1 Slice D: env-aware policy hardening (`prod`-sensitive rules).
- Branch-aware trace graph evolution (`trace_graph_v2.json`).
- Lightweight graph renderer (`trace_graph.json` -> HTML/SVG) for demo UX.

## Regression Gates
Run before release:
```bash
python -m py_compile src/main.py scripts/caps_guard.py scripts/regression_suite.py src/core/langgraph_flow.py src/core/mcp.py src/core/execution_runtime.py src/core/policy_engine.py
python scripts/regression_suite.py --policy-only
python scripts/regression_suite.py --guard-only
python scripts/regression_suite.py --hitl-only
```
