# CAPS Project

Context Action Planning Service

## Team
- Sagar
- Aneesh

## Architecture (Current)

`User Prompt -> MCPService -> Action Parse (LLM) -> Reconcile/Normalize -> CIR -> Task Graph Builder -> Verifier -> Compiler -> Execution Plan`

Execution runtime path:
- Dry-run executor validates resolver and branch behavior before live adapters.
- Live adapter wiring is the next phase.

## Current Status
- Action-parse-first pipeline is implemented and used by default.
- Reconcile, verification, and compile gates are in place for deterministic planning.
- Resolver gates are injected before side-effect steps (`resolve_location`, `resolve_recipient`).
- Focused regression suite is implemented and passing.
- Dry-run execution simulation is implemented and validated.
- Manifest-backed guard + trace layer is implemented (`scripts/caps_guard.py`).
- HITL review gating for sink tools is implemented with approve/reject paths.

## Project Phases
- Phase 0: Architecture baseline and interfaces
- Phase 1: MCP <-> Ollama local vertical slice
- Phase 2: Deterministic planning + execution hardening (in progress)
- Phase 3: UI integration

## Requirements
- Python 3.10+
- Ollama installed and running locally
- An available local model, default:
  - `ollama pull llama3.2:3b`

## Run
```bash
python3 src/main.py --prompt "Summarize this meeting in 3 bullet points"
```

Optional flags:
```bash
python3 src/main.py --model llama3.2:3b --temperature 0.2 --prompt "hello"
python3 src/main.py --prompt "..." --action-parse
python3 src/main.py --prompt "..." --structured-intent
```

Environment defaults (optional):
```bash
export CAPS_DEFAULT_MODEL="llama3.2:3b"
export CAPS_DEFAULT_TEMPERATURE="0.2"
export CAPS_STRICT_MODE="true"
```

Strict mode can be toggled per run:
```bash
python3 src/main.py --prompt "..." --strict
python3 src/main.py --prompt "..." --no-strict
```

## Regression Gate
Run the focused 6-case regression suite before adapter changes:
```bash
python3 scripts/regression_suite.py
```

Guard-only and policy-only gates:
```bash
python3 scripts/regression_suite.py --guard-only
python3 scripts/regression_suite.py --policy-only
```

## Guard CLI (MVP)
Execute guarded flows from a prompt or a plan:
```bash
python3 scripts/caps_guard.py execute --manifest src/manifest.json --prompt "If weather is below -20C in Toronto, text Jacob I am not coming to university today."
python3 scripts/caps_guard.py execute --manifest src/manifest.json --plan /tmp/caps_guard_rw_demo.json --output-dir /tmp/caps_guard_demo
```

Full-loop prompt review demo (pause + resume on same thread):
```bash
python3 scripts/caps_guard.py execute --manifest src/manifest.json --prompt "If weather is below -20C in Toronto, text Jacob I am not coming to university today." --thread-id guard-demo-1 --sqlite-path .caps_guard_demo.sqlite --output-dir /tmp/guard_demo_block
python3 scripts/caps_guard.py execute --manifest src/manifest.json --resume-review approve --thread-id guard-demo-1 --sqlite-path .caps_guard_demo.sqlite --output-dir /tmp/guard_demo_approve
```

Policy check without execution:
```bash
python3 scripts/caps_guard.py check --manifest src/manifest.json --tool messaging_api --args-json '{"message":"hello"}'
```

### Guard Artifacts
`execute` emits:
- `result.json`
- `trace.json`
- `trace_graph.json`
- `summary.txt`

`trace.json` canonical event types:
- `decision`
- `review_resume`
- `tool_call`
- `tool_result`
- `final_summary`

`trace.json` run provenance fields:
- `run_id`: backward-compatible alias of `current_run_id`
- `current_run_id`: latest execution run id present in events
- `artifact_run_id`: CLI invocation id that wrote the artifact
- `run_ids`: ordered unique run ids seen in `events[*].run_id`

`final_summary.payload` canonical fields:
- `decision_counts`
- `tool_call_count`
- `tool_result_count`
- `tool_error_count`
- `event_count`
- `reviewed_tools`
- `blocked_tools`
- `sink_step_count`
- `execution_result_count`

`execution_result_count` semantics:
- This is phase-local, not global across all prior runs.
- For the blocked/pending-review artifact, it counts safe-phase execution results produced before sink execution.
- For the approve/resume artifact, it counts sink-phase execution results produced during the resumed run.
- Use `trace.json.events` + `run_ids` for full cross-run accounting; use `execution_result_count` for the current artifact phase summary.

`trace_graph.json` is a deterministic nodes/edges view built from `trace.json`:
- `nodes`: one node per event (ordered)
- `edges`: linear `next` edges between ordered nodes
- `summary`: `node_count`, `edge_count`, `event_count`

## Dry-Run Execution
Simulate compiled plan execution without real adapters:
```bash
python3 src/main.py --prompt "If weather is below -20C in Toronto, text Jacob I am not coming to university today." > /tmp/caps_out.json
python3 src/core/execution_dry_run.py --input /tmp/caps_out.json --temp-c -25 --time 16:20
```

## Docs
- Problem definition: `PROBLEM_DEFINITION.md`
- Delivery phases: `PHASES.md`
- System-level flow diagrams: `SYSTEM_FLOW.md`
- Finalized phase-2 architecture/use-cases/backlog: `USE_CASES_AND_FLOW.md`
- Shareable project summary: `SHAREABLE_REPORT.md`
- Resolver contracts: `RESOLVER_CONTRACTS.md`
- RAG integration path: `RAG_integ_path.md`

## General Capability Roadmap
CAPS is being built as a general worker, not a single-use flow.

Planned intent coverage includes:
- Calendar scheduling and updates
- Email summarization and conditional send flows
- Weather/notification automation
- Multi-step API workflows with conditions
- Broader system-integrated task orchestration

Implementation note:
- CAPS keeps a verification-first boundary: parser output is never executed directly.
