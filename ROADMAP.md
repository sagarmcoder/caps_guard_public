# CAPS Guard Roadmap

## v0.1 Completed Steps
- Built deterministic guard decisions at tool execution boundary:
  - `ALLOW`, `REVIEW_REQUIRED`, `BLOCK`.
- Added manifest-driven policy evaluation (`manifest*.json`) with side-effect classes.
- Implemented HITL pause/resume flow for actionable sink steps.
- Added canonical trace artifacts:
  - `trace.json`
  - `trace_graph.json`
- Added CLI commands:
  - `execute`
  - `check`
  - `render-trace`
- Added regression coverage for:
  - guard CLI contract
  - side-effect classification
  - argument pattern blocking
  - trace renderer output
- Added runnable LangGraph demo:
  - `examples/langgraph_demo/`

## Current Scope (v0.1)
- Standalone guardrail + audit layer for tool-calling workflows.
- Local-first execution and review flow.
- Docker-supported local usage.

## Next Steps (Post-v0.1)
- Environment-aware policy rules (`dev`/`prod`) with regression coverage.
- Branch-aware graph artifact version while preserving `trace_graph.json` compatibility.
- Renderer UX improvements (labels, filtering, export polish).
- Expanded integration demos and adapter examples.

## Out of Scope (v0.1)
- Hosted review platform and transport integrations (webhook/queue/polling).
- Full enterprise deployment framework.
