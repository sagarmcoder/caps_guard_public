# CAPS Guard Trace Schema (v0.1)

This document defines the advanced trace contract used by CAPS Guard artifacts.

## Decision Outcomes
- `ALLOW`
- `REVIEW_REQUIRED`
- `BLOCK`

## Canonical Reason Codes (bounded set)
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

## Canonical Event Types in `trace.json`
- `decision`
- `tool_call`
- `tool_result`
- `review_resume`
- `final_summary`

## `decision` Event Payload Fields
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

## Top-Level Trace Provenance Fields
- `trace_id`: workflow trace lineage id.
- `run_id`: backward-compatible alias of `current_run_id`.
- `current_run_id`: latest run id present in events.
- `artifact_run_id`: writer run id for this emitted artifact (the local CLI invocation that wrote the file).
- `run_ids`: ordered unique run ids seen in `events[*].run_id`.

## `final_summary.payload` Includes
- `decision_counts`
- `tool_call_count`
- `tool_result_count`
- `tool_error_count`
- `event_count`
- `reviewed_tools`
- `blocked_tools`
- `sink_step_count`
- `execution_result_count`

## `execution_result_count` Contract
- It is phase-local to the current artifact summary (not a global cross-run counter).
- For a blocked/pending-review artifact, it reflects safe-phase results.
- For an approve/resume artifact, it reflects resumed sink-phase results.
- Use `trace.json.events` + `run_ids` for cross-run accounting.

## Stability Statement (v0.1)
- `trace.json` event contract and top-level provenance fields are treated as stable for v0.1.
- New fields may be added, but existing documented fields are not intended to be renamed/removed in v0.1.
