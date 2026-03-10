#!/usr/bin/env python3
import argparse
import html
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from config import (  # noqa: E402
    DEFAULT_CLARIFY_LLM_POLISH,
    DEFAULT_MODEL,
    DEFAULT_STRICT_MODE,
    DEFAULT_TEMPERATURE,
)
from core.execution_runtime import execute_plan, execute_safe_steps, execute_sink_steps  # noqa: E402
from core.manifest_loader import build_manifest_context, load_manifest  # noqa: E402
from core.mcp import MCPRequest, MCPService  # noqa: E402
from core.policy_engine import evaluate_tool_policy  # noqa: E402
from llm.ollama_client import OllamaClient  # noqa: E402


ALLOWED_TRACE_EVENT_TYPES = {"decision", "review_resume", "tool_call", "tool_result", "final_summary"}
EVENT_TYPE_ORDER = {
    "decision": 0,
    "review_resume": 1,
    "tool_call": 2,
    "tool_result": 3,
    "final_summary": 4,
}


def _ensure_output_dir(path: str | None, thread_id: str | None = None) -> Path:
    if path:
        out = Path(path)
    else:
        suffix = thread_id or f"run_{int(time.time())}"
        out = Path("/tmp") / f"caps_guard_{suffix}"
    out.mkdir(parents=True, exist_ok=True)
    return out


def _load_json_file(path: str) -> Dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def _extract_trace_events(state_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    values = state_payload.get("values", {}) or {}
    warm_cache = values.get("warm_cache", {}) or {}
    events = warm_cache.get("trace_events", []) or []
    canonical_events: List[Dict[str, Any]] = []
    for event in events:
        if not isinstance(event, dict):
            continue
        event_type = event.get("event_type")
        if event_type not in ALLOWED_TRACE_EVENT_TYPES:
            continue
        canonical_events.append(
            {
                "event_type": event_type,
                "trace_id": event.get("trace_id"),
                "plan_id": event.get("plan_id"),
                "step_id": event.get("step_id"),
                "tool_name": event.get("tool_name"),
                "timestamp_ms": event.get("timestamp_ms"),
                "payload": event.get("payload", {}) or {},
                "run_id": event.get("run_id"),
            }
        )
    return canonical_events


def _decision_event_from_policy(
    *,
    trace_id: str,
    plan_id: str,
    step_id: str,
    tool_name: str | None,
    decision: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "event_type": "decision",
        "trace_id": trace_id,
        "plan_id": plan_id,
        "step_id": step_id,
        "tool_name": tool_name,
        "timestamp_ms": int(time.time() * 1000),
        "payload": {
            "decision": decision.get("decision"),
            "reason_code": decision.get("reason_code"),
            "rule_id": decision.get("rule_id"),
            "manifest_id": decision.get("manifest_id"),
            "manifest_version": decision.get("manifest_version"),
            "timestamp_ms": decision.get("timestamp_ms"),
            "precedence_resolved": decision.get("precedence_resolved", False),
            "resolution_reason_code": decision.get("resolution_reason_code"),
            "winning_decision": decision.get("winning_decision"),
            "winning_reason_code": decision.get("winning_reason_code"),
            "winning_rule_id": decision.get("winning_rule_id"),
        },
    }


def _merge_trace_events(*event_lists: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    for events in event_lists:
        merged.extend(events or [])
    # Keep only the last final_summary event and place it at the end for stable readability.
    non_final = [e for e in merged if e.get("event_type") != "final_summary"]
    final_events = [e for e in merged if e.get("event_type") == "final_summary"]
    ordered = _stable_sort_trace_events(non_final)
    if final_events:
        ordered.append(_stable_sort_trace_events(final_events)[-1])
    return ordered


def _stable_sort_trace_events(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        events,
        key=lambda e: (
            int(e.get("timestamp_ms") or 0),
            str(e.get("step_id") or ""),
            EVENT_TYPE_ORDER.get(str(e.get("event_type")), 99),
            str(e.get("tool_name") or ""),
        ),
    )


def _summary_from_trace(trace_events: List[Dict[str, Any]]) -> Dict[str, Any]:
    decisions = {"ALLOW": 0, "REVIEW_REQUIRED": 0, "BLOCK": 0}
    tool_call_count = 0
    tool_result_count = 0
    tool_error_count = 0
    for event in trace_events:
        event_type = event.get("event_type")
        payload = event.get("payload", {}) or {}
        if event_type == "decision":
            decision = payload.get("decision")
            if decision in decisions:
                decisions[decision] += 1
        elif event_type == "tool_call":
            tool_call_count += 1
        elif event_type == "tool_result":
            tool_result_count += 1
            if payload.get("ok") is False:
                tool_error_count += 1
    return {
        "decision_counts": decisions,
        "tool_call_count": tool_call_count,
        "tool_result_count": tool_result_count,
        "tool_error_count": tool_error_count,
        "event_count": len(trace_events),
    }


def _audit_metrics_from_trace(trace_events: List[Dict[str, Any]]) -> Dict[str, Any]:
    summary = _summary_from_trace(trace_events)
    reviewed_tools: List[str] = []
    blocked_tools: List[str] = []
    for event in trace_events:
        if event.get("event_type") != "decision":
            continue
        payload = event.get("payload", {}) or {}
        tool_name = event.get("tool_name")
        if not isinstance(tool_name, str) or not tool_name:
            continue
        if payload.get("decision") == "REVIEW_REQUIRED":
            reviewed_tools.append(tool_name)
        if payload.get("decision") == "BLOCK":
            blocked_tools.append(tool_name)
    return {
        **summary,
        "reviewed_tools": sorted(set(reviewed_tools)),
        "blocked_tools": sorted(set(blocked_tools)),
    }


def _normalize_trace_with_audit_summary(trace_events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ordered = _stable_sort_trace_events(trace_events)
    non_final = [e for e in ordered if e.get("event_type") != "final_summary"]
    final_events = [e for e in ordered if e.get("event_type") == "final_summary"]
    metrics = _audit_metrics_from_trace(non_final)
    # Keep contract consistent everywhere: event_count equals len(events) including final_summary.
    metrics["event_count"] = len(non_final) + 1

    if final_events:
        final = dict(final_events[-1])
        payload = dict(final.get("payload", {}) or {})
        # Canonicalize sink count field name and drop legacy alias from emitted artifacts.
        legacy_sink_count = payload.pop("actionable_sink_count", None)
        if "sink_step_count" not in payload and legacy_sink_count is not None:
            payload["sink_step_count"] = legacy_sink_count
        if "sink_step_count" not in payload:
            payload["sink_step_count"] = 0
        payload.update(metrics)
        payload["paused_for_review"] = bool(payload.get("pending_review", False))
        final["payload"] = payload
    else:
        final = {
            "event_type": "final_summary",
            "trace_id": (non_final[-1].get("trace_id") if non_final else None),
            "plan_id": (non_final[-1].get("plan_id") if non_final else None),
            "step_id": None,
            "tool_name": None,
            "timestamp_ms": int(time.time() * 1000),
            "payload": {**metrics, "paused_for_review": False},
        }
    return [*non_final, final]


def _summary_line(summary: Dict[str, Any]) -> str:
    dc = summary.get("decision_counts", {})
    return (
        f"ALLOW={dc.get('ALLOW', 0)} "
        f"REVIEW_REQUIRED={dc.get('REVIEW_REQUIRED', 0)} "
        f"BLOCK={dc.get('BLOCK', 0)} "
        f"tool_calls={summary.get('tool_call_count', 0)} "
        f"tool_results={summary.get('tool_result_count', 0)} "
        f"tool_errors={summary.get('tool_error_count', 0)} "
        f"events={summary.get('event_count', 0)}"
    )


def _build_trace_graph(trace_events: List[Dict[str, Any]]) -> Dict[str, Any]:
    nodes: List[Dict[str, Any]] = []
    edges: List[Dict[str, Any]] = []
    previous_id: str | None = None

    for index, event in enumerate(trace_events):
        node_id = f"event_{index + 1:04d}"
        payload = event.get("payload", {}) or {}
        node = {
            "id": node_id,
            "order": index,
            "event_type": event.get("event_type"),
            "step_id": event.get("step_id"),
            "tool_name": event.get("tool_name"),
            "timestamp_ms": event.get("timestamp_ms"),
            "decision": payload.get("decision"),
            "reason_code": payload.get("reason_code"),
        }
        nodes.append(node)
        if previous_id is not None:
            edges.append(
                {
                    "id": f"edge_{index:04d}",
                    "from": previous_id,
                    "to": node_id,
                    "relation": "next",
                }
            )
        previous_id = node_id

    return {
        "nodes": nodes,
        "edges": edges,
        "summary": {
            "node_count": len(nodes),
            "edge_count": len(edges),
            "event_count": len(trace_events),
        },
    }


def _ordered_run_ids(trace_events: List[Dict[str, Any]]) -> List[str]:
    run_ids: List[str] = []
    seen: set[str] = set()
    for event in trace_events:
        run_id = event.get("run_id")
        if isinstance(run_id, str) and run_id and run_id not in seen:
            seen.add(run_id)
            run_ids.append(run_id)
    return run_ids


def _print_payload(payload: Dict[str, Any], fmt: str) -> None:
    if fmt == "text":
        print(payload.get("summary_line", ""))
        return
    print(json.dumps(payload, indent=2))


def _decision_color(decision: str | None) -> str:
    if decision == "ALLOW":
        return "#2e8b57"
    if decision == "REVIEW_REQUIRED":
        return "#d4a017"
    if decision == "BLOCK":
        return "#c0392b"
    return "#5f6b7a"


def _render_trace_html(trace_payload: Dict[str, Any], title: str) -> str:
    events = trace_payload.get("events", []) or []
    cards: List[str] = []
    edge_blocks: List[str] = []

    for idx, event in enumerate(events):
        payload = event.get("payload", {}) or {}
        decision = payload.get("decision")
        color = _decision_color(decision if isinstance(decision, str) else None)
        event_type = str(event.get("event_type", "unknown"))
        step_id = event.get("step_id")
        tool_name = event.get("tool_name")
        reason_code = payload.get("reason_code")
        rule_id = payload.get("rule_id")
        timestamp = event.get("timestamp_ms")
        run_id = event.get("run_id")

        # Keep params visible for tool_call nodes without flooding the card body.
        args_blob = None
        if event_type == "tool_call":
            args_blob = payload.get("params")
        tooltip_payload = {
            "event_type": event_type,
            "decision": decision,
            "reason_code": reason_code,
            "rule_id": rule_id,
            "step_id": step_id,
            "tool_name": tool_name,
            "timestamp_ms": timestamp,
            "run_id": run_id,
            "args": args_blob,
        }
        tooltip = html.escape(json.dumps(tooltip_payload, indent=2, sort_keys=True))

        cards.append(
            (
                '<div class="event-card" style="border-left: 6px solid {color}" title="{tooltip}">'
                '<div class="event-head">#{idx} {event_type}</div>'
                '<div class="event-meta"><b>step</b>: {step}</div>'
                '<div class="event-meta"><b>tool</b>: {tool}</div>'
                '<div class="event-meta"><b>decision</b>: {decision}</div>'
                '<div class="event-meta"><b>reason</b>: {reason}</div>'
                '<div class="event-meta"><b>rule</b>: {rule}</div>'
                '</div>'
            ).format(
                color=color,
                tooltip=tooltip,
                idx=idx + 1,
                event_type=html.escape(event_type),
                step=html.escape(str(step_id)),
                tool=html.escape(str(tool_name)),
                decision=html.escape(str(decision)),
                reason=html.escape(str(reason_code)),
                rule=html.escape(str(rule_id)),
            )
        )

        if idx < len(events) - 1:
            edge_blocks.append(
                (
                    '<div class="edge-row">'
                    '<span class="edge-line" style="background:{color}"></span>'
                    '<span class="edge-label">{label}</span>'
                    "</div>"
                ).format(
                    color=color,
                    label=html.escape(str(decision if isinstance(decision, str) else event_type)),
                )
            )

    summary = trace_payload.get("summary", {}) or {}
    summary_blob = html.escape(json.dumps(summary, indent=2, sort_keys=True))
    trace_id = html.escape(str(trace_payload.get("trace_id")))
    current_run_id = html.escape(str(trace_payload.get("current_run_id", trace_payload.get("run_id"))))
    run_ids = html.escape(json.dumps(trace_payload.get("run_ids", [])))

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{html.escape(title)}</title>
  <style>
    body {{
      font-family: ui-sans-serif, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      margin: 0;
      background: #f7f8fa;
      color: #1f2937;
    }}
    .wrap {{ max-width: 1080px; margin: 0 auto; padding: 24px; }}
    .header {{
      background: white;
      border: 1px solid #e5e7eb;
      border-radius: 12px;
      padding: 16px;
      margin-bottom: 18px;
    }}
    .meta {{ font-size: 13px; color: #4b5563; line-height: 1.6; }}
    .summary {{
      background: #111827;
      color: #e5e7eb;
      border-radius: 12px;
      padding: 14px;
      white-space: pre-wrap;
      font-size: 12px;
      margin-top: 12px;
    }}
    .event-card {{
      background: white;
      border: 1px solid #e5e7eb;
      border-radius: 10px;
      padding: 12px 14px;
      margin: 10px 0;
    }}
    .event-head {{ font-weight: 700; margin-bottom: 6px; }}
    .event-meta {{ font-size: 13px; color: #374151; margin: 2px 0; }}
    .edge-row {{
      display: flex;
      align-items: center;
      gap: 8px;
      margin: 4px 0 10px 8px;
    }}
    .edge-line {{
      display: inline-block;
      width: 36px;
      height: 4px;
      border-radius: 999px;
    }}
    .edge-label {{
      font-size: 12px;
      color: #6b7280;
      text-transform: uppercase;
      letter-spacing: .02em;
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="header">
      <h1 style="margin:0 0 8px 0;">{html.escape(title)}</h1>
      <div class="meta"><b>trace_id:</b> {trace_id}</div>
      <div class="meta"><b>current_run_id:</b> {current_run_id}</div>
      <div class="meta"><b>run_ids:</b> {run_ids}</div>
      <div class="meta"><b>events:</b> {len(events)}</div>
      <div class="summary">{summary_blob}</div>
    </div>
    {''.join(cards[:1] + [item for pair in zip(edge_blocks, cards[1:]) for item in pair] if cards else [])}
  </div>
</body>
</html>
"""


def _run_render_trace(args: argparse.Namespace) -> int:
    trace_payload = _load_json_file(args.trace)
    events = trace_payload.get("events")
    if not isinstance(events, list):
        raise ValueError("trace file must contain an 'events' list")

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    html_payload = _render_trace_html(trace_payload, title=args.title)
    output_path.write_text(html_payload, encoding="utf-8")

    response = {
        "mode": "render-trace",
        "artifacts": {
            "trace_json": str(Path(args.trace)),
            "html": str(output_path),
        },
        "summary_line": f"rendered {len(events)} events to {output_path}",
    }
    _print_payload(response, args.format)
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CAPS Guard CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    p_exec = sub.add_parser("execute", help="Run guarded execution from prompt or plan.")
    p_exec.add_argument("--manifest", default="src/manifest.json", help="Manifest JSON path")
    p_exec.add_argument("--prompt", default=None, help="Prompt to run through CAPS LangGraph")
    p_exec.add_argument(
        "--plan",
        default=None,
        help="Path to JSON plan payload. Accepted: {execution_plan, task_graph} or raw execution_plan",
    )
    p_exec.add_argument("--user-id", default="local-user")
    p_exec.add_argument("--model", default=DEFAULT_MODEL)
    p_exec.add_argument("--temperature", type=float, default=DEFAULT_TEMPERATURE)
    p_exec.add_argument("--strict", action=argparse.BooleanOptionalAction, default=DEFAULT_STRICT_MODE)
    p_exec.add_argument(
        "--clarify-llm-polish",
        action=argparse.BooleanOptionalAction,
        default=DEFAULT_CLARIFY_LLM_POLISH,
    )
    p_exec.add_argument(
        "--execute-live",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Execute runtime steps (default true for guard trace visibility)",
    )
    p_exec.add_argument("--thread-id", default=None, help="LangGraph thread id for prompt mode")
    p_exec.add_argument("--sqlite-path", default=".caps_guard.sqlite", help="SQLite state path")
    p_exec.add_argument(
        "--resume-review",
        choices=["approve", "reject"],
        default=None,
        help="Resume a paused prompt-mode review for the given --thread-id.",
    )
    p_exec.add_argument(
        "--approve-sinks",
        action="store_true",
        help="For plan mode, approve actionable sink execution after REVIEW_REQUIRED decisions.",
    )
    p_exec.add_argument("--output-dir", default=None, help="Artifact directory (trace/result/summary)")
    p_exec.add_argument("--format", choices=["json", "text"], default="json")

    p_check = sub.add_parser("check", help="Evaluate policy decision without executing tools.")
    p_check.add_argument("--manifest", default="src/manifest.json", help="Manifest JSON path")
    p_check.add_argument("--tool", required=True, help="Tool name to evaluate")
    p_check.add_argument("--step-id", default="check_step", help="Synthetic step id")
    p_check.add_argument("--args-json", default="{}", help="Inline JSON args payload")
    p_check.add_argument("--args-file", default=None, help="Args JSON file path")
    p_check.add_argument(
        "--approved-for-sink",
        action="store_true",
        help="Set true to simulate post-review sink execution",
    )
    p_check.add_argument("--output-dir", default=None, help="Artifact directory")
    p_check.add_argument("--format", choices=["json", "text"], default="json")

    p_render = sub.add_parser("render-trace", help="Render trace.json into a shareable HTML view.")
    p_render.add_argument("--trace", required=True, help="Path to trace.json artifact")
    p_render.add_argument("--output", required=True, help="Output HTML path")
    p_render.add_argument("--title", default="CAPS Guard Trace", help="HTML page title")
    p_render.add_argument("--format", choices=["json", "text"], default="json")

    return parser


def _run_execute(args: argparse.Namespace) -> int:
    if args.resume_review:
        if args.plan:
            raise ValueError("--resume-review cannot be used with --plan.")
        if args.prompt:
            raise ValueError("--resume-review cannot be used with --prompt.")
        if not args.thread_id:
            raise ValueError("--resume-review requires --thread-id.")
    elif bool(args.prompt) == bool(args.plan):
        raise ValueError("Use exactly one of --prompt or --plan.")

    out_dir = _ensure_output_dir(args.output_dir, args.thread_id)
    artifact_run_id = f"run_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"
    manifest = load_manifest(args.manifest)
    manifest_context = build_manifest_context(manifest)

    if args.prompt or args.resume_review:
        llm = OllamaClient(model=args.model)
        mcp = MCPService(
            llm_client=llm,
            strict_mode=args.strict,
            clarify_llm_polish=args.clarify_llm_polish,
        )
        thread_id = args.thread_id or f"{args.user_id}:guard"
        if args.resume_review:
            result = mcp.resume_action_parse_langgraph(
                thread_id=thread_id,
                sqlite_path=args.sqlite_path,
                decision=args.resume_review,
            )
        else:
            request = MCPRequest(user_id=args.user_id, prompt=args.prompt, temperature=args.temperature)
            result = mcp.process_action_parse_langgraph(
                request=request,
                execute_live=args.execute_live,
                thread_id=thread_id,
                sqlite_path=args.sqlite_path,
                manifest_path=args.manifest,
            )
        state_payload = mcp.get_action_parse_langgraph_state(
            thread_id=thread_id,
            sqlite_path=args.sqlite_path,
        )
        trace_events = _extract_trace_events(state_payload)
        state_header = state_payload.get("state_header", {}) or {}
    else:
        payload = _load_json_file(args.plan)
        execution_plan = payload.get("execution_plan", payload)
        task_graph = payload.get("task_graph", {"tasks": payload.get("tasks", [])})
        request_context = {
            "user_id": args.user_id,
            "context_mode": "stub",
            "manifest_context": manifest_context,
        }

        if args.execute_live:
            safe_runtime = execute_safe_steps(
                execution_plan=execution_plan,
                task_graph=task_graph,
                request_context=request_context,
            )
            plan_id = execution_plan.get("plan_id", "plan_unknown")
            trace_id = safe_runtime.get("trace_id")
            safe_trace = list(safe_runtime.get("trace_events", []))
            actionable_sink_steps = list(safe_runtime.get("actionable_sink_steps", []))
            review_decisions: List[Dict[str, Any]] = []
            for sink in actionable_sink_steps:
                decision = evaluate_tool_policy(
                    step_id=sink.get("id", "step"),
                    tool_name=sink.get("tool_name"),
                    params={},
                    manifest_context=manifest_context,
                    approved_for_sink=False,
                    trace_id=trace_id,
                )
                review_decisions.append(
                    _decision_event_from_policy(
                        trace_id=trace_id,
                        plan_id=plan_id,
                        step_id=sink.get("id", "step"),
                        tool_name=sink.get("tool_name"),
                        decision=decision,
                    )
                )

            sink_runtime = None
            if actionable_sink_steps and args.approve_sinks:
                sink_runtime = execute_sink_steps(
                    execution_plan=execution_plan,
                    task_graph=task_graph,
                    request_context={
                        **request_context,
                        "trace_id": trace_id,
                    },
                    safe_runtime=safe_runtime,
                    approved_for_sink=True,
                    emit_decision_events=False,
                )

            if sink_runtime:
                runtime_execution = {
                    "trace_id": trace_id,
                    "plan_id": plan_id,
                    "blocked": bool(sink_runtime.get("blocked", False)),
                    "execution_results": list(safe_runtime.get("execution_results", []))
                    + list(sink_runtime.get("execution_results", [])),
                    "condition_state": sink_runtime.get("condition_state", {}),
                    "safe_runtime_execution": safe_runtime,
                    "sink_runtime_execution": sink_runtime,
                    "review_required": len(actionable_sink_steps) > 0,
                    "review_decision": "approve",
                }
                trace_events = _merge_trace_events(
                    safe_trace,
                    review_decisions,
                    list(sink_runtime.get("trace_events", [])),
                )
            else:
                runtime_execution = {
                    "trace_id": trace_id,
                    "plan_id": plan_id,
                    "blocked": bool(safe_runtime.get("blocked", False)) or len(actionable_sink_steps) > 0,
                    "execution_results": list(safe_runtime.get("execution_results", [])),
                    "condition_state": safe_runtime.get("condition_state", {}),
                    "safe_runtime_execution": safe_runtime,
                    "sink_runtime_execution": None,
                    "review_required": len(actionable_sink_steps) > 0,
                    "review_decision": "pending" if len(actionable_sink_steps) > 0 else None,
                }
                trace_events = _merge_trace_events(safe_trace, review_decisions)
            state_header = {
                "manifest_id": manifest_context.get("manifest_id"),
                "manifest_version": manifest_context.get("manifest_version"),
                "trace_id": trace_id,
            }
        else:
            runtime_execution = execute_plan(
                execution_plan=execution_plan,
                task_graph=task_graph,
                request_context=request_context,
            )
            trace_events = []
            for event in runtime_execution.get("trace_events", []) or []:
                if event.get("event_type") in ALLOWED_TRACE_EVENT_TYPES:
                    trace_events.append(event)
            state_header = {
                "manifest_id": manifest_context.get("manifest_id"),
                "manifest_version": manifest_context.get("manifest_version"),
                "trace_id": runtime_execution.get("trace_id"),
            }

        result = {
            "execution_plan": execution_plan,
            "task_graph": task_graph,
            "runtime_execution": runtime_execution,
            "manifest_context": manifest_context,
        }

    trace_events = _normalize_trace_with_audit_summary(trace_events)
    trace_events = [
        (
            event
            if isinstance(event.get("run_id"), str) and event.get("run_id")
            else {**event, "run_id": artifact_run_id}
        )
        for event in trace_events
    ]
    run_ids = _ordered_run_ids(trace_events)
    current_run_id = run_ids[-1] if run_ids else artifact_run_id
    summary = _summary_from_trace(trace_events)
    summary_line = _summary_line(summary)
    trace_graph = _build_trace_graph(trace_events)
    trace_id = next((event.get("trace_id") for event in trace_events if event.get("trace_id")), None)

    _write_json(out_dir / "result.json", result)
    _write_json(
        out_dir / "trace.json",
        {
            # Backward-compatible alias for existing consumers.
            "run_id": current_run_id,
            "current_run_id": current_run_id,
            "artifact_run_id": artifact_run_id,
            "run_ids": run_ids,
            "trace_id": trace_id,
            "events": trace_events,
            "summary": summary,
        },
    )
    _write_json(out_dir / "trace_graph.json", trace_graph)
    with (out_dir / "summary.txt").open("w", encoding="utf-8") as f:
        f.write(summary_line + "\n")

    response = {
        "mode": "execute",
        # Backward-compatible alias for existing consumers.
        "run_id": current_run_id,
        "current_run_id": current_run_id,
        "artifact_run_id": artifact_run_id,
        "run_ids": run_ids,
        "artifacts": {
            "output_dir": str(out_dir),
            "result_json": str(out_dir / "result.json"),
            "trace_json": str(out_dir / "trace.json"),
            "trace_graph_json": str(out_dir / "trace_graph.json"),
            "summary_txt": str(out_dir / "summary.txt"),
        },
        "manifest_id": manifest_context.get("manifest_id"),
        "manifest_version": manifest_context.get("manifest_version"),
        "state_header": state_header,
        "summary": summary,
        "summary_line": summary_line,
    }
    _print_payload(response, args.format)
    return 0


def _run_check(args: argparse.Namespace) -> int:
    out_dir = _ensure_output_dir(args.output_dir)
    run_id = f"run_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"
    manifest = load_manifest(args.manifest)
    manifest_context = build_manifest_context(manifest)

    if args.args_file:
        params = _load_json_file(args.args_file)
    else:
        params = json.loads(args.args_json)
        if not isinstance(params, dict):
            raise ValueError("--args-json must decode to an object")

    trace_id = f"trace_check_{int(time.time() * 1000)}"
    decision = evaluate_tool_policy(
        step_id=args.step_id,
        tool_name=args.tool,
        params=params,
        manifest_context=manifest_context,
        approved_for_sink=args.approved_for_sink,
        trace_id=trace_id,
    )

    _write_json(out_dir / "check_result.json", {"decision": decision, "params": params})
    summary_line = (
        f"{decision.get('decision')} {decision.get('reason_code')} "
        f"tool={decision.get('tool_name')} rule={decision.get('rule_id')}"
    )
    with (out_dir / "summary.txt").open("w", encoding="utf-8") as f:
        f.write(summary_line + "\n")

    response = {
        "mode": "check",
        "run_id": run_id,
        "decision": decision,
        "summary_line": summary_line,
        "artifacts": {
            "output_dir": str(out_dir),
            "check_result_json": str(out_dir / "check_result.json"),
            "summary_txt": str(out_dir / "summary.txt"),
        },
    }
    _print_payload(response, args.format)
    return 0


def main() -> int:
    args = _build_parser().parse_args()
    if args.command == "execute":
        return _run_execute(args)
    if args.command == "check":
        return _run_check(args)
    if args.command == "render-trace":
        return _run_render_trace(args)
    raise ValueError(f"Unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
