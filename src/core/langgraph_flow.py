from contextlib import ExitStack
from pathlib import Path
import time
import uuid
from typing import Any, Dict, TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.types import Command, interrupt

from core.manifest_loader import build_manifest_context, load_manifest


from core.task_graph_builder import build_task_graph_from_cir
from core.task_graph_compiler import compile_task_graph
from core.task_graph_verifier import verify_task_graph
from core.execution_runtime import execute_plan, execute_safe_steps, execute_sink_steps
from core.policy_engine import DECISION_BLOCK, DECISION_REVIEW, evaluate_tool_policy




class CAPSGraphState(TypedDict, total=False):
    """State schema for the CAPS processing graph.
    """

    request_payload: Dict[str, Any]
    run_id: str | None
    execute_live: bool
    manifest_context: Dict[str, Any]
    context: Dict[str, Any]
    action_parse: Dict[str, Any] | None
    task_graph: Dict[str, Any]
    task_graph_verification: Dict[str, Any]
    execution_plan: Dict[str, Any]
    runtime_execution: Dict[str, Any] | None
    safe_runtime_execution: Dict[str, Any] | None
    hot_state: Dict[str, Any]
    warm_cache: Dict[str, Any]
    cold_refs: Dict[str, Any]







def build_sqlite_checkpointer(db_path: str, stack: ExitStack):
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return stack.enter_context(SqliteSaver.from_conn_string(str(path)))

def _request_payload_to_request(payload: Dict[str, Any]):
    from core.mcp import MCPRequest

    return MCPRequest(
        user_id=str(payload.get("user_id", "local-user")),
        prompt=str(payload.get("prompt", "")),
        temperature=float(payload.get("temperature", 0.0)),
    )


def _rebuild_action_parse_from_task_graph(
    task_graph: Dict[str, Any],
    request_payload: Dict[str, Any],
    cold_refs: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    graph_id = str(task_graph.get("graph_id", "graph"))
    suffix = graph_id.split("_")[-1] if "_" in graph_id else graph_id
    parse_id = None
    if isinstance(cold_refs, dict):
        parse_id = cold_refs.get("parse_id")
    if not isinstance(parse_id, str) or not parse_id.strip():
        parse_id = f"parse_reconstructed_{suffix}"
    return {
        "schema_version": "1.0",
        "parse_id": parse_id,
        "tasks": list(task_graph.get("tasks", [])),
        "clarification_questions": list(task_graph.get("clarification_questions", [])),
        "notes": [],
        "source_prompt": str(request_payload.get("prompt", "")),
    }

SINK_STEP_TYPES = {"send_message", "send_email", "schedule_event"}


def _active_plan_pointers(execution_plan: Dict[str, Any]) -> list[Dict[str, Any]]:
    steps = execution_plan.get("steps", [])
    return [
        {
            "id": step.get("id"),
            "type": step.get("type"),
            "tool_name": step.get("tool_name"),
            "condition": step.get("condition"),
        }
        for step in steps
    ]


def _bucket_defaults(bucket_name: str) -> Dict[str, Any]:
    if bucket_name == "hot_state":
        return {
            "source_prompt": "",
            "execute_live": False,
            "context_mode": None,
            "active_plan": [],
            "condition_state": {},
            "actionable_sink_steps": [],
            "review_required": False,
            "review_decision": None,
            "current_run_id": None,
            "active_policy_flags": [],
            "active_tools": [],
            "sink_tools": [],
        }
    if bucket_name == "warm_cache":
        return {
            "location": {},
            "weather": {},
            "identity": {},
            "review_summary": None,
            "review_response": None,
            "last_action_result": None,
            "memory_digest": None,
            "trace_events": [],
        }
    if bucket_name == "cold_refs":
        return {
            "parse_id": None,
            "task_graph_id": None,
            "verification_valid": None,
            "verification_issue_count": None,
            "plan_id": None,
        }
    return {}


def _merge_bucket(state: CAPSGraphState, bucket_name: str, updates: Dict[str, Any]) -> Dict[str, Any]:
    bucket = _bucket_defaults(bucket_name)
    bucket.update(dict(state.get(bucket_name, {})))
    bucket.update(updates)
    return bucket


def _last_action_result(execution_results: list[Dict[str, Any]]) -> Dict[str, Any] | None:
    for step in reversed(execution_results):
        status = step.get("status")
        if status in {"skipped_by_condition", "ok", "blocked_policy_violation"} or str(status).startswith("failed_") or str(status).startswith("blocked_"):
            return {
                "step_id": step.get("step_id"),
                "type": step.get("type"),
                "status": status,
            }
    return None


def _sink_policy_params(
    step: Dict[str, Any],
    task_graph: Dict[str, Any],
    safe_runtime_execution: Dict[str, Any] | None,
) -> Dict[str, Any]:
    runtime_state = (safe_runtime_execution or {}).get("runtime_state", {}) or {}
    identity = runtime_state.get("identity", {}) or {}
    tasks = list(task_graph.get("tasks", []))
    step_type = step.get("type")

    if step_type == "send_message":
        send_task = next((t for t in tasks if t.get("action") == "send_message"), {})
        params = dict(send_task.get("params", {}))
        if "recipient_ref" in identity:
            params["recipient_ref"] = identity.get("recipient_ref")
        if "recipient_id" in identity:
            params["recipient_id"] = identity.get("recipient_id")
        if "recipient_resolved" in identity:
            params["recipient_resolved"] = identity.get("recipient_resolved")
        return params

    if step_type == "send_email":
        email_task = next((t for t in tasks if t.get("action") == "send_email"), {})
        params = dict(email_task.get("params", {}))
        if "recipient_ref" in identity:
            params["recipient_ref"] = identity.get("recipient_ref")
        if "recipient_id" in identity:
            params["recipient_id"] = identity.get("recipient_id")
        if "recipient_resolved" in identity:
            params["recipient_resolved"] = identity.get("recipient_resolved")
        return params

    if step_type == "schedule_event":
        schedule_task = next((t for t in tasks if t.get("action") == "schedule_meeting"), {})
        return dict(schedule_task.get("params", {}))

    return {}


def _event_from_policy_decision(
    *,
    trace_id: str | None,
    plan_id: str,
    step: Dict[str, Any],
    decision: Dict[str, Any],
    run_id: str | None = None,
) -> Dict[str, Any]:
    return {
        "event_type": "decision",
        "trace_id": trace_id,
        "plan_id": plan_id,
        "step_id": step.get("id"),
        "tool_name": step.get("tool_name"),
        "timestamp_ms": decision.get("timestamp_ms"),
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
        "run_id": run_id,
    }


def _splice_events_before_final_summary(
    trace_events: list[Dict[str, Any]],
    new_events: list[Dict[str, Any]],
) -> list[Dict[str, Any]]:
    if not new_events:
        return list(trace_events)
    base = list(trace_events)
    final_idx = next((i for i, e in enumerate(base) if e.get("event_type") == "final_summary"), None)
    if final_idx is None:
        return [*base, *new_events]
    return [*base[:final_idx], *new_events, *base[final_idx:]]


def _audit_metrics_from_trace_events(trace_events: list[Dict[str, Any]]) -> Dict[str, Any]:
    decision_counts = {"ALLOW": 0, "REVIEW_REQUIRED": 0, "BLOCK": 0}
    tool_call_count = 0
    tool_result_count = 0
    tool_error_count = 0
    reviewed_tools: list[str] = []
    blocked_tools: list[str] = []

    for event in trace_events:
        if not isinstance(event, dict):
            continue
        event_type = event.get("event_type")
        payload = event.get("payload", {}) or {}
        tool_name = event.get("tool_name")
        if event_type == "decision":
            decision = payload.get("decision")
            if decision in decision_counts:
                decision_counts[decision] += 1
            if decision == "REVIEW_REQUIRED" and isinstance(tool_name, str) and tool_name:
                reviewed_tools.append(tool_name)
            if decision == "BLOCK" and isinstance(tool_name, str) and tool_name:
                blocked_tools.append(tool_name)
        elif event_type == "tool_call":
            tool_call_count += 1
        elif event_type == "tool_result":
            tool_result_count += 1
            if payload.get("ok") is False:
                tool_error_count += 1

    return {
        "decision_counts": decision_counts,
        "tool_call_count": tool_call_count,
        "tool_result_count": tool_result_count,
        "tool_error_count": tool_error_count,
        "reviewed_tools": sorted(set(reviewed_tools)),
        "blocked_tools": sorted(set(blocked_tools)),
    }


def _route_after_human_review(state: "CAPSGraphState") -> str:
    decision = (state.get("hot_state", {}).get("review_decision") or "").strip().lower()
    if decision == "approve":
        return "execute"
    return "finalize"


def _build_deterministic_runtime_summary(
    task_graph: Dict[str, Any],
    safe_runtime: Dict[str, Any] | None,
    runtime_execution: Dict[str, Any] | None,
    review_summary: Dict[str, Any] | None,
    review_decision: str | None,
) -> str | None:
    send_task = next(
        (t for t in task_graph.get("tasks", []) if t.get("action") in {"send_message", "send_email"}),
        None,
    )
    recipient_ref = None
    if isinstance(send_task, dict):
        recipient_ref = send_task.get("params", {}).get("recipient_ref")
    recipient_label = str(recipient_ref) if isinstance(recipient_ref, str) and recipient_ref.strip() else "the recipient"

    if runtime_execution and runtime_execution.get("blocked"):
        sink_count = len((review_summary or {}).get("sink_steps", []))
        return f"Approval required before executing {sink_count} side-effect step(s)."

    if (review_decision or "").strip().lower() == "reject":
        return "Execution rejected during human review. No side-effect actions were executed."

    if runtime_execution and runtime_execution.get("execution_results"):
        results = runtime_execution.get("execution_results", [])
        send_ok = any(
            step.get("type") == "send_message" and step.get("status") == "ok"
            for step in results
        )
        email_ok = any(
            step.get("type") == "send_email" and step.get("status") == "ok"
            for step in results
        )
        schedule_ok = any(
            step.get("type") == "schedule_event" and step.get("status") == "ok"
            for step in results
        )
        if send_ok:
            return f"Condition met. Message sent to {recipient_label}."
        if email_ok:
            return f"Condition met. Email sent to {recipient_label}."
        if schedule_ok:
            return "Condition met. Meeting scheduled."

    if safe_runtime:
        condition_state = safe_runtime.get("condition_state", {})
        actionable_sinks = safe_runtime.get("actionable_sink_steps", [])
        if not actionable_sinks and any(v is False for v in condition_state.values()):
            temp_c = safe_runtime.get("runtime_state", {}).get("weather", {}).get("temperature_c")
            if isinstance(temp_c, (int, float)) and send_task and send_task.get("action") == "send_message":
                return f"Weather is {temp_c:.1f}C; condition not met. No message sent to {recipient_label}."
            return "Condition not met. No side-effect actions were executed."

    return None


def _build_state_header(values: Dict[str, Any]) -> Dict[str, Any]:
    hot_state = _bucket_defaults("hot_state")
    hot_state.update(dict(values.get("hot_state", {})))
    warm_cache = _bucket_defaults("warm_cache")
    warm_cache.update(dict(values.get("warm_cache", {})))
    cold_refs = _bucket_defaults("cold_refs")
    cold_refs.update(dict(values.get("cold_refs", {})))
    manifest_context = dict(values.get("manifest_context", {}))
    trace_id = None
    runtime_execution = values.get("runtime_execution", {}) or {}
    safe_runtime_execution = values.get("safe_runtime_execution", {}) or {}
    if isinstance(runtime_execution, dict):
        trace_id = runtime_execution.get("trace_id")
    if not trace_id and isinstance(safe_runtime_execution, dict):
        trace_id = safe_runtime_execution.get("trace_id")
    if not trace_id:
        events = warm_cache.get("trace_events", []) or []
        if isinstance(events, list) and events:
            trace_id = events[0].get("trace_id")
    return {
        "initialized": bool(values),
        "source_prompt": hot_state.get("source_prompt"),
        "active_plan": hot_state.get("active_plan"),
        "condition_state": hot_state.get("condition_state"),
        "actionable_sink_steps": hot_state.get("actionable_sink_steps"),
        "review_required": hot_state.get("review_required"),
        "review_decision": hot_state.get("review_decision"),
        "last_action_result": warm_cache.get("last_action_result"),
        "memory_digest": warm_cache.get("memory_digest"),
        "identity": warm_cache.get("identity"),
        "location": warm_cache.get("location"),
        "weather": warm_cache.get("weather"),
        "parse_id": cold_refs.get("parse_id"),
        "plan_id": cold_refs.get("plan_id"),
        "task_graph_id": cold_refs.get("task_graph_id"),
        "verification_valid": cold_refs.get("verification_valid"),
        "verification_issue_count": cold_refs.get("verification_issue_count"),
        "manifest_id": manifest_context.get("manifest_id"),
        "manifest_version": manifest_context.get("manifest_version"),
        "active_policy_flags": hot_state.get("active_policy_flags", []),
        "active_tools": hot_state.get("active_tools", []),
        "sink_tools": hot_state.get("sink_tools", []),
        "trace_id": trace_id,
    }




def acquire_context_node(state: CAPSGraphState, service: Any) -> Dict[str, Any]:
    request = _request_payload_to_request(state["request_payload"])
    context = service._acquire_context(request)
    hot_state = _merge_bucket(
        state,
        "hot_state",
        {
            "source_prompt": request.prompt,
            "execute_live": bool(state.get("execute_live", False)),
            "context_mode": context["context_mode"],
            # New prompt invocations must start with a clean review/runtime view.
            "active_plan": [],
            "condition_state": {},
            "actionable_sink_steps": [],
            "review_required": False,
            "review_decision": None,
            "current_run_id": state.get("run_id") or state.get("hot_state", {}).get("current_run_id"),
            "active_policy_flags": list(state.get("manifest_context", {}).get("active_policy_flags", [])),
            "active_tools": list(state.get("manifest_context", {}).get("active_tools", [])),
            "sink_tools": list(state.get("manifest_context", {}).get("sink_tools", [])),
        },
    )
    warm_cache = _merge_bucket(
        state,
        "warm_cache",
        {
            "location": dict(state.get("warm_cache", {}).get("location", {})),
            "weather": dict(state.get("warm_cache", {}).get("weather", {})),
            "identity": dict(state.get("warm_cache", {}).get("identity", {})),
            "review_summary": None,
            "review_response": None,
            "last_action_result": state.get("warm_cache", {}).get("last_action_result"),
            "memory_digest": None,
            "trace_events": [],
        },
    )
    cold_refs = _merge_bucket(
        state,
        "cold_refs",
        {
            "parse_id": state.get("cold_refs", {}).get("parse_id"),
            "task_graph_id": state.get("cold_refs", {}).get("task_graph_id"),
            "verification_valid": state.get("cold_refs", {}).get("verification_valid"),
            "verification_issue_count": state.get("cold_refs", {}).get("verification_issue_count"),
            "plan_id": state.get("cold_refs", {}).get("plan_id"),
        },
    )
    return {
        "context": context,
        "runtime_execution": None,
        "safe_runtime_execution": None,
        "hot_state": hot_state,
        "warm_cache": warm_cache,
        "cold_refs": cold_refs,
    }


def action_parse_node(state: CAPSGraphState, service: Any) -> Dict[str, Any]:
    request = _request_payload_to_request(state["request_payload"])
    context = state["context"]
    parsed = service._parse_action_request(request, context)
    cold_refs = _merge_bucket(
        state,
        "cold_refs",
        {
            "parse_id": parsed.get("parse_id"),
        },
    )
    return {"action_parse": parsed, "cold_refs": cold_refs}


def reconcile_node(state: CAPSGraphState, service: Any) -> Dict[str, Any]:
    request = _request_payload_to_request(state["request_payload"])
    parsed = state["action_parse"]

    parsed = service._reconcile_action_parse(parsed, request.prompt)

    if not parsed.get("tasks") and not parsed.get("clarification_questions"):
        parsed["clarification_questions"] = [
            "Can you clarify what exact action CAPS should perform?"
        ]

    cold_refs = _merge_bucket(
        state,
        "cold_refs",
        {
            "parse_id": parsed.get("parse_id"),
        },
    )
    return {"action_parse": parsed, "cold_refs": cold_refs}


def cir_node(state: CAPSGraphState, service: Any) -> Dict[str, Any]:
    request = _request_payload_to_request(state["request_payload"])
    parsed = state["action_parse"]
    cir = service._build_cir_with_fallback(parsed, request.prompt)
    task_graph = build_task_graph_from_cir(cir, request.prompt)
    cold_refs = _merge_bucket(
        state,
        "cold_refs",
        {
            "task_graph_id": task_graph.get("graph_id"),
        },
    )
    return {
        "action_parse": None,
        "task_graph": task_graph,
        "cold_refs": cold_refs,
    }



def task_graph_node(state: CAPSGraphState, service: Any) -> Dict[str, Any]:
    return {"task_graph": state["task_graph"]}



def clarification_polish_node(state: CAPSGraphState, service: Any) -> Dict[str, Any]:
    request = _request_payload_to_request(state["request_payload"])
    task_graph = state["task_graph"]

    if service.clarify_llm_polish and task_graph.get("needs_clarification"):
        task_graph["clarification_questions"] = service._polish_clarification_questions(
            task_graph.get("clarification_questions", []),
            request.prompt,
        )

    return {"task_graph": task_graph}


def verify_node(state: CAPSGraphState, service: Any) -> Dict[str, Any]:
    request = _request_payload_to_request(state["request_payload"])
    task_graph = state["task_graph"]
    verification = verify_task_graph(task_graph, request.prompt)
    cold_refs = _merge_bucket(
        state,
        "cold_refs",
        {
            "task_graph_id": task_graph.get("graph_id"),
            "verification_valid": verification.get("valid"),
            "verification_issue_count": len(verification.get("issues", [])),
        },
    )
    return {"task_graph_verification": verification, "cold_refs": cold_refs}


def compile_node(state: CAPSGraphState, service: Any) -> Dict[str, Any]:
    task_graph = state["task_graph"]
    verification = state["task_graph_verification"]
    execution_plan = compile_task_graph(task_graph, verification)
    hot_state = _merge_bucket(
        state,
        "hot_state",
        {
            "active_plan": _active_plan_pointers(execution_plan),
        },
    )
    cold_refs = _merge_bucket(
        state,
        "cold_refs",
        {
            "plan_id": execution_plan.get("plan_id"),
        },
    )
    return {"execution_plan": execution_plan, "hot_state": hot_state, "cold_refs": cold_refs}

def execute_safe_node(state: CAPSGraphState, service: Any) -> Dict[str, Any]:
    request = _request_payload_to_request(state["request_payload"])
    execution_plan = state["execution_plan"]
    task_graph = state["task_graph"]
    execute_live = state.get("execute_live", False)

    safe_runtime_execution = None
    actionable_sink_steps: list[Dict[str, Any]] = []
    review_sink_steps: list[Dict[str, Any]] = []
    review_required = False
    review_summary = None

    if execute_live and execution_plan.get("final_action") == "return_response":
        current_run_id = state.get("hot_state", {}).get("current_run_id") or state.get("run_id")
        safe_runtime_execution = execute_safe_steps(
            execution_plan=execution_plan,
            task_graph=task_graph,
            request_context={
                "user_id": request.user_id,
                "context_mode": state["context"]["context_mode"],
                "run_id": current_run_id,
                "manifest_context": state.get("manifest_context", {}),
            },
        )
        actionable_sink_steps = list(safe_runtime_execution.get("actionable_sink_steps", []))
        trace_id = safe_runtime_execution.get("trace_id")
        plan_id = safe_runtime_execution.get("plan_id") or execution_plan.get("plan_id", "plan_unknown")
        step_by_id = {
            step.get("id"): step
            for step in execution_plan.get("steps", [])
            if isinstance(step, dict) and step.get("id") is not None
        }
        review_decision_events: list[Dict[str, Any]] = []
        review_sink_steps = []
        blocked_sink_steps: list[Dict[str, Any]] = []

        for sink in actionable_sink_steps:
            step = step_by_id.get(sink.get("id"), sink)
            policy_decision = evaluate_tool_policy(
                step_id=str(step.get("id", sink.get("id", "step"))),
                tool_name=step.get("tool_name"),
                params=_sink_policy_params(step, task_graph, safe_runtime_execution),
                manifest_context=state.get("manifest_context", {}),
                approved_for_sink=False,
                trace_id=trace_id,
            )
            review_decision_events.append(
                _event_from_policy_decision(
                    trace_id=trace_id,
                    plan_id=plan_id,
                    step=step,
                    decision=policy_decision,
                    run_id=current_run_id,
                )
            )
            decision = policy_decision.get("decision")
            if decision == DECISION_REVIEW:
                review_sink_steps.append(sink)
            elif decision == DECISION_BLOCK:
                blocked_sink_steps.append(sink)

        safe_trace_events = _splice_events_before_final_summary(
            list(safe_runtime_execution.get("trace_events", [])),
            review_decision_events,
        )

        # Pending review is a controlled execution block in the current run.
        review_required = (
            (not safe_runtime_execution.get("blocked", False))
            and (len(blocked_sink_steps) == 0)
            and (len(review_sink_steps) > 0)
        )
        if blocked_sink_steps:
            safe_runtime_execution["blocked"] = True

        final_summary_event = next(
            (event for event in safe_trace_events if event.get("event_type") == "final_summary"),
            None,
        )
        if isinstance(final_summary_event, dict):
            payload = dict(final_summary_event.get("payload", {}) or {})
            non_final_events = [event for event in safe_trace_events if event.get("event_type") != "final_summary"]
            payload.update(_audit_metrics_from_trace_events(non_final_events))
            payload["event_count"] = len(non_final_events) + 1
            payload["blocked"] = bool(payload.get("blocked", False) or review_required or len(blocked_sink_steps) > 0)
            payload["pending_review"] = bool(review_required)
            payload["paused_for_review"] = bool(review_required)
            payload["sink_step_count"] = len(actionable_sink_steps)
            final_summary_event["payload"] = payload

        safe_runtime_execution["trace_events"] = safe_trace_events

        if review_required:
            review_summary = {
                "required": True,
                "reason": "actionable_live_side_effect_execution",
                "sink_steps": review_sink_steps,
                "final_action": execution_plan.get("final_action"),
                "requires_tools": execution_plan.get("requires_tools", False),
                "safe_execution_results": safe_runtime_execution.get("execution_results", []),
                "condition_state": safe_runtime_execution.get("condition_state", {}),
            }

    pending_memory_digest = None
    if review_required:
        pending_memory_digest = _build_deterministic_runtime_summary(
            task_graph,
            safe_runtime_execution,
            {"blocked": True},
            review_summary,
            None,
        )

    hot_state = _merge_bucket(
        state,
        "hot_state",
        {
            "condition_state": (safe_runtime_execution or {}).get("condition_state", {}),
            "actionable_sink_steps": review_sink_steps if execute_live else actionable_sink_steps,
            "review_required": review_required,
            # Any prior approval/rejection is stale once a new safe-execution pass starts.
            "review_decision": None,
        },
    )
    warm_runtime = (safe_runtime_execution or {}).get("runtime_state", {})
    warm_cache = _merge_bucket(
        state,
        "warm_cache",
        {
            "location": warm_runtime.get("location", {}),
            "weather": warm_runtime.get("weather", {}),
            "identity": warm_runtime.get("identity", {}),
            "review_summary": review_summary,
            "last_action_result": _last_action_result((safe_runtime_execution or {}).get("execution_results", [])),
            # Ensure paused runs don't surface stale "message sent" digests from prior executions.
            "memory_digest": pending_memory_digest,
            "trace_events": list((safe_runtime_execution or {}).get("trace_events", [])),
        },
    )
    return {
        "safe_runtime_execution": safe_runtime_execution,
        "hot_state": hot_state,
        "warm_cache": warm_cache,
    }



def human_review_node(state: CAPSGraphState, service: Any) -> Dict[str, Any]:
    hot_state = dict(state.get("hot_state", {}))
    warm_cache = dict(state.get("warm_cache", {}))

    if not hot_state.get("review_required"):
        hot_state["review_decision"] = "approve"
        return {
            "hot_state": hot_state,
            "warm_cache": warm_cache,
        }

    review_summary = warm_cache.get("review_summary") or {
        "required": True,
        "reason": "live_side_effect_execution",
        "sink_steps": [],
    }

    response = interrupt(review_summary)
    if isinstance(response, dict):
        decision = str(response.get("decision", "reject")).strip().lower()
        if decision not in {"approve", "reject"}:
            decision = "reject"
        if isinstance(response.get("run_id"), str) and response.get("run_id"):
            hot_state["current_run_id"] = response.get("run_id")
        hot_state["review_decision"] = decision
        warm_cache["review_response"] = response
        trace_events = list(warm_cache.get("trace_events", []) or [])
        safe_runtime = state.get("safe_runtime_execution") or {}
        run_id = hot_state.get("current_run_id")
        trace_events.append(
            {
                "event_type": "review_resume",
                "trace_id": safe_runtime.get("trace_id"),
                "plan_id": safe_runtime.get("plan_id") or state.get("execution_plan", {}).get("plan_id"),
                "step_id": None,
                "tool_name": None,
                "timestamp_ms": int(time.time() * 1000),
                "payload": {"action": decision},
                "run_id": run_id,
            }
        )
        warm_cache["trace_events"] = trace_events
        return {
            "hot_state": hot_state,
            "warm_cache": warm_cache,
        }

    decision = str(response).strip().lower()
    if decision not in {"approve", "reject"}:
        decision = "reject"
    hot_state["review_decision"] = decision
    warm_cache["review_response"] = {"decision": decision}
    trace_events = list(warm_cache.get("trace_events", []) or [])
    safe_runtime = state.get("safe_runtime_execution") or {}
    run_id = hot_state.get("current_run_id")
    trace_events.append(
        {
            "event_type": "review_resume",
            "trace_id": safe_runtime.get("trace_id"),
            "plan_id": safe_runtime.get("plan_id") or state.get("execution_plan", {}).get("plan_id"),
            "step_id": None,
            "tool_name": None,
            "timestamp_ms": int(time.time() * 1000),
            "payload": {"action": decision},
            "run_id": run_id,
        }
    )
    warm_cache["trace_events"] = trace_events
    return {
        "hot_state": hot_state,
        "warm_cache": warm_cache,
    }

def _route_after_safe_execution(state: CAPSGraphState) -> str:
    if state.get("hot_state", {}).get("review_required"):
        return "human_review"
    return "finalize"


def execute_node(state: CAPSGraphState, service: Any) -> Dict[str, Any]:
    request = _request_payload_to_request(state["request_payload"])
    execution_plan = state["execution_plan"]
    task_graph = state["task_graph"]
    execute_live = state.get("execute_live", False)

    runtime_execution = None

    if not execute_live or execution_plan.get("final_action") != "return_response":
        return {"runtime_execution": None}

    safe_runtime = state.get("safe_runtime_execution")
    if safe_runtime is None:
        runtime_execution = execute_plan(
            execution_plan=execution_plan,
            task_graph=task_graph,
            request_context={
                "user_id": request.user_id,
                "context_mode": state["context"]["context_mode"],
                "manifest_context": state.get("manifest_context", {}),
            },
        )
        warm_cache = _merge_bucket(
            state,
            "warm_cache",
            {
                "last_action_result": _last_action_result(runtime_execution.get("execution_results", [])),
                "trace_events": list(runtime_execution.get("trace_events", [])),
            },
        )
        return {"runtime_execution": runtime_execution, "warm_cache": warm_cache}

    runtime_execution = execute_sink_steps(
        execution_plan=execution_plan,
        task_graph=task_graph,
        request_context={
            "user_id": request.user_id,
            "context_mode": state["context"]["context_mode"],
            "trace_id": safe_runtime.get("trace_id"),
            "run_id": state.get("hot_state", {}).get("current_run_id"),
            "manifest_context": state.get("manifest_context", {}),
        },
        safe_runtime=safe_runtime,
    )

    merged_results = list(safe_runtime.get("execution_results", [])) + list(runtime_execution.get("execution_results", []))
    existing_trace_events = list(state.get("warm_cache", {}).get("trace_events", []))
    if not existing_trace_events:
        existing_trace_events = list(safe_runtime.get("trace_events", []))
    merged_runtime = {
        "trace_id": runtime_execution.get("trace_id") or safe_runtime.get("trace_id"),
        "plan_id": runtime_execution.get("plan_id") or safe_runtime.get("plan_id"),
        "blocked": bool(safe_runtime.get("blocked", False) or runtime_execution.get("blocked", False)),
        "execution_results": merged_results,
        "condition_state": runtime_execution.get("condition_state") or safe_runtime.get("condition_state", {}),
    }
    warm_cache = _merge_bucket(
        state,
        "warm_cache",
        {
            "last_action_result": _last_action_result(merged_results),
            "trace_events": existing_trace_events + list(runtime_execution.get("trace_events", [])),
        },
    )
    return {"runtime_execution": merged_runtime, "warm_cache": warm_cache}



def finalize_node(state: CAPSGraphState, service: Any) -> Dict[str, Any]:
    warm_cache = _merge_bucket(state, "warm_cache", {})
    hot_state = _merge_bucket(state, "hot_state", {})
    summary = _build_deterministic_runtime_summary(
        state["task_graph"],
        state.get("safe_runtime_execution"),
        state.get("runtime_execution"),
        warm_cache.get("review_summary"),
        hot_state.get("review_decision"),
    )
    warm_cache["memory_digest"] = summary
    return {"warm_cache": warm_cache}



def build_caps_graph(service: Any, checkpointer: Any | None = None):
    graph = StateGraph(CAPSGraphState)

    graph.add_node("acquire_context", lambda state: acquire_context_node(state, service))
    graph.add_node("action_parse", lambda state: action_parse_node(state, service))
    graph.add_node("reconcile", lambda state: reconcile_node(state, service))
    graph.add_node("cir", lambda state: cir_node(state, service))
    graph.add_node("task_graph", lambda state: task_graph_node(state, service))
    graph.add_node("clarification_polish", lambda state: clarification_polish_node(state, service))
    graph.add_node("verify", lambda state: verify_node(state, service))
    graph.add_node("compile", lambda state: compile_node(state, service))
    graph.add_node("execute_safe", lambda state: execute_safe_node(state, service))
    graph.add_node("human_review", lambda state: human_review_node(state, service))
    graph.add_node("execute", lambda state: execute_node(state, service))

    graph.add_node("finalize", lambda state: finalize_node(state, service))


    graph.add_edge(START, "acquire_context")
    graph.add_edge("acquire_context", "action_parse")
    graph.add_edge("action_parse", "reconcile")
    graph.add_edge("reconcile", "cir")
    graph.add_edge("cir", "task_graph")
    graph.add_edge("task_graph", "clarification_polish")
    graph.add_edge("clarification_polish", "verify")
    graph.add_edge("verify", "compile")
    graph.add_edge("compile", "execute_safe")
    graph.add_conditional_edges(
        "execute_safe",
        _route_after_safe_execution,
        {
            "human_review": "human_review",
            "finalize": "finalize",
        },
    )
    graph.add_conditional_edges(
        "human_review",
        _route_after_human_review,
        {
            "execute": "execute",
            "finalize": "finalize",
        },
    )
    graph.add_edge("execute", "finalize")



    graph.add_edge("finalize", END)

    return graph.compile(checkpointer=checkpointer)


def _build_result_from_snapshot(
    service: Any,
    snapshot: Any,
    invoke_result: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    persisted_state = dict(snapshot.values)
    context = persisted_state["context"]
    cold_refs = persisted_state.get("cold_refs", {})
    parsed = _rebuild_action_parse_from_task_graph(
        persisted_state["task_graph"],
        persisted_state["request_payload"],
        cold_refs=cold_refs,
    )

    runtime_execution = persisted_state.get("runtime_execution")
    safe_runtime = persisted_state.get("safe_runtime_execution")
    hot_state = persisted_state.get("hot_state", {})
    warm_cache = persisted_state.get("warm_cache", {})
    review_summary = warm_cache.get("review_summary")
    review_decision = hot_state.get("review_decision")

    if invoke_result and "__interrupt__" in invoke_result:
        interrupt_items = invoke_result.get("__interrupt__", [])
        interrupt_value = interrupt_items[0].value if interrupt_items else review_summary
        runtime_execution = {
            "blocked": True,
            "pending_node": "human_review",
            "review_summary": _json_safe(interrupt_value),
            "execution_results": [],
            "condition_state": {},
        }
    elif review_decision == "reject":
        safe_runtime = safe_runtime or {}
        runtime_execution = {
            "blocked": False,
            "rejected": True,
            "pending_node": None,
            "review_summary": review_summary,
            "review_response": warm_cache.get("review_response"),
            "execution_results": list(safe_runtime.get("execution_results", [])),
            "condition_state": safe_runtime.get("condition_state", {}),
        }
    elif runtime_execution is None and safe_runtime:
        runtime_execution = {
            "trace_id": safe_runtime.get("trace_id"),
            "plan_id": safe_runtime.get("plan_id"),
            "blocked": bool(safe_runtime.get("blocked", False)),
            "execution_results": list(safe_runtime.get("execution_results", [])),
            "condition_state": safe_runtime.get("condition_state", {}),
        }

    summary = _build_deterministic_runtime_summary(
        persisted_state["task_graph"],
        safe_runtime,
        runtime_execution,
        review_summary,
        review_decision,
    )
    if runtime_execution is not None and summary:
        runtime_execution["summary"] = summary


    return service._build_action_parse_result(
        context=context,
        parsed=parsed,
        task_graph=persisted_state["task_graph"],
        graph_verification=persisted_state["task_graph_verification"],
        execution_plan=persisted_state["execution_plan"],
        runtime_execution=runtime_execution,
    )



def run_action_parse_graph(
    service: Any,
    request: Any,
    execute_live: bool = False,
    thread_id: str | None = None,
    sqlite_path: str | None = None,
    manifest_path: str | None = None,
) -> Dict[str, Any]:
    with ExitStack() as stack:
        checkpointer = None
        if sqlite_path:
            checkpointer = build_sqlite_checkpointer(sqlite_path, stack)
        manifest = load_manifest(manifest_path)
        manifest_context = build_manifest_context(manifest)

        app = build_caps_graph(service, checkpointer=checkpointer)
        config = {"configurable": {"thread_id": thread_id or f"{request.user_id}:default"}}
        run_id = f"run_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"

        invoke_result = app.invoke(
            {
                "request_payload": {
                    "user_id": request.user_id,
                    "prompt": request.prompt,
                    "temperature": request.temperature,
                },
                "run_id": run_id,
                "execute_live": execute_live,
                "manifest_context": manifest_context,
            },
            config=config,
        )
        snapshot = app.get_state(config)
        return _build_result_from_snapshot(service, snapshot, invoke_result=invoke_result)

def resume_action_parse_graph(
    service: Any,
    thread_id: str,
    sqlite_path: str,
    decision: str,
) -> Dict[str, Any]:
    with ExitStack() as stack:
        checkpointer = build_sqlite_checkpointer(sqlite_path, stack)
        app = build_caps_graph(service, checkpointer=checkpointer)
        config = {"configurable": {"thread_id": thread_id}}
        run_id = f"run_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"

        invoke_result = app.invoke(
            Command(resume={"decision": decision, "run_id": run_id}),
            config=config,
        )
        snapshot = app.get_state(config)
        return _build_result_from_snapshot(service, snapshot, invoke_result=invoke_result)


def _json_safe(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return str(value)


def _snapshot_to_dict(snapshot: Any) -> Dict[str, Any]:
    next_nodes = getattr(snapshot, "next", ()) or ()
    values = getattr(snapshot, "values", {}) or {}
    config = getattr(snapshot, "config", {}) or {}
    metadata = getattr(snapshot, "metadata", {}) or {}
    return {
        "next": _json_safe(list(next_nodes)),
        "values": _json_safe(dict(values)),
        "state_header": _json_safe(_build_state_header(dict(values))),
        "config": _json_safe(dict(config)),
        "metadata": _json_safe(dict(metadata)),
        "created_at": _json_safe(getattr(snapshot, "created_at", None)),
    }


def get_action_parse_graph_state(
    service: Any,
    thread_id: str,
    sqlite_path: str,
) -> Dict[str, Any]:
    with ExitStack() as stack:
        checkpointer = build_sqlite_checkpointer(sqlite_path, stack)
        app = build_caps_graph(service, checkpointer=checkpointer)
        config = {"configurable": {"thread_id": thread_id}}
        snapshot = app.get_state(config)
        result = _snapshot_to_dict(snapshot)
        result["thread_id"] = thread_id
        return result



def get_action_parse_graph_history(
    service: Any,
    thread_id: str,
    sqlite_path: str,
    limit: int = 10,
) -> Dict[str, Any]:
    with ExitStack() as stack:
        checkpointer = build_sqlite_checkpointer(sqlite_path, stack)
        app = build_caps_graph(service, checkpointer=checkpointer)
        config = {"configurable": {"thread_id": thread_id}}
        snapshots = []
        for snapshot in app.get_state_history(config, limit=limit):
            snapshots.append(_snapshot_to_dict(snapshot))

        return {
            "thread_id": thread_id,
            "count": len(snapshots),
            "history": snapshots,
        }
