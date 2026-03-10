import time
import uuid
from typing import Any, Dict, List

from adapters.identity_api import resolve_recipient
from adapters.location_api import resolve_location
from adapters.messaging_api import send_message
from adapters.weather_api import fetch_weather
from adapters.email_api import send_email
from adapters.calendar_api import schedule_event
from core.policy_engine import DECISION_BLOCK, DECISION_REVIEW, evaluate_tool_policy



def _adapter_dispatch(tool_name: str):
    table = {
        "identity_api": resolve_recipient,
        "location_api": resolve_location,
        "weather_api": fetch_weather,
        "messaging_api": send_message,
        "email_api": send_email,
        "calendar_api": schedule_event,
    }
    return table.get(tool_name)

SAFE_STEP_TYPES = {
    "resolve_location",
    "fetch_data",
    "evaluate_condition",
    "branch_if_true",
    "policy_check",
    "resolve_recipient",
}

SINK_STEP_TYPES = {
    "send_message",
    "send_email",
    "schedule_event",
}



def _is_condition_true(condition: str | None, condition_state: Dict[str, bool]) -> bool:
    if not condition:
        return True
    return bool(condition_state.get(condition, False))

def _is_condition_known(condition: str | None, condition_state: Dict[str, bool]) -> bool:
    if not condition:
        return True
    return condition in condition_state



def _normalize_error_status(step_type: str, error_code: str) -> str:
    code = (error_code or "UNKNOWN_ERROR").lower()
    if step_type in {"resolve_location", "resolve_recipient"}:
        return f"blocked_{code}"
    return f"failed_{code}"

def _make_idempotency_key(plan_id: str, step_id: str, recipient_id: str, message: str) -> str:
    """Generate a deterministic idempotency key based on plan, step, recipient, and message. In a real implementation, you might want to include more context or use a more robust hashing mechanism to avoid collisions."""
    raw = f"{plan_id}|{step_id}|{recipient_id}|{message}"
    return f"idem_{abs(hash(raw)) % 1000000000}"


def _is_sink_tool(step: Dict[str, Any], manifest_context: Dict[str, Any] | None) -> bool:
    manifest_context = manifest_context or {}
    sink_tools = set(manifest_context.get("sink_tools", []) or [])
    tool_name = step.get("tool_name")
    if isinstance(tool_name, str) and tool_name in sink_tools:
        return True
    return step.get("type") in SINK_STEP_TYPES


def _policy_params_for_step(
    step: Dict[str, Any],
    tasks: List[Dict[str, Any]],
    runtime_state: Dict[str, Any],
) -> Dict[str, Any]:
    def _identity_params_from_send_task(task: Dict[str, Any]) -> Dict[str, Any]:
        raw = dict(task.get("params", {}))
        params: Dict[str, Any] = {}
        if "recipient_ref" in raw:
            params["recipient_ref"] = raw.get("recipient_ref")
        elif "recipient" in raw:
            params["recipient_ref"] = raw.get("recipient")
        if "recipient_resolved" in raw:
            params["recipient_resolved"] = raw.get("recipient_resolved")
        if "recipient_id" in raw:
            params["recipient_id"] = raw.get("recipient_id")
        return params

    step_type = step.get("type")
    if step_type in {"resolve_location", "fetch_data"}:
        weather_task = next((t for t in tasks if t.get("action") == "fetch_weather"), {})
        params = dict(weather_task.get("params", {}))
        if step_type == "fetch_data" and runtime_state.get("location"):
            params.update(runtime_state["location"])
        return params
    if step_type == "resolve_recipient":
        send_task = next((t for t in tasks if t.get("action") in {"send_message", "send_email"}), {})
        return _identity_params_from_send_task(send_task)
    if step_type == "send_message":
        send_task = next((t for t in tasks if t.get("action") == "send_message"), {})
        params = dict(send_task.get("params", {}))
        if runtime_state.get("identity"):
            params.update(runtime_state["identity"])
        return params
    if step_type == "send_email":
        send_task = next((t for t in tasks if t.get("action") == "send_email"), {})
        params = dict(send_task.get("params", {}))
        if runtime_state.get("identity"):
            params.update(runtime_state["identity"])
        return params
    if step_type == "schedule_event":
        sched_task = next((t for t in tasks if t.get("action") == "schedule_meeting"), {})
        return dict(sched_task.get("params", {}))
    return {}


def _emit_trace_event(
    trace_events: List[Dict[str, Any]],
    *,
    event_type: str,
    trace_id: str,
    plan_id: str,
    step_id: str | None = None,
    tool_name: str | None = None,
    payload: Dict[str, Any] | None = None,
) -> None:
    trace_events.append(
        {
            "event_type": event_type,
            "trace_id": trace_id,
            "plan_id": plan_id,
            "step_id": step_id,
            "tool_name": tool_name,
            "timestamp_ms": int(time.time() * 1000),
            "payload": payload or {},
        }
    )


def _stamp_trace_run_id(trace_events: List[Dict[str, Any]], run_id: str | None) -> List[Dict[str, Any]]:
    if not isinstance(run_id, str) or not run_id:
        return trace_events
    for event in trace_events:
        if isinstance(event, dict) and not event.get("run_id"):
            event["run_id"] = run_id
    return trace_events


def _build_audit_summary(trace_events: List[Dict[str, Any]]) -> Dict[str, Any]:
    decision_counts = {"ALLOW": 0, "REVIEW_REQUIRED": 0, "BLOCK": 0}
    tool_call_count = 0
    tool_result_count = 0
    tool_error_count = 0
    reviewed_tools: List[str] = []
    blocked_tools: List[str] = []

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
        "event_count": len(trace_events),
        "reviewed_tools": sorted(set(reviewed_tools)),
        "blocked_tools": sorted(set(blocked_tools)),
    }


def _run_policy_check(tasks: List[Dict[str, Any]], runtime_state: Dict[str, Any]) -> Dict[str, Any]:
    """For this example, we have a simple policy that blocks any message sending action if the message is empty or if the recipient cannot be resolved. In a real implementation, this could be much more complex and involve multiple checks against the runtime state and task parameters.
    """
    send_task = next((t for t in tasks if t.get("action") in {"send_message", "send_email"}), {})
    params = dict(send_task.get("params", {}))

    # Merge resolved identity if available
    if runtime_state.get("identity"):
        params.update(runtime_state["identity"])

    message = params.get("message")
    recipient_id = params.get("recipient_id")
    recipient_ref = params.get("recipient_ref")

    reasons: List[str] = []
    allowed = True

    if not isinstance(message, str) or not message.strip():
        allowed = False
        reasons.append("empty_message")

    # Require at least one recipient identity signal before side-effect
    if not isinstance(recipient_id, str) or not recipient_id.strip():
        if not isinstance(recipient_ref, str) or not recipient_ref.strip():
            allowed = False
            reasons.append("recipient_unresolved")

    return {"allowed": allowed, "reasons": reasons}


def _invoke_adapter_with_retry(
    fn,
    params: Dict[str, Any],
    request_context: Dict[str, Any],
    trace_id: str,
    max_retries: int,
) -> Dict[str, Any]:
    attempts = 0
    last: Dict[str, Any] | None = None
    max_attempts = 1 + max(0, int(max_retries))
    while attempts < max_attempts:
        attempts += 1
        try:
            resp = fn(params, request_context, trace_id)
        except Exception as exc:  # noqa: BLE001
            resp = {
                "ok": False,
                "data": None,
                "error_code": "ADAPTER_EXCEPTION",
                "retryable": False,
                "message": str(exc),
            }

        last = resp if isinstance(resp, dict) else {
            "ok": False,
            "data": None,
            "error_code": "ADAPTER_INVALID_RESPONSE",
            "retryable": False,
            "message": "adapter returned non-dict response",
        }
        if last.get("ok"):
            break
        if not last.get("retryable", False):
            break

    if not isinstance(last, dict):
        last = {
            "ok": False,
            "data": None,
            "error_code": "ADAPTER_UNKNOWN_FAILURE",
            "retryable": False,
            "message": "adapter did not return response",
        }

    last["attempts"] = attempts
    return last

def _execute_step_sequence(
    steps: List[Dict[str, Any]],
    tasks: List[Dict[str, Any]],
    request_context: Dict[str, Any],
    trace_id: str,
    plan_id: str,
    max_retries: int,
    runtime_state: Dict[str, Any],
    condition_state: Dict[str, bool],
    manifest_context: Dict[str, Any] | None = None,
    approved_for_sink: bool = False,
    emit_decision_events: bool = True,
    trace_events: List[Dict[str, Any]] | None = None,
) -> tuple[List[Dict[str, Any]], bool]:
    execution_results: List[Dict[str, Any]] = []
    blocked = False
    trace_events = trace_events if trace_events is not None else []

    for step in steps:
        step_id = step.get("id", "step")
        step_type = step.get("type")
        tool_name = step.get("tool_name")
        condition = step.get("condition")

        started = time.time()

        if condition and not _is_condition_known(condition, condition_state):
            execution_results.append(
                {
                    "step_id": step_id,
                    "type": step_type,
                    "tool_name": tool_name,
                    "status": "failed_missing_condition_state",
                    "detail": {"reason": "condition_not_evaluated", "condition": condition},
                    "latency_ms": int((time.time() - started) * 1000),
                    "trace_id": trace_id,
                    "plan_id": plan_id,
                }
            )
            blocked = True
            break

        if not _is_condition_true(condition, condition_state):
            execution_results.append(
                {
                    "step_id": step_id,
                    "type": step_type,
                    "tool_name": tool_name,
                    "status": "skipped_by_condition",
                    "detail": {"simulated": True, "reason": "condition_false"},
                    "latency_ms": int((time.time() - started) * 1000),
                    "trace_id": trace_id,
                    "plan_id": plan_id,
                }
            )
            continue

        step_params_for_policy = _policy_params_for_step(step, tasks, runtime_state)
        policy_decision = evaluate_tool_policy(
            step_id=step_id,
            tool_name=tool_name,
            params=step_params_for_policy,
            manifest_context=manifest_context,
            approved_for_sink=approved_for_sink if _is_sink_tool(step, manifest_context) else False,
            trace_id=trace_id,
        )
        policy_decision["trace_id"] = trace_id
        if emit_decision_events:
            _emit_trace_event(
                trace_events,
                event_type="decision",
                trace_id=trace_id,
                plan_id=plan_id,
                step_id=step_id,
                tool_name=tool_name,
                payload={
                    "decision": policy_decision.get("decision"),
                    "reason_code": policy_decision.get("reason_code"),
                    "rule_id": policy_decision.get("rule_id"),
                    "manifest_id": policy_decision.get("manifest_id"),
                    "manifest_version": policy_decision.get("manifest_version"),
                    "timestamp_ms": policy_decision.get("timestamp_ms"),
                    "precedence_resolved": policy_decision.get("precedence_resolved", False),
                    "resolution_reason_code": policy_decision.get("resolution_reason_code"),
                    "winning_decision": policy_decision.get("winning_decision"),
                    "winning_reason_code": policy_decision.get("winning_reason_code"),
                    "winning_rule_id": policy_decision.get("winning_rule_id"),
                },
            )
        if policy_decision.get("decision") == DECISION_BLOCK:
            execution_results.append(
                {
                    "step_id": step_id,
                    "type": step_type,
                    "tool_name": tool_name,
                    "status": "blocked_policy_violation",
                    "detail": {"policy_decision": policy_decision},
                    "latency_ms": int((time.time() - started) * 1000),
                    "trace_id": trace_id,
                    "plan_id": plan_id,
                }
            )
            blocked = True
            break
        if policy_decision.get("decision") == DECISION_REVIEW:
            execution_results.append(
                {
                    "step_id": step_id,
                    "type": step_type,
                    "tool_name": tool_name,
                    "status": "blocked_review_required",
                    "detail": {"policy_decision": policy_decision},
                    "latency_ms": int((time.time() - started) * 1000),
                    "trace_id": trace_id,
                    "plan_id": plan_id,
                }
            )
            blocked = True
            break

        status = "ok"
        detail: Dict[str, Any] = {}

        if step_type in {"clarification_required", "build_response"}:
            detail = {"simulated": True}

        elif step_type == "policy_check":
            decision = _run_policy_check(tasks, runtime_state)
            detail = {"policy_decision": decision}
            if not decision.get("allowed", False):
                status = "blocked_policy_violation"
                blocked = True

        elif step_type == "evaluate_condition":
            eval_task = next((t for t in tasks if t.get("action") == "evaluate_condition"), {})
            eval_task_id = eval_task.get("id", "cond")
            params = eval_task.get("params", {})
            metric = params.get("metric")
            operator = params.get("operator")
            threshold = params.get("threshold_value")

            result = False
            if metric == "temperature" and operator == "<" and isinstance(threshold, (int, float)):
                temp_c = runtime_state.get("weather", {}).get("temperature_c")
                if isinstance(temp_c, (int, float)):
                    result = temp_c < float(threshold)

            if metric == "time_of_day" and operator == "==" and isinstance(threshold, str):
                current_time = request_context.get("override_time")
                if isinstance(current_time, str):
                    result = current_time == threshold

            condition_state[f"{eval_task_id}:true"] = bool(result)
            condition_state[f"{eval_task_id}:false"] = not bool(result)
            detail = {"result": bool(result), "condition_token_true": f"{eval_task_id}:true"}

        elif step_type == "branch_if_true":
            detail = {"simulated": True}

        elif step_type == "resolve_location" and tool_name == "location_api":
            weather_task = next((t for t in tasks if t.get("action") == "fetch_weather"), {})
            params = dict(weather_task.get("params", {}))
            fn = _adapter_dispatch(tool_name)
            _emit_trace_event(
                trace_events,
                event_type="tool_call",
                trace_id=trace_id,
                plan_id=plan_id,
                step_id=step_id,
                tool_name=tool_name,
                payload={"params": params},
            )
            resp = _invoke_adapter_with_retry(fn, params, request_context, trace_id, max_retries)
            if not resp.get("ok"):
                status = _normalize_error_status(step_type, resp.get("error_code", "LOCATION_ERROR"))
                blocked = True
            else:
                runtime_state["location"] = resp["data"]
            detail = resp
            _emit_trace_event(
                trace_events,
                event_type="tool_result",
                trace_id=trace_id,
                plan_id=plan_id,
                step_id=step_id,
                tool_name=tool_name,
                payload={"ok": resp.get("ok"), "error_code": resp.get("error_code"), "status": status},
            )

        elif step_type == "fetch_data" and tool_name == "weather_api":
            weather_task = next((t for t in tasks if t.get("action") == "fetch_weather"), {})
            params = dict(weather_task.get("params", {}))
            if runtime_state.get("location"):
                params.update(runtime_state["location"])
            fn = _adapter_dispatch(tool_name)
            _emit_trace_event(
                trace_events,
                event_type="tool_call",
                trace_id=trace_id,
                plan_id=plan_id,
                step_id=step_id,
                tool_name=tool_name,
                payload={"params": params},
            )
            resp = _invoke_adapter_with_retry(fn, params, request_context, trace_id, max_retries)
            if not resp.get("ok"):
                status = _normalize_error_status(step_type, resp.get("error_code", "WEATHER_ERROR"))
            else:
                runtime_state["weather"] = resp["data"]
            detail = resp
            _emit_trace_event(
                trace_events,
                event_type="tool_result",
                trace_id=trace_id,
                plan_id=plan_id,
                step_id=step_id,
                tool_name=tool_name,
                payload={"ok": resp.get("ok"), "error_code": resp.get("error_code"), "status": status},
            )

        elif step_type == "resolve_recipient" and tool_name == "identity_api":
            send_task = next((t for t in tasks if t.get("action") in {"send_message", "send_email"}), {})
            raw = dict(send_task.get("params", {}))
            params = {
                "recipient_ref": raw.get("recipient_ref", raw.get("recipient")),
                "recipient_resolved": raw.get("recipient_resolved"),
                "recipient_id": raw.get("recipient_id"),
            }
            fn = _adapter_dispatch(tool_name)
            _emit_trace_event(
                trace_events,
                event_type="tool_call",
                trace_id=trace_id,
                plan_id=plan_id,
                step_id=step_id,
                tool_name=tool_name,
                payload={"params": params},
            )
            resp = _invoke_adapter_with_retry(fn, params, request_context, trace_id, max_retries)
            if not resp.get("ok"):
                status = _normalize_error_status(step_type, resp.get("error_code", "IDENTITY_ERROR"))
                blocked = True
            else:
                runtime_state["identity"] = resp["data"]
            detail = resp
            _emit_trace_event(
                trace_events,
                event_type="tool_result",
                trace_id=trace_id,
                plan_id=plan_id,
                step_id=step_id,
                tool_name=tool_name,
                payload={"ok": resp.get("ok"), "error_code": resp.get("error_code"), "status": status},
            )

        elif step_type == "send_message" and tool_name == "messaging_api":
            send_task = next((t for t in tasks if t.get("action") == "send_message"), {})
            params = dict(send_task.get("params", {}))
            if runtime_state.get("identity"):
                params.update(runtime_state["identity"])
            recipient_id = params.get("recipient_id") or ""
            message = params.get("message") or ""
            params["idempotency_key"] = _make_idempotency_key(plan_id, step_id, str(recipient_id), str(message))
            fn = _adapter_dispatch(tool_name)
            _emit_trace_event(
                trace_events,
                event_type="tool_call",
                trace_id=trace_id,
                plan_id=plan_id,
                step_id=step_id,
                tool_name=tool_name,
                payload={"params": params},
            )
            resp = _invoke_adapter_with_retry(fn, params, request_context, trace_id, max_retries)
            if not resp.get("ok"):
                status = _normalize_error_status(step_type, resp.get("error_code", "MESSAGE_ERROR"))
            detail = resp
            _emit_trace_event(
                trace_events,
                event_type="tool_result",
                trace_id=trace_id,
                plan_id=plan_id,
                step_id=step_id,
                tool_name=tool_name,
                payload={"ok": resp.get("ok"), "error_code": resp.get("error_code"), "status": status},
            )

        elif step_type == "schedule_event" and tool_name == "calendar_api":
            sched_task = next((t for t in tasks if t.get("action") == "schedule_meeting"), {})
            params = dict(sched_task.get("params", {}))
            fn = _adapter_dispatch(tool_name)
            _emit_trace_event(
                trace_events,
                event_type="tool_call",
                trace_id=trace_id,
                plan_id=plan_id,
                step_id=step_id,
                tool_name=tool_name,
                payload={"params": params},
            )
            resp = _invoke_adapter_with_retry(fn, params, request_context, trace_id, max_retries)
            if not resp.get("ok"):
                status = _normalize_error_status(step_type, resp.get("error_code", "CALENDAR_ERROR"))
            detail = resp
            _emit_trace_event(
                trace_events,
                event_type="tool_result",
                trace_id=trace_id,
                plan_id=plan_id,
                step_id=step_id,
                tool_name=tool_name,
                payload={"ok": resp.get("ok"), "error_code": resp.get("error_code"), "status": status},
            )

        elif step_type == "send_email" and tool_name == "email_api":
            send_task = next((t for t in tasks if t.get("action") == "send_email"), {})
            params = dict(send_task.get("params", {}))
            if runtime_state.get("identity"):
                params.update(runtime_state["identity"])
            recipient_id = params.get("recipient_id") or ""
            message = params.get("message") or ""
            params["idempotency_key"] = _make_idempotency_key(plan_id, step_id, str(recipient_id), str(message))
            fn = _adapter_dispatch(tool_name)
            _emit_trace_event(
                trace_events,
                event_type="tool_call",
                trace_id=trace_id,
                plan_id=plan_id,
                step_id=step_id,
                tool_name=tool_name,
                payload={"params": params},
            )
            resp = _invoke_adapter_with_retry(fn, params, request_context, trace_id, max_retries)
            if not resp.get("ok"):
                status = _normalize_error_status(step_type, resp.get("error_code", "EMAIL_ERROR"))
            detail = resp
            _emit_trace_event(
                trace_events,
                event_type="tool_result",
                trace_id=trace_id,
                plan_id=plan_id,
                step_id=step_id,
                tool_name=tool_name,
                payload={"ok": resp.get("ok"), "error_code": resp.get("error_code"), "status": status},
            )

        else:
            detail = {"simulated": True, "note": "unhandled step type/tool in runtime"}

        execution_results.append(
            {
                "step_id": step_id,
                "type": step_type,
                "tool_name": tool_name,
                "status": status,
                "detail": detail,
                "latency_ms": int((time.time() - started) * 1000),
                "trace_id": trace_id,
                "plan_id": plan_id,
            }
        )

        if blocked:
            break

    return execution_results, blocked

def execute_safe_steps(
    execution_plan: Dict[str, Any],
    task_graph: Dict[str, Any],
    request_context: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    request_context = request_context or {}
    trace_id = request_context.get("trace_id") or f"trace_{uuid.uuid4().hex[:10]}"
    max_retries = int(request_context.get("max_retries", 1))
    plan_id = execution_plan.get("plan_id", "plan_unknown")
    steps = execution_plan.get("steps", [])
    tasks = task_graph.get("tasks", [])

    safe_steps = [step for step in steps if step.get("type") in SAFE_STEP_TYPES]

    runtime_state: Dict[str, Any] = {
        "weather": {},
        "identity": {},
        "location": {},
    }
    condition_state: Dict[str, bool] = {}
    trace_events: List[Dict[str, Any]] = []
    manifest_context = request_context.get("manifest_context") or {}
    run_id = request_context.get("run_id")

    execution_results, blocked = _execute_step_sequence(
        safe_steps,
        tasks,
        request_context,
        trace_id,
        plan_id,
        max_retries,
        runtime_state,
        condition_state,
        manifest_context=manifest_context,
        approved_for_sink=False,
        trace_events=trace_events,
    )

    actionable_sink_steps = []
    for step in steps:
        if not _is_sink_tool(step, manifest_context):
            continue
        condition = step.get("condition")
        if condition and not condition_state.get(condition, False):
            continue
        step_type = step.get("type")
        if step_type in {"send_message", "send_email"}:
            recipient_id = runtime_state.get("identity", {}).get("recipient_id")
            if not isinstance(recipient_id, str) or not recipient_id.strip():
                continue
        sink_summary = {
            "id": step.get("id"),
            "type": step.get("type"),
            "tool_name": step.get("tool_name"),
            "condition": step.get("condition"),
            "description": step.get("description"),
        }
        if step_type in {"send_message", "send_email"}:
            sink_summary["recipient_id"] = runtime_state.get("identity", {}).get("recipient_id")
            sink_summary["recipient_ref"] = runtime_state.get("identity", {}).get("recipient_ref")
        actionable_sink_steps.append(sink_summary)

    _emit_trace_event(
        trace_events,
        event_type="final_summary",
        trace_id=trace_id,
        plan_id=plan_id,
        payload={
            "blocked": blocked,
            "execution_result_count": len(execution_results),
            "actionable_sink_count": len(actionable_sink_steps),
            **_build_audit_summary(trace_events),
        },
    )
    trace_events = _stamp_trace_run_id(trace_events, run_id)

    return {
        "trace_id": trace_id,
        "plan_id": plan_id,
        "blocked": blocked,
        "execution_results": execution_results,
        "condition_state": condition_state,
        "runtime_state": runtime_state,
        "actionable_sink_steps": actionable_sink_steps,
        "trace_events": trace_events,
    }


def execute_sink_steps(
    execution_plan: Dict[str, Any],
    task_graph: Dict[str, Any],
    request_context: Dict[str, Any] | None = None,
    safe_runtime: Dict[str, Any] | None = None,
    approved_for_sink: bool = True,
    emit_decision_events: bool = True,
) -> Dict[str, Any]:
    request_context = request_context or {}
    safe_runtime = safe_runtime or {}

    trace_id = safe_runtime.get("trace_id") or request_context.get("trace_id") or f"trace_{uuid.uuid4().hex[:10]}"
    max_retries = int(request_context.get("max_retries", 1))
    plan_id = execution_plan.get("plan_id", "plan_unknown")
    steps = execution_plan.get("steps", [])
    tasks = task_graph.get("tasks", [])

    sink_steps = [step for step in steps if step.get("type") in SINK_STEP_TYPES]

    runtime_state = safe_runtime.get("runtime_state") or {
        "weather": {},
        "identity": {},
        "location": {},
    }
    condition_state = safe_runtime.get("condition_state") or {}
    trace_events: List[Dict[str, Any]] = []
    manifest_context = request_context.get("manifest_context") or {}
    run_id = request_context.get("run_id")

    execution_results, blocked = _execute_step_sequence(
        sink_steps,
        tasks,
        request_context,
        trace_id,
        plan_id,
        max_retries,
        runtime_state,
        condition_state,
        manifest_context=manifest_context,
        approved_for_sink=approved_for_sink,
        emit_decision_events=emit_decision_events,
        trace_events=trace_events,
    )

    _emit_trace_event(
        trace_events,
        event_type="final_summary",
        trace_id=trace_id,
        plan_id=plan_id,
        payload={
            "blocked": blocked,
            "execution_result_count": len(execution_results),
            "sink_step_count": len(sink_steps),
            **_build_audit_summary(trace_events),
        },
    )
    trace_events = _stamp_trace_run_id(trace_events, run_id)

    return {
        "trace_id": trace_id,
        "plan_id": plan_id,
        "blocked": blocked,
        "execution_results": execution_results,
        "condition_state": condition_state,
        "runtime_state": runtime_state,
        "trace_events": trace_events,
    }



def execute_plan(
    execution_plan: Dict[str, Any],
    task_graph: Dict[str, Any],
    request_context: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    request_context = request_context or {}
    trace_id = request_context.get("trace_id") or f"trace_{uuid.uuid4().hex[:10]}"
    max_retries = int(request_context.get("max_retries", 1))
    plan_id = execution_plan.get("plan_id", "plan_unknown")
    steps = execution_plan.get("steps", [])
    tasks = task_graph.get("tasks", [])

    # task action lookup to fetch params for runtime calls
    task_by_action: Dict[str, List[Dict[str, Any]]] = {}
    for t in tasks:
        task_by_action.setdefault(t.get("action", ""), []).append(t)

    condition_state: Dict[str, bool] = {}
    execution_results: List[Dict[str, Any]] = []
    blocked = False

    # shared mutable context for resolved fields and fetched data
    runtime_state: Dict[str, Any] = {
        "weather": {},
        "identity": {},
        "location": {},
    }
    trace_events: List[Dict[str, Any]] = []
    manifest_context = request_context.get("manifest_context") or {}
    run_id = request_context.get("run_id")

    for step in steps:
        step_id = step.get("id", "step")
        step_type = step.get("type")
        tool_name = step.get("tool_name")
        condition = step.get("condition")

        started = time.time()

        if condition and not _is_condition_known(condition, condition_state):
            execution_results.append(
                {
                    "step_id": step_id,
                    "type": step_type,
                    "tool_name": tool_name,
                    "status": "failed_missing_condition_state",
                    "detail": {"reason": "condition_not_evaluated", "condition": condition},
                    "latency_ms": int((time.time() - started) * 1000),
                    "trace_id": trace_id,
                    "plan_id": plan_id,
                }
            )
            blocked = True
            break

        if not _is_condition_true(condition, condition_state):
            execution_results.append(
                {
                    "step_id": step_id,
                    "type": step_type,
                    "tool_name": tool_name,
                    "status": "skipped_by_condition",
                    "detail": {"simulated": True, "reason": "condition_false"},
                    "latency_ms": int((time.time() - started) * 1000),
                    "trace_id": trace_id,
                    "plan_id": plan_id,
                }
            )
            continue


        step_params_for_policy = _policy_params_for_step(step, tasks, runtime_state)
        policy_decision = evaluate_tool_policy(
            step_id=step_id,
            tool_name=tool_name,
            params=step_params_for_policy,
            manifest_context=manifest_context,
            approved_for_sink=False,
            trace_id=trace_id,
        )
        policy_decision["trace_id"] = trace_id
        _emit_trace_event(
            trace_events,
            event_type="decision",
            trace_id=trace_id,
            plan_id=plan_id,
            step_id=step_id,
            tool_name=tool_name,
            payload={
                "decision": policy_decision.get("decision"),
                "reason_code": policy_decision.get("reason_code"),
                "rule_id": policy_decision.get("rule_id"),
                "manifest_id": policy_decision.get("manifest_id"),
                "manifest_version": policy_decision.get("manifest_version"),
                "timestamp_ms": policy_decision.get("timestamp_ms"),
                "precedence_resolved": policy_decision.get("precedence_resolved", False),
                "resolution_reason_code": policy_decision.get("resolution_reason_code"),
                "winning_decision": policy_decision.get("winning_decision"),
                "winning_reason_code": policy_decision.get("winning_reason_code"),
                "winning_rule_id": policy_decision.get("winning_rule_id"),
            },
        )
        if policy_decision.get("decision") in {DECISION_BLOCK, DECISION_REVIEW}:
            status = "blocked_policy_violation" if policy_decision.get("decision") == DECISION_BLOCK else "blocked_review_required"
            execution_results.append(
                {
                    "step_id": step_id,
                    "type": step_type,
                    "tool_name": tool_name,
                    "status": status,
                    "detail": {"policy_decision": policy_decision},
                    "latency_ms": int((time.time() - started) * 1000),
                    "trace_id": trace_id,
                    "plan_id": plan_id,
                }
            )
            blocked = True
            break

        status = "ok"
        detail: Dict[str, Any] = {}

        # planner-only/control steps
        if step_type in {"clarification_required", "build_response"}:
            detail = {"simulated": True}

        elif step_type == "policy_check":
            decision = _run_policy_check(tasks, runtime_state)
            detail = {"policy_decision": decision}
            if not decision.get("allowed", False):
                status = "blocked_policy_violation"
                blocked = True


        elif step_type == "evaluate_condition":
            eval_task = next((t for t in tasks if t.get("action") == "evaluate_condition"), {})
            eval_task_id = eval_task.get("id", "cond")
            params = eval_task.get("params", {})
            metric = params.get("metric")
            operator = params.get("operator")
            threshold = params.get("threshold_value")

            result = False
            if metric == "temperature" and operator == "<" and isinstance(threshold, (int, float)):
                temp_c = runtime_state.get("weather", {}).get("temperature_c")
                if isinstance(temp_c, (int, float)):
                    result = temp_c < float(threshold)

            condition_state[f"{eval_task_id}:true"] = bool(result)
            condition_state[f"{eval_task_id}:false"] = not bool(result)
            detail = {"result": bool(result), "condition_token_true": f"{eval_task_id}:true"}

        elif step_type == "branch_if_true":
            detail = {"simulated": True}

        # adapter-backed steps
        elif step_type == "resolve_location" and tool_name == "location_api":
            weather_task = next((t for t in tasks if t.get("action") == "fetch_weather"), {})
            params = dict(weather_task.get("params", {}))
            fn = _adapter_dispatch(tool_name)
            _emit_trace_event(
                trace_events,
                event_type="tool_call",
                trace_id=trace_id,
                plan_id=plan_id,
                step_id=step_id,
                tool_name=tool_name,
                payload={"params": params},
            )
            resp = _invoke_adapter_with_retry(fn, params, request_context, trace_id, max_retries)
            if not resp.get("ok"):
                status = _normalize_error_status(step_type, resp.get("error_code", "LOCATION_ERROR"))
                blocked = True
            else:
                runtime_state["location"] = resp["data"]
            detail = resp
            _emit_trace_event(
                trace_events,
                event_type="tool_result",
                trace_id=trace_id,
                plan_id=plan_id,
                step_id=step_id,
                tool_name=tool_name,
                payload={"ok": resp.get("ok"), "error_code": resp.get("error_code"), "status": status},
            )

        elif step_type == "fetch_data" and tool_name == "weather_api":
            weather_task = next((t for t in tasks if t.get("action") == "fetch_weather"), {})
            params = dict(weather_task.get("params", {}))
            # inject resolved location from previous step
            if runtime_state.get("location"):
                params.update(runtime_state["location"])
            fn = _adapter_dispatch(tool_name)
            _emit_trace_event(
                trace_events,
                event_type="tool_call",
                trace_id=trace_id,
                plan_id=plan_id,
                step_id=step_id,
                tool_name=tool_name,
                payload={"params": params},
            )
            resp = _invoke_adapter_with_retry(fn, params, request_context, trace_id, max_retries)
            if not resp.get("ok"):
                status = _normalize_error_status(step_type, resp.get("error_code", "WEATHER_ERROR"))
            else:
                runtime_state["weather"] = resp["data"]
            detail = resp
            _emit_trace_event(
                trace_events,
                event_type="tool_result",
                trace_id=trace_id,
                plan_id=plan_id,
                step_id=step_id,
                tool_name=tool_name,
                payload={"ok": resp.get("ok"), "error_code": resp.get("error_code"), "status": status},
            )

        elif step_type == "resolve_recipient" and tool_name == "identity_api":
            send_task = next(
                (t for t in tasks if t.get("action") in {"send_message", "send_email"}),
                {},
            )
            raw = dict(send_task.get("params", {}))
            params = {
                "recipient_ref": raw.get("recipient_ref", raw.get("recipient")),
                "recipient_resolved": raw.get("recipient_resolved"),
                "recipient_id": raw.get("recipient_id"),
            }
            fn = _adapter_dispatch(tool_name)
            _emit_trace_event(
                trace_events,
                event_type="tool_call",
                trace_id=trace_id,
                plan_id=plan_id,
                step_id=step_id,
                tool_name=tool_name,
                payload={"params": params},
            )
            resp = _invoke_adapter_with_retry(fn, params, request_context, trace_id, max_retries)
            if not resp.get("ok"):
                status = _normalize_error_status(step_type, resp.get("error_code", "IDENTITY_ERROR"))
                blocked = True
            else:
                runtime_state["identity"] = resp["data"]
            detail = resp
            _emit_trace_event(
                trace_events,
                event_type="tool_result",
                trace_id=trace_id,
                plan_id=plan_id,
                step_id=step_id,
                tool_name=tool_name,
                payload={"ok": resp.get("ok"), "error_code": resp.get("error_code"), "status": status},
            )

        elif step_type == "send_message" and tool_name == "messaging_api":
            send_task = next((t for t in tasks if t.get("action") == "send_message"), {})
            params = dict(send_task.get("params", {}))
            if runtime_state.get("identity"):
                params.update(runtime_state["identity"])
            recipient_id = params.get("recipient_id") or ""
            message = params.get("message") or ""
            params["idempotency_key"] = _make_idempotency_key(plan_id, step_id, str(recipient_id), str(message))
            fn = _adapter_dispatch(tool_name)
            _emit_trace_event(
                trace_events,
                event_type="tool_call",
                trace_id=trace_id,
                plan_id=plan_id,
                step_id=step_id,
                tool_name=tool_name,
                payload={"params": params},
            )
            resp = _invoke_adapter_with_retry(fn, params, request_context, trace_id, max_retries)
            if not resp.get("ok"):
                status = _normalize_error_status(step_type, resp.get("error_code", "MESSAGE_ERROR"))
            detail = resp
            _emit_trace_event(
                trace_events,
                event_type="tool_result",
                trace_id=trace_id,
                plan_id=plan_id,
                step_id=step_id,
                tool_name=tool_name,
                payload={"ok": resp.get("ok"), "error_code": resp.get("error_code"), "status": status},
            )
        elif step_type == "schedule_event" and tool_name == "calendar_api":
            sched_task = next((t for t in tasks if t.get("action") == "schedule_meeting"), {})
            params = dict(sched_task.get("params", {}))
            fn = _adapter_dispatch(tool_name)
            _emit_trace_event(
                trace_events,
                event_type="tool_call",
                trace_id=trace_id,
                plan_id=plan_id,
                step_id=step_id,
                tool_name=tool_name,
                payload={"params": params},
            )
            resp = _invoke_adapter_with_retry(fn, params, request_context, trace_id, max_retries)
            if not resp.get("ok"):
                status = _normalize_error_status(step_type, resp.get("error_code", "CALENDAR_ERROR"))
            detail = resp
            _emit_trace_event(
                trace_events,
                event_type="tool_result",
                trace_id=trace_id,
                plan_id=plan_id,
                step_id=step_id,
                tool_name=tool_name,
                payload={"ok": resp.get("ok"), "error_code": resp.get("error_code"), "status": status},
            )

        elif step_type == "send_email" and tool_name == "email_api":
            send_task = next((t for t in tasks if t.get("action") == "send_email"), {})
            params = dict(send_task.get("params", {}))
            if runtime_state.get("identity"):
                params.update(runtime_state["identity"])
            recipient_id = params.get("recipient_id") or ""
            message = params.get("message") or ""
            params["idempotency_key"] = _make_idempotency_key(plan_id, step_id, str(recipient_id), str(message))
            fn = _adapter_dispatch(tool_name)
            _emit_trace_event(
                trace_events,
                event_type="tool_call",
                trace_id=trace_id,
                plan_id=plan_id,
                step_id=step_id,
                tool_name=tool_name,
                payload={"params": params},
            )
            resp = _invoke_adapter_with_retry(fn, params, request_context, trace_id, max_retries)
            if not resp.get("ok"):
                status = _normalize_error_status(step_type, resp.get("error_code", "EMAIL_ERROR"))
            detail = resp
            _emit_trace_event(
                trace_events,
                event_type="tool_result",
                trace_id=trace_id,
                plan_id=plan_id,
                step_id=step_id,
                tool_name=tool_name,
                payload={"ok": resp.get("ok"), "error_code": resp.get("error_code"), "status": status},
            )


        else:
            detail = {"simulated": True, "note": "unhandled step type/tool in runtime"}


        execution_results.append(
            {
                "step_id": step_id,
                "type": step_type,
                "tool_name": tool_name,
                "status": status,
                "detail": detail,
                "latency_ms": int((time.time() - started) * 1000),
                "trace_id": trace_id,
                "plan_id": plan_id,
            }
        )

        # hard stop on blocked resolver gates
        if blocked:
            break

    _emit_trace_event(
        trace_events,
        event_type="final_summary",
        trace_id=trace_id,
        plan_id=plan_id,
        payload={
            "blocked": blocked,
            "execution_result_count": len(execution_results),
            **_build_audit_summary(trace_events),
        },
    )
    trace_events = _stamp_trace_run_id(trace_events, run_id)

    return {
        "trace_id": trace_id,
        "plan_id": plan_id,
        "blocked": blocked,
        "execution_results": execution_results,
        "condition_state": condition_state,
        "trace_events": trace_events,
    }
