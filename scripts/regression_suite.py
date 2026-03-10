#!/usr/bin/env python3
import json
import os
import re
import subprocess
import sys
import argparse
import tempfile
import uuid
from pathlib import Path
from dataclasses import dataclass
from typing import Callable, Dict, List


# Ensure `src/` is importable when this script is executed directly.
REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

@dataclass
class Case:
    name: str
    prompt: str
    check: Callable[[Dict], None]


def _run_prompt(prompt: str) -> Dict:
    cmd = [sys.executable, "src/main.py", "--prompt", prompt]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(SRC_DIR) + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    try:
        out = subprocess.check_output(cmd, text=True, cwd=str(REPO_ROOT), env=env, stderr=subprocess.STDOUT)
        return json.loads(out)
    except subprocess.CalledProcessError as exc:
        output = exc.output or ""
        if "Failed to reach Ollama" in output:
            raise RuntimeError("Ollama is unavailable. Start Ollama or run with --cycle-only.") from exc
        raise RuntimeError(f"prompt run failed: {output.strip()}") from exc


def _run_prompt_langgraph(prompt: str, sqlite_path: str, thread_id: str, extra_args: List[str] | None = None) -> Dict:
    cmd = [
        sys.executable,
        "src/main.py",
        "--use-langgraph",
        "--sqlite-path",
        sqlite_path,
        "--thread-id",
        thread_id,
        "--prompt",
        prompt,
    ]
    if extra_args:
        cmd.extend(extra_args)

    env = os.environ.copy()
    env["PYTHONPATH"] = str(SRC_DIR) + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    try:
        out = subprocess.check_output(cmd, text=True, cwd=str(REPO_ROOT), env=env, stderr=subprocess.STDOUT)
        return json.loads(out)
    except subprocess.CalledProcessError as exc:
        output = exc.output or ""
        if "Failed to reach Ollama" in output:
            raise RuntimeError("Ollama is unavailable. Start Ollama or run with --cycle-only.") from exc
        raise RuntimeError(f"langgraph prompt run failed: {output.strip()}") from exc


def _run_guard(args: List[str]) -> Dict:
    cmd = [sys.executable, "scripts/caps_guard.py", *args]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(SRC_DIR) + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    try:
        out = subprocess.check_output(cmd, text=True, cwd=str(REPO_ROOT), env=env, stderr=subprocess.STDOUT)
        return json.loads(out)
    except subprocess.CalledProcessError as exc:
        output = exc.output or ""
        if "Failed to reach Ollama" in output:
            raise RuntimeError("Ollama is unavailable. Start Ollama or run with --cycle-only.") from exc
        raise RuntimeError(f"guard command failed: {output.strip()}") from exc


def _assert(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)


def _check_weather_missing_location(payload: Dict) -> None:
    tg = payload["task_graph"]
    _assert(tg["needs_clarification"] is True, "weather(no location): expected clarification")
    qs = " ".join(tg.get("clarification_questions", [])).lower()
    _assert("location" in qs, "weather(no location): expected location clarification")


def _check_weather_with_location(payload: Dict) -> None:
    tg = payload["task_graph"]
    _assert(tg["needs_clarification"] is False, "weather(with location): should be executable")
    fw = next(t for t in tg["tasks"] if t["action"] == "fetch_weather")
    params = fw["params"]
    _assert("location_ref" in params, "weather(with location): missing location_ref")
    loc = params.get("location_ref")
    _assert(
        isinstance(loc, str) and loc.strip().lower() not in {"from_context", "location_from_context", "unknown", ""},
        "weather(with location): location_ref should be concrete",
    )
    _assert(payload["execution_plan"]["requires_tools"] is True, "weather(with location): requires_tools should be true")


def _check_schedule_email(payload: Dict) -> None:
    tg = payload["task_graph"]
    _assert(any(t["action"] == "schedule_meeting" for t in tg["tasks"]), "schedule+email: missing schedule_meeting")
    _assert(any(t["action"] == "send_email" for t in tg["tasks"]), "schedule+email: missing send_email")
    ep_steps = payload["execution_plan"]["steps"]
    step_types = [s["type"] for s in ep_steps]
    _assert("resolve_recipient" in step_types, "schedule+email: missing resolve_recipient step")
    _assert("send_email" in step_types, "schedule+email: missing send_email step")


def _check_email_text(payload: Dict) -> None:
    tg = payload["task_graph"]
    actions = {t["action"] for t in tg["tasks"]}
    _assert("send_email" in actions and "send_message" in actions, "email+text: expected both send actions")
    ep_step_types = [s["type"] for s in payload["execution_plan"]["steps"]]
    _assert(ep_step_types.count("resolve_recipient") >= 2, "email+text: expected resolver gates before sends")


def _check_time_conditional(payload: Dict) -> None:
    tg = payload["task_graph"]
    eval_task = next(t for t in tg["tasks"] if t["action"] == "evaluate_condition")
    send_task = next(t for t in tg["tasks"] if t["action"] == "send_message")
    cond = send_task.get("condition", "")
    _assert(bool(re.match(r"^[A-Za-z0-9_]+:(true|false)$", cond)), "time conditional: invalid condition token")
    _assert(cond.startswith(f"{eval_task['id']}:"), "time conditional: token must reference evaluate task id")

    steps = payload["execution_plan"]["steps"]
    step_types = [s["type"] for s in steps]
    idx_eval = step_types.index("evaluate_condition")
    idx_policy = step_types.index("policy_check")
    idx_send = step_types.index("send_message")
    _assert(idx_eval < idx_policy, "time conditional: evaluate_condition must run before policy_check")
    _assert(idx_eval < idx_send, "time conditional: evaluate_condition must run before send_message")
    message = send_task.get("params", {}).get("message", "")
    _assert(isinstance(message, str) and not message.lower().startswith("text "), "time conditional: command echo leaked into message body")


def _check_ambiguous(payload: Dict) -> None:
    ap = payload["action_parse"]
    _assert(ap["tasks"] == [], "ambiguous: expected no tasks")
    qs = ap.get("clarification_questions", [])
    _assert(len(qs) > 0, "ambiguous: expected clarification question")
    _assert(payload["execution_plan"]["final_action"] == "request_clarification", "ambiguous: final action mismatch")

def _check_cycle_compile_guard() -> None:
    """Test that a task graph with a dependency cycle fails to compile and results in a clarification plan."""
    from core.task_graph_compiler import compile_task_graph

    task_graph = {
        "schema_version": "1.0",
        "graph_id": "graph_cycle_test",
        "intent": "action_parse",
        "needs_clarification": False,
        "clarification_questions": [],
        "tasks": [
            {
                "id": "t1",
                "action": "send_message",
                "params": {"recipient_ref": "boss", "message": "hi"},
                "depends_on": ["t2"],
                "condition": None,
                "side_effect": True,
            },
            {
                "id": "t2",
                "action": "evaluate_condition",
                "params": {"metric": "time_of_day", "operator": "==", "threshold_value": "16:20"},
                "depends_on": ["t1"],
                "condition": None,
                "side_effect": False,
            },
        ],
    }
    verification = {"valid": True, "issues": []}
    plan = compile_task_graph(task_graph, verification)
    _assert(plan["final_action"] == "request_clarification", "cycle guard: expected clarification final action")
    notes = " ".join(plan.get("notes", [])).lower()
    _assert("cycle" in notes, "cycle guard: expected cycle note in plan")

def _check_langgraph_checkpoint_recovery() -> None:
    prompt = "If weather is below -20C in Toronto, text Jacob I am not coming to university today."
    thread_id = f"regression-{uuid.uuid4().hex[:8]}"

    with tempfile.TemporaryDirectory() as tmpdir:
        sqlite_path = str(Path(tmpdir) / "caps_state.sqlite")

        run_payload = _run_prompt_langgraph(
            prompt,
            sqlite_path=sqlite_path,
            thread_id=thread_id,
        )
        _assert(run_payload["execution_plan"]["final_action"] == "return_response", "checkpoint recovery: expected executable plan")

        state_payload = _run_prompt_langgraph(
            "state",
            sqlite_path=sqlite_path,
            thread_id=thread_id,
            extra_args=["--show-langgraph-state"],
        )
        _assert(state_payload["thread_id"] == thread_id, "checkpoint recovery: wrong thread_id in state")
        _assert(state_payload["next"] == [], "checkpoint recovery: final state should have no next nodes")
        values = state_payload.get("values", {})
        state_header = state_payload.get("state_header", {})
        _assert("execution_plan" in values, "checkpoint recovery: execution_plan missing from persisted state")
        _assert("task_graph" in values, "checkpoint recovery: task_graph missing from persisted state")
        _assert(values.get("action_parse") is None, "checkpoint recovery: action_parse should be cleared after graph build")
        _assert("cir" not in values, "checkpoint recovery: cir should not be persisted")
        _assert("final_result" not in values, "checkpoint recovery: final_result should not be persisted")
        _assert(values.get("request_payload", {}).get("prompt") == prompt, "checkpoint recovery: request payload prompt mismatch")
        hot_state = values.get("hot_state", {})
        warm_cache = values.get("warm_cache", {})
        cold_refs = values.get("cold_refs", {})
        _assert(state_header.get("initialized") is True, "checkpoint recovery: state_header should be initialized")
        _assert(state_header.get("source_prompt") == prompt, "checkpoint recovery: state_header source_prompt mismatch")
        _assert(state_header.get("plan_id") == values.get("execution_plan", {}).get("plan_id"), "checkpoint recovery: state_header plan_id mismatch")
        _assert(state_header.get("manifest_id") == "caps-default", "checkpoint recovery: state_header manifest_id mismatch")
        _assert(state_header.get("manifest_version") == "1.0", "checkpoint recovery: state_header manifest_version mismatch")
        _assert(hot_state.get("source_prompt") == prompt, "checkpoint recovery: hot_state source_prompt mismatch")
        _assert(isinstance(hot_state.get("active_plan"), list), "checkpoint recovery: missing hot_state active_plan")
        _assert(isinstance(warm_cache, dict), "checkpoint recovery: warm_cache missing")
        _assert("last_action_result" in warm_cache, "checkpoint recovery: warm_cache last_action_result missing")
        _assert("memory_digest" in warm_cache, "checkpoint recovery: warm_cache memory_digest missing")
        _assert(cold_refs.get("plan_id") == values.get("execution_plan", {}).get("plan_id"), "checkpoint recovery: cold_refs plan_id mismatch")
        _assert(values.get("manifest_context", {}).get("manifest_id") == "caps-default", "checkpoint recovery: manifest_context manifest_id mismatch")


        history_payload = _run_prompt_langgraph(
            "history",
            sqlite_path=sqlite_path,
            thread_id=thread_id,
            extra_args=["--show-langgraph-history", "--history-limit", "5"],
        )
        _assert(history_payload["thread_id"] == thread_id, "checkpoint recovery: wrong thread_id in history")
        _assert(history_payload["count"] > 0, "checkpoint recovery: expected non-empty history")

        history = history_payload.get("history", [])
        _assert(any(snapshot.get("next") == [] for snapshot in history), "checkpoint recovery: expected final snapshot in history")
        _assert(
            any(snapshot.get("next") == ["finalize"] for snapshot in history)
            or any(snapshot.get("next") == ["execute"] for snapshot in history)
            or any(snapshot.get("next") == ["compile"] for snapshot in history),
            "checkpoint recovery: expected intermediate graph progression in history",
        )


def _check_langgraph_hitl_paths() -> None:
    # Use deterministic thresholds so live-weather variance does not make HITL tests flaky.
    false_prompt = "If weather is below -100C in Toronto, text Jacob I am not coming to university today."
    true_prompt = "If weather is below 100C in Toronto, text Jacob I am not coming to university today."

    with tempfile.TemporaryDirectory() as tmpdir:
        sqlite_path = str(Path(tmpdir) / "caps_hitl.sqlite")

        false_thread = f"hitl-false-{uuid.uuid4().hex[:8]}"
        false_payload = _run_prompt_langgraph(
            false_prompt,
            sqlite_path=sqlite_path,
            thread_id=false_thread,
            extra_args=["--execute-live"],
        )
        false_runtime = false_payload.get("runtime_execution")
        _assert(isinstance(false_runtime, dict), "hitl false: expected runtime_execution summary payload")
        _assert(false_runtime.get("blocked") is False, "hitl false: should not block for review")
        _assert("condition not met" in str(false_runtime.get("summary", "")).lower(), "hitl false: missing condition-not-met summary")

        false_state = _run_prompt_langgraph(
            "state",
            sqlite_path=sqlite_path,
            thread_id=false_thread,
            extra_args=["--show-langgraph-state"],
        )
        _assert(false_state.get("next") == [], "hitl false: final state should have no next nodes")
        false_hot = false_state.get("values", {}).get("hot_state", {})
        false_header = false_state.get("state_header", {})
        false_trace_events = false_state.get("values", {}).get("warm_cache", {}).get("trace_events", [])
        _assert(false_hot.get("review_required") in {False, None}, "hitl false: should not require review")
        _assert(isinstance(false_hot.get("condition_state"), dict), "hitl false: missing hot_state condition_state")
        _assert("condition not met" in str(false_header.get("memory_digest", "")).lower(), "hitl false: missing state_header memory_digest")
        _assert(
            all(e.get("event_type") in {"decision", "review_resume", "tool_call", "tool_result", "final_summary"} for e in false_trace_events),
            "hitl false: unexpected trace event type",
        )

        approve_thread = f"hitl-approve-{uuid.uuid4().hex[:8]}"
        approve_blocked = _run_prompt_langgraph(
            true_prompt,
            sqlite_path=sqlite_path,
            thread_id=approve_thread,
            extra_args=["--execute-live"],
        )
        approve_runtime = approve_blocked.get("runtime_execution", {})
        _assert(approve_runtime.get("blocked") is True, "hitl approve: expected initial review block")
        sink_steps = approve_runtime.get("review_summary", {}).get("sink_steps", [])
        _assert(len(sink_steps) > 0, "hitl approve: expected actionable sink steps")
        _assert(isinstance(sink_steps[0].get("recipient_id"), str), "hitl approve: expected resolved recipient in review payload")

        approve_blocked_state = _run_prompt_langgraph(
            "state",
            sqlite_path=sqlite_path,
            thread_id=approve_thread,
            extra_args=["--show-langgraph-state"],
        )
        blocked_header = approve_blocked_state.get("state_header", {}) or {}
        blocked_trace_events = (
            approve_blocked_state.get("values", {}).get("warm_cache", {}).get("trace_events", [])
        )
        blocked_decisions = [
            e for e in blocked_trace_events
            if e.get("event_type") == "decision"
            and e.get("tool_name") == "messaging_api"
        ]
        blocked_run_ids = {e.get("run_id") for e in blocked_trace_events if isinstance(e, dict)}
        _assert(all(isinstance(r, str) and r for r in blocked_run_ids), "hitl approve: blocked trace events missing run_id")
        _assert(len(blocked_run_ids) == 1, "hitl approve: blocked trace should use one run_id")
        _assert(
            any((e.get("payload", {}) or {}).get("decision") == "REVIEW_REQUIRED" for e in blocked_decisions),
            "hitl approve: expected REVIEW_REQUIRED decision in blocked trace",
        )
        blocked_final = [e for e in blocked_trace_events if e.get("event_type") == "final_summary"]
        _assert(blocked_final, "hitl approve: missing final_summary in blocked trace")
        blocked_final_payload = blocked_final[-1].get("payload", {}) or {}
        _assert(blocked_final_payload.get("blocked") is True, "hitl approve: blocked trace final_summary should be blocked")
        _assert(blocked_final_payload.get("pending_review") is True, "hitl approve: blocked trace should mark pending_review")
        _assert(
            blocked_final_payload.get("paused_for_review") is True,
            "hitl approve: blocked trace should mark paused_for_review",
        )
        _assert(
            "messaging_api" in (blocked_final_payload.get("reviewed_tools") or []),
            "hitl approve: blocked trace should include reviewed tool",
        )
        _assert(
            blocked_header.get("review_decision") not in {"approve", "reject"},
            "hitl approve: blocked state should not carry resolved review_decision",
        )
        blocked_digest = str(blocked_header.get("memory_digest", "") or "").lower()
        _assert(
            ("approval required" in blocked_digest) or ("pending" in blocked_digest),
            "hitl approve: blocked state should describe pending approval, not completed side-effects",
        )

        approve_final = _run_prompt_langgraph(
            "resume",
            sqlite_path=sqlite_path,
            thread_id=approve_thread,
            extra_args=["--resume-review", "approve"],
        )
        approve_final_runtime = approve_final.get("runtime_execution", {})
        _assert(approve_final_runtime.get("blocked") is False, "hitl approve: should unblock after approval")
        _assert(
            any(step.get("type") == "send_message" and step.get("status") == "ok" for step in approve_final_runtime.get("execution_results", [])),
            "hitl approve: expected successful send_message after approval",
        )
        approve_final_state = _run_prompt_langgraph(
            "state",
            sqlite_path=sqlite_path,
            thread_id=approve_thread,
            extra_args=["--show-langgraph-state"],
        )
        approve_trace_events = (
            approve_final_state.get("values", {}).get("warm_cache", {}).get("trace_events", [])
        )
        approve_decisions = [
            e for e in approve_trace_events
            if e.get("event_type") == "decision" and e.get("tool_name") == "messaging_api"
        ]
        approve_run_ids = {e.get("run_id") for e in approve_trace_events if isinstance(e, dict)}
        _assert(all(isinstance(r, str) and r for r in approve_run_ids), "hitl approve: approve trace events missing run_id")
        _assert(
            len(approve_run_ids) >= 2,
            "hitl approve: approve trace should preserve prior and current run_id values",
        )
        approve_review_resume = [e for e in approve_trace_events if e.get("event_type") == "review_resume"]
        _assert(approve_review_resume, "hitl approve: expected review_resume event after resume")
        _assert(
            any((e.get("payload", {}) or {}).get("action") == "approve" for e in approve_review_resume),
            "hitl approve: review_resume action should be approve",
        )
        _assert(
            any((e.get("payload", {}) or {}).get("decision") == "REVIEW_REQUIRED" for e in approve_decisions),
            "hitl approve: expected REVIEW_REQUIRED decision to persist after resume",
        )
        blocked_trace_id = next((e.get("trace_id") for e in blocked_trace_events if e.get("trace_id")), None)
        approve_trace_id = next((e.get("trace_id") for e in approve_trace_events if e.get("trace_id")), None)
        _assert(
            blocked_trace_id == approve_trace_id and isinstance(blocked_trace_id, str) and blocked_trace_id,
            "hitl approve: trace_id should remain stable across pause/resume for one workflow thread",
        )

        reject_thread = f"hitl-reject-{uuid.uuid4().hex[:8]}"
        reject_blocked = _run_prompt_langgraph(
            true_prompt,
            sqlite_path=sqlite_path,
            thread_id=reject_thread,
            extra_args=["--execute-live"],
        )
        _assert(reject_blocked.get("runtime_execution", {}).get("blocked") is True, "hitl reject: expected initial review block")

        reject_final = _run_prompt_langgraph(
            "resume",
            sqlite_path=sqlite_path,
            thread_id=reject_thread,
            extra_args=["--resume-review", "reject"],
        )
        reject_final_runtime = reject_final.get("runtime_execution", {})
        _assert(reject_final_runtime.get("rejected") is True, "hitl reject: expected rejected result after rejection")
        _assert(
            not any(step.get("type") == "send_message" for step in reject_final_runtime.get("execution_results", [])),
            "hitl reject: should not execute send_message after rejection",
        )


def _check_policy_precedence_pack() -> None:
    from core.policy_engine import (
        DECISION_ALLOW,
        DECISION_BLOCK,
        DECISION_REVIEW,
        REASON_ARGS_FORBIDDEN_PATTERN,
        REASON_REVIEW_POLICY_MATCHED,
        REASON_POLICY_CONFLICT,
        REASON_TOOL_DENYLISTED,
        REASON_TOOL_NOT_ALLOWED,
        REASON_TOOL_UNKNOWN,
        evaluate_tool_policy,
    )

    manifest = {
        "manifest_id": "test-manifest",
        "manifest_version": "1.0",
        "active_tools": ["weather_api", "messaging_api", "email_api", "notes_api", "calendar_api"],
        "tool_side_effect_classes": {
            "weather_api": "READ",
            "messaging_api": "WRITE",
            "email_api": "WRITE",
            "notes_api": "WRITE",
            "calendar_api": "IRREVERSIBLE",
        },
        "sink_tools": ["messaging_api", "email_api"],
        "review_policies": {
            "sink_tools_require_review": True,
            "write_tools_require_review": True,
            "block_irreversible_tools": True,
            "precedence": ["BLOCK", "REVIEW_REQUIRED", "ALLOW"],
            "deny_tools": ["email_api"],
            "allow_tools": ["weather_api", "messaging_api", "email_api", "notes_api", "calendar_api"],
            "forbidden_arg_patterns": ["rm -rf"],
        },
        "constraint_flags": {
            "enforce_tool_registry": True,
            "allow_unknown_tools": False,
        },
    }

    deny_vs_allow = evaluate_tool_policy(
        step_id="s1",
        tool_name="email_api",
        params={},
        manifest_context=manifest,
        approved_for_sink=False,
    )
    _assert(deny_vs_allow["decision"] == DECISION_BLOCK, "policy pack: deny should beat allow")
    _assert(deny_vs_allow["reason_code"] == REASON_TOOL_DENYLISTED, "policy pack: deny reason mismatch")

    deny_vs_review = evaluate_tool_policy(
        step_id="s2",
        tool_name="email_api",
        params={},
        manifest_context=manifest,
        approved_for_sink=False,
    )
    _assert(deny_vs_review["decision"] == DECISION_BLOCK, "policy pack: deny should beat review")
    _assert(deny_vs_review.get("precedence_resolved") is True, "policy pack: expected precedence resolution marker")
    _assert(
        deny_vs_review.get("resolution_reason_code") == REASON_POLICY_CONFLICT,
        "policy pack: expected POLICY_CONFLICT_RESOLVED marker",
    )
    _assert(deny_vs_review.get("winning_rule_id"), "policy pack: missing winning_rule_id")

    review_vs_allow = evaluate_tool_policy(
        step_id="s3",
        tool_name="messaging_api",
        params={"message": "hi"},
        manifest_context=manifest,
        approved_for_sink=False,
    )
    _assert(review_vs_allow["decision"] == DECISION_REVIEW, "policy pack: review should beat allow")
    _assert(review_vs_allow["reason_code"] == REASON_REVIEW_POLICY_MATCHED, "policy pack: review reason mismatch")

    unknown_tool = evaluate_tool_policy(
        step_id="s4",
        tool_name="payments_api",
        params={},
        manifest_context=manifest,
        approved_for_sink=False,
    )
    _assert(unknown_tool["decision"] == DECISION_BLOCK, "policy pack: unknown tool should block")
    _assert(unknown_tool["reason_code"] == REASON_TOOL_UNKNOWN, "policy pack: unknown tool reason mismatch")

    sink_review = evaluate_tool_policy(
        step_id="s5",
        tool_name="messaging_api",
        params={"message": "hello"},
        manifest_context=manifest,
        approved_for_sink=False,
    )
    _assert(sink_review["decision"] == DECISION_REVIEW, "policy pack: sink tool should trigger review")
    _assert(sink_review.get("rule_id") == "REVIEW_SINK_TOOL", "policy pack: sink review rule id mismatch")

    write_class_review = evaluate_tool_policy(
        step_id="s5b",
        tool_name="notes_api",
        params={"text": "draft"},
        manifest_context=manifest,
        approved_for_sink=False,
    )
    _assert(write_class_review["decision"] == DECISION_REVIEW, "policy pack: write class should trigger review")
    _assert(
        write_class_review["reason_code"] == REASON_REVIEW_POLICY_MATCHED,
        "policy pack: write class review reason mismatch",
    )
    _assert(
        write_class_review.get("rule_id") == "REVIEW_WRITE_CLASS",
        "policy pack: write class review rule id mismatch",
    )

    irreversible_block = evaluate_tool_policy(
        step_id="s5c",
        tool_name="calendar_api",
        params={"title": "prod deploy"},
        manifest_context=manifest,
        approved_for_sink=False,
    )
    _assert(irreversible_block["decision"] == DECISION_BLOCK, "policy pack: irreversible tool should block")
    _assert(
        irreversible_block["reason_code"] == REASON_TOOL_NOT_ALLOWED,
        "policy pack: irreversible block reason mismatch",
    )
    _assert(
        irreversible_block.get("rule_id") == "BLOCK_IRREVERSIBLE",
        "policy pack: irreversible block rule id mismatch",
    )

    forbidden_args = evaluate_tool_policy(
        step_id="s6",
        tool_name="weather_api",
        params={"query": "rm -rf /"},
        manifest_context=manifest,
        approved_for_sink=False,
    )
    _assert(forbidden_args["decision"] == DECISION_BLOCK, "policy pack: forbidden args should block")
    _assert(forbidden_args["reason_code"] == REASON_ARGS_FORBIDDEN_PATTERN, "policy pack: forbidden args reason mismatch")

    allowlist_mode = evaluate_tool_policy(
        step_id="s7",
        tool_name="location_api",
        params={},
        manifest_context={
            **manifest,
            "constraint_flags": {"enforce_tool_registry": False, "allow_unknown_tools": True},
            "review_policies": {
                **manifest["review_policies"],
                "allow_tools": ["weather_api"],
                "deny_tools": [],
                "forbidden_arg_patterns": [],
            },
        },
        approved_for_sink=False,
    )
    _assert(allowlist_mode["decision"] == DECISION_BLOCK, "policy pack: allowlist should block non-allowed tool")
    _assert(allowlist_mode["reason_code"] == REASON_TOOL_NOT_ALLOWED, "policy pack: allowlist reason mismatch")

    default_deny_mode = evaluate_tool_policy(
        step_id="s8",
        tool_name="custom_api",
        params={},
        manifest_context={
            **manifest,
            "constraint_flags": {
                "enforce_tool_registry": False,
                "allow_unknown_tools": True,
                "default_deny": True,
            },
            "review_policies": {
                **manifest["review_policies"],
                "allow_tools": [],
                "deny_tools": [],
                "forbidden_arg_patterns": [],
                "sink_tools_require_review": False,
            },
            "sink_tools": [],
        },
        approved_for_sink=False,
    )
    _assert(default_deny_mode["decision"] == DECISION_BLOCK, "policy pack: default deny should block")
    _assert(default_deny_mode["rule_id"] == "DEFAULT_DENY", "policy pack: default deny rule id mismatch")
    _assert(
        default_deny_mode["reason_code"] == REASON_TOOL_NOT_ALLOWED,
        "policy pack: default deny reason mismatch",
    )


def _check_guard_cli_contract() -> None:
    with tempfile.TemporaryDirectory(prefix="caps_guard_reg_") as td:
        base = Path(td)
        env = os.environ.copy()
        env["PYTHONPATH"] = str(SRC_DIR) + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")

        invalid_resume_missing_thread = subprocess.run(
            [
                sys.executable,
                "scripts/caps_guard.py",
                "execute",
                "--manifest",
                "src/manifest.json",
                "--resume-review",
                "approve",
            ],
            cwd=str(REPO_ROOT),
            env=env,
            capture_output=True,
            text=True,
        )
        _assert(
            invalid_resume_missing_thread.returncode != 0,
            "guard execute: resume-review without thread-id should fail",
        )
        _assert(
            "--resume-review requires --thread-id" in (invalid_resume_missing_thread.stdout + invalid_resume_missing_thread.stderr),
            "guard execute: missing resume-review/thread-id validation error",
        )

        invalid_resume_with_prompt = subprocess.run(
            [
                sys.executable,
                "scripts/caps_guard.py",
                "execute",
                "--manifest",
                "src/manifest.json",
                "--prompt",
                "hello",
                "--thread-id",
                "t1",
                "--resume-review",
                "approve",
            ],
            cwd=str(REPO_ROOT),
            env=env,
            capture_output=True,
            text=True,
        )
        _assert(
            invalid_resume_with_prompt.returncode != 0,
            "guard execute: resume-review with prompt should fail",
        )
        _assert(
            "--resume-review cannot be used with --prompt" in (invalid_resume_with_prompt.stdout + invalid_resume_with_prompt.stderr),
            "guard execute: missing resume-review/prompt validation error",
        )

        check_dir = base / "check"
        check_payload = _run_guard(
            [
                "check",
                "--manifest",
                "src/manifest.json",
                "--tool",
                "messaging_api",
                "--args-json",
                '{"message":"hello"}',
                "--output-dir",
                str(check_dir),
            ]
        )
        decision = check_payload.get("decision", {})
        _assert(decision.get("decision") == "REVIEW_REQUIRED", "guard check: expected REVIEW_REQUIRED for sink tool")
        _assert(decision.get("reason_code") == "REVIEW_POLICY_MATCHED", "guard check: reason code mismatch")
        _assert(isinstance(decision.get("trace_id"), str) and decision.get("trace_id"), "guard check: trace_id should be set")
        _assert((check_dir / "check_result.json").exists(), "guard check: missing check_result.json")
        _assert((check_dir / "summary.txt").exists(), "guard check: missing summary.txt")

        plan_file = base / "plan.json"
        plan_file.write_text(
            json.dumps(
                {
                    "execution_plan": {
                        "schema_version": "1.0",
                        "plan_id": "plan_guard_regression",
                        "intent": "action_parse",
                        "requires_tools": False,
                        "steps": [
                            {
                                "id": "step_1",
                                "type": "build_response",
                                "description": "Build final deterministic response.",
                                "requires_tool": False,
                                "tool_name": None,
                                "input_keys": ["execution_results"],
                                "condition": None,
                            }
                        ],
                        "final_action": "return_response",
                        "notes": [],
                    },
                    "task_graph": {
                        "schema_version": "1.0",
                        "graph_id": "graph_guard_regression",
                        "intent": "action_parse",
                        "tasks": [],
                        "needs_clarification": False,
                        "clarification_questions": [],
                    },
                }
            ),
            encoding="utf-8",
        )

        exec_dir = base / "execute"
        exec_payload = _run_guard(
            [
                "execute",
                "--manifest",
                "src/manifest.json",
                "--plan",
                str(plan_file),
                "--no-execute-live",
                "--output-dir",
                str(exec_dir),
            ]
        )
        artifacts = exec_payload.get("artifacts", {})
        trace_path = Path(artifacts.get("trace_json", ""))
        _assert(trace_path.exists(), "guard execute: missing trace.json")
        trace_payload = json.loads(trace_path.read_text(encoding="utf-8"))
        _assert(isinstance(trace_payload.get("run_id"), str) and trace_payload.get("run_id"), "guard execute: missing run_id")
        _assert(
            isinstance(trace_payload.get("current_run_id"), str) and trace_payload.get("current_run_id"),
            "guard execute: missing current_run_id",
        )
        _assert(
            isinstance(trace_payload.get("artifact_run_id"), str) and trace_payload.get("artifact_run_id"),
            "guard execute: missing artifact_run_id",
        )
        _assert(isinstance(trace_payload.get("run_ids"), list), "guard execute: missing run_ids")
        events = trace_payload.get("events", [])
        _assert(len(events) > 0, "guard execute: trace events should not be empty")
        _assert(
            all(e.get("event_type") in {"decision", "review_resume", "tool_call", "tool_result", "final_summary"} for e in events),
            "guard execute: unexpected trace event type",
        )
        _assert(all(isinstance(e.get("run_id"), str) and e.get("run_id") for e in events), "guard execute: missing event run_id")
        ordered_run_ids: list[str] = []
        for event in events:
            event_run_id = event.get("run_id")
            if event_run_id not in ordered_run_ids:
                ordered_run_ids.append(event_run_id)
        _assert(trace_payload.get("run_ids") == ordered_run_ids, "guard execute: run_ids ordering mismatch")
        _assert(
            trace_payload.get("current_run_id") == ordered_run_ids[-1],
            "guard execute: current_run_id should match latest event run_id",
        )
        _assert(
            trace_payload.get("run_id") == trace_payload.get("current_run_id"),
            "guard execute: run_id alias should match current_run_id",
        )
        decision_events = [e for e in events if e.get("event_type") == "decision"]
        _assert(len(decision_events) > 0, "guard execute: expected decision event")
        d_payload = decision_events[0].get("payload", {})
        _assert(d_payload.get("reason_code") == "NON_TOOL_STEP", "guard execute: non-tool reason code mismatch")
        for key in [
            "decision",
            "reason_code",
            "rule_id",
            "manifest_id",
            "manifest_version",
            "timestamp_ms",
        ]:
            _assert(key in d_payload, f"guard execute: decision payload missing '{key}'")

        trace_graph_path = Path(artifacts.get("trace_graph_json", ""))
        _assert(trace_graph_path.exists(), "guard execute: missing trace_graph.json")
        trace_graph_payload = json.loads(trace_graph_path.read_text(encoding="utf-8"))
        nodes = trace_graph_payload.get("nodes", [])
        edges = trace_graph_payload.get("edges", [])
        _assert(len(nodes) == len(events), "guard execute: trace_graph node/event mismatch")
        _assert(len(edges) == max(0, len(nodes) - 1), "guard execute: trace_graph edge count mismatch")
        for idx, node in enumerate(nodes):
            event = events[idx]
            _assert(node.get("order") == idx, "guard execute: trace_graph node order mismatch")
            _assert(node.get("event_type") == event.get("event_type"), "guard execute: trace_graph event_type mismatch")
            _assert(node.get("step_id") == event.get("step_id"), "guard execute: trace_graph step_id mismatch")
            _assert(node.get("tool_name") == event.get("tool_name"), "guard execute: trace_graph tool_name mismatch")


def _check_guard_cli_read_write_review_flow() -> None:
    with tempfile.TemporaryDirectory(prefix="caps_guard_rw_") as td:
        base = Path(td)
        plan_file = base / "plan_rw.json"
        plan_file.write_text(
            json.dumps(
                {
                    "execution_plan": {
                        "schema_version": "1.0",
                        "plan_id": "plan_guard_rw",
                        "intent": "action_parse",
                        "requires_tools": True,
                        "steps": [
                            {
                                "id": "step_1",
                                "type": "resolve_recipient",
                                "description": "Resolve recipient before send.",
                                "requires_tool": True,
                                "tool_name": "identity_api",
                                "input_keys": ["params"],
                                "condition": None,
                            },
                            {
                                "id": "step_2",
                                "type": "send_message",
                                "description": "Send message action.",
                                "requires_tool": True,
                                "tool_name": "messaging_api",
                                "input_keys": ["params"],
                                "condition": None,
                            },
                        ],
                        "final_action": "return_response",
                        "notes": [],
                    },
                    "task_graph": {
                        "schema_version": "1.0",
                        "graph_id": "graph_guard_rw",
                        "intent": "action_parse",
                        "tasks": [
                            {
                                "id": "t1",
                                "action": "send_message",
                                "params": {
                                    "recipient_ref": "Jacob",
                                    "recipient_resolved": False,
                                    "message": "Hello from CAPS guard",
                                },
                                "depends_on": [],
                                "condition": None,
                                "side_effect": True,
                            }
                        ],
                        "needs_clarification": False,
                        "clarification_questions": [],
                    },
                }
            ),
            encoding="utf-8",
        )

        no_approve_dir = base / "execute_no_approve"
        no_approve_payload = _run_guard(
            [
                "execute",
                "--manifest",
                "src/manifest.json",
                "--plan",
                str(plan_file),
                "--output-dir",
                str(no_approve_dir),
            ]
        )
        trace_no_approve = json.loads((no_approve_dir / "trace.json").read_text(encoding="utf-8"))
        events_no_approve = trace_no_approve.get("events", [])
        summary_no_approve = trace_no_approve.get("summary", {})
        _assert(
            trace_no_approve.get("run_id") == trace_no_approve.get("current_run_id"),
            "guard rw flow (no approve): run_id alias mismatch",
        )
        no_approve_run_ids = []
        for event in events_no_approve:
            event_run_id = event.get("run_id")
            if event_run_id not in no_approve_run_ids:
                no_approve_run_ids.append(event_run_id)
        _assert(no_approve_run_ids, "guard rw flow (no approve): missing event run_ids")
        _assert(
            trace_no_approve.get("run_ids") == no_approve_run_ids,
            "guard rw flow (no approve): run_ids ordering mismatch",
        )
        _assert(
            trace_no_approve.get("current_run_id") == no_approve_run_ids[-1],
            "guard rw flow (no approve): current_run_id mismatch",
        )
        sequence_no_approve = [(e.get("event_type"), e.get("step_id"), e.get("tool_name")) for e in events_no_approve]
        _assert(
            sequence_no_approve
            == [
                ("decision", "step_1", "identity_api"),
                ("tool_call", "step_1", "identity_api"),
                ("tool_result", "step_1", "identity_api"),
                ("decision", "step_2", "messaging_api"),
                ("final_summary", None, None),
            ],
            f"guard rw flow (no approve): unexpected event ordering {sequence_no_approve}",
        )
        _assert(
            ("tool_call", "step_1", "identity_api") in sequence_no_approve,
            "guard rw flow (no approve): missing READ tool_call",
        )
        _assert(
            ("tool_result", "step_1", "identity_api") in sequence_no_approve,
            "guard rw flow (no approve): missing READ tool_result",
        )
        write_decisions = [
            e
            for e in events_no_approve
            if e.get("event_type") == "decision" and e.get("step_id") == "step_2"
        ]
        _assert(write_decisions, "guard rw flow (no approve): missing WRITE decision")
        _assert(
            write_decisions[0].get("payload", {}).get("decision") == "REVIEW_REQUIRED",
            "guard rw flow (no approve): WRITE should be REVIEW_REQUIRED",
        )
        _assert(
            not any(e.get("event_type") == "tool_call" and e.get("step_id") == "step_2" for e in events_no_approve),
            "guard rw flow (no approve): WRITE tool_call should not execute before approval",
        )
        no_approve_final = [e for e in events_no_approve if e.get("event_type") == "final_summary"][-1]
        no_approve_final_payload = no_approve_final.get("payload", {}) or {}
        _assert(
            no_approve_final_payload.get("event_count") == len(events_no_approve),
            "guard rw flow (no approve): final summary event_count mismatch",
        )
        _assert(
            summary_no_approve.get("event_count") == len(events_no_approve),
            "guard rw flow (no approve): trace summary event_count mismatch",
        )
        _assert(
            no_approve_final_payload.get("execution_result_count") == 1,
            "guard rw flow (no approve): execution_result_count should reflect safe-phase results",
        )
        _assert(
            "sink_step_count" in no_approve_final_payload,
            "guard rw flow (no approve): missing canonical sink_step_count",
        )
        _assert(
            no_approve_final_payload.get("paused_for_review") is False,
            "guard rw flow (no approve): paused_for_review should be false",
        )
        _assert(
            "actionable_sink_count" not in no_approve_final_payload,
            "guard rw flow (no approve): legacy actionable_sink_count should not be emitted",
        )
        for key in [
            "decision_counts",
            "tool_call_count",
            "tool_result_count",
            "tool_error_count",
            "event_count",
            "reviewed_tools",
            "blocked_tools",
        ]:
            _assert(key in (no_approve_final.get("payload", {}) or {}), f"guard rw flow (no approve): final summary missing {key}")
        no_approve_graph_path = Path((no_approve_payload.get("artifacts", {}) or {}).get("trace_graph_json", ""))
        _assert(no_approve_graph_path.exists(), "guard rw flow (no approve): missing trace_graph.json")
        no_approve_graph = json.loads(no_approve_graph_path.read_text(encoding="utf-8"))
        no_approve_nodes = no_approve_graph.get("nodes", [])
        no_approve_edges = no_approve_graph.get("edges", [])
        _assert(
            len(no_approve_nodes) == len(events_no_approve),
            "guard rw flow (no approve): trace_graph node/event mismatch",
        )
        _assert(
            len(no_approve_edges) == max(0, len(no_approve_nodes) - 1),
            "guard rw flow (no approve): trace_graph edge count mismatch",
        )
        for idx, node in enumerate(no_approve_nodes):
            event = events_no_approve[idx]
            _assert(node.get("event_type") == event.get("event_type"), "guard rw flow (no approve): graph event_type mismatch")
            _assert(node.get("step_id") == event.get("step_id"), "guard rw flow (no approve): graph step_id mismatch")
            _assert(node.get("tool_name") == event.get("tool_name"), "guard rw flow (no approve): graph tool_name mismatch")

        approve_dir = base / "execute_approve"
        approve_payload = _run_guard(
            [
                "execute",
                "--manifest",
                "src/manifest.json",
                "--plan",
                str(plan_file),
                "--approve-sinks",
                "--output-dir",
                str(approve_dir),
            ]
        )
        trace_approve = json.loads((approve_dir / "trace.json").read_text(encoding="utf-8"))
        events_approve = trace_approve.get("events", [])
        summary_approve = trace_approve.get("summary", {})
        _assert(
            trace_approve.get("run_id") == trace_approve.get("current_run_id"),
            "guard rw flow (approve): run_id alias mismatch",
        )
        approve_run_ids = []
        for event in events_approve:
            event_run_id = event.get("run_id")
            if event_run_id not in approve_run_ids:
                approve_run_ids.append(event_run_id)
        _assert(approve_run_ids, "guard rw flow (approve): missing event run_ids")
        _assert(
            trace_approve.get("run_ids") == approve_run_ids,
            "guard rw flow (approve): run_ids ordering mismatch",
        )
        _assert(
            trace_approve.get("current_run_id") == approve_run_ids[-1],
            "guard rw flow (approve): current_run_id mismatch",
        )
        sequence_approve = [(e.get("event_type"), e.get("step_id"), e.get("tool_name")) for e in events_approve]
        _assert(
            sequence_approve
            == [
                ("decision", "step_1", "identity_api"),
                ("tool_call", "step_1", "identity_api"),
                ("tool_result", "step_1", "identity_api"),
                ("decision", "step_2", "messaging_api"),
                ("tool_call", "step_2", "messaging_api"),
                ("tool_result", "step_2", "messaging_api"),
                ("final_summary", None, None),
            ],
            f"guard rw flow (approve): unexpected event ordering {sequence_approve}",
        )
        _assert(
            any(e.get("event_type") == "tool_call" and e.get("step_id") == "step_2" for e in events_approve),
            "guard rw flow (approve): missing WRITE tool_call after approval",
        )
        _assert(
            any(e.get("event_type") == "tool_result" and e.get("step_id") == "step_2" for e in events_approve),
            "guard rw flow (approve): missing WRITE tool_result after approval",
        )
        _assert(
            any(
                e.get("event_type") == "decision"
                and e.get("step_id") == "step_2"
                and e.get("payload", {}).get("decision") == "REVIEW_REQUIRED"
                for e in events_approve
            ),
            "guard rw flow (approve): missing pre-approval REVIEW_REQUIRED decision",
        )
        approve_final = [e for e in events_approve if e.get("event_type") == "final_summary"][-1]
        approve_final_payload = approve_final.get("payload", {}) or {}
        _assert(
            approve_final_payload.get("event_count") == len(events_approve),
            "guard rw flow (approve): final summary event_count mismatch",
        )
        _assert(
            summary_approve.get("event_count") == len(events_approve),
            "guard rw flow (approve): trace summary event_count mismatch",
        )
        _assert(
            approve_final_payload.get("execution_result_count") == 1,
            "guard rw flow (approve): execution_result_count should reflect sink-phase results",
        )
        _assert(
            "sink_step_count" in approve_final_payload,
            "guard rw flow (approve): missing canonical sink_step_count",
        )
        _assert(
            approve_final_payload.get("paused_for_review") is False,
            "guard rw flow (approve): paused_for_review should be false",
        )
        _assert(
            "actionable_sink_count" not in approve_final_payload,
            "guard rw flow (approve): legacy actionable_sink_count should not be emitted",
        )
        for key in [
            "decision_counts",
            "tool_call_count",
            "tool_result_count",
            "tool_error_count",
            "event_count",
            "reviewed_tools",
            "blocked_tools",
        ]:
            _assert(key in (approve_final.get("payload", {}) or {}), f"guard rw flow (approve): final summary missing {key}")
        approve_graph_path = Path((approve_payload.get("artifacts", {}) or {}).get("trace_graph_json", ""))
        _assert(approve_graph_path.exists(), "guard rw flow (approve): missing trace_graph.json")
        approve_graph = json.loads(approve_graph_path.read_text(encoding="utf-8"))
        approve_nodes = approve_graph.get("nodes", [])
        approve_edges = approve_graph.get("edges", [])
        _assert(len(approve_nodes) == len(events_approve), "guard rw flow (approve): trace_graph node/event mismatch")
        _assert(
            len(approve_edges) == max(0, len(approve_nodes) - 1),
            "guard rw flow (approve): trace_graph edge count mismatch",
        )
        for idx, node in enumerate(approve_nodes):
            event = events_approve[idx]
            _assert(node.get("event_type") == event.get("event_type"), "guard rw flow (approve): graph event_type mismatch")
            _assert(node.get("step_id") == event.get("step_id"), "guard rw flow (approve): graph step_id mismatch")
            _assert(node.get("tool_name") == event.get("tool_name"), "guard rw flow (approve): graph tool_name mismatch")


def _check_guard_side_effect_classification_flow() -> None:
    with tempfile.TemporaryDirectory(prefix="caps_guard_sidefx_") as td:
        base = Path(td)
        manifest_file = base / "manifest_sidefx.json"
        manifest_file.write_text(
            json.dumps(
                {
                    "manifest_id": "caps-sidefx-demo",
                    "manifest_version": "1.0",
                    "tool_registry": [
                        {
                            "name": "identity_api",
                            "binding": "identity_api",
                            "description": "Resolve recipient identities.",
                            "side_effect_class": "READ",
                        },
                        {
                            "name": "messaging_api",
                            "binding": "messaging_api",
                            "description": "Send outbound messages.",
                            "side_effect_class": "WRITE",
                        },
                        {
                            "name": "calendar_api",
                            "binding": "calendar_api",
                            "description": "Apply irreversible schedule mutations.",
                            "side_effect_class": "IRREVERSIBLE",
                        },
                    ],
                    "sink_tools": [],
                    "review_policies": {
                        "sink_tools_require_review": False,
                        "write_tools_require_review": True,
                        "block_irreversible_tools": True,
                        "precedence": ["BLOCK", "REVIEW_REQUIRED", "ALLOW"],
                    },
                    "constraint_flags": {
                        "enforce_tool_registry": True,
                        "allow_unknown_tools": False,
                    },
                }
            ),
            encoding="utf-8",
        )

        write_plan = base / "plan_write_non_sink.json"
        write_plan.write_text(
            json.dumps(
                {
                    "execution_plan": {
                        "schema_version": "1.0",
                        "plan_id": "plan_write_non_sink",
                        "intent": "action_parse",
                        "requires_tools": True,
                        "steps": [
                            {
                                "id": "step_1",
                                "type": "resolve_recipient",
                                "description": "Resolve recipient before send.",
                                "requires_tool": True,
                                "tool_name": "identity_api",
                                "input_keys": ["params"],
                                "condition": None,
                            },
                            {
                                "id": "step_2",
                                "type": "send_message",
                                "description": "Send message action.",
                                "requires_tool": True,
                                "tool_name": "messaging_api",
                                "input_keys": ["params"],
                                "condition": None,
                            },
                        ],
                        "final_action": "return_response",
                        "notes": [],
                    },
                    "task_graph": {
                        "schema_version": "1.0",
                        "graph_id": "graph_write_non_sink",
                        "intent": "action_parse",
                        "tasks": [
                            {
                                "id": "t1",
                                "action": "send_message",
                                "params": {
                                    "recipient_ref": "Jacob",
                                    "recipient_resolved": False,
                                    "message": "Hello from side-effect class demo",
                                },
                                "depends_on": [],
                                "condition": None,
                                "side_effect": True,
                            }
                        ],
                        "needs_clarification": False,
                        "clarification_questions": [],
                    },
                }
            ),
            encoding="utf-8",
        )

        write_dir = base / "write_non_sink"
        _run_guard(
            [
                "execute",
                "--manifest",
                str(manifest_file),
                "--plan",
                str(write_plan),
                "--output-dir",
                str(write_dir),
            ]
        )
        write_trace = json.loads((write_dir / "trace.json").read_text(encoding="utf-8"))
        write_events = write_trace.get("events", [])
        write_decisions = [
            e
            for e in write_events
            if e.get("event_type") == "decision" and e.get("step_id") == "step_2"
        ]
        _assert(write_decisions, "guard sidefx: missing write decision")
        write_payload = write_decisions[0].get("payload", {}) or {}
        _assert(write_payload.get("decision") == "REVIEW_REQUIRED", "guard sidefx: write should require review")
        _assert(
            write_payload.get("reason_code") == "REVIEW_POLICY_MATCHED",
            "guard sidefx: write review reason mismatch",
        )
        _assert(
            write_payload.get("rule_id") == "REVIEW_WRITE_CLASS",
            "guard sidefx: write review rule id mismatch",
        )
        _assert(
            not any(e.get("event_type") == "tool_call" and e.get("step_id") == "step_2" for e in write_events),
            "guard sidefx: write tool_call should not run before review",
        )

        irreversible_plan = base / "plan_irreversible.json"
        irreversible_plan.write_text(
            json.dumps(
                {
                    "execution_plan": {
                        "schema_version": "1.0",
                        "plan_id": "plan_irreversible_block",
                        "intent": "action_parse",
                        "requires_tools": True,
                        "steps": [
                            {
                                "id": "step_1",
                                "type": "schedule_event",
                                "description": "Schedule irreversible calendar event.",
                                "requires_tool": True,
                                "tool_name": "calendar_api",
                                "input_keys": ["params"],
                                "condition": None,
                            }
                        ],
                        "final_action": "return_response",
                        "notes": [],
                    },
                    "task_graph": {
                        "schema_version": "1.0",
                        "graph_id": "graph_irreversible_block",
                        "intent": "action_parse",
                        "tasks": [
                            {
                                "id": "t1",
                                "action": "schedule_meeting",
                                "params": {
                                    "title": "Prod deploy",
                                    "start_time": "2026-03-08T16:00:00Z",
                                    "duration_minutes": 30,
                                },
                                "depends_on": [],
                                "condition": None,
                                "side_effect": True,
                            }
                        ],
                        "needs_clarification": False,
                        "clarification_questions": [],
                    },
                }
            ),
            encoding="utf-8",
        )

        irreversible_dir = base / "irreversible_block"
        _run_guard(
            [
                "execute",
                "--manifest",
                str(manifest_file),
                "--plan",
                str(irreversible_plan),
                "--approve-sinks",
                "--output-dir",
                str(irreversible_dir),
            ]
        )
        irreversible_trace = json.loads((irreversible_dir / "trace.json").read_text(encoding="utf-8"))
        irreversible_events = irreversible_trace.get("events", [])
        irreversible_decisions = [
            e
            for e in irreversible_events
            if e.get("event_type") == "decision" and e.get("step_id") == "step_1"
        ]
        _assert(irreversible_decisions, "guard sidefx: missing irreversible decision")
        irreversible_payload = irreversible_decisions[0].get("payload", {}) or {}
        _assert(irreversible_payload.get("decision") == "BLOCK", "guard sidefx: irreversible tool should block")
        _assert(
            irreversible_payload.get("reason_code") == "TOOL_NOT_ALLOWED",
            "guard sidefx: irreversible block reason mismatch",
        )
        _assert(
            irreversible_payload.get("rule_id") == "BLOCK_IRREVERSIBLE",
            "guard sidefx: irreversible block rule id mismatch",
        )
        _assert(
            not any(e.get("event_type") == "tool_call" and e.get("step_id") == "step_1" for e in irreversible_events),
            "guard sidefx: irreversible tool_call should not execute when blocked",
        )
        irreversible_final = [e for e in irreversible_events if e.get("event_type") == "final_summary"][-1]
        _assert(
            (irreversible_final.get("payload", {}) or {}).get("blocked") is True,
            "guard sidefx: irreversible final summary should be blocked",
        )


def _check_guard_args_forbidden_pattern_flow() -> None:
    with tempfile.TemporaryDirectory(prefix="caps_guard_args_") as td:
        base = Path(td)
        manifest_file = base / "manifest_args.json"
        manifest_file.write_text(
            json.dumps(
                {
                    "manifest_id": "caps-args-regression",
                    "manifest_version": "1.0",
                    "tool_registry": [
                        {
                            "name": "weather_api",
                            "binding": "weather_api",
                            "description": "Fetch weather data.",
                            "side_effect_class": "READ",
                        }
                    ],
                    "sink_tools": [],
                    "review_policies": {
                        "sink_tools_require_review": False,
                        "precedence": ["BLOCK", "REVIEW_REQUIRED", "ALLOW"],
                        "forbidden_arg_patterns": ["drop table"],
                    },
                    "constraint_flags": {
                        "enforce_tool_registry": True,
                        "allow_unknown_tools": False,
                    },
                }
            ),
            encoding="utf-8",
        )

        check_dir = base / "check_args_block"
        check_payload = _run_guard(
            [
                "check",
                "--manifest",
                str(manifest_file),
                "--tool",
                "weather_api",
                "--args-json",
                '{"query":"drop table users"}',
                "--output-dir",
                str(check_dir),
            ]
        )
        check_decision = check_payload.get("decision", {}) or {}
        _assert(check_decision.get("decision") == "BLOCK", "guard args: check should block")
        _assert(
            check_decision.get("reason_code") == "ARGS_FORBIDDEN_PATTERN",
            "guard args: check reason code mismatch",
        )
        _assert(
            isinstance(check_decision.get("rule_id"), str)
            and check_decision.get("rule_id").startswith("FORBIDDEN_ARG_PATTERN:"),
            "guard args: check rule id mismatch",
        )

        plan_file = base / "plan_args_block.json"
        plan_file.write_text(
            json.dumps(
                {
                    "execution_plan": {
                        "schema_version": "1.0",
                        "plan_id": "plan_args_block",
                        "intent": "action_parse",
                        "requires_tools": True,
                        "steps": [
                            {
                                "id": "step_1",
                                "type": "fetch_data",
                                "description": "Fetch weather data.",
                                "requires_tool": True,
                                "tool_name": "weather_api",
                                "input_keys": ["params"],
                                "condition": None,
                            }
                        ],
                        "final_action": "return_response",
                        "notes": [],
                    },
                    "task_graph": {
                        "schema_version": "1.0",
                        "graph_id": "graph_args_block",
                        "intent": "action_parse",
                        "tasks": [
                            {
                                "id": "t1",
                                "action": "fetch_weather",
                                "params": {
                                    "query": "drop table users",
                                    "location_ref": "Toronto",
                                    "location_resolved": False,
                                },
                                "depends_on": [],
                                "condition": None,
                                "side_effect": False,
                            }
                        ],
                        "needs_clarification": False,
                        "clarification_questions": [],
                    },
                }
            ),
            encoding="utf-8",
        )

        exec_dir = base / "execute_args_block"
        _run_guard(
            [
                "execute",
                "--manifest",
                str(manifest_file),
                "--plan",
                str(plan_file),
                "--output-dir",
                str(exec_dir),
            ]
        )

        trace_payload = json.loads((exec_dir / "trace.json").read_text(encoding="utf-8"))
        events = trace_payload.get("events", [])
        decisions = [
            e
            for e in events
            if e.get("event_type") == "decision" and e.get("step_id") == "step_1"
        ]
        _assert(decisions, "guard args: execute missing decision")
        payload = decisions[0].get("payload", {}) or {}
        _assert(payload.get("decision") == "BLOCK", "guard args: execute should block")
        _assert(payload.get("reason_code") == "ARGS_FORBIDDEN_PATTERN", "guard args: execute reason mismatch")
        _assert(
            isinstance(payload.get("rule_id"), str) and payload.get("rule_id").startswith("FORBIDDEN_ARG_PATTERN:"),
            "guard args: execute rule id mismatch",
        )
        _assert(
            not any(e.get("event_type") == "tool_call" and e.get("step_id") == "step_1" for e in events),
            "guard args: execute should not call blocked tool",
        )
        final_event = [e for e in events if e.get("event_type") == "final_summary"][-1]
        _assert((final_event.get("payload", {}) or {}).get("blocked") is True, "guard args: final summary should be blocked")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run CAPS focused regression prompts (requires local Ollama).")
    parser.add_argument("--cycle-only", action="store_true", help="Run only compiler cycle guard test (no Ollama needed).")
    parser.add_argument(
        "--checkpoint-only",
        action="store_true",
        help="Run only LangGraph checkpoint persistence/recovery test.",
    )
    parser.add_argument(
        "--hitl-only",
        action="store_true",
        help="Run only LangGraph HITL approve/reject regression checks.",
    )
    parser.add_argument(
        "--policy-only",
        action="store_true",
        help="Run only policy precedence and reason-code checks.",
    )
    parser.add_argument(
        "--guard-only",
        action="store_true",
        help="Run only guard CLI contract checks (no Ollama needed).",
    )

    args = parser.parse_args()

    if args.cycle_only:
        failures: List[str] = []
        try:
            _check_cycle_compile_guard()
            print("[PASS] cycle_compile_guard")
        except Exception as exc:  # noqa: BLE001
            failures.append(f"cycle_compile_guard: {exc}")
            print(f"[FAIL] cycle_compile_guard -> {exc}")

        if failures:
            print("\nRegression failures:")
            for f in failures:
                print(f"- {f}")
            sys.exit(1)
        print("\nAll regression cases passed.")
        return
    
    if args.checkpoint_only:
        failures: List[str] = []
        try:
            _check_langgraph_checkpoint_recovery()
            print("[PASS] langgraph_checkpoint_recovery")
        except Exception as exc:  # noqa: BLE001
            failures.append(f"langgraph_checkpoint_recovery: {exc}")
            print(f"[FAIL] langgraph_checkpoint_recovery -> {exc}")

        if failures:
            print("\nRegression failures:")
            for f in failures:
                print(f"- {f}")
            sys.exit(1)
        print("\nAll regression cases passed.")
        return

    if args.hitl_only:
        failures: List[str] = []
        try:
            _check_langgraph_hitl_paths()
            print("[PASS] langgraph_hitl_paths")
        except Exception as exc:  # noqa: BLE001
            failures.append(f"langgraph_hitl_paths: {exc}")
            print(f"[FAIL] langgraph_hitl_paths -> {exc}")

        if failures:
            print("\nRegression failures:")
            for f in failures:
                print(f"- {f}")
            sys.exit(1)
        print("\nAll regression cases passed.")
        return

    if args.policy_only:
        failures: List[str] = []
        try:
            _check_policy_precedence_pack()
            print("[PASS] policy_precedence_pack")
        except Exception as exc:  # noqa: BLE001
            failures.append(f"policy_precedence_pack: {exc}")
            print(f"[FAIL] policy_precedence_pack -> {exc}")

        if failures:
            print("\nRegression failures:")
            for f in failures:
                print(f"- {f}")
            sys.exit(1)
        print("\nAll regression cases passed.")
        return

    if args.guard_only:
        failures: List[str] = []
        try:
            _check_guard_cli_contract()
            print("[PASS] guard_cli_contract")
        except Exception as exc:  # noqa: BLE001
            failures.append(f"guard_cli_contract: {exc}")
            print(f"[FAIL] guard_cli_contract -> {exc}")
        try:
            _check_guard_cli_read_write_review_flow()
            print("[PASS] guard_cli_read_write_review_flow")
        except Exception as exc:  # noqa: BLE001
            failures.append(f"guard_cli_read_write_review_flow: {exc}")
            print(f"[FAIL] guard_cli_read_write_review_flow -> {exc}")
        try:
            _check_guard_side_effect_classification_flow()
            print("[PASS] guard_side_effect_classification_flow")
        except Exception as exc:  # noqa: BLE001
            failures.append(f"guard_side_effect_classification_flow: {exc}")
            print(f"[FAIL] guard_side_effect_classification_flow -> {exc}")
        try:
            _check_guard_args_forbidden_pattern_flow()
            print("[PASS] guard_args_forbidden_pattern_flow")
        except Exception as exc:  # noqa: BLE001
            failures.append(f"guard_args_forbidden_pattern_flow: {exc}")
            print(f"[FAIL] guard_args_forbidden_pattern_flow -> {exc}")

        if failures:
            print("\nRegression failures:")
            for f in failures:
                print(f"- {f}")
            sys.exit(1)
        print("\nAll regression cases passed.")
        return

    cases: List[Case] = [
        Case("weather_missing_location", "If weather is below -20C, text Jacob I am not coming to university today.", _check_weather_missing_location),
        Case("weather_with_location", "If weather is below -20C in Toronto, text Jacob I am not coming to university today.", _check_weather_with_location),
        Case("schedule_email", "Schedule a meeting with Aneesh tomorrow at 3 PM and send a confirmation email.", _check_schedule_email),
        Case("email_and_text", "Send email to my boss and text my friend that I am late.", _check_email_text),
        Case("time_conditional", "If the time is 4:20 pm, text my besties group it's time to blaze up.", _check_time_conditional),
        Case("ambiguous", "...", _check_ambiguous),
    ]

    failures: List[str] = []
    for case in cases:
        try:
            payload = _run_prompt(case.prompt)
            case.check(payload)
            print(f"[PASS] {case.name}")
        except Exception as exc:  # noqa: BLE001
            failures.append(f"{case.name}: {exc}")
            print(f"[FAIL] {case.name} -> {exc}")

    try:
        _check_cycle_compile_guard()
        print("[PASS] cycle_compile_guard")
    except Exception as exc:  # noqa: BLE001
        failures.append(f"cycle_compile_guard: {exc}")
        print(f"[FAIL] cycle_compile_guard -> {exc}")

    try:
        _check_langgraph_checkpoint_recovery()
        print("[PASS] langgraph_checkpoint_recovery")
    except Exception as exc:  # noqa: BLE001
        failures.append(f"langgraph_checkpoint_recovery: {exc}")
        print(f"[FAIL] langgraph_checkpoint_recovery -> {exc}")

    try:
        _check_langgraph_hitl_paths()
        print("[PASS] langgraph_hitl_paths")
    except Exception as exc:  # noqa: BLE001
        failures.append(f"langgraph_hitl_paths: {exc}")
        print(f"[FAIL] langgraph_hitl_paths -> {exc}")

    try:
        _check_policy_precedence_pack()
        print("[PASS] policy_precedence_pack")
    except Exception as exc:  # noqa: BLE001
        failures.append(f"policy_precedence_pack: {exc}")
        print(f"[FAIL] policy_precedence_pack -> {exc}")

    try:
        _check_guard_cli_contract()
        print("[PASS] guard_cli_contract")
    except Exception as exc:  # noqa: BLE001
        failures.append(f"guard_cli_contract: {exc}")
        print(f"[FAIL] guard_cli_contract -> {exc}")
    try:
        _check_guard_cli_read_write_review_flow()
        print("[PASS] guard_cli_read_write_review_flow")
    except Exception as exc:  # noqa: BLE001
        failures.append(f"guard_cli_read_write_review_flow: {exc}")
        print(f"[FAIL] guard_cli_read_write_review_flow -> {exc}")
    try:
        _check_guard_side_effect_classification_flow()
        print("[PASS] guard_side_effect_classification_flow")
    except Exception as exc:  # noqa: BLE001
        failures.append(f"guard_side_effect_classification_flow: {exc}")
        print(f"[FAIL] guard_side_effect_classification_flow -> {exc}")
    try:
        _check_guard_args_forbidden_pattern_flow()
        print("[PASS] guard_args_forbidden_pattern_flow")
    except Exception as exc:  # noqa: BLE001
        failures.append(f"guard_args_forbidden_pattern_flow: {exc}")
        print(f"[FAIL] guard_args_forbidden_pattern_flow -> {exc}")


    if failures:
        print("\nRegression failures:")
        for f in failures:
            print(f"- {f}")
        sys.exit(1)

    print("\nAll regression cases passed.")


if __name__ == "__main__":
    main()
