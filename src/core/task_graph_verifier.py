import re
from typing import Any, Dict, List, Set


ACTION_REQUIRED_PARAMS = {
    "fetch_weather": ["location_ref"],
    "evaluate_condition": ["metric", "operator", "threshold_value"],
    "send_message": ["recipient_ref", "message"],
    "send_email": ["recipient_ref", "message"],
    "schedule_meeting": ["recipient", "date", "time"],
}

def _is_placeholder(v):
    return isinstance(v, str) and v.strip().lower() in {
        "from_context",
        "location_from_context",
        "recipient_from_context",
        "unknown",
        "",
    }


def _tokenize_head(text: Any, limit: int = 8) -> list[str]:
    if not isinstance(text, str):
        return []
    return re.findall(r"[A-Za-z0-9_@'.-]+", text.lower())[:limit]


def _has_head_command_echo(task: Dict[str, Any]) -> bool:
    params = task.get("params", {}) if isinstance(task.get("params"), dict) else {}
    message = params.get("message")
    if not isinstance(message, str):
        return False
    head_tokens = _tokenize_head(message, limit=8)
    if not head_tokens:
        return False

    if len(head_tokens) >= 2 and head_tokens[0] == "send" and head_tokens[1] == "email":
        verb_match = True
    else:
        verb_match = head_tokens[0] in {"text", "message", "email", "notify", "send", "tell", "call"}
    if not verb_match:
        return False

    recipient_anchor = task.get("recipient_anchor")
    if isinstance(recipient_anchor, str) and recipient_anchor.strip():
        recipient_tokens = _tokenize_head(recipient_anchor, limit=4)
        if recipient_tokens and all(token in head_tokens for token in recipient_tokens):
            return True

    verb_anchor = task.get("verb_anchor")
    if isinstance(verb_anchor, str) and verb_anchor.strip():
        verb_tokens = _tokenize_head(verb_anchor, limit=2)
        if verb_tokens and head_tokens[:len(verb_tokens)] == verb_tokens:
            return True

    return False

def verify_task_graph(task_graph: Dict[str, Any], prompt: str = "") -> Dict[str, Any]:
    issues: List[str] = []
    tasks = task_graph.get("tasks", [])
    if task_graph.get("needs_clarification"):
        return {"valid": True, "issues": []}
    task_ids: Set[str] = {t["id"] for t in tasks if "id" in t}

    for t in tasks:
        action = t["action"]
        params = t.get("params", {})
        depends_on = t.get("depends_on", [])
        side_effect = t.get("side_effect", False)

        for dep in depends_on:
            if dep not in task_ids:
                issues.append(f"Task '{t['id']}' depends on unknown task '{dep}'.")

        required = ACTION_REQUIRED_PARAMS.get(action, [])
        for key in required:
            if key not in params or _is_placeholder(params.get(key)):
                issues.append(f"Task '{t['id']}' action '{action}' missing param '{key}'.")

        if action in {"send_message", "send_email"} and _has_head_command_echo(t):
            issues.append(f"Task '{t['id']}' action '{action}' has command echo in message body.")

        if action == "evaluate_condition":
            metric = params.get("metric")
            if metric == "temperature" and (
                "threshold_unit" not in params or _is_placeholder(params.get("threshold_unit"))
            ):
                issues.append(f"Task '{t['id']}' action '{action}' missing param 'threshold_unit'.")

        if side_effect and action not in {"send_message", "send_email", "schedule_meeting"}:
            issues.append(f"Task '{t['id']}' side_effect true for unexpected action '{action}'.")

    # Conditional flow checks.
    condition_task_ids: Set[str] = {
        t["id"] for t in tasks if t.get("action") == "evaluate_condition" and "id" in t
    }
    condition_pattern = re.compile(r"^([A-Za-z0-9_]+):(true|false)$")
    for t in tasks:
        if t.get("action") not in {"send_message", "send_email"}:
            continue

        condition = t.get("condition")
        if condition_task_ids and not condition:
            issues.append(f"Task '{t['id']}' missing condition binding.")
            continue
        if not condition:
            continue

        m = condition_pattern.match(str(condition).strip())
        if not m:
            issues.append(
                f"Task '{t['id']}' has invalid condition token '{condition}'. Expected '<task_id>:true|false'."
            )
            continue

        cond_task_id = m.group(1)
        if cond_task_id not in condition_task_ids:
            issues.append(
                f"Task '{t['id']}' condition references unknown condition task '{cond_task_id}'."
            )
            continue

        deps = t.get("depends_on", [])
        if isinstance(deps, list) and cond_task_id not in deps:
            issues.append(
                f"Task '{t['id']}' condition task '{cond_task_id}' is not present in depends_on."
            )

    prompt_l = (prompt or "").lower()
    actions = {t["action"] for t in tasks if "action" in t}

    if "weather" in prompt_l and "fetch_weather" not in actions:
        issues.append("Prompt mentions weather but task graph has no 'fetch_weather' action.")

    if "weather" in prompt_l:
        weather_task_ids = {t["id"] for t in tasks if t.get("action") == "fetch_weather" and "id" in t}
        for t in tasks:
            if t.get("action") != "evaluate_condition":
                continue
            deps = t.get("depends_on", [])
            if not any(dep in weather_task_ids for dep in deps):
                issues.append(
                    f"Task '{t['id']}' evaluate_condition must depend on a fetch_weather task."
                )

    if ("text" in prompt_l or "message" in prompt_l) and "send_message" not in actions:
        issues.append("Prompt implies messaging but task graph has no 'send_message' action.")

    if "email" in prompt_l and "send_email" not in actions and "summarize_email" not in actions:
        issues.append("Prompt implies email action but task graph has no email-related action.")

    return {
        "valid": len(issues) == 0,
        "issues": issues,
    }
