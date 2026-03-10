from typing import Any, Dict, List


class ActionParseValidationError(ValueError):
    pass


ALLOWED_ACTIONS = {
    "fetch_weather",
    "evaluate_condition",
    "send_message",
    "send_email",
    "schedule_meeting",
    "summarize_email",
}



def _expect_str(name: str, value: Any) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ActionParseValidationError(f"Field '{name}' must be a non-empty string.")


def _expect_list(name: str, value: Any) -> None:
    if not isinstance(value, list):
        raise ActionParseValidationError(f"Field '{name}' must be a list.")


def _expect_bool(name: str, value: Any) -> None:
    if not isinstance(value, bool):
        raise ActionParseValidationError(f"Field '{name}' must be a boolean.")
    
def _allow_action_for_prompt(action: str, prompt_text: str) -> bool:
    p = prompt_text.lower()

    # summarize_email only valid when prompt clearly references email inbox/summary behavior
    if action == "summarize_email":
        return "email" in p and ("summarize" in p or "unread" in p or "inbox" in p)

    return True



def validate_action_parse(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise ActionParseValidationError("ActionParse payload must be a JSON object.")

    required = ["schema_version", "parse_id", "tasks"]
    for field in required:
        if field not in payload:
            raise ActionParseValidationError(f"Missing required field '{field}'.")

    if payload["schema_version"] != "1.0":
        raise ActionParseValidationError("Field 'schema_version' must be '1.0'.")

    _expect_str("parse_id", payload["parse_id"])
    _expect_list("tasks", payload["tasks"])

    normalized_tasks: List[Dict[str, Any]] = []
    prompt_text = str(payload.get("source_prompt", ""))

    for i, task in enumerate(payload["tasks"]):
        if not isinstance(task, dict):
            raise ActionParseValidationError(f"tasks[{i}] must be an object.")

        for k in ["id", "action"]:
            if k not in task:
                raise ActionParseValidationError(f"tasks[{i}] missing '{k}'.")
            _expect_str(f"tasks[{i}].{k}", task[k])

        action = task["action"].strip()
        if not _allow_action_for_prompt(action, prompt_text):
            raise ActionParseValidationError(
                f"tasks[{i}].action '{action}' is not valid for this prompt context."
            )

        if action not in ALLOWED_ACTIONS:
            raise ActionParseValidationError(
                f"tasks[{i}].action '{action}' is not in allowed actions."
            )

        params = task.get("params", {})
        if not isinstance(params, dict):
            raise ActionParseValidationError(f"tasks[{i}].params must be an object.")

        depends_on = task.get("depends_on", [])
        _expect_list(f"tasks[{i}].depends_on", depends_on)
        for j, dep in enumerate(depends_on):
            _expect_str(f"tasks[{i}].depends_on[{j}]", dep)

        condition = task.get("condition")
        if condition is not None:
            _expect_str(f"tasks[{i}].condition", condition)

        side_effect = task.get("side_effect", False)
        _expect_bool(f"tasks[{i}].side_effect", side_effect)

        verb_anchor = task.get("verb_anchor")
        if verb_anchor is not None:
            _expect_str(f"tasks[{i}].verb_anchor", verb_anchor)

        recipient_anchor = task.get("recipient_anchor")
        if recipient_anchor is not None:
            _expect_str(f"tasks[{i}].recipient_anchor", recipient_anchor)

        normalized_task = {
            "id": task["id"].strip(),
            "action": action,
            "params": params,
            "depends_on": depends_on,
            "condition": condition,
            "side_effect": side_effect,
        }
        if isinstance(verb_anchor, str) and verb_anchor.strip():
            normalized_task["verb_anchor"] = verb_anchor.strip()
        if isinstance(recipient_anchor, str) and recipient_anchor.strip():
            normalized_task["recipient_anchor"] = recipient_anchor.strip()

        normalized_tasks.append(normalized_task)

    clarification_questions = payload.get("clarification_questions", [])
    _expect_list("clarification_questions", clarification_questions)
    for i, q in enumerate(clarification_questions):
        _expect_str(f"clarification_questions[{i}]", q)

    return {
        "schema_version": "1.0",
        "parse_id": payload["parse_id"].strip(),
        "tasks": normalized_tasks,
        "clarification_questions": clarification_questions,
        "notes": payload.get("notes", []),
        "source_prompt": prompt_text,
    }
