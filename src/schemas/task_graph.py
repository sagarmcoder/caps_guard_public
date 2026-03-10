from typing import Any, Dict, List


class TaskGraphValidationError(ValueError):
    pass


def _expect_str(name: str, value: Any) -> None:
    if not isinstance(value, str) or not value.strip():
        raise TaskGraphValidationError(f"Field '{name}' must be a non-empty string.")


def _expect_bool(name: str, value: Any) -> None:
    if not isinstance(value, bool):
        raise TaskGraphValidationError(f"Field '{name}' must be a boolean.")


def _expect_list(name: str, value: Any) -> None:
    if not isinstance(value, list):
        raise TaskGraphValidationError(f"Field '{name}' must be a list.")


def validate_task_graph(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise TaskGraphValidationError("TaskGraph payload must be a JSON object.")

    required = ["schema_version", "graph_id", "intent", "tasks"]
    for field in required:
        if field not in payload:
            raise TaskGraphValidationError(f"Missing required field '{field}'.")

    if payload["schema_version"] != "1.0":
        raise TaskGraphValidationError("Field 'schema_version' must be '1.0'.")

    _expect_str("graph_id", payload["graph_id"])
    _expect_str("intent", payload["intent"])
    _expect_list("tasks", payload["tasks"])

    normalized_tasks: List[Dict[str, Any]] = []
    for i, task in enumerate(payload["tasks"]):
        if not isinstance(task, dict):
            raise TaskGraphValidationError(f"tasks[{i}] must be an object.")
        for key in ["id", "action"]:
            if key not in task:
                raise TaskGraphValidationError(f"tasks[{i}] missing '{key}'.")
            _expect_str(f"tasks[{i}].{key}", task[key])

        params = task.get("params", {})
        if not isinstance(params, dict):
            raise TaskGraphValidationError(f"tasks[{i}].params must be an object.")

        depends_on = task.get("depends_on", [])
        _expect_list(f"tasks[{i}].depends_on", depends_on)
        for j, dep in enumerate(depends_on):
            _expect_str(f"tasks[{i}].depends_on[{j}]", dep)

        condition = task.get("condition")
        if condition is not None:
            _expect_str(f"tasks[{i}].condition", condition)

        side_effect = task.get("side_effect", False)
        _expect_bool(f"tasks[{i}].side_effect", side_effect)

        normalized_task = {
            "id": task["id"].strip(),
            "action": task["action"].strip(),
            "params": params,
            "depends_on": depends_on,
            "condition": condition,
            "side_effect": side_effect,
        }

        verb_anchor = task.get("verb_anchor")
        recipient_anchor = task.get("recipient_anchor")
        if isinstance(verb_anchor, str) and verb_anchor.strip():
            normalized_task["verb_anchor"] = verb_anchor.strip()
        if isinstance(recipient_anchor, str) and recipient_anchor.strip():
            normalized_task["recipient_anchor"] = recipient_anchor.strip()

        normalized_tasks.append(normalized_task)

    return {
        "schema_version": "1.0",
        "graph_id": payload["graph_id"].strip(),
        "intent": payload["intent"].strip(),
        "tasks": normalized_tasks,
        "needs_clarification": bool(payload.get("needs_clarification", False)),
        "clarification_questions": payload.get("clarification_questions", []),
    }
