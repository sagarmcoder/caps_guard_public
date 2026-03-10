from typing import Any, Dict, List


class ExecutionPlanValidationError(ValueError):
    pass


def _expect_str(name: str, value: Any) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ExecutionPlanValidationError(f"Field '{name}' must be a non-empty string.")


def _expect_bool(name: str, value: Any) -> None:
    if not isinstance(value, bool):
        raise ExecutionPlanValidationError(f"Field '{name}' must be a boolean.")


def _expect_list(name: str, value: Any) -> None:
    if not isinstance(value, list):
        raise ExecutionPlanValidationError(f"Field '{name}' must be a list.")


def validate_execution_plan(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise ExecutionPlanValidationError("ExecutionPlan payload must be a JSON object.")

    required = ["schema_version", "plan_id", "intent", "requires_tools", "steps", "final_action"]
    for field in required:
        if field not in payload:
            raise ExecutionPlanValidationError(f"Missing required field '{field}'.")

    if payload["schema_version"] != "1.0":
        raise ExecutionPlanValidationError("Field 'schema_version' must be '1.0'.")

    _expect_str("plan_id", payload["plan_id"])
    _expect_str("intent", payload["intent"])
    _expect_bool("requires_tools", payload["requires_tools"])
    _expect_list("steps", payload["steps"])
    _expect_str("final_action", payload["final_action"])

    normalized_steps: List[Dict[str, Any]] = []
    for i, step in enumerate(payload["steps"]):
        if not isinstance(step, dict):
            raise ExecutionPlanValidationError(f"steps[{i}] must be an object.")

        for key in ["id", "type", "description"]:
            if key not in step:
                raise ExecutionPlanValidationError(f"steps[{i}] missing '{key}'.")
            _expect_str(f"steps[{i}].{key}", step[key])

        if "requires_tool" in step:
            _expect_bool(f"steps[{i}].requires_tool", step["requires_tool"])
        else:
            step["requires_tool"] = False

        if "tool_name" in step and step["tool_name"] is not None:
            if not isinstance(step["tool_name"], str):
                raise ExecutionPlanValidationError(f"steps[{i}].tool_name must be a string or null.")

        if "input_keys" in step:
            _expect_list(f"steps[{i}].input_keys", step["input_keys"])
            for j, item in enumerate(step["input_keys"]):
                _expect_str(f"steps[{i}].input_keys[{j}]", item)
        else:
            step["input_keys"] = []

        if "condition" in step and step["condition"] is not None:
            _expect_str(f"steps[{i}].condition", step["condition"])

        normalized_steps.append(
            {
                "id": step["id"].strip(),
                "type": step["type"].strip(),
                "description": step["description"].strip(),
                "requires_tool": step["requires_tool"],
                "tool_name": step.get("tool_name"),
                "input_keys": step["input_keys"],
                "condition": step.get("condition"),
            }
        )

    return {
        "schema_version": "1.0",
        "plan_id": payload["plan_id"].strip(),
        "intent": payload["intent"].strip(),
        "requires_tools": payload["requires_tools"],
        "steps": normalized_steps,
        "final_action": payload["final_action"].strip(),
        "notes": payload.get("notes", []),
    }
