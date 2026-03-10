from typing import Any, Dict, List


class CIRValidationError(ValueError):
    pass


ALLOWED_ACTIONS = {
    "fetch_weather",
    "evaluate_condition",
    "send_message",
    "send_email",
    "schedule_meeting",
    "summarize_email",
}


def _is_placeholder(v: Any) -> bool:
    return isinstance(v, str) and v.strip().lower() in {"from_context", "unknown", ""}


def validate_cir(cir: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(cir, dict):
        raise CIRValidationError("CIR must be an object.")
    tasks = cir.get("tasks", [])
    if not isinstance(tasks, list):
        raise CIRValidationError("CIR.tasks must be a list.")

    ids = set()
    for i, t in enumerate(tasks):
        if not isinstance(t, dict):
            raise CIRValidationError(f"tasks[{i}] must be an object.")
        for k in ("id", "action", "params", "depends_on", "side_effect"):
            if k not in t:
                raise CIRValidationError(f"tasks[{i}] missing '{k}'.")
        if t["action"] not in ALLOWED_ACTIONS:
            raise CIRValidationError(f"tasks[{i}] invalid action '{t['action']}'.")
        if t["id"] in ids:
            raise CIRValidationError(f"Duplicate task id '{t['id']}'.")
        ids.add(t["id"])

    return cir
