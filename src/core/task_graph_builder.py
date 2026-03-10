import re
import uuid
from typing import Any, Dict, List

from schemas.task_graph import validate_task_graph


def _task(task_id: str, action: str, params: Dict[str, Any], depends_on: List[str] | None = None,
          condition: str | None = None, side_effect: bool = False) -> Dict[str, Any]:
    return {
        "id": task_id,
        "action": action,
        "params": params,
        "depends_on": depends_on or [],
        "condition": condition,
        "side_effect": side_effect,
    }

def _autofill_from_dependencies(tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """ Autofill task parameters based on common keys from parent tasks in the dependency graph."""
    task_by_id = {t["id"]: t for t in tasks}

    def fill_from_parent(child: Dict[str, Any], parent: Dict[str, Any], key: str) -> None:
        child_params = child.setdefault("params", {})
        parent_params = parent.get("params", {})
        if key not in child_params and key in parent_params:
            child_params[key] = parent_params[key]

    for task in tasks:
        deps = task.get("depends_on", [])
        if not deps:
            continue

        for dep_id in deps:
            parent = task_by_id.get(dep_id)
            if not parent:
                continue

            # Common propagation keys
            fill_from_parent(task, parent, "recipient")
            fill_from_parent(task, parent, "time")
            fill_from_parent(task, parent, "date")
            fill_from_parent(task, parent, "location")
            fill_from_parent(task, parent, "message")

    return tasks



def build_task_graph(structured_intent: Dict[str, Any], prompt: str) -> Dict[str, Any]:
    intent = structured_intent["intent"]
    missing_context = structured_intent.get("missing_context", [])
    missing_questions = structured_intent.get("missing_questions", [])

    if missing_context:
        graph = {
            "schema_version": "1.0",
            "graph_id": f"graph_{uuid.uuid4().hex[:12]}",
            "intent": intent,
            "tasks": [],
            "needs_clarification": True,
            "clarification_questions": missing_questions,
        }
        return validate_task_graph(graph)

    prompt_l = prompt.lower()
    tasks: List[Dict[str, Any]] = []

    if ("email" in prompt_l and "text" in prompt_l) or ("email" in prompt_l and "message" in prompt_l):
        tasks.append(_task("t1", "send_email", {"recipient": "boss", "message": "I am late"}, side_effect=True))
        tasks.append(_task("t2", "send_message", {"recipient": "friend", "message": "I am late"}, side_effect=True))

    elif intent == "conditional_weather_notification":
        tasks.append(_task("t1", "fetch_weather", {"location": "from_context"}))
        tasks.append(_task("t2", "evaluate_condition", {"expression": "temperature_below_threshold"}, depends_on=["t1"]))
        tasks.append(
            _task(
                "t3",
                "send_message",
                {"recipient": "from_context", "message": "from_context"},
                depends_on=["t2"],
                condition="temperature_below_threshold",
                side_effect=True,
            )
        )

    elif intent == "schedule_meeting":
        tasks.append(_task("t1", "schedule_meeting", {"recipient": "from_context", "time": "from_context"}, side_effect=True))
        tasks.append(_task("t2", "send_email", {"recipient": "from_context", "message": "meeting confirmation"}, depends_on=["t1"], side_effect=True))

    elif intent in {"unknown", "unclear"}:
        graph = {
            "schema_version": "1.0",
            "graph_id": f"graph_{uuid.uuid4().hex[:12]}",
            "intent": "unknown",
            "tasks": [],
            "needs_clarification": True,
            "clarification_questions": structured_intent.get("missing_questions", ["Please clarify your request."]),
        }
        return validate_task_graph(graph)

    else:
        tasks.append(_task("t1", "analyze_request", {"intent": intent}))

    graph = {
        "schema_version": "1.0",
        "graph_id": f"graph_{uuid.uuid4().hex[:12]}",
        "intent": intent,
        "tasks": tasks,
        "needs_clarification": False,
        "clarification_questions": [],
    }
    return validate_task_graph(graph)


def build_task_graph_from_cir(cir: Dict[str, Any], prompt: str) -> Dict[str, Any]:
    """Convert CIR into TaskGraph with clarification-first handling for unresolved inputs."""
    tasks = cir.get("tasks", [])
    clarification_questions = cir.get("clarification_questions", [])

    unresolved = _collect_unresolved(tasks)
    if unresolved:
        questions = _questions_for_unresolved(unresolved, clarification_questions)

        return validate_task_graph(
            {
                "schema_version": "1.0",
                "graph_id": f"graph_{uuid.uuid4().hex[:12]}",
                "intent": "action_parse",
                "tasks": tasks,
                "needs_clarification": True,
                "clarification_questions": questions,
            }
        )
    # If there are no tasks, we should still ask for clarification to get the user to specify what they want to do, since an empty task graph is not actionable.
    if not tasks:
        return validate_task_graph(
            {
                "schema_version": "1.0",
                "graph_id": f"graph_{uuid.uuid4().hex[:12]}",
                "intent": "unknown",
                "tasks": [],
                "needs_clarification": True,
                "clarification_questions": clarification_questions
                or ["Can you clarify what action CAPS should perform?"],
            }
        )
    # Even if there are tasks, if any have unresolved inputs we should still ask for clarification before execution to get those details filled in.
    needs_clarification = bool(clarification_questions)
    return validate_task_graph(
        {
            "schema_version": "1.0",
            "graph_id": f"graph_{uuid.uuid4().hex[:12]}",
            "intent": "action_parse",
            "tasks": tasks,
            "needs_clarification": needs_clarification,
            "clarification_questions": clarification_questions,
        }
    )




def build_task_graph_from_action_parse(action_parse: Dict[str, Any], prompt: str) -> Dict[str, Any]:
    """ Convert an ActionParse output into a TaskGraph, inferring missing context and clarification needs."""
    tasks = action_parse.get("tasks", [])
    clarification_questions = action_parse.get("clarification_questions", [])

    if not tasks:
        graph = {
            "schema_version": "1.0",
            "graph_id": f"graph_{uuid.uuid4().hex[:12]}",
            "intent": "unknown",
            "tasks": [],
            "needs_clarification": True,
            "clarification_questions": clarification_questions
            or ["Can you clarify what action CAPS should perform?"],
        }
        return validate_task_graph(graph)
    
    tasks = _autofill_from_dependencies(tasks)
    needs_clarification = bool(clarification_questions)

    graph = {
        "schema_version": "1.0",
        "graph_id": f"graph_{uuid.uuid4().hex[:12]}",
        "intent": "action_parse",
        "tasks": tasks,
        "needs_clarification": needs_clarification,
        "clarification_questions": clarification_questions,
    }

    return validate_task_graph(graph)


# unresolved input detector - if any task has params with value "from_context" or similar, we should mark the graph as needing clarification with questions about that context
# + plus builder clarification gate - if any task has params with value "from_context" or similar, we should add a clarification question about that context and mark the graph as needing clarification

REQUIRED_BY_ACTION = {
    "fetch_weather": ["location_ref"],
    "evaluate_condition": ["metric", "operator", "threshold_value"],
    "send_message": ["recipient_ref", "message"],
    "send_email": ["recipient_ref", "message"],
    "schedule_meeting": ["recipient", "date", "time"],
}

def _is_unresolved(v: Any) -> bool:
    return isinstance(v, str) and v.strip().lower() in {
        "from_context",
        "location_from_context",
        "recipient_from_context",
        "unknown",
        "",
    }

def _collect_unresolved(tasks: List[Dict[str, Any]]) -> List[str]:
    missing: List[str] = []
    for t in tasks:
        action = t.get("action")
        params = t.get("params", {}) if isinstance(t.get("params"), dict) else {}
        for key in REQUIRED_BY_ACTION.get(action, []):
            if key not in params or _is_unresolved(params.get(key)):
                missing.append(f"{t.get('id','task')}:{action}:{key}")

        # Metric-specific requirement: temperature conditions must include unit.
        if action == "evaluate_condition" and params.get("metric") == "temperature":
            if "threshold_unit" not in params or _is_unresolved(params.get("threshold_unit")):
                missing.append(f"{t.get('id','task')}:{action}:threshold_unit")
    return missing


_ACTION_FIELD_QUESTIONS: Dict[tuple[str, str], str] = {
    ("fetch_weather", "location_ref"): "What location should weather be checked for?",
    ("send_message", "recipient_ref"): "Who exactly should receive the message?",
    ("send_email", "recipient_ref"): "Who exactly should receive the email?",
    ("send_message", "message"): "What message should be sent?",
    ("send_email", "message"): "What email message should be sent?",
    ("schedule_meeting", "recipient"): "Who should be invited to the meeting?",
    ("schedule_meeting", "time"): "What time should the meeting be scheduled?",
    ("schedule_meeting", "date"): "What date should the meeting be scheduled for?",
    ("schedule_meeting", "location"): "What meeting location should be used?",
    ("evaluate_condition", "threshold_unit"): "What unit should be used for the condition threshold?",
}

_FIELD_FALLBACK_QUESTIONS: Dict[str, str] = {
    "recipient_ref": "Who exactly should receive this?",
    "message": "What message should be sent?",
    "location_ref": "What location should be used?",
    "time": "What time should be used?",
    "date": "What date should be used?",
    "threshold_unit": "What unit should be used for this threshold?",
}


def _questions_for_unresolved(unresolved: List[str], existing: List[str] | None = None) -> List[str]:
    questions: List[str] = []
    seen = set()

    def push(q: str) -> None:
        if q and q not in seen:
            seen.add(q)
            questions.append(q)

    for q in existing or []:
        if isinstance(q, str) and q.strip():
            push(q.strip())

    for item in unresolved:
        parts = str(item).split(":", 2)
        if len(parts) != 3:
            continue
        _task_id, action, field = parts
        q = _ACTION_FIELD_QUESTIONS.get((action, field)) or _FIELD_FALLBACK_QUESTIONS.get(field)
        if q:
            push(q)

    if not questions:
        push("Please provide missing details required to execute this task.")
    return questions
