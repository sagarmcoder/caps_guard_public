import uuid
from typing import Any, Dict, List

from schemas.execution_plan import validate_execution_plan
import re


def _mk_step(i: int, step_type: str, desc: str, requires_tool: bool, tool_name: str | None,
             input_keys: List[str], condition: str | None = None) -> Dict[str, Any]:
    return {
        "id": f"step_{i}",
        "type": step_type,
        "description": desc,
        "requires_tool": requires_tool,
        "tool_name": tool_name,
        "input_keys": input_keys,
        "condition": condition,
    }

def _condition_source_task_id(condition: str | None) -> str | None:
    """Extract source task ID from condition string if it follows the format 'task_id:true/false'. Returns None if format is invalid or condition is None."""
    if not isinstance(condition, str):
        return None
    m = re.match(r"^([A-Za-z0-9_]+):(true|false)$", condition.strip())
    if not m:
        return None
    return m.group(1)


def _toposort_tasks(tasks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Perform topological sort of tasks based on 'depends_on' and 'condition' references. Tasks with missing/invalid IDs or dependencies will be placed at the end in original order."""
    if not tasks:
        return tasks

    id_to_task: Dict[str, Dict[str, Any]] = {}
    original_index: Dict[str, int] = {}

    for i, task in enumerate(tasks):
        tid = task.get("id")
        if isinstance(tid, str) and tid and tid not in id_to_task:
            id_to_task[tid] = task
            original_index[tid] = i

    # Keep malformed/no-id tasks in original order at end.
    passthrough = [t for t in tasks if not isinstance(t.get("id"), str) or t.get("id") not in id_to_task]

    indegree: Dict[str, int] = {tid: 0 for tid in id_to_task}
    edges: Dict[str, set[str]] = {tid: set() for tid in id_to_task}

    def add_edge(src: str, dst: str) -> None:
        if src == dst:
            return
        if src not in id_to_task or dst not in id_to_task:
            return
        if dst in edges[src]:
            return
        edges[src].add(dst)
        indegree[dst] += 1

    for tid, task in id_to_task.items():
        deps = task.get("depends_on", [])
        if isinstance(deps, list):
            for dep in deps:
                if isinstance(dep, str):
                    add_edge(dep, tid)

        cond_src = _condition_source_task_id(task.get("condition"))
        if cond_src:
            add_edge(cond_src, tid)

    ready = [tid for tid, deg in indegree.items() if deg == 0]
    ready.sort(key=lambda x: original_index[x])

    ordered_ids: List[str] = []
    while ready:
        current = ready.pop(0)
        ordered_ids.append(current)

        for nxt in sorted(edges[current], key=lambda x: original_index[x]):
            indegree[nxt] -= 1
            if indegree[nxt] == 0:
                ready.append(nxt)
        ready.sort(key=lambda x: original_index[x])

    if len(ordered_ids) != len(id_to_task):
        cycle_nodes = [tid for tid, deg in indegree.items() if deg > 0]
        raise ValueError(f"Task graph contains dependency cycle: {', '.join(cycle_nodes)}")

    ordered_tasks = [id_to_task[tid] for tid in ordered_ids]
    ordered_tasks.extend(passthrough)
    return ordered_tasks


def compile_task_graph(task_graph: Dict[str, Any], verification: Dict[str, Any]) -> Dict[str, Any]:
    if task_graph.get("needs_clarification", False):
        plan = {
            "schema_version": "1.0",
            "plan_id": f"plan_{uuid.uuid4().hex[:12]}",
            "intent": task_graph.get("intent", "unknown"),
            "requires_tools": False,
            "steps": [
                _mk_step(1, "clarification_required", "Ask user for missing context.", False, None, ["clarification_questions"]),
                _mk_step(2, "build_response", "Build clarification response.", False, None, ["clarification_questions"]),
            ],
            "final_action": "request_clarification",
            "notes": ["Task graph requires clarification before execution."],
        }
        return validate_execution_plan(plan)

    if not verification["valid"]:
        plan = {
            "schema_version": "1.0",
            "plan_id": f"plan_{uuid.uuid4().hex[:12]}",
            "intent": task_graph.get("intent", "unknown"),
            "requires_tools": False,
            "steps": [
                _mk_step(1, "clarification_required", "Task graph failed verification; request clarification.", False, None, ["issues"]),
            ],
            "final_action": "request_clarification",
            "notes": verification["issues"],
        }
        return validate_execution_plan(plan)
#    At this point we have a verified task graph that we can attempt to compile into an execution plan.
    try:
        ordered_tasks = _toposort_tasks(task_graph.get("tasks", []))
    except ValueError as exc:
        plan = {
            "schema_version": "1.0",
            "plan_id": f"plan_{uuid.uuid4().hex[:12]}",
            "intent": task_graph.get("intent", "unknown"),
            "requires_tools": False,
            "steps": [
                _mk_step(
                    1,
                    "clarification_required",
                    "Task graph has invalid dependency cycle; request clarification.",
                    False,
                    None,
                    ["issues"],
                ),
            ],
            "final_action": "request_clarification",
            "notes": [str(exc)],
        }
        return validate_execution_plan(plan)

    steps: List[Dict[str, Any]] = []
    idx = 1

    for t in ordered_tasks:
        action = t["action"]
        side_effect = t.get("side_effect", False)

        if side_effect:
            steps.append(
                _mk_step(
                    idx,
                    "policy_check",
                    f"Run policy checks for action '{action}'.",
                    False,
                    None,
                    ["safety_checks"],
                    t.get("condition"),
                )
            )
            idx += 1

        if action == "fetch_weather":
            steps.append(
                _mk_step(
                    idx,
                    "resolve_location",
                    "Resolve location before fetching weather.",
                    True,
                    "location_api",
                    ["params"],
                    t.get("condition"),
                )
            )
            idx += 1
            steps.append(_mk_step(idx, "fetch_data", "Fetch weather data.", True, "weather_api", ["params"], None))
        elif action == "evaluate_condition":
            condition_ref = f"{t.get('id', 'condition')}:true"
            steps.append(_mk_step(idx, "evaluate_condition", "Evaluate branching condition.", False, None, ["params"], None))
            idx += 1
            steps.append(_mk_step(idx, "branch_if_true", "Branch if condition is true.", False, None, ["condition_result"], condition_ref))
        elif action == "send_message":
            steps.append(
                _mk_step(
                    idx,
                    "resolve_recipient",
                    "Resolve recipient identity before sending message.",
                    True,
                    "identity_api",
                    ["params"],
                    t.get("condition"),
                )
            )
            idx += 1
            steps.append(_mk_step(idx, "send_message", "Send message action.", True, "messaging_api", ["params"], t.get("condition")))
        elif action == "send_email":
            steps.append(
                _mk_step(
                    idx,
                    "resolve_recipient",
                    "Resolve recipient identity before sending email.",
                    True,
                    "identity_api",
                    ["params"],
                    t.get("condition"),
                )
            )
            idx += 1
            steps.append(_mk_step(idx, "send_email", "Send email action.", True, "email_api", ["params"], t.get("condition")))
        elif action == "schedule_meeting":
            steps.append(_mk_step(idx, "schedule_event", "Schedule meeting action.", True, "calendar_api", ["params"], t.get("condition")))
        else:
            steps.append(_mk_step(idx, "analyze_intent", f"Process generic task action '{action}'.", False, None, ["params"], t.get("condition")))

        idx += 1

    steps.append(_mk_step(idx, "build_response", "Build final deterministic response.", False, None, ["execution_results"], None))

    plan = {
        "schema_version": "1.0",
        "plan_id": f"plan_{uuid.uuid4().hex[:12]}",
        "intent": task_graph.get("intent", "unknown"),
        "requires_tools": any(s["requires_tool"] for s in steps),
        "steps": steps,
        "final_action": "return_response",
        "notes": ["Compiled from verified task graph."],
    }
    return validate_execution_plan(plan)
