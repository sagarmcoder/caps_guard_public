from dataclasses import dataclass
import json
import uuid
from typing import Any, Dict

from core.entity_extract import extract_entities, looks_ambiguous, looks_informational
from llm.ollama_client import OllamaClient
from schemas.structured_intent import SchemaValidationError, validate_structured_intent
from core.task_graph_builder import (
    build_task_graph,
    build_task_graph_from_action_parse,
    build_task_graph_from_cir,
)
from core.task_graph_verifier import verify_task_graph
from core.task_graph_compiler import compile_task_graph

from schemas.action_parse import ActionParseValidationError, validate_action_parse
from schemas.cir import CIRValidationError, validate_cir

from core.execution_runtime import execute_plan


@dataclass
class MCPRequest:
    user_id: str
    prompt: str
    temperature: float = 0.2


class MCPService:
    def __init__(self, llm_client: OllamaClient, strict_mode: bool = True, clarify_llm_polish: bool = False):
        self.llm_client = llm_client
        self.strict_mode = strict_mode
        self.clarify_llm_polish = clarify_llm_polish

    def _acquire_context(self, request: MCPRequest) -> Dict[str, str]:
        # Phase-1 stub: this will later load memory, policies, and task history.
        return {"context_mode": "stub", "user_id": request.user_id}

    def process(self, request: MCPRequest) -> Dict[str, str]:
        context = self._acquire_context(request)
        composed_prompt = (
            f"[User ID: {request.user_id}]\n"
            f"[Context Mode: {context['context_mode']}]\n\n"
            f"{request.prompt}"
        )
        output = self.llm_client.generate(composed_prompt, temperature=request.temperature)
        return {
            "output": output,
            "model": self.llm_client.model,
            "context_mode": context["context_mode"],
        }
    def _derive_location_ref(self, prompt: str) -> str | None:
        import re
        p = prompt.lower()

        # common "in <place>" pattern
        m = re.search(r"\bin\s+([A-Za-z][A-Za-z0-9_\-\s]{1,40})", prompt)
        if m:
            val = m.group(1).strip().rstrip(".")
            # trim trailing helper words if present
            val = re.sub(r"\s+(today|tomorrow|tonight)$", "", val, flags=re.IGNORECASE).strip()
            if val:
                return val

        # fallback markers
        if "weather" in p:
            return "location_from_context"
        return None

    def _extract_explicit_location_ref(self, prompt: str) -> str | None:
        import re
        m = re.search(r"\bin\s+([A-Za-z][A-Za-z0-9_\-\s]{1,40})", prompt)
        if not m:
            return None
        val = m.group(1).strip().rstrip(".")
        val = re.sub(r"\s+(today|tomorrow|tonight)$", "", val, flags=re.IGNORECASE).strip()
        return val or None


    def _annotate_location_resolution(self, parsed: Dict[str, Any], prompt: str) -> Dict[str, Any]:
        explicit_loc = self._extract_explicit_location_ref(prompt)
        for t in parsed.get("tasks", []):
            if t.get("action") != "fetch_weather":
                continue

            params = t.setdefault("params", {})
            # Canonicalize legacy key if model emitted `location`.
            if "location_ref" not in params and isinstance(params.get("location"), str):
                params["location_ref"] = params["location"]
            if "location_ref" not in params:
                loc = self._derive_location_ref(prompt)
                if loc:
                    params["location_ref"] = loc
            if explicit_loc and str(params.get("location_ref", "")).strip().lower() in {"from_context", "location_from_context", "unknown", ""}:
                params["location_ref"] = explicit_loc

            if "location_resolved" not in params:
                params["location_resolved"] = False

        return parsed

    def _extract_condition_spec(self, prompt: str) -> Dict[str, Any] | None:
        import re

        p = prompt.lower()

        # Temperature threshold conditions: below -20C, under 10 F, < 3
        weather = re.search(
            r"(?:below|under|<)\s*(-?\d+(?:\.\d+)?)\s*([cf]|celsius|fahrenheit)?",
            p,
        )
        if weather:
            value = float(weather.group(1))
            unit_raw = (weather.group(2) or "c").lower()
            unit = "C" if unit_raw in {"c", "celsius"} else "F"
            return {
                "metric": "temperature",
                "operator": "<",
                "threshold_value": value,
                "threshold_unit": unit,
                "expression": "temperature_below_threshold",
            }

        # Time conditions: if time is 4:20 pm / 4.20 pm
        time_m = re.search(
            r"(?:if\s+)?(?:the\s+)?time\s*(?:is|=|==)\s*(\d{1,2})[:.](\d{2})\s*(am|pm)?",
            p,
        )
        if time_m:
            hour = int(time_m.group(1))
            minute = int(time_m.group(2))
            meridiem = (time_m.group(3) or "").lower()
            if meridiem == "pm" and hour < 12:
                hour += 12
            if meridiem == "am" and hour == 12:
                hour = 0
            time_24 = f"{hour:02d}:{minute:02d}"
            return {
                "metric": "time_of_day",
                "operator": "==",
                "threshold_value": time_24,
                "threshold_unit": None,
                "expression": "time_equals_threshold",
            }

        return None

    def _to_cir(self, action_parse: Dict[str, Any], prompt: str) -> Dict[str, Any]:
        condition_spec = self._extract_condition_spec(prompt)
        tasks = action_parse.get("tasks", [])
        fetch_ids = [t.get("id") for t in tasks if t.get("action") == "fetch_weather" and t.get("id")]

        # enrich evaluate_condition
        for t in tasks:
            if t.get("action") == "fetch_weather":
                p = t.setdefault("params", {})
                if "location_ref" not in p and isinstance(p.get("location"), str):
                    p["location_ref"] = p["location"]
            if t.get("action") == "evaluate_condition":
                p = t.setdefault("params", {})
                if condition_spec:
                    p.setdefault("expression", condition_spec.get("expression"))
                    p.setdefault("metric", condition_spec.get("metric"))
                    p.setdefault("operator", condition_spec.get("operator"))
                    p.setdefault("threshold_value", condition_spec.get("threshold_value"))
                    if condition_spec.get("threshold_unit") is not None:
                        p.setdefault("threshold_unit", condition_spec.get("threshold_unit"))
                else:
                    p.setdefault("metric", "temperature")
                    p.setdefault("operator", "<")
                deps = t.get("depends_on", [])
                if not isinstance(deps, list):
                    deps = []
                if fetch_ids:
                    fetch_id = fetch_ids[0]
                    if fetch_id not in deps:
                        deps = deps + [fetch_id]
                t["depends_on"] = deps

        # bind run_if for downstream side effects after evaluate_condition
        eval_ids = [t["id"] for t in tasks if t.get("action") == "evaluate_condition"]
        if eval_ids:
            cond_ref = f"{eval_ids[0]}:true"
            for t in tasks:
                if t.get("action") in {"send_message", "send_email"} and eval_ids[0] in t.get("depends_on", []):
                    # Canonical condition token for downstream compiler/verifier.
                    t["condition"] = cond_ref

        return {
            "schema_version": "1.0",
            "cir_id": action_parse.get("parse_id", "cir_auto"),
            "tasks": tasks,
            "clarification_questions": action_parse.get("clarification_questions", []),
            "source_prompt": prompt,
        }
    def _is_contact_endpoint(self, value: Any) -> bool:
        """Determine if a value resembles a contact endpoint like an email or phone number."""
        if not isinstance(value, str):
            return False
        v = value.strip()
        if not v:
            return False
        # Email or phone-like token
        import re
        return bool(
            re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", v)
            or re.match(r"^\+?[0-9][0-9\-\s]{6,}$", v)
        )

    def _derive_logical_recipient(self, prompt: str) -> str | None:
        import re

        p = prompt.lower()

        # Group-style references first (avoid capturing "my" as recipient).
        match = re.search(r"\b(?:my|the)\s+(\w+(?:\s+\w+){0,2})\s+group\b", p)
        if match:
            group_name = match.group(1).replace(" ", "_")
            return f"{group_name}_group"

        # Prefer named recipient after direct action verbs.
        m = re.search(r"\b(?:text|message|email|notify|call)\s+([A-Za-z][A-Za-z0-9_-]*)", prompt)
        if m:
            candidate = m.group(1).strip()
            if candidate.lower() not in {"my", "the", "a", "an", "to", "for"}:
                return candidate

        # Meeting-style references.
        m_with = re.search(r"\bwith\s+([A-Za-z][A-Za-z0-9_-]*)", prompt)
        if m_with:
            candidate = m_with.group(1).strip()
            if candidate.lower() not in {"my", "the", "a", "an"}:
                return candidate

        # Common role references.
        if "boss" in p:
            return "boss"
        if "friend" in p:
            return "friend"
        if "besties" in p or "group" in p:
            return "besties_group"
        return None

    def _derive_message_payload(self, prompt: str) -> str:
        import re

        # If user provided a quoted or "that ..." message, keep it.
        m = re.search(r"\bthat\b\s+(.+)$", prompt, re.IGNORECASE)
        if m and m.group(1).strip():
            return m.group(1).strip().rstrip(".")

        # Fall back to trailing clause after comma.
        if "," in prompt:
            tail = prompt.split(",", 1)[1].strip()
            if tail:
                return tail.rstrip(".")

        return "from_context"

    def _extract_quoted_message(self, prompt: str) -> str | None:
        import re

        m = re.search(r'"([^"]+)"', prompt)
        if m and m.group(1).strip():
            return m.group(1).strip()
        return None

    def _extract_that_clause_message(self, prompt: str) -> str | None:
        import re

        m = re.search(r"\bthat\b\s+(.+)$", prompt, re.IGNORECASE)
        if not m or not m.group(1).strip():
            return None
        return m.group(1).strip().rstrip(".")

    def _action_verbs_for_task(self, action: str) -> list[str]:
        if action == "send_email":
            return ["send email", "email"]
        if action == "send_message":
            return ["text", "message", "notify", "send"]
        return []

    def _find_anchor_span(self, prompt: str, anchor: str) -> tuple[int, int] | None:
        import re

        if not isinstance(anchor, str) or not anchor.strip():
            return None
        pattern = re.compile(re.escape(anchor.strip()), re.IGNORECASE)
        match = pattern.search(prompt)
        if match:
            return match.start(), match.end()
        return None

    def _derive_task_anchors(self, task: Dict[str, Any], prompt: str) -> Dict[str, Any]:
        """Populate verb/recipient anchors deterministically when the parser omits them."""
        action = task.get("action")
        if action not in {"send_message", "send_email"}:
            return task

        prompt_l = prompt.lower()
        if not task.get("verb_anchor"):
            for verb in self._action_verbs_for_task(action):
                if verb in prompt_l:
                    task["verb_anchor"] = verb
                    break

        if task.get("recipient_anchor"):
            return task

        params = task.get("params", {}) if isinstance(task.get("params"), dict) else {}
        recipient_candidates = [
            params.get("recipient_anchor"),
            params.get("recipient"),
            params.get("recipient_ref"),
        ]
        variants: list[str] = []
        for candidate in recipient_candidates:
            if not isinstance(candidate, str) or self._is_unresolved_value(candidate):
                continue
            variants.append(candidate)
            variants.append(candidate.replace("_", " "))
            if candidate.endswith("_group"):
                variants.append(candidate[:-6].replace("_", " ") + " group")

        for variant in variants:
            span = self._find_anchor_span(prompt, variant)
            if span:
                task["recipient_anchor"] = prompt[span[0]:span[1]]
                return task

        import re

        if action == "send_message":
            patterns = [
                r"\b(?:text|message|notify)\s+((?:my|the)\s+[A-Za-z][A-Za-z0-9_-]*(?:\s+[A-Za-z][A-Za-z0-9_-]*){0,2}\s+group)\b",
                r"\b(?:text|message|notify)\s+((?:my|the)\s+[A-Za-z][A-Za-z0-9_-]+)\b",
                r"\b(?:text|message|notify)\s+([A-Za-z][A-Za-z0-9_-]+)\b",
            ]
        else:
            patterns = [
                r"\b(?:send\s+email|email)\s+to\s+((?:my|the)\s+[A-Za-z][A-Za-z0-9_-]+)\b",
                r"\b(?:send\s+email|email)\s+to\s+([A-Za-z][A-Za-z0-9_.@-]+)\b",
                r"\b(?:send\s+email|email)\s+([A-Za-z][A-Za-z0-9_.@-]+)\b",
            ]

        for pattern in patterns:
            m = re.search(pattern, prompt, re.IGNORECASE)
            if m and m.group(1).strip():
                task["recipient_anchor"] = m.group(1).strip()
                return task
        return task

    def _slice_message_from_prompt(self, task: Dict[str, Any], prompt: str) -> str | None:
        quoted = self._extract_quoted_message(prompt)
        if quoted:
            return quoted

        that_clause = self._extract_that_clause_message(prompt)
        if that_clause:
            return that_clause

        task = self._derive_task_anchors(task, prompt)
        verb_anchor = task.get("verb_anchor")
        recipient_anchor = task.get("recipient_anchor")
        if not isinstance(verb_anchor, str) or not isinstance(recipient_anchor, str):
            return None

        verb_span = self._find_anchor_span(prompt, verb_anchor)
        recipient_span = self._find_anchor_span(prompt, recipient_anchor)
        if not verb_span or not recipient_span:
            return None
        if recipient_span[0] < verb_span[0]:
            return None

        body = prompt[recipient_span[1]:].strip(" ,:;-")
        if body.lower().startswith("that "):
            body = body[5:].strip()
        body = body.rstrip(".").strip()
        return body or None

    def _has_explicit_message_content(self, prompt: str) -> bool:
        import re
        if re.search(r"\"[^\"]+\"", prompt):
            return True
        if re.search(r"\bthat\b\s+.+$", prompt, re.IGNORECASE):
            return True
        m = re.search(
            r"\b(?:text|message|email|notify)\s+[A-Za-z][A-Za-z0-9_\-]*(?:\s+group)?\s+(.+)$",
            prompt,
            re.IGNORECASE,
        )
        return bool(m and m.group(1).strip())

    def _condition_source_task_id(self, condition: Any) -> str | None:
        import re

        if not isinstance(condition, str):
            return None
        m = re.match(r"^([A-Za-z0-9_]+):(true|false)$", condition.strip())
        if not m:
            return None
        return m.group(1)

    def _task_priority(self, action: str) -> int:
        if action == "fetch_weather":
            return 0
        if action == "evaluate_condition":
            return 1
        if action == "schedule_meeting":
            return 2
        if action in {"send_email", "send_message"}:
            return 3
        return 4

    def _order_tasks_canonically(self, tasks: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
        if not tasks:
            return tasks

        id_to_task = {str(t.get("id")): t for t in tasks if isinstance(t.get("id"), str) and t.get("id")}
        original_index = {str(t.get("id")): idx for idx, t in enumerate(tasks) if isinstance(t.get("id"), str) and t.get("id")}
        indegree = {tid: 0 for tid in id_to_task}
        edges: Dict[str, set[str]] = {tid: set() for tid in id_to_task}

        def add_edge(src: str, dst: str) -> None:
            if src == dst or src not in id_to_task or dst not in id_to_task:
                return
            if dst in edges[src]:
                return
            edges[src].add(dst)
            indegree[dst] += 1

        for tid, task in id_to_task.items():
            for dep in task.get("depends_on", []) if isinstance(task.get("depends_on"), list) else []:
                if isinstance(dep, str):
                    add_edge(dep, tid)
            cond_src = self._condition_source_task_id(task.get("condition"))
            if cond_src:
                add_edge(cond_src, tid)

        ready = [tid for tid, deg in indegree.items() if deg == 0]

        def ready_key(task_id: str) -> tuple[int, int]:
            return (
                self._task_priority(str(id_to_task[task_id].get("action", ""))),
                original_index.get(task_id, 10**6),
            )

        ready.sort(key=ready_key)
        ordered: list[Dict[str, Any]] = []

        while ready:
            current = ready.pop(0)
            ordered.append(id_to_task[current])
            for nxt in edges[current]:
                indegree[nxt] -= 1
                if indegree[nxt] == 0:
                    ready.append(nxt)
            ready.sort(key=ready_key)

        if len(ordered) != len(tasks):
            return tasks
        return ordered

    def _reindex_tasks(self, tasks: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
        if not tasks:
            return tasks

        id_map: Dict[str, str] = {}
        for idx, task in enumerate(tasks, start=1):
            old_id = str(task.get("id"))
            id_map[old_id] = f"t{idx}"

        reindexed: list[Dict[str, Any]] = []
        for idx, task in enumerate(tasks, start=1):
            cloned = dict(task)
            cloned["id"] = f"t{idx}"

            deps = cloned.get("depends_on", [])
            if isinstance(deps, list):
                cloned["depends_on"] = [id_map.get(dep, dep) for dep in deps if isinstance(dep, str)]

            cond = cloned.get("condition")
            cond_src = self._condition_source_task_id(cond)
            if cond_src and isinstance(cond, str):
                suffix = cond.split(":", 1)[1]
                cloned["condition"] = f"{id_map.get(cond_src, cond_src)}:{suffix}"

            params = cloned.get("params", {}) if isinstance(cloned.get("params"), dict) else {}
            if isinstance(params.get("source_task_id"), str):
                params["source_task_id"] = id_map.get(params["source_task_id"], params["source_task_id"])
            cloned["params"] = params
            reindexed.append(cloned)

        return reindexed

    def _canonicalize_parse_artifacts(self, parsed: Dict[str, Any], prompt: str) -> Dict[str, Any]:
        tasks = parsed.get("tasks", [])
        if not isinstance(tasks, list):
            return parsed

        tasks = [self._normalize_task_params(t) for t in tasks]
        tasks = [self._derive_task_anchors(t, prompt) for t in tasks]
        tasks = [self._normalize_task_anchors(t, prompt) for t in tasks]
        tasks = self._enforce_schedule_email_dependency(tasks)
        tasks = self._order_tasks_canonically(tasks)
        tasks = self._reindex_tasks(tasks)
        parsed["tasks"] = tasks
        return parsed

    def _recover_conditional_tasks(self, parsed: Dict[str, Any], prompt: str) -> Dict[str, Any]:
        condition_spec = self._extract_condition_spec(prompt)
        if not condition_spec:
            return parsed

        p = prompt.lower()
        wants_message = any(x in p for x in ["text", "message", "notify"])
        wants_email = "email" in p
        if not (wants_message or wants_email):
            return parsed

        # If parser already produced a conditional chain, keep it.
        existing_tasks = parsed.get("tasks", [])
        if existing_tasks:
            has_eval = any(t.get("action") == "evaluate_condition" for t in existing_tasks)
            if has_eval:
                return parsed

            has_send = any(t.get("action") in {"send_message", "send_email"} for t in existing_tasks)
            if not has_send:
                return parsed

            # Upgrade partial parse: add missing evaluate condition (and weather fetch if required),
            # then bind all send actions to canonical condition token.
            task_ids = {str(t.get("id")) for t in existing_tasks if t.get("id")}
            next_idx = 1
            while f"t{next_idx}" in task_ids:
                next_idx += 1

            fetch_id = None
            needs_fetch = condition_spec.get("metric") == "temperature" or "weather" in p
            if needs_fetch:
                fetch_task = next((t for t in existing_tasks if t.get("action") == "fetch_weather"), None)
                if fetch_task:
                    fetch_id = fetch_task.get("id")
                else:
                    fetch_id = f"t{next_idx}"
                    next_idx += 1
                    existing_tasks.insert(
                        0,
                        {
                            "id": fetch_id,
                            "action": "fetch_weather",
                            "params": {"location_ref": "location_from_context", "location_resolved": False},
                            "depends_on": [],
                            "condition": None,
                            "side_effect": False,
                        },
                    )

            cond_id = f"t{next_idx}"
            cond_params = {
                "expression": condition_spec.get("expression"),
                "metric": condition_spec.get("metric"),
                "operator": condition_spec.get("operator"),
                "threshold_value": condition_spec.get("threshold_value"),
            }
            if condition_spec.get("threshold_unit") is not None:
                cond_params["threshold_unit"] = condition_spec.get("threshold_unit")
            existing_tasks.append(
                {
                    "id": cond_id,
                    "action": "evaluate_condition",
                    "params": cond_params,
                    "depends_on": [fetch_id] if fetch_id else [],
                    "condition": None,
                    "side_effect": False,
                }
            )

            cond_ref = f"{cond_id}:true"
            for t in existing_tasks:
                if t.get("action") in {"send_message", "send_email"}:
                    deps = t.get("depends_on", [])
                    if not isinstance(deps, list):
                        deps = []
                    if cond_id not in deps:
                        deps.append(cond_id)
                    t["depends_on"] = deps
                    t["condition"] = cond_ref

            parsed["tasks"] = existing_tasks
            parsed.setdefault("notes", []).append("conditional_recovery_applied")
            return parsed

        tasks: list[Dict[str, Any]] = []
        next_id = 1
        fetch_id = None

        # Weather-like conditions need weather fetch before condition eval.
        if condition_spec.get("metric") == "temperature" or "weather" in p:
            fetch_id = f"t{next_id}"
            tasks.append(
                {
                    "id": fetch_id,
                    "action": "fetch_weather",
                    "params": {"location_ref": "location_from_context", "location_resolved": False},
                    "depends_on": [],
                    "condition": None,
                    "side_effect": False,
                }
            )
            next_id += 1

        cond_id = f"t{next_id}"
        condition_params = {
            "expression": condition_spec.get("expression"),
            "metric": condition_spec.get("metric"),
            "operator": condition_spec.get("operator"),
            "threshold_value": condition_spec.get("threshold_value"),
        }
        if condition_spec.get("threshold_unit") is not None:
            condition_params["threshold_unit"] = condition_spec.get("threshold_unit")
        tasks.append(
            {
                "id": cond_id,
                "action": "evaluate_condition",
                "params": condition_params,
                "depends_on": [fetch_id] if fetch_id else [],
                "condition": None,
                "side_effect": False,
            }
        )
        next_id += 1

        recipient_ref = self._derive_logical_recipient(prompt) or "recipient_from_context"
        message = self._derive_message_payload(prompt)
        condition_ref = f"{cond_id}:true"

        if wants_message:
            tasks.append(
                {
                    "id": f"t{next_id}",
                    "action": "send_message",
                    "params": {
                        "recipient_ref": recipient_ref,
                        "recipient_resolved": False,
                        "message": message,
                    },
                    "depends_on": [cond_id],
                    "condition": condition_ref,
                    "side_effect": True,
                }
            )
            next_id += 1

        if wants_email:
            tasks.append(
                {
                    "id": f"t{next_id}",
                    "action": "send_email",
                    "params": {
                        "recipient_ref": recipient_ref,
                        "recipient_resolved": False,
                        "message": message,
                    },
                    "depends_on": [cond_id],
                    "condition": condition_ref,
                    "side_effect": True,
                }
            )

        parsed["tasks"] = tasks
        parsed["clarification_questions"] = []
        parsed["notes"] = [
            n
            for n in parsed.get("notes", [])
            if n not in {"ambiguous_prompt", "domain_mismatch_detected", "domain_mismatch_fallback", "ambiguous_prompt_fallback"}
        ]
        parsed.setdefault("notes", []).append("conditional_recovery_applied")
        return parsed

    def _recover_direct_send_tasks(self, parsed: Dict[str, Any], prompt: str) -> Dict[str, Any]:
        """Recover direct send intents when parser returns no tasks."""
        if parsed.get("tasks"):
            return parsed

        p = prompt.lower()
        wants_message = any(x in p for x in ["text", "message", "notify"])
        wants_email = "email" in p
        if not (wants_message or wants_email):
            return parsed

        recipient_ref = self._derive_logical_recipient(prompt) or "from_context"
        explicit_message = self._has_explicit_message_content(prompt)
        message = self._derive_message_payload(prompt) if explicit_message else "from_context"

        tasks: list[Dict[str, Any]] = []
        next_id = 1
        if wants_email:
            tasks.append(
                {
                    "id": f"t{next_id}",
                    "action": "send_email",
                    "params": {
                        "recipient_ref": recipient_ref,
                        "recipient_resolved": False,
                        "message": message,
                    },
                    "depends_on": [],
                    "condition": None,
                    "side_effect": True,
                }
            )
            next_id += 1
        if wants_message:
            tasks.append(
                {
                    "id": f"t{next_id}",
                    "action": "send_message",
                    "params": {
                        "recipient_ref": recipient_ref,
                        "recipient_resolved": False,
                        "message": message,
                    },
                    "depends_on": [],
                    "condition": None,
                    "side_effect": True,
                }
            )

        parsed["tasks"] = tasks
        parsed["clarification_questions"] = []
        parsed["notes"] = [
            n
            for n in parsed.get("notes", [])
            if n not in {"parser_unreliable_fallback", "domain_mismatch_detected", "domain_mismatch_fallback"}
        ]
        return parsed

    def _is_unresolved_value(self, value: Any) -> bool:
        if not isinstance(value, str):
            return False
        return value.strip().lower() in {"from_context", "location_from_context", "unknown", ""}

    def _canonicalize_placeholder(self, value: Any) -> Any:
        if not isinstance(value, str):
            return value
        lowered = value.strip().lower()
        if lowered in {"from_context", "location_from_context", "recipient_from_context", "unknown", ""}:
            return "from_context"
        return value.strip()

    def _extract_schedule_signals(self, prompt: str) -> Dict[str, Any]:
        import re

        p = prompt.lower()
        with_match = re.search(r"\bwith\s+([A-Za-z][A-Za-z0-9_-]*)", prompt)
        recipient = with_match.group(1).strip() if with_match else None
        has_time = bool(re.search(r"\b\d{1,2}(?::\d{2})?\s*(?:am|pm)\b", p))
        has_date = bool(re.search(r"\b(today|tomorrow|tonight|next\s+\w+)\b", p))

        return {
            "recipient": recipient,
            "has_recipient": bool(recipient),
            "has_time": has_time,
            "has_date": has_date,
        }

    def _normalize_schedule_task_params(self, parsed: Dict[str, Any], prompt: str) -> Dict[str, Any]:
        signals = self._extract_schedule_signals(prompt)
        for task in parsed.get("tasks", []):
            if task.get("action") != "schedule_meeting":
                continue
            params = task.setdefault("params", {})

            if signals["has_recipient"]:
                if "recipient" not in params or self._is_unresolved_value(params.get("recipient")):
                    params["recipient"] = signals["recipient"]
            else:
                params["recipient"] = "from_context"

            if not signals["has_time"]:
                params["time"] = "from_context"
            if not signals["has_date"]:
                params["date"] = "from_context"

        return parsed

    def _recover_schedule_tasks(self, parsed: Dict[str, Any], prompt: str) -> Dict[str, Any]:
        """Recover common schedule/email workflows when the parser drops one of the tasks."""
        prompt_l = prompt.lower()
        wants_schedule = "schedule" in prompt_l or "meeting" in prompt_l
        wants_email = "email" in prompt_l
        if not wants_schedule:
            return parsed

        tasks = parsed.get("tasks", [])
        if not isinstance(tasks, list):
            tasks = []

        schedule_task = next((t for t in tasks if t.get("action") == "schedule_meeting"), None)
        email_task = next((t for t in tasks if t.get("action") == "send_email"), None)
        if schedule_task and (email_task or not wants_email):
            return parsed

        signals = self._extract_schedule_signals(prompt)
        used_ids = {str(t.get("id")) for t in tasks if t.get("id")}
        next_idx = 1
        while f"t{next_idx}" in used_ids:
            next_idx += 1

        if schedule_task is None:
            schedule_params: Dict[str, Any] = {
                "recipient": signals["recipient"] if signals["has_recipient"] else "from_context",
                "date": "from_context",
                "time": "from_context",
            }

            for existing in tasks:
                params = existing.get("params", {}) if isinstance(existing.get("params"), dict) else {}
                recipient = params.get("recipient")
                if isinstance(recipient, str) and not self._is_unresolved_value(recipient):
                    schedule_params["recipient"] = recipient
                    break
                recipient_ref = params.get("recipient_ref")
                if isinstance(recipient_ref, str) and not self._is_unresolved_value(recipient_ref):
                    schedule_params["recipient"] = recipient_ref
                    break

            import re

            date_match = re.search(r"\b(today|tomorrow|tonight|next\s+\w+)\b", prompt, re.IGNORECASE)
            if date_match:
                schedule_params["date"] = date_match.group(1)

            time_match = re.search(r"\b(\d{1,2}(?::\d{2})?\s*(?:am|pm))\b", prompt, re.IGNORECASE)
            if time_match:
                schedule_params["time"] = time_match.group(1)

            schedule_task = {
                "id": f"t{next_idx}",
                "action": "schedule_meeting",
                "params": schedule_params,
                "depends_on": [],
                "condition": None,
                "side_effect": True,
            }
            next_idx += 1
            tasks.insert(0, schedule_task)

        if wants_email and email_task is None:
            schedule_params = schedule_task.get("params", {}) if isinstance(schedule_task.get("params"), dict) else {}
            recipient = schedule_params.get("recipient", "from_context")
            email_params: Dict[str, Any] = {
                "recipient": recipient,
                "recipient_ref": recipient if isinstance(recipient, str) else "from_context",
                "recipient_resolved": False,
                "message": "from_context",
            }
            email_task = {
                "id": f"t{next_idx}",
                "action": "send_email",
                "params": email_params,
                "depends_on": [schedule_task["id"]],
                "condition": None,
                "side_effect": True,
            }
            tasks.append(email_task)

        parsed["tasks"] = tasks
        parsed["clarification_questions"] = [
            q
            for q in parsed.get("clarification_questions", [])
            if "clarify what task" not in q.lower()
        ]
        parsed.setdefault("notes", []).append("schedule_recovery_applied")
        return parsed

    def _derive_schedule_confirmation_message(self, tasks: list[Dict[str, Any]]) -> str | None:
        schedule_task = next((t for t in tasks if t.get("action") == "schedule_meeting"), None)
        if not schedule_task:
            return None
        params = schedule_task.get("params", {}) if isinstance(schedule_task.get("params"), dict) else {}
        date = params.get("date")
        time_value = params.get("time")
        recipient = params.get("recipient")
        if not isinstance(date, str) or self._is_unresolved_value(date):
            return None
        if not isinstance(time_value, str) or self._is_unresolved_value(time_value):
            return None
        if isinstance(recipient, str) and not self._is_unresolved_value(recipient):
            return f"Meeting confirmation for {date.strip()} at {time_value.strip()} with {recipient.strip()}."
        return f"Meeting confirmation for {date.strip()} at {time_value.strip()}."

    def _annotate_recipient_resolution(self, parsed: Dict[str, Any], prompt: str) -> Dict[str, Any]:
        """Annotate send_message/send_email tasks with recipient resolution status based on prompt context."""
        prompt_l = prompt.lower()

        for t in parsed.get("tasks", []):
            if t.get("action") not in {"send_email", "send_message"}:
                continue

            params = t.setdefault("params", {})
            recipient = params.get("recipient")

            # Always preserve a logical recipient reference
            if recipient and "recipient_ref" not in params:
                params["recipient_ref"] = recipient
            if "recipient_ref" not in params:
                logical = self._derive_logical_recipient(prompt)
                if logical:
                    params["recipient_ref"] = logical

            # Default unresolved unless explicitly confirmed
            if "recipient_resolved" not in params:
                params["recipient_resolved"] = False

            # Only mark resolved if recipient is concrete endpoint and actually appears in prompt
            if recipient and self._is_contact_endpoint(recipient):
                params["recipient_resolved"] = recipient.lower() in prompt_l
                # Avoid treating hallucinated endpoints as canonical identity.
                if not params["recipient_resolved"]:
                    logical = self._derive_logical_recipient(prompt)
                    if logical:
                        params["recipient_ref"] = logical

            # Do not pre-resolve identities in parser path; runtime resolver is source of truth.
            params["recipient_resolved"] = False
            params.pop("recipient_id", None)

        return parsed

    
    def _normalize_task_params(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize task parameters with action-aware logic and canonical key mapping."""
        params = task.get("params", {})
        if not isinstance(params, dict):
            params = {}

        action = task.get("action", "")

        # Canonical alias mapping
        alias_map = {
            "to": "recipient",
            "name": "recipient",
            "person": "recipient",
            "body": "message",
            "text": "message",
            "temp": "temperature",
        }

        normalized: Dict[str, Any] = {}
        for k, v in params.items():
            target = alias_map.get(k, k)
            normalized[target] = v

        # Action-level canonicalization for weather inputs.
        if action == "fetch_weather" and "location_ref" not in normalized and "location" in normalized:
            normalized["location_ref"] = normalized["location"]
        if action == "fetch_weather" and "location_ref" in normalized and "location" in normalized:
            normalized.pop("location", None)

        # Action-specific allowed keys
        allowed_by_action = {
            "fetch_weather": {"location_ref", "location_resolved", "location_id", "units"},
            "evaluate_condition": {
                "expression",
                "threshold",
                "metric",
                "operator",
                "threshold_value",
                "threshold_unit",
                "source_task_id",
            },
            "send_message": {"recipient", "recipient_ref", "recipient_resolved", "message"},
            "send_email": {"recipient", "recipient_ref", "recipient_resolved", "message", "subject"},
            "schedule_meeting": {"recipient", "time", "date", "location"},
            "summarize_email": {"mailbox", "filter", "limit"},
        }


        allowed = allowed_by_action.get(action, set())
        if allowed:
            normalized = {k: v for k, v in normalized.items() if k in allowed}

        # Action-aware fill-ins
        if action == "send_email":
            if "message" not in normalized and "subject" in normalized:
                normalized["message"] = normalized["subject"]

        # Normalize trivial empty strings to missing
        normalized = {
            k: v for k, v in normalized.items()
            if not (isinstance(v, str) and not v.strip())
        }

        # Canonicalize placeholders and trim string values.
        for key, value in list(normalized.items()):
            normalized[key] = self._canonicalize_placeholder(value)

        if action in {"send_message", "send_email"}:
            recipient = normalized.get("recipient")
            recipient_ref = normalized.get("recipient_ref")
            if isinstance(recipient, str) and not self._is_unresolved_value(recipient):
                if not isinstance(recipient_ref, str) or self._is_unresolved_value(recipient_ref):
                    normalized["recipient_ref"] = recipient.strip()
            if "recipient" in normalized:
                normalized.pop("recipient", None)

            if "recipient_ref" not in normalized:
                normalized["recipient_ref"] = "from_context"

            message = normalized.get("message")
            if isinstance(message, str):
                normalized["message"] = " ".join(message.split()).strip()

        if action == "fetch_weather":
            if "location_ref" not in normalized:
                normalized["location_ref"] = "from_context"
            else:
                normalized["location_ref"] = self._canonicalize_placeholder(normalized["location_ref"])

        if action == "schedule_meeting":
            for key in {"recipient", "date", "time", "location"}:
                if key in normalized:
                    normalized[key] = self._canonicalize_placeholder(normalized[key])

        task["params"] = normalized
        return task

    def _normalize_task_anchors(self, task: Dict[str, Any], prompt: str) -> Dict[str, Any]:
        for key in ("verb_anchor", "recipient_anchor"):
            value = task.get(key)
            if not isinstance(value, str) or not value.strip():
                task.pop(key, None)
                continue
            span = self._find_anchor_span(prompt, value)
            if span:
                task[key] = prompt[span[0]:span[1]].strip()
            else:
                task[key] = value.strip()
        return task



    def _reconcile_structured_intent(
        self, structured: Dict[str, Any], prompt: str, repaired: bool
    ) -> Dict[str, Any]:
        entities = extract_entities(prompt)
        structured.setdefault("provided_context", [])
        structured.setdefault("missing_context", [])
        structured.setdefault("constraints", [])
        structured.setdefault("safety_checks", [])
        structured.setdefault("missing_questions", [])

        # Ambiguous/junk prompt fallback.
        if looks_ambiguous(prompt):
            structured["intent"] = "unknown"
            structured["requires_tools"] = False
            structured["required_context"] = []
            structured["provided_context"] = []
            structured["missing_context"] = []
            structured["constraints"] = []
            structured["safety_checks"] = []
            structured["missing_questions"] = [
                "Can you clarify what task you want CAPS to perform?"
            ]
            structured["fallback_reason"] = "ambiguous_prompt"
            structured["confidence"] = 0.2
            return structured

        # Informational guard (no tool/action execution needed).
        if looks_informational(prompt) and not entities["has_messaging"]:
            structured["requires_tools"] = False
            structured["required_context"] = []
            structured["missing_context"] = []
            structured["missing_questions"] = []

        contradictions_removed = False

        # If prompt already has time/date, remove time-related missing asks.
        if entities["has_time"]:
            before_q = len(structured["missing_questions"])
            before_c = len(structured["missing_context"])
            structured["missing_questions"] = [
                q
                for q in structured.get("missing_questions", [])
                if "time" not in q.lower() and "when" not in q.lower() and "date" not in q.lower()
            ]
            structured["missing_context"] = [
                c
                for c in structured.get("missing_context", [])
                if "time" not in c.lower() and "date" not in c.lower()
            ]
            if len(structured["missing_questions"]) < before_q or len(structured["missing_context"]) < before_c:
                contradictions_removed = True

        # Messaging intent should require tools.
        if entities["has_messaging"]:
            structured["requires_tools"] = True

        # If recipient detected, register as provided context.
        if entities["recipient_name"]:
            pc = structured.setdefault("provided_context", [])
            if "recipient_name" not in pc:
                pc.append("recipient_name")

        # Add extracted entities into provided context for consistency.
        if entities["temperature_threshold"] and "temperature_threshold" not in structured["provided_context"]:
            structured["provided_context"].append("temperature_threshold")
        if entities["has_time"] and "time_reference" not in structured["provided_context"]:
            structured["provided_context"].append("time_reference")

        # Confidence score (v1 heuristic).
        confidence = 0.6
        if not repaired:
            confidence += 0.2
        if not contradictions_removed:
            confidence += 0.1

        aligns = True
        if entities["has_messaging"] and not structured.get("requires_tools", False):
            aligns = False
        if entities["has_time"]:
            if any("time" in x.lower() or "date" in x.lower() for x in structured.get("missing_context", [])):
                aligns = False
            if any("time" in x.lower() or "date" in x.lower() for x in structured.get("missing_questions", [])):
                aligns = False
        if aligns:
            confidence += 0.1

        confidence = max(0.0, min(0.9, confidence))
        structured["confidence"] = round(confidence, 2)
        if structured["confidence"] < 0.5:
            structured["fallback_reason"] = "low_confidence"
            if not structured["missing_questions"]:
                structured["missing_questions"] = [
                    "Can you clarify the task details before execution?"
                ]

        return structured
    def _extract_json_object(self, text: str) -> Dict[str, Any]:
        # Try strict parse first.
        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass

        # Parse first JSON object from mixed output.
        start = text.find("{")
        if start == -1:
            raise SchemaValidationError("Model output did not contain a JSON object.")
        decoder = json.JSONDecoder()
        try:
            parsed, _end = decoder.raw_decode(text[start:])
        except json.JSONDecodeError as exc:
            raise SchemaValidationError("Model output JSON could not be parsed.") from exc
        if not isinstance(parsed, dict):
            raise SchemaValidationError("Parsed JSON must be an object.")
        return parsed

    def _sanitize_action_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        payload.setdefault("schema_version", "1.0")
        payload.setdefault("parse_id", "auto-parse-id")
        payload.setdefault("tasks", [])
        payload.setdefault("clarification_questions", [])
        payload.setdefault("notes", [])

        tasks = payload.get("tasks", [])
        if isinstance(tasks, list):
            sanitized_tasks = []
            for i, task in enumerate(tasks):
                if not isinstance(task, dict):
                    continue
                action = task.get("action")
                # Drop malformed tasks early to avoid validator crashes.
                if not isinstance(action, str) or not action.strip():
                    continue
                task.setdefault("id", f"task_{i + 1}")
                task.setdefault("depends_on", [])
                task.setdefault("condition", None)
                task["side_effect"] = action in {
                    "send_message",
                    "send_email",
                    "schedule_meeting",
                }
                task.setdefault("params", {})
                if not isinstance(task.get("verb_anchor"), str):
                    task.pop("verb_anchor", None)
                if not isinstance(task.get("recipient_anchor"), str):
                    task.pop("recipient_anchor", None)
                sanitized_tasks.append(task)
            payload["tasks"] = sanitized_tasks
        return payload

    def _enforce_schedule_email_dependency(self, tasks: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
        schedule_ids = [
            t.get("id")
            for t in tasks
            if t.get("action") == "schedule_meeting" and t.get("id")
        ]
        if not schedule_ids:
            return tasks

        primary_schedule_id = schedule_ids[0]
        for task in tasks:
            if task.get("action") != "send_email":
                continue
            deps = task.get("depends_on", [])
            if not isinstance(deps, list):
                deps = []
            if primary_schedule_id not in deps:
                deps.append(primary_schedule_id)
            task["depends_on"] = deps

        # Deterministic order for common schedule->email flows.
        def _sort_key(task: Dict[str, Any]) -> int:
            action = task.get("action")
            if action == "schedule_meeting":
                return 0
            if action == "send_email":
                return 1
            return 2

        return sorted(tasks, key=_sort_key)
    
    def _reconcile_action_parse(self, parsed: Dict[str, Any], prompt: str) -> Dict[str, Any]:
        """ Reconcile action parse with prompt entities and intent to fix any contradictions or add missing tasks."""
        entities = extract_entities(prompt)

        parsed.setdefault("tasks", [])
        parsed.setdefault("clarification_questions", [])
        parsed.setdefault("notes", [])
        parsed["parse_id"] = f"parse_{uuid.uuid4().hex[:12]}"

        # Ambiguous prompt hard fallback
        if looks_ambiguous(prompt):
            parsed["tasks"] = []
            parsed["clarification_questions"] = [
                "Can you clarify what task you want CAPS to perform?"
            ]
            parsed["notes"].append("ambiguous_prompt_fallback")
            return parsed

        # Recover direct send intents if parser produced no tasks.
        parsed = self._recover_direct_send_tasks(parsed, prompt)
        parsed = self._recover_schedule_tasks(parsed, prompt)

        # Normalize side_effect for all actions.
        for t in parsed["tasks"]:
            action = t.get("action")
            t["side_effect"] = action in {"send_message", "send_email", "schedule_meeting"}

        parsed["tasks"] = [self._normalize_task_params(t) for t in parsed["tasks"]]
        parsed = self._normalize_schedule_task_params(parsed, prompt)
        parsed = self._annotate_recipient_resolution(parsed, prompt)
        parsed = self._annotate_location_resolution(parsed, prompt)
        parsed["tasks"] = [self._derive_task_anchors(t, prompt) for t in parsed["tasks"]]
        parsed["tasks"] = [self._normalize_task_anchors(t, prompt) for t in parsed["tasks"]]
        parsed["tasks"] = [self._normalize_task_params(t) for t in parsed["tasks"]]
        parsed["tasks"] = self._enforce_schedule_email_dependency(parsed["tasks"])

        prompt_l = prompt.lower()
        actions = {t.get("action") for t in parsed["tasks"]}

        weather_terms = ("weather" in prompt_l) or ("temperature" in prompt_l) or ("below -" in prompt_l)
        wants_message = ("text" in prompt_l) or ("message" in prompt_l)
        wants_email = "email" in prompt_l

        domain_mismatch = False
        expected_core_actions = set()
        if weather_terms:
            expected_core_actions.add("fetch_weather")
        if wants_message:
            expected_core_actions.add("send_message")
        if wants_email:
            expected_core_actions.add("send_email")

        if weather_terms and "fetch_weather" not in actions:
            domain_mismatch = True
        if wants_message and "send_message" not in actions:
            domain_mismatch = True
        if wants_email and "send_email" not in actions and "summarize_email" not in actions:
            domain_mismatch = True

        # reject unrelated actions in weather flow unless prompt also mentions email
        if weather_terms and "summarize_email" in actions and not wants_email:
            domain_mismatch = True

        if domain_mismatch:
        # Hard-fail only when none of the expected core actions are present.
            if not expected_core_actions.intersection(actions):
                recoverable_conditional = bool(self._extract_condition_spec(prompt)) and (
                    wants_message or wants_email
                )
                if recoverable_conditional:
                    parsed["notes"].append("domain_mismatch_recoverable_conditional")
                else:
                    parsed["notes"].append("domain_mismatch_detected")
                    parsed["tasks"] = []
                    parsed["clarification_questions"] = [
                        "I could not reliably map your request to valid actions. Can you restate it with clear task details?"
                    ]
                    parsed["notes"].append("domain_mismatch_fallback")
                    return parsed



        # If time exists in prompt, remove time clarification
        if entities["has_time"]:
            parsed["clarification_questions"] = [
                q for q in parsed["clarification_questions"]
                if "time" not in q.lower() and "when" not in q.lower() and "date" not in q.lower()
            ]
        # If fetch_weather already has concrete location_ref, remove stale location clarification.
        has_concrete_location = False
        for task in parsed.get("tasks", []):
            if task.get("action") != "fetch_weather":
                continue
            params = task.get("params", {}) if isinstance(task.get("params"), dict) else {}
            loc_ref = params.get("location_ref")
            if isinstance(loc_ref, str) and loc_ref.strip().lower() not in {"from_context", "location_from_context", "unknown", ""}:
                has_concrete_location = True
                break
        if has_concrete_location:
            parsed["clarification_questions"] = [
                q for q in parsed.get("clarification_questions", [])
                if "location" not in q.lower() and "weather be checked" not in q.lower()
            ]

        parsed = self._recover_conditional_tasks(parsed, prompt)
        if parsed.get("tasks"):
            parsed["clarification_questions"] = [
                q
                for q in parsed.get("clarification_questions", [])
                if "clarify what task" not in q.lower()
            ]
        parsed = self._annotate_recipient_resolution(parsed, prompt)
        parsed = self._annotate_location_resolution(parsed, prompt)
        parsed = self._normalize_conditions(parsed)
        parsed = self._normalize_message_defaults(parsed, prompt)
        parsed = self._canonicalize_parse_artifacts(parsed, prompt)
        parsed = self._normalize_conditions(parsed)
        parsed = self._clean_parse_notes(parsed)

        return parsed

    def _normalize_conditions(self, parsed: Dict[str, Any]) -> Dict[str, Any]:
        """Ensure side-effect tasks bind to canonical `<evaluate_task_id>:true` tokens."""
        tasks = parsed.get("tasks", [])
        task_ids = {t.get("id") for t in tasks if t.get("id")}
        first_id_by_action: Dict[str, str] = {}
        for t in tasks:
            action = t.get("action")
            tid = t.get("id")
            if isinstance(action, str) and isinstance(tid, str) and action not in first_id_by_action:
                first_id_by_action[action] = tid

        # Canonicalize dependency aliases to actual task ids and enforce weather->eval edge.
        for t in tasks:
            deps = t.get("depends_on", [])
            if not isinstance(deps, list):
                deps = []
            normalized_deps = []
            for dep in deps:
                if not isinstance(dep, str):
                    continue
                if dep in task_ids:
                    normalized_deps.append(dep)
                elif dep in first_id_by_action:
                    normalized_deps.append(first_id_by_action[dep])

            if t.get("action") == "evaluate_condition":
                params = t.get("params", {}) if isinstance(t.get("params"), dict) else {}
                if params.get("metric") == "temperature":
                    fw = first_id_by_action.get("fetch_weather")
                    if fw and fw not in normalized_deps:
                        normalized_deps.append(fw)

            dedup = []
            seen = set()
            for dep in normalized_deps:
                if dep not in seen:
                    dedup.append(dep)
                    seen.add(dep)
            t["depends_on"] = dedup

        eval_ids = [t.get("id") for t in tasks if t.get("action") == "evaluate_condition" and t.get("id")]
        if not eval_ids:
            return parsed
        valid_refs = {f"{eid}:true" for eid in eval_ids} | {f"{eid}:false" for eid in eval_ids}
        for task in tasks:
            if task.get("action") not in {"send_message", "send_email"}:
                continue
            deps = task.get("depends_on", [])
            dep_eval = next((eid for eid in eval_ids if eid in deps), None)
            if dep_eval is None:
                dep_eval = eval_ids[0]
                deps.append(dep_eval)
                task["depends_on"] = deps
            cond = task.get("condition")
            expected_ref = f"{dep_eval}:true"
            if not isinstance(cond, str) or cond not in valid_refs:
                task["condition"] = expected_ref
        return parsed

    def _normalize_message_defaults(self, parsed: Dict[str, Any], prompt: str) -> Dict[str, Any]:
        schedule_confirmation = self._derive_schedule_confirmation_message(parsed.get("tasks", []))
        for task in parsed.get("tasks", []):
            if task.get("action") not in {"send_message", "send_email"}:
                continue
            params = task.setdefault("params", {})
            sliced_message = self._slice_message_from_prompt(task, prompt)
            if sliced_message:
                params["message"] = sliced_message
            elif task.get("action") == "send_email" and schedule_confirmation:
                params["message"] = schedule_confirmation
            else:
                params["message"] = "from_context"
        return parsed

    def _clean_parse_notes(self, parsed: Dict[str, Any]) -> Dict[str, Any]:
        """Keep notes high-signal for users while preserving fallback diagnostics."""
        keep_always = {"ambiguous_prompt_fallback", "domain_mismatch_fallback", "parser_unreliable_fallback"}
        tasks = parsed.get("tasks", [])
        cleaned = []
        for note in parsed.get("notes", []):
            if note in keep_always:
                cleaned.append(note)
                continue
        parsed["notes"] = cleaned
        return parsed

    def _polish_clarification_questions(self, questions: list[str], prompt: str) -> list[str]:
        """Optional LLM helper to improve wording only; does not decide missing fields."""
        if not questions:
            return questions
        prompt_text = (
            "Rewrite these clarification questions to be concise and user-friendly.\n"
            "Do not change meaning, do not add or remove required fields.\n"
            "Return ONLY JSON: {\"questions\": [\"...\"]}\n\n"
            f"User request:\n{prompt}\n\n"
            f"Questions:\n{json.dumps(questions)}"
        )
        try:
            raw = self.llm_client.generate(prompt_text, temperature=0.0)
            payload = self._extract_json_object(raw)
            candidate = payload.get("questions")
            if not isinstance(candidate, list):
                return questions
            cleaned: list[str] = []
            for q in candidate:
                if isinstance(q, str) and q.strip():
                    cleaned.append(q.strip())
            if not cleaned:
                return questions
            # Keep cardinality aligned to avoid semantic drift.
            if len(cleaned) != len(questions):
                return questions
            # Enforce semantic intent parity question-by-question.
            source_tags = [self._clarification_tag(q) for q in questions]
            polished_tags = [self._clarification_tag(q) for q in cleaned]
            if source_tags != polished_tags:
                return questions
            return cleaned
        except Exception:  # noqa: BLE001
            return questions

    def _clarification_tag(self, question: str) -> str:
        import re

        q = (question or "").strip().lower()
        if not q:
            return "generic"

        if re.search(r"\b(message|text|sms|email message|email body|content)\b", q):
            return "message"
        if re.search(r"\b(recipient|who should|who exactly|invite)\b", q):
            return "recipient"
        if re.search(r"\b(location|where|city|place)\b", q):
            return "location"
        if re.search(r"\b(date|day)\b", q):
            return "date"
        if re.search(r"\b(time|when)\b", q):
            return "time"
        if re.search(r"\b(unit|celsius|fahrenheit|threshold)\b", q):
            return "threshold_unit"
        if re.search(r"\b(clarify|missing details|required details|restate)\b", q):
            return "generic"
        return "generic"


    def _action_parser_system_prompt(self) -> str:
        return """
You are an action parser for CAPS.
Return ONLY valid JSON in this exact schema:
{
  "schema_version": "1.0",
  "parse_id": string,
  "tasks": [
    {
      "id": string,
      "action": "fetch_weather" | "evaluate_condition" | "send_message" | "send_email" | "schedule_meeting" | "summarize_email",
      "params": object,
      "depends_on": string[],
      "condition": string | null,
      "side_effect": boolean,
      "verb_anchor"?: string,
      "recipient_anchor"?: string
    }
  ],
  "clarification_questions": string[],
  "notes": string[]
}

Rules:
- Output only JSON (no markdown).
- Prefer multiple tasks for multi-action prompts.
- Use side_effect=true for send/schedule actions.
- For send actions, include `verb_anchor` and `recipient_anchor` when they are explicit in the user prompt.
- If request is unclear, return empty tasks and a clarification question.
- Do not invent unrelated domains.
- Do NOT copy example parse_id/task ids/values directly into output.
- Examples are reference only. Generate output only from current user request.
- If uncertain, return empty tasks with clarification_questions.

Examples:

Request: "If weather is below -20C, text Jacob I am not coming to university today."
Output:
{
  "schema_version": "1.0",
  "parse_id": "example_weather_1",
  "tasks": [
    {
      "id": "t1",
      "action": "fetch_weather",
      "params": {"location": "from_context"},
      "depends_on": [],
      "condition": null,
      "side_effect": false
    },
    {
      "id": "t2",
      "action": "evaluate_condition",
      "params": {"expression": "temperature_below_threshold"},
      "depends_on": ["t1"],
      "condition": null,
      "side_effect": false
    },
    {
      "id": "t3",
      "action": "send_message",
      "params": {"recipient": "Jacob", "message": "I am not coming to university today."},
      "depends_on": ["t2"],
      "condition": "temperature_below_threshold",
      "side_effect": true,
      "verb_anchor": "text",
      "recipient_anchor": "Jacob"
    }
  ],
  "clarification_questions": ["What location should weather be checked for?"],
  "notes": []
}

Request: "Schedule a meeting with Aneesh tomorrow at 3 PM and send a confirmation email."
Output:
{
  "schema_version": "1.0",
  "parse_id": "example_schedule_1",
  "tasks": [
    {
      "id": "t1",
      "action": "schedule_meeting",
      "params": {"recipient": "Aneesh", "date": "tomorrow", "time": "3 PM"},
      "depends_on": [],
      "condition": null,
      "side_effect": true
    },
    {
      "id": "t2",
      "action": "send_email",
      "params": {"recipient": "Aneesh", "message": "Meeting confirmation for tomorrow at 3 PM."},
      "depends_on": ["t1"],
      "condition": null,
      "side_effect": true,
      "verb_anchor": "email",
      "recipient_anchor": "Aneesh"
    }
  ],
  "clarification_questions": [],
  "notes": []
}

Request: "Send email to my boss and text my friend that I am late."
Output:
{
  "schema_version": "1.0",
  "parse_id": "example_multi_1",
  "tasks": [
    {
      "id": "t1",
      "action": "send_email",
      "params": {"recipient": "boss", "message": "I am late."},
      "depends_on": [],
      "condition": null,
      "side_effect": true,
      "verb_anchor": "email",
      "recipient_anchor": "boss"
    },
    {
      "id": "t2",
      "action": "send_message",
      "params": {"recipient": "friend", "message": "I am late."},
      "depends_on": [],
      "condition": null,
      "side_effect": true,
      "verb_anchor": "text",
      "recipient_anchor": "friend"
    }
  ],
  "clarification_questions": [],
  "notes": []
}

Request: "..."
Output:
{
  "schema_version": "1.0",
  "parse_id": "example_ambiguous_1",
  "tasks": [],
  "clarification_questions": ["Can you clarify what task you want CAPS to perform?"],
  "notes": ["ambiguous_prompt"]
}
""".strip()

    def _parse_action_request(self, request: MCPRequest, context: Dict[str, Any]) -> Dict[str, Any]:
        parser_prompt = self._action_parser_system_prompt()
        composed_prompt = (
            f"[System]\\n{parser_prompt}\\n\\n"
            f"[User ID: {request.user_id}]\\n"
            f"[Context Mode: {context['context_mode']}]\\n\\n"
            f"[User Request]\\n{request.prompt}"
        )

        raw_output = self.llm_client.generate(composed_prompt, temperature=0.0)

        try:
            parsed_payload = self._extract_json_object(raw_output)
            parsed_payload["source_prompt"] = request.prompt
            parsed_payload = self._sanitize_action_payload(parsed_payload)
            parsed = validate_action_parse(parsed_payload)
        except (SchemaValidationError, ActionParseValidationError):
            repair_prompt = (
                "Return ONLY corrected JSON matching action-parse schema version 1.0.\\n\\n"
                f"Original request:\\n{request.prompt}\\n\\n"
                f"Previous output:\\n{raw_output}"
            )
            repaired_output = self.llm_client.generate(repair_prompt, temperature=0.0)
            try:
                parsed_payload = self._extract_json_object(repaired_output)
                parsed_payload["source_prompt"] = request.prompt
                parsed_payload = self._sanitize_action_payload(parsed_payload)
                parsed = validate_action_parse(parsed_payload)
            except (SchemaValidationError, ActionParseValidationError):
                parsed = {
                    "schema_version": "1.0",
                    "parse_id": "fallback-parse-id",
                    "tasks": [],
                    "clarification_questions": [
                        "I could not parse this request reliably. Can you restate the task clearly?"
                    ],
                    "notes": ["parser_unreliable_fallback"],
                    "source_prompt": request.prompt,
                }

        return parsed

    def _build_cir_with_fallback(self, parsed: Dict[str, Any], prompt: str) -> Dict[str, Any]:
        try:
            cir = validate_cir(self._to_cir(parsed, prompt))
        except CIRValidationError:
            parsed["tasks"] = []
            if not parsed.get("clarification_questions"):
                parsed["clarification_questions"] = [
                    "I could not build a valid execution graph. Can you clarify the task details?"
                ]
            cir = validate_cir(self._to_cir(parsed, prompt))
        return cir

    def _build_action_parse_result(
        self,
        context: Dict[str, Any],
        parsed: Dict[str, Any],
        task_graph: Dict[str, Any],
        graph_verification: Dict[str, Any],
        execution_plan: Dict[str, Any],
        runtime_execution: Dict[str, Any] | None,
    ) -> Dict[str, Any]:
        return {
            "action_parse": parsed,
            "task_graph": task_graph,
            "task_graph_verification": graph_verification,
            "execution_plan": execution_plan,
            "runtime_execution": runtime_execution,
            "model": self.llm_client.model,
            "context_mode": context["context_mode"],
            "strict_mode": self.strict_mode,
            "clarify_llm_polish": self.clarify_llm_polish,
        }

    def process_action_parse(self, request: MCPRequest, execute_live: bool = False) -> Dict[str, Any]:
        """Process user request into action-first schema and compile it into plan artifacts."""
        context = self._acquire_context(request)
        parsed = self._parse_action_request(request, context)

        parsed = self._reconcile_action_parse(parsed, request.prompt)

        if not parsed.get("tasks") and not parsed.get("clarification_questions"):
            parsed["clarification_questions"] = [
                "Can you clarify what exact action CAPS should perform?"
            ]

        cir = self._build_cir_with_fallback(parsed, request.prompt)

        task_graph = build_task_graph_from_cir(cir, request.prompt)
        if self.clarify_llm_polish and task_graph.get("needs_clarification"):
            task_graph["clarification_questions"] = self._polish_clarification_questions(
                task_graph.get("clarification_questions", []),
                request.prompt,
            )
        graph_verification = verify_task_graph(task_graph, request.prompt)

        execution_plan = compile_task_graph(task_graph, graph_verification)


        runtime_execution = None
        if execute_live and execution_plan.get("final_action") == "return_response":
            runtime_execution = execute_plan(
                execution_plan=execution_plan,
                task_graph=task_graph,
                request_context={
                    "user_id": request.user_id,
                    "context_mode": context["context_mode"],
                },
            )
        return self._build_action_parse_result(
            context=context,
            parsed=parsed,
            task_graph=task_graph,
            graph_verification=graph_verification,
            execution_plan=execution_plan,
            runtime_execution=runtime_execution,
        )

    def process_action_parse_langgraph(
        self,
        request: MCPRequest,
        execute_live: bool = False,
        thread_id: str | None = None,
        sqlite_path: str | None = None,
        manifest_path: str | None = None,
    ) -> Dict[str, Any]:
        from core.langgraph_flow import run_action_parse_graph

        return run_action_parse_graph(
            self,
            request,
            execute_live=execute_live,
            thread_id=thread_id,
            sqlite_path=sqlite_path,
            manifest_path=manifest_path,
        )

    def get_action_parse_langgraph_state(
        self,
        thread_id: str,
        sqlite_path: str,
    ) -> Dict[str, Any]:
        from core.langgraph_flow import get_action_parse_graph_state

        return get_action_parse_graph_state(
            self,
            thread_id=thread_id,
            sqlite_path=sqlite_path,
        )

    def get_action_parse_langgraph_history(
        self,
        thread_id: str,
        sqlite_path: str,
        limit: int = 10,
    ) -> Dict[str, Any]:
        from core.langgraph_flow import get_action_parse_graph_history

        return get_action_parse_graph_history(
            self,
            thread_id=thread_id,
            sqlite_path=sqlite_path,
            limit=limit,
        )

    def resume_action_parse_langgraph(
        self,
        thread_id: str,
        sqlite_path: str,
        decision: str,
    ) -> Dict[str, Any]:
        from core.langgraph_flow import resume_action_parse_graph

        return resume_action_parse_graph(
            self,
            thread_id=thread_id,
            sqlite_path=sqlite_path,
            decision=decision,
        )



    def process_structured_intent(self, request: MCPRequest) -> Dict[str, Any]:
        context = self._acquire_context(request)
        schema_prompt = """
You are an intent structurer for CAPS.
Return ONLY valid JSON matching this exact schema:
{
  "schema_version": "1.0",
  "intent": string,
  "requires_tools": boolean,
  "provided_context": string[],
  "required_context": [
    { "type": string, "required": boolean, "why"?: string }
  ],
  "missing_context": string[],
  "missing_questions": string[],
  "constraints"?: string[],
  "safety_checks"?: string[]
}

Rules:
- Output only JSON (no markdown, no comments).
- Use empty arrays where appropriate.
- Do not execute tasks or invent external data.
- If the request asks to send text/message/email/call/notify, set requires_tools=true.
- If the request includes weather lookup + messaging condition, include both weather and messaging related required_context items.
- missing_questions must be specific and actionable (e.g., location, recipient id, message body).
- Do not put context into missing fields if already present in user request.


Example:
Request: "If weather is below -20C, text Jacob I am not coming to university today."
Output:
{
  "schema_version": "1.0",
  "intent": "conditional_weather_notification",
  "requires_tools": true,
  "provided_context": ["temperature_threshold", "recipient_name", "message_body"],
  "required_context": [
    { "type": "location", "required": true, "why": "Weather lookup requires a location." },
    { "type": "weather_threshold", "required": true, "why": "Need threshold to evaluate condition." },
    { "type": "recipient_contact", "required": true, "why": "Need resolvable identity for Jacob." },
    { "type": "message_body", "required": true, "why": "Need outbound text message content." }
  ],
  "missing_context": ["location"],
  "missing_questions": ["What location should weather be checked for?"],
  "constraints": ["Send message only if temperature is below threshold."],
  "safety_checks": ["Confirm recipient identity before sending message."]
}
""".strip()

        composed_prompt = (
            f"[System]\n{schema_prompt}\n\n"
            f"[User ID: {request.user_id}]\n"
            f"[Context Mode: {context['context_mode']}]\n\n"
            f"[User Request]\n{request.prompt}"
        )

        repaired = False
        raw_output = self.llm_client.generate(composed_prompt, temperature=0.0)
        try:
            structured = validate_structured_intent(self._extract_json_object(raw_output))
        except SchemaValidationError:
            repaired = True
            repair_prompt = (
                "Return ONLY corrected JSON matching schema version 1.0.\n\n"
                f"Original request:\n{request.prompt}\n\n"
                f"Previous output:\n{raw_output}"
            )
            repaired_output = self.llm_client.generate(repair_prompt, temperature=0.0)
            structured = validate_structured_intent(self._extract_json_object(repaired_output))

        structured = self._reconcile_structured_intent(structured, request.prompt, repaired)

        action_out = self.process_action_parse(request)
        action_parse = action_out["action_parse"]
        task_graph = action_out["task_graph"]
        graph_verification = action_out["task_graph_verification"]
        execution_plan = action_out["execution_plan"]

        return {
            "structured_intent": structured,
            "action_parse": action_parse,
            "task_graph": task_graph,
            "task_graph_verification": graph_verification,
            "execution_plan": execution_plan,
            "model": self.llm_client.model,
            "context_mode": context["context_mode"],
        }
