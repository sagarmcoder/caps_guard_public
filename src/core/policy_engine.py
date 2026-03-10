import time
from typing import Any, Dict, List


DECISION_ALLOW = "ALLOW"
DECISION_REVIEW = "REVIEW_REQUIRED"
DECISION_BLOCK = "BLOCK"

REASON_TOOL_ALLOWLISTED = "TOOL_ALLOWLISTED"
REASON_NON_TOOL_STEP = "NON_TOOL_STEP"
REASON_TOOL_DENYLISTED = "TOOL_DENYLISTED"
REASON_SINK_REQUIRES_REVIEW = "SINK_REQUIRES_REVIEW"
REASON_ARGS_FORBIDDEN_PATTERN = "ARGS_FORBIDDEN_PATTERN"
REASON_TOOL_UNKNOWN = "TOOL_UNKNOWN"
REASON_TOOL_NOT_ALLOWED = "TOOL_NOT_ALLOWED"
REASON_POLICY_CONFLICT = "POLICY_CONFLICT_RESOLVED"
REASON_REVIEW_POLICY_MATCHED = "REVIEW_POLICY_MATCHED"
REASON_EXECUTION_ERROR = "EXECUTION_ERROR"


def _normalize_precedence(raw: Any) -> List[str]:
    default = [DECISION_BLOCK, DECISION_REVIEW, DECISION_ALLOW]
    if not isinstance(raw, list):
        return default
    alias_map = {
        "DENY": DECISION_BLOCK,
        "BLOCK": DECISION_BLOCK,
        "REVIEW": DECISION_REVIEW,
        "REVIEW_REQUIRED": DECISION_REVIEW,
        "ALLOW": DECISION_ALLOW,
    }
    normalized = []
    for item in raw:
        if not isinstance(item, str):
            continue
        key = item.strip().upper()
        if not key:
            continue
        normalized.append(alias_map.get(key, key))
    if not normalized:
        return default
    # Ensure all expected decisions exist in precedence ordering.
    for decision in default:
        if decision not in normalized:
            normalized.append(decision)
    return normalized


def evaluate_tool_policy(
    *,
    step_id: str,
    tool_name: str | None,
    params: Dict[str, Any] | None,
    manifest_context: Dict[str, Any] | None,
    approved_for_sink: bool = False,
    trace_id: str | None = None,
) -> Dict[str, Any]:
    params = params or {}
    manifest_context = manifest_context or {}

    manifest_id = manifest_context.get("manifest_id")
    manifest_version = manifest_context.get("manifest_version")
    review_policies = manifest_context.get("review_policies", {}) or {}
    constraint_flags = manifest_context.get("constraint_flags", {}) or {}
    sink_tools = set(manifest_context.get("sink_tools", []) or [])
    tool_side_effect_classes = manifest_context.get("tool_side_effect_classes", {}) or {}
    active_tools = set(manifest_context.get("active_tools", []) or [])
    deny_tools = set(review_policies.get("deny_tools", []) or [])
    allow_tools = list(review_policies.get("allow_tools", []) or [])
    forbidden_arg_patterns = review_policies.get("forbidden_arg_patterns", []) or []
    precedence = _normalize_precedence(review_policies.get("precedence"))
    write_tools_require_review = bool(review_policies.get("write_tools_require_review", False))
    block_irreversible_tools = bool(review_policies.get("block_irreversible_tools", False))
    side_effect_class = str(tool_side_effect_classes.get(tool_name, "READ")).strip().upper()

    base = {
        "trace_id": trace_id,
        "step_id": step_id,
        "tool_name": tool_name,
        "manifest_id": manifest_id,
        "manifest_version": manifest_version,
        "timestamp_ms": int(time.time() * 1000),
    }

    if not tool_name:
        return {
            **base,
            "decision": DECISION_ALLOW,
            "reason_code": REASON_NON_TOOL_STEP,
            "rule_id": "NON_TOOL_STEP",
        }

    candidates: List[Dict[str, str]] = []

    if tool_name in deny_tools:
        candidates.append(
            {
                "decision": DECISION_BLOCK,
                "reason_code": REASON_TOOL_DENYLISTED,
                "rule_id": f"DENY_TOOL:{tool_name}",
            }
        )

    enforce_registry = bool(constraint_flags.get("enforce_tool_registry", False))
    allow_unknown_tools = bool(constraint_flags.get("allow_unknown_tools", True))
    default_deny = bool(constraint_flags.get("default_deny", False))
    if enforce_registry and tool_name not in active_tools and not allow_unknown_tools:
        candidates.append(
            {
                "decision": DECISION_BLOCK,
                "reason_code": REASON_TOOL_UNKNOWN,
                "rule_id": "ENFORCE_TOOL_REGISTRY",
            }
        )

    if allow_tools and tool_name not in set(allow_tools):
        candidates.append(
            {
                "decision": DECISION_BLOCK,
                "reason_code": REASON_TOOL_NOT_ALLOWED,
                "rule_id": "ALLOWLIST_MODE",
            }
        )

    if forbidden_arg_patterns and isinstance(params, dict):
        # Minimal safe scan: check stringified params for forbidden patterns.
        payload = str(params).lower()
        for pattern in forbidden_arg_patterns:
            if isinstance(pattern, str) and pattern.strip() and pattern.strip().lower() in payload:
                candidates.append(
                    {
                        "decision": DECISION_BLOCK,
                        "reason_code": REASON_ARGS_FORBIDDEN_PATTERN,
                        "rule_id": f"FORBIDDEN_ARG_PATTERN:{pattern.strip()}",
                    }
                )
                break

    if block_irreversible_tools and side_effect_class == "IRREVERSIBLE":
        candidates.append(
            {
                "decision": DECISION_BLOCK,
                "reason_code": REASON_TOOL_NOT_ALLOWED,
                "rule_id": "BLOCK_IRREVERSIBLE",
            }
        )

    if (
        write_tools_require_review
        and side_effect_class == "WRITE"
        and tool_name not in sink_tools
        and not approved_for_sink
    ):
        candidates.append(
            {
                "decision": DECISION_REVIEW,
                "reason_code": REASON_REVIEW_POLICY_MATCHED,
                "rule_id": "REVIEW_WRITE_CLASS",
            }
        )

    if (
        bool(review_policies.get("sink_tools_require_review", False))
        and tool_name in sink_tools
        and not approved_for_sink
    ):
        candidates.append(
            {
                "decision": DECISION_REVIEW,
                "reason_code": REASON_REVIEW_POLICY_MATCHED,
                "rule_id": "REVIEW_SINK_TOOL",
            }
        )

    if not candidates:
        if default_deny:
            return {
                **base,
                "decision": DECISION_BLOCK,
                "reason_code": REASON_TOOL_NOT_ALLOWED,
                "rule_id": "DEFAULT_DENY",
            }
        return {
            **base,
            "decision": DECISION_ALLOW,
            "reason_code": REASON_TOOL_ALLOWLISTED,
            "rule_id": "DEFAULT_ALLOW",
        }

    rank = {name: idx for idx, name in enumerate(precedence)}
    chosen = sorted(
        candidates,
        key=lambda item: rank.get(item["decision"], 999),
    )[0].copy()

    if chosen.get("decision") == DECISION_BLOCK and not chosen.get("rule_id"):
        chosen["rule_id"] = "DEFAULT_DENY"

    if len(candidates) > 1:
        chosen["precedence_resolved"] = True
        chosen["precedence_order"] = precedence
        chosen["resolution_reason_code"] = REASON_POLICY_CONFLICT
        chosen["winning_decision"] = chosen.get("decision")
        chosen["winning_reason_code"] = chosen.get("reason_code")
        chosen["winning_rule_id"] = chosen.get("rule_id")
        chosen["matched_rules"] = [
            {
                "decision": item.get("decision"),
                "reason_code": item.get("reason_code"),
                "rule_id": item.get("rule_id"),
            }
            for item in candidates
        ]

    return {**base, **chosen}
