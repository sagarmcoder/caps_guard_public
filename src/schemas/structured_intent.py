from typing import Any, Dict


class SchemaValidationError(ValueError):
    pass


def _normalize_optional_str_list(value: Any, field_name: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        if any(not isinstance(x, str) for x in value):
            raise SchemaValidationError(f"Field '{field_name}' must be a list of strings.")
        return value
    raise SchemaValidationError(f"Field '{field_name}' must be a list of strings.")


def _normalize_required_str_list(value: Any, field_name: str) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(x, str) for x in value):
        raise SchemaValidationError(f"Field '{field_name}' must be a list of strings.")
    return value


def _looks_tool_intent(intent: str) -> bool:
    lowered = intent.lower()
    keywords = [
        "weather",
        "text",
        "message",
        "email",
        "call",
        "send",
        "notify",
        "api",
    ]
    return any(token in lowered for token in keywords)


def validate_structured_intent(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise SchemaValidationError("StructuredIntent payload must be a JSON object.")

    required_fields = [
        "schema_version",
        "intent",
        "requires_tools",
        "required_context",
        "missing_questions",
        "provided_context",
        "missing_context",

    ]
    for field in required_fields:
        if field not in payload:
            raise SchemaValidationError(f"Missing required field '{field}'.")

    if payload["schema_version"] != "1.0":
        raise SchemaValidationError("Field 'schema_version' must be '1.0'.")
    if not isinstance(payload["intent"], str):
        raise SchemaValidationError("Field 'intent' must be a string.")
    if not isinstance(payload["requires_tools"], bool):
        raise SchemaValidationError("Field 'requires_tools' must be a boolean.")
    if not isinstance(payload["required_context"], list):
        raise SchemaValidationError("Field 'required_context' must be a list.")
    if not isinstance(payload["missing_questions"], list):
        raise SchemaValidationError("Field 'missing_questions' must be a list.")

    for idx, item in enumerate(payload["required_context"]):
        if not isinstance(item, dict):
            raise SchemaValidationError(f"required_context[{idx}] must be an object.")
        if "type" not in item or "required" not in item:
            raise SchemaValidationError(
                f"required_context[{idx}] must include 'type' and 'required'."
            )
        if not isinstance(item["type"], str):
            raise SchemaValidationError(f"required_context[{idx}].type must be a string.")
        if not isinstance(item["required"], bool):
            raise SchemaValidationError(f"required_context[{idx}].required must be a boolean.")
        if "why" in item and not isinstance(item["why"], str):
            raise SchemaValidationError(f"required_context[{idx}].why must be a string.")

    for idx, q in enumerate(payload["missing_questions"]):
        if not isinstance(q, str):
            raise SchemaValidationError(f"missing_questions[{idx}] must be a string.")

    provided_context = _normalize_required_str_list(payload["provided_context"], "provided_context")
    missing_context = _normalize_required_str_list(payload["missing_context"], "missing_context")
    constraints = _normalize_optional_str_list(payload.get("constraints"), "constraints")
    safety_checks = _normalize_optional_str_list(payload.get("safety_checks"), "safety_checks")
    confidence = payload.get("confidence", 0.0)
    if not isinstance(confidence, (int, float)) or confidence < 0.0 or confidence > 1.0:
        raise SchemaValidationError("Field 'confidence' must be a number between 0.0 and 1.0.")
    fallback_reason = payload.get("fallback_reason")
    if fallback_reason is not None and not isinstance(fallback_reason, str):
        raise SchemaValidationError("Field 'fallback_reason' must be a string when provided.")

    intent = payload["intent"].strip()
    if not intent:
        intent = "unknown"
    if intent in {"unknown", "unclear"} and not payload["missing_questions"]:
        raise SchemaValidationError("Unknown intents must include at least one clarification question.")
    requires_tools = payload["requires_tools"]
    required_context = payload["required_context"]
    if _looks_tool_intent(intent):
        if not requires_tools:
            raise SchemaValidationError(
                "Tool-like intents must set 'requires_tools' to true."
            )
        if not required_context:
            raise SchemaValidationError(
                "Tool-like intents must include non-empty 'required_context'."
            )

    return {
        "schema_version": "1.0",
        "intent": intent,
        "requires_tools": requires_tools,
        "required_context": required_context,
        "missing_questions": payload["missing_questions"],
        "constraints": constraints,
        "safety_checks": safety_checks,
        "provided_context": provided_context,
        "missing_context": missing_context,
        "confidence": float(confidence),
        "fallback_reason": fallback_reason,
    }
