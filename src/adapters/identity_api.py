from typing import Any, Dict

from adapters.base import ok_response, error_response


def resolve_recipient(params: Dict[str, Any], request_context: Dict[str, Any], trace_id: str) -> Dict[str, Any]:
    recipient_ref = params.get("recipient_ref") or params.get("recipient")
    if not isinstance(recipient_ref, str) or not recipient_ref.strip():
        return error_response("IDENTITY_INVALID_INPUT", "recipient_ref is required", retryable=False)

    ref = recipient_ref.strip().lower()
    if ref in {"unknown", "recipient_from_context", "from_context"}:
        return error_response("IDENTITY_UNRESOLVED", "recipient could not be resolved", retryable=False)

    return ok_response(
        {
            "recipient_resolved": True,
            "recipient_id": f"recipient_{abs(hash(ref)) % 100000}",
            "recipient_ref": recipient_ref,
        },
        message="recipient resolved",
    )
