from typing import Any, Dict

from adapters.base import ok_response, error_response


def send_message(params: Dict[str, Any], request_context: Dict[str, Any], trace_id: str) -> Dict[str, Any]:
    recipient_id = params.get("recipient_id")
    message = params.get("message")
    idempotency_key = params.get("idempotency_key")

    if not isinstance(recipient_id, str) or not recipient_id.strip():
        return error_response("MESSAGE_RECIPIENT_REQUIRED", "recipient_id is required", retryable=False)
    if not isinstance(message, str) or not message.strip():
        return error_response("MESSAGE_BODY_REQUIRED", "message is required", retryable=False)
    if not isinstance(idempotency_key, str) or not idempotency_key.strip():
        return error_response("MESSAGE_IDEMPOTENCY_REQUIRED", "idempotency_key is required", retryable=False)

    # Stub send result; real provider integration later.
    return ok_response(
        {
            "sent": True,
            "recipient_id": recipient_id,
            "message": message,
            "idempotency_key": idempotency_key,
            "provider_message_id": f"msg_{abs(hash((recipient_id, message, trace_id))) % 1000000}",
        },
        message="message sent",
    )
