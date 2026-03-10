from typing import Any, Dict

from adapters.base import ok_response, error_response


def send_email(params: Dict[str, Any], request_context: Dict[str, Any], trace_id: str) -> Dict[str, Any]:
    recipient_id = params.get("recipient_id")
    recipient_ref = params.get("recipient_ref") or params.get("recipient")
    message = params.get("message")
    subject = params.get("subject") if isinstance(params.get("subject"), str) else "CAPS Notification"
    idempotency_key = params.get("idempotency_key")

    if (not isinstance(recipient_id, str) or not recipient_id.strip()) and (
        not isinstance(recipient_ref, str) or not recipient_ref.strip()
    ):
        return error_response("EMAIL_RECIPIENT_REQUIRED", "recipient_id or recipient_ref is required", retryable=False)

    if not isinstance(message, str) or not message.strip():
        return error_response("EMAIL_BODY_REQUIRED", "message is required", retryable=False)

    if not isinstance(idempotency_key, str) or not idempotency_key.strip():
        return error_response("EMAIL_IDEMPOTENCY_REQUIRED", "idempotency_key is required", retryable=False)

    rid = recipient_id if isinstance(recipient_id, str) and recipient_id.strip() else recipient_ref
    return ok_response(
        {
            "sent": True,
            "recipient_id": rid,
            "recipient_ref": recipient_ref,
            "subject": subject,
            "message": message,
            "idempotency_key": idempotency_key,
            "provider_email_id": f"email_{abs(hash((rid, subject, message, trace_id))) % 1000000}",
        },
        message="email sent",
    )
