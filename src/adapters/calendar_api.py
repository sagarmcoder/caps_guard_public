from typing import Any, Dict

from adapters.base import ok_response, error_response


def schedule_event(params: Dict[str, Any], request_context: Dict[str, Any], trace_id: str) -> Dict[str, Any]:
    recipient = params.get("recipient")
    date = params.get("date")
    time_value = params.get("time")
    title = params.get("title") if isinstance(params.get("title"), str) else "CAPS Meeting"

    if not isinstance(recipient, str) or not recipient.strip():
        return error_response("CALENDAR_RECIPIENT_REQUIRED", "recipient is required", retryable=False)

    if not isinstance(date, str) or not date.strip() or date.strip().lower() in {"from_context", "unknown"}:
        return error_response("CALENDAR_DATE_REQUIRED", "date is required", retryable=False)

    if not isinstance(time_value, str) or not time_value.strip() or time_value.strip().lower() in {"from_context", "unknown"}:
        return error_response("CALENDAR_TIME_REQUIRED", "time is required", retryable=False)

    recipient_clean = recipient.strip()
    date_clean = date.strip()
    time_clean = time_value.strip()

    return ok_response(
        {
            "scheduled": True,
            "title": title,
            "recipient": recipient_clean,
            "date": date_clean,
            "time": time_clean,
            "provider_event_id": f"evt_{abs(hash((recipient_clean, date_clean, time_clean, trace_id))) % 1000000}",
        },
        message="event scheduled",
    )
