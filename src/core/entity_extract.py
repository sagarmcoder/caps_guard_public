import re
from typing import Dict, Any


TIME_PATTERNS = [
    r"\b\d{1,2}(:\d{2})?\s?(am|pm)\b",
    r"\b(?:tomorrow|today|tonight|next\s+\w+)\b",
]
TEMP_PATTERN = r"(-?\d+)\s?(?:°?\s?[CFcf]|degrees?)"
MESSAGE_VERBS = r"\b(text|message|email|send|notify|call)\b"
INFO_PATTERNS = [
    r"\bexplain\b",
    r"\bdefine\b",
    r"\bwhat is\b",
    r"\bhow does\b",
]


def looks_informational(prompt: str) -> bool:
    p = prompt.lower().strip()
    return any(re.search(pat, p, re.IGNORECASE) for pat in INFO_PATTERNS)


def looks_ambiguous(prompt: str) -> bool:
    p = prompt.strip()
    if len(p) < 3:
        return True
    if p in {"...", "..", ".", "??", "?"}:
        return True
    if not re.search(r"[A-Za-z0-9]", p):
        return True
    return False


def extract_entities(prompt: str) -> Dict[str, Any]:
    p = prompt.lower()

    has_time = any(re.search(pat, p, re.IGNORECASE) for pat in TIME_PATTERNS)
    temp_match = re.search(TEMP_PATTERN, p, re.IGNORECASE)
    has_messaging = bool(re.search(MESSAGE_VERBS, p, re.IGNORECASE))

    recipient = None
    m = re.search(r"\b(?:text|message|email|notify|call)\s+([A-Z][a-zA-Z0-9_-]+)", prompt)
    if m:
        recipient = m.group(1)

    return {
        "has_time": has_time,
        "temperature_threshold": temp_match.group(1) if temp_match else None,
        "has_messaging": has_messaging,
        "recipient_name": recipient,
    }
