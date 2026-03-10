from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional


@dataclass
class AdapterResponse:
    ok: bool
    data: Optional[Dict[str, Any]]
    error_code: Optional[str]
    retryable: bool
    message: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def ok_response(data: Dict[str, Any], message: str = "ok") -> Dict[str, Any]:
    return AdapterResponse(
        ok=True,
        data=data,
        error_code=None,
        retryable=False,
        message=message,
    ).to_dict()


def error_response(
    error_code: str,
    message: str,
    retryable: bool = False,
    data: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return AdapterResponse(
        ok=False,
        data=data,
        error_code=error_code,
        retryable=retryable,
        message=message,
    ).to_dict()
