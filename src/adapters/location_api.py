import json
import urllib.parse
import urllib.request
from typing import Any, Dict

from adapters.base import ok_response, error_response


_GEOCODE_URL = "https://geocoding-api.open-meteo.com/v1/search"


def resolve_location(params: Dict[str, Any], request_context: Dict[str, Any], trace_id: str) -> Dict[str, Any]:
    location_ref = params.get("location_ref")
    if not isinstance(location_ref, str) or not location_ref.strip():
        return error_response("LOCATION_INVALID_INPUT", "location_ref is required", retryable=False)

    q = location_ref.strip()
    if q.lower() in {"unknown", "location_from_context", "from_context"}:
        return error_response("LOCATION_UNRESOLVED", "location could not be resolved", retryable=False)

    try:
        query = urllib.parse.urlencode({"name": q, "count": 1, "language": "en", "format": "json"})
        with urllib.request.urlopen(f"{_GEOCODE_URL}?{query}", timeout=8) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        return error_response("LOCATION_UNAVAILABLE", f"geocoding request failed: {exc}", retryable=True)

    results = data.get("results") or []
    if not results:
        return error_response("LOCATION_UNRESOLVED", "no location match found", retryable=False)

    top = results[0]
    lat = top.get("latitude")
    lon = top.get("longitude")
    if lat is None or lon is None:
        return error_response("LOCATION_UNRESOLVED", "location match missing coordinates", retryable=False)

    location_id = f"{float(lat):.4f},{float(lon):.4f}"
    return ok_response(
        {
            "location_resolved": True,
            "location_id": location_id,
            "location_ref": q,
            "latitude": float(lat),
            "longitude": float(lon),
            "timezone": top.get("timezone"),
            "country": top.get("country"),
        },
        message="location resolved",
    )
