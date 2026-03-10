import json
import urllib.parse
import urllib.request
from typing import Any, Dict

from adapters.base import ok_response, error_response


_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"


def _coords_from_location_id(location_id: str) -> tuple[float, float] | None:
    try:
        a, b = location_id.split(",", 1)
        return float(a), float(b)
    except Exception:
        return None


def fetch_weather(params: Dict[str, Any], request_context: Dict[str, Any], trace_id: str) -> Dict[str, Any]:
    location_id = params.get("location_id")
    location_ref = params.get("location_ref")

    if not isinstance(location_id, str) or not location_id.strip():
        return error_response("WEATHER_LOCATION_REQUIRED", "resolved location_id is required", retryable=False)

    coords = _coords_from_location_id(location_id.strip())
    if not coords:
        return error_response("WEATHER_INVALID_LOCATION_ID", "location_id must be 'lat,lon'", retryable=False)

    lat, lon = coords
    try:
        query = urllib.parse.urlencode(
            {
                "latitude": lat,
                "longitude": lon,
                "current": "temperature_2m,weather_code",
                "timezone": "auto",
            }
        )
        with urllib.request.urlopen(f"{_FORECAST_URL}?{query}", timeout=8) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        return error_response("WEATHER_UNAVAILABLE", f"forecast request failed: {exc}", retryable=True)

    current = data.get("current") or {}
    temp_c = current.get("temperature_2m")
    weather_code = current.get("weather_code")
    if temp_c is None:
        return error_response("WEATHER_INVALID_RESPONSE", "missing temperature_2m", retryable=False)

    return ok_response(
        {
            "location_ref": location_ref,
            "location_id": location_id,
            "temperature_c": float(temp_c),
            "weather_code": weather_code,
            "raw_current": current,
        },
        message="weather fetched",
    )
