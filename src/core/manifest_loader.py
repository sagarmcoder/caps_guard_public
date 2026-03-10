import json
from pathlib import Path
from typing import Any, Dict, List


DEFAULT_MANIFEST_PATH = "src/manifest.json"
SIDE_EFFECT_CLASSES = {"READ", "WRITE", "IRREVERSIBLE"}


class ManifestValidationError(ValueError):
    pass


def _require_str(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ManifestValidationError(f"manifest field '{field}' must be a non-empty string")
    return value.strip()


def _normalize_tool_registry(tool_registry: Any) -> List[Dict[str, Any]]:
    if not isinstance(tool_registry, list):
        raise ManifestValidationError("manifest field 'tool_registry' must be a list")

    normalized: List[Dict[str, Any]] = []
    seen_names: set[str] = set()
    for idx, tool in enumerate(tool_registry):
        if not isinstance(tool, dict):
            raise ManifestValidationError(f"tool_registry[{idx}] must be an object")

        name = _require_str(tool.get("name"), f"tool_registry[{idx}].name")
        if name in seen_names:
            raise ManifestValidationError(f"duplicate tool_registry name '{name}'")
        seen_names.add(name)

        binding = tool.get("binding", name)
        binding = _require_str(binding, f"tool_registry[{idx}].binding")

        description = tool.get("description", "")
        if description is None:
            description = ""
        if not isinstance(description, str):
            raise ManifestValidationError(f"tool_registry[{idx}].description must be a string")

        side_effect_class = str(tool.get("side_effect_class", "READ")).strip().upper()
        if side_effect_class not in SIDE_EFFECT_CLASSES:
            raise ManifestValidationError(
                f"tool_registry[{idx}].side_effect_class must be one of {sorted(SIDE_EFFECT_CLASSES)}"
            )

        normalized_tool: Dict[str, Any] = {
            "name": name,
            "binding": binding,
            "description": description,
            "side_effect_class": side_effect_class,
        }
        if "data_sensitivity" in tool and tool.get("data_sensitivity") is not None:
            ds = tool.get("data_sensitivity")
            if not isinstance(ds, str):
                raise ManifestValidationError(f"tool_registry[{idx}].data_sensitivity must be a string")
            normalized_tool["data_sensitivity"] = ds.strip().upper()

        normalized.append(normalized_tool)

    return normalized


def _normalize_sink_tools(sink_tools: Any, tool_names: set[str]) -> List[str]:
    if sink_tools is None:
        return []
    if not isinstance(sink_tools, list):
        raise ManifestValidationError("manifest field 'sink_tools' must be a list")
    normalized: List[str] = []
    for idx, tool_name in enumerate(sink_tools):
        if not isinstance(tool_name, str) or not tool_name.strip():
            raise ManifestValidationError(f"sink_tools[{idx}] must be a non-empty string")
        name = tool_name.strip()
        if name not in tool_names:
            raise ManifestValidationError(f"sink_tools[{idx}] references unknown tool '{name}'")
        if name not in normalized:
            normalized.append(name)
    return normalized


def _normalize_dict(obj: Any, field: str) -> Dict[str, Any]:
    if obj is None:
        return {}
    if not isinstance(obj, dict):
        raise ManifestValidationError(f"manifest field '{field}' must be an object")
    return dict(obj)


def validate_manifest(manifest: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(manifest, dict):
        raise ManifestValidationError("manifest must be a JSON object")

    manifest_id = _require_str(manifest.get("manifest_id"), "manifest_id")
    manifest_version = _require_str(manifest.get("manifest_version"), "manifest_version")
    tool_registry = _normalize_tool_registry(manifest.get("tool_registry"))
    tool_names = {tool["name"] for tool in tool_registry}
    sink_tools = _normalize_sink_tools(manifest.get("sink_tools"), tool_names)
    review_policies = _normalize_dict(manifest.get("review_policies"), "review_policies")
    constraint_flags = _normalize_dict(manifest.get("constraint_flags"), "constraint_flags")

    return {
        "manifest_id": manifest_id,
        "manifest_version": manifest_version,
        "tool_registry": tool_registry,
        "sink_tools": sink_tools,
        "review_policies": review_policies,
        "constraint_flags": constraint_flags,
    }


def build_manifest_context(manifest: Dict[str, Any]) -> Dict[str, Any]:
    tool_registry = manifest.get("tool_registry", [])
    active_tools = [tool.get("name") for tool in tool_registry if isinstance(tool, dict)]
    tool_side_effect_classes = {
        tool.get("name"): str(tool.get("side_effect_class", "READ")).strip().upper()
        for tool in tool_registry
        if isinstance(tool, dict) and isinstance(tool.get("name"), str) and tool.get("name").strip()
    }
    review_policies = dict(manifest.get("review_policies", {}))
    constraint_flags = dict(manifest.get("constraint_flags", {}))

    active_policy_flags: List[str] = []
    if review_policies.get("sink_tools_require_review", False):
        active_policy_flags.append("HITL_FOR_SINKS")
    if constraint_flags.get("enforce_tool_registry", False):
        active_policy_flags.append("ENFORCE_TOOL_REGISTRY")
    if not constraint_flags.get("allow_unknown_tools", True):
        active_policy_flags.append("BLOCK_UNKNOWN_TOOLS")

    return {
        "manifest_id": manifest.get("manifest_id"),
        "manifest_version": manifest.get("manifest_version"),
        "active_tools": active_tools,
        "tool_side_effect_classes": tool_side_effect_classes,
        "sink_tools": list(manifest.get("sink_tools", [])),
        "active_policy_flags": active_policy_flags,
        "review_policies": review_policies,
        "constraint_flags": constraint_flags,
    }


def load_manifest(manifest_path: str | None = None) -> Dict[str, Any]:
    path_str = manifest_path or DEFAULT_MANIFEST_PATH
    path = Path(path_str)
    if not path.exists():
        raise ManifestValidationError(f"manifest file not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    return validate_manifest(raw)
