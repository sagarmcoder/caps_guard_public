import os


DEFAULT_MODEL = os.getenv("CAPS_DEFAULT_MODEL", "llama3.2:3b")
DEFAULT_TEMPERATURE = float(os.getenv("CAPS_DEFAULT_TEMPERATURE", "0.2"))
DEFAULT_STRICT_MODE = os.getenv("CAPS_STRICT_MODE", "true").strip().lower() in {"1", "true", "yes", "on"}
DEFAULT_CLARIFY_LLM_POLISH = os.getenv("CAPS_CLARIFY_LLM_POLISH", "false").strip().lower() in {"1", "true", "yes", "on"}
