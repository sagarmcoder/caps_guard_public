import json
import urllib.error
import urllib.request


class OllamaClient:
    def __init__(self, model: str = "llama3.1:8b", base_url: str = "http://localhost:11434"):
        self.model = model
        self.endpoint = f"{base_url}/api/generate"

    def generate(self, prompt: str, temperature: float = 0.2) -> str:
        payload = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": temperature},
        }
        body = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            self.endpoint,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                data = json.loads(response.read().decode("utf-8"))
                return data.get("response", "").strip()
        except urllib.error.URLError as exc:
            raise RuntimeError(
                "Failed to reach Ollama. Ensure Ollama is running at "
                "http://localhost:11434 and a model is pulled."
            ) from exc
