# LangGraph Demo (Pause/Resume + Trace Render)

This demo shows the v0.1 core story end-to-end:
- Prompt execution pauses on sink tool (`REVIEW_REQUIRED`).
- Resume with human approval.
- Render trace into shareable HTML.

## Prerequisites
- Run from repo root.
- Ollama available locally for `--prompt` path.

## Commands
```bash
rm -f .caps_guard_demo.sqlite
rm -rf /tmp/caps_guard_demo_block /tmp/caps_guard_demo_approve

python scripts/caps_guard.py execute \
  --manifest src/manifest_demo.json \
  --prompt "If weather is below 100C in Toronto, text Jacob I am not coming to university today." \
  --thread-id langgraph-demo-1 \
  --sqlite-path .caps_guard_demo.sqlite \
  --output-dir /tmp/caps_guard_demo_block \
  > /tmp/caps_guard_demo_block_stdout.json

python scripts/caps_guard.py execute \
  --manifest src/manifest_demo.json \
  --resume-review approve \
  --thread-id langgraph-demo-1 \
  --sqlite-path .caps_guard_demo.sqlite \
  --output-dir /tmp/caps_guard_demo_approve \
  > /tmp/caps_guard_demo_approve_stdout.json

python scripts/caps_guard.py render-trace \
  --trace /tmp/caps_guard_demo_approve/trace.json \
  --output /tmp/caps_guard_demo_approve/trace_render.html \
  --title "CAPS Guard LangGraph Demo Trace"
```

## Expected Results
- `/tmp/caps_guard_demo_block/trace.json` has `REVIEW_REQUIRED` for `messaging_api`.
- `/tmp/caps_guard_demo_approve/trace.json` includes `review_resume` and sink `tool_call/tool_result`.
- `/tmp/caps_guard_demo_approve/trace_render.html` opens as a visual event timeline.

## Optional: Open Rendered HTML
```bash
open /tmp/caps_guard_demo_approve/trace_render.html
```
