#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

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

echo "Demo complete."
echo "Trace JSON:  /tmp/caps_guard_demo_approve/trace.json"
echo "Trace HTML:  /tmp/caps_guard_demo_approve/trace_render.html"
