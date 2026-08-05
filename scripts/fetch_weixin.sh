#!/usr/bin/env bash
# Fetch a WeChat public-account article as Markdown.
# Proxy extraction is attempted first. Verification/login walls automatically
# fall back to the bundled Playwright extractor.
set -euo pipefail

URL="${1:-}"
if [[ -z "$URL" || "$URL" != https://mp.weixin.qq.com/* ]]; then
  echo "Usage: fetch_weixin.sh https://mp.weixin.qq.com/s/... [--json]" >&2
  exit 64
fi

OUTPUT_MODE="${2:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Proxy output cannot honor the JSON contract, so structured callers go
# directly to the browser extractor.
if [[ "$OUTPUT_MODE" != "--json" ]]; then
  if OUTPUT=$(bash "$SCRIPT_DIR/fetch.sh" "$URL" 2>/dev/null); then
    printf '%s\n' "$OUTPUT"
    exit 0
  fi
fi

if python3 -c 'import playwright, bs4, lxml' >/dev/null 2>&1; then
  exec python3 "$SCRIPT_DIR/fetch_weixin.py" "$URL" ${OUTPUT_MODE:+"$OUTPUT_MODE"}
fi

if command -v uv >/dev/null 2>&1; then
  exec uv run --quiet \
    --with playwright \
    --with beautifulsoup4 \
    --with lxml \
    python "$SCRIPT_DIR/fetch_weixin.py" "$URL" ${OUTPUT_MODE:+"$OUTPUT_MODE"}
fi

cat >&2 <<'EOF'
ERROR: WeChat proxy extraction was blocked and the Playwright fallback is unavailable.
Install once:
  python3 -m pip install playwright beautifulsoup4 lxml
  python3 -m playwright install chromium
Or install uv so the Python packages can run in an isolated environment:
  https://docs.astral.sh/uv/
EOF
exit 1
