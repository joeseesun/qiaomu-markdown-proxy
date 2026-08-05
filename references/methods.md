# Fetch Methods Reference

## Proxy Cascade (General URLs)

Use `scripts/fetch.sh` for automatic proxy cascade with fallback. Try in order until success:

### 1. r.jina.ai

```bash
bash ~/.agents/skills/qiaomu-markdown-proxy/scripts/fetch.sh "https://example.com"
```

Wide coverage, preserves image links. Try this first.

### 2. defuddle.md

Automatically tried by `fetch.sh` if r.jina.ai fails. Cleaner output with YAML frontmatter.

### 3. agent-fetch

Last resort local tool, automatically tried if both proxies fail.

### With Custom Proxy

```bash
bash ~/.agents/skills/qiaomu-markdown-proxy/scripts/fetch.sh "https://example.com" "http://127.0.0.1:7890"
```

## PDF to Markdown

### Remote PDF URL

r.jina.ai handles PDF URLs directly:

```bash
curl -sL "https://r.jina.ai/https://example.com/paper.pdf"
```

If that fails, download and extract locally:

```bash
curl -sL "https://example.com/paper.pdf" -o /tmp/input.pdf
bash ~/.agents/skills/qiaomu-markdown-proxy/scripts/extract_pdf.sh /tmp/input.pdf
```

### Local PDF File

```bash
bash ~/.agents/skills/qiaomu-markdown-proxy/scripts/extract_pdf.sh /path/to/file.pdf
```

The script tries three methods in order:

1. **marker-pdf** (best quality, requires: `pip install marker-pdf`)
   - Best for papers, tables, complex layouts
   - Preserves formatting and structure

2. **pdftotext** (fast, requires: `brew install poppler`)
   - Good for text-heavy PDFs
   - Fast extraction with layout preservation

3. **pypdf** (no-dependency fallback, requires: `pip install pypdf`)
   - Works everywhere Python is available
   - Basic text extraction

## WeChat Public Account (公众号)

Use the dedicated wrapper. It first runs the proxy cascade, rejects WeChat verification/login walls, and then falls back to the bundled Playwright extractor:

```bash
bash ~/.agents/skills/qiaomu-markdown-proxy/scripts/fetch_weixin.sh "https://mp.weixin.qq.com/s/abc123"
```

If Python packages are missing and `uv` exists, the wrapper runs them in an isolated environment. A Chromium runtime is still required for the browser fallback.

**Manual setup** (only when neither local packages nor `uv` are available):
```bash
python3 -m pip install playwright beautifulsoup4 lxml
python3 -m playwright install chromium
```

**Output**: YAML frontmatter (title, author, date, url) + Markdown body

**JSON output**:
```bash
bash ~/.agents/skills/qiaomu-markdown-proxy/scripts/fetch_weixin.sh "URL" --json
```

## Feishu / Lark Document

Built-in API script for Feishu documents. Requires app credentials:

```bash
export FEISHU_APP_ID=your_app_id
export FEISHU_APP_SECRET=your_app_secret
python3 ~/.agents/skills/qiaomu-markdown-proxy/scripts/fetch_feishu.py "https://xxx.feishu.cn/docx/xxxxxxxx"
```

**Supported types**:
- `docx` - New-style documents
- `doc` - Legacy documents
- `wiki` - Wiki pages (auto-resolves to actual document)

**Required permissions**: `docx:document:readonly`, `wiki:wiki:readonly`

**Output**: YAML frontmatter (title, document_id, url) + Markdown body

**JSON output**:
```bash
python3 ~/.agents/skills/qiaomu-markdown-proxy/scripts/fetch_feishu.py "URL" --json
```

## YouTube Videos

Use the dedicated `qiaomu-youtube-download` skill for YouTube content. It handles:
- Video download
- Subtitle extraction
- Transcript generation

Do not use qiaomu-markdown-proxy for YouTube URLs.

## Content Validation

The `fetch.sh` script validates content before returning:

- Must have more than 5 lines
- Filters out common error pages:
  - "Don't miss what's happening" (Twitter login wall)
  - "Access Denied"
  - "404 Not Found"

If validation fails, automatically tries the next method.
