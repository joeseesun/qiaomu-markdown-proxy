---
name: qiaomu-markdown-proxy
description: "Read, fetch, extract, parse, or convert a URL or link into clean Markdown. Use FIRST whenever a user asks to read source content from a webpage, especially WeChat/微信公众号 mp.weixin.qq.com, Feishu/Lark docs, X/Twitter, PDFs, arXiv papers, or a URL that will feed a later summary, rewrite, article, podcast, or analysis. For WeChat and Feishu, prefer this specialist extractor over generic web open or generic content parsers. Exclude YouTube, pure web search, already-pasted text, and local non-PDF files."
version: 2.1.0
---

# Markdown Proxy - URL to Markdown

将任意 URL 转为干净的 Markdown。支持需要登录的页面、PDF、专有平台。

## Trigger Priority（先触发，再分流）

- 用户给出 URL 并要求“读取、抓取、提取、解析、转 Markdown”时，优先使用本 Skill。
- 用户要求“读取这篇链接后再写稿、总结、翻译、做播客或分析”时，先用本 Skill 取回原文，再把 Markdown 交给下游 Skill；不要因为最终任务是写作而跳过抓取。
- `mp.weixin.qq.com` 和飞书文档属于专用路由。不要先试普通网页打开或通用内容解析器。
- 用户已经贴出全文时不触发。YouTube 交给 `qiaomu-youtube-download`，普通搜索问题交给搜索工具。

## URL Routing (先判断再执行)

收到 URL 后，先判断类型，不同类型走不同通道：

| URL Pattern | Route To | Reason |
|-------------|----------|--------|
| `mp.weixin.qq.com` | `scripts/fetch_weixin.sh` | 先代理，验证码页自动回退 Playwright |
| `feishu.cn/docx/` `feishu.cn/wiki/` `larksuite.com/docx/` | `scripts/fetch_feishu.py` | 需飞书 API 认证 |
| `youtube.com` `youtu.be` | `qiaomu-youtube-download` skill | YouTube 有专用工具链 |
| `huggingface.co/papers/` | 提取 arXiv ID → `scripts/extract_tex.py` | HuggingFace 论文页实际是 arXiv 镜像，先找到 arXiv 链接再走 LaTeX 提取 |
| `arxiv.org/abs/` `arxiv.org/pdf/` | `scripts/extract_tex.py` | 从 LaTeX 源码提取结构化内容 (章节/图表/公式) |
| `.pdf` (URL or local path) | `scripts/extract_pdf.sh` | PDF 专用提取 |
| All other URLs | `scripts/fetch.sh` | 代理级联自动 fallback |

## Workflow

### Step 1: Route by URL Type

```
if URL contains "mp.weixin.qq.com":
    → bash ~/.agents/skills/qiaomu-markdown-proxy/scripts/fetch_weixin.sh "URL"
    → Done

if URL contains "feishu.cn/docx/" or "feishu.cn/wiki/" or "larksuite.com/docx/":
    → python3 ~/.agents/skills/qiaomu-markdown-proxy/scripts/fetch_feishu.py "URL"
    → Done

if URL contains "huggingface.co/papers/":
    → First fetch the page (WebFetch) to find the arXiv URL
    → Then python3 ~/.agents/skills/qiaomu-markdown-proxy/scripts/extract_tex.py "{arxiv_url}"
    → Done

if URL contains "arxiv.org/abs/" or "arxiv.org/pdf/":
    → python3 ~/.agents/skills/qiaomu-markdown-proxy/scripts/extract_tex.py "URL"
    → Done

if URL contains "youtube.com" or "youtu.be":
    → Call qiaomu-youtube-download skill
    → Done

if URL ends with ".pdf" or is local PDF path:
    if remote URL:
        → Try: curl -sL "https://r.jina.ai/{url}"
        → If fails: download + extract_pdf.sh
    if local path:
        → bash ~/.agents/skills/qiaomu-markdown-proxy/scripts/extract_pdf.sh "PATH"
    → Done

else:
    → bash ~/.agents/skills/qiaomu-markdown-proxy/scripts/fetch.sh "URL"
    → Done
```

### Step 2: Display Content

After fetching, show to user:

```
Title:  {title}
Author: {author} (if available)
Source: {platform} (公众号 / 飞书文档 / 网页 / PDF)
URL:    {original_url}

Summary
{3-5 sentence summary}

Content
{full Markdown, truncated at 200 lines if long}
```

### Step 3: Continue or Save

- **Composite request**（“读取后写稿/总结/分析”）：把提取结果直接交给下游任务，在同一轮继续；除非用户要求，不必额外保存源文件。
- **Extraction-only request**（“只读取/转 Markdown”）：保存到 `~/Downloads/{title}.md`，使用 YAML frontmatter。

- Filename: use article title, remove special characters.
- Format: YAML frontmatter (title, author, date, url, source) + Markdown body.
- Tell the user the saved path.
- Skip saving if the user says “just preview” or “don't save”.

Only stop after extraction when extraction was the complete request. If the user asked for a downstream deliverable, continue to that deliverable.

## Examples

### General URL
```bash
bash ~/.agents/skills/qiaomu-markdown-proxy/scripts/fetch.sh "https://example.com/article"
```

### X/Twitter Post
```bash
bash ~/.agents/skills/qiaomu-markdown-proxy/scripts/fetch.sh "https://x.com/username/status/1234567890"
```

### WeChat Article
```bash
bash ~/.agents/skills/qiaomu-markdown-proxy/scripts/fetch_weixin.sh "https://mp.weixin.qq.com/s/abc123"
```

### Feishu Document
```bash
python3 ~/.agents/skills/qiaomu-markdown-proxy/scripts/fetch_feishu.py "https://xxx.feishu.cn/docx/xxxxxxxx"
```

### arXiv LaTeX Source
```bash
python3 ~/.agents/skills/qiaomu-markdown-proxy/scripts/extract_tex.py "https://arxiv.org/abs/1706.03762"
```

### PDF (Remote)
```bash
curl -sL "https://r.jina.ai/https://example.com/paper.pdf"
```

### PDF (Local)
```bash
bash ~/.agents/skills/qiaomu-markdown-proxy/scripts/extract_pdf.sh "/path/to/paper.pdf"
```

### With Custom Proxy
```bash
bash ~/.agents/skills/qiaomu-markdown-proxy/scripts/fetch.sh "https://example.com" "http://127.0.0.1:7890"
```

## Notes

- r.jina.ai and defuddle.md require no API key
- `fetch.sh` handles proxy cascade with automatic fallback
- Content validation: filters error, login-wall, and WeChat verification pages; requires >5 lines
- WeChat wrapper prefers dependency-free proxies, then uses local Playwright; when Python packages are missing and `uv` is available, it runs them in an isolated environment
- Playwright fallback requires a Chromium runtime; install once with `python3 -m playwright install chromium` if absent
- Feishu script requires: `FEISHU_APP_ID` + `FEISHU_APP_SECRET` env vars
- PDF extraction tries: marker-pdf → pdftotext → pypdf
- For detailed method documentation, see `references/methods.md`
