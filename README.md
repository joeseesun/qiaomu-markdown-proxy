# qiaomu-markdown-proxy

> 给 Agent 一个链接，它会先选对读取通道，再把完整原文交给后续的总结、写作、翻译或分析任务。

微信、飞书、X/Twitter、PDF、arXiv 和普通网页走不同的专用路线。遇到公众号验证码页会自动回退到浏览器提取，不会把“环境异常”误当正文。

**[English](#english) | [中文](#中文)**

---

<!-- qiaomu-profile:start -->
## 关于向阳乔木

向阳乔木（乔向阳 / Joe）是一位实践型 AI 产品与内容创作者，长期把前沿 AI 变化转译成可复用的工作流、产品判断、AI 编程实践、AI 搜索实践和 GEO/AI 营销方法。

- 个人网站: https://qiaomu.ai
- 博客: https://blog.qiaomu.ai
- X: https://x.com/vista8
- GitHub: https://github.com/joeseesun/
- 微信公众号: 向阳乔木推荐看

### 支持与关注

| 打赏支持 | 微信公众号 |
|---|---|
| <img src="assets/qiaomu-profile/qiaomu_reward_qr.png" alt="向阳乔木打赏二维码" width="180" /> | <img src="assets/qiaomu-profile/qiaomu_wechat_public_account_qr.jpg" alt="向阳乔木推荐看公众号二维码" width="180" /> |
| 感谢支持乔木持续分享 AI 实践 | 扫码关注「向阳乔木推荐看」 |

<!-- qiaomu-profile:end -->

<a name="english"></a>
## English

### Features

Send any URL to Claude, and it automatically fetches the full content as Markdown. Six content types have dedicated extraction:

| URL Type | Method | Why |
|----------|--------|-----|
| WeChat Articles (`mp.weixin.qq.com`) | Proxy-first wrapper with Playwright fallback | Rejects verification walls before returning content |
| Feishu/Lark Docs (`feishu.cn`, `larksuite.com`) | Built-in Feishu API script | Requires API authentication, auto-converts to Markdown |
| YouTube | Dedicated YouTube skill | Video content has its own toolchain |
| arXiv / Hugging Face Papers | Built-in LaTeX source extractor | Preserves sections, equations, figures, and tables |
| PDF (remote or local) | Built-in PDF extraction (`extract_pdf.sh`) | Three-method cascade: marker-pdf → pdftotext → pypdf |
| All other URLs | Proxy cascade via `fetch.sh`: r.jina.ai → defuddle.md → agent-fetch | Free, no API key, content validation built-in |

### Prerequisites

- [ ] [Claude Code](https://docs.anthropic.com/en/docs/claude-code) installed
- [ ] **curl** (built-in on macOS/Linux)
- [ ] (Optional - WeChat scraping) Python 3.8+ with playwright
  ```bash
  python3 -m pip install playwright beautifulsoup4 lxml
  python3 -m playwright install chromium
  ```
- [ ] (Optional - PDF extraction) One of:
  - **marker-pdf** (best quality): `pip install marker-pdf`
  - **pdftotext** (fast): `brew install poppler`
  - **pypdf** (fallback): `pip install pypdf`
- [ ] (Optional - Proxy fallback) [agent-fetch](https://github.com/teng-lin/agent-fetch)
  ```bash
  npx agent-fetch --help  # No pre-install needed, npx auto-downloads
  ```
- [ ] (Optional - Feishu docs) Environment variables `FEISHU_APP_ID` and `FEISHU_APP_SECRET`
  ```bash
  echo $FEISHU_APP_ID  # Verify configured
  ```

### Installation

```bash
npx skills add joeseesun/qiaomu-markdown-proxy
```

Verify:
```bash
ls ~/.agents/skills/qiaomu-markdown-proxy/SKILL.md
```

### Usage

Just send Claude a URL:

- "Read this article: https://example.com/post"
- "Fetch this tweet: https://x.com/user/status/123456"
- "Read this WeChat article: https://mp.weixin.qq.com/s/abc123"
- "Convert this Feishu doc to Markdown: https://xxx.feishu.cn/docx/xxxxxxxx"
- "Read this WeChat post, then rewrite it in first person: https://mp.weixin.qq.com/s/abc123"

### Proxy Priority

1. **r.jina.ai** — Most complete content, preserves image links
2. **defuddle.md** — Cleaner output with YAML frontmatter
3. **[agent-fetch](https://github.com/teng-lin/agent-fetch)** — Local tool, no network proxy needed
4. **defuddle CLI** — Local CLI, good for standard web pages

### Feishu/Lark Document Support

Built-in `fetch_feishu.py` script fetches documents via Feishu Open API and auto-converts to Markdown:

- Supports new docs (docx), legacy docs (doc), and wiki pages
- Auto-parses document blocks into Markdown format
- Supports headings, lists, code blocks, quotes, todos, equations, images, etc.
- Requires `FEISHU_APP_ID` and `FEISHU_APP_SECRET` environment variables
- App needs `docx:document:readonly` permission

### Troubleshooting

| Issue | Solution |
|-------|----------|
| WeChat scraping fails | Run `playwright install chromium` to install browser |
| WeChat returns `环境异常` | Use `scripts/fetch_weixin.sh`; it rejects verification pages and falls back to Playwright |
| Python packages are missing | Install `uv`, or run `python3 -m pip install playwright beautifulsoup4 lxml` |
| Feishu returns permission error | Check `FEISHU_APP_ID` and `FEISHU_APP_SECRET` env vars, confirm app has document read permission |
| Feishu wiki page fails | Confirm app has `wiki:wiki:readonly` permission |
| r.jina.ai returns empty | Auto-falls back to defuddle.md (no action needed) |
| All proxies fail | URL may have strict auth restrictions, try `npx agent-fetch` |

### Credits

- [r.jina.ai](https://r.jina.ai) — Free URL-to-Markdown proxy by Jina AI
- [defuddle.md](https://defuddle.md) — Clean article extraction service
- [agent-fetch](https://github.com/teng-lin/agent-fetch) — Local URL content extraction tool
- [Playwright](https://playwright.dev/) — Browser automation for WeChat scraping
- [Feishu Open Platform](https://open.feishu.cn/) — Feishu Document API

---

<a name="中文"></a>
## 中文

### 功能

给 Claude 发一个 URL，自动抓取完整内容并转为 Markdown。支持六种内容类型的专用抓取：

如果最终目标是“读取后写稿、总结、翻译或分析”，本 Skill 负责先拿到可信原文，再把内容交给下游任务，同一轮继续完成，不会停在抓取结果。

| URL 类型 | 抓取方式 | 原因 |
|----------|---------|------|
| 微信公众号 (`mp.weixin.qq.com`) | 代理优先、Playwright 回退的专用入口 | 验证码页不会被误判为正文 |
| 飞书文档 (`feishu.cn/docx/`, `/wiki/`, `/docs/`) | 内置飞书 API 脚本 | 需要 API 认证，自动转 Markdown |
| YouTube | 专用 YouTube skill | 视频内容有专用工具链 |
| arXiv / Hugging Face 论文 | 内置 LaTeX 源码提取 | 保留章节、公式、图片和表格 |
| PDF（远程 URL 或本地文件） | 内置 PDF 提取（`extract_pdf.sh`） | 三级 fallback：marker-pdf → pdftotext → pypdf |
| 其他所有 URL | 代理级联 `fetch.sh`：r.jina.ai → defuddle.md → agent-fetch | 免费、无需 API key、内置内容验证 |

### 前置条件

- [ ] 已安装 [Claude Code](https://docs.anthropic.com/en/docs/claude-code)
- [ ] **curl**（macOS/Linux 自带）
- [ ] （可选 - 公众号抓取）Python 3.8+ 及 playwright
  ```bash
  python3 -m pip install playwright beautifulsoup4 lxml
  python3 -m playwright install chromium
  ```
- [ ] （可选 - PDF 提取）以下任一：
  - **marker-pdf**（最佳质量）：`pip install marker-pdf`
  - **pdftotext**（速度快）：`brew install poppler`
  - **pypdf**（兜底）：`pip install pypdf`
- [ ] （可选 - 代理降级）[agent-fetch](https://github.com/teng-lin/agent-fetch)
  ```bash
  npx agent-fetch --help  # 无需预装，npx 自动下载
  ```
- [ ] （可选 - 飞书抓取）环境变量 `FEISHU_APP_ID` 和 `FEISHU_APP_SECRET`
  ```bash
  echo $FEISHU_APP_ID  # 验证已配置
  ```

### 安装

```bash
npx skills add joeseesun/qiaomu-markdown-proxy
```

验证：
```bash
ls ~/.agents/skills/qiaomu-markdown-proxy/SKILL.md
```

### 你可以直接这样说

直接给 Claude 发 URL：

- "帮我读一下这篇文章：https://example.com/post"
- "抓取这条推文：https://x.com/user/status/123456"
- "读一下这篇公众号：https://mp.weixin.qq.com/s/abc123"
- "读取这篇公众号，然后用第一人称重写：https://mp.weixin.qq.com/s/abc123"
- "把这个飞书文档转成 Markdown：https://xxx.feishu.cn/docx/xxxxxxxx"
- "读一下这个飞书知识库页面：https://xxx.feishu.cn/wiki/xxxxxxxx"
- "提取这个 PDF：https://example.com/paper.pdf"
- "转换本地 PDF：/path/to/document.pdf"

维护者验证：

```bash
python3 ~/.agents/skills/qiaomu-meta-skill/scripts/validate_skill.py ~/.agents/skills/qiaomu-markdown-proxy
python3 -m unittest discover -s ~/.agents/skills/qiaomu-markdown-proxy/tests -p 'test_*.py'
```

### 代理优先级

1. **r.jina.ai** — 内容最完整，保留图片链接
2. **defuddle.md** — 输出更干净，带 YAML frontmatter
3. **[agent-fetch](https://github.com/teng-lin/agent-fetch)** — 本地工具，无需网络代理
4. **defuddle CLI** — 本地 CLI，适合普通网页

### 飞书文档支持

内置 `fetch_feishu.py` 脚本，通过飞书开放 API 抓取文档内容并自动转为 Markdown：

- 支持新版文档（docx）、旧版文档（doc）、知识库页面（wiki）
- 自动解析文档 blocks 并转换为 Markdown 格式
- 支持标题、列表、代码块、引用、待办、公式、图片等
- 需要飞书应用的 `FEISHU_APP_ID` 和 `FEISHU_APP_SECRET` 环境变量
- 应用需要 `docx:document:readonly` 权限

### 常见问题

| 问题 | 解决方法 |
|------|----------|
| 公众号返回“环境异常” | 使用 `scripts/fetch_weixin.sh`，它会识别验证页并回退 Playwright |
| 公众号浏览器抓取失败 | 运行 `python3 -m playwright install chromium` 安装浏览器 |
| 缺少 Python 依赖 | 安装 `uv`，或运行 `python3 -m pip install playwright beautifulsoup4 lxml` |
| 飞书文档返回权限错误 | 检查 `FEISHU_APP_ID` 和 `FEISHU_APP_SECRET` 环境变量，确认应用有文档读取权限 |
| 飞书知识库页面抓取失败 | 确认应用有 `wiki:wiki:readonly` 权限 |
| PDF 提取失败 | 安装任一工具：`pip install marker-pdf`、`brew install poppler`、`pip install pypdf` |
| r.jina.ai 返回空内容 | 自动降级到 defuddle.md（无需手动操作） |
| 所有代理都失败 | URL 可能有严格认证限制，尝试 `npx agent-fetch` |

### 致谢

- [r.jina.ai](https://r.jina.ai) — Jina AI 提供的免费 URL 转 Markdown 代理
- [defuddle.md](https://defuddle.md) — 干净的文章提取服务
- [agent-fetch](https://github.com/teng-lin/agent-fetch) — 本地 URL 内容提取工具
- [Playwright](https://playwright.dev/) — 微信公众号抓取的浏览器自动化
- [飞书开放平台](https://open.feishu.cn/) — 飞书文档 API

## 许可与边界

- 抓取会访问用户提供的外部 URL，请遵守目标网站的访问规则与内容版权。
- 飞书读取需要用户自己的应用凭据，仓库不会保存任何 token 或 cookie。
- 默认只读取内容；保存源文件、后续发布或账号操作由用户请求和下游工具决定。

## License

MIT. Copyright (c) 向阳乔木.
