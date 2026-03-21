# markdown-proxy

> Convert any URL to clean Markdown, with built-in support for login-required pages (X/Twitter, WeChat, Feishu/Lark docs, etc.)

> 将任意 URL 转为干净的 Markdown，支持需要登录的页面（X/Twitter、微信公众号、飞书文档等）。

**[English](#english) | [中文](#中文)**

---

<a name="english"></a>
## English

### Features

Send any URL to Claude, and it automatically fetches the full content as Markdown. Four special platforms have dedicated extraction:

| URL Type | Method | Why |
|----------|--------|-----|
| WeChat Articles (`mp.weixin.qq.com`) | Built-in Playwright script | Anti-scraping protection requires headless browser |
| Feishu/Lark Docs (`feishu.cn`, `larksuite.com`) | Built-in Feishu API script | Requires API authentication, auto-converts to Markdown |
| YouTube | Dedicated YouTube skill | Video content has its own toolchain |
| All other URLs | Proxy cascade: r.jina.ai → defuddle.md → agent-fetch | Free, no API key needed |

### Prerequisites

- [ ] [Claude Code](https://docs.anthropic.com/en/docs/claude-code) installed
- [ ] **curl** (built-in on macOS/Linux)
- [ ] (WeChat scraping) Python 3.8+ with playwright
  ```bash
  pip install playwright beautifulsoup4 lxml
  playwright install chromium
  ```
- [ ] (Proxy fallback) [agent-fetch](https://github.com/teng-lin/agent-fetch) — local fallback when online proxies fail
  ```bash
  npx agent-fetch --help  # No pre-install needed, npx auto-downloads
  ```
- [ ] (Feishu docs) Environment variables `FEISHU_APP_ID` and `FEISHU_APP_SECRET`
  ```bash
  echo $FEISHU_APP_ID  # Verify configured
  ```

### Installation

```bash
npx skills add joeseesun/markdown-proxy
```

Verify:
```bash
ls ~/.claude/skills/markdown-proxy/SKILL.md
```

### Usage

Just send Claude a URL:

- "Read this article: https://example.com/post"
- "Fetch this tweet: https://x.com/user/status/123456"
- "Read this WeChat article: https://mp.weixin.qq.com/s/abc123"
- "Convert this Feishu doc to Markdown: https://xxx.feishu.cn/docx/xxxxxxxx"

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

给 Claude 发一个 URL，自动抓取完整内容并转为 Markdown。支持四种特殊平台的专用抓取：

| URL 类型 | 抓取方式 | 原因 |
|----------|---------|------|
| 微信公众号 (`mp.weixin.qq.com`) | 内置 Playwright 脚本 | 公众号有反爬，需无头浏览器 |
| 飞书文档 (`feishu.cn/docx/`, `/wiki/`, `/docs/`) | 内置飞书 API 脚本 | 需要 API 认证，自动转 Markdown |
| YouTube | 专用 YouTube skill | 视频内容有专用工具链 |
| 其他所有 URL | 代理级联：r.jina.ai → defuddle.md → agent-fetch | 免费、无需 API key |

### 前置条件

- [ ] 已安装 [Claude Code](https://docs.anthropic.com/en/docs/claude-code)
- [ ] **curl**（macOS/Linux 自带）
- [ ] （公众号抓取）Python 3.8+ 及 playwright
  ```bash
  pip install playwright beautifulsoup4 lxml
  playwright install chromium
  ```
- [ ] （代理降级）[agent-fetch](https://github.com/teng-lin/agent-fetch) — 当在线代理都失败时的本地回退工具
  ```bash
  npx agent-fetch --help  # 无需预装，npx 自动下载
  ```
- [ ] （飞书抓取）环境变量 `FEISHU_APP_ID` 和 `FEISHU_APP_SECRET`
  ```bash
  echo $FEISHU_APP_ID  # 验证已配置
  ```

### 安装

```bash
npx skills add joeseesun/markdown-proxy
```

验证：
```bash
ls ~/.claude/skills/markdown-proxy/SKILL.md
```

### 使用示例

直接给 Claude 发 URL：

- "帮我读一下这篇文章：https://example.com/post"
- "抓取这条推文：https://x.com/user/status/123456"
- "读一下这篇公众号：https://mp.weixin.qq.com/s/abc123"
- "把这个飞书文档转成 Markdown：https://xxx.feishu.cn/docx/xxxxxxxx"
- "读一下这个飞书知识库页面：https://xxx.feishu.cn/wiki/xxxxxxxx"

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
|------|---------.|
| 公众号抓取失败 | 运行 `playwright install chromium` 安装浏览器 |
| 飞书文档返回权限错误 | 检查 `FEISHU_APP_ID` 和 `FEISHU_APP_SECRET` 环境变量，确认应用有文档读取权限 |
| 飞书知识库页面抓取失败 | 确认应用有 `wiki:wiki:readonly` 权限 |
| r.jina.ai 返回空内容 | 自动降级到 defuddle.md（无需手动操作） |
| 所有代理都失败 | URL 可能有严格认证限制，尝试 `npx agent-fetch` |

### 致谢

- [r.jina.ai](https://r.jina.ai) — Jina AI 提供的免费 URL 转 Markdown 代理
- [defuddle.md](https://defuddle.md) — 干净的文章提取服务
- [agent-fetch](https://github.com/teng-lin/agent-fetch) — 本地 URL 内容提取工具
- [Playwright](https://playwright.dev/) — 微信公众号抓取的浏览器自动化
- [飞书开放平台](https://open.feishu.cn/) — 飞书文档 API

---

## 关注作者

- **X (Twitter)**: [@vista8](https://x.com/vista8)
- **微信公众号「向阳乔木推荐看」**

<p align="center">
  <img src="https://github.com/joeseesun/terminal-boost/raw/main/assets/wechat-qr.jpg?raw=true" alt="向阳乔木推荐看公众号二维码" width="300">
</p>
