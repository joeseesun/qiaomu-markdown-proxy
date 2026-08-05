# Qiaomu Markdown Proxy 2.1.0 Creation Handoff

## Result

- Skill: `qiaomu-markdown-proxy` 2.1.0
- Job: select the correct extractor for user-provided URLs, return clean Markdown, and continue into downstream writing or analysis when requested
- Mode: Governed, because the public Skill performs network access and may write extracted files

## Reference skills studied

- `qiaomu-markdown-proxy` 2.0.0: retained its specialist routers and local scripts; corrected discovery, contradictory WeChat routing, and composite-task behavior.
- `content-parser`: learned from its explicit extraction contract; rejected it as the first route for WeChat and Feishu because the local specialist has narrower domain knowledge.
- `qiaomu-url2blog`: learned from its explicit extract-before-write pipeline; generalized that handoff beyond blog publishing.

## Generalization decision

- Core mechanism: a specialist URL extractor should run before generic fetchers when the URL pattern is known.
- Core mechanism: “read this URL, then write/summarize/analyze” is one composite request; extraction must not stop the workflow.
- Eval fixture: the supplied WeChat article and its verification-wall failure are retained only as runtime evidence, not as topic-specific instructions.

## Advantages and limits

- `design advantage`: the single-line description names user intent, high-value URL families, downstream actions, and exclusions, reducing overlap with generic content parsers.
- `validated advantage`: trigger regression cases cover WeChat composite writing, extraction-only, Feishu, PDF, arXiv, X/Twitter, generic webpages, YouTube boundaries, pasted text, and pure search.
- `validated advantage`: the proxy validator rejects WeChat verification walls, and unit tests cover both rejection and valid article-shaped Markdown.
- `validated advantage`: the live WeChat wrapper returned real article metadata and body after proxy rejection and Playwright fallback.
- `hypothesis`: the single-line YAML description will display more reliably in every host catalog; cross-host catalog rendering remains `missing evidence` until observed after installation.
- Provider or human comparative output review: `missing evidence`; this release claims routing and extraction reliability only.

## Rollback and trust boundary

- Rollback: revert the 2.1.0 release or call `scripts/fetch.sh` / `scripts/fetch_weixin.py` directly.
- Trust: only user-provided URLs and documented proxy services are contacted. Remote content is treated as data and is never executed.
- Secrets: Feishu credentials remain user-owned environment variables and are never written to the package.
