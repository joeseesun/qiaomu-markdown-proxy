# Prior-Art Research

## Scope decision

This iteration fixes a concrete false-negative routing regression in an existing public Skill. Broad catalog research was not run because the user identified the canonical package and the failure was reproduced locally. The change passed the generalization gate as a recurring domain-neutral behavior: when a user provides a specialist URL and asks for a downstream deliverable, source extraction must be selected first and must continue into the downstream task.

## References inspected

### `qiaomu-markdown-proxy` 2.0.0

- Keep: specialist URL routing, proxy cascade, built-in WeChat and Feishu extractors.
- Adapt: make the frontmatter description single-line and intent-rich so catalogs can surface it reliably.
- Reject: conflicting WeChat instructions and unconditional stopping after extraction.

### `content-parser`

- Keep: explicit URL extraction contract and structured metadata.
- Reject for this route: generic API extraction should not outrank a local specialist that already handles WeChat and Feishu.

### `qiaomu-url2blog`

- Keep: source extraction as an explicit prerequisite for downstream writing.
- Adapt: generalize that handoff to any summary, rewrite, translation, podcast, or analysis request.
- Reject: publishing-specific media and draft workflows are outside this Skill.

## Live evidence

- Generic web open could not read the supplied WeChat URL.
- The old proxy validator accepted a WeChat “环境异常” verification page as valid content.
- The bundled Playwright extractor successfully returned the article title, author, publish date, and body when dependencies were supplied through `uv`.

Catalog popularity and third-party rating evidence: `missing evidence`, intentionally not claimed for this targeted regression fix.
