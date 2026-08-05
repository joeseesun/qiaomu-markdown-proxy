#!/usr/bin/env python3
"""Fetch WeChat (公众号) article as Markdown. Standalone script using Playwright + BeautifulSoup."""

import argparse
import json
import asyncio
import re

async def fetch_weixin_article(url: str) -> dict:
    """Fetch and parse a WeChat article, return dict with title, author, publish_time, content."""
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        return {"error": "playwright not installed. Run: pip install playwright && playwright install chromium"}

    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return {"error": "beautifulsoup4 not installed. Run: pip install beautifulsoup4 lxml"}

    html = None
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_selector("#js_content", timeout=15000)
            html = await page.content()
        except Exception as e:
            await browser.close()
            return {"error": f"Failed to load page: {e}"}
        await browser.close()

    if not html:
        return {"error": "No HTML content retrieved"}

    soup = BeautifulSoup(html, "lxml")

    # Extract title
    title_el = soup.select_one("#activity-name")
    title = title_el.get_text(strip=True) if title_el else ""

    # Extract author
    author_el = soup.select_one("#js_author_name") or soup.select_one(".rich_media_meta_text")
    author = author_el.get_text(strip=True) if author_el else ""

    # Extract publish time
    time_el = soup.select_one("#publish_time")
    publish_time = time_el.get_text(strip=True) if time_el else ""

    # Extract content
    content_el = soup.select_one("#js_content")
    if not content_el:
        return {"error": "Could not find article content (#js_content)"}

    # Convert to markdown-like text
    # Remove scripts and styles
    for tag in content_el.find_all(["script", "style"]):
        tag.decompose()

    # Process images - extract src or data-src
    for img in content_el.find_all("img"):
        src = img.get("data-src") or img.get("src") or ""
        if src:
            img.replace_with(f"\n![image]({src})\n")
        else:
            img.decompose()

    # Get text with basic formatting
    lines = []
    for element in content_el.find_all(["p", "h1", "h2", "h3", "h4", "section", "blockquote"]):
        text = element.get_text(strip=True)
        if not text:
            continue
        tag = element.name
        if tag in ("h1", "h2", "h3", "h4"):
            prefix = "#" * int(tag[1])
            lines.append(f"{prefix} {text}")
        elif tag == "blockquote":
            lines.append(f"> {text}")
        else:
            lines.append(text)

    content = "\n\n".join(lines)

    # If structured extraction got nothing, fall back to plain text
    if not content.strip():
        content = content_el.get_text("\n", strip=True)

    return {
        "title": title,
        "author": author,
        "publish_time": publish_time,
        "content": content,
        "url": url,
    }


def format_as_markdown(result: dict) -> str:
    """Format result dict as a Markdown document."""
    if "error" in result:
        return f"Error: {result['error']}"

    parts = ["---"]
    if result.get("title"):
        parts.append(f"title: \"{result['title']}\"")
    if result.get("author"):
        parts.append(f"author: \"{result['author']}\"")
    if result.get("publish_time"):
        parts.append(f"date: \"{result['publish_time']}\"")
    parts.append(f"url: \"{result['url']}\"")
    parts.append("---")
    parts.append("")
    if result.get("title"):
        parts.append(f"# {result['title']}")
        parts.append("")
    parts.append(result.get("content", ""))
    return "\n".join(parts)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch a WeChat public-account article as Markdown.")
    parser.add_argument("url", help="WeChat article URL under mp.weixin.qq.com")
    parser.add_argument("--json", action="store_true", dest="use_json", help="Return structured JSON")
    args = parser.parse_args()

    result = asyncio.run(fetch_weixin_article(args.url))

    if args.use_json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(format_as_markdown(result))
