#!/usr/bin/env python3
"""Fetch Feishu/Lark document as Markdown. Standalone script using Feishu Open API."""

import argparse
import json
import os
import re
import requests

FEISHU_API_BASE = "https://open.feishu.cn/open-apis"


def get_tenant_access_token():
    """获取 tenant_access_token"""
    app_id = os.environ.get("FEISHU_APP_ID")
    app_secret = os.environ.get("FEISHU_APP_SECRET")
    if not app_id or not app_secret:
        return None, "环境变量 FEISHU_APP_ID 或 FEISHU_APP_SECRET 未设置"

    url = f"{FEISHU_API_BASE}/auth/v3/tenant_access_token/internal"
    resp = requests.post(url, json={"app_id": app_id, "app_secret": app_secret})
    data = resp.json()
    if data.get("code") != 0:
        return None, f"获取 token 失败: {data.get('msg', resp.text)}"
    return data["tenant_access_token"], None


def parse_feishu_url(url):
    """从飞书 URL 解析 document_id 和文档类型"""
    patterns = [
        (r"feishu\.cn/docx/([A-Za-z0-9]+)", "docx"),
        (r"feishu\.cn/docs/([A-Za-z0-9]+)", "doc"),
        (r"feishu\.cn/wiki/([A-Za-z0-9]+)", "wiki"),
        (r"larksuite\.com/docx/([A-Za-z0-9]+)", "docx"),
        (r"larksuite\.com/docs/([A-Za-z0-9]+)", "doc"),
        (r"larksuite\.com/wiki/([A-Za-z0-9]+)", "wiki"),
    ]
    for pattern, doc_type in patterns:
        m = re.search(pattern, url)
        if m:
            return m.group(1), doc_type
    return None, None


def get_document_info(token, doc_id):
    """获取文档元信息（标题等）"""
    url = f"{FEISHU_API_BASE}/docx/v1/documents/{doc_id}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers)
    data = resp.json()
    if data.get("code") == 0:
        return data.get("data", {}).get("document", {})
    return {}


def get_document_blocks(token, doc_id):
    """获取文档所有 blocks"""
    url = f"{FEISHU_API_BASE}/docx/v1/documents/{doc_id}/blocks"
    headers = {"Authorization": f"Bearer {token}"}
    all_blocks = []
    page_token = None

    while True:
        params = {"page_size": 500}
        if page_token:
            params["page_token"] = page_token
        resp = requests.get(url, headers=headers, params=params)
        data = resp.json()
        if data.get("code") != 0:
            return None, f"获取 blocks 失败: {data.get('msg', resp.text)}"

        items = data.get("data", {}).get("items", [])
        all_blocks.extend(items)

        if not data.get("data", {}).get("has_more", False):
            break
        page_token = data["data"].get("page_token")

    return all_blocks, None


def get_wiki_node(token, wiki_token):
    """获取知识库节点信息，返回实际的 obj_token 和 obj_type"""
    url = f"{FEISHU_API_BASE}/wiki/v2/spaces/get_node"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(url, headers=headers, params={"token": wiki_token})
    data = resp.json()
    if data.get("code") == 0:
        node = data.get("data", {}).get("node", {})
        return node.get("obj_token"), node.get("obj_type")
    return None, None


def extract_text_from_elements(elements):
    """从 text_run / mention_user 等元素中提取文本"""
    if not elements:
        return ""
    parts = []
    for el in elements:
        if "text_run" in el:
            tr = el["text_run"]
            text = tr.get("content", "")
            style = tr.get("text_element_style", {})
            if style.get("bold"):
                text = f"**{text}**"
            if style.get("italic"):
                text = f"*{text}*"
            if style.get("strikethrough"):
                text = f"~~{text}~~"
            if style.get("inline_code"):
                text = f"`{text}`"
            if style.get("link", {}).get("url"):
                import urllib.parse
                link_url = urllib.parse.unquote(style["link"]["url"])
                text = f"[{text}]({link_url})"
            parts.append(text)
        elif "mention_user" in el:
            parts.append(f"@{el['mention_user'].get('user_id', 'user')}")
        elif "equation" in el:
            parts.append(f"${el['equation'].get('content', '')}$")
    return "".join(parts)


def blocks_to_markdown(blocks):
    """将飞书 blocks 转为 Markdown"""
    lines = []
    ordered_list_counter = {}  # parent_id -> counter

    for block in blocks:
        block_type = block.get("block_type")
        parent_id = block.get("parent_id", "")

        # 1 = Page, 2 = Text, 3 = Heading1, 4 = Heading2, ..., 9 = Heading7+
        # 10 = BulletList, 11 = OrderedList, 12 = Code, 13 = Quote
        # 14 = Equation, 15 = Todo, 16 = Divider
        # 17 = Image, 18 = TableCell, 19 = Table
        # 22 = Callout, 23 = ChatCard, 27 = Grid, 28 = GridColumn

        if block_type == 2:  # Text
            text_data = block.get("text", {})
            text = extract_text_from_elements(text_data.get("elements", []))
            if text.strip():
                lines.append(text)
            else:
                lines.append("")

        elif block_type in (3, 4, 5, 6, 7, 8, 9):  # Heading 1-7
            level = block_type - 2
            heading_data = block.get("heading" + str(level), {}) or block.get("heading", {})
            # Try multiple key formats
            for key in [f"heading{level}", "heading"]:
                if key in block:
                    heading_data = block[key]
                    break
            text = extract_text_from_elements(heading_data.get("elements", []))
            lines.append(f"{'#' * level} {text}")

        elif block_type == 10:  # Bullet list
            text_data = block.get("bullet", {})
            text = extract_text_from_elements(text_data.get("elements", []))
            lines.append(f"- {text}")

        elif block_type == 11:  # Ordered list
            text_data = block.get("ordered", {})
            text = extract_text_from_elements(text_data.get("elements", []))
            counter = ordered_list_counter.get(parent_id, 0) + 1
            ordered_list_counter[parent_id] = counter
            lines.append(f"{counter}. {text}")

        elif block_type == 12:  # Code block
            code_data = block.get("code", {})
            text = extract_text_from_elements(code_data.get("elements", []))
            lang = code_data.get("style", {}).get("language", "")
            # Map language codes
            lang_map = {1: "plaintext", 2: "abap", 3: "ada", 4: "apache", 5: "apex",
                        6: "assembly", 7: "bash", 8: "c", 9: "csharp", 10: "cpp",
                        11: "clojure", 12: "cmake", 13: "coffeescript", 14: "css",
                        15: "d", 16: "dart", 17: "delphi", 18: "django", 19: "dockerfile",
                        20: "elixir", 21: "elm", 22: "erlang", 23: "fortran",
                        24: "fsharp", 25: "go", 26: "graphql", 27: "groovy", 28: "haskell",
                        29: "html", 30: "http", 31: "java", 32: "javascript",
                        33: "json", 34: "julia", 35: "kotlin", 36: "latex", 37: "lisp",
                        38: "lua", 39: "makefile", 40: "markdown", 41: "matlab",
                        42: "nginx", 43: "objectivec", 44: "ocaml", 45: "perl",
                        46: "php", 47: "powershell", 48: "properties", 49: "protobuf",
                        50: "python", 51: "r", 52: "ruby", 53: "rust", 54: "scala",
                        55: "scheme", 56: "scss", 57: "shell", 58: "sql", 59: "swift",
                        60: "thrift", 61: "toml", 62: "typescript", 63: "vbnet",
                        64: "verilog", 65: "vhdl", 66: "visual_basic", 67: "vue",
                        68: "xml", 69: "yaml"}
            lang_str = lang_map.get(lang, "") if isinstance(lang, int) else str(lang)
            lines.append(f"```{lang_str}")
            lines.append(text)
            lines.append("```")

        elif block_type == 13:  # Quote
            text_data = block.get("quote", {})
            text = extract_text_from_elements(text_data.get("elements", []))
            lines.append(f"> {text}")

        elif block_type == 14:  # Equation block
            eq_data = block.get("equation", {})
            text = extract_text_from_elements(eq_data.get("elements", []))
            lines.append(f"$$\n{text}\n$$")

        elif block_type == 15:  # Todo
            todo_data = block.get("todo", {})
            text = extract_text_from_elements(todo_data.get("elements", []))
            done = todo_data.get("style", {}).get("done", False)
            checkbox = "[x]" if done else "[ ]"
            lines.append(f"- {checkbox} {text}")

        elif block_type == 16:  # Divider
            lines.append("---")

        elif block_type == 17:  # Image
            image_data = block.get("image", {})
            token_val = image_data.get("token", "")
            lines.append(f"![image](feishu-image://{token_val})")

        elif block_type == 22:  # Callout
            callout_data = block.get("callout", {})
            # Callout is a container, children will be processed separately
            emoji = callout_data.get("emoji_id", "")
            if emoji:
                lines.append(f"> {emoji}")

        elif block_type == 1:  # Page (root), skip
            pass

        else:
            # Unknown block type, try to extract any text
            for key in block:
                if isinstance(block[key], dict) and "elements" in block[key]:
                    text = extract_text_from_elements(block[key]["elements"])
                    if text.strip():
                        lines.append(text)
                    break

    return "\n\n".join(lines)


def fetch_feishu_doc(url_or_id):
    """主函数：获取飞书文档并转为 Markdown"""
    # 解析 URL
    doc_id, doc_type = parse_feishu_url(url_or_id)
    if not doc_id:
        # 可能直接传了 doc_token
        doc_id = url_or_id
        doc_type = "docx"

    # 获取 token
    token, err = get_tenant_access_token()
    if err:
        return {"error": err}

    # Wiki 需要先获取实际文档 ID
    if doc_type == "wiki":
        real_id, real_type = get_wiki_node(token, doc_id)
        if real_id:
            doc_id = real_id
            doc_type = real_type or "docx"
        else:
            return {"error": f"无法获取知识库节点信息: {doc_id}"}

    # 获取文档信息
    doc_info = get_document_info(token, doc_id)
    title = doc_info.get("title", "")

    # 获取 blocks
    blocks, err = get_document_blocks(token, doc_id)
    if err:
        return {"error": err}

    # 转换为 Markdown
    content = blocks_to_markdown(blocks)

    return {
        "title": title,
        "document_id": doc_id,
        "url": url_or_id,
        "content": content,
    }


def format_as_markdown(result):
    """格式化为 Markdown 文档"""
    if "error" in result:
        return f"Error: {result['error']}"

    parts = ["---"]
    if result.get("title"):
        parts.append(f'title: "{result["title"]}"')
    parts.append(f'document_id: "{result["document_id"]}"')
    if result.get("url"):
        parts.append(f'url: "{result["url"]}"')
    parts.append("---")
    parts.append("")
    if result.get("title"):
        parts.append(f"# {result['title']}")
        parts.append("")
    parts.append(result.get("content", ""))
    return "\n".join(parts)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch a Feishu/Lark document as Markdown. Requires FEISHU_APP_ID and FEISHU_APP_SECRET."
    )
    parser.add_argument("url_or_token", help="Feishu/Lark document URL or document token")
    parser.add_argument("--json", action="store_true", dest="use_json", help="Return structured JSON")
    args = parser.parse_args()

    result = fetch_feishu_doc(args.url_or_token)

    if args.use_json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(format_as_markdown(result))
