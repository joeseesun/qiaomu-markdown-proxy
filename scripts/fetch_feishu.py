#!/usr/bin/env python3
"""Fetch Feishu/Lark document as Markdown. Standalone script using Feishu Open API."""

import sys
import json
import os
import re
import requests

FEISHU_API_BASE = "https://open.feishu.cn/open-apis"


def download_image(file_token, save_dir, access_token):
    """下载飞书图片到本地，返回文件名或 None"""
    url = f"{FEISHU_API_BASE}/drive/v1/medias/{file_token}/download"
    headers = {"Authorization": f"Bearer {access_token}"}
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 200 and resp.content:
            content_type = resp.headers.get("Content-Type", "")
            ext = "png"
            if "jpeg" in content_type or "jpg" in content_type:
                ext = "jpg"
            elif "gif" in content_type:
                ext = "gif"
            elif "webp" in content_type:
                ext = "webp"

            os.makedirs(save_dir, exist_ok=True)
            filename = f"{file_token}.{ext}"
            filepath = os.path.join(save_dir, filename)
            with open(filepath, "wb") as f:
                f.write(resp.content)
            return filename
    except Exception as e:
        print(f"图片下载失败 {file_token}: {e}", file=sys.stderr)
    return None


def download_all_images(blocks, save_dir, access_token):
    """下载所有图片，返回 {token: filename} 映射"""
    image_map = {}
    for block in blocks:
        if "image" in block:
            image_data = block["image"]
            file_token = image_data.get("token", "")
            if file_token and file_token not in image_map:
                filename = download_image(file_token, save_dir, access_token)
                if filename:
                    image_map[file_token] = filename
    return image_map


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


def render_cell_content(cell_block, block_index, image_map=None, image_dir_name=None):
    """渲染表格单元格内容，将子块拼接为单行文本"""
    children_ids = cell_block.get("children", [])
    parts = []
    for child_id in children_ids:
        child = block_index.get(child_id)
        if not child:
            continue
        # 检查是否有图片数据
        if "image" in child:
            image_data = child["image"]
            token_val = image_data.get("token", "")
            if image_map and token_val in image_map:
                parts.append(f"![image]({image_dir_name}/{image_map[token_val]})")
            elif token_val:
                parts.append(f"![image](feishu-image://{token_val})")
        else:
            # 尝试提取文本
            for key in child:
                if isinstance(child[key], dict) and "elements" in child[key]:
                    text = extract_text_from_elements(child[key]["elements"])
                    if text.strip():
                        parts.append(text)
                    break
    return " ".join(parts).replace("|", "\\|")


def render_table(table_block, block_index, image_map=None, image_dir_name=None):
    """将飞书 Table block 渲染为 Markdown 表格"""
    table_data = table_block.get("table", {})
    table_prop = table_data.get("property", {})
    row_count = table_prop.get("row_size", 0)
    col_count = table_prop.get("column_size", 0)
    # 使用 cells 列表（按行优先排列）
    cell_ids = table_data.get("cells", []) or table_block.get("children", [])

    if not cell_ids or row_count == 0 or col_count == 0:
        return ""

    rows = []
    for r in range(row_count):
        row = []
        for c in range(col_count):
            idx = r * col_count + c
            if idx < len(cell_ids):
                cell_id = cell_ids[idx]
                cell_block = block_index.get(cell_id, {})
                cell_text = render_cell_content(cell_block, block_index, image_map, image_dir_name)
                row.append(cell_text if cell_text else " ")
            else:
                row.append(" ")
        rows.append(row)

    # 构建 Markdown 表格
    md_lines = []
    if rows:
        md_lines.append("| " + " | ".join(rows[0]) + " |")
        md_lines.append("| " + " | ".join(["---"] * col_count) + " |")
        for row in rows[1:]:
            md_lines.append("| " + " | ".join(row) + " |")

    return "\n".join(md_lines)


LANG_MAP = {1: "plaintext", 2: "abap", 3: "ada", 4: "apache", 5: "apex",
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


def detect_block_kind(block):
    """基于实际数据 key 判断块类型，比 block_type 编号更可靠"""
    keys = set(block.keys()) - {"block_id", "block_type", "parent_id", "children", "comment_ids"}
    if "page" in keys:
        return "page"
    if "table" in keys:
        return "table"
    if "table_cell" in keys:
        return "table_cell"
    if "image" in keys:
        return "image"
    if "grid" in keys:
        return "grid"
    if "grid_column" in keys:
        return "grid_column"
    # heading1-9
    for i in range(1, 10):
        if f"heading{i}" in keys:
            return f"heading{i}"
    if "text" in keys:
        return "text"
    if "bullet" in keys:
        return "bullet"
    if "ordered" in keys:
        return "ordered"
    if "code" in keys:
        return "code"
    if "quote" in keys:
        return "quote"
    if "equation" in keys:
        return "equation"
    if "todo" in keys:
        return "todo"
    if "divider" in keys:
        return "divider"
    if "callout" in keys:
        return "callout"
    return "unknown"


def blocks_to_markdown(blocks, image_map=None, image_dir_name=None):
    """将飞书 blocks 转为 Markdown"""
    lines = []
    ordered_list_counter = {}  # parent_id -> counter

    # 建立 block_id -> block 索引，用于表格渲染
    block_index = {b.get("block_id"): b for b in blocks if b.get("block_id")}
    # 收集所有表格/分栏子块 ID，避免重复渲染
    container_child_ids = set()
    for b in blocks:
        kind = detect_block_kind(b)
        if kind in ("table", "grid"):
            container_child_ids.update(b.get("children", []))
            for child_id in b.get("children", []):
                child = block_index.get(child_id, {})
                container_child_ids.update(child.get("children", []))

    for block in blocks:
        block_id = block.get("block_id", "")
        parent_id = block.get("parent_id", "")

        # 跳过已作为容器子块处理的 block
        if block_id in container_child_ids:
            continue

        kind = detect_block_kind(block)

        if kind == "text":
            text_data = block.get("text", {})
            text = extract_text_from_elements(text_data.get("elements", []))
            if text.strip():
                lines.append(text)
            else:
                lines.append("")

        elif kind.startswith("heading"):
            level = int(kind[-1])  # heading1 -> 1
            heading_data = block.get(kind, {})
            text = extract_text_from_elements(heading_data.get("elements", []))
            lines.append(f"{'#' * level} {text}")

        elif kind == "bullet":
            text_data = block.get("bullet", {})
            text = extract_text_from_elements(text_data.get("elements", []))
            lines.append(f"- {text}")

        elif kind == "ordered":
            text_data = block.get("ordered", {})
            text = extract_text_from_elements(text_data.get("elements", []))
            counter = ordered_list_counter.get(parent_id, 0) + 1
            ordered_list_counter[parent_id] = counter
            lines.append(f"{counter}. {text}")

        elif kind == "code":
            code_data = block.get("code", {})
            text = extract_text_from_elements(code_data.get("elements", []))
            lang = code_data.get("style", {}).get("language", "")
            lang_str = LANG_MAP.get(lang, "") if isinstance(lang, int) else str(lang)
            lines.append(f"```{lang_str}")
            lines.append(text)
            lines.append("```")

        elif kind == "quote":
            text_data = block.get("quote", {})
            text = extract_text_from_elements(text_data.get("elements", []))
            lines.append(f"> {text}")

        elif kind == "equation":
            eq_data = block.get("equation", {})
            text = extract_text_from_elements(eq_data.get("elements", []))
            lines.append(f"$$\n{text}\n$$")

        elif kind == "todo":
            todo_data = block.get("todo", {})
            text = extract_text_from_elements(todo_data.get("elements", []))
            done = todo_data.get("style", {}).get("done", False)
            checkbox = "[x]" if done else "[ ]"
            lines.append(f"- {checkbox} {text}")

        elif kind == "divider":
            lines.append("---")

        elif kind == "image":
            image_data = block.get("image", {})
            token_val = image_data.get("token", "")
            if image_map and token_val in image_map:
                lines.append(f"![image]({image_dir_name}/{image_map[token_val]})")
            elif token_val:
                lines.append(f"![image](feishu-image://{token_val})")

        elif kind == "table_cell":
            pass  # 由 table 统一渲染

        elif kind == "table":
            table_md = render_table(block, block_index, image_map, image_dir_name)
            if table_md:
                lines.append(table_md)

        elif kind == "callout":
            callout_data = block.get("callout", {})
            emoji = callout_data.get("emoji_id", "")
            if emoji:
                lines.append(f"> {emoji}")

        elif kind in ("page", "grid", "grid_column"):
            pass  # 容器块，跳过

        else:
            # Unknown block, try to extract any text
            for key in block:
                if isinstance(block[key], dict) and "elements" in block[key]:
                    text = extract_text_from_elements(block[key]["elements"])
                    if text.strip():
                        lines.append(text)
                    break

    return "\n\n".join(lines)


def fetch_feishu_doc(url_or_id, save_dir=None):
    """主函数：获取飞书文档并转为 Markdown。save_dir 非空时下载图片到本地。"""
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

    # 下载图片（save_dir 非空时）
    image_map = {}
    image_dir_name = None
    if save_dir and title:
        image_dir_name = f"{title}_images"
        image_save_dir = os.path.join(save_dir, image_dir_name)
        image_map = download_all_images(blocks, image_save_dir, token)

    # 转换为 Markdown
    content = blocks_to_markdown(blocks, image_map=image_map, image_dir_name=image_dir_name)

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
    if len(sys.argv) < 2:
        print("Usage: fetch_feishu.py <feishu_url_or_doc_token> [--json]", file=sys.stderr)
        print("  需要环境变量: FEISHU_APP_ID, FEISHU_APP_SECRET", file=sys.stderr)
        sys.exit(1)

    url = sys.argv[1]
    use_json = "--json" in sys.argv

    save_dir = os.path.expanduser("~/Downloads") if not use_json else None
    result = fetch_feishu_doc(url, save_dir=save_dir)

    if use_json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(format_as_markdown(result))
