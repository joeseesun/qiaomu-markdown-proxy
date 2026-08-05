#!/usr/bin/env python3
"""
Extract structured content from arXiv LaTeX source.

Input:  arXiv URL or arXiv ID
Output: Clean markdown with sections, figures (captions preserved),
        tables (structured data extracted), and equations.

Usage:
  extract_tex.py https://arxiv.org/abs/2411.xxxxx
  extract_tex.py 2411.xxxxx
  extract_tex.py --json 2411.xxxxx   # JSON output for programmatic use
  extract_tex.py --json --output-dir /tmp/arxiv-2605.05242 2411.xxxxx
                                     # Persist extracted files for media upload
"""

import argparse
import os
import re
import json
import tarfile
import tempfile
import shutil
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError
from html.parser import HTMLParser


def extract_arxiv_id(input_str):
    """Extract arXiv ID from URL or raw ID string."""
    # arXiv URLs: arxiv.org/abs/2411.12345 or arxiv.org/abs/2411.12345v1
    patterns = [
        r'arxiv\.org/abs/([0-9]+\.[0-9]+(?:v[0-9]+)?)',
        r'arxiv\.org/pdf/([0-9]+\.[0-9]+(?:v[0-9]+)?)',
        r'^([0-9]+\.[0-9]+(?:v[0-9]+)?)$',
    ]
    for pat in patterns:
        m = re.search(pat, input_str)
        if m:
            return m.group(1)
    raise ValueError(f"Cannot extract arXiv ID from: {input_str}")


def download_source(arxiv_id, tmpdir):
    """Download arXiv LaTeX source tarball."""
    url = f"https://arxiv.org/e-print/{arxiv_id}"
    req = Request(url, headers={"User-Agent": "arxiv-tex-extractor/1.0"})

    # Try tarball first, fall back to PDF-only
    try:
        resp = urlopen(req, timeout=30)
        data = resp.read()
    except URLError as e:
        # Maybe the paper only has PDF source
        raise RuntimeError(f"Failed to download source for {arxiv_id}: {e}")

    # Check if we got a gzipped tarball or PDF
    if data[:4] == b'\x1f\x8b':  # gzip magic
        tarpath = Path(tmpdir) / "source.tar.gz"
        tarpath.write_bytes(data)
        with tarfile.open(tarpath, "r:gz") as tar:
            tar.extractall(tmpdir, filter='data')
        tarpath.unlink()
        return True
    elif data[:4] == b'%PDF':  # PDF - no TeX source available
        return False
    else:
        # Try as uncompressed tar
        try:
            tarpath = Path(tmpdir) / "source.tar"
            tarpath.write_bytes(data)
            with tarfile.open(tarpath, "r") as tar:
                tar.extractall(tmpdir, filter='data')
            tarpath.unlink()
            return True
        except tarfile.ReadError:
            return False


def find_main_tex(tmpdir):
    """Find the main .tex file in extracted source."""
    tex_files = list(Path(tmpdir).glob("**/*.tex"))
    if not tex_files:
        return None

    # Priority: filename starts with "main", then largest file, then any
    main_names = [f for f in tex_files if f.stem.lower() in ("main", "paper", "article", "root")]
    if main_names:
        return main_names[0]

    # Look for \documentclass in files
    for f in tex_files:
        try:
            content = f.read_text(errors="replace")
            if r"\begin{document}" in content:
                return f
        except Exception:
            continue

    # Fallback: largest .tex file
    return max(tex_files, key=lambda f: f.stat().st_size)


def strip_latex_comments(content):
    """Remove LaTeX comments (lines starting with %, ignoring escaped %)."""
    lines = []
    for line in content.split("\n"):
        # Remove comments but not \%
        stripped = re.sub(r'(?<!\\)%.*$', '', line)
        if stripped.strip() or line.strip() == "":
            lines.append(stripped)
    return "\n".join(lines)


def resolve_inputs(content, base_dir):
    r"""Resolve \input{xxx} and \include{xxx} recursively."""
    base = Path(base_dir)

    def _resolve(match):
        fname = match.group(1).strip()
        # Try with and without .tex extension
        for suffix in (".tex", ""):
            path = base / (fname + suffix) if suffix else base / fname
            if not path.exists():
                # Try relative to base_dir subdirs
                for subdir in base.glob("*/"):
                    path = subdir / (fname + (suffix or ""))
                    if not path.exists():
                        continue
            if path.exists():
                try:
                    sub = path.read_text(errors="replace")
                    sub = strip_latex_comments(sub)
                    # Strip any \documentclass / preamble from sub-file
                    sub = re.sub(r'^.*?\\begin\{document\}', '', sub, flags=re.DOTALL)
                    sub = re.sub(r'\\end\{document\}.*$', '', sub, flags=re.DOTALL)
                    # Remove preamble commands
                    sub = re.sub(r'\\usepackage(\[.*?\])?\{.*?\}', '', sub)
                    return resolve_inputs(sub, base_dir)
                except Exception:
                    pass
        # File not found in any subdir of base_dir
        return f"[引用文件: {fname}]"

    # Handle \include and \input
    content = re.sub(r'\\(?:input|include)\{([^}]+)\}', _resolve, content)
    return content


def strip_body_formatting(text):
    """Remove LaTeX formatting commands from body text, keeping content."""
    # Order matters

    # Remove \begin{xxx}...\end{xxx} for layout-only envs (center, flushleft, etc)
    # Keep equation, figure, table, align, gather for structural parsing
    text = re.sub(r'\\begin\{(?:center|flushleft|flushright|itemize|enumerate)\}(.*?)\\end\{(?:center|flushleft|flushright|itemize|enumerate)\}',
                  lambda m: m.group(1), text, flags=re.DOTALL)

    # \item → bullet
    text = re.sub(r'\\item\s+', '\n- ', text)
    text = re.sub(r'\\item\[(.*?)\]', r'\n- **\1**: ', text)

    # \paragraph{...} → **...**\n
    text = re.sub(r'\\paragraph\{([^}]+)\}', r'\n**\1** ', text)
    # \textbf{...} → **...**
    text = re.sub(r'\\textbf\{([^}]+)\}', r'**\1**', text)
    # \emph{...} → *...*
    text = re.sub(r'\\emph\{([^}]+)\}', r'*\1*', text)
    # \textit{...} → *...*
    text = re.sub(r'\\textit\{([^}]+)\}', r'*\1*', text)

    # \url{...} → keep URL
    text = re.sub(r'\\url\{([^}]+)\}', r'\1', text)
    # \href{url}{text} → [text](url)
    text = re.sub(r'\\href\{([^}]+)\}\{([^}]+)\}', r'[\2](\1)', text)

    # \cite{...} → [文献引用]
    text = re.sub(r'\\cite(?:\[.*?\])?\{([^}]+)\}', '[文献]', text)
    text = re.sub(r'\\citep(?:\[.*?\])?\{([^}]+)\}', '[文献]', text)
    text = re.sub(r'\\citet(?:\[.*?\])?\{([^}]+)\}', '[文献]', text)

    # \ref{...} → [引用]
    text = re.sub(r'\\ref\{([^}]+)\}', r'[\1]', text)
    text = re.sub(r'\\label\{[^}]*\}', '', text)

    # \footnote{...} → (注: ...)
    text = re.sub(r'\\footnote\{([^}]*(?:\{[^}]*\}[^}]*)*)\}',
                  r'(注: \1)', text)

    # Remove common formatting commands (keep their content)
    simple_cmds = [
        'texttt', 'textrm', 'textsf', 'textsc', 'underline',
        'text', 'mbox', 'small', 'large', 'Large', 'LARGE',
        'tiny', 'footnotesize', 'normalsize', 'sc', 'bf',
        'it', 'tt', 'rm', 'sf',
    ]
    for cmd in simple_cmds:
        text = re.sub(rf'\\{cmd}\{{([^}}]*(?:\{{[^}}]*\}}[^}}]*)*)\}}',
                      r'\1', text)

    # Remove simple commands with no args
    text = re.sub(r'\\(?:noindent|newpage|clearpage|pagebreak|linebreak)\b', '', text)
    text = re.sub(r'\\vspace\{[^}]*\}', '', text)
    text = re.sub(r'\\hspace\{[^}]*\}', '', text)

    # Clean up LaTeX special chars
    text = text.replace(r'\&', '&')
    text = text.replace(r'\%', '%')
    text = text.replace(r'\#', '#')
    text = text.replace(r'\_', '_')
    text = text.replace(r'\~', '~')
    text = text.replace('~', ' ')
    text = text.replace(r'\ ', ' ')
    text = text.replace(r'``', '"')
    text = text.replace("''", '"')

    # Remove remaining simple \begin/\end for table-aligned envs (tabularx, etc.)
    text = re.sub(r'\\begin\{(?:tabular|tabularx|tabu|longtable|wraptable)\*?\}.*?\\end\{(?:tabular|tabularx|tabu|longtable|wraptable)\*?\}',
                  '[表格数据]', text, flags=re.DOTALL)

    # Clean up whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' +', ' ', text)

    return text


def _find_matching_brace(text, start):
    """Find the matching closing brace for an opening brace at start-1.
    Returns the index of the closing brace, or -1 if not found."""
    depth = 0
    i = start
    while i < len(text):
        if text[i] == '{':
            depth += 1
        elif text[i] == '}':
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return -1

def extract_title_author(content):
    """Extract title and author from LaTeX preamble."""
    title_match = re.search(r'\\title\{((?:[^{}]|\{[^{}]*\})*)\}', content, re.DOTALL)
    title = title_match.group(1).strip() if title_match else ""

    # Clean title
    title = strip_body_formatting(title)
    title = re.sub(r'\s+', ' ', title).strip()

    # Extract authors using brace counting (handles nested braces in \thanks etc.)
    authors = []
    author_cmd = r'\author{'
    author_start = content.find(author_cmd)
    if author_start >= 0:
        brace_start = author_start + len(author_cmd) - 1  # position OF '{'
        brace_end = _find_matching_brace(content, brace_start)
        if brace_end >= 0:
            author_text = content[brace_start + 1:brace_end]

            # Remove \thanks{...} blocks (content may span multiple lines)
            author_text = re.sub(r'\\thanks\{(?:[^{}]|\{[^{}]*\})*\}', '', author_text, flags=re.DOTALL)
            # Remove \footnotemark[...]
            author_text = re.sub(r'\\footnotemark\[.*?\]', '', author_text)
            # Normalize \And/\AND/\and to separator
            author_text = re.sub(r'\\AND\b', '|||', author_text)
            author_text = re.sub(r'\\And\b', '|||', author_text)
            author_text = re.sub(r'\\and\b', '|||', author_text)

            for part in author_text.split('|||'):
                # Strip LaTeX line breaks
                part = re.sub(r'\\\\[ \t]*', ' ', part)
                # Remove brace groups (affiliations, formatting)
                part = re.sub(r'\{[^}]*\}', '', part)
                # Remove remaining LaTeX commands
                part = re.sub(r'\\[a-zA-Z]+\b', '', part)
                part = strip_body_formatting(part.strip())
                part = re.sub(r'\s+', ' ', part).strip()
                # Remove emails, URLs, stray braces
                part = re.sub(r'\S+@\S+', '', part)
                part = re.sub(r'http\S+', '', part)
                part = re.sub(r'[{}]', '', part)
                part = part.strip(',. ')
                if part and len(part) > 3:
                    authors.append(part)

    return title, authors


def extract_abstract(content):
    """Extract abstract from document body."""
    m = re.search(r'\\begin\{abstract\}(.*?)\\end\{abstract\}', content, re.DOTALL)
    if m:
        return strip_body_formatting(m.group(1).strip())
    return ""


def extract_table_data(table_body):
    """Parse LaTeX tabular data into markdown table."""
    lines = []
    inside = False
    for line in table_body.split("\n"):
        line = line.strip()
        if line.startswith(r"\begin{tabular"):
            inside = True
            continue
        if line.startswith(r"\end{tabular"):
            break
        if inside and "&" in line and not line.startswith("%"):
            # Skip \hline, \toprule etc
            if re.match(r'^\\(?:hline|toprule|midrule|bottomrule|cline)', line):
                continue
            # Skip column spec lines
            if re.match(r'^[lcr|]+\s*$', line):
                continue
            # Split by &, clean each cell
            cells = []
            for cell in re.split(r'(?<!\\)&', line):
                cell = cell.strip()
                cell = re.sub(r'\\\\$', '', cell)
                cell = strip_body_formatting(cell)
                cell = re.sub(r'\s+', ' ', cell).strip()
                cells.append(cell)
            if any(cells):
                lines.append(cells)

    if not lines:
        return "[表格数据]"

    # Build markdown table
    result = []
    # Header row
    result.append("| " + " | ".join(lines[0]) + " |")
    result.append("|" + "|".join(["---"] * len(lines[0])) + "|")
    for row in lines[1:]:
        # Pad to match header length
        while len(row) < len(lines[0]):
            row.append("")
        result.append("| " + " | ".join(row[:len(lines[0])]) + " |")

    return "\n".join(result)


def parse_document(content, base_dir, abstract):
    """Parse LaTeX document body into structured sections."""
    # Remove \maketitle early, before any position-based work
    content = re.sub(r'\\maketitle', '', content)

    # Verify \begin{document} exists
    doc_start_marker = r"\begin{document}"
    if doc_start_marker not in content:
        return abstract or ""

    pos = content.index(doc_start_marker) + len(doc_start_marker)

    # Extract abstract first (use content from \begin{document} onward)
    doc_body = content[pos:]
    abs_match = re.search(r'\\begin\{abstract\}(.*?)\\end\{abstract\}', doc_body, re.DOTALL)

    result_parts = []
    if abs_match:
        pos = pos + abs_match.end()  # pos now points past \end{abstract}
        result_parts.append(("\n## Abstract\n\n" + extract_abstract(content) + "\n", pos))

    last_pos = pos

    # Find all structural markers in the remaining content
    sec_pattern = r'\\(section|subsection|subsubsection)\*?\{(.*?)\}'
    figure_pattern = r'\\begin\{figure\*?\}(.*?)\\end\{figure\*?\}'
    table_pattern = r'\\begin\{table\*?\}(.*?)\\end\{table\*?\}'
    equation_pattern = r'\\begin\{(equation|align|gather|multline)\*?\}(.*?)\\end\{\1\*?\}'

    elements = []
    for m in re.finditer(sec_pattern, content):
        elements.append(("section", m.start(), m.end(), m))
    for m in re.finditer(figure_pattern, content, re.DOTALL):
        elements.append(("figure", m.start(), m.end(), m))
    for m in re.finditer(table_pattern, content, re.DOTALL):
        elements.append(("table", m.start(), m.end(), m))
    for m in re.finditer(equation_pattern, content, re.DOTALL):
        elements.append(("equation", m.start(), m.end(), m))

    elements.sort(key=lambda x: x[1])

    figure_count = 0
    table_count = 0

    for elem_type, start, end, match in elements:
        if start < pos:
            continue

        # Body text before this element
        body_text = content[last_pos:start]
        body_text = strip_body_formatting(body_text).strip()
        if body_text and len(body_text) > 20:
            result_parts.append((body_text + "\n", start))

        if elem_type == "section":
            level_name = {"section": "#", "subsection": "##", "subsubsection": "###"}
            level = match.group(1)
            title = strip_body_formatting(match.group(2)).strip()
            prefix = level_name.get(level, "####")
            result_parts.append((f"\n{prefix} {title}\n\n", end))

        elif elem_type == "figure":
            figure_count += 1
            fig_content = match.group(1)
            cap_match = re.search(r'\\caption\{((?:[^{}]|\{[^{}]*\})*)\}', fig_content, re.DOTALL)
            caption = ""
            if cap_match:
                caption = strip_body_formatting(cap_match.group(1).strip())
                caption = re.sub(r'\s+', ' ', caption)
            img_paths = re.findall(r'\\includegraphics(?:\[.*?\])?\{([^}]+)\}', fig_content)
            img_refs = ", ".join(img_paths) if img_paths else ""
            result_parts.append((
                f"\n**Figure {figure_count}**" +
                (f": {caption}\n" if caption else "\n") +
                (f"[图片: {img_refs}]\n\n" if img_refs else "\n"),
                end
            ))

        elif elem_type == "table":
            table_count += 1
            tab_content = match.group(1)
            cap_match = re.search(r'\\caption\{((?:[^{}]|\{[^{}]*\})*)\}', tab_content, re.DOTALL)
            caption = ""
            if cap_match:
                caption = strip_body_formatting(cap_match.group(1).strip())
                caption = re.sub(r'\s+', ' ', caption)
            tab_match = re.search(r'\\begin\{tabular\*?\}(.*?)\\end\{tabular\*?\}', tab_content, re.DOTALL)
            if tab_match:
                table_md = extract_table_data(tab_match.group(0))
            else:
                table_md = "[表格 - 无 tabular 数据]"
            result_parts.append((
                f"\n**Table {table_count}**" +
                (f": {caption}\n" if caption else "\n") +
                f"{table_md}\n\n",
                end
            ))

        elif elem_type == "equation":
            eq_content = match.group(2).strip()
            result_parts.append((f"\n$$\n{eq_content}\n$$\n\n", end))

        last_pos = end

    # Remaining body text after last element
    rest = content[last_pos:]
    rest = re.sub(r'\\end\{document\}.*', '', rest, flags=re.DOTALL)
    rest = strip_body_formatting(rest).strip()
    if rest and len(rest) > 20:
        result_parts.append((rest + "\n", last_pos))

    # Sort by position and join
    result_parts.sort(key=lambda x: x[1])
    return "".join(p[0] for p in result_parts)


def find_media_files(source_dir, base_name=""):
    """Scan extracted source for image files and map them to figure references.

    Returns:
        dict: {filename_stem: [relative_paths]} for all image files found.
        Also returns a flat list of all image paths keyed by 'all'.
    """
    img_exts = {'.pdf', '.png', '.jpg', '.jpeg', '.eps', '.svg'}
    media = {"_all": []}
    source = Path(source_dir)

    for f in sorted(source.rglob("*")):
        if f.suffix.lower() in img_exts and f.is_file():
            rel = str(f.relative_to(source))
            media["_all"].append(rel)
            # Index by stem (fig1, figure1, etc.) for easy lookup
            stem = f.stem.lower().replace("_", "").replace("-", "")
            if stem not in media:
                media[stem] = []
            media[stem].append(rel)

    return media


def _resolve_figure_files(img_refs, media_map, source_dir):
    r"""Given \includegraphics references, find matching local files.

    Args:
        img_refs: list of paths from \includegraphics{xxx}
        media_map: result from find_media_files()
        source_dir: Path to extracted source

    Returns:
        list of relative paths to matching files (may be empty)
    """
    found = []
    source = Path(source_dir)
    for ref in img_refs:
        ref_path = Path(ref)
        ref_stem = ref_path.stem.lower().replace("_", "").replace("-", "")

        # Try exact match first
        exact = source / ref
        if exact.exists():
            found.append(str(ref))
            continue

        # Try with different extensions
        for ext in ('.pdf', '.png', '.jpg', '.jpeg', '.eps'):
            candidate = source / (ref_path.parent / (ref_path.stem + ext))
            if candidate.exists():
                found.append(str(candidate.relative_to(source)))
                break
        else:
            # Try fuzzy match via media_map
            if ref_stem in media_map:
                found.extend(media_map[ref_stem])

    return found


def extract_tex(arxiv_input, output_json=False, output_dir=None):
    """Main extraction pipeline.

    Args:
        arxiv_input: arXiv URL or ID
        output_json: if True, return dict instead of markdown string
        output_dir: if set, persist extracted files here (no auto-cleanup)
    """
    arxiv_id = extract_arxiv_id(arxiv_input)

    # Use persistent dir if specified, otherwise temp
    if output_dir:
        tmpdir = output_dir
        Path(tmpdir).mkdir(parents=True, exist_ok=True)
        # Clean any previous extraction
        for item in Path(tmpdir).iterdir():
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()
    else:
        tmpdir = tempfile.mkdtemp()

    try:
        success = download_source(arxiv_id, tmpdir)
        if not success:
            return {"error": "No LaTeX source available. This paper may only have PDF.", "arxiv_id": arxiv_id}

        main_tex = find_main_tex(tmpdir)
        if not main_tex:
            return {"error": "No .tex file found in source archive.", "arxiv_id": arxiv_id}

        content = main_tex.read_text(errors="replace")
        content = strip_latex_comments(content)

        # Resolve \input/\include
        content = resolve_inputs(content, main_tex.parent)
        content = strip_latex_comments(content)  # re-strip after input resolution

        # Extract metadata
        title, authors = extract_title_author(content)
        abstract = extract_abstract(content)

        # Scan for media files (before parsing document for figure-file mapping)
        media_map = find_media_files(tmpdir)

        # Parse document body
        doc_match = re.search(r'\\begin\{document\}(.*?)\\end\{document\}', content, re.DOTALL)
        if not doc_match:
            return {"error": "No \\begin{document} found.", "arxiv_id": arxiv_id}

        doc_body = doc_match.group(0)
        doc_content = parse_document(doc_body, main_tex.parent, abstract)

        # Build figure media inventory from the parsed content
        figures = []
        figure_pattern = r'\*\*Figure (\d+)\*\*:?\s*(.*?)(?:\n\[图片:\s*(.*?)\])?\n'
        for m in re.finditer(figure_pattern, doc_content):
            fig_num = int(m.group(1))
            caption = m.group(2).strip() if m.group(2) else ""
            img_refs_str = m.group(3) or ""
            img_refs = [r.strip() for r in img_refs_str.split(",") if r.strip()]
            local_files = _resolve_figure_files(img_refs, media_map, tmpdir) if img_refs else []
            figures.append({
                "index": fig_num,
                "caption": caption,
                "tex_refs": img_refs,
                "local_files": local_files,
            })

        # Build media inventory
        media = {
            "source_dir": str(Path(tmpdir).resolve()),
            "all_images": media_map.get("_all", []),
            "figures": figures,
        }

        # Build final markdown
        md = f"""---
title: "{title}"
authors: {json.dumps(authors, ensure_ascii=False)}
arxiv: https://arxiv.org/abs/{arxiv_id}
source: LaTeX source
---

# {title}

**Authors**: {", ".join(authors)}

**arXiv**: [{arxiv_id}](https://arxiv.org/abs/{arxiv_id})

{doc_content}
"""

        if output_json:
            return {
                "arxiv_id": arxiv_id,
                "title": title,
                "authors": authors,
                "abstract": abstract,
                "markdown": md,
                "tex_file": str(main_tex.name),
                "media": media,
            }

        return md

    finally:
        # Only cleanup if using temp dir (not user-specified output_dir)
        if not output_dir:
            shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract structured Markdown from arXiv LaTeX source.")
    parser.add_argument("arxiv_input", help="arXiv URL or identifier, for example 1706.03762")
    parser.add_argument("--json", action="store_true", dest="as_json", help="Return structured JSON")
    parser.add_argument("--output-dir", help="Persist extracted source and media files to this directory")
    args = parser.parse_args()

    try:
        result = extract_tex(args.arxiv_input, output_json=args.as_json, output_dir=args.output_dir)
        if args.as_json:
            print(json.dumps(result, ensure_ascii=False))
        else:
            if isinstance(result, dict) and "error" in result:
                parser.error(result["error"])
            print(result)
    except Exception as e:
        parser.error(str(e))
