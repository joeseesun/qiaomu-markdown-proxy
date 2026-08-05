import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]


class RoutingTests(unittest.TestCase):
    def test_description_is_single_line_and_mentions_wechat_priority(self):
        lines = (ROOT / "SKILL.md").read_text(encoding="utf-8").splitlines()
        description_lines = [line for line in lines[:12] if line.startswith("description:")]
        self.assertEqual(len(description_lines), 1)
        self.assertNotEqual(description_lines[0].strip(), "description: |")
        self.assertIn("mp.weixin.qq.com", description_lines[0])
        self.assertIn("Use FIRST", description_lines[0])

    def test_fetch_rejects_wechat_verification_wall(self):
        verification_page = "\n".join(
            [
                "Title: Weixin Official Accounts Platform",
                "URL Source: https://mp.weixin.qq.com/s/example",
                "Warning: This page maybe requiring CAPTCHA",
                "Markdown Content:",
                "## 环境异常",
                "当前环境异常，完成验证后即可继续访问。",
                "去验证",
            ]
        )
        result = self._run_fetch_with_fake_curl(verification_page)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("All fetch methods failed", result.stderr)

    def test_fetch_accepts_real_article_shaped_markdown(self):
        article = "\n".join(
            [
                "# A useful article",
                "",
                "Paragraph one.",
                "",
                "Paragraph two.",
                "",
                "Paragraph three.",
            ]
        )
        result = self._run_fetch_with_fake_curl(article)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("A useful article", result.stdout)

    def test_wechat_wrapper_rejects_non_wechat_url_without_network(self):
        result = subprocess.run(
            ["bash", str(ROOT / "scripts" / "fetch_weixin.sh"), "https://example.com"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 64)

    def test_arxiv_id_parser_supports_abs_and_pdf_urls(self):
        module_path = ROOT / "scripts" / "extract_tex.py"
        spec = importlib.util.spec_from_file_location("extract_tex", module_path)
        self.assertIsNotNone(spec)
        self.assertIsNotNone(spec.loader)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self.assertEqual(module.extract_arxiv_id("https://arxiv.org/abs/1706.03762"), "1706.03762")
        self.assertEqual(module.extract_arxiv_id("https://arxiv.org/pdf/1706.03762v7"), "1706.03762v7")

    def _run_fetch_with_fake_curl(self, output):
        with tempfile.TemporaryDirectory() as directory:
            fake_bin = Path(directory)
            curl = fake_bin / "curl"
            curl.write_text("#!/bin/sh\nprintf '%s\\n' \"$FAKE_CURL_OUTPUT\"\n", encoding="utf-8")
            curl.chmod(0o755)
            env = dict(os.environ)
            env["PATH"] = f"{fake_bin}:/usr/bin:/bin"
            env["FAKE_CURL_OUTPUT"] = output
            return subprocess.run(
                ["bash", str(ROOT / "scripts" / "fetch.sh"), "https://example.com/post"],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
                env=env,
            )


if __name__ == "__main__":
    unittest.main()
