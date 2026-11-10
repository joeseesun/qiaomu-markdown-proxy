import importlib.util
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def load_extract_tex():
    module_path = ROOT / "scripts" / "extract_tex.py"
    spec = importlib.util.spec_from_file_location("extract_tex", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ArxivIdTests(unittest.TestCase):
    def test_new_style_ids_still_work(self):
        module = load_extract_tex()
        self.assertEqual(module.extract_arxiv_id("https://arxiv.org/abs/1706.03762"), "1706.03762")
        self.assertEqual(module.extract_arxiv_id("https://arxiv.org/pdf/1706.03762v7"), "1706.03762v7")
        self.assertEqual(module.extract_arxiv_id("2411.12345"), "2411.12345")

    def test_old_style_id_from_url(self):
        module = load_extract_tex()
        self.assertEqual(
            module.extract_arxiv_id("https://arxiv.org/abs/hep-th/9901001"),
            "hep-th/9901001",
        )

    def test_old_style_raw_id(self):
        module = load_extract_tex()
        self.assertEqual(module.extract_arxiv_id("hep-th/9901001"), "hep-th/9901001")

    def test_old_style_id_with_version(self):
        module = load_extract_tex()
        self.assertEqual(
            module.extract_arxiv_id("https://arxiv.org/pdf/hep-th/9901001v2"),
            "hep-th/9901001v2",
        )

    def test_garbage_still_rejected(self):
        module = load_extract_tex()
        with self.assertRaises(ValueError):
            module.extract_arxiv_id("not-an-arxiv-id")


if __name__ == "__main__":
    unittest.main()
