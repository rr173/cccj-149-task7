"""identity.normalize_reviewer / validate_reviewer 的纯函数性质测试。"""
import unittest

from pv_service import identity


class TestNormalizeReviewer(unittest.TestCase):
    def test_trims_surrounding_whitespace_only(self):
        self.assertEqual("alice", identity.normalize_reviewer("  alice  "))
        self.assertEqual("alice", identity.normalize_reviewer("\talice\n"))
        # 内部空白保留
        self.assertEqual("a b", identity.normalize_reviewer(" a b "))
        self.assertNotEqual(
            identity.normalize_reviewer("ab"),
            identity.normalize_reviewer("a b"))

    def test_case_insensitive(self):
        self.assertEqual(identity.normalize_reviewer("Alice"),
                         identity.normalize_reviewer("ALICE"))
        # casefold 比 lower 更彻底（德语 ß -> ss）
        self.assertEqual(identity.normalize_reviewer("Straße"),
                         identity.normalize_reviewer("STRASSE"))

    def test_unicode_nfkc_equivalence(self):
        # 全角拉丁字母
        self.assertEqual(identity.normalize_reviewer("revＡ"),
                         identity.normalize_reviewer("revA"))
        # 埃格斯特朗符 U+212B 与 A 上圈 U+00C5 兼容等价
        self.assertEqual(identity.normalize_reviewer("Å"),
                         identity.normalize_reviewer("Å"))
        # 兼容字符（连字）折叠
        self.assertEqual(identity.normalize_reviewer("ﬁle"),
                         identity.normalize_reviewer("file"))

    def test_deterministic_and_idempotent(self):
        for v in ["alice", " ALICE ", "Ａlice", "Å"]:
            once = identity.normalize_reviewer(v)
            self.assertEqual(once, identity.normalize_reviewer(once))


class TestValidateReviewer(unittest.TestCase):
    def test_returns_key_for_valid(self):
        self.assertEqual("alice", identity.validate_reviewer("  Alice  "))

    def test_blank_rejected(self):
        for bad in ("", "   ", "\t\n", "　"):  # 含全角空格
            with self.assertRaises(ValueError) as cm:
                identity.validate_reviewer(bad)
            self.assertTrue(str(cm.exception).startswith("BLANK_REVIEWER"))

    def test_non_string_rejected(self):
        for bad in (None, 123, 1.5, ["a"], {"a": 1}, object()):
            with self.assertRaises(ValueError) as cm:
                identity.validate_reviewer(bad)
            self.assertTrue(str(cm.exception).startswith("INVALID_REVIEWER"))

    def test_too_long_rejected(self):
        with self.assertRaises(ValueError) as cm:
            identity.validate_reviewer("x" * (identity.MAX_REVIEWER_LEN + 1))
        self.assertTrue(str(cm.exception).startswith("REVIEWER_TOO_LONG"))


class TestCoerceAndSame(unittest.TestCase):
    def test_coerce_soft(self):
        self.assertEqual(("alice", "  Alice  "),
                         identity.coerce_reviewer("  Alice  "))
        self.assertEqual((None, None), identity.coerce_reviewer(""))
        self.assertEqual((None, None), identity.coerce_reviewer(None))
        self.assertEqual((None, None), identity.coerce_reviewer(7))

    def test_is_same_identity(self):
        self.assertTrue(identity.is_same_identity("alice", " ALICE "))
        self.assertTrue(identity.is_same_identity("revＡ", "revA"))
        self.assertFalse(identity.is_same_identity("alice", "bob"))
        # 空白/非法身份按不同身份处理（不抛异常）
        self.assertFalse(identity.is_same_identity("", " "))
        self.assertFalse(identity.is_same_identity(None, "alice"))


if __name__ == "__main__":
    unittest.main()
