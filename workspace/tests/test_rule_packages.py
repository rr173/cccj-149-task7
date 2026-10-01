"""规则包内容规范、签名、生效区间、版本指纹的单元测试。"""
import unittest

from pv_service import rule_pkg as rp
from pv_service.rule_pkg import (
    RulePackageError, build_content, canonical_content_bytes, content_hash,
    intervals_overlap, normalize_interval, sign_content, verify_signature,
    version_key, build_engine, legacy_view)

SECRET = "unit-test-secret"


class RulePackageModelTest(unittest.TestCase):
    def test_canonical_bytes_order_independent(self):
        a = {"a": 1, "b": [1, 2, 3], "c": {"x": "y"}}
        b = {"c": {"x": "y"}, "b": [1, 2, 3], "a": 1}
        self.assertEqual(canonical_content_bytes(a), canonical_content_bytes(b))

    def test_content_hash_deterministic(self):
        c = build_content(jurisdiction="CN", version="1.0",
                          effective_from="2026-10-01", routine_days=45)
        self.assertEqual(content_hash(c), content_hash(json_roundtrip(c)))

    def test_different_content_different_hash(self):
        c1 = build_content(jurisdiction="CN", version="1.0",
                           effective_from="2026-10-01", routine_days=45)
        c2 = build_content(jurisdiction="CN", version="1.0",
                           effective_from="2026-10-01", routine_days=46)
        self.assertNotEqual(content_hash(c1), content_hash(c2))

    def test_signature_roundtrip_and_tamper(self):
        c = build_content(jurisdiction="CN", version="1.0",
                          effective_from="2026-10-01")
        sig = sign_content(c, SECRET)
        self.assertTrue(verify_signature(c, sig, SECRET))
        # 错误密钥
        self.assertFalse(verify_signature(c, sig, "other-secret"))
        # 内容被篡改
        tampered = dict(c)
        tampered["summary"] = "changed"
        self.assertFalse(verify_signature(tampered, sig, SECRET))
        # 空签名
        self.assertFalse(verify_signature(c, "", SECRET))

    def test_build_content_defaults_and_params(self):
        c = build_content(jurisdiction="CN", version="2026.1",
                          effective_from="2026-10-01", serious_days=10,
                          listed_days=20, routine_days=45,
                          summary="x", parameters={"custom": 1})
        self.assertEqual(c["effective_to"], None)
        self.assertEqual(c["parameters"]["serious_days"], 10)
        self.assertEqual(c["parameters"]["custom"], 1)

    def test_build_content_validation(self):
        with self.assertRaises(RulePackageError) as cm:
            build_content(jurisdiction=" ", version="1", effective_from="2026-10-01")
        self.assertEqual(cm.exception.code, "BAD_JURISDICTION")
        with self.assertRaises(RulePackageError):
            build_content(jurisdiction="CN", version="1", effective_from="not-a-date")
        with self.assertRaises(RulePackageError):
            build_content(jurisdiction="CN", version="1",
                          effective_from="2026-10-01", effective_to="2026-09-01")
        with self.assertRaises(RulePackageError):
            build_content(jurisdiction="CN", version="1",
                          effective_from="2026-10-01", serious_days=0)
        with self.assertRaises(RulePackageError):
            build_content(jurisdiction="CN", version="1",
                          effective_from="2026-10-01", serious_days=True)

    def test_interval_half_open(self):
        # 首尾相接 [a,b) 与 [b,c) 不相交
        self.assertFalse(intervals_overlap("2026-01-01", "2026-06-01",
                                           "2026-06-01", "2027-01-01"))
        # 真正相交
        self.assertTrue(intervals_overlap("2026-01-01", "2026-06-02",
                                          "2026-06-01", None))
        # 开放区间与后续有限区间
        self.assertTrue(intervals_overlap("2026-01-01", None,
                                          "2030-01-01", "2031-01-01"))
        # 开放区间与更早的
        self.assertFalse(intervals_overlap("2030-01-01", None,
                                           "2020-01-01", "2021-01-01"))

    def test_normalize_interval_accepts_datetime(self):
        start, end = normalize_interval("2026-10-01T08:00:00",
                                        "2027-01-01T00:00:00")
        self.assertEqual((start, end), ("2026-10-01", "2027-01-01"))

    def test_version_ordering(self):
        self.assertLess(version_key("1.0"), version_key("1.1"))
        self.assertLess(version_key("1.9"), version_key("1.10"))
        with self.assertRaises(RulePackageError):
            version_key("")

    def test_engine_reflects_days(self):
        c = build_content(jurisdiction="CN", version="1",
                          effective_from="2026-10-01", serious_days=7,
                          listed_days=21, routine_days=60)
        eng = build_engine(c)
        self.assertEqual(eng._days, {"R1": 7, "R2": 21, "R3": 60})
        legacy = build_engine(None)
        self.assertEqual(legacy._days, {"R1": 15, "R2": 30, "R3": 90})
        self.assertTrue(legacy_view()["builtin"])


def json_roundtrip(obj):
    import json
    return json.loads(json.dumps(obj))


if __name__ == "__main__":
    unittest.main()
