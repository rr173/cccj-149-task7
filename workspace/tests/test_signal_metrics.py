"""PRR/ROR 指标与阈值判定纯函数测试。"""
import math
import unittest

from pv_service.signal_metrics import Z95, compute_metrics, evaluate


class TestMetrics(unittest.TestCase):
    def test_known_values(self):
        # a=4,b=2,c=1,d=4
        m = compute_metrics(4, 2, 1, 4)
        self.assertAlmostEqual(m["prr"], (4 / 6) / (1 / 5), places=6)
        self.assertAlmostEqual(m["ror"], 4 * 4 / (2 * 1), places=6)
        self.assertEqual(m["n"], 11)
        # 手工核对 ROR 对数正态 CI
        se = math.sqrt(1 / 4 + 1 / 2 + 1 / 1 + 1 / 4)
        self.assertAlmostEqual(m["ror_ci_high"], math.exp(math.log(8) + Z95 * se),
                               places=5)
        self.assertLess(m["ror_ci_low"], m["ror"])
        self.assertGreater(m["chi_square"], 0)

    def test_zero_cells_are_none_not_fabricated(self):
        m = compute_metrics(3, 5, 0, 4)
        self.assertIsNone(m["prr"])
        self.assertIsNone(m["ror"])
        m2 = compute_metrics(3, 0, 2, 5)
        self.assertIsNone(m2["ror"])  # b=0 不可估计
        self.assertIsNotNone(m2["prr"])

    def test_continuity_correction_explicit_and_deterministic(self):
        m1 = compute_metrics(3, 0, 2, 5, continuity_correction=True)
        m2 = compute_metrics(3, 0, 2, 5, continuity_correction=True)
        self.assertEqual(m1, m2)
        self.assertIsNotNone(m1["ror"])  # 修正后可估计
        # 默认不修正：与修正结果不同（口径显式化）
        self.assertNotEqual(m1["ror"],
                            compute_metrics(3, 0, 2, 5)["ror"])

    def test_evaluate_insufficient(self):
        m = compute_metrics(1, 1, 1, 1)
        v = evaluate(m, {"min_n": 10})
        self.assertEqual(v["determination"], "INSUFFICIENT")
        v2 = evaluate(m, {"min_a": 5})
        self.assertEqual(v2["determination"], "INSUFFICIENT")

    def test_evaluate_signal_all_gates(self):
        m = compute_metrics(20, 5, 3, 100)
        crit = {"min_n": 50, "prr_min": 2, "prr_ci_lower_than": 1,
                "ror_min": 2, "ror_ci_lower_than": 1,
                "chi_square_min": 4, "require": "ALL"}
        self.assertEqual(evaluate(m, crit)["determination"], "SIGNAL")

    def test_evaluate_no_signal_when_ci_crosses_one(self):
        # 点估计高但 CI 很宽跨 1
        m = compute_metrics(3, 2, 1, 3)
        crit = {"min_n": 5, "ror_min": 2, "ror_ci_lower_than": 1}
        self.assertEqual(evaluate(m, crit)["determination"], "NO_SIGNAL")

    def test_evaluate_any_vs_all(self):
        m = compute_metrics(3, 2, 1, 3)
        crit = {"min_n": 5, "prr_min": 2, "ror_ci_lower_than": 1, "require": "ANY"}
        self.assertEqual(evaluate(m, crit)["determination"], "SIGNAL")
        crit["require"] = "ALL"
        self.assertEqual(evaluate(m, crit)["determination"], "NO_SIGNAL")


if __name__ == "__main__":
    unittest.main()
