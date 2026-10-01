"""多语言/别名归一化与规则引擎单测。"""
import unittest
from datetime import date

from pv_service import normalize
from pv_service.rules import (RuleContext, RuleEngine, SeriousUnlistedRule,
                              assess_listedness, significant_changes)


class TestNormalize(unittest.TestCase):
    def test_drug_multilingual(self):
        self.assertEqual(normalize.canonical_drug("布洛芬"),
                         normalize.canonical_drug("Ibuprofen"))
        self.assertEqual(normalize.canonical_drug("IBUPROFENO"), "ibuprofen")

    def test_reaction_multilingual(self):
        self.assertEqual(normalize.canonical_reaction("皮疹"),
                         normalize.canonical_reaction("sarpullido"))
        self.assertEqual(normalize.canonical_reaction("肝损伤"),
                         normalize.canonical_reaction("Drug-Induced Liver Injury"))

    def test_name_alias(self):
        self.assertEqual(normalize.canonical_name("张三"),
                         normalize.canonical_name("老张"))
        self.assertNotEqual(normalize.canonical_name("张三"),
                            normalize.canonical_name("李四"))

    def test_seriousness_criteria_multilang(self):
        self.assertTrue(normalize.is_serious(
            {"narrative": "患者住院治疗", "seriousness": "mild"}))
        self.assertTrue(normalize.is_serious({"seriousness": "grave"}))
        self.assertTrue(normalize.is_serious(
            {"narrative": "life-threatening reaction"}))
        self.assertFalse(normalize.is_serious({"seriousness": "mild"}))

    def test_date_parsing(self):
        self.assertEqual(normalize.parse_date("2026/09/05"), date(2026, 9, 5))
        self.assertEqual(normalize.parse_date("2026年9月5日"), date(2026, 9, 5))
        self.assertIsNone(normalize.parse_date(""))

    def test_causality_multilang(self):
        self.assertEqual(normalize.canonical_causality("很可能"), "PROBABLE")
        self.assertEqual(normalize.canonical_causality("probablemente"), "PROBABLE")


class TestRules(unittest.TestCase):
    def setUp(self):
        self.engine = RuleEngine()

    def ctx(self, snap, is_followup=False, prior=None, aware=date(2026, 9, 10),
            listed_fn=None):
        return RuleContext("c", snap, aware, is_followup, prior,
                           is_listed_fn=listed_fn)

    def test_serious_unlisted_15d(self):
        snap = {"drugs": [{"name": "warfarin", "causality": "probable"}],
                "reactions": ["肝损伤"], "seriousness": "严重"}
        d = self.engine.evaluate(self.ctx(snap, listed_fn=lambda a, b: False))
        self.assertTrue(d.expedited)
        self.assertEqual(d.days_due, 15)
        self.assertEqual(d.due_date, "2026-09-25")

    def test_serious_listed_30d(self):
        snap = {"drugs": [{"name": "ibuprofen", "causality": "probable"}],
                "reactions": ["皮疹"], "seriousness": "grave"}
        d = self.engine.evaluate(self.ctx(snap, listed_fn=lambda a, b: True))
        self.assertFalse(d.expedited)
        self.assertEqual(d.days_due, 30)

    def test_nonserious_90d(self):
        snap = {"drugs": [{"name": "x"}], "reactions": ["y"], "seriousness": "mild"}
        d = self.engine.evaluate(self.ctx(snap))
        self.assertEqual(d.days_due, 90)

    def test_followup_no_new_info_no_report(self):
        snap = {"drugs": [{"name": "x"}], "reactions": ["y"], "seriousness": "mild"}
        d = self.engine.evaluate(self.ctx(dict(snap), True, dict(snap)))
        self.assertFalse(d.should_report)
        self.assertEqual(d.rule_id, "R0_FOLLOWUP_NO_NEW_INFO")

    def test_followup_seriousness_escalation_to_expedited(self):
        old = {"drugs": [{"name": "warfarin"}], "reactions": ["肝损伤"],
               "seriousness": "mild"}
        new = {"drugs": [{"name": "warfarin"}], "reactions": ["肝损伤"],
               "seriousness": "严重", "seriousness_criteria": ["hospitalisation"]}
        changes = significant_changes(old, new)
        self.assertTrue(any("seriousness" in c for c in changes))
        d = self.engine.evaluate(self.ctx(new, True, old, listed_fn=lambda a, b: False))
        self.assertTrue(d.should_report)
        self.assertTrue(d.expedited)
        self.assertEqual(d.report_type, "FOLLOWUP")

    def test_drug_timing_change_is_significant(self):
        old = {"drugs": [{"name": "x", "start_date": "2026-09-05"}]}
        new = {"drugs": [{"name": "x", "start_date": "2026-09-02"}]}
        self.assertIn("drug_timing_corrected", significant_changes(old, new))

    def test_decision_is_reviewable(self):
        snap = {"drugs": [{"name": "warfarin", "causality": "probable"}],
                "reactions": ["肝损伤"], "seriousness": "严重"}
        d = self.engine.evaluate(self.ctx(snap, listed_fn=lambda a, b: False))
        body = d.as_dict()
        self.assertIn("rule_id", body)
        self.assertTrue(body["rationale"])
        self.assertTrue(body["listedness"])


if __name__ == "__main__":
    unittest.main()
