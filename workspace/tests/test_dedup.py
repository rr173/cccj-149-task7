"""疑似重复检测：多语言/别名/缺字段/强标识/成簇/否决后不再提议。"""
from tests.base import BasePVTest


class TestDedup(BasePVTest):
    def test_multilingual_alias_duplicate_proposed(self):
        r1 = self.app.cases.intake("HOSPITAL", "H-1", self.zhang_patient())
        r2 = self.app.cases.intake("HOTLINE", "HL-1", self.zhang_multilang())
        self.assertEqual(1, len(r2["duplicate_clusters"]))
        cluster = r2["duplicate_clusters"][0]
        self.assertIn(r1["case_id"], cluster["members"])
        edge = cluster["edges"][0]
        self.assertGreaterEqual(edge["score"], 70)
        factors = {x["factor"] for x in edge["reasons"]}
        self.assertIn("strong_identifier", factors)

    def test_strong_identifier_short_circuits_to_max_score(self):
        from pv_service.dedup import build_profile, score_profiles
        a = build_profile("a", {"patient": {"name": "张三"},
                                "patient_identifier": "X1", "drugs": [], "reactions": []})
        b = build_profile("b", {"patient": {"name": "李四"},
                                "patient_identifier": "X1", "drugs": [], "reactions": []})
        score, reasons = score_profiles(a, b)
        self.assertEqual(score, 100)

    def test_missing_fields_still_clusters_on_name_drugs_reactions(self):
        # 缺身份证/出生日期：姓名别名 + 同药同反应 + 日期接近 应过阈值
        p1 = self.zhang_patient()
        for k in ("patient_identifier", "dob", "gender"):
            p1.pop(k)
        p2 = self.zhang_multilang()
        for k in ("patient_identifier", "dob", "gender"):
            p2.pop(k)
        r1 = self.app.cases.intake("HOSPITAL", "H-2", p1)
        r2 = self.app.cases.intake("ENTERPRISE", "E-2", p2)
        self.assertTrue(r2["duplicate_clusters"])

    def test_unrelated_patients_not_clustered(self):
        self.app.cases.intake("HOSPITAL", "H-3", self.zhang_patient())
        other = {"patient_name": "王五", "gender": "男", "dob": "1955-01-01",
                 "patient_identifier": "ID-1", "drugs": [{"name": "warfarin"}],
                 "reactions": ["出血"], "seriousness": "严重",
                 "event_date": "2026-09-01"}
        r = self.app.cases.intake("HOTLINE", "HL-3", other)
        self.assertFalse(r["duplicate_clusters"])

    def test_no_automerge_two_cases_until_human_confirms(self):
        r1 = self.app.cases.intake("HOSPITAL", "H-4", self.zhang_patient())
        r2 = self.app.cases.intake("HOTLINE", "HL-4", self.zhang_multilang())
        wb = {c["case_id"]: c for c in self.app.cases.workbench()}
        self.assertIn(r1["case_id"], wb)
        self.assertIn(r2["case_id"], wb)
        clusters = self.app.cases.list_clusters()
        self.assertEqual("PROPOSED", clusters[0]["status"])

    def test_rejected_edge_is_not_reproposed(self):
        self.app.cases.intake("HOSPITAL", "H-5", self.zhang_patient())
        r2 = self.app.cases.intake("HOTLINE", "HL-5", self.zhang_multilang())
        cid = r2["duplicate_clusters"][0]["cluster_id"]
        self.app.cases.reject_cluster(cid, reason="不同患者，身份证填写有误", actor="liu")
        # 新来源再次构成同一边 -> 不应再提议
        p3 = self.zhang_multilang()
        r3 = self.app.cases.intake("ENTERPRISE", "E-5", p3)
        # 新病例与被否决两边都不应再形成 PROPOSED 簇
        proposed = self.app.cases.list_clusters("PROPOSED")
        self.assertEqual([], proposed)
        self.assertEqual([], r3["duplicate_clusters"])

    def test_three_way_cluster_merges_existing_proposed(self):
        r1 = self.app.cases.intake("HOSPITAL", "H-6", self.zhang_patient())
        r2 = self.app.cases.intake("HOTLINE", "HL-6", self.zhang_multilang())
        c1 = r2["duplicate_clusters"][0]["cluster_id"]
        p3 = self.zhang_patient()
        p3["patient_name"] = "San Zhang"
        r3 = self.app.cases.intake("ENTERPRISE", "E-6", p3)
        c2 = r3["duplicate_clusters"][0]
        self.assertEqual(3, len(c2["members"]))
        # 旧簇被 SUPERSEDED 指向新簇，审计保留
        old = [c for c in self.app.cases.list_clusters("SUPERSEDED") if c["id"] == c1]
        self.assertEqual(1, len(old))
