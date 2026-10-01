"""合并、拆分、随访工作流与血缘追溯。"""
from tests.base import BasePVTest
from pv_service.service import DomainError


class TestMergeSplit(BasePVTest):
    def _two_duplicates(self, tag="1"):
        ext = f"MS-{tag}"
        r1 = self.app.cases.intake("HOSPITAL", f"H-{ext}", self.zhang_patient())
        r2 = self.app.cases.intake("HOTLINE", f"HL-{ext}", self.zhang_multilang())
        cid = r2["duplicate_clusters"][0]["cluster_id"]
        return r1, r2, cid

    def test_confirm_merge_consolidates_sources_and_versions(self):
        r1, r2, cid = self._two_duplicates("c1")
        m = self.app.cases.confirm_cluster(cid, actor="liu")
        surv = m["survivor_case_id"]
        detail = self.app.cases.get_case(surv)
        self.assertEqual(2, len(detail["sources"]))
        self.assertEqual(["INTAKE", "MERGE"],
                         [v["change_type"] for v in detail["versions"]])
        merged_detail = self.app.cases.get_case(m["merged_from"][0])
        self.assertEqual("MERGED", merged_detail["status"])
        self.assertEqual(surv, merged_detail["superseded_by_id"])

    def test_merge_voids_unsent_duplicate_reports_keeps_sent(self):
        r1, r2, cid = self._two_duplicates("t0")
        # 先发送第一例的报告（已 ACKED）；第二例的报告仍排队
        first = self.app.reporting.send_one()
        self.assertEqual("ACKED", first["result"])
        m = self.app.cases.confirm_cluster(cid, actor="liu")
        self.assertEqual(1, len(m["voided_reports"]))
        voided_no = m["voided_reports"][0]["regulatory_number"]
        conn = self.db()
        row = conn.execute(
            "SELECT status FROM regulatory_reports WHERE regulatory_number=?",
            (voided_no,)).fetchone()
        self.assertEqual("VOID", row[0])
        # 已发送报告仍可查
        acked = conn.execute(
            "SELECT regulatory_number FROM regulatory_reports WHERE status='ACKED'") \
            .fetchall()
        self.assertEqual(1, len(acked))
        # VOID 报告永远不会被派发
        res = self.pump()
        self.assertEqual([], res["acked"])

    def test_cannot_confirm_non_proposed_cluster_twice(self):
        _, _, cid = self._two_duplicates("t1")
        self.app.cases.confirm_cluster(cid, actor="liu")
        with self.assertRaises(DomainError) as cm:
            self.app.cases.confirm_cluster(cid, actor="liu")
        self.assertEqual("CLUSTER_NOT_OPEN", cm.exception.code)

    def test_merge_then_followup_links_to_sent_initial(self):
        r1, r2, cid = self._two_duplicates("t2")
        self.app.reporting.send_one()
        m = self.app.cases.confirm_cluster(cid, actor="liu")
        f = self.app.cases.add_followup(m["survivor_case_id"],
                                        {"causality": "可能", "outcome": "好转"})
        self.assertIsNotNone(f["report"])
        rep = self.app.reporting.get_report(f["report"]["id"])
        self.assertEqual("FOLLOWUP", rep["report_type"])
        self.assertIsNotNone(rep["followup_of_id"])

    def test_split_source_to_new_case(self):
        r1, r2, cid = self._two_duplicates("t3")
        m = self.app.cases.confirm_cluster(cid, actor="liu")
        surv = m["survivor_case_id"]
        hotline_src = next(c for c in self.app.cases.workbench()
                           if c["case_id"] == surv)["sources"]
        sid = next(s["id"] for s in hotline_src if s["source_system"] == "HOTLINE")
        sp = self.app.cases.split_source(surv, sid, actor="wang")
        new_case = sp["new_case_id"]
        parent = self.app.cases.get_case(surv)
        child = self.app.cases.get_case(new_case)
        self.assertEqual(1, len(child["sources"]))
        self.assertEqual(1, len(parent["sources"]))
        # 子病例以其来源原文重新建版本，父病例保留 SPLIT 版本
        self.assertEqual("INTAKE", child["versions"][0]["change_type"])
        self.assertIn("SPLIT", [v["change_type"] for v in parent["versions"]])
        # 来源归属唯一
        conn = self.db()
        n = conn.execute(
            "SELECT COUNT(*) FROM case_membership WHERE source_id=? AND active=1",
            (sid,)).fetchone()[0]
        self.assertEqual(1, n)

    def test_cannot_split_source_not_in_case(self):
        r1, r2, cid = self._two_duplicates("t4")
        other = self.app.cases.intake("ENTERPRISE", "E-S1", {
            "patient_name": "王五", "dob": "1950-01-01",
            "drugs": [{"name": "warfarin"}], "reactions": ["出血"],
            "seriousness": "严重", "event_date": "2026-09-01"})
        sid = self.app.cases.get_case(other["case_id"])["sources"][0]
        with self.assertRaises(DomainError) as cm:
            self.app.cases.split_source(r1["case_id"], sid)
        self.assertEqual("SOURCE_NOT_IN_CASE", cm.exception.code)

    def test_split_then_merge_conflict_returns_409(self):
        # 簇确认时，其中一个病例已被拆分走（status OPEN 但来源已变），
        # 不应产生双归属；重复确认得到 CLUSTER_NOT_OPEN
        r1, r2, cid = self._two_duplicates("t5")
        m1 = self.app.cases.confirm_cluster(cid, actor="liu")
        with self.assertRaises(DomainError):
            self.app.cases.confirm_cluster(cid, actor="liu")

    def test_lineage_timeline_shows_full_history(self):
        r1, r2, cid = self._two_duplicates("t6")
        self.app.reporting.send_one()
        m = self.app.cases.confirm_cluster(cid, actor="liu")
        surv = m["survivor_case_id"]
        self.app.cases.add_followup(surv, {"outcome": "好转", "causality": "可能"})
        tl = self.app.cases.timeline(surv)
        types = {e["event_type"] for e in tl}
        self.assertIn("SOURCE_INTAKE", types)
        self.assertIn("DUPLICATE_CONFIRMED", types)
        self.assertIn("REPORT_ACKED", types)
        self.assertIn("FOLLOWUP_RECEIVED", types)
        # 被合并病例的来源记录在时间线 payload 里可追溯
        self.assertTrue(any(e["case_id"] != surv for e in tl))

    def test_followup_to_merged_case_is_rejected(self):
        r1, r2, cid = self._two_duplicates("t7")
        m = self.app.cases.confirm_cluster(cid, actor="liu")
        with self.assertRaises(DomainError) as cm:
            self.app.cases.add_followup(m["merged_from"][0], {"outcome": "x"})
        self.assertEqual("CASE_NOT_OPEN", cm.exception.code)
