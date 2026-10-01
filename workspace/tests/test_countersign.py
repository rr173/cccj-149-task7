"""双人会签核心流程：评审单冻结、双签放行、否决、撤回、失效与修订号。"""
import shutil
import tempfile
import unittest
from datetime import datetime

from pv_service.app import Application, FixedClock
from pv_service.service import DomainError


class CountersignBase(unittest.TestCase):
    START = datetime(2026, 9, 10, 9, 0, 0)

    def _make_app(self, **kw):
        self.dir = tempfile.mkdtemp(prefix="pv-cs-")
        self.clock = FixedClock(self.START)
        self.app = Application(self.dir, clock=self.clock, **kw)
        self.app.seed_demo_reference()
        return self.app

    def tearDown(self):
        self.app.shutdown()
        shutil.rmtree(self.dir, ignore_errors=True)

    def serious_unlisted_intake(self, ext="H-CS-1"):
        """严重 + 非预期（warfarin/rash 未列入参考表）-> 15 日加急 QUEUED。"""
        return self.app.cases.intake("HOSPITAL", ext, {
            "patient_name": "王五", "dob": "1970-05-05",
            "patient_identifier": "ID-555",
            "drugs": [{"name": "warfarin", "start_date": "2026-09-05"}],
            "reactions": ["皮疹"], "seriousness": "严重住院",
            "event_date": "2026-09-08"})

    def approve_with_two(self, sheet_id, a="revA", b="revB"):
        self.app.reviews.approve(sheet_id, a)
        return self.app.reviews.approve(sheet_id, b)


class TestCountersignCore(CountersignBase):
    def setUp(self):
        self._make_app(countersign_required=True)

    # -- 评审单冻结 ----------------------------------------------------------
    def test_sheet_freezes_revision_items_and_is_immutable(self):
        r = self.serious_unlisted_intake()
        rid = r["report"]["id"]
        sh = self.app.reviews.create_sheet([rid], requested_by="init")
        self.assertEqual("OPEN", sh["status"])
        self.assertEqual(1, sh["revision"])
        self.assertEqual(2, sh["required_approvals"])
        self.assertEqual(1, len(sh["items"]))
        item = sh["items"][0]
        self.assertEqual("PENDING_REPORT", item["item_type"])
        self.assertEqual(rid, item["ref_id"])
        self.assertTrue(item["snapshot"]["expedited"])
        self.assertEqual("R1_SERIOUS_UNLISTED_15D",
                         item["snapshot"]["decision"]["rule_id"])
        # 冻结项不可改
        import sqlite3
        from pv_service import db as dbmod
        conn = dbmod.connect(self.app.db_path)
        try:
            with self.assertRaises(sqlite3.Error):
                conn.execute("UPDATE review_sheet_items SET snapshot_json='{}'")
        finally:
            conn.close()

    def test_create_sheet_idempotent_same_content(self):
        r = self.serious_unlisted_intake()
        rid = r["report"]["id"]
        s1 = self.app.reviews.create_sheet([rid])
        s2 = self.app.reviews.create_sheet([rid])
        self.assertEqual(s1["sheet_id"], s2["sheet_id"])
        self.assertTrue(s2.get("idempotent"))

    def test_create_empty_review_rejected(self):
        with self.assertRaises(DomainError) as cm:
            self.app.reviews.create_sheet()
        self.assertEqual("EMPTY_REVIEW", cm.exception.code)

    def test_report_not_pending_rejected(self):
        r = self.serious_unlisted_intake()
        rid = r["report"]["id"]
        s = self.app.reviews.create_sheet([rid])
        self.approve_with_two(s["sheet_id"])
        self.app.reporting.pump()  # 发出 -> ACKED
        with self.assertRaises(DomainError) as cm:
            self.app.reviews.create_sheet([rid])
        self.assertEqual("REPORT_NOT_PENDING", cm.exception.code)

    # -- 发送闸门 ------------------------------------------------------------
    def test_dispatch_blocked_before_countersign(self):
        r = self.serious_unlisted_intake()
        rid = r["report"]["id"]
        with self.assertRaises(DomainError) as cm:
            self.app.reporting.send_one(rid)
        self.assertEqual(403, cm.exception.status)
        self.assertEqual("COUNTERSIGN_REQUIRED", cm.exception.code)
        with self.assertRaises(DomainError):
            self.app.reporting.pump()

    def test_dispatch_after_two_distinct_approvals(self):
        r = self.serious_unlisted_intake()
        rid = r["report"]["id"]
        s = self.app.reviews.create_sheet([rid])
        self.app.reviews.approve(s["sheet_id"], "revA")
        # 单人不足，仍拦截
        with self.assertRaises(DomainError):
            self.app.reporting.send_one(rid)
        sh = self.app.reviews.approve(s["sheet_id"], "revB")
        self.assertEqual("APPROVED", sh["status"])
        res = self.app.reporting.pump()
        self.assertEqual(1, len(res["acked"]))

    def test_same_reviewer_resubmit_does_not_accumulate(self):
        r = self.serious_unlisted_intake()
        rid = r["report"]["id"]
        s = self.app.reviews.create_sheet([rid])
        self.app.reviews.approve(s["sheet_id"], "revA")
        for _ in range(3):
            with self.assertRaises(DomainError) as cm:
                self.app.reviews.approve(s["sheet_id"], "revA")
            self.assertEqual("ALREADY_SIGNED", cm.exception.code)
        sh = self.app.reviews.get_sheet(s["sheet_id"])
        self.assertEqual(1, sh["approval_count"])
        self.assertEqual("OPEN", sh["status"])
        # 仍然拦截发送
        with self.assertRaises(DomainError):
            self.app.reporting.send_one(rid)

    # -- 否决 ----------------------------------------------------------------
    def test_veto_ends_round_immediately(self):
        r = self.serious_unlisted_intake()
        rid = r["report"]["id"]
        s = self.app.reviews.create_sheet([rid])
        self.app.reviews.approve(s["sheet_id"], "revA")
        out = self.app.reviews.veto(s["sheet_id"], "revB", reason="信息不足")
        self.assertEqual("REJECTED", out["status"])
        # 终态后再签署/否决都拒绝
        with self.assertRaises(DomainError) as cm:
            self.app.reviews.approve(s["sheet_id"], "revC")
        self.assertEqual("SHEET_NOT_OPEN", cm.exception.code)
        with self.assertRaises(DomainError):
            self.app.reporting.send_one(rid)
        # 否决意见也占用身份槽，不能重复否决
        with self.assertRaises(DomainError):
            self.app.reviews.veto(s["sheet_id"], "revB")

    def test_first_veto_wins_even_second_would_approve(self):
        r = self.serious_unlisted_intake()
        rid = r["report"]["id"]
        s = self.app.reviews.create_sheet([rid])
        self.app.reviews.veto(s["sheet_id"], "revA")
        with self.assertRaises(DomainError):
            self.app.reviews.approve(s["sheet_id"], "revB")
        self.assertEqual("REJECTED", self.app.reviews.get_sheet(s["sheet_id"])["status"])

    # -- 撤回 ----------------------------------------------------------------
    def test_withdraw_allows_replacement_signature(self):
        r = self.serious_unlisted_intake()
        rid = r["report"]["id"]
        s = self.app.reviews.create_sheet([rid])
        self.app.reviews.approve(s["sheet_id"], "revA")
        self.app.reviews.withdraw(s["sheet_id"], "revA")
        sh = self.app.reviews.get_sheet(s["sheet_id"])
        self.assertEqual(0, sh["approval_count"])
        # 撤回后本人可重新签署
        self.app.reviews.approve(s["sheet_id"], "revA")
        self.app.reviews.approve(s["sheet_id"], "revB")
        self.assertEqual("APPROVED",
                         self.app.reviews.get_sheet(s["sheet_id"])["status"])
        # 历史流水仍包含撤回记录
        types = [e["event_type"] for e in
                 self.app.reviews.audit_trail(sheet_id=s["sheet_id"])]
        self.assertIn("WITHDRAWN", types)

    def test_withdraw_without_signature_and_veto_final(self):
        r = self.serious_unlisted_intake()
        s = self.app.reviews.create_sheet([r["report"]["id"]])
        with self.assertRaises(DomainError) as cm:
            self.app.reviews.withdraw(s["sheet_id"], "revZ")
        self.assertEqual("NOT_SIGNED", cm.exception.code)
        self.app.reviews.veto(s["sheet_id"], "revA")
        with self.assertRaises(DomainError) as cm:
            self.app.reviews.withdraw(s["sheet_id"], "revA")
        self.assertEqual("VETO_FINAL", cm.exception.code)

    def test_withdraw_concurrent_with_second_approval_single_outcome(self):
        # revA 与 revB 已签署后达成 APPROVED；对已完成单撤回必须失败
        r = self.serious_unlisted_intake()
        s = self.app.reviews.create_sheet([r["report"]["id"]])
        self.app.reviews.approve(s["sheet_id"], "revA")
        self.app.reviews.approve(s["sheet_id"], "revB")
        with self.assertRaises(DomainError):
            self.app.reviews.withdraw(s["sheet_id"], "revA")
        self.assertEqual("APPROVED",
                         self.app.reviews.get_sheet(s["sheet_id"])["status"])

    # -- 失效 ----------------------------------------------------------------
    def test_followup_invalidates_open_sheet_but_keeps_opinions(self):
        r = self.serious_unlisted_intake()
        rid = r["report"]["id"]
        s = self.app.reviews.create_sheet([rid])
        self.app.reviews.approve(s["sheet_id"], "revA")
        # 评审期间新材料入库
        f = self.app.cases.add_followup(r["case_id"], {
            "outcome": "死亡"})
        sh = self.app.reviews.get_sheet(s["sheet_id"])
        self.assertEqual("INVALIDATED", sh["status"])
        self.assertEqual("NEW_MATERIAL", sh["invalidated_reason"])
        # 已有意见保留但不能推动发送
        self.assertEqual(1, sh["approval_count"])
        self.assertFalse(sh["released_for_dispatch"])
        # 旧单不能再签
        with self.assertRaises(DomainError) as cm:
            self.app.reviews.approve(s["sheet_id"], "revB")
        self.assertEqual("SHEET_INVALIDATED", cm.exception.code)
        # 旧待发送决定仍不能发
        with self.assertRaises(DomainError):
            self.app.reporting.send_one(rid)
        # 新单只能引用最新修订号，且内容包含随访产生的新决定
        new_rid = f["report"]["id"]
        s2 = self.app.reviews.create_sheet([new_rid])
        self.assertEqual(sh["current_revision"], s2["revision"])
        self.assertGreater(s2["revision"], s["revision"])

    def test_merge_and_split_invalidate(self):
        # 两份同事件报文 -> PROPOSED 簇
        r1 = self.app.cases.intake("HOSPITAL", "H-M1", {
            "patient_name": "张三", "dob": "1980-03-03",
            "patient_identifier": "ID-1",
            "drugs": [{"name": "drug-x"}], "reactions": ["rx-y"],
            "seriousness": "mild", "event_date": "2026-09-01"})
        r2 = self.app.cases.intake("HOTLINE", "HL-M1", {
            "patient_name": "老张", "dob": "1980-03-03",
            "patient_identifier": "ID-1",
            "drugs": [{"name": "drug-x"}], "reactions": ["rx-y"],
            "seriousness": "mild", "event_date": "2026-09-01"})
        # 全量内容单（含归并候选）
        s = self.app.reviews.create_sheet()
        types = {i["item_type"] for i in s["items"]}
        self.assertIn("MERGE_CANDIDATE", types)
        cluster_id = r2["duplicate_clusters"][0]["cluster_id"]
        self.app.cases.confirm_cluster(cluster_id, actor="specialist")
        sh = self.app.reviews.get_sheet(s["sheet_id"])
        self.assertEqual("INVALIDATED", sh["status"])
        self.assertEqual("MERGED", sh["invalidated_reason"])

        # 拆解同样失效
        src = self.app.cases.get_case(r1["case_id"])["sources"][0]
        s2 = self.app.reviews.create_sheet()
        self.app.cases.split_source(r1["case_id"], src, actor="specialist")
        self.assertEqual("INVALIDATED",
                         self.app.reviews.get_sheet(s2["sheet_id"])["status"])

    def test_recalculate_rules_invalidates_and_history_records_reason(self):
        r = self.serious_unlisted_intake()
        s = self.app.reviews.create_sheet([r["report"]["id"]])
        out = self.app.cases.recalculate_rules(actor="admin")
        self.assertGreaterEqual(out["cases_recalculated"], 1)
        sh = self.app.reviews.get_sheet(s["sheet_id"])
        self.assertEqual("INVALIDATED", sh["status"])
        self.assertEqual("RULE_RECALCULATED", sh["invalidated_reason"])
        hist = self.app.reviews.revision_history()
        codes = [e["reason_code"] for e in hist]
        self.assertIn("RULE_RECALCULATED", codes)

    def test_seriousness_change_appears_as_item(self):
        r = self.app.cases.intake("HOSPITAL", "H-SER-1", {
            "patient_name": "赵六", "dob": "1990-01-01",
            "drugs": [{"name": "ibuprofen"}], "reactions": ["nausea"],
            "seriousness": "不严重", "event_date": "2026-09-01"})
        f = self.app.cases.add_followup(r["case_id"], {"seriousness": "严重住院"})
        self.assertTrue(f["significant_changes"])
        s = self.app.reviews.create_sheet()
        sc = [i for i in s["items"] if i["item_type"] == "SERIOUSNESS_CHANGE"]
        self.assertEqual(1, len(sc))
        self.assertFalse(sc[0]["snapshot"]["from_serious"])
        self.assertTrue(sc[0]["snapshot"]["to_serious"])

    # -- 历史 / 审计查询 ------------------------------------------------------
    def test_history_and_audit_queries(self):
        r = self.serious_unlisted_intake()
        rid = r["report"]["id"]
        s = self.app.reviews.create_sheet([rid])
        self.app.reviews.approve(s["sheet_id"], "revA")
        self.app.reviews.veto(s["sheet_id"], "revB")
        hist = self.app.reviews.decision_history(report_id=rid, status="REJECTED")
        self.assertEqual(1, len(hist))
        self.assertEqual(s["sheet_id"], hist[0]["sheet_id"])
        audit = self.app.reviews.audit_trail(sheet_id=s["sheet_id"])
        etypes = [e["event_type"] for e in audit]
        for t in ("CREATED", "APPROVED", "VETOED", "VETOED_COMPLETED"):
            self.assertIn(t, etypes)
        approvals = self.app.reviews.dispatch_approvals()
        self.assertEqual([], approvals)  # 被否决的单不产生放行

    def test_decisions_mirrored_to_case_timeline_chain(self):
        r = self.serious_unlisted_intake()
        rid = r["report"]["id"]
        cid = r["case_id"]
        s = self.app.reviews.create_sheet([rid])
        self.approve_with_two(s["sheet_id"])
        tl = self.app.cases.timeline(cid)
        kinds = [e["event_type"] for e in tl]
        self.assertIn("REVIEW_COUNTERSIGNED", kinds)
        # 哈希链仍可逐帧核验
        from pv_service.audit import verify_chain
        verify_chain(tl)

        # 否决同样镜像
        r2 = self.app.cases.intake("HOSPITAL", "H-MIRROR-2", {
            "patient_name": "新患者", "dob": "2002-02-02",
            "patient_identifier": "ID-MIR-2",
            "drugs": [{"name": "warfarin"}], "reactions": ["皮疹"],
            "seriousness": "严重", "event_date": "2026-09-09"})
        s2 = self.app.reviews.create_sheet([r2["report"]["id"]])
        self.app.reviews.veto(s2["sheet_id"], "revX")
        tl2 = self.app.cases.timeline(r2["case_id"])
        self.assertIn("REVIEW_VETOED", [e["event_type"] for e in tl2])
        verify_chain(tl2)

    # -- 批量与失败重试 ------------------------------------------------------
    def test_one_approved_sheet_releases_all_frozen_reports(self):
        ids = []
        for i in range(3):
            r = self.serious_unlisted_intake(f"H-BATCH-{i}")
            ids.append(r["report"]["id"])
        s = self.app.reviews.create_sheet(ids)
        self.assertEqual(3, sum(1 for i in s["items"]
                                if i["item_type"] == "PENDING_REPORT"))
        self.approve_with_two(s["sheet_id"])
        pump = self.app.reporting.pump()
        self.assertEqual(3, len(pump["acked"]))

    def test_gate_survives_failure_and_requeue_but_not_new_report(self):
        from pv_service.gateway import RegulatorGateway
        # 网关先失败一次（可重试）
        gw_path = f"{self.app.data_dir}/gw2.db"
        gateway = RegulatorGateway(gw_path, clock=self.clock,
                                   fail_script={"fail_times": 1})
        self.app.reporting._gateway = gateway
        self.app.reporting._gateway_fn = None
        r = self.serious_unlisted_intake()
        rid = r["report"]["id"]
        s = self.app.reviews.create_sheet([rid])
        self.approve_with_two(s["sheet_id"])
        res = self.app.reporting.send_one(rid)
        self.assertEqual("RETRY_SCHEDULED", res["result"])
        # 闸门仍在，人工 requeue 后无需再会签
        self.app.reporting.requeue(rid)
        self.app.reporting._gateway = RegulatorGateway(gw_path, clock=self.clock)
        res = self.app.reporting.send_one(rid)
        self.assertEqual("ACKED", res["result"])
        # 修订后的新报告不在旧单冻结范围内，必须重新会签
        r2 = self.app.cases.intake("HOSPITAL", "H-GATE-NEW", {
            "patient_name": "新人", "dob": "2001-01-01",
            "patient_identifier": "ID-NEW",
            "drugs": [{"name": "warfarin"}], "reactions": ["皮疹"],
            "seriousness": "严重", "event_date": "2026-09-09"})
        with self.assertRaises(DomainError) as cm:
            self.app.reporting.send_one(r2["report"]["id"])
        self.assertEqual("COUNTERSIGN_REQUIRED", cm.exception.code)

    def test_voided_frozen_report_skipped_on_completion(self):
        r1 = self.app.cases.intake("HOSPITAL", "H-V1", {
            "patient_name": "同患者", "dob": "1980-01-01",
            "patient_identifier": "ID-DUP",
            "drugs": [{"name": "drug-x"}], "reactions": ["rx-y"],
            "seriousness": "mild", "event_date": "2026-09-01"})
        r2 = self.app.cases.intake("HOTLINE", "HL-V1", {
            "patient_name": "同患者别名", "dob": "1980-01-01",
            "patient_identifier": "ID-DUP",
            "drugs": [{"name": "drug-x"}], "reactions": ["rx-y"],
            "seriousness": "mild", "event_date": "2026-09-01"})
        rid2 = r2["report"]["id"]
        # 建单冻结 r2 的待发送决定（不被会签操作作废，内容不变）
        s = self.app.reviews.create_sheet([rid2])
        # 直接把报告置 VOID 而不触发修订，专门验证完成时跳过已 VOID 的冻结决定
        from pv_service import db as dbmod
        conn = dbmod.connect(self.app.db_path)
        try:
            with dbmod.transaction(conn):
                conn.execute(
                    "UPDATE regulatory_reports SET status='VOID' WHERE id=?", (rid2,))
        finally:
            conn.close()
        # 单子仍 OPEN，完成会签时跳过 VOID 决定，不产生放行走记录
        self.app.reviews.approve(s["sheet_id"], "revA")
        sh = self.app.reviews.approve(s["sheet_id"], "revB")
        self.assertEqual("APPROVED", sh["status"])
        self.assertEqual([], self.app.reviews.dispatch_approvals(rid2))


class TestCountersignCompat(CountersignBase):
    def test_disabled_by_default_old_callers_unaffected(self):
        self._make_app(countersign_required=False)
        r = self.serious_unlisted_intake()
        res = self.app.reporting.pump()
        self.assertEqual(1, len(res["acked"]))

    def test_explicit_compat_switch_allows_ungated(self):
        self._make_app(countersign_required=True, allow_countersign_compat=True)
        r = self.serious_unlisted_intake()
        rid = r["report"]["id"]
        # 不带开关仍拦截
        with self.assertRaises(DomainError):
            self.app.reporting.send_one(rid)
        # 显式兼容开关放行
        res = self.app.reporting.send_one(rid, allow_ungated=True)
        self.assertEqual("ACKED", res["result"])

    def test_compat_switch_ignored_when_not_enabled_on_server(self):
        self._make_app(countersign_required=True, allow_countersign_compat=False)
        r = self.serious_unlisted_intake()
        with self.assertRaises(DomainError) as cm:
            self.app.reporting.pump(allow_ungated=True)
        self.assertEqual("COUNTERSIGN_REQUIRED", cm.exception.code)


if __name__ == "__main__":
    unittest.main()
