"""规则治理服务：登记校验、试算、受控启用、已发送不可改、回滚、定时、审计。"""
import unittest

from tests.base import BasePVTest
from pv_service.rule_pkg import build_content, LEGACY_PACKAGE_ID
from pv_service.service import DomainError

SECRET = "pv-demo-signing-secret"


def make_content(version="2026.1", **over):
    params = dict(jurisdiction="CN", version=version,
                  effective_from="2026-10-01",
                  serious_days=10, listed_days=20, routine_days=45)
    params.update(over)
    return build_content(**params)


def serious_payload(i=1):
    return {"patient_name": f"张三{i}", "dob": "1980-03-03",
            "drugs": [{"name": "布洛芬", "causality": "可能"}],
            "reactions": ["皮疹"], "seriousness": "严重",
            "event_date": "2026-09-07"}


def nonserious_payload(i=2):
    p = serious_payload(i)
    p["seriousness"] = "不严重"
    return p


class RuleGovernanceTest(BasePVTest):
    def setUp(self):
        super().setUp()
        self.gov = self.app.rules_gov

    def _reg(self, content):
        return self.gov.register_package(
            content, self.gov.sign(content, SECRET), registered_by="reg")

    # -- 登记与三校验 --------------------------------------------------------
    def test_register_and_get(self):
        c = make_content()
        pkg = self._reg(c)
        self.assertFalse(pkg["deduped"])
        self.assertEqual(pkg["version"], "2026.1")
        got = self.gov.get_package(package_row_id=pkg["id"])
        self.assertEqual(got["content_hash"], pkg["content_hash"])

    def test_same_content_returns_original_version(self):
        c = make_content()
        p1 = self._reg(c)
        p2 = self._reg(c)
        self.assertTrue(p2["deduped"])
        self.assertEqual(p1["id"], p2["id"])

    def test_different_content_same_version_rejected(self):
        self._reg(make_content(routine_days=45))
        c2 = make_content(routine_days=46)
        with self.assertRaises(DomainError) as cm:
            self._reg(c2)
        self.assertEqual(cm.exception.code, "VERSION_CONFLICT")
        self.assertEqual(cm.exception.status, 409)

    def test_interval_conflict_rejected(self):
        self._reg(make_content(version="2026.1"))
        c2 = make_content(version="2026.2", effective_from="2026-11-01")
        with self.assertRaises(DomainError) as cm:
            self._reg(c2)
        self.assertEqual(cm.exception.code, "INTERVAL_CONFLICT")
        # 首尾相接的下一年区间合法
        c3 = make_content(version="2027.1", effective_from="2027-01-01",
                          effective_to=None)
        # 2026.1 是开放区间 -> 仍冲突；先给 2026.1 一个 closed end 才行。
        with self.assertRaises(DomainError):
            self._reg(c3)

    def test_closed_intervals_can_abut(self):
        self._reg(make_content(version="2026.1", effective_to="2027-01-01"))
        c2 = make_content(version="2027.1", effective_from="2027-01-01")
        pkg = self._reg(c2)
        self.assertFalse(pkg["deduped"])

    def test_bad_signature_rejected(self):
        c = make_content()
        with self.assertRaises(DomainError) as cm:
            self.gov.register_package(c, "deadbeef")
        self.assertEqual(cm.exception.code, "BAD_SIGNATURE")

    def test_unknown_signer_rejected(self):
        c = make_content()
        with self.assertRaises(DomainError) as cm:
            self.gov.register_package(c, "00" * 16, signer_id="nobody")
        self.assertEqual(cm.exception.code, "UNKNOWN_SIGNER")

    def test_dependency_must_exist(self):
        c = make_content()
        c["depends_on"] = [{"package": "CN", "version": "9.9"}]
        with self.assertRaises(DomainError) as cm:
            self.gov.register_package(c, self.gov.sign(c, SECRET))
        self.assertEqual(cm.exception.code, "DEPENDENCY_MISSING")
        # 依赖已登记后通过
        base = make_content(version="1.0", effective_from="2026-01-01",
                            effective_to="2026-10-01", routine_days=90)
        self._reg(base)
        c["depends_on"] = [{"package": "CN", "version": "1.0"}]
        self.assertTrue(self._reg(c)["id"])

    def test_package_immutable(self):
        pkg = self._reg(make_content())
        conn = self.db()
        with self.assertRaises(sqlite_error()):
            conn.execute("UPDATE rule_packages SET summary='x' WHERE id=?",
                         (pkg["id"],))

    # -- 试算（只读）---------------------------------------------------------
    def _two_cases(self):
        r1 = self.app.cases.intake("HOSPITAL", "H-1", serious_payload(1),
                                   "2026-09-10T09:00:00")
        r2 = self.app.cases.intake("HOSPITAL", "H-2", nonserious_payload(2),
                                   "2026-09-10T10:00:00")
        return r1["report"]["id"], r2["report"]["id"]

    def test_trial_diffs_and_affected_set_readonly(self):
        rid, rid2 = self._two_cases()
        self._reg(make_content())
        t = self.gov.create_trial("CN", "2026.1")
        t = self.gov.run_trial_until_done(t["trial_id"])
        self.assertEqual(t["status"], "COMPLETED")
        self.assertEqual(t["total_units"], 2)
        self.assertEqual(t["changed_units"], 2)
        prog = self.gov.trial_progress(t["trial_id"])
        self.assertEqual(len(prog["affected_case_ids"]), 2)
        diffs = self.gov.trial_diffs(t["trial_id"])
        days_changes = {d["case_id"]: d["diff"]["fields"]["days_due"]["new"]
                        for d in diffs}
        # 严重 listed 30 -> 20；非严重 90 -> 45
        self.assertIn(20, days_changes.values())
        self.assertIn(45, days_changes.values())
        # 试算不改报告
        rep = self.app.reporting.get_report(rid)
        self.assertEqual(rep["days_due"], 30)
        self.assertEqual(rep["rule_version"], "0")

    def test_trial_idempotent_create(self):
        self._two_cases()
        self._reg(make_content())
        t1 = self.gov.create_trial("CN", "2026.1", idempotency_key="k")
        t2 = self.gov.create_trial("CN", "2026.1", idempotency_key="k")
        self.assertEqual(t1["trial_id"], t2["trial_id"])
        self.assertTrue(t2["deduped"])

    # -- 启用：未发送重算 / 已发送只更正 -------------------------------------
    def test_activate_recalcs_unsent_keeps_sent(self):
        rid, rid2 = self._two_cases()
        # rid 发送（严重，30 日）；rid2 未发送（90 日）
        self.assertEqual(self.app.reporting.send_one(rid)["result"], "ACKED")
        self._reg(make_content())
        act = self.gov.activate("CN", "2026.1", actor="officer")
        self.assertEqual(act["status"], "COMPLETED")
        self.assertEqual(act["job"]["status"], "COMPLETED")

        # 已发送：原版本与历史结果保留
        sent = self.app.reporting.get_report(rid)
        self.assertEqual(sent["status"], "ACKED")
        self.assertEqual(sent["rule_package_id"], LEGACY_PACKAGE_ID)
        self.assertEqual(sent["days_due"], 30)
        # 仅生成可复核更正候选（候选口径 20 日）
        corrs = self.gov.correction_candidates(report_id=rid)
        self.assertEqual(len(corrs), 1)
        self.assertEqual(corrs[0]["candidate"]["decision"]["days_due"], 20)
        self.assertEqual(corrs[0]["original"]["decision"]["days_due"], 30)

        # 未发送：按新版本重算
        unsent = self.app.reporting.get_report(rid2)
        self.assertEqual(unsent["status"], "QUEUED")
        self.assertEqual(unsent["days_due"], 45)
        self.assertEqual(unsent["rule_package_id"], "CN")
        self.assertEqual(unsent["rule_version"], "2026.1")
        # 监管编号不变
        self.assertIsNotNone(unsent["regulatory_number"])

        cur = self.gov.current_version("CN")
        self.assertEqual(cur["version"], "2026.1")
        self.assertEqual(cur["current_seq"], 1)

    def test_new_intake_after_activation_uses_new_rule(self):
        self._reg(make_content())
        self.gov.activate("CN", "2026.1")
        r = self.app.cases.intake("HOSPITAL", "H-N", nonserious_payload(9),
                                  "2026-10-02T09:00:00")
        rep = self.app.reporting.get_report(r["report"]["id"])
        self.assertEqual(rep["days_due"], 45)
        self.assertEqual(rep["rule_version"], "2026.1")

    def test_activate_when_new_rule_says_no_report_voids_unsent(self):
        from pv_service.rules import Decision
        rid = self.app.cases.intake(
            "HOSPITAL", "H-1", nonserious_payload(1),
            "2026-09-10T09:00:00")["report"]["id"]
        number_before = self.app.reporting.get_report(rid)["regulatory_number"]
        self._reg(make_content())

        class NoReportEngine:
            def evaluate(self, ctx):
                return Decision(False, False, 0, "", "R9_NO_REPORT",
                                "候选口径判定该情形无需报送")

        self.gov.engine_override = NoReportEngine()
        try:
            self.gov.activate("CN", "2026.1")
        finally:
            self.gov.engine_override = None

        rep = self.app.reporting.get_report(rid)
        # 未发送决定在新版本下无需报送：VOID（编号保留），不覆盖、不发送
        self.assertEqual(rep["status"], "VOID")
        self.assertEqual(rep["regulatory_number"], number_before)
        self.assertEqual(rep["rule_version"], "2026.1")
        self.assertFalse(rep["decision"]["should_report"])

    def test_reactivation_idempotent_with_same_key(self):
        self._two_cases()
        self._reg(make_content())
        a1 = self.gov.activate("CN", "2026.1", idempotency_key="fixed")
        a2 = self.gov.activate("CN", "2026.1", idempotency_key="fixed")
        self.assertEqual(a1["activation_id"], a2["activation_id"])
        self.assertTrue(a2["deduped"])

    def test_optimistic_concurrency_stale_view_rejected(self):
        self._reg(make_content())
        with self.assertRaises(DomainError) as cm:
            self.gov.activate("CN", "2026.1",
                              expected_package_id="CN", expected_version="999")
        self.assertEqual(cm.exception.code, "VERSION_MOVED")

    # -- 回滚 ----------------------------------------------------------------
    def test_rollback_restores_previous_version(self):
        rid, rid2 = self._two_cases()
        self._reg(make_content(version="2026.1", effective_to="2027-01-01"))
        c2 = make_content(version="2027.1", effective_from="2027-01-01",
                          routine_days=40)
        self._reg(c2)
        self.gov.activate("CN", "2026.1")
        self.assertEqual(self.app.reporting.get_report(rid2)["days_due"], 45)
        self.gov.activate("CN", "2027.1")
        self.assertEqual(self.app.reporting.get_report(rid2)["days_due"], 40)
        # 回滚到上一版本（2026.1）
        rb = self.gov.rollback(actor="officer")
        self.assertEqual(rb["package_id"], "CN")
        self.assertEqual(rb["version"], "2026.1")
        self.assertEqual(self.app.reporting.get_report(rid2)["days_due"], 45)

    def test_rollback_to_legacy_when_no_prior(self):
        rid, _ = self._two_cases()
        self._reg(make_content())
        self.gov.activate("CN", "2026.1")
        rb = self.gov.rollback()
        self.assertEqual(rb["package_id"], LEGACY_PACKAGE_ID)
        self.assertEqual(rb["version"], "0")
        self.assertEqual(self.app.reporting.get_report(rid)["days_due"], 30)

    # -- 定时 ----------------------------------------------------------------
    def test_schedule_fires_at_due_time(self):
        self.app.cases.intake("HOSPITAL", "H-1", nonserious_payload(1),
                              "2026-09-10T09:00:00")
        self._reg(make_content(version="2027.1", effective_from="2027-01-01"))
        sch = self.gov.schedule("CN", "2027.1", "2027-01-01T00:00:00")
        self.assertEqual(sch["status"], "PENDING")
        # 未到点不触发
        self.assertEqual(self.gov.fire_due_schedules(now="2026-12-31T23:59:59"), [])
        self.assertEqual(self.gov.current_version("CN")["version"], "0")
        fired = self.gov.fire_due_schedules(now="2027-01-01T00:00:00")
        self.assertEqual(len(fired), 1)
        self.assertEqual(fired[0]["kind"], "SCHEDULED")
        self.assertEqual(self.gov.current_version("CN")["version"], "2027.1")
        # 计划已 FIRED 且关联启用记录；重复触发无新结果
        self.assertEqual(self.gov.list_schedules()[0]["status"], "FIRED")
        self.assertEqual(self.gov.fire_due_schedules(now="2027-01-02T00:00:00"), [])

    def test_only_one_pending_schedule(self):
        self._reg(make_content(version="2026.1", effective_to="2027-01-01"))
        c2 = make_content(version="2027.1", effective_from="2027-01-01",
                          routine_days=40)
        self._reg(c2)
        self.gov.schedule("CN", "2026.1", "2026-12-01T00:00:00")
        with self.assertRaises(DomainError) as cm:
            self.gov.schedule("CN", "2027.1", "2027-01-01T00:00:00")
        self.assertEqual(cm.exception.code, "SCHEDULE_EXISTS")

    def test_cancel_schedule(self):
        self._reg(make_content(version="2027.1", effective_from="2027-01-01"))
        sid = self.gov.schedule("CN", "2027.1", "2027-01-01T00:00:00")["schedule_id"]
        self.assertEqual(self.gov.cancel_schedule(sid)["status"], "CANCELLED")
        self.assertEqual(self.gov.fire_due_schedules(now="2027-02-01T00:00:00"), [])

    # -- 审计链 --------------------------------------------------------------
    def test_audit_chain_records_and_verifies(self):
        self._two_cases()
        c = make_content()
        self._reg(c)
        self.gov.activate("CN", "2026.1")
        self.gov.rollback()
        types = {e["event_type"] for e in self.gov.audit_trail(limit=1000)}
        self.assertIn("RULE_PACKAGE_REGISTERED", types)
        self.assertIn("RULE_ACTIVATED", types)
        self.assertIn("RULE_ROLLED_BACK", types)
        report = self.gov.verify_audit_chain()
        self.assertTrue(report["valid"])
        self.assertGreaterEqual(report["events"], 3)


def sqlite_error():
    import sqlite3
    return sqlite3.IntegrityError


if __name__ == "__main__":
    unittest.main()
