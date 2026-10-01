"""监管报送：编号唯一、重试幂等、崩溃恢复、加急优先、人工重发。"""
import json
import sqlite3

from pv_service.app import Application, FixedClock
from pv_service.gateway import RegulatorGateway
from pv_service.reporting import ReportingService
from tests.base import BasePVTest


class TestReporting(BasePVTest):
    def _serious_unlisted_case(self):
        return self.app.cases.intake("ENTERPRISE", "E-R1", {
            "patient_name": "李四", "gender": "女", "dob": "1971-06-06",
            "drugs": [{"name": "atorvastatin"}], "reactions": ["肝损伤"],
            "seriousness": "严重", "seriousness_criteria": ["住院"],
            "event_date": "2026-09-09"})

    def test_initial_ack_records_receipt_on_timeline(self):
        r = self._serious_unlisted_case()
        res = self.app.reporting.send_one()
        self.assertEqual("ACKED", res["result"])
        rep = self.app.reporting.get_report(res["report_id"])
        self.assertEqual("ACKED", rep["status"])
        self.assertTrue(rep["ack_id"].startswith("ACK-"))
        tl = [e["event_type"] for e in self.app.cases.timeline(r["case_id"])]
        self.assertIn("REPORT_ACKED", tl)

    def test_expedited_reports_dispatched_first(self):
        # 先建两个非严重，再建一个严重非预期；队列里加急应先被取走
        for i in range(2):
            self.app.cases.intake("HOSPITAL", f"H-Q{i}", {
                "patient_name": f"病人{i}", "dob": "1990-01-01",
                "drugs": [{"name": "x"}], "reactions": ["nausea"],
                "seriousness": "mild", "event_date": "2026-09-01"})
        s = self._serious_unlisted_case()
        first = self.app.reporting.send_one()
        rep = self.app.reporting.get_report(first["report_id"])
        self.assertEqual(1, rep["expedited"])
        self.assertEqual(s["report"]["id"], rep["id"])

    def test_failure_retry_keeps_same_regulatory_number(self):
        # 网关前 2 次失败，第 3 次成功
        self.app.reporting._gateway_fn = None
        app2 = Application(self.dir, clock=self.clock,
                           gateway_fail_script={"fail_times": 2})
        r = app2.cases.intake("ENTERPRISE", "E-R2", {
            "patient_name": "王五", "drugs": [{"name": "warfarin"}],
            "reactions": ["肝损伤"], "seriousness": "严重",
            "event_date": "2026-09-08"})
        number = r["report"]["regulatory_number"]
        res1 = app2.reporting.send_one()
        self.assertEqual("RETRY_SCHEDULED", res1["result"])
        self.assertEqual(number, res1["regulatory_number"])
        # 退避未到不发送
        self.assertIsNone(app2.reporting.send_one())
        self.clock.advance(seconds=6)
        res2 = app2.reporting.send_one()
        self.assertEqual("RETRY_SCHEDULED", res2["result"])
        self.assertEqual(number, res2["regulatory_number"])
        self.clock.advance(seconds=31)
        res3 = app2.reporting.send_one()
        self.assertEqual("ACKED", res3["result"])
        self.assertEqual(number, res3["regulatory_number"])
        # 网关侧只有一张回执（同一编号幂等）
        gw = RegulatorGateway(app2.gateway_db, clock=self.clock)
        self.assertEqual(1, gw.receipt_count())
        app2.shutdown()

    def test_restart_recovery_does_not_duplicate_number(self):
        # 模拟：报告卡在 SENDING 后进程重启
        r = self._serious_unlisted_case()
        rid = r["report"]["id"]
        conn = self.db()
        conn.execute("UPDATE regulatory_reports SET status='SENDING', attempts=1 WHERE id=?",
                     (rid,))
        number_before = conn.execute(
            "SELECT regulatory_number FROM regulatory_reports WHERE id=?", (rid,)
        ).fetchone()[0]

        # 新进程：重建 ReportingService 自动恢复
        rs = ReportingService(self.app.db_path,
                              gateway_fn=lambda: RegulatorGateway(self.app.gateway_db,
                                                                  clock=self.clock),
                              clock=self.clock)
        rep = rs.get_report(rid)
        self.assertEqual("QUEUED", rep["status"])
        self.assertEqual(number_before, rep["regulatory_number"])
        res = rs.send_one()
        self.assertEqual("ACKED", res["result"])
        self.assertEqual(number_before, res["regulatory_number"])
        # 序列未跳过、无重复
        numbers = [row[0] for row in self.db().execute(
            "SELECT regulatory_number FROM regulatory_reports")]
        self.assertEqual(len(numbers), len(set(numbers)))

    def test_ack_window_crash_replay_returns_same_ack(self):
        # 网关已受理但本地记录前“崩溃”：重放必须拿到同一个 ack 而不是新建编号
        r = self._serious_unlisted_case()
        rid = r["report"]["id"]
        rep = self.app.reporting.get_report(rid)
        gw = RegulatorGateway(self.app.gateway_db, clock=self.clock)
        first = gw.submit(rep["regulatory_number"], rep["payload"])
        second = gw.submit(rep["regulatory_number"], rep["payload"])
        self.assertEqual(first["ack_id"], second["ack_id"])
        self.assertTrue(second["idempotent_replay"])
        self.assertEqual(1, gw.receipt_count())

    def test_permanent_failure_marks_failed_and_keeps_number(self):
        app2 = Application(self.dir, clock=self.clock,
                           gateway_fail_script={"fail_times": 0})
        r = app2.cases.intake("ENTERPRISE", "E-R3", {
            "patient_name": "赵六", "drugs": [{"name": "x"}],
            "reactions": ["y"], "seriousness": "mild", "event_date": "2026-09-01"})
        rid = r["report"]["id"]
        number = r["report"]["regulatory_number"]
        # 用永久拒收的假网关替换该线程网关
        from pv_service.gateway import GatewayError

        class PermanentGateway(RegulatorGateway):
            def submit(self, n, p):
                raise GatewayError("500 permanent", retryable=False)

        rs = app2.reporting
        rs._gw_local.gw = PermanentGateway(self.app.gateway_db, clock=self.clock)
        res = rs.send_one(rid)
        self.assertEqual("FAILED", res["result"])
        self.assertEqual(number, res["regulatory_number"])
        # 人工重发：复用同一编号（requeue 不新建行、不新建编号）
        app2.reporting.requeue(rid)
        self.assertEqual(number, app2.reporting.get_report(rid)["regulatory_number"])
        app2.shutdown()

    def test_numbers_unique_under_repeated_dispatch(self):
        for i in range(5):
            self._serious_unlisted_case()
            self.app.cases.intake("HOSPITAL", f"H-U{i}", {
                "patient_name": f"n{i}", "drugs": [{"name": "x"}],
                "reactions": ["y"], "seriousness": "mild", "event_date": "2026-09-01"})
        self.app.reporting.pump(max_batches=20)
        rows = self.db().execute(
            "SELECT regulatory_number, COUNT(*) c FROM regulatory_reports "
            "GROUP BY regulatory_number HAVING c>1").fetchall()
        self.assertEqual([], rows)

    def test_voided_report_is_never_sent(self):
        r1 = self.app.cases.intake("HOSPITAL", "H-V1", {
            "patient_name": "张三", "patient_identifier": "ID-998",
            "drugs": [{"name": "布洛芬"}], "reactions": ["皮疹"],
            "seriousness": "不严重", "event_date": "2026-09-07"})
        r2 = self.app.cases.intake("HOTLINE", "HL-V1", {
            "patient_name": "老张", "patient_identifier": "ID-998",
            "drugs": [{"name": "ibuprofen"}], "reactions": ["rash"],
            "seriousness": "mild", "event_date": "2026-09-07"})
        cid = r2["duplicate_clusters"][0]["cluster_id"]
        m = self.app.cases.confirm_cluster(cid, actor="liu")
        voided = m["voided_reports"][0]
        res = self.app.reporting.send_one(voided["id"])
        self.assertEqual("VOID", res["result"])
        self.app.reporting.pump()
        # 只有 survivor 的报告送达；被作废编号从未到达网关（不会 2 份）
        gw = RegulatorGateway(self.app.gateway_db, clock=self.clock)
        self.assertEqual(1, gw.receipt_count())
        receipts = {row[0] for row in gw.conn.execute(
            "SELECT regulatory_number FROM gateway_receipts")}
        self.assertNotIn(voided["regulatory_number"], receipts)

    def test_report_version_uniqueness_blocks_duplicate_planning(self):
        # 数据库层：同一病例版本至多一份报告
        r = self._serious_unlisted_case()
        rid = r["report"]["id"]
        version_id = self.app.reporting.get_report(rid)["case_version_id"]
        conn = self.db()
        with self.assertRaises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO regulatory_reports(id,case_id,case_version_id,report_type,"
                "regulatory_number,status,expedited,days_due,due_date,decision_json,"
                "payload_json,created_at,updated_at) "
                "VALUES('x',?,?, 'INITIAL','DUP-1','QUEUED',0,90,'2026-12-01','{}','{}',?,?)",
                (r["case_id"], version_id, "2026-09-10", "2026-09-10"))
        conn.rollback()
