"""端到端恢复：模拟进程崩溃后用全新进程重开数据库。"""
import time

from tests.base import BasePVTest
from pv_service.app import Application
from pv_service.gateway import RegulatorGateway


class TestRestartRecovery(BasePVTest):
    def _serious(self, ext="E-K1"):
        return self.app.cases.intake("ENTERPRISE", ext, {
            "patient_name": "李四", "gender": "女", "dob": "1971-06-06",
            "drugs": [{"name": "atorvastatin"}], "reactions": ["肝损伤"],
            "seriousness": "严重", "seriousness_criteria": ["住院"],
            "event_date": "2026-09-09"})

    def test_new_process_recovers_sending_and_keeps_number(self):
        r = self._serious()
        number = r["report"]["regulatory_number"]
        # 模拟崩溃：网关已受理，本地停在 SENDING
        rid = r["report"]["id"]
        rep = self.app.reporting.get_report(rid)
        gw = RegulatorGateway(self.app.gateway_db, clock=self.clock)
        resp = gw.submit(number, rep["payload"])
        conn = self.db()
        conn.execute("UPDATE regulatory_reports SET status='SENDING' WHERE id=?", (rid,))

        # “新进程”：重建 Application（构造时自动 recover_stuck）
        app2 = Application(self.dir, clock=self.clock)
        rep2 = app2.reporting.get_report(rid)
        self.assertEqual("QUEUED", rep2["status"])
        res = app2.reporting.send_one()
        self.assertEqual("ACKED", res["result"])
        self.assertEqual(number, res["regulatory_number"])
        # 取到的是网关同一个 ack（幂等重放，而非新编号）
        self.assertEqual(resp["ack_id"], res["ack_id"])
        self.assertTrue(res["idempotent_replay"])
        # 恢复事件进入时间线
        types = [e["event_type"] for e in app2.cases.timeline(r["case_id"])]
        self.assertIn("REPORT_RECOVERED", types)
        app2.shutdown()

    def test_data_survives_full_restart_and_new_number_continues_sequence(self):
        r = self._serious("E-K2")
        app2 = Application(self.dir, clock=self.clock)
        # 旧数据可读
        detail = app2.cases.get_case(r["case_id"])
        self.assertEqual(1, len(detail["versions"]))
        # 新报告编号在序列上继续，不复用、不跳号
        r2 = app2.cases.intake("HOSPITAL", "H-K2", {
            "patient_name": "王五", "patient_identifier": "ID-Z",
            "drugs": [{"name": "warfarin"}], "reactions": ["肝损伤"],
            "seriousness": "严重", "event_date": "2026-09-08"})
        self.assertEqual(int(r["report"]["regulatory_number"][-5:]) + 1,
                         int(r2["report"]["regulatory_number"][-5:]))
        app2.shutdown()

    def test_background_dispatcher_eventually_acks(self):
        app2 = Application(self.dir, clock=self.clock, auto_dispatch=True,
                           poll_seconds=0.05)
        r = self._serious("E-K3")
        rid = r["report"]["id"]
        deadline = time.time() + 5
        while time.time() < deadline:
            if app2.reporting.get_report(rid)["status"] == "ACKED":
                break
            time.sleep(0.05)
        self.assertEqual("ACKED", app2.reporting.get_report(rid)["status"])
        app2.shutdown()
