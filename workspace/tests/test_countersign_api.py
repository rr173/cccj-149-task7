"""双人会签 HTTP/JSON 接口端到端测试。"""
import json
import threading
import urllib.request
import urllib.error

from tests.base import BasePVTest
from tests.test_api import ApiClient
from pv_service.api import create_server


class CountersignApiBase(BasePVTest):
    ENFORCE = False
    COMPAT = False

    def setUp(self):
        import tempfile
        import shutil
        from datetime import datetime
        from pv_service.app import Application, FixedClock
        self.dir2 = tempfile.mkdtemp(prefix="pv-csapi-")
        self.clock = FixedClock(datetime(2026, 9, 10, 9, 0, 0))
        self.app = Application(self.dir2, clock=self.clock,
                               countersign_required=self.ENFORCE,
                               allow_countersign_compat=self.COMPAT)
        self.app.seed_demo_reference()
        self.server = create_server(self.app, "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.api = ApiClient(f"http://127.0.0.1:{self.port}")

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        import shutil
        self.app.shutdown()
        shutil.rmtree(self.dir2, ignore_errors=True)

    def _serious_case(self, ext="H-API-1"):
        code, r = self.api.call("POST", "/api/v1/intake", {
            "source_system": "HOSPITAL", "external_id": ext,
            "payload": {
                "patient_name": "王五", "dob": "1970-05-05",
                "patient_identifier": "ID-API-1",
                "drugs": [{"name": "warfarin", "start_date": "2026-09-05"}],
                "reactions": ["皮疹"], "seriousness": "严重住院",
                "event_date": "2026-09-08"}})
        self.assertEqual(201, code)
        return r


class TestCountersignApiEnforced(CountersignApiBase):
    ENFORCE = True

    def test_full_review_cycle_over_http(self):
        r = self._serious_case()
        rid = r["report"]["id"]

        # 未会签 -> 派发 403
        code, body = self.api.call("POST", "/api/v1/reports/dispatch", {},
                                   expect_error=True)
        self.assertEqual(403, code)
        self.assertEqual("COUNTERSIGN_REQUIRED", body["error"])

        # 发起评审
        code, sh = self.api.call("POST", "/api/v1/reviews",
                                 {"report_ids": [rid], "requested_by": "init"})
        self.assertEqual(201, code)
        sid = sh["sheet_id"]
        self.assertEqual(1, sh["revision"])

        # 重复发起 -> 幂等同单
        code, sh2 = self.api.call("POST", "/api/v1/reviews", {"report_ids": [rid]})
        self.assertEqual(sid, sh2["sheet_id"])

        # 待办 + 领取
        code, todos = self.api.call("GET", "/api/v1/reviews/todos?reviewer=revA")
        self.assertEqual(1, len(todos))
        code, _ = self.api.call("POST", f"/api/v1/reviews/{sid}/claim",
                                {"reviewer": "revA"})
        self.assertEqual(200, code)
        code, err = self.api.call("POST", f"/api/v1/reviews/{sid}/claim",
                                  {"reviewer": "revB"}, expect_error=True)
        self.assertEqual(409, code)
        self.assertEqual("TODO_ALREADY_CLAIMED", err["error"])
        code, _ = self.api.call("POST", f"/api/v1/reviews/{sid}/release",
                                {"reviewer": "revA"})
        self.assertEqual(200, code)

        # 同人重复签署不累计
        code, _ = self.api.call("POST", f"/api/v1/reviews/{sid}/approve",
                                {"reviewer": "revA", "comment": "ok"})
        code, err = self.api.call("POST", f"/api/v1/reviews/{sid}/approve",
                                  {"reviewer": "revA"}, expect_error=True)
        self.assertEqual(409, code)
        self.assertEqual("ALREADY_SIGNED", err["error"])
        # 单人不足
        code, err = self.api.call("POST", "/api/v1/reports/dispatch", {},
                                  expect_error=True)
        self.assertEqual(403, code)

        # 第二人签署 -> APPROVED 放行
        code, sh = self.api.call("POST", f"/api/v1/reviews/{sid}/approve",
                                 {"reviewer": "revB"})
        self.assertEqual("APPROVED", sh["status"])
        code, pump = self.api.call("POST", "/api/v1/reports/dispatch", {})
        self.assertEqual(1, len(pump["acked"]))

        # 决定历史 / 审计 / 放行记录
        code, hist = self.api.call(
            "GET", f"/api/v1/reviews/history?report_id={rid}&status=APPROVED")
        self.assertEqual(1, len(hist))
        code, audit = self.api.call(
            "GET", f"/api/v1/reviews/audit?sheet_id={sid}")
        self.assertIn("APPROVED_COMPLETED", [e["event_type"] for e in audit])
        code, approvals = self.api.call(
            "GET", f"/api/v1/reviews/approvals?report_id={rid}")
        self.assertEqual(1, len(approvals))
        self.assertEqual({"revA", "revB"}, set(approvals[0]["approved_by"]))

    def test_veto_over_http(self):
        r = self._serious_case()
        sid = self.api.call("POST", "/api/v1/reviews",
                            {"report_ids": [r["report"]["id"]]})[1]["sheet_id"]
        self.api.call("POST", f"/api/v1/reviews/{sid}/approve",
                      {"reviewer": "revA"})
        code, sh = self.api.call("POST", f"/api/v1/reviews/{sid}/veto",
                                 {"reviewer": "revB", "reason": "缺材料"})
        self.assertEqual("REJECTED", sh["status"])
        code, err = self.api.call("POST", "/api/v1/reports/dispatch", {},
                                  expect_error=True)
        self.assertEqual(403, code)

    def test_invalidation_over_http(self):
        r = self._serious_case()
        rid = r["report"]["id"]
        sid = self.api.call("POST", "/api/v1/reviews", {"report_ids": [rid]})[1]
        sid = sid["sheet_id"]
        self.api.call("POST", f"/api/v1/reviews/{sid}/approve",
                      {"reviewer": "revA"})
        # 随访入库 -> 旧单失效
        code, f = self.api.call("POST", f"/api/v1/cases/{r['case_id']}/followup",
                                {"content": {"outcome": "死亡"}})
        self.assertEqual(201, code)
        code, sh = self.api.call("GET", f"/api/v1/reviews/{sid}")
        self.assertEqual("INVALIDATED", sh["status"])
        # 失效原因
        code, revs = self.api.call("GET", "/api/v1/reviews/revisions")
        self.assertIn("NEW_MATERIAL", [x["reason_code"] for x in revs])
        # 规则重算接口
        code, out = self.api.call("POST", "/api/v1/rules/recalculate",
                                  {"actor": "admin"})
        self.assertEqual(200, code)
        self.assertIn("revision", out)


class TestCountersignApiCompat(CountersignApiBase):
    ENFORCE = True
    COMPAT = True

    def test_explicit_compat_switch_in_dispatch_body(self):
        r = self._serious_case()
        code, err = self.api.call("POST", "/api/v1/reports/dispatch", {},
                                  expect_error=True)
        self.assertEqual(403, code)
        code, pump = self.api.call("POST", "/api/v1/reports/dispatch",
                                   {"allow_ungated": True})
        self.assertEqual(200, code)
        self.assertEqual(1, len(pump["acked"]))


class TestCountersignApiOff(CountersignApiBase):
    ENFORCE = False

    def test_reviews_available_but_dispatch_unaffected(self):
        r = self._serious_case()
        # 评审接口仍可用
        code, sh = self.api.call("POST", "/api/v1/reviews",
                                 {"report_ids": [r["report"]["id"]]})
        self.assertEqual(201, code)
        # 未会签也能发（兼容旧行为）
        code, pump = self.api.call("POST", "/api/v1/reports/dispatch", {})
        self.assertEqual(200, code)
        self.assertEqual(1, len(pump["acked"]))
        # allow_ungated 在未开兼容时无影响（本来就不强制）
