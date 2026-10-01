"""药物安全法规规则治理 HTTP 接口测试。"""
import json
import threading
import urllib.request
import urllib.error

from tests.base import BasePVTest
from pv_service.api import create_server
from pv_service.rule_pkg import build_content, sign_content

SECRET = "pv-demo-signing-secret"


class RuleApiTest(BasePVTest):
    def setUp(self):
        super().setUp()
        self.server = create_server(self.app, "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        self.base = f"http://127.0.0.1:{self.port}/api/v1"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        # 一个病例（非严重 90 日）
        self.app.cases.intake(
            "HOSPITAL", "H-1",
            {"patient_name": "P1", "dob": "1980-01-01",
             "drugs": [{"name": "布洛芬", "causality": "可能"}],
             "reactions": ["皮疹"], "seriousness": "不严重",
             "event_date": "2026-09-07"}, "2026-09-10T09:00:00")

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        super().tearDown()

    def _req(self, method, path, body=None):
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(
            self.base + path, data=data, method=method,
            headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            return e.code, json.loads(e.read().decode("utf-8"))

    def _content(self, **over):
        params = dict(jurisdiction="CN", version="2026.1",
                      effective_from="2026-10-01", routine_days=45)
        params.update(over)
        return build_content(**params)

    def test_register_list_get(self):
        c = self._content()
        code, pkg = self._req("POST", "/rule-packages",
                              {"content": c,
                               "signature": sign_content(c, SECRET)})
        self.assertEqual(code, 201)
        code, lst = self._req("GET", "/rule-packages?jurisdiction=CN")
        self.assertEqual(code, 200)
        self.assertGreaterEqual(len(lst), 1)
        code, got = self._req("GET", f"/rule-packages/{pkg['id']}")
        self.assertEqual(code, 200)
        self.assertEqual(got["content_hash"], pkg["content_hash"])

    def test_register_bad_signature_422(self):
        code, body = self._req("POST", "/rule-packages",
                               {"content": self._content(), "signature": "xx"})
        self.assertEqual(code, 422)
        self.assertEqual(body["error"], "BAD_SIGNATURE")

    def test_trial_flow(self):
        c = self._content()
        self._req("POST", "/rule-packages",
                  {"content": c, "signature": sign_content(c, SECRET)})
        code, trial = self._req("POST", "/rules/trials",
                                {"candidate": {"package_id": "CN",
                                               "version": "2026.1"}})
        self.assertIn(code, (201, 202))
        tid = trial["trial_id"]
        if trial["status"] != "COMPLETED":
            code, _ = self._req("POST", f"/rules/trials/{tid}/run", {})
            self.assertEqual(code, 200)
        code, prog = self._req("GET", f"/rules/trials/{tid}/progress")
        self.assertEqual(code, 200)
        self.assertGreaterEqual(prog["affected_report_count"], 1)
        code, diffs = self._req("GET", f"/rules/trials/{tid}/diffs")
        self.assertEqual(code, 200)
        self.assertTrue(any(d["diff"]["changed"] for d in diffs))

    def test_activate_current_and_corrections(self):
        # 先发送报告（ACKED）
        rid = self.app.reporting.due()[0]["id"]
        self.app.reporting.send_one(rid)
        c = self._content(listed_days=20)
        self._req("POST", "/rule-packages",
                  {"content": c, "signature": sign_content(c, SECRET)})
        code, act = self._req("POST", "/rules/activate",
                              {"target": {"package_id": "CN",
                                          "version": "2026.1"},
                               "actor": "officer"})
        self.assertEqual(code, 200)
        self.assertEqual(act["job"]["status"], "COMPLETED")
        code, cur = self._req("GET", "/rules/current?jurisdiction=CN")
        self.assertEqual(code, 200)
        self.assertEqual(cur["version"], "2026.1")
        code, corrs = self._req("GET", f"/rules/corrections?report_id={rid}")
        self.assertEqual(code, 200)
        self.assertEqual(len(corrs), 1)

    def test_rollback_and_audit(self):
        c = self._content()
        self._req("POST", "/rule-packages",
                  {"content": c, "signature": sign_content(c, SECRET)})
        self._req("POST", "/rules/activate",
                  {"target": {"package_id": "CN", "version": "2026.1"}})
        code, rb = self._req("POST", "/rules/rollback", {})
        self.assertEqual(code, 200)
        self.assertEqual(rb["version"], "0")
        code, ver = self._req("GET", "/rules/audit/verify")
        self.assertEqual(code, 200)
        self.assertTrue(ver["valid"])

    def test_schedule_create_cancel(self):
        c = self._content(version="2027.1", effective_from="2027-01-01")
        self._req("POST", "/rule-packages",
                  {"content": c, "signature": sign_content(c, SECRET)})
        code, sch = self._req("POST", "/rules/schedules",
                              {"target": {"package_id": "CN", "version": "2027.1"},
                               "effective_at": "2027-01-01T00:00:00"})
        self.assertEqual(code, 201)
        sid = sch["schedule_id"]
        code, _ = self._req("POST", f"/rules/schedules/{sid}/cancel", {})
        self.assertEqual(code, 200)

    def test_recover_endpoint(self):
        code, body = self._req("POST", "/rules/recover", {"resume": True})
        self.assertEqual(code, 200)
        self.assertIn("trials_reset", body)


if __name__ == "__main__":
    unittest.main()
