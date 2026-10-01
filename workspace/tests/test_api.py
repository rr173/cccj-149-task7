"""HTTP/JSON 接口端到端测试（真实起服 + urllib 客户端）。"""
import json
import threading
import urllib.request
import urllib.error

from tests.base import BasePVTest
from pv_service.api import create_server


class ApiClient:
    def __init__(self, base):
        self.base = base

    def call(self, method, path, body=None, expect_error=False):
        data = json.dumps(body or {}).encode("utf-8")
        req = urllib.request.Request(self.base + path, data=data if method == "POST" else None,
                                     method=method,
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status, json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            payload = json.loads(e.read().decode("utf-8"))
            if expect_error:
                return e.code, payload
            raise AssertionError(f"{method} {path} -> {e.code}: {payload}")


class TestApi(BasePVTest):
    def setUp(self):
        super().setUp()
        self.server = create_server(self.app, "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.api = ApiClient(f"http://127.0.0.1:{self.port}")

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        super().tearDown()

    def test_health(self):
        code, body = self.api.call("GET", "/health")
        self.assertEqual(200, code)
        self.assertEqual("ok", body["status"])

    def test_full_workflow_over_http(self):
        payload = self.zhang_patient()
        code, r1 = self.api.call("POST", "/api/v1/intake",
                                 {"source_system": "HOSPITAL", "external_id": "H-W1",
                                  "payload": payload})
        self.assertEqual(201, code)
        cid1 = r1["case_id"]

        code, r2 = self.api.call("POST", "/api/v1/intake",
                                 {"source_system": "HOTLINE", "external_id": "HL-W1",
                                  "payload": self.zhang_multilang()})
        self.assertEqual(201, code)
        self.assertTrue(r2["duplicate_clusters"])
        cluster_id = r2["duplicate_clusters"][0]["cluster_id"]

        # 工作台能看到待复核簇
        code, wb = self.api.call("GET", "/api/v1/cases")
        self.assertEqual(200, code)
        flagged = [c for c in wb if c["open_duplicate_clusters"]]
        self.assertEqual(2, len(flagged))

        # 复核簇列表
        code, clusters = self.api.call("GET", "/api/v1/duplicates")
        self.assertEqual(1, len(clusters))

        # 确认合并
        code, m = self.api.call("POST", f"/api/v1/duplicates/{cluster_id}/confirm",
                                {"actor": "officer-liu"})
        self.assertEqual(cid1, m["survivor_case_id"])
        self.assertEqual(1, len(m["voided_reports"]))

        # 随访升级为加急
        code, f = self.api.call("POST", f"/api/v1/cases/{cid1}/followup",
                                {"content": {"seriousness": "严重",
                                             "seriousness_criteria": ["住院"],
                                             "new_reactions": ["肝损伤"],
                                             "causality": "很可能"}})
        self.assertEqual(201, code)
        self.assertTrue(f["report"])

        # 派发
        code, pump = self.api.call("POST", "/api/v1/reports/dispatch", {})
        acked = pump["acked"]
        self.assertGreaterEqual(len(acked), 1)

        # 报告详情含回执
        rid = f["report"]["id"]
        code, rep = self.api.call("GET", f"/api/v1/reports/{rid}")
        self.assertEqual(200, code)
        self.assertEqual("ACKED", rep["status"])
        self.assertTrue(rep["ack"]["ack_id"])

        # 时间线
        code, detail = self.api.call("GET", f"/api/v1/cases/{cid1}")
        types = [e["event_type"] for e in detail["timeline"]]
        for t in ("SOURCE_INTAKE", "DUPLICATE_CONFIRMED", "REPORT_QUEUED_EXPEDITED",
                  "REPORT_ACKED", "FOLLOWUP_RECEIVED", "REPORT_VOIDED_BY_MERGE"):
            self.assertIn(t, types)

    def test_idempotent_intake_same_external_id(self):
        body = {"source_system": "ENTERPRISE", "external_id": "DUP-1",
                "payload": self.zhang_patient()}
        _, a = self.api.call("POST", "/api/v1/intake", body)
        _, b = self.api.call("POST", "/api/v1/intake", body)
        self.assertTrue(b["idempotent"])
        self.assertEqual(a["case_id"], b["case_id"])

    def test_404_unknown_case(self):
        code, body = self.api.call("GET", "/api/v1/cases/nope", expect_error=True)
        self.assertEqual(404, code)
        self.assertEqual("CASE_NOT_FOUND", body["error"])

    def test_400_bad_source(self):
        code, body = self.api.call("POST", "/api/v1/intake",
                                   {"source_system": "FAX", "external_id": "1",
                                    "payload": {}}, expect_error=True)
        self.assertEqual(400, code)

    def test_reports_due_view(self):
        self.api.call("POST", "/api/v1/intake",
                      {"source_system": "HOSPITAL", "external_id": "H-D1",
                       "payload": self.zhang_patient()})
        code, due = self.api.call("GET", "/api/v1/reports/due")
        self.assertEqual(200, code)
        self.assertEqual(1, len(due))
        self.assertEqual("QUEUED", due[0]["status"])
