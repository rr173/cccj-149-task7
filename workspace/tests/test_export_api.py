"""证据包导出/下载/核验/时间线 HTTP 接口端到端测试。"""
import json
import threading
import urllib.request
import urllib.error

from tests.base import BasePVTest
from pv_service.api import create_server
from pv_service.verifier import verify_package


class ApiClient:
    def __init__(self, base):
        self.base = base

    def call(self, method, path, body=None, raw=False):
        data = json.dumps(body or {}).encode("utf-8")
        req = urllib.request.Request(
            self.base + path, data=data if method == "POST" else None,
            method=method, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                payload = resp.read()
                if raw:
                    return resp.status, dict(resp.headers), payload
                return resp.status, json.loads(payload.decode("utf-8"))
        except urllib.error.HTTPError as e:
            payload = e.read()
            if raw:
                return e.code, dict(e.headers), payload
            return e.code, json.loads(payload.decode("utf-8"))


class TestExportApi(BasePVTest):
    def setUp(self):
        super().setUp()
        self.server = create_server(self.app, "127.0.0.1", 0)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.api = ApiClient(f"http://127.0.0.1:{self.port}")
        code, r = self.api.call("POST", "/api/v1/intake", {
            "source_system": "HOSPITAL", "external_id": "H-API-1",
            "payload": self.zhang_patient()})
        self.cid = r["case_id"]
        self.api.call("POST", "/api/v1/reports/dispatch", {})

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        super().tearDown()

    def test_export_status_download_verify_flow(self):
        # 发起（默认等待完成）
        code, task = self.api.call("POST", "/api/v1/exports", {
            "case_id": self.cid, "requested_by": "officer-chen", "wait": True})
        self.assertIn(code, (201, 202))
        self.assertEqual("COMPLETED", task["status"])
        self.assertTrue(task["package_root"])
        tid = task["task_id"]

        # 任务详情 / 列表
        code, got = self.api.call("GET", f"/api/v1/exports/{tid}")
        self.assertEqual(200, code)
        self.assertEqual("COMPLETED", got["status"])
        code, lst = self.api.call("GET", f"/api/v1/exports?case_id={self.cid}")
        self.assertEqual(200, code)
        self.assertTrue(any(t["task_id"] == tid for t in lst))

        # 重复发起 -> 同一任务（deduped）
        code, again = self.api.call("POST", "/api/v1/exports", {
            "case_id": self.cid, "requested_by": "someone-else"})
        self.assertEqual(tid, again["task_id"])
        self.assertTrue(again["deduped"])

        # 下载
        code, headers, data = self.api.call(
            "GET", f"/api/v1/exports/{tid}/download", raw=True)
        self.assertEqual(200, code)
        self.assertEqual("application/x-tar", headers["Content-Type"])
        self.assertIn("attachment", headers["Content-Disposition"])
        rep = verify_package(data)
        self.assertTrue(rep["valid"], rep["errors"])

        # 服务端核验 -> 200 + 留档
        code, vrep = self.api.call("POST", f"/api/v1/exports/{tid}/verify", {})
        self.assertEqual(200, code)
        self.assertTrue(vrep["valid"])
        code, runs = self.api.call("GET", f"/api/v1/exports/{tid}/verifications")
        self.assertEqual(200, code)
        self.assertEqual(1, len(runs))
        self.assertTrue(runs[0]["ok"])

    def test_export_async_progress_then_poll(self):
        code, task = self.api.call("POST", "/api/v1/exports", {
            "case_id": self.cid, "wait": False})
        self.assertEqual(202, code)
        tid = task["task_id"]
        # 轮询直到完成
        import time
        for _ in range(100):
            _, got = self.api.call("GET", f"/api/v1/exports/{tid}")
            if got["status"] in ("COMPLETED", "FAILED"):
                break
            time.sleep(0.02)
        self.assertEqual("COMPLETED", got["status"])

    def test_download_unknown_task_404(self):
        code, headers, body = self.api.call(
            "GET", "/api/v1/exports/nope/download", raw=True)
        self.assertEqual(404, code)
        self.assertEqual("TASK_NOT_FOUND", json.loads(body)["error"])

    def test_timeline_endpoint_hashes(self):
        code, body = self.api.call("GET", f"/api/v1/cases/{self.cid}/timeline")
        self.assertEqual(200, code)
        events = body["timeline"]
        self.assertGreater(len(events), 0)
        self.assertEqual(1, events[0]["seq"])
        self.assertTrue(events[0]["entry_hash"])
        self.assertTrue(events[0]["prev_hash"])

    def test_export_nonexistent_case_404(self):
        code, body = self.api.call("POST", "/api/v1/exports",
                                   {"case_id": "nope", "wait": True})
        self.assertEqual(404, code)
        self.assertEqual("CASE_NOT_FOUND", body["error"])
