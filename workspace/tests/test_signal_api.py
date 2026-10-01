"""信号窗口计算 HTTP 接口测试（标准库 http.server）。"""
import json
import shutil
import tempfile
import threading
import unittest
import urllib.request
import urllib.error
from datetime import datetime

from pv_service.api import create_server
from pv_service.app import Application, FixedClock


def _request(server, method, path, body=None):
    url = f"http://127.0.0.1:{server.server_address[1]}{path}"
    data = None
    headers = {"Content-Type": "application/json"}
    if body is not None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode("utf-8"))


class SignalApiBase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="pv-signal-api-")
        self.clock = FixedClock(datetime(2026, 3, 31, 9))
        self.app = Application(self.dir, clock=self.clock)
        self.server = create_server(self.app, "127.0.0.1", 0)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.app.seed_demo_reference()

    def tearDown(self):
        self.server.shutdown()
        self.app.shutdown()
        shutil.rmtree(self.dir, ignore_errors=True)

    def setup_definition(self):
        code, terms = _request(self.server, "POST", "/api/v1/signals/terms",
                               {"term_version": "t1",
                                "mappings": {"肝炎": "HEP", "头痛": "HEAD",
                                             "皮疹": "RASH"},
                                "created_by": "ana"})
        self.assertEqual(code, 201)
        code, df = _request(self.server, "POST", "/api/v1/signals/definitions",
                            {"definition_code": "D1", "created_by": "ana",
                             "config": {
                                 "drug_groups": {"X": ["X"], "Y": ["Y"]},
                                 "event_groups": {"HEP": ["HEP"]},
                                 "windows": [{"id": "W1", "start": "2026-01-01",
                                              "end": "2026-04-01"}],
                                 "min_n": 1,
                                 "prr_min": 2, "ror_min": 2,
                                 "ror_ci_lower_than": 1, "require": "ALL"}})
        self.assertEqual(code, 201)
        return terms["term_version_id"], df["definition_id"]

    def add_case(self, **kw):
        code, out = _request(self.server, "POST", "/api/v1/signals/cases", kw)
        self.assertEqual(code, 201, out)
        return out


class TestSignalApi(SignalApiBase):
    def test_end_to_end(self):
        tid, did = self.setup_definition()
        self.add_case(case_id="s1", source_id="s1", drugs=["X"], events=["肝炎"],
                      report_date="2026-02-10")
        self.add_case(case_id="s2", source_id="s2", drugs=["X"], events=["头痛"],
                      report_date="2026-02-10")
        self.add_case(case_id="s3", source_id="s3", drugs=["Y"], events=["肝炎"],
                      report_date="2026-02-10")

        # 提交并等待发布
        code, run = _request(self.server, "POST", "/api/v1/signals/runs",
                             {"definition_id": did, "cutoff": "2026-04-01T00:00:00",
                              "term_version_id": tid, "wait": True})
        self.assertEqual(code, 201, run)
        self.assertEqual(run["status"], "PUBLISHED")
        rid = run["run_id"]

        # 幂等重放：同一计算键
        code, run2 = _request(self.server, "POST", "/api/v1/signals/runs",
                              {"definition_id": did, "cutoff": "2026-04-01T00:00:00",
                               "term_version_id": tid, "wait": False})
        self.assertEqual(code, 200)
        self.assertTrue(run2.get("deduped"))
        self.assertEqual(run2["run_id"], rid)

        # 运行与进度
        code, got = _request(self.server, "GET", f"/api/v1/signals/runs/{rid}")
        self.assertEqual(code, 200)
        self.assertEqual(got["committed_shards"], got["shard_count"])

        # 窗口结果
        code, results = _request(
            self.server, "GET",
            f"/api/v1/signals/definitions/{did}/results")
        self.assertEqual(code, 200)
        hep = [r for r in results if r["drug_group"] == "X"
               and r["event_group"] == "HEP"][0]
        self.assertEqual(hep["counts"], {"a": 1, "b": 1, "c": 1, "d": 0, "n": 3})

        # 修订详情（含入选清单）
        rev_id = hep["revision_id"]
        code, rev = _request(self.server, "GET",
                             f"/api/v1/signals/revisions/{rev_id}")
        self.assertEqual(code, 200)
        self.assertEqual([x["source_id"] for x in rev["roster"]["a"]], ["s1"])
        self.assertIn("diff", rev)

        # 贡献追踪
        code, contrib = _request(
            self.server, "GET",
            f"/api/v1/signals/contributions?source_id=s1&definition_id={did}")
        self.assertEqual(code, 200)
        self.assertEqual(len(contrib), 1)
        self.assertEqual(contrib[0]["cells"]["X|HEP"], "a")

        # 确认信号（链头号 1）
        code, disp = _request(
            self.server, "POST",
            f"/api/v1/signals/revisions/{rev_id}/disposition",
            {"disposition": "CONFIRMED", "expected_revision_no": 1,
             "analyst": "alice", "note": "ok"})
        self.assertEqual(code, 200, disp)
        self.assertEqual(disp["disposition"], "CONFIRMED")

        # 过期号被拒
        code, err = _request(
            self.server, "POST",
            f"/api/v1/signals/revisions/{rev_id}/disposition",
            {"disposition": "REFUTED", "expected_revision_no": 0,
             "analyst": "bob"})
        self.assertEqual(code, 409)
        self.assertEqual(err["error"], "STALE_REVISION")

        # 修订差异列表
        code, diffs = _request(
            self.server, "GET",
            f"/api/v1/signals/definitions/{did}/diffs")
        self.assertEqual(code, 200)

        # 审计与链核验
        code, ver = _request(self.server, "GET",
                             "/api/v1/signals/audit/verify")
        self.assertEqual(code, 200)
        self.assertTrue(ver["valid"])

        # 复演核验
        code, rep = _request(self.server, "GET",
                             f"/api/v1/signals/runs/{rid}/verify")
        self.assertEqual(code, 200)
        self.assertTrue(rep["valid"])

    def test_shard_progress_and_exclusions(self):
        tid, did = self.setup_definition()
        self.add_case(case_id="s1", source_id="s1", drugs=["X"], events=["肝炎"],
                      report_date="2026-02-10")
        self.add_case(case_id="s2", source_id="s2", drugs=["W"], events=["肝炎"],
                      report_date="2026-02-10")
        code, run = _request(self.server, "POST", "/api/v1/signals/runs",
                             {"definition_id": did, "cutoff": "2026-04-01T00:00:00",
                              "term_version_id": tid, "wait": False})
        self.assertEqual(code, 202)
        rid = run["run_id"]
        # 推进部分分片（不发布）
        code, part = _request(self.server, "POST",
                              f"/api/v1/signals/runs/{rid}/shards",
                              {"max_shards": 1})
        self.assertLessEqual(part["committed_shards"], 1)
        # 显式发布到完成
        code, done = _request(self.server, "POST",
                              f"/api/v1/signals/runs/{rid}/publish", {})
        self.assertEqual(done["status"], "PUBLISHED")
        code, exc = _request(self.server, "GET",
                             f"/api/v1/signals/runs/{rid}/exclusions")
        self.assertEqual(code, 200)
        reasons = {e["source_id"]: e["reason"] for e in exc["W1"]}
        self.assertEqual(reasons["s2"], "DRUG_NOT_MONITORED")

    def test_validation_errors(self):
        # 空定义
        code, err = _request(self.server, "POST", "/api/v1/signals/definitions",
                             {"definition_code": "bad", "config": {}})
        self.assertEqual(code, 400)
        # 恢复接口
        code, rec = _request(self.server, "POST", "/api/v1/signals/recover", {})
        self.assertEqual(code, 200)
        self.assertIn("recoverable", rec)


if __name__ == "__main__":
    unittest.main()
