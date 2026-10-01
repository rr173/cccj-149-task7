"""导出构建与随访/合并/拆分并发交错：包必须绑定发起时版本快照。"""
import io
import json
import tarfile

from tests.base import BasePVTest
from pv_service.app import Application
from pv_service.verifier import verify_package


def _files(data):
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:*") as t:
        return {m.name: t.extractfile(m).read() for m in t.getmembers()}


class TestConcurrentMutationDuringExport(BasePVTest):
    def _app_with_events(self, on_stage):
        return Application(self.dir, clock=self.clock, auto_dispatch=False,
                           auto_export=False, export_build_events=on_stage)

    def test_followup_during_build_package_pins_old_version(self):
        r = self.app.cases.intake("HOSPITAL", "H-CFX-1", self.zhang_patient())
        cid = r["case_id"]
        self.app.reporting.pump()
        self.app.exports.stop_worker()

        def events(stage, task_id):
            if stage == "AFTER_ASSEMBLE":
                # 快照已固定、最终化之前：随访升级为严重（产生 v2）
                self.clock.advance(days=1)
                self.app.cases.add_followup(cid, {
                    "seriousness": "严重", "seriousness_criteria": ["住院"]})
                self.app.reporting.pump()

        app = self._app_with_events(events)
        t = app.exports.request_export(cid)
        tid = t["task_id"]
        app.exports.run_next()           # 同步执行，钩子在构建中触发随访
        task = app.exports.get_task(tid)
        self.assertEqual("COMPLETED", task["status"], task.get("error_message"))
        path, _ = app.exports.package_path(tid)
        data = path.read_bytes()
        rep = verify_package(data)
        self.assertTrue(rep["valid"], rep["errors"])
        snap = json.loads(_files(data)["export/snapshot.json"])
        self.assertEqual(1, snap["version_no"])          # 绑定 v1
        self.assertFalse(snap["snapshot"].get("serious"))
        cases = json.loads(_files(data)[f"cases/{cid}.json"])
        self.assertEqual(1, len(cases["versions"]))      # 不含 v2
        app.shutdown()

    def test_merge_during_build_of_survivor_pins_pre_merge_state(self):
        a = self.app.cases.intake("HOSPITAL", "H-CMG-1", self.zhang_patient())
        b = self.app.cases.intake("HOTLINE", "HL-CMG-1", self.zhang_multilang())
        cluster = b["duplicate_clusters"][0]["cluster_id"]
        cid = a["case_id"]
        self.app.reporting.pump()
        self.app.exports.stop_worker()

        def events(stage, task_id):
            if stage == "AFTER_ASSEMBLE":
                # 导出 cid 构建期间，专员确认合并 -> cid 产生 MERGE 新版本
                self.app.cases.confirm_cluster(cluster, actor="officer")

        app = self._app_with_events(events)
        t = app.exports.request_export(cid)
        app.exports.run_next()
        task = app.exports.get_task(t["task_id"])
        self.assertEqual("COMPLETED", task["status"], task.get("error_message"))
        data = app.exports.package_path(t["task_id"])[0].read_bytes()
        rep = verify_package(data)
        self.assertTrue(rep["valid"], rep["errors"])
        files = _files(data)
        cases = json.loads(files[f"cases/{cid}.json"])
        # 绑定的是发起时版本：只有 INTAKE，没有 MERGE 版本
        self.assertEqual(["INTAKE"], [v["change_type"] for v in cases["versions"]])
        rels = json.loads(files["relations.json"])["relations"]
        self.assertEqual([], rels)
        app.shutdown()

    def test_split_during_build_pins_pre_split_membership(self):
        a = self.app.cases.intake("HOSPITAL", "H-CSP-1", self.zhang_patient())
        cid = a["case_id"]
        self.app.reporting.pump()
        wb = self.app.cases.workbench()
        src = next(s["id"] for c in wb if c["case_id"] == cid for s in c["sources"])
        self.app.exports.stop_worker()

        def events(stage, task_id):
            if stage == "AFTER_ASSEMBLE":
                self.app.cases.split_source(cid, src, actor="officer")

        app = self._app_with_events(events)
        t = app.exports.request_export(cid)
        app.exports.run_next()
        task = app.exports.get_task(t["task_id"])
        self.assertEqual("COMPLETED", task["status"], task.get("error_message"))
        data = app.exports.package_path(t["task_id"])[0].read_bytes()
        rep = verify_package(data)
        self.assertTrue(rep["valid"], rep["errors"])
        files = _files(data)
        mem = json.loads(files["memberships.json"])
        # 来源在截止时仍归属该病例（拆分发生在快照固定之后）
        self.assertIn(src, {m["source_id"] for m in mem["memberships"]
                           if m["active_at_cut"] == 1})
        rels = json.loads(files["relations.json"])["relations"]
        self.assertEqual([], rels)
        app.shutdown()
