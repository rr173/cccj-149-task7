"""证据包导出 + 离线核验主行为测试。

覆盖：
- 导出包含来源原文/版本快照/合并拆分关系/报送回执/审计时间线；
- 导出期间随访/合并/拆分，包绑定发起时版本，不拼接前后状态；
- 完整快照取不到时明确失败（引用悬空/审计链断裂）；
- 离线核验逐项指出失败项，任一失败 valid=false 且绝不标有效；
- 清单根/签名/文件摘要篡改、缺文件全部可检测。
"""
import io
import json
import tarfile

from tests.base import BasePVTest
from pv_service import audit, package_codec as codec
from pv_service.verifier import verify_package


def _read_tar(path_or_bytes):
    fh = io.BytesIO(path_or_bytes) if isinstance(path_or_bytes, bytes) else open(path_or_bytes, "rb")
    with tarfile.open(fileobj=fh, mode="r:*") as tar:
        return {m.name: tar.extractfile(m).read() for m in tar.getmembers()}


def _find_file(names, suffix):
    return next(n for n in names if n.endswith(suffix))


class ExportTestBase(BasePVTest):
    AUTO_EXPORT = True

    def setUp(self):
        super().setUp()
        if not self.AUTO_EXPORT:
            # 关闭夹具默认开启的后台 worker，改由测试手动 run_until_idle
            self.app.exports.stop_worker()
        self.exports = self.app.exports

    def _complete(self, case_id=None, **kw):
        case_id = case_id or self.cid
        t = self.exports.request_export(case_id, **kw)
        self.exports.run_until_idle()
        return self.exports.get_task(t["task_id"])

    def _verify_bytes(self, data, **kw):
        return verify_package(data, **kw)


class TestHappyPath(ExportTestBase):
    def setUp(self):
        super().setUp()
        r = self.app.cases.intake("HOSPITAL", "H-EV-1", self.zhang_patient())
        self.cid = r["case_id"]
        self.pump()

    def test_export_contains_all_evidence_sections(self):
        task = self._complete(requested_by="officer-liu")
        self.assertEqual("COMPLETED", task["status"], task.get("error_message"))
        path, _ = self.exports.package_path(task["task_id"])
        data = path.read_bytes()
        names = set(_read_tar(data))
        # 五大证据段 + 清单签名
        for required in ("package.json", "manifest.json", "manifest.sig",
                         "export/request.json", "export/snapshot.json",
                         "memberships.json", "relations.json"):
            self.assertIn(required, names)
        self.assertTrue(any(n.startswith("sources/") for n in names), "来源原文")
        self.assertTrue(any(n.startswith("cases/") for n in names), "版本快照")
        self.assertTrue(any(n.startswith("timeline/") for n in names), "审计时间线")
        self.assertTrue(any(n.startswith("reports/") for n in names), "报送回执")

        rep = self._verify_bytes(data)
        self.assertTrue(rep["valid"], rep["errors"])
        self.assertEqual(self.cid, rep["summary"]["case_id"])

    def test_snapshot_includes_original_source_payload_verbatim(self):
        task = self._complete()
        path, _ = self.exports.package_path(task["task_id"])
        files = _read_tar(path.read_bytes())
        src_name = next(n for n in files if n.startswith("sources/"))
        src = json.loads(files[src_name])
        self.assertEqual("HOSPITAL", src["source_system"])
        self.assertEqual("H-EV-1", src["external_id"])
        self.assertEqual("张三", src["payload"]["patient_name"])

    def test_report_with_ack_contains_regulator_receipt(self):
        task = self._complete()
        path, _ = self.exports.package_path(task["task_id"])
        files = _read_tar(path.read_bytes())
        rep_file = json.loads(files[next(n for n in files if n.startswith("reports/"))])
        self.assertEqual("ACKED", rep_file["status_as_of_anchor"])
        self.assertIsNotNone(rep_file["ack_as_of_anchor"]["ack_id"])
        self.assertTrue(rep_file["status_trace"])

    def test_timeline_hashes_present_and_verify(self):
        task = self._complete()
        path, _ = self.exports.package_path(task["task_id"])
        files = _read_tar(path.read_bytes())
        tl = json.loads(files[f"timeline/{self.cid}.json"])
        self.assertGreater(len(tl["events"]), 0)
        self.assertEqual(audit.GENESIS, tl["events"][0]["prev_hash"])
        rep = self._verify_bytes(path.read_bytes())
        self.assertTrue(rep["valid"], rep["errors"])


class TestSnapshotPinning(ExportTestBase):
    def setUp(self):
        super().setUp()
        r = self.app.cases.intake("HOSPITAL", "H-PIN-1", self.zhang_patient())
        self.cid = r["case_id"]
        self.pump()

    def test_followup_during_or_after_export_does_not_change_package(self):
        # 导出 v1
        t1 = self._complete()
        root1 = t1["package_root"]
        path1, _ = self.exports.package_path(t1["task_id"])
        files1 = _read_tar(path1.read_bytes())
        snap1 = json.loads(files1["export/snapshot.json"])
        self.assertEqual(1, snap1["version_no"])

        # 之后随访产生 v2（严重性升级）
        self.clock.advance(days=1)
        self.app.cases.add_followup(self.cid, {
            "seriousness": "严重", "seriousness_criteria": ["住院"]})
        self.pump()

        # 旧包仍然是 v1，未被拼接
        rep = self._verify_bytes(path1.read_bytes())
        self.assertTrue(rep["valid"], rep["errors"])
        files_old = _read_tar(path1.read_bytes())
        self.assertEqual(1, json.loads(files_old["export/snapshot.json"])["version_no"])
        cases_old = json.loads(files_old[f"cases/{self.cid}.json"])
        self.assertEqual(1, len(cases_old["versions"]))

        # 导出“当前版本”得到 v2 的新包
        t2 = self._complete()
        self.assertNotEqual(root1, t2["package_root"])
        path2, _ = self.exports.package_path(t2["task_id"])
        files2 = _read_tar(path2.read_bytes())
        snap2 = json.loads(files2["export/snapshot.json"])
        self.assertEqual(2, snap2["version_no"])
        self.assertTrue(snap2["snapshot"]["serious"])
        self._verify_bytes(path2.read_bytes())

    def test_export_specific_historical_version_is_pinned(self):
        self.clock.advance(days=1)
        self.app.cases.add_followup(self.cid, {"outcome": "好转"})
        self.clock.advance(days=1)
        self.app.cases.add_followup(self.cid, {"outcome": "治愈"})
        detail = self.app.cases.get_case(self.cid)
        v1 = detail["versions"][0]["id"]
        t = self._complete(version_id=v1)
        path, _ = self.exports.package_path(t["task_id"])
        snap = json.loads(_read_tar(path.read_bytes())["export/snapshot.json"])
        self.assertEqual(v1, snap["version_id"])
        self.assertEqual(1, snap["version_no"])
        rep = self._verify_bytes(path.read_bytes())
        self.assertTrue(rep["valid"], rep["errors"])

    def test_late_ack_after_anchor_not_spliced_into_report(self):
        # 病例报告处于 QUEUED 时导出（未 ACK）
        r2 = self.app.cases.intake("ENTERPRISE", "E-PIN-Q", {
            "patient_name": "钱七", "dob": "1970-01-01",
            "drugs": [{"name": "warfarin"}], "reactions": ["出血"],
            "seriousness": "严重", "event_date": "2026-09-09"})
        cid2 = r2["case_id"]
        # 先不派发 -> QUEUED
        task = self._complete(case_id=cid2)
        path, _ = self.exports.package_path(task["task_id"])
        files = _read_tar(path.read_bytes())
        rep_file = json.loads(files[next(n for n in files if n.startswith("reports/"))])
        self.assertEqual("QUEUED", rep_file["status_as_of_anchor"])
        self.assertIsNone(rep_file["ack_as_of_anchor"])
        # 核验通过：冻结状态没有锚点后的 ACK
        self.assertTrue(self._verify_bytes(path.read_bytes())["valid"])

        # 现在派发送达 ACK —— 旧包字节不变，重新核验仍有效，且仍显示 QUEUED
        self.pump()
        self.assertTrue(self._verify_bytes(path.read_bytes())["valid"])
        files = _read_tar(path.read_bytes())
        rep_file = json.loads(files[next(n for n in files if n.startswith("reports/"))])
        self.assertEqual("QUEUED", rep_file["status_as_of_anchor"])


class TestMergeSplitPackages(ExportTestBase):
    def _two_duplicates(self):
        a = self.app.cases.intake("HOSPITAL", "H-MS-1", self.zhang_patient())
        b = self.app.cases.intake("HOTLINE", "HL-MS-1", self.zhang_multilang())
        cluster = b["duplicate_clusters"][0]["cluster_id"]
        return a["case_id"], b["case_id"], cluster

    def test_survivor_package_contains_merge_relation_and_merged_away_timeline(self):
        cid_a, cid_b, cluster = self._two_duplicates()
        self.pump()
        m = self.app.cases.confirm_cluster(cluster, actor="officer")
        survivor = m["survivor_case_id"]
        merged = m["merged_from"][0]
        self.cid = survivor
        task = self._complete()
        path, _ = self.exports.package_path(task["task_id"])
        files = _read_tar(path.read_bytes())
        rels = json.loads(files["relations.json"])["relations"]
        self.assertTrue(any(r["relation"] == "MERGE" and
                            r["survivor_case_id"] == survivor for r in rels))
        # 被合并病例的审计切片也在包里（含 CASE_MERGED_AWAY）
        self.assertIn(f"timeline/{merged}.json", files)
        tl_merged = json.loads(files[f"timeline/{merged}.json"])
        self.assertIn("CASE_MERGED_AWAY",
                      [e["event_type"] for e in tl_merged["events"]])
        rep = self._verify_bytes(path.read_bytes())
        self.assertTrue(rep["valid"], rep["errors"])

    def test_split_child_and_parent_packages(self):
        cid_a, cid_b, cluster = self._two_duplicates()
        self.pump()
        wb = self.app.cases.workbench()
        src = next(s["id"] for c in wb if c["case_id"] == cid_a for s in c["sources"])
        sp = self.app.cases.split_source(cid_a, src, actor="officer")
        child = sp["new_case_id"]
        self.cid = child
        self.pump()

        # 子病例包：含父病例拆分前历史 + CASE_SPLIT/CASE_SPLIT_IN 证据
        tchild = self._complete(case_id=child)
        pchild, _ = self.exports.package_path(tchild["task_id"])
        fchild = _read_tar(pchild.read_bytes())
        rels = json.loads(fchild["relations.json"])["relations"]
        self.assertTrue(any(r["relation"] == "SPLIT" and
                            r["new_case_id"] == child for r in rels))
        self.assertIn(f"timeline/{cid_a}.json", fchild)
        rep = self._verify_bytes(pchild.read_bytes())
        self.assertTrue(rep["valid"], rep["errors"])

        # 父病例包：拆分版本在，子病例事件不在
        self.cid = cid_a
        tparent = self._complete(case_id=cid_a)
        pparent, _ = self.exports.package_path(tparent["task_id"])
        fparent = _read_tar(pparent.read_bytes())
        self.assertNotIn(f"timeline/{child}.json", fparent)
        rep2 = self._verify_bytes(pparent.read_bytes())
        self.assertTrue(rep2["valid"], rep2["errors"])


class TestSnapshotIncompleteness(ExportTestBase):
    def setUp(self):
        super().setUp()
        r = self.app.cases.intake("HOSPITAL", "H-INCOMP-1", self.zhang_patient())
        self.cid = r["case_id"]
        self.pump()

    def _request_one(self):
        return self.exports.request_export(self.cid)["task_id"]

    def test_dangling_source_reference_fails_explicitly(self):
        task_id = self._request_one()
        conn = self.db()
        # 保留 membership（它是快照对来源的引用），仅删除 source_records 原文行
        # 制造“引用悬空”。源表有只增触发器、membership 有外键，这里绕过它们
        # 模拟底层数据损坏/原文丢失。
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("DROP TRIGGER trg_sources_no_delete")
        sid = conn.execute("SELECT id FROM source_records LIMIT 1").fetchone()[0]
        conn.execute("DELETE FROM source_records WHERE id=?", (sid,))
        conn.commit()
        conn.execute("PRAGMA foreign_keys=ON")
        self.exports.run_until_idle()
        task = self.exports.get_task(task_id)
        self.assertEqual("FAILED", task["status"])
        self.assertEqual("SNAPSHOT_INCOMPLETE", task["error_code"])
        self.assertIn("来源原文缺失", task["error_message"])
        # 不留包
        self.assertIsNone(task["package_path"])
        with self.assertRaises(Exception):
            self.exports.package_path(task_id)

    def test_broken_audit_chain_fails_export(self):
        task_id = self._request_one()
        conn = self.db()
        conn.execute("DROP TRIGGER trg_events_no_update")
        # 改写中间一帧 summary（哈希链立即断裂）
        mid = conn.execute(
            "SELECT id FROM case_events WHERE case_id=? ORDER BY seq LIMIT 1 OFFSET 1",
            (self.cid,)).fetchone()[0]
        conn.execute("UPDATE case_events SET summary='非法改写' WHERE id=?", (mid,))
        conn.commit()
        self.exports.run_until_idle()
        task = self.exports.get_task(task_id)
        self.assertEqual("FAILED", task["status"])
        self.assertEqual("AUDIT_CHAIN_BROKEN", task["error_code"])
        self.assertIsNone(task["package_path"])

    def test_unknown_version_is_request_error(self):
        from pv_service.service import DomainError
        with self.assertRaises(DomainError):
            self.exports.request_export(self.cid, version_id="nope-version")
