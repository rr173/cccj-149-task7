"""证据包：并发去重、进程重启唯一、崩溃恢复、确定性构建。"""
import json
import threading
import tarfile

from tests.test_evidence_export import ExportTestBase
from pv_service.app import Application


def _tar_names(path):
    with tarfile.open(path, "r:*") as t:
        return {m.name for m in t.getmembers()}


class TestExportDedup(ExportTestBase):
    def setUp(self):
        super().setUp()
        r = self.app.cases.intake("HOSPITAL", "H-DED-1", self.zhang_patient())
        self.cid = r["case_id"]
        self.pump()
        self.detail = self.app.cases.get_case(self.cid)
        self.vid = self.detail["versions"][-1]["id"]

    def test_concurrent_identical_requests_yield_single_task(self):
        results, errors = [], []
        barrier = threading.Barrier(8)

        def fire():
            try:
                barrier.wait()
                t = self.app.exports.request_export(self.cid, version_id=self.vid,
                                                    requested_by="officer")
                results.append(t["task_id"])
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        threads = [threading.Thread(target=fire) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertFalse(errors, errors)
        self.assertEqual(1, len(set(results)), "相同导出只能产生一个任务")
        tasks = self.app.exports.list_tasks(self.cid)
        self.assertEqual(1, len(tasks))
        self.app.exports.run_until_idle()
        task = self.exports.get_task(results[0])
        self.assertEqual("COMPLETED", task["status"])

    def test_repeat_request_after_completion_returns_same_package(self):
        t1 = self.app.exports.request_export(self.cid, requested_by="a")
        self.app.exports.run_until_idle()
        t1v = self.exports.get_task(t1["task_id"])
        # 完全相同请求再来一次（不同发起人也不影响默认幂等键）
        t2 = self.app.exports.request_export(self.cid, requested_by="b")
        self.app.exports.run_until_idle()
        t2v = self.exports.get_task(t2["task_id"])
        self.assertEqual(t1v["task_id"], t2v["task_id"])
        self.assertTrue(t2v["deduped"])
        self.assertEqual(t1v["package_root"], t2v["package_root"])
        p1, _ = self.exports.package_path(t1v["task_id"])
        p2, _ = self.exports.package_path(t2v["task_id"])
        self.assertEqual(p1, p2)

    def test_only_one_content_addressed_file_on_disk(self):
        t = self.app.exports.request_export(self.cid)
        self.app.exports.run_until_idle()
        t = self.exports.get_task(t["task_id"])
        pkg_dir = self.app.exports.packages_dir
        files = list(pkg_dir.glob("*.tar"))
        self.assertEqual(1, len(files))
        self.assertEqual(f"{t['package_root']}.tar", files[0].name)

    def test_different_versions_get_different_packages(self):
        t1 = self.app.exports.request_export(self.cid, version_id=self.vid)
        self.app.exports.run_until_idle()
        root1 = self.exports.get_task(t1["task_id"])["package_root"]
        self.clock.advance(days=1)
        self.app.cases.add_followup(self.cid, {"outcome": "好转"})
        t2 = self.app.exports.request_export(self.cid)  # 默认绑定最新版
        self.app.exports.run_until_idle()
        root2 = self.exports.get_task(t2["task_id"])["package_root"]
        self.assertNotEqual(root1, root2)
        self.assertEqual(2, len(list(self.app.exports.packages_dir.glob("*.tar"))))

    def test_explicit_idempotency_key_dedups_only_same_key(self):
        t1 = self.app.exports.request_export(self.cid, idempotency_key="run-A")
        t2 = self.app.exports.request_export(self.cid, idempotency_key="run-B")
        self.assertNotEqual(t1["task_id"], t2["task_id"])
        self.app.exports.run_until_idle()
        # 不同幂等键 = 不同导出请求（request 进清单根），各自独立成包
        root_a = self.exports.get_task(t1["task_id"])["package_root"]
        root_b = self.exports.get_task(t2["task_id"])["package_root"]
        self.assertNotEqual(root_a, root_b)
        self.assertEqual(2, len(list(self.app.exports.packages_dir.glob("*.tar"))))
        # 同键再来：去重到同一任务同一包
        t3 = self.app.exports.request_export(self.cid, idempotency_key="run-A")
        self.assertEqual(t1["task_id"], t3["task_id"])
        self.assertEqual(root_a, self.exports.get_task(t3["task_id"])["package_root"])
        self.assertEqual(2, len(list(self.app.exports.packages_dir.glob("*.tar"))))


class TestRestartAndCrash(ExportTestBase):
    AUTO_EXPORT = False

    def setUp(self):
        super().setUp()
        r = self.app.cases.intake("HOSPITAL", "H-RST-1", self.zhang_patient())
        self.cid = r["case_id"]
        self.pump()

    def test_restart_with_completed_package_keeps_single_package(self):
        t = self.app.exports.request_export(self.cid, requested_by="li")
        self.app.exports.run_until_idle()
        t = self.exports.get_task(t["task_id"])
        root_before = t["package_root"]
        path_before = self.exports.package_path(t["task_id"])[0]
        bytes_before = path_before.read_bytes()

        # “新进程”：旧进程先干净退出
        self.app.shutdown()
        app2 = Application(self.dir, clock=self.clock, auto_export=False)
        t2 = app2.exports.get_task(t["task_id"])
        self.assertEqual("COMPLETED", t2["status"])
        self.assertEqual(root_before, t2["package_root"])
        path_after, _ = app2.exports.package_path(t["task_id"])
        self.assertEqual(bytes_before, path_after.read_bytes())
        self.assertEqual(1, len(list(app2.exports.packages_dir.glob("*.tar"))))
        app2.shutdown()

    def test_building_task_requeued_on_restart_and_succeeds_once(self):
        t = self.app.exports.request_export(self.cid)
        tid = t["task_id"]
        # 先退出“崩溃进程”（夹具 worker 已停，任务保持 PENDING）
        self.app.shutdown()

        # 模拟崩溃在构建中途：独立连接把任务置为 BUILDING（不经任何服务实例）
        import sqlite3
        conn = sqlite3.connect(self.app.db_path)
        cur = conn.execute("UPDATE export_tasks SET status='BUILDING' WHERE id=?",
                           (tid,))
        conn.commit()
        self.assertEqual(1, cur.rowcount)
        conn.close()

        # “新进程”启动：构造期自动 recover，把 BUILDING 重置回 PENDING
        app2 = Application(self.dir, clock=self.clock, auto_export=False)
        self.assertEqual("PENDING", app2.exports.get_task(tid)["status"])
        app2.exports.run_until_idle()
        tv = app2.exports.get_task(tid)
        self.assertEqual("COMPLETED", tv["status"])
        self.assertEqual(1, len(list(app2.exports.packages_dir.glob("*.tar"))))
        app2.shutdown()

    def test_manual_recover_resets_building(self):
        t = self.app.exports.request_export(self.cid)
        tid = t["task_id"]
        import sqlite3
        conn = sqlite3.connect(self.app.db_path)
        conn.execute("UPDATE export_tasks SET status='BUILDING' WHERE id=?", (tid,))
        conn.commit(); conn.close()
        rec = self.app.exports.recover()
        self.assertEqual(1, rec["reset_building"])
        self.assertEqual("PENDING", self.app.exports.get_task(tid)["status"])

    def test_crash_hook_then_restart_leaves_one_package(self):
        # 用钩子让首次构建在 ASSEMBLED 后崩溃
        crashed = {"n": 0}

        def hook(stage, task_id):
            if stage == "ASSEMBLED":
                crashed["n"] += 1
                raise RuntimeError("simulated process crash after assembly")

        app = Application(self.dir, clock=self.clock, auto_export=False,
                         export_crash_hook=hook)
        t = app.exports.request_export(self.cid)
        tid = t["task_id"]
        app.exports.run_until_idle()
        self.assertGreaterEqual(crashed["n"], 1)
        tv = app.exports.get_task(tid)
        self.assertEqual("FAILED", tv["status"])
        self.assertEqual("BUILD_ERROR", tv["error_code"])
        app.shutdown()

        # 新进程无钩子：同一任务不会自动重跑（FAILED），但相同请求重试复用该任务
        app2 = Application(self.dir, clock=self.clock, auto_export=False)
        again = app2.exports.request_export(self.cid)
        self.assertEqual(tid, again["task_id"])
        app2.exports.run_until_idle()
        tv2 = app2.exports.get_task(tid)
        self.assertEqual("COMPLETED", tv2["status"])
        self.assertEqual(1, len(list(app2.exports.packages_dir.glob("*.tar"))))
        app2.shutdown()

    def test_temp_dirs_cleaned_on_restart(self):
        tmp = self.app.exports.tmp_dir
        (tmp / "leftover-task").mkdir(parents=True)
        (tmp / "leftover-task" / "junk.tar").write_bytes(b"x")
        self.app.shutdown()
        app2 = Application(self.dir, clock=self.clock, auto_export=False)
        self.assertFalse((tmp / "leftover-task").exists())
        app2.shutdown()


class TestDeterministicBuild(ExportTestBase):
    AUTO_EXPORT = False

    def test_same_request_in_two_processes_byte_identical(self):
        import shutil
        import tempfile
        r = self.app.cases.intake("HOSPITAL", "H-DET-1", self.zhang_patient())
        cid = r["case_id"]
        self.pump()
        detail = self.app.cases.get_case(cid)
        vid = detail["versions"][-1]["id"]
        # 固定签名种子（两进程一致）才能字节相同
        seed = bytes.fromhex("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60")
        self.app.shutdown()
        app1 = Application(self.dir, clock=self.clock, signer_seed=seed, auto_export=False)
        t1 = app1.exports.request_export(cid, version_id=vid, idempotency_key="k")
        app1.exports.run_until_idle()
        b1 = app1.exports.package_path(t1["task_id"])[0].read_bytes()
        root1 = app1.exports.get_task(t1["task_id"])["package_root"]
        app1.shutdown()

        # 把数据目录整体复制到全新位置（模拟另一台机器/另一份内容寻址存储），
        # 新进程用同样种子对同一 case+version+key 重建。
        d2 = tempfile.mkdtemp(prefix="pv-det-")
        self.addCleanup(lambda: shutil.rmtree(d2, ignore_errors=True))
        shutil.copytree(self.dir, d2, dirs_exist_ok=True)
        # 清掉两边已生成的包与任务，强制在新进程重新构建
        shutil.rmtree(d2 + "/packages", ignore_errors=True)
        shutil.rmtree(d2 + "/export_tmp", ignore_errors=True)
        import sqlite3
        c2 = sqlite3.connect(d2 + "/pv.db")
        c2.execute("DELETE FROM export_tasks")
        c2.commit(); c2.close()

        app2 = Application(d2, clock=self.clock, signer_seed=seed)
        t2 = app2.exports.request_export(cid, version_id=vid, idempotency_key="k")
        app2.exports.run_until_idle()
        b2 = app2.exports.package_path(t2["task_id"])[0].read_bytes()
        root2 = app2.exports.get_task(t2["task_id"])["package_root"]
        app2.shutdown()
        self.assertEqual(root1, root2)
        self.assertEqual(b1, b2, "确定性构建：同请求跨进程字节一致")
