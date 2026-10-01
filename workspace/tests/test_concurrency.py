"""并发安全：来源唯一归属、编号唯一、合并/拆分/发送竞争。

使用多线程 + Barrier 制造高竞争窗口；SQLite BEGIN IMMEDIATE、部分唯一索引
和共享 RLock 共同保证不变量。
"""
import sqlite3
import threading

from tests.base import BasePVTest


def _wait_barrier(barrier):
    if barrier:
        barrier.wait(timeout=10)


class TestConcurrency(BasePVTest):
    def test_concurrent_number_allocation_never_duplicates(self):
        n = 20
        barrier = threading.Barrier(n)
        errors = []

        def worker(i):
            try:
                _wait_barrier(barrier)
                self.app.cases.intake("HOSPITAL", f"H-C{i}", {
                    "patient_name": f"互不相同患者{i}", "dob": f"200{i % 9}-01-01",
                    "patient_identifier": f"PID-{i}",
                    "drugs": [{"name": f"drug-{i}"}], "reactions": [f"rx-{i}"],
                    "seriousness": "mild", "event_date": "2026-09-01"})
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual([], errors)
        rows = self.db().execute(
            "SELECT regulatory_number, COUNT(*) c FROM regulatory_reports "
            "GROUP BY regulatory_number HAVING c>1").fetchall()
        self.assertEqual([], rows)
        seq = self.db().execute("SELECT last FROM number_sequence").fetchone()[0]
        self.assertEqual(n, seq)

    def test_concurrent_split_of_same_source_only_one_wins(self):
        r = self.app.cases.intake("HOSPITAL", "H-SC", {
            "patient_name": "张三", "dob": "1980-03-03",
            "drugs": [{"name": "布洛芬"}], "reactions": ["皮疹"],
            "seriousness": "不严重", "event_date": "2026-09-07"})
        sid = self.app.cases.get_case(r["case_id"])["sources"][0]
        n = 8
        barrier = threading.Barrier(n)
        results, errors = [], []

        def worker():
            try:
                _wait_barrier(barrier)
                results.append(self.app.cases.split_source(r["case_id"], sid))
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(1, len(results), "只有一次拆分能成功")
        self.assertEqual(n - 1, len(errors))
        # 唯一归属不变量
        n_active = self.db().execute(
            "SELECT COUNT(*) FROM case_membership WHERE source_id=? AND active=1",
            (sid,)).fetchone()[0]
        self.assertEqual(1, n_active)
        # 部分唯一索引层面再验证：任何来源都只有一行 active
        dup = self.db().execute(
            "SELECT source_id FROM case_membership WHERE active=1 "
            "GROUP BY source_id HAVING COUNT(*)>1").fetchall()
        self.assertEqual([], dup)

    def test_concurrent_merge_and_dispatch_no_duplicate_submission(self):
        # 两个重复病例各有一份排队报告：合并与发送同时发生
        r1 = self.app.cases.intake("HOSPITAL", "H-CD", {
            "patient_name": "张三", "patient_identifier": "ID-7",
            "dob": "1980-03-03", "drugs": [{"name": "布洛芬"}],
            "reactions": ["皮疹"], "seriousness": "不严重",
            "event_date": "2026-09-07"})
        r2 = self.app.cases.intake("HOTLINE", "HL-CD", {
            "patient_name": "老张", "patient_identifier": "ID-7",
            "dob": "1980-03-03", "drugs": [{"name": "ibuprofen"}],
            "reactions": ["rash"], "seriousness": "mild",
            "event_date": "2026-09-07"})
        cid = r2["duplicate_clusters"][0]["cluster_id"]
        outcomes = []
        lock = threading.Lock()

        def merge():
            try:
                with lock:
                    m = self.app.cases.confirm_cluster(cid, actor="liu")
                    outcomes.append(("merge_ok", m))
            except Exception as e:  # noqa: BLE001
                outcomes.append(("merge_err", str(e)))

        def dispatch():
            try:
                # 与合并抢锁：共享 RLock 保证二者不交错
                res = self.app.reporting.send_one()
                outcomes.append(("dispatch", res))
            except Exception as e:  # noqa: BLE001
                outcomes.append(("dispatch_err", str(e)))

        t1 = threading.Thread(target=merge)
        t2 = threading.Thread(target=dispatch)
        t1.start(); t2.start(); t1.join(); t2.join()
        # 无论谁先：最终网关最多收到 1 份该事件的报告
        self.app.reporting.pump()
        from pv_service.gateway import RegulatorGateway
        gw = RegulatorGateway(self.app.gateway_db, clock=self.clock)
        # survivor 1 份（如果先 dispatch 后 merge，被合并方那份会被作废；
        # 如果先 merge 后 dispatch，被作废的那份根本不会发）
        survivor = next(m for k, m in outcomes if k == "merge_ok")["survivor_case_id"]
        sent_for_event = gw.conn.execute(
            "SELECT COUNT(*) FROM gateway_receipts").fetchone()[0]
        self.assertLessEqual(sent_for_event, 1)
        statuses = {row[0]: row[1] for row in self.db().execute(
            "SELECT regulatory_number, status FROM regulatory_reports")}
        void_or_ack = set(statuses.values())
        self.assertTrue(void_or_ack <= {"ACKED", "VOID"})

    def test_concurrent_followups_create_ordered_versions(self):
        r = self.app.cases.intake("HOSPITAL", "H-FU", {
            "patient_name": "张三", "patient_identifier": "ID-8",
            "dob": "1980-03-03", "drugs": [{"name": "布洛芬"}],
            "reactions": ["皮疹"], "seriousness": "不严重",
            "event_date": "2026-09-07"})
        n = 6
        barrier = threading.Barrier(n)

        def worker(i):
            _wait_barrier(barrier)
            self.app.cases.add_followup(r["case_id"],
                                        {"narrative": f"补充说明 {i}"})

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        versions = [row[0] for row in self.db().execute(
            "SELECT version_no FROM case_versions WHERE case_id=? ORDER BY version_no",
            (r["case_id"],))]
        self.assertEqual(list(range(1, n + 2)), versions)

    def test_idempotent_intake_under_duplicate_concurrent_requests(self):
        payload = {"patient_name": "张三", "patient_identifier": "ID-9",
                   "drugs": [{"name": "布洛芬"}], "reactions": ["皮疹"],
                   "seriousness": "不严重", "event_date": "2026-09-07"}
        n = 5
        barrier = threading.Barrier(n)
        results, errors = [], []

        def worker():
            try:
                _wait_barrier(barrier)
                results.append(self.app.cases.intake("HOSPITAL", "H-IDEMP", payload))
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual([], errors)
        case_ids = {x["case_id"] for x in results}
        self.assertEqual(1, len(case_ids))
        self.assertEqual(1, self.db().execute(
            "SELECT COUNT(*) FROM source_records WHERE external_id='H-IDEMP'").fetchone()[0])
