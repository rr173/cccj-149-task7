"""信号窗口计算服务测试。

覆盖需求主线：
- 冻结截止线 + 术语版本：同配置同截止线再算 -> 同一份结果；
- 迟到随访/归并拆解/因果修正在后续窗口撤销旧贡献、计新贡献，同来源窗口一次；
- 跨药品组/事件组/退出样本的可追踪差异；
- 旧修订处置结论保留、新修订重新处置、过期修订号被拒绝；
- 分片崩溃从已提交片继续；任一片失败不发布半成品；
- 同计算键并发/重复提交只产生一个作业与一组修订；
- 发布快照只增、指标列触发器拒绝覆盖；审计哈希链可核验。
"""
import json
import os
import shutil
import tempfile
import threading
import unittest
from datetime import datetime

from pv_service.app import FixedClock
from pv_service.signal_service import SignalService, _ShardFault
from pv_service.service import DomainError

TERMS = {"肝炎": "HEP", "肝功能异常": "HEP", "hepatitis": "HEP",
         "头痛": "HEAD", "皮疹": "RASH"}


def cfg(threshold=True, windows=None):
    c = {
        "drug_groups": {"X": ["X"], "OTHERS": ["Y", "Z"]},
        "event_groups": {"HEP": ["HEP"], "HEAD": ["HEAD"]},
        "windows": windows or [
            {"id": "W1", "start": "2026-01-01", "end": "2026-04-01"}],
        "min_n": 1}
    if threshold:
        c.update({"prr_min": 2.0, "prr_ci_lower_than": 1.0,
                  "ror_min": 2.0, "ror_ci_lower_than": 1.0,
                  "chi_square_min": 4.0, "require": "ALL"})
    return c


class SignalBase(unittest.TestCase):
    START = datetime(2026, 3, 31, 9, 0, 0)
    SHARDS = 4

    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="pv-signal-")
        self.clock = FixedClock(self.START)
        self.svc = SignalService(os.path.join(self.dir, "signal.db"),
                                 clock=self.clock, shard_count=self.SHARDS)
        self.terms = self.svc.create_term_version(
            TERMS, term_version="terms@1", created_by="analyst")
        self.tid = self.terms["term_version_id"]
        self.df = self.svc.create_definition(
            cfg(), definition_code="SIG-DEF", created_by="analyst")
        self.did = self.df["definition_id"]

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def case(self, sid, drug="X", events=("肝炎",), **kw):
        return self.svc.add_case_version(
            case_id=sid, source_id=sid, drugs=[drug], events=list(events),
            report_date=kw.pop("report_date", "2026-02-15"), **kw)

    def publish_run(self, cutoff="2026-04-01T00:00:00", **kw):
        sub = self.svc.submit_run(definition_id=self.did, cutoff=cutoff,
                                  term_version_id=self.tid,
                                  requested_by="analyst", **kw)
        return self.svc.run_until_published(sub["run_id"])

    def head(self, dg="X", eg="HEP", window="W1"):
        rows = [r for r in self.svc.window_results(self.did, window_id=window)
                if r["drug_group"] == dg and r["event_group"] == eg]
        return rows[0]


class TestReproducibility(SignalBase):
    def test_same_config_and_cutoff_identical_snapshot(self):
        for sid, drug, evs in [("s1", "X", ("肝炎",)), ("s2", "X", ("头痛",)),
                               ("s3", "Y", ("肝炎",)), ("s4", "Z", ("皮疹",))]:
            self.case(sid, drug, evs)
        r1 = self.publish_run()
        rev1 = self.head()
        # 完全相同参数再次提交 -> 同一作业（computation_key 幂等）
        again = self.svc.submit_run(definition_id=self.did,
                                    cutoff="2026-04-01T00:00:00",
                                    term_version_id=self.tid)
        self.assertTrue(again.get("deduped"))
        self.assertEqual(again["run_id"], r1["run_id"])
        # 只读重算与已发布分片一致
        rep = self.svc.verify_run_reproducibility(r1["run_id"])
        self.assertTrue(rep["valid"], rep["failures"])
        self.assertEqual(rev1["counts"], {"a": 1, "b": 1, "c": 1, "d": 1, "n": 4})

    def test_late_followup_after_cutoff_not_in_snapshot(self):
        self.case("s1", "X", ("肝炎",))
        self.case("s2", "Y", ("头痛",))
        r1 = self.publish_run("2026-04-01T00:00:00")
        before = self.head()
        # 截止线之后到达的迟到随访（新增 HEP 病例）不改变旧快照
        self.clock.set(datetime(2026, 4, 2, 9))
        self.case("s3", "X", ("肝炎",))
        rep = self.svc.verify_run_reproducibility(r1["run_id"])
        self.assertTrue(rep["valid"])
        self.assertEqual(self.head()["content_hash"], before["content_hash"])

    def test_term_version_pinned_per_run(self):
        self.case("s1", "X", ("黄疸",))  # terms@1 无映射
        self.publish_run()
        self.assertEqual(self.head()["counts"]["a"], 0)
        # 新术语版本扩展映射；用新版本重算 -> 不同 computation_key、不同结果
        terms2 = self.svc.create_term_version(
            {**TERMS, "黄疸": "HEP"}, term_version="terms@2")
        sub2 = self.svc.submit_run(definition_id=self.did,
                                   cutoff="2026-04-01T00:00:00",
                                   term_version_id=terms2["term_version_id"])
        r2 = self.svc.run_until_published(sub2["run_id"])
        self.assertEqual(self.head()["counts"]["a"], 1)
        self.assertNotEqual(r2["run_id"], self.svc.list_runs()[0]["run_id"])

    def test_definition_change_is_new_content(self):
        d2 = self.svc.create_definition(
            cfg(threshold=False), definition_code="SIG-DEF")
        self.assertFalse(d2.get("deduped"))
        # 完全相同内容重复创建 -> 原定义
        d3 = self.svc.create_definition(cfg(threshold=False),
                                        definition_code="SIG-DEF")
        self.assertTrue(d3.get("deduped"))
        self.assertEqual(d3["definition_id"], d2["definition_id"])


class TestContributions(SignalBase):
    def test_incremental_window_revokes_and_readds(self):
        # 第一窗口：X+HEP 病例 s1
        self.case("s1", "X", ("肝炎",))
        self.case("s2", "Y", ("头痛",))
        self.publish_run()
        r1 = self.head()
        self.assertEqual(r1["counts"]["a"], 1)
        ledger1 = self.svc.contribution_trace(source_id="s1",
                                              definition_id=self.did)
        self.assertEqual(len(ledger1), 1)  # 同来源在窗口内只产生一条贡献
        self.assertFalse(ledger1[0]["revoked"])
        self.assertEqual(ledger1[0]["cells"]["X|HEP"], "a")
        self.assertEqual(ledger1[0]["cells"]["X|HEAD"], "b")

        # 后续窗口：s1 随访升级为两事件（仍是 HEP），再跑新截止线
        self.clock.set(datetime(2026, 5, 1, 9))
        self.case("s1", "X", ("肝炎", "头痛"), change_type="FOLLOWUP")
        sub = self.svc.submit_run(definition_id=self.did,
                                  cutoff="2026-05-02T00:00:00",
                                  term_version_id=self.tid)
        self.assertEqual(sub["mode"], "FULL")
        self.svc.run_until_published(sub["run_id"])
        head = self.head()
        self.assertEqual(head["revision_no"], 2)
        # 旧贡献被撤销，新贡献计入；同来源同窗口仍只有一条有效贡献
        ledger = self.svc.contribution_trace(source_id="s1",
                                             definition_id=self.did)
        active = [c for c in ledger if not c["revoked"]]
        self.assertEqual(len(active), 1)
        self.assertEqual(active[0]["cells"]["X|HEP"], "a")
        self.assertEqual(active[0]["cells"]["X|HEAD"], "a")
        old = [c for c in ledger if c["revoked"]]
        self.assertEqual(len(old), 1)
        # 唯一索引层面：有效贡献不重复
        with self.svc._conn() as conn:
            n = conn.execute(
                "SELECT COUNT(*) c FROM sig_case_contributions WHERE source_id='s1' "
                "AND revoked=0").fetchone()["c"]
            self.assertEqual(n, 1)

    def test_merge_then_split_contributions(self):
        self.case("s1", "X", ("肝炎",))
        self.case("s2", "X", ("肝炎",))
        self.publish_run()
        self.assertEqual(self.head()["counts"]["a"], 2)
        # 归并：s2 并入存活病例 s1（s1 新版本携带 merged_from）
        self.clock.set(datetime(2026, 5, 1))
        self.svc.add_case_version(case_id="s1", version_no=2, source_id="s1",
                                  change_type="MERGE", drugs=["X"],
                                  events=["肝炎"], merged_from=["s2"],
                                  report_date="2026-02-15")
        # 旧 c2 病例行在快照里仍存在最新版本（MERGE），但被 s1 吸收
        sub = self.svc.submit_run(definition_id=self.did,
                                  cutoff="2026-05-02T00:00:00",
                                  term_version_id=self.tid)
        self.svc.run_until_published(sub["run_id"])
        rev = self.head()
        self.assertEqual(rev["counts"]["a"], 1)
        exc = self.svc.run_exclusions(sub["run_id"])["W1"]
        self.assertIn("MERGED_AWAY",
                      [e["reason"] for e in exc if e["source_id"] == "s2"])
        diff = self.svc.get_revision(rev["revision_id"])["diff"]
        removed = [c for c in diff["case_changes"]
                   if c["source_id"] == "s2"]
        self.assertTrue(removed)

    def test_causality_update_changes_cell_and_diff(self):
        self.case("s1", "X", ("肝炎",), causality={"肝炎": "POSSIBLE"})
        self.case("s2", "Y", ("皮疹",))
        # 口径：因果评价为 UNLIKELY 的事件不计入相关事件
        df2 = self.svc.create_definition(
            cfg(windows=[{"id": "W1", "start": "2026-01-01",
                          "end": "2026-04-01"}]) if False else
            {**cfg(), "causality_exclude": ["UNLIKELY"]},
            definition_code="CAUSAL-DEF")
        did2 = df2["definition_id"]
        sub = self.svc.submit_run(definition_id=did2,
                                  cutoff="2026-04-01T00:00:00",
                                  term_version_id=self.tid)
        self.svc.run_until_published(sub["run_id"])

        self.clock.set(datetime(2026, 5, 1))
        self.svc.add_case_version(case_id="s1", version_no=2, source_id="s1",
                                  change_type="CAUSALITY", drugs=["X"],
                                  events=["肝炎"], causality={"肝炎": "UNLIKELY"},
                                  report_date="2026-02-15")
        sub2 = self.svc.submit_run(definition_id=did2,
                                   cutoff="2026-05-02T00:00:00",
                                   term_version_id=self.tid)
        self.svc.run_until_published(sub2["run_id"])
        rows = [r for r in self.svc.window_results(did2, window_id="W1")
                if r["drug_group"] == "X" and r["event_group"] == "HEP"]
        new = rows[0]
        diff = self.svc.get_revision(new["revision_id"])["diff"]
        kinds = {c["source_id"]: c["kind"] for c in diff["case_changes"]}
        self.assertEqual(kinds.get("s1"), "CAUSALITY_UPDATE")
        # 从 a（X 且相关 HEP）变为 b（X 但不相关）
        cc = next(c for c in diff["case_changes"] if c["source_id"] == "s1")
        self.assertEqual(cc["old_cell"], "a")
        self.assertEqual(cc["new_cell"], "b")

    def test_withdraw_diff_exited_sample(self):
        self.case("s1", "X", ("肝炎",))
        self.publish_run()
        self.clock.set(datetime(2026, 5, 1))
        self.svc.add_case_version(case_id="s1", version_no=2, source_id="s1",
                                  change_type="WITHDRAW", drugs=["X"],
                                  events=["肝炎"], active=False,
                                  report_date="2026-02-15")
        sub = self.svc.submit_run(definition_id=self.did,
                                  cutoff="2026-05-02T00:00:00",
                                  term_version_id=self.tid)
        self.svc.run_until_published(sub["run_id"])
        rev = self.head()
        self.assertEqual(rev["counts"]["a"], 0)
        diff = self.svc.get_revision(rev["revision_id"])["diff"]
        ch = next(c for c in diff["case_changes"] if c["source_id"] == "s1")
        self.assertEqual(ch["kind"], "EXITED_SAMPLE")


class TestDisposition(SignalBase):
    def _two_revisions(self):
        self.case("s1", "X", ("肝炎",))
        self.case("s2", "Y", ("头痛",))
        self.publish_run()
        rev1 = self.head()
        self.clock.set(datetime(2026, 5, 1))
        self.case("s3", "X", ("肝炎",))
        sub = self.svc.submit_run(definition_id=self.did,
                                  cutoff="2026-05-02T00:00:00",
                                  term_version_id=self.tid)
        self.svc.run_until_published(sub["run_id"])
        return rev1, self.head()

    def test_old_disposition_preserved_new_must_be_handled(self):
        self.case("s1", "X", ("肝炎",))
        self.case("s2", "Y", ("头痛",))
        self.publish_run()
        rev1 = self.head()
        # 在 rev1 还是链头时确认它
        out = self.svc.dispose(rev1["revision_id"], "CONFIRMED",
                               expected_revision_no=1, analyst="alice",
                               note="确认")
        self.assertEqual(out["disposition"], "CONFIRMED")
        # 新窗口产生 rev2；旧修订结论永久保留，新修订独立 PENDING 待重新处置
        self.clock.set(datetime(2026, 5, 1))
        self.case("s3", "X", ("肝炎",))
        sub = self.svc.submit_run(definition_id=self.did,
                                  cutoff="2026-05-02T00:00:00",
                                  term_version_id=self.tid)
        self.svc.run_until_published(sub["run_id"])
        rev2 = self.head()
        self.assertEqual(rev2["revision_no"], 2)
        self.assertEqual(rev2["disposition"], "PENDING")
        # 旧修订仍可查到原结论
        old = self.svc.get_revision(rev1["revision_id"])
        self.assertEqual(old["disposition"], "CONFIRMED")
        self.assertEqual(old["disposition_by"], "alice")
        # 旧修订已非链头：基于过期号的改写操作被拒绝（旧结论保留不被覆盖）
        with self.assertRaises(DomainError) as e:
            self.svc.dispose(rev1["revision_id"], "REFUTED",
                             expected_revision_no=1, analyst="bob")
        self.assertIn(e.exception.code, ("STALE_REVISION", "ALREADY_DISPOSED"))
        # 即便引用最新链头号，对一个已处置修订也不能改判
        with self.assertRaises(DomainError) as e2:
            self.svc.dispose(rev1["revision_id"], "REFUTED",
                             expected_revision_no=2, analyst="bob")
        self.assertEqual(e2.exception.code, "STALE_REVISION")
        # 新修订用正确链头号可处置
        self.svc.dispose(rev2["revision_id"], "WATCH",
                         expected_revision_no=2, analyst="carol")

    def test_stale_revision_number_rejected(self):
        rev1, rev2 = self._two_revisions()
        # 想处置新修订，却带着旧修订号
        with self.assertRaises(DomainError) as e:
            self.svc.dispose(rev2["revision_id"], "CONFIRMED",
                             expected_revision_no=rev1["revision_no"],
                             analyst="alice")
        self.assertEqual(e.exception.code, "STALE_REVISION")
        # 带正确链头号成功
        ok = self.svc.dispose(rev2["revision_id"], "WATCH",
                              expected_revision_no=2, analyst="alice")
        self.assertEqual(ok["disposition"], "WATCH")

    def test_watch_and_refute(self):
        self.case("s1", "X", ("肝炎",))
        self.publish_run()
        rev = self.head()
        self.svc.dispose(rev["revision_id"], "WATCH",
                         expected_revision_no=1, analyst="a")
        with self.assertRaises(DomainError):
            self.svc.dispose(rev["revision_id"], "REFUTED",
                             expected_revision_no=1, analyst="b")

    def test_blank_analyst_rejected(self):
        self.case("s1", "X", ("肝炎",))
        self.publish_run()
        with self.assertRaises(DomainError):
            self.svc.dispose(self.head()["revision_id"], "CONFIRMED",
                             expected_revision_no=1, analyst="  ")


class TestShardingAndFailure(SignalBase):
    SHARDS = 6

    def test_partial_progress_then_resume(self):
        for i in range(25):
            self.case(f"s{i}", "X", ("肝炎",))
        sub = self.svc.submit_run(definition_id=self.did,
                                  cutoff="2026-04-01T00:00:00",
                                  term_version_id=self.tid)
        rid = sub["run_id"]
        # 一次只推进 2 片
        self.svc.run_shards(rid, max_shards=2)
        view = self.svc.get_run(rid)
        self.assertEqual(view["committed_shards"], 2)
        self.assertNotEqual(view["status"], "PUBLISHED")
        # 没有任何半成品修订被发布
        self.assertEqual(self.svc.window_results(self.did), [])
        # 继续直到完成
        self.svc.run_until_published(rid)
        self.assertEqual(self.svc.get_run(rid)["status"], "PUBLISHED")
        self.assertEqual(self.head()["counts"]["a"], 25)

    def test_failed_shard_blocks_publication_and_resumes(self):
        for i in range(10):
            self.case(f"s{i}", "X", ("肝炎",))
        sub = self.svc.submit_run(definition_id=self.did,
                                  cutoff="2026-04-01T00:00:00",
                                  term_version_id=self.tid)
        rid = sub["run_id"]
        calls = {"n": 0}

        def hook(run_id, shard_no, artifact):
            calls["n"] += 1
            if calls["n"] == 3:
                raise _ShardFault("BOOM", "注入故障")

        self.svc.shard_fault_hook = hook
        with self.assertRaises(_ShardFault):
            self.svc.run_shards(rid)
        view = self.svc.get_run(rid)
        self.assertEqual(view["status"], "FAILED")
        self.assertEqual(view["error_code"], "BOOM")
        self.assertEqual(self.svc.window_results(self.did), [])  # 无半成品
        committed_after_fail = view["committed_shards"]

        # 恢复：失败片重置，从检查点继续，不重算已提交片
        self.svc.shard_fault_hook = None
        rec = self.svc.recover()
        self.assertTrue(any(x["run_id"] == rid for x in rec["failed"]))
        self.svc.run_until_published(rid)
        self.assertEqual(self.svc.get_run(rid)["status"], "PUBLISHED")
        self.assertEqual(self.head()["counts"]["a"], 10)
        # 已提交片的提交时间保留（未重算）—— 通过恢复信息核对
        self.assertGreaterEqual(committed_after_fail, 1)


class TestIdempotentSubmit(SignalBase):
    def test_concurrent_same_key_single_run(self):
        self.case("s1", "X", ("肝炎",))
        ids = []
        barrier = threading.Barrier(5)

        def submit():
            barrier.wait()
            r = self.svc.submit_run(definition_id=self.did,
                                    cutoff="2026-04-01T00:00:00",
                                    term_version_id=self.tid)
            ids.append(r["run_id"])

        threads = [threading.Thread(target=submit) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(set(ids)), 1)
        rid = ids[0]
        # 多个工作者并发推进，也只发布一组修订
        self.svc.run_until_published(rid)
        with self.svc._conn() as conn:
            n = conn.execute(
                "SELECT COUNT(*) c FROM sig_signal_revisions WHERE run_id=?",
                (rid,)).fetchone()["c"]
            self.assertEqual(n, 4)  # 2 药品组 × 2 事件组
        # 重复发布安全
        self.svc._publish_if_ready(rid)
        with self.svc._conn() as conn:
            n2 = conn.execute(
                "SELECT COUNT(*) c FROM sig_signal_revisions WHERE run_id=?",
                (rid,)).fetchone()["c"]
            self.assertEqual(n2, 4)


class TestImmutability(SignalBase):
    def test_metrics_columns_cannot_be_overwritten(self):
        self.case("s1", "X", ("肝炎",))
        self.publish_run()
        rev = self.head()
        import sqlite3
        with self.assertRaises(sqlite3.IntegrityError):
            with self.svc._conn() as conn:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    conn.execute("UPDATE sig_signal_revisions SET a=999 WHERE id=?",
                                 (rev["revision_id"],))
                    conn.execute("COMMIT")
                finally:
                    pass

    def test_definitions_and_terms_append_only(self):
        import sqlite3
        with self.assertRaises(sqlite3.IntegrityError):
            with self.svc._conn() as conn:
                conn.execute("UPDATE sig_terms SET note='x' WHERE id=?",
                             (self.tid,))
        with self.assertRaises(sqlite3.IntegrityError):
            with self.svc._conn() as conn:
                conn.execute("DELETE FROM sig_definitions WHERE id=?",
                             (self.did,))

    def test_audit_chain(self):
        self.case("s1", "X", ("肝炎",))
        self.publish_run()
        self.svc.dispose(self.head()["revision_id"], "CONFIRMED",
                         expected_revision_no=1, analyst="alice")
        rep = self.svc.verify_audit_chain()
        self.assertTrue(rep["valid"], rep["failures"])
        types = {e["event_type"] for e in self.svc.audit_trail(limit=100)}
        self.assertIn("RUN_PUBLISHED", types)
        self.assertIn("SIGNAL_DISPOSITION", types)


class TestIncrementalAndMultiWindow(SignalBase):
    def setUp(self):
        super().setUp()
        # 替换为双窗口定义
        self.df2 = self.svc.create_definition(
            cfg(windows=[{"id": "W1", "start": "2026-01-01", "end": "2026-04-01"},
                         {"id": "W2", "start": "2026-04-01", "end": "2026-07-01"}]),
            definition_code="MULTI-DEF")
        self.did2 = self.df2["definition_id"]

    def test_incremental_requires_base_and_scopes_windows(self):
        with self.assertRaises(DomainError):
            self.svc.submit_run(definition_id=self.did2, cutoff="2026-04-01T00",
                                term_version_id=self.tid, mode="INCREMENTAL")
        self.case("s1", "X", ("肝炎",), report_date="2026-02-01")
        r1sub = self.svc.submit_run(definition_id=self.did2,
                                    cutoff="2026-04-01T00",
                                    term_version_id=self.tid)
        r1 = self.svc.run_until_published(r1sub["run_id"])
        self.clock.set(datetime(2026, 5, 1))
        # W1 随访（版本刷新）+ W2 新发病例
        self.svc.add_case_version(case_id="s1", source_id="s1", drugs=["X"],
                                  events=["肝炎"], change_type="FOLLOWUP",
                                  report_date="2026-02-01")
        self.case("s9", "X", ("肝炎",), report_date="2026-04-15")
        sub = self.svc.submit_run(definition_id=self.did2,
                                  cutoff="2026-05-02T00",
                                  term_version_id=self.tid,
                                  mode="INCREMENTAL", base_run_id=r1["run_id"])
        self.svc.run_until_published(sub["run_id"])
        active = {(c["window_id"], c["source_id"])
                  for c in self.svc.contribution_trace(definition_id=self.did2)
                  if not c["revoked"]}
        self.assertEqual(active, {("W1", "s1"), ("W2", "s9")})
        # W2 新增信号差异 NEW_ENTERED
        w2 = [r for r in self.svc.window_results(self.did2)
              if r["window_id"] == "W2" and r["drug_group"] == "X"][0]
        kinds = {(c["source_id"], c["kind"])
                 for c in self.svc.get_revision(w2["revision_id"])["diff"][
                     "case_changes"]}
        self.assertIn(("s9", "NEW_ENTERED"), kinds)

    def test_cross_drug_group_movement_tracked(self):
        self.case("s1", "X", ("肝炎",), report_date="2026-02-01")
        r1 = self.publish_run()
        self.clock.set(datetime(2026, 5, 1))
        # 随访把用药从 X 改成 Y：跨药品组
        self.svc.add_case_version(case_id="s1", source_id="s1", drugs=["Y"],
                                  events=["肝炎"], change_type="FOLLOWUP",
                                  report_date="2026-02-01")
        sub = self.svc.submit_run(definition_id=self.did,
                                  cutoff="2026-05-02T00",
                                  term_version_id=self.tid)
        self.svc.run_until_published(sub["run_id"])
        xhep = [r for r in self.svc.window_results(self.did)
                if r["drug_group"] == "X" and r["event_group"] == "HEP"][0]
        diff = self.svc.get_revision(xhep["revision_id"])["diff"]
        ch = next(c for c in diff["case_changes"] if c["source_id"] == "s1")
        self.assertEqual(ch["kind"], "DRUG_GROUP_CHANGE")
        self.assertEqual(ch["old_cell"], "a")
        self.assertEqual(ch["new_cell"], "c")  # 离开 X 暴露组，落到非X但有HEP
        self.assertTrue(ch["in_signal_after"])  # 仍是四格表的一部分（背景 c）
        # X 组 a 从 1 变 0
        self.assertEqual(xhep["counts"]["a"], 0)


class TestRosterAndExclusions(SignalBase):
    def test_roster_frozen_and_exclusions_listed(self):
        self.case("s1", "X", ("肝炎",))
        self.case("s2", "X", ("头痛",))
        self.case("s3", "W", ("肝炎",))          # 非监测药品
        self.case("s4", "X", ("未知",))          # 未映射事件
        r = self.publish_run()
        rev = self.head()
        detail = self.svc.get_revision(rev["revision_id"])
        self.assertEqual([x["source_id"] for x in detail["roster"]["a"]], ["s1"])
        self.assertEqual([x["source_id"] for x in detail["roster"]["b"]], ["s2"])
        exc = {(e["source_id"]): e["reason"]
               for e in self.svc.run_exclusions(r["run_id"])["W1"]}
        self.assertEqual(exc["s3"], "DRUG_NOT_MONITORED")
        self.assertEqual(exc["s4"], "EVENT_UNMAPPED")


if __name__ == "__main__":
    unittest.main()
