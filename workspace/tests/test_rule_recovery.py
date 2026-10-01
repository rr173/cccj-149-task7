"""规则治理批量作业/试算的进程中断、检查点续跑、分片失败、幂等测试。"""
import tempfile
import shutil
import unittest

from tests.base import BasePVTest
from pv_service.rule_pkg import build_content
from pv_service.rule_gov import RuleGovernanceService, ProcessInterrupted
from pv_service.service import DomainError

SECRET = "pv-demo-signing-secret"


def make_content(version="2026.1", **over):
    params = dict(jurisdiction="CN", version=version,
                  effective_from="2026-10-01",
                  serious_days=10, listed_days=20, routine_days=45)
    params.update(over)
    return build_content(**params)


class RuleRecoveryTest(BasePVTest):
    def _gov_small(self, shard_size=2, crash_hook=None):
        return RuleGovernanceService(
            self.app.db_path, self.app.cases, clock=self.clock,
            shared_lock=self.app._lock, shard_size=shard_size,
            seed_demo_signer=True, auto_start=False, crash_hook=crash_hook)

    def _cases(self, n, *, sent=0):
        rids = []
        for i in range(n):
            payload = {"patient_name": f"P{i}", "dob": "1980-01-01",
                       "drugs": [{"name": "布洛芬", "causality": "可能"}],
                       "reactions": ["皮疹"],
                       "seriousness": "严重" if i % 2 else "不严重",
                       "event_date": "2026-09-07"}
            rid = self.app.cases.intake(
                "HOSPITAL", f"H-{i}", payload,
                "2026-09-10T09:00:00")["report"]["id"]
            rids.append(rid)
        for r in rids[:sent]:
            self.app.reporting.send_one(r)
        return rids

    def _reg(self, gov, **over):
        c = make_content(**over)
        gov.register_package(c, gov.sign(c, SECRET))
        return c

    # -- 试算中断续跑，不重复计数 --------------------------------------------
    def test_trial_resumes_from_checkpoint_no_double_count(self):
        self._cases(5)
        gov = self._gov_small(shard_size=2)
        self.app.rules_gov = gov
        self.app.cases.rule_resolver = gov.resolve
        self._reg(gov)
        t = gov.create_trial("CN", "2026.1")

        state = {"hits": 0}
        def hook(phase, tid, shard, pos):
            if phase == "TRIAL_UNIT" and state["hits"] == 2:
                state["hits"] += 1
                raise ProcessInterrupted("crash")
            state["hits"] += 1
        gov.crash_hook = hook
        with self.assertRaises(ProcessInterrupted):
            gov.run_trial(t["trial_id"])
        gov.crash_hook = None

        mid = gov.get_trial(t["trial_id"])
        self.assertEqual(mid["done_units"], 2)
        gov.recover()
        done = gov.run_trial_until_done(t["trial_id"])
        self.assertEqual(done["status"], "COMPLETED")
        self.assertEqual(done["done_units"], 5)
        # changed_units 与实际 changed 行数一致（无重复累加）
        changed_rows = len(gov.trial_diffs(t["trial_id"]))
        self.assertEqual(changed_rows, done["changed_units"])

    # -- 启用作业中断：无部分启用，续跑只补未完成分片 ------------------------
    def test_job_crash_leaves_no_partial_activation(self):
        rids = self._cases(6, sent=2)
        gov = self._gov_small(shard_size=2)
        self.app.rules_gov = gov
        self.app.cases.rule_resolver = gov.resolve
        self._reg(gov)

        fired = {"v": 0}
        def hook(phase, jid, shard, pos):
            if phase == "JOB_UNIT" and shard == 0 and pos == 1 and fired["v"] == 0:
                fired["v"] = 1
                raise ProcessInterrupted("crash after unit 1")
        gov.crash_hook = hook
        with self.assertRaises(ProcessInterrupted):
            gov.activate("CN", "2026.1")
        gov.crash_hook = None

        job = gov.list_jobs("CN")[0]
        self.assertFalse(job["applied"])
        self.assertEqual(job["staged_units"], 1)
        # 版本指针未动；业务报告与发送状态未变
        self.assertEqual(gov.current_version("CN")["version"], "0")
        self.assertEqual(self.app.reporting.get_report(rids[2])["days_due"], 90)
        self.assertEqual(self.app.reporting.get_report(rids[0])["status"], "ACKED")
        self.assertEqual(len(gov.correction_candidates()), 0)

        # 恢复续跑
        gov.recover()
        resumed = gov.resume_pending()["jobs"]
        job2 = next(j for j in resumed if j["job_id"] == job["job_id"])
        self.assertEqual(job2["status"], "COMPLETED")
        self.assertEqual(job2["staged_units"], 6)
        self.assertEqual(gov.current_version("CN")["version"], "2026.1")
        # 2 个已发送 -> 2 个更正候选；其余未发送重算
        self.assertEqual(len(gov.correction_candidates()), 2)
        self.assertEqual(self.app.reporting.get_report(rids[2])["rule_version"],
                         "2026.1")

    def test_resume_is_idempotent(self):
        self._cases(4, sent=1)
        gov = self._gov_small(shard_size=2)
        self.app.rules_gov = gov
        self.app.cases.rule_resolver = gov.resolve
        self._reg(gov)
        gov.activate("CN", "2026.1")
        audit_before = len(gov.audit_trail(limit=1000))
        corr_before = len(gov.correction_candidates())
        gov.resume_pending()
        self.assertEqual(len(gov.audit_trail(limit=1000)), audit_before)
        self.assertEqual(len(gov.correction_candidates()), corr_before)

    # -- 分片失败：标记失败、不应用、可重新发起 ------------------------------
    def test_shard_failure_does_not_apply(self):
        rids = self._cases(4)
        gov = self._gov_small(shard_size=2)
        self.app.rules_gov = gov
        self.app.cases.rule_resolver = gov.resolve
        self._reg(gov)

        def hook(phase, jid, shard, pos):
            if phase == "JOB_UNIT" and shard == 1:
                raise RuntimeError("disk failure")
        gov.crash_hook = hook
        with self.assertRaises(DomainError) as cm:
            gov.activate("CN", "2026.1")
        self.assertEqual(cm.exception.code, "SHARD_FAILED")
        gov.crash_hook = None

        job = gov.list_jobs("CN")[0]
        self.assertEqual(job["status"], "FAILED")
        self.assertFalse(job["applied"])
        self.assertEqual(gov.current_version("CN")["version"], "0")
        # 业务报告未变
        for r in rids:
            self.assertEqual(self.app.reporting.get_report(r)["rule_version"], "0")
        # 审计记录失败事件
        self.assertTrue(any(e["event_type"] == "RULE_JOB_FAILED"
                            for e in gov.audit_trail(limit=1000)))


if __name__ == "__main__":
    unittest.main()
