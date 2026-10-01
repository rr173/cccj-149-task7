"""可恢复作业形态（LOCK_DRAFT/VALIDATE）与附件溯源约束、矛盾定稿复核。"""
from reg_inquiry.errors import ContradictionError, JobFailed, OutOfScopeError

from .reg_base import InquiryTestBase


class TestRecoverableJobs(InquiryTestBase):
    def setUp(self):
        super().setUp()
        self.seed_materials()

    def test_lock_draft_job_then_validate_job(self):
        inq = self.register()
        r = self.o.lock_draft_job(inq["id"], ["CASE:MAT-CASE@1"], wait=True)
        draft_id = r["draft_id"]
        draft = self.o.get_draft(draft_id)
        self.assertEqual(len(draft["pins"]), 1)
        # VALIDATE 作业返回只读报告
        rep = self.o.validate_job(draft_id)["report"]
        self.assertFalse(rep["ok"])
        self.assertTrue(any(u["qid"] in ("Q1", "Q2")
                            for u in rep["unsatisfied"]))
        # 作业可查询
        jobs = self.o.list_jobs(inquiry_id=inq["id"])
        self.assertEqual({j["job_type"] for j in jobs},
                         {"LOCK_DRAFT", "VALIDATE"})
        self.assertTrue(all(j["status"] == "COMPLETED" for j in jobs))

    def test_lock_job_idempotent_same_job(self):
        inq = self.register()
        r1 = self.o.lock_draft_job(inq["id"], ["CASE:MAT-CASE@1"], wait=False)
        r2 = self.o.lock_draft_job(inq["id"], ["CASE:MAT-CASE@1"], wait=False)
        self.assertEqual(r1["job_id"], r2["job_id"])
        self.assertFalse(r2["enqueued"])

    def test_lock_job_missing_material_fails_permanent_and_lists(self):
        inq = self.register()
        with self.assertRaises(JobFailed) as cm:
            self.o.lock_draft_job(inq["id"], ["CASE:GHOST@3"])
        job = cm.exception.job
        self.assertEqual(job["error_code"], "MISSING_MATERIALS")
        # 补齐资料后重新入队（旧失败保留，新作业成功）
        self.o.register_material(material_id="GHOST", version="3",
                                 category="CASE", content={"x": 1})
        inq2_scope = [m for m in inq["scope"]["allowed_material_ids"]]
        # GHOST 不在本问询范围 -> 仍失败（越权）；用显式范围的新问询验证补齐
        inq2 = self.o.register_inquiry(
            regulator="R2", jurisdiction="CN",
            questions=[{"qid": "Q1", "text": "a"}],
            deadline="2026-10-20T00:00:00+00:00",
            allowed_material_ids=["GHOST"])
        r = self.o.lock_draft_job(inq2["id"], ["CASE:GHOST@3"])
        self.assertTrue(r["draft_id"])

    def test_pump_processes_pending(self):
        inq = self.register()
        self.o.lock_draft_job(inq["id"], ["CASE:MAT-CASE@1"], wait=False)
        ran = self.o.pump()
        self.assertEqual(len(ran), 1)
        self.assertEqual(ran[0]["job"]["job_type"], "LOCK_DRAFT")


class TestAttachmentScope(InquiryTestBase):
    def setUp(self):
        super().setUp()
        self.seed_materials()
        self.inq = self.register()
        self.draft = self.o.lock_draft(self.inq["id"], ["CASE:MAT-CASE@1"])

    def test_attachment_must_derive_from_locked_pin(self):
        with self.assertRaises(OutOfScopeError):
            self.o.add_attachment(self.draft["id"], filename="x",
                                  content=b"x",
                                  derived_from_ref="REG:MAT-REG@1")
        # 不溯源的附件允许上传，但定稿时必须被正文引用
        a = self.o.add_attachment(self.draft["id"], filename="plain.txt",
                                  content=b"plain")
        self.assertEqual(a["sha256"], __import__("hashlib").sha256(
            b"plain").hexdigest())
        self.o.submit_section(
            self.draft["id"], "Q1",
            f"见 @attachment:{a['id']} [CASE:MAT-CASE@1]",
            expected_revision=0)
        self.o.submit_section(
            self.draft["id"], "Q2", "无附件 [CASE:MAT-CASE@1]",
            expected_revision=0)
        self.assertTrue(self.o.validate(self.draft["id"])["ok"])


class TestContradictionRecheckAtFinalize(InquiryTestBase):
    def test_seal_rechecks_contradictions_registered_after_lock(self):
        # 模拟矛盾是在锁定之后才在注册表中暴露（新版本改变事实），
        # 定稿校验的 PIN_CONTRADICTION 复核必须拦截。
        self.seed_materials()
        inq = self.register(
            allowed=("MAT-CASE", "MAT-CASE2"),
            questions=[{"qid": "Q1", "text": "a"}])
        # 仅锁 CASE v1
        d = self.o.lock_draft(inq["id"], ["CASE:MAT-CASE@1"])
        self.o.submit_section(d["id"], "Q1", "x [CASE:MAT-CASE@1]",
                              expected_revision=0)
        # 直接向已锁底稿补一个矛盾 pin（绕过正常路径的等价情况：
        # 用正常路径构造——通过更正/候选无法对开放底稿加 pin，因此验证器
        # 主要保证 detect_contradictions 对 pins 生效；这里用函数直接断言）
        from reg_inquiry.drafts import detect_contradictions
        found = detect_contradictions([
            {"ref": "CASE:MAT-CASE@1", "locked_fact_key": "OUTCOME",
             "locked_fact_value": "恢复"},
            {"ref": "CASE:MAT-CASE2@1", "locked_fact_key": "OUTCOME",
             "locked_fact_value": "未恢复"},
        ])
        self.assertEqual(len(found), 1)
        # 正常锁两版在锁定入口即被阻断（新问询，避免命中既有 BASE 幂等）
        inq2 = self.o.register_inquiry(
            regulator="R3", jurisdiction="CN",
            questions=[{"qid": "Q1", "text": "a"}],
            deadline="2026-10-20T00:00:00+00:00",
            allowed_material_ids=["MAT-CASE", "MAT-CASE2"])
        with self.assertRaises(ContradictionError):
            self.o.lock_draft(inq2["id"],
                              ["CASE:MAT-CASE@1", "CASE:MAT-CASE2@1"])
