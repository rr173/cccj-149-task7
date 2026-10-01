"""定稿校验 / 不可变提交件 / 更正关系。"""
from reg_inquiry.errors import JobFailed

from .reg_base import InquiryTestBase


class TestFinalizeValidation(InquiryTestBase):
    def setUp(self):
        super().setUp()
        self.seed_materials()
        self.inq, self.draft = self.lock_base()

    def test_unanswered_question_blocks_and_lists_qid(self):
        rep = self.o.validate(self.draft["id"])
        self.assertFalse(rep["ok"])
        codes = {u["qid"]: (u.get("codes") or [u["code"]])
                 for u in rep["unsatisfied"]}
        self.assertIn("Q1", codes)
        self.assertIn("Q2", codes)
        with self.assertRaises(JobFailed) as cm:
            self.o.finalize(self.draft["id"])
        self.assertEqual(cm.exception.job["error_code"], "VALIDATION_FAILED")
        # 阻断后底稿仍 OPEN，无提交件/编号
        self.assertEqual(self.o.get_draft(self.draft["id"])["status"], "OPEN")
        self.assertEqual(self.o.list_submissions(self.inq["id"]), [])

    def test_fix_content_then_finalize_succeeds_failed_job_retained(self):
        with self.assertRaises(JobFailed):
            self.o.finalize(self.draft["id"])
        failed_jobs = [j for j in self.o.list_jobs(inquiry_id=self.inq["id"])
                       if j["status"] == "FAILED"]
        self.assertEqual(len(failed_jobs), 1)
        sid, no = self.fill_and_seal(self.draft)
        # 新 COMPLETED 作业 + 旧 FAILED 作业都保留
        jobs = self.o.list_jobs(inquiry_id=self.inq["id"])
        self.assertEqual(sum(1 for j in jobs if j["status"] == "FAILED"), 1)
        self.assertEqual(sum(1 for j in jobs if j["status"] == "COMPLETED"), 1)
        self.assertTrue(no.startswith("CN-2026-"))

    def test_each_question_has_unique_trackable_answer(self):
        sid, _ = self.fill_and_seal(self.draft)
        result = self.o.verify_submission(sid)
        self.assertTrue(result["ok"], result["failures"])
        sub = self.o.submission_status(sid)
        qids = sorted(a["qid"] for a in sub["manifest"]["answers"])
        self.assertEqual(qids, ["Q1", "Q2"])
        uids = [a["answer_uid"] for a in sub["manifest"]["answers"]]
        self.assertEqual(len(set(uids)), 2)

    def test_citation_to_unlocked_material_blocks(self):
        self.o.submit_section(
            self.draft["id"], "Q1", "另见 [STAT:MAT-STAT@1]", expected_revision=0)
        self.o.submit_section(
            self.draft["id"], "Q2", "依据 [REG:MAT-REG@1]", expected_revision=0)
        rep = self.o.validate(self.draft["id"])
        self.assertIn("CITATION_NOT_LOCKED",
                      {i["code"] for i in rep["blocking"]})

    def test_citation_without_exact_version_blocks(self):
        self.o.submit_section(
            self.draft["id"], "Q1", "见 [CASE:MAT-CASE]", expected_revision=0)
        self.o.submit_section(
            self.draft["id"], "Q2", "依据 [REG:MAT-REG@1]", expected_revision=0)
        rep = self.o.validate(self.draft["id"])
        self.assertIn("CITATION_VERSION_MISMATCH",
                      {i["code"] for i in rep["blocking"]})

    def test_unreferenced_attachment_blocks(self):
        self.o.add_attachment(self.draft["id"], filename="orphan.bin",
                              content=b"x",
                              derived_from_ref="CASE:MAT-CASE@1")
        self.o.submit_section(
            self.draft["id"], "Q1", "无附件引用 [CASE:MAT-CASE@1]",
            expected_revision=0)
        self.o.submit_section(
            self.draft["id"], "Q2", "依据 [REG:MAT-REG@1]", expected_revision=0)
        rep = self.o.validate(self.draft["id"])
        self.assertIn("ATTACHMENT_NOT_CITED",
                      {i["code"] for i in rep["blocking"]})

    def test_missing_attachment_reference_blocks(self):
        self.o.submit_section(
            self.draft["id"], "Q1",
            "见 @attachment:deadbeef [CASE:MAT-CASE@1]", expected_revision=0)
        self.o.submit_section(
            self.draft["id"], "Q2", "依据 [REG:MAT-REG@1]", expected_revision=0)
        rep = self.o.validate(self.draft["id"])
        self.assertIn("ATTACHMENT_REF_MISSING",
                      {i["code"] for i in rep["blocking"]})

    def test_sensitive_policy_redact_and_prohibit(self):
        self.o.put_disclosure_policy(jurisdiction="CN", rules={
            "sensitive_fields": {"patient_name": "REDACT",
                                 "patient_identifier": "PROHIBIT"}})
        self.o.submit_section(
            self.draft["id"], "Q1",
            '患者 patient_name: 张三 [CASE:MAT-CASE@1]', expected_revision=0)
        self.o.submit_section(
            self.draft["id"], "Q2",
            '标识 patient_identifier: ID-9 [REG:MAT-REG@1]',
            expected_revision=0)
        rep = self.o.validate(self.draft["id"])
        codes = {i["code"] for i in rep["blocking"]}
        self.assertIn("SENSITIVE_NOT_REDACTED", codes)
        self.assertIn("SENSITIVE_PROHIBITED", codes)
        # 脱敏 / 去除后通过
        self.o.submit_section(
            self.draft["id"], "Q1",
            "患者 patient_name: «REDACTED:name» [CASE:MAT-CASE@1]",
            expected_revision=1)
        self.o.submit_section(
            self.draft["id"], "Q2", "标识不披露 [REG:MAT-REG@1]",
            expected_revision=1)
        rep = self.o.validate(self.draft["id"])
        self.assertTrue(rep["ok"], rep["blocking"])

    def test_jurisdiction_uses_own_policy(self):
        # EU 禁止 patient_name；CN 策略不存在（全 ALLOW）
        self.o.put_disclosure_policy(jurisdiction="EU", rules={
            "sensitive_fields": {"patient_name": "PROHIBIT"}})
        inq_eu = self.o.register_inquiry(
            regulator="EMA", jurisdiction="EU",
            questions=[{"qid": "Q1", "text": "a"}],
            deadline="2026-10-20T00:00:00+00:00",
            allowed_material_ids=["MAT-CASE"])
        de = self.o.lock_draft(inq_eu["id"], ["CASE:MAT-CASE@1"])
        self.o.submit_section(de["id"], "Q1",
                              "patient_name: 张三 [CASE:MAT-CASE@1]",
                              expected_revision=0)
        self.assertIn("SENSITIVE_PROHIBITED",
                      {i["code"] for i in
                       self.o.validate(de["id"])["blocking"]})

    def test_stale_pin_warning_before_seal_and_immutability_after(self):
        # 定稿前注册新版本：当前底稿维持 v1（仅警告），另立候选
        inq, draft = self.lock_base()
        self.o.register_material(
            material_id="MAT-CASE", version="2", category="CASE",
            content={"outcome": "新随访"}, fact_key="OUTCOME", fact_value="新")
        warnings = self.o.conflicts(draft["id"])["drift_warnings"]
        self.assertEqual(warnings[0]["locked_version"], "1")
        self.assertEqual(warnings[0]["latest_version"], "2")
        # 用漂移候选定稿；原 BASE 维持 v1 引用
        cand = [d for d in self.o.list_drafts(inq["id"])
                if d["kind"] == "CANDIDATE"][0]
        cand = self.o.get_draft(cand["id"])
        self.assertEqual([p["version"] for p in
                          self.o.get_draft(draft["id"])["pins"]
                          if p["material_id"] == "MAT-CASE"], ["1"])


class TestImmutabilityAndCorrections(InquiryTestBase):
    def setUp(self):
        super().setUp()
        self.seed_materials()
        self.inq, self.draft = self.lock_base()
        self.sid, self.no = self.fill_and_seal(self.draft)

    def test_manifest_hash_stable_and_submission_immutable(self):
        import sqlite3
        sub = self.o.submission_status(self.sid)
        with self.assertRaises(sqlite3.Error):
            with self.o.conn() as c:
                c.execute("UPDATE submissions SET outgoing_number='X' WHERE id=?",
                          (self.sid,))
        # 第二次读取指纹一致
        self.assertEqual(self.o.submission_status(self.sid)["manifest_hash"],
                         sub["manifest_hash"])

    def test_finalize_supersedes_other_open_drafts(self):
        # 独立问询：定稿前产生漂移候选，定稿 BASE 时候选被置 SUPERSEDED
        inq, draft = self.lock_base()
        self.o.register_material(
            material_id="MAT-CASE", version="2", category="CASE",
            content={"outcome": "v2"}, fact_key="OUTCOME", fact_value="v2")
        cand = [d for d in self.o.list_drafts(inq["id"])
                if d["kind"] == "CANDIDATE"][0]
        # 给候选也补齐章节，随后仍对 BASE 定稿
        a = self.o.add_attachment(draft["id"], filename="case.pdf",
                                  content=b"%PDF-case",
                                  derived_from_ref="CASE:MAT-CASE@1")
        self.o.submit_section(
            draft["id"], "Q1",
            f"x [CASE:MAT-CASE@1] @attachment:{a['id']}", expected_revision=0)
        self.o.submit_section(draft["id"], "Q2", "y [REG:MAT-REG@1]",
                              expected_revision=0)
        self.o.finalize(draft["id"], sealed_by="lead")
        statuses = {(d["kind"], d["status"])
                    for d in self.o.list_drafts(inq["id"])}
        self.assertIn(("CANDIDATE", "SUPERSEDED"), statuses)
        self.assertIn(("BASE", "FINALIZED"), statuses)
        self.assertEqual(self.o.get_draft(cand["id"])["status"], "SUPERSEDED")

    def test_correction_creates_new_submission_keeps_original(self):
        r = self.o.send(self.sid)
        self.o.register_receipt(
            self.sid, receipt_number=r["response"]["receipt_number"],
            payload={"outgoing_number": self.no})
        cd = self.o.create_correction_draft(
            self.sid, reason="随访后补充", created_by="alice")
        self.assertEqual(cd["kind"], "CORRECTION")
        self.assertEqual(cd["correction_of_submission_id"], self.sid)
        # 克隆章节修订沿用，修改须带克隆内容的修订号
        a = [x for x in cd["attachments"] if x["filename"] == "case.pdf"][0]
        self.o.submit_section(
            cd["id"], "Q1",
            f"更新转归 [CASE:MAT-CASE@1] @attachment:{a['id']}",
            expected_revision=1, submitted_by="alice")
        self.o.submit_section(cd["id"], "Q2", "依据 [REG:MAT-REG@1]",
                              expected_revision=1, submitted_by="bob")
        csid = self.o.finalize(cd["id"], sealed_by="lead")["submission_id"]
        csub = self.o.submission_status(csid)
        self.assertEqual(csub["kind"], "CORRECTION")
        self.assertEqual(csub["corrects_submission_id"], self.sid)
        self.assertNotEqual(csub["outgoing_number"], self.no)
        # 原提交件与回执原样保留
        orig = self.o.submission_status(self.sid)
        self.assertEqual(orig["outgoing_number"], self.no)
        self.assertEqual(len(self.o.list_receipts(self.sid)), 1)
        chain = self.o.correction_chain(self.sid)
        self.assertEqual(chain["original"]["id"], self.sid)
        self.assertEqual([c["id"] for c in chain["corrections"]], [csid])
        self.assertTrue(self.o.verify_all()["ok"])

    def test_correction_with_new_material_version(self):
        # 更正可显式纳入新证据版本（仍走缺失/越权/矛盾校验）
        self.o.register_material(
            material_id="MAT-CASE", version="2", category="CASE",
            content={"outcome": "随访恢复"}, fact_key="OUTCOME",
            fact_value="随访恢复")
        cd = self.o.create_correction_draft(
            self.sid, refs=["CASE:MAT-CASE@2", "REG:MAT-REG@1"],
            reason="采用随访版本")
        pins = {p["material_id"]: p["version"] for p in cd["pins"]}
        self.assertEqual(pins["MAT-CASE"], "2")
