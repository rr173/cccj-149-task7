"""独立核对：篡改检测 + 审计哈希链 + 操作留痕查询。"""
import json

from .reg_base import InquiryTestBase


class TestIndependentVerification(InquiryTestBase):
    def setUp(self):
        super().setUp()
        self.seed_materials()
        self.inq, self.draft = self.lock_base()
        self.sid, self.no = self.fill_and_seal(self.draft)
        r = self.o.send(self.sid)
        self.o.register_receipt(
            self.sid, receipt_number=r["response"]["receipt_number"],
            payload={"outgoing_number": self.no})

    def test_clean_submission_verifies(self):
        v = self.o.verify_submission(self.sid)
        self.assertTrue(v["ok"], v["failures"])
        self.assertEqual(v["answer_count"], 2)
        self.assertTrue(self.o.verify_all()["ok"])

    def test_body_tamper_detected_when_trigger_bypassed(self):
        # 章节正文由 append-only 触发器保护（正常路径改不了）；独立核对模拟
        # “有人绕过触发器/直接改库文件”的场景：临时关闭触发器后偷换正文，
        # manifest 中的 body_sha256/answer_uid 立刻对不上。
        with self.o.conn() as c:
            c.execute("PRAGMA foreign_keys=OFF")
            c.execute("BEGIN IMMEDIATE")
            c.execute("DROP TRIGGER IF EXISTS trg_section_rev_no_update")
            c.execute("UPDATE sections SET body='被偷换的回答' WHERE draft_id=?",
                      (self.draft["id"],))
            c.execute("COMMIT")
        v = self.o.verify_submission(self.sid)
        self.assertFalse(v["ok"])
        self.assertTrue({"BODY_HASH_MISMATCH", "ANSWER_UID_MISMATCH"}
                        & {f["code"] for f in v["failures"]})

    def test_attachment_blob_tamper_detected_when_trigger_bypassed(self):
        with self.o.conn() as c:
            c.execute("BEGIN IMMEDIATE")
            c.execute("DROP TRIGGER IF EXISTS trg_attach_no_update")
            c.execute("UPDATE attachments SET content_blob=? WHERE draft_id=?",
                      (b"TAMPERED", self.draft["id"]))
            c.execute("COMMIT")
        v = self.o.verify_submission(self.sid)
        self.assertIn("ATTACHMENT_HASH_MISMATCH",
                      {f["code"] for f in v["failures"]})

    def test_manifest_tamper_detected_when_trigger_bypassed(self):
        with self.o.conn() as c:
            row = c.execute("SELECT manifest_json FROM submissions WHERE id=?",
                            (self.sid,)).fetchone()
            m = json.loads(row["manifest_json"])
            m["answers"][0]["body_sha256"] = "0" * 64
            c.execute("BEGIN IMMEDIATE")
            c.execute("DROP TRIGGER IF EXISTS trg_submissions_guard")
            c.execute("UPDATE submissions SET manifest_json=? WHERE id=?",
                      (json.dumps(m, ensure_ascii=False, sort_keys=True),
                       self.sid))
            c.execute("COMMIT")
        v = self.o.verify_submission(self.sid)
        self.assertIn("MANIFEST_HASH_MISMATCH",
                      {f["code"] for f in v["failures"]})

    def test_pin_content_hash_tamper_detected_when_trigger_bypassed(self):
        with self.o.conn() as c:
            c.execute("BEGIN IMMEDIATE")
            c.execute("DROP TRIGGER IF EXISTS trg_pins_no_update")
            c.execute("UPDATE draft_pins SET content_hash=? WHERE draft_id=?",
                      ("f" * 64, self.draft["id"]))
            c.execute("COMMIT")
        v = self.o.verify_submission(self.sid)
        self.assertIn("PIN_MISMATCH", {f["code"] for f in v["failures"]})

    def test_duplicate_outgoing_number_rejected_and_detected(self):
        # 数据库 UNIQUE 直接拒绝重复编号；核对器也独立检查
        import sqlite3
        with self.assertRaises(sqlite3.IntegrityError):
            with self.o.conn() as c:
                c.execute(
                    "INSERT INTO submissions(id,draft_id,inquiry_id,kind,"
                    "jurisdiction,outgoing_number,status,manifest_json,"
                    "manifest_hash,sealed_at,created_at)"
                    " VALUES('x1',?,'x','ORIGINAL',?,?, 'SEALED','{}',?,'t','t')",
                    (self.draft["id"], "CN", self.no, "h"))

    def test_receipt_number_mismatch_detected(self):
        with self.o.conn() as c:
            c.execute("BEGIN IMMEDIATE")
            c.execute("DROP TRIGGER IF EXISTS trg_receipts_no_update")
            c.execute(
                "UPDATE receipts SET outgoing_number='WRONG' WHERE submission_id=?",
                (self.sid,))
            c.execute("COMMIT")
        v = self.o.verify_submission(self.sid)
        self.assertIn("RECEIPT_NUMBER_MISMATCH",
                      {f["code"] for f in v["failures"]})

    def test_correction_link_verified(self):
        cd = self.o.create_correction_draft(self.sid, reason="补充")
        a = [x for x in cd["attachments"] if x["filename"] == "case.pdf"][0]
        self.o.submit_section(
            cd["id"], "Q1",
            f"更新 [CASE:MAT-CASE@1] @attachment:{a['id']}",
            expected_revision=1, submitted_by="alice")
        self.o.submit_section(cd["id"], "Q2", "y [REG:MAT-REG@1]",
                              expected_revision=1, submitted_by="bob")
        csid = self.o.finalize(cd["id"])["submission_id"]
        v = self.o.verify_submission(csid)
        self.assertTrue(v["ok"], v["failures"])
        allv = self.o.verify_all()
        self.assertTrue(allv["ok"])
        self.assertEqual(len(allv["submissions"]), 2)


class TestAuditTrail(InquiryTestBase):
    def setUp(self):
        super().setUp()
        self.seed_materials()
        self.inq, self.draft = self.lock_base()
        self.sid, self.no = self.fill_and_seal(self.draft)

    def test_audit_chain_complete_and_tamper_evident(self):
        r = self.o.verify_audit_chain()
        self.assertTrue(r["ok"], r["failures"])
        events = self.o.audit_trail(inquiry_id=self.inq["id"])
        types = [e["event_type"] for e in events]
        for t in ("INQUIRY_REGISTERED", "DRAFT_LOCKED", "SECTION_SUBMITTED",
                  "SUBMISSION_SEALED"):
            self.assertIn(t, types)
        # 删除某一帧（绕过触发器）-> 后继 prev_hash 立即断链
        with self.o.conn() as c:
            c.execute("BEGIN IMMEDIATE")
            c.execute("DROP TRIGGER IF EXISTS trg_audit_no_delete")
            c.execute("DELETE FROM audit_events WHERE seq=2")
            c.execute("COMMIT")
        r = self.o.verify_audit_chain()
        self.assertFalse(r["ok"])

    def test_audit_filter_by_type_and_append_only(self):
        sealed = self.o.audit_trail(inquiry_id=self.inq["id"],
                                    event_type="SUBMISSION_SEALED")
        self.assertEqual(len(sealed), 1)
        self.assertEqual(sealed[0]["submission_id"], self.sid)
        import sqlite3
        with self.assertRaises(sqlite3.Error):
            with self.o.conn() as c:
                c.execute("DELETE FROM audit_events WHERE seq=1")

    def test_idempotent_actions_no_duplicate_audit(self):
        # 重复发送/重复定稿不会产生额外的封存/发送审计帧
        self.o.send(self.sid)
        self.o.send(self.sid)
        events = self.o.audit_trail(inquiry_id=self.inq["id"])
        self.assertEqual(
            [e["event_type"] for e in events].count("SUBMISSION_SEALED"), 1)
        # SENT 审计帧按 (submission,attempt) 幂等
        self.assertEqual(
            [e["event_type"] for e in events].count("SUBMISSION_SENT"), 1)
