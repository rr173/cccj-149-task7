"""监管问询答复编排器测试。

覆盖：问询登记幂等、底稿版本锁定、引用漂移产生带原因新候选、资料缺失/
引用越权/版本矛盾阻止定稿、章节乐观并发、问题唯一可追踪、附件一致性、
辖区披露策略、不可变提交件、可恢复作业（崩溃续跑/重复请求/双节点）、
回执幂等、更正关系、独立核对、操作留痕。
"""
import shutil
import sqlite3
import tempfile
import threading
import unittest
from datetime import datetime

from inquiry_reply import DomainError, FixedClock, InquiryReplyService
from inquiry_reply import db as dbmod


class FlakyTransport:
    """前 fail_times 次发送抛瞬时故障，之后委托给真实通道。"""

    def __init__(self, inner, fail_times):
        self.inner = inner
        self.left = fail_times
        self.calls = 0

    def send(self, conn, submission, now):
        self.calls += 1
        if self.left > 0:
            self.left -= 1
            raise DomainError("TRANSPORT_ERROR", "模拟网关瞬时故障", 503)
        return self.inner.send(conn, submission, now)


class BaseInquiryTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="inquiry-reply-test-")
        self.db_path = f"{self.dir}/inquiry_reply.db"
        self.clock = FixedClock(datetime(2026, 9, 20, 9, 0, 0))
        self.svc = self.make_service()
        ev = self.svc.evidence
        ev.upsert("case:1001", "CASE", "患者甲，女，62岁，2型糖尿病，用药后皮疹。")
        ev.upsert("msg:2001", "MESSAGE", "2026-09-01 已报送 AE-2026-001（初始报告）。")
        ev.upsert("reg:3001", "REGULATION", "《药品不良反应报告和监测管理办法》第二十一条。")
        ev.upsert("stat:4001", "STATISTIC", "2026H1 共收到 AE 报告 128 例，其中严重 12 例。")

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def make_service(self, **kw):
        kw.setdefault("worker_id", "w1")
        kw.setdefault("clock", self.clock)
        return InquiryReplyService(self.db_path, **kw)

    # -- 常用构造 ------------------------------------------------------------
    def make_inquiry(self, **kw):
        defaults = dict(
            regulator="NMPA", jurisdiction="CN", deadline="2026-10-15",
            questions=[{"question_id": "Q1", "text": "说明病例情况与已报送情况"},
                       {"question_id": "Q2", "text": "说明统计口径与法规依据"}],
            allowed_scope=["case:*", "msg:*", "reg:*", "stat:*"],
            actor="pm", idempotency_key=None)
        defaults.update(kw)
        return self.svc.register_inquiry(**defaults)["inquiry_id"]

    def make_draft(self, inquiry_id, evidence_ids=None):
        return self.svc.lock_draft(
            inquiry_id=inquiry_id,
            evidence_ids=evidence_ids or ["case:1001", "msg:2001",
                                          "reg:3001", "stat:4001"],
            actor="pm")["draft_id"]

    def fill_draft(self, draft_id):
        self.svc.submit_section(
            draft_id=draft_id, section_key="sec-q1", question_id="Q1",
            content="病例与报送情况见 [[ev:case:1001]]、[[ev:msg:2001]]，"
                    "明细见附件 [[att:A1]]。",
            author="alice", base_revision=0)
        self.svc.submit_section(
            draft_id=draft_id, section_key="sec-q2", question_id="Q2",
            content="统计口径见 [[ev:stat:4001]]，法规依据见 [[ev:reg:3001]]。",
            author="bob", base_revision=0)
        self.svc.put_attachment(
            draft_id=draft_id, attachment_id="A1", name="病例清单",
            content="病例清单：case:1001（随访 2 次）",
            author="alice", base_revision=0)

    def make_finalized(self):
        inq = self.make_inquiry()
        draft = self.make_draft(inq)
        self.fill_draft(draft)
        result = self.svc.finalize_and_send(draft_id=draft, actor="pm")
        return inq, draft, result

    def count(self, table):
        conn = dbmod.connect(self.db_path)
        try:
            return conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"]
        finally:
            conn.close()


class TestInquiryRegistration(BaseInquiryTest):
    def test_register_and_idempotent_replay(self):
        kw = dict(regulator="NMPA", jurisdiction="CN", deadline="2026-10-15",
                  questions=["问题一", "问题二"], allowed_scope=["case:*"],
                  actor="pm", idempotency_key="REQ-1")
        first = self.svc.register_inquiry(**kw)
        self.assertFalse(first["deduped"])
        self.assertEqual([q["question_id"] for q in first["questions"]],
                         ["Q1", "Q2"])
        again = self.svc.register_inquiry(**kw)
        self.assertTrue(again["deduped"])
        self.assertEqual(first["inquiry_id"], again["inquiry_id"])
        self.assertEqual(self.count("inquiries"), 1)
        audits = self.svc.audit_trail("inquiry", first["inquiry_id"])
        self.assertEqual([a["action"] for a in audits], ["INQUIRY_REGISTERED"])

    def test_register_validates_input(self):
        with self.assertRaises(DomainError):
            self.svc.register_inquiry(
                regulator="NMPA", jurisdiction="CN", deadline="2026-10-15",
                questions=[], allowed_scope=["case:*"], actor="pm")
        with self.assertRaises(DomainError):
            self.svc.register_inquiry(
                regulator="NMPA", jurisdiction="CN", deadline="2026-10-15",
                questions=["q"], allowed_scope=[], actor="pm")


class TestDraftLocking(BaseInquiryTest):
    def test_lock_pins_exact_versions(self):
        inq = self.make_inquiry()
        draft = self.svc.get_draft(self.make_draft(inq))
        pins = {c["evidence_id"]: c for c in draft["citations"]}
        self.assertEqual(pins["case:1001"]["version"], 1)
        self.assertEqual(pins["case:1001"]["kind"], "CASE")
        self.assertTrue(pins["case:1001"]["content_hash"])

    def test_evidence_change_never_mixes_into_locked_draft(self):
        inq = self.make_inquiry()
        draft_id = self.make_draft(inq)
        self.svc.evidence.upsert("case:1001", "CASE", "患者甲随访：皮疹已消退。")
        # 旧底稿锁定版本原样保留（变化不悄悄混入）
        pins = {c["evidence_id"]: c
                for c in self.svc.get_draft(draft_id)["citations"]}
        self.assertEqual(pins["case:1001"]["version"], 1)
        # 冲突查询与定稿校验都能发现漂移
        conflicts = self.svc.query_conflicts(draft_id)
        self.assertTrue(conflicts["has_conflicts"])
        self.assertEqual(conflicts["evidence_drift"],
                         [{"evidence_id": "case:1001", "locked_version": 1,
                           "current_version": 2}])
        report = self.svc.validate_draft(draft_id)
        self.assertIn("STALE_CITATION", report["unmet"])

    def test_refresh_creates_reasoned_candidate(self):
        inq = self.make_inquiry()
        old_id = self.make_draft(inq)
        self.fill_draft(old_id)
        self.svc.evidence.upsert("stat:4001", "STATISTIC",
                                 "2026H1 共收到 AE 报告 131 例，其中严重 13 例。")
        new_draft = self.svc.refresh_draft(draft_id=old_id, actor="pm")
        self.assertIsNotNone(new_draft)
        self.assertEqual(new_draft["kind"], "REFRESH")
        self.assertEqual(new_draft["based_on_draft_id"], old_id)
        self.assertEqual(new_draft["reasons"],
                         [{"type": "EVIDENCE_UPDATED", "evidence_id": "stat:4001",
                           "locked_version": 1, "current_version": 2}])
        # 新候选锁定当前版本；章节原样复制；旧底稿被取代且内容未变
        pins = {c["evidence_id"]: c for c in new_draft["citations"]}
        self.assertEqual(pins["stat:4001"]["version"], 2)
        self.assertEqual(len(self.svc.get_sections(new_draft["draft_id"])), 2)
        old = self.svc.get_draft(old_id)
        self.assertEqual(old["status"], "SUPERSEDED")
        old_pins = {c["evidence_id"]: c for c in old["citations"]}
        self.assertEqual(old_pins["stat:4001"]["version"], 1)
        # 新候选可通过校验
        self.assertTrue(self.svc.validate_draft(new_draft["draft_id"])["ok"])
        # 无漂移时 refresh 返回 None
        self.assertIsNone(
            self.svc.refresh_draft(draft_id=new_draft["draft_id"], actor="pm"))

    def test_locked_draft_content_immutable_at_db_level(self):
        inq = self.make_inquiry()
        draft_id = self.make_draft(inq)
        conn = dbmod.connect(self.db_path)
        try:
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute("UPDATE drafts SET citations_json='[]' "
                             "WHERE draft_id=?", (draft_id,))
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute("DELETE FROM drafts WHERE draft_id=?", (draft_id,))
        finally:
            conn.close()


class TestFinalizeBlocking(BaseInquiryTest):
    def assert_finalize_blocked(self, draft_id, *codes):
        report = self.svc.validate_draft(draft_id)
        self.assertFalse(report["ok"])
        for code in codes:
            self.assertIn(code, report["unmet"], report["issues"])
        with self.assertRaises(DomainError) as cm:
            self.svc.finalize_and_send(draft_id=draft_id, actor="pm")
        self.assertEqual(cm.exception.code, "VALIDATION_FAILED")
        for code in codes:
            self.assertIn(code, cm.exception.report["unmet"])
        return cm.exception.report

    def test_missing_evidence_blocks(self):
        inq = self.make_inquiry()
        draft_id = self.make_draft(inq, evidence_ids=["case:1001", "case:9999"])
        self.fill_draft(draft_id)
        report = self.assert_finalize_blocked(draft_id, "MISSING_EVIDENCE")
        detail = [i for i in report["issues"]
                  if i["code"] == "MISSING_EVIDENCE"][0]
        self.assertEqual(detail["detail"]["evidence_id"], "case:9999")

    def test_scope_violation_blocks(self):
        inq = self.make_inquiry(allowed_scope=["case:*"])
        draft_id = self.make_draft(inq, evidence_ids=["case:1001", "msg:2001"])
        self.fill_draft(draft_id)
        self.assert_finalize_blocked(draft_id, "SCOPE_VIOLATION")

    def test_unlocked_inline_citation_blocks(self):
        inq = self.make_inquiry()
        draft_id = self.make_draft(inq, evidence_ids=["case:1001"])
        self.fill_draft(draft_id)  # 正文引用了未锁定的 msg:2001 / reg:3001 / stat:4001
        self.assert_finalize_blocked(draft_id, "UNLOCKED_CITATION")

    def test_version_conflict_blocks(self):
        inq = self.make_inquiry()
        draft_id = self.make_draft(inq)
        self.fill_draft(draft_id)
        # 章节显式引用与锁定版本矛盾的版本
        self.svc.submit_section(
            draft_id=draft_id, section_key="sec-q1", question_id="Q1",
            content="病例见 [[ev:case:1001@9]]，附件 [[att:A1]]。",
            author="alice", base_revision=1)
        self.assert_finalize_blocked(draft_id, "VERSION_CONFLICT")

    def test_conflicting_versions_between_sections_block(self):
        inq = self.make_inquiry()
        draft_id = self.make_draft(inq)
        self.svc.submit_section(
            draft_id=draft_id, section_key="sec-q1", question_id="Q1",
            content="病例见 [[ev:case:1001@1]]，附件 [[att:A1]]。",
            author="alice", base_revision=0)
        self.svc.submit_section(
            draft_id=draft_id, section_key="sec-q2", question_id="Q2",
            content="对比 [[ev:case:1001@2]] 与 [[ev:stat:4001]]。",
            author="bob", base_revision=0)
        self.svc.put_attachment(
            draft_id=draft_id, attachment_id="A1", name="清单",
            content="case:1001", author="alice", base_revision=0)
        self.assert_finalize_blocked(draft_id, "VERSION_CONFLICT")

    def test_stale_citation_blocks_until_refresh(self):
        inq = self.make_inquiry()
        old_id = self.make_draft(inq)
        self.fill_draft(old_id)
        self.svc.evidence.upsert("case:1001", "CASE", "随访更新：皮疹消退。")
        self.assert_finalize_blocked(old_id, "STALE_CITATION")
        # 生成带原因的新候选后即可定稿
        new_draft = self.svc.refresh_draft(draft_id=old_id, actor="pm")
        result = self.svc.finalize_and_send(
            draft_id=new_draft["draft_id"], actor="pm")
        self.assertTrue(result["outbound_no"].startswith("OUT-"))

    def test_question_coverage_blocks(self):
        inq = self.make_inquiry()
        draft_id = self.make_draft(inq)
        # 只回答 Q1 → Q2 未回答
        self.svc.submit_section(
            draft_id=draft_id, section_key="sec-q1", question_id="Q1",
            content="见 [[ev:case:1001]]，附件 [[att:A1]]。",
            author="alice", base_revision=0)
        self.svc.put_attachment(
            draft_id=draft_id, attachment_id="A1", name="清单",
            content="case:1001", author="alice", base_revision=0)
        self.assert_finalize_blocked(draft_id, "QUESTION_UNANSWERED")
        # Q2 被两个章节回答 → 无法唯一追踪
        self.svc.submit_section(
            draft_id=draft_id, section_key="sec-q2a", question_id="Q2",
            content="口径一 [[ev:stat:4001]]。", author="bob", base_revision=0)
        self.svc.submit_section(
            draft_id=draft_id, section_key="sec-q2b", question_id="Q2",
            content="口径二 [[ev:stat:4001]]。", author="carol", base_revision=0)
        self.assert_finalize_blocked(draft_id, "QUESTION_AMBIGUOUS")
        # 空回答
        self.svc.submit_section(
            draft_id=draft_id, section_key="sec-q2b", question_id="Q3",
            content="  ", author="carol", base_revision=1)
        self.assert_finalize_blocked(draft_id, "UNKNOWN_QUESTION")

    def test_attachment_consistency_blocks(self):
        inq = self.make_inquiry()
        draft_id = self.make_draft(inq)
        self.svc.submit_section(
            draft_id=draft_id, section_key="sec-q1", question_id="Q1",
            content="见 [[ev:case:1001]] 与附件 [[att:A1]]。",
            author="alice", base_revision=0)
        self.svc.submit_section(
            draft_id=draft_id, section_key="sec-q2", question_id="Q2",
            content="见 [[ev:stat:4001]]。", author="bob", base_revision=0)
        # 正文引用了不存在的附件
        self.assert_finalize_blocked(draft_id, "ATTACHMENT_MISSING")
        # 补上 A1 但又多了未被引用的 A2
        self.svc.put_attachment(
            draft_id=draft_id, attachment_id="A1", name="清单",
            content="case:1001", author="alice", base_revision=0)
        self.svc.put_attachment(
            draft_id=draft_id, attachment_id="A2", name="多余",
            content="多余附件", author="alice", base_revision=0)
        self.assert_finalize_blocked(draft_id, "ATTACHMENT_UNREFERENCED")

    def test_sensitive_field_blocks_per_jurisdiction(self):
        inq = self.make_inquiry(jurisdiction="CN")
        draft_id = self.make_draft(inq)
        self.svc.submit_section(
            draft_id=draft_id, section_key="sec-q1", question_id="Q1",
            content="患者身份证 11010119900307777X，见 [[ev:case:1001]] "
                    "与附件 [[att:A1]]。",
            author="alice", base_revision=0)
        self.svc.submit_section(
            draft_id=draft_id, section_key="sec-q2", question_id="Q2",
            content="见 [[ev:stat:4001]]。", author="bob", base_revision=0)
        self.svc.put_attachment(
            draft_id=draft_id, attachment_id="A1", name="清单",
            content="case:1001", author="alice", base_revision=0)
        report = self.assert_finalize_blocked(draft_id, "SENSITIVE_FIELD")
        hit = [i for i in report["issues"]
               if i["code"] == "SENSITIVE_FIELD"][0]
        self.assertEqual(hit["detail"]["field"], "national_id")
        self.assertNotIn("11010119900307777X", str(report))  # 命中值脱敏
        # 脱敏后放行
        self.svc.submit_section(
            draft_id=draft_id, section_key="sec-q1", question_id="Q1",
            content="患者身份证 110***********7X，见 [[ev:case:1001]] "
                    "与附件 [[att:A1]]。",
            author="alice", base_revision=1)
        self.assertTrue(self.svc.validate_draft(draft_id)["ok"])

    def test_sensitive_policy_eu_email(self):
        inq = self.make_inquiry(jurisdiction="EU")
        draft_id = self.make_draft(inq)
        self.svc.submit_section(
            draft_id=draft_id, section_key="sec-q1", question_id="Q1",
            content="联系人 zhang.san@example.com，见 [[ev:case:1001]] "
                    "与 [[att:A1]]。",
            author="alice", base_revision=0)
        self.svc.submit_section(
            draft_id=draft_id, section_key="sec-q2", question_id="Q2",
            content="见 [[ev:stat:4001]]。", author="bob", base_revision=0)
        self.svc.put_attachment(
            draft_id=draft_id, attachment_id="A1", name="清单",
            content="case:1001", author="alice", base_revision=0)
        self.assert_finalize_blocked(draft_id, "SENSITIVE_FIELD")

    def test_deadline_passed_is_warning_not_blocker(self):
        inq = self.make_inquiry(deadline="2026-09-01")
        draft_id = self.make_draft(inq)
        self.fill_draft(draft_id)
        report = self.svc.validate_draft(draft_id)
        self.assertTrue(report["ok"])
        self.assertEqual([w["code"] for w in report["warnings"]],
                         ["DEADLINE_PASSED"])


class TestSectionCollaboration(BaseInquiryTest):
    def test_optimistic_revision_control(self):
        inq = self.make_inquiry()
        draft_id = self.make_draft(inq)
        r = self.svc.submit_section(
            draft_id=draft_id, section_key="s1", question_id="Q1",
            content="v1", author="alice", base_revision=0)
        self.assertEqual(r["revision"], 1)
        # 过期提交不得覆盖新内容
        with self.assertRaises(DomainError) as cm:
            self.svc.submit_section(
                draft_id=draft_id, section_key="s1", content="stale",
                author="bob", base_revision=0)
        self.assertEqual(cm.exception.code, "STALE_REVISION")
        self.assertEqual(cm.exception.current_revision, 1)
        # 基于调用方看到的最新修订号提交成功
        r = self.svc.submit_section(
            draft_id=draft_id, section_key="s1", content="v2",
            author="bob", base_revision=1)
        self.assertEqual(r["revision"], 2)
        sec = self.svc.get_sections(draft_id)[0]
        self.assertEqual((sec["content"], sec["author"]), ("v2", "bob"))

    def test_concurrent_same_section_single_winner(self):
        inq = self.make_inquiry()
        draft_id = self.make_draft(inq)
        barrier = threading.Barrier(2)
        outcomes = []

        def submit(author):
            barrier.wait()
            try:
                self.svc.submit_section(
                    draft_id=draft_id, section_key="s1", content=f"by {author}",
                    author=author, base_revision=0)
                outcomes.append(("ok", author))
            except DomainError as e:
                outcomes.append((e.code, author))

        threads = [threading.Thread(target=submit, args=(a,))
                   for a in ("alice", "bob")]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(sorted(o[0] for o in outcomes),
                         ["STALE_REVISION", "ok"])
        # 赢家的内容未被覆盖；败者基于新修订号重试成功
        sec = self.svc.get_sections(draft_id)[0]
        self.assertEqual(sec["revision"], 1)
        loser = [a for code, a in outcomes if code == "STALE_REVISION"][0]
        self.svc.submit_section(
            draft_id=draft_id, section_key="s1", content=f"retry {loser}",
            author=loser, base_revision=1)
        self.assertEqual(self.svc.get_sections(draft_id)[0]["content"],
                         f"retry {loser}")

    def test_concurrent_different_sections_both_succeed(self):
        inq = self.make_inquiry()
        draft_id = self.make_draft(inq)
        barrier = threading.Barrier(2)
        errors = []

        def submit(key, author):
            barrier.wait()
            try:
                self.svc.submit_section(
                    draft_id=draft_id, section_key=key, content=key,
                    author=author, base_revision=0)
            except DomainError as e:  # pragma: no cover
                errors.append(e)

        threads = [threading.Thread(target=submit, args=("s1", "alice")),
                   threading.Thread(target=submit, args=("s2", "bob"))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(self.svc.get_sections(draft_id)), 2)

    def test_attachment_revision_control(self):
        inq = self.make_inquiry()
        draft_id = self.make_draft(inq)
        self.svc.put_attachment(draft_id=draft_id, attachment_id="A1",
                                name="n1", content="c1", author="alice",
                                base_revision=0)
        with self.assertRaises(DomainError):
            self.svc.put_attachment(draft_id=draft_id, attachment_id="A1",
                                    name="n2", content="c2", author="bob",
                                    base_revision=0)
        r = self.svc.put_attachment(draft_id=draft_id, attachment_id="A1",
                                    name="n2", content="c2", author="bob",
                                    base_revision=1)
        self.assertEqual(r["revision"], 2)

    def test_finalized_draft_rejects_edits(self):
        _, draft_id, _ = self.make_finalized()
        with self.assertRaises(DomainError) as cm:
            self.svc.submit_section(draft_id=draft_id, section_key="sec-q1",
                                    content="改动", author="alice",
                                    base_revision=1)
        self.assertEqual(cm.exception.code, "DRAFT_NOT_OPEN")


class TestFinalizeSendRecovery(BaseInquiryTest):
    def test_finalize_success(self):
        inq, draft_id, result = self.make_finalized()
        self.assertEqual(result["outbound_no"], "OUT-000001")
        sub = self.svc.get_submission(result["submission_id"])
        self.assertEqual(sub["status"], "SENT")
        self.assertEqual(sub["payload"]["answers"], {"Q1": "sec-q1",
                                                     "Q2": "sec-q2"})
        self.assertEqual(self.svc.get_draft(draft_id)["status"], "FINALIZED")
        self.assertEqual(self.svc.get_inquiry(inq)["status"], "ANSWERED")
        self.assertEqual(self.count("submissions"), 1)
        self.assertEqual(self.count("outbox"), 1)
        job = self.svc.get_job(self._only_job_id())
        self.assertEqual(job["status"], "DONE")
        self.assertEqual(job["checkpoint"], 3)

    def _only_job_id(self):
        conn = dbmod.connect(self.db_path)
        try:
            return conn.execute("SELECT job_id FROM jobs").fetchone()["job_id"]
        finally:
            conn.close()

    def test_duplicate_requests_single_submission(self):
        inq = self.make_inquiry()
        draft_id = self.make_draft(inq)
        self.fill_draft(draft_id)
        r1 = self.svc.finalize_and_send(draft_id=draft_id, actor="pm")
        r2 = self.svc.finalize_and_send(draft_id=draft_id, actor="pm")
        self.assertEqual(r1, r2)
        self.assertEqual(self.count("submissions"), 1)
        self.assertEqual(self.count("outbox"), 1)
        self.assertEqual(self.count("jobs"), 1)
        # 履历不重复：定稿/发送各一条
        actions = [a["action"] for a in
                   self.svc.audit_trail("submission", r1["submission_id"])]
        self.assertEqual(actions.count("SUBMISSION_FINALIZED"), 1)
        self.assertEqual(actions.count("SUBMISSION_SENT"), 1)

    def test_two_workers_same_reply_key(self):
        inq = self.make_inquiry()
        draft_id = self.make_draft(inq)
        self.fill_draft(draft_id)
        other = self.make_service(worker_id="w2")  # 第二个工作节点（独立锁）
        barrier = threading.Barrier(2)
        results, errors = [], []

        def run(svc):
            barrier.wait()
            try:
                results.append(svc.finalize_and_send(draft_id=draft_id,
                                                     actor="pm"))
            except DomainError as e:  # pragma: no cover
                errors.append(e)

        threads = [threading.Thread(target=run, args=(self.svc,)),
                   threading.Thread(target=run, args=(other,))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0], results[1])
        self.assertEqual(self.count("submissions"), 1)
        self.assertEqual(self.count("outbox"), 1)
        self.assertEqual(self.count("jobs"), 1)

    def test_send_failure_resume_no_duplicates(self):
        flaky = FlakyTransport(self.svc.transport, fail_times=1)
        self.svc.transport = flaky
        inq = self.make_inquiry()
        draft_id = self.make_draft(inq)
        self.fill_draft(draft_id)
        with self.assertRaises(DomainError) as cm:
            self.svc.finalize_and_send(draft_id=draft_id, actor="pm")
        self.assertEqual(cm.exception.code, "TRANSPORT_ERROR")
        # finalize 已提交、send 失败：提交件在但未外发
        self.assertEqual(self.count("submissions"), 1)
        self.assertEqual(self.count("outbox"), 0)
        job_id = self._only_job_id()
        job = self.svc.get_job(job_id)
        self.assertEqual((job["status"], job["checkpoint"]), ("FAILED", 2))
        # 恢复后续跑：不产生重复提交件/外发编号
        job = self.svc.retry_job(job_id)
        self.assertEqual(job["status"], "DONE")
        self.assertGreaterEqual(job["attempts"], 2)
        self.assertEqual(self.count("submissions"), 1)
        self.assertEqual(self.count("outbox"), 1)
        sub = self.svc.list_submissions(inq)[0]
        self.assertEqual((sub["status"], sub["outbound_no"]),
                         ("SENT", "OUT-000001"))

    def test_finalize_step_replay_reuses_submission(self):
        flaky = FlakyTransport(self.svc.transport, fail_times=1)
        self.svc.transport = flaky
        inq = self.make_inquiry()
        draft_id = self.make_draft(inq)
        self.fill_draft(draft_id)
        with self.assertRaises(DomainError):
            self.svc.finalize_and_send(draft_id=draft_id, actor="pm")
        job_id = self._only_job_id()
        # 模拟崩溃丢失检查点：步骤履历与检查点回退，但 finalize 效果已提交
        conn = dbmod.connect(self.db_path)
        try:
            conn.execute("DROP TRIGGER trg_job_steps_no_delete")
            conn.execute("DELETE FROM job_steps")
            conn.execute("UPDATE jobs SET checkpoint=0, status='FAILED'")
        finally:
            conn.close()
        # 重放全部步骤：validate 幂等跳过、finalize 复用原提交件与外发编号
        job = self.svc.retry_job(job_id)
        self.assertEqual(job["status"], "DONE")
        self.assertEqual(self.count("submissions"), 1)
        self.assertEqual(self.count("outbox"), 1)
        self.assertEqual(self._all_outbound(), ["OUT-000001"])

    def test_run_pending_jobs_recovers_failed(self):
        flaky = FlakyTransport(self.svc.transport, fail_times=2)
        self.svc.transport = flaky
        drafts = []
        for _ in range(2):
            inq = self.make_inquiry()
            d = self.make_draft(inq)
            self.fill_draft(d)
            drafts.append(d)
        for d in drafts:
            with self.assertRaises(DomainError):
                self.svc.finalize_and_send(draft_id=d, actor="pm")
        done = self.svc.run_pending_jobs()
        self.assertEqual(len(done), 2)
        self.assertTrue(all(j["status"] == "DONE" for j in done))
        self.assertEqual(self.count("submissions"), 2)
        self.assertEqual(self.count("outbox"), 2)
        self.assertEqual(set(self._all_outbound()),
                         {"OUT-000001", "OUT-000002"})

    def _all_outbound(self):
        conn = dbmod.connect(self.db_path)
        try:
            return [r["outbound_no"] for r in
                    conn.execute("SELECT outbound_no FROM submissions")]
        finally:
            conn.close()


class TestReceiptCorrectionAudit(BaseInquiryTest):
    def test_receipt_idempotent(self):
        _, _, result = self.make_finalized()
        sid = result["submission_id"]
        r1 = self.svc.register_receipt(submission_id=sid,
                                       regulator_ref="NMPA-ACK-1",
                                       payload={"ok": True}, actor="pm")
        self.assertFalse(r1["deduped"])
        self.assertEqual(self.svc.get_submission(sid)["status"], "ACKED")
        r2 = self.svc.register_receipt(submission_id=sid,
                                       regulator_ref="NMPA-ACK-1",
                                       payload={"ok": True}, actor="pm")
        self.assertTrue(r2["deduped"])
        self.assertEqual(r1["receipt_id"], r2["receipt_id"])
        self.assertEqual(self.count("receipts"), 1)

    def test_correction_flow_preserves_original(self):
        inq, _, result = self.make_finalized()
        sid = result["submission_id"]
        self.svc.register_receipt(submission_id=sid, regulator_ref="ACK-1",
                                  payload={}, actor="pm")
        original_payload = self.svc.get_submission(sid)["payload"]
        # 定稿后资料变化 + 业务勘误 → 更正件
        self.svc.evidence.upsert("stat:4001", "STATISTIC",
                                 "2026H1 共收到 AE 报告 131 例，严重 13 例。")
        corr = self.svc.create_correction(submission_id=sid,
                                          reason="统计口径勘误", actor="pm")
        self.assertEqual(corr["kind"], "CORRECTION")
        self.assertEqual(corr["corrects_submission_id"], sid)
        types = [r["type"] for r in corr["reasons"]]
        self.assertEqual(types, ["CORRECTION", "EVIDENCE_UPDATED"])
        # 更正底稿锁定当前版本并复制章节
        pins = {c["evidence_id"]: c for c in corr["citations"]}
        self.assertEqual(pins["stat:4001"]["version"], 2)
        self.assertEqual(len(self.svc.get_sections(corr["draft_id"])), 2)
        # 编辑更正底稿并定稿
        self.svc.submit_section(
            draft_id=corr["draft_id"], section_key="sec-q2", question_id="Q2",
            content="统计口径见 [[ev:stat:4001]]（已勘误为 131 例），"
                    "法规依据见 [[ev:reg:3001]]。",
            author="bob", base_revision=1)
        corr_result = self.svc.finalize_and_send(draft_id=corr["draft_id"],
                                                 actor="pm")
        self.assertEqual(corr_result["outbound_no"], "OUT-000002")
        # 原提交件保留且 payload 未变；状态标记 CORRECTED；回执仍在
        orig = self.svc.get_submission(sid)
        self.assertEqual(orig["status"], "CORRECTED")
        self.assertEqual(orig["payload"], original_payload)
        chain = self.svc.correction_chain(sid)
        self.assertEqual(len(chain), 2)
        self.assertEqual(chain[0]["submission_id"], sid)
        self.assertEqual(len(chain[0]["receipts"]), 1)
        self.assertEqual(chain[1]["corrects_submission_id"], sid)
        # 更正链可从更正件反向查询
        self.assertEqual(self.svc.correction_chain(
            corr_result["submission_id"])[0]["submission_id"], sid)

    def test_submission_immutable_at_db_level(self):
        _, _, result = self.make_finalized()
        sid = result["submission_id"]
        self.svc.register_receipt(submission_id=sid, regulator_ref="ACK-1",
                                  payload={}, actor="pm")
        conn = dbmod.connect(self.db_path)
        try:
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute("UPDATE submissions SET payload_json='{}' "
                             "WHERE submission_id=?", (sid,))
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute("UPDATE submissions SET outbound_no='X' "
                             "WHERE submission_id=?", (sid,))
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute("DELETE FROM submissions WHERE submission_id=?",
                             (sid,))
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute("DELETE FROM receipts")
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute("DELETE FROM audit_log")
        finally:
            conn.close()


class TestIndependentCheck(BaseInquiryTest):
    def test_independent_check_passes(self):
        _, _, result = self.make_finalized()
        self.svc.register_receipt(submission_id=result["submission_id"],
                                  regulator_ref="ACK-1", payload={}, actor="pm")
        report = self.svc.independent_check(result["submission_id"])
        self.assertTrue(report["ok"], report)
        names = {c["check"] for c in report["checks"]}
        self.assertIn("payload_integrity", names)
        self.assertIn("outbound_number_unique", names)
        self.assertIn("questions_uniquely_answered", names)
        self.assertIn("attachments_consistent", names)
        self.assertIn("citations_verifiable", names)
        self.assertIn("disclosure_policy_compliant", names)
        self.assertIn("finalize_job_completed", names)
        self.assertIn("audit_trail_present", names)

    def test_independent_check_detects_tamper(self):
        _, _, result = self.make_finalized()
        sid = result["submission_id"]
        conn = dbmod.connect(self.db_path)
        try:
            conn.execute("DROP TRIGGER trg_submissions_guard")
            conn.execute("UPDATE submissions SET payload_json=? "
                         "WHERE submission_id=?",
                         ('{"inquiry":null}', sid))
        finally:
            conn.close()
        report = self.svc.independent_check(sid)
        self.assertFalse(report["ok"])
        integrity = [c for c in report["checks"]
                     if c["check"] == "payload_integrity"][0]
        self.assertFalse(integrity["ok"])

    def test_independent_check_correction_chain(self):
        _, _, result = self.make_finalized()
        corr = self.svc.create_correction(
            submission_id=result["submission_id"], reason="勘误", actor="pm")
        corr_result = self.svc.finalize_and_send(draft_id=corr["draft_id"],
                                                 actor="pm")
        report = self.svc.independent_check(corr_result["submission_id"])
        self.assertTrue(report["ok"], report)
        retained = [c for c in report["checks"]
                    if c["check"] == "corrected_original_retained"][0]
        self.assertTrue(retained["ok"])


class TestAuditTrail(BaseInquiryTest):
    def test_full_lifecycle_trail(self):
        inq, draft_id, result = self.make_finalized()
        actions = [a["action"] for a in self.svc.audit_trail()]
        self.assertEqual(actions[0], "INQUIRY_REGISTERED")
        for expected in ("DRAFT_LOCKED", "SECTION_SUBMITTED", "ATTACHMENT_PUT",
                         "JOB_CREATED", "JOB_CLAIMED", "SUBMISSION_FINALIZED",
                         "SUBMISSION_SENT", "JOB_DONE"):
            self.assertIn(expected, actions)
        # 按实体过滤
        sub_trail = self.svc.audit_trail("submission", result["submission_id"])
        self.assertEqual([a["action"] for a in sub_trail],
                         ["SUBMISSION_FINALIZED", "SUBMISSION_SENT"])
        draft_trail = self.svc.audit_trail("draft", draft_id)
        self.assertEqual(draft_trail[0]["action"], "DRAFT_LOCKED")
        # 留痕只增（数据库兜底）
        conn = dbmod.connect(self.db_path)
        try:
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute("UPDATE audit_log SET action='X'")
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
