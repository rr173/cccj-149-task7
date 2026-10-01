"""发送与重试、回执、崩溃恢复、双节点幂等。"""
import threading

from reg_inquiry import db as dbmod
from reg_inquiry.errors import JobFailed
from reg_inquiry.transport import RegulatorTransport

from .reg_base import InquiryTestBase


class TestSendAndReceipt(InquiryTestBase):
    def setUp(self):
        super().setUp()
        self.seed_materials()
        self.inq, self.draft = self.lock_base()
        self.sid, self.no = self.fill_and_seal(self.draft)

    def test_send_success_events_and_number(self):
        r = self.o.send(self.sid)
        self.assertEqual(r["status"], "SENT")
        self.assertIn("receipt_number", r["response"])
        sub = self.o.submission_status(self.sid)
        self.assertEqual(sub["attempts"], 1)
        types = [e["event_type"] for e in sub["events"]]
        self.assertIn("SENDING", types)
        self.assertIn("SENT", types)
        self.assertEqual(self.gw.accept_count(self.no), 1)

    def test_repeat_send_idempotent_single_accept(self):
        self.o.send(self.sid)
        r2 = self.o.send(self.sid)
        r3 = self.o.send(self.sid)
        self.assertTrue(r2.get("skipped"))
        self.assertTrue(r3.get("skipped"))
        self.assertEqual(self.gw.accept_count(self.no), 1)
        sub = self.o.submission_status(self.sid)
        self.assertEqual(sub["attempts"], 1)
        # 只有一个提交件、一个外发编号
        self.assertEqual(len(self.o.list_submissions(self.inq["id"])), 1)

    def test_register_receipt_delivers_and_cross_checks_number(self):
        r = self.o.send(self.sid)
        rec = self.o.register_receipt(
            self.sid, receipt_number=r["response"]["receipt_number"],
            payload={"outgoing_number": self.no, "regulator": "NMPA"})
        self.assertEqual(rec["outgoing_number"], self.no)
        self.assertEqual(self.o.submission_status(self.sid)["status"],
                         "DELIVERED")
        # 回执号幂等：重复登记返回同一行
        rec2 = self.o.register_receipt(
            self.sid, receipt_number=r["response"]["receipt_number"],
            payload={"outgoing_number": self.no})
        self.assertEqual(rec["id"], rec2["id"])
        # 回执外发编号错配 -> 拒绝
        with self.assertRaises(Exception):
            self.o.register_receipt(
                self.sid, receipt_number="ACK-OTHER",
                payload={"outgoing_number": "CN-2026-99999"})

    def test_transport_reject_permanent_no_retry(self):
        gw = RegulatorTransport(self.path("gw2.db"), clock=self.clock,
                                reject=True, reject_reason="BAD_FORMAT")
        node = self.new_node(gw=gw)
        with self.assertRaises(JobFailed) as cm:
            node.send(self.sid)
        self.assertEqual(cm.exception.job["error_code"], "TRANSPORT_REJECT")
        self.assertEqual(self.o.submission_status(self.sid)["status"],
                         "REJECTED")
        self.assertEqual(gw.total_attempts(self.no), 1)


class TestSendRetryAndRecovery(InquiryTestBase):
    fail_times = 2

    def setUp(self):
        super().setUp()
        self.seed_materials()
        self.inq, self.draft = self.lock_base()
        self.sid, self.no = self.fill_and_seal(self.draft)

    def test_temp_failures_retried_single_accept(self):
        r = self.o.send(self.sid)
        self.assertEqual(r["status"], "SENT")
        # 前两次临时失败、第三次成功：网关只受理一次，本地 attempts=3
        self.assertEqual(self.gw.total_attempts(self.no), 3)
        self.assertEqual(self.gw.accept_count(self.no), 1)
        self.assertEqual(self.o.submission_status(self.sid)["attempts"], 3)
        types = [e["event_type"] for e
                 in self.o.submission_status(self.sid)["events"]]
        self.assertEqual(types.count("SENDING"), 3)
        self.assertEqual(types.count("SENT"), 1)
        self.assertIn("FAILED", types)

    def test_recover_stuck_sending_then_idempotent_resend(self):
        r = self.o.send(self.sid)
        self.o.register_receipt(
            self.sid, receipt_number=r["response"]["receipt_number"],
            payload={"outgoing_number": self.no})
        # 模拟崩溃：卡在 SENDING
        with self.o.conn() as c, dbmod.transaction(c):
            c.execute("UPDATE submissions SET status='SENDING' WHERE id=?",
                      (self.sid,))
        # 新节点启动恢复（回执已在）：不再外发
        node = self.new_node(auto_recover=True)
        self.assertEqual(node.submission_status(self.sid)["status"], "QUEUED")
        rr = node.send(self.sid)
        self.assertTrue(rr.get("skipped"))
        self.assertEqual(self.gw.accept_count(self.no), 1)
        self.assertEqual(node.submission_status(self.sid)["status"],
                         "DELIVERED")

    def test_recover_without_receipt_replays_via_gateway_idempotency(self):
        # 网关已受理但本地回执/状态未落库时崩溃：重放取回同一受理
        self.o.send(self.sid)
        self.assertEqual(self.gw.accept_count(self.no), 1)
        with self.o.conn() as c, dbmod.transaction(c):
            c.execute("UPDATE submissions SET status='SENDING' WHERE id=?",
                      (self.sid,))
        node = self.new_node(auto_recover=True)
        rr = node.send(self.sid)
        self.assertEqual(rr["status"], "SENT")
        # 网关幂等：仍是同一受理，没有第二条
        self.assertEqual(self.gw.accept_count(self.no), 1)


class TestTwoNodeConcurrency(InquiryTestBase):
    def setUp(self):
        super().setUp()
        self.seed_materials()
        self.inq, self.draft = self.lock_base()
        self.sid, self.no = self.fill_and_seal(self.draft)

    def test_four_nodes_same_submission_single_outgoing(self):
        results, errors = [], []
        barrier = threading.Barrier(4)

        def send():
            barrier.wait()
            node = self.new_node()
            try:
                results.append(node.send(self.sid))
            except Exception as e:  # noqa: BLE001
                errors.append(type(e).__name__)

        ts = [threading.Thread(target=send) for _ in range(4)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(self.o.list_submissions(self.inq["id"])), 1)
        self.assertEqual(self.gw.accept_count(self.no), 1)
        sub = self.o.submission_status(self.sid)
        # 编号唯一、履历无重复 SENT
        sent = [e for e in sub["events"] if e["event_type"] == "SENT"]
        self.assertEqual(len(sent), 1)

    def test_concurrent_finalize_single_submission(self):
        # 另起一个尚未定稿的问询，双节点同时定稿
        inq = self.o.register_inquiry(
            regulator="R", jurisdiction="CN",
            questions=[{"qid": "Q1", "text": "a"}],
            deadline="2026-10-20T00:00:00+00:00",
            allowed_material_ids=["MAT-CASE"])
        d = self.o.lock_draft(inq["id"], ["CASE:MAT-CASE@1"])
        self.o.submit_section(d["id"], "Q1", "x [CASE:MAT-CASE@1]",
                              expected_revision=0)
        out = []
        barrier = threading.Barrier(2)

        def fin():
            barrier.wait()
            node = self.new_node()
            try:
                out.append(node.finalize(d["id"]))
            except Exception as e:  # noqa: BLE001
                out.append(("ERR", type(e).__name__))

        ts = [threading.Thread(target=fin) for _ in range(2)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        subs = self.o.list_submissions(inq["id"])
        self.assertEqual(len(subs), 1)
        self.assertEqual(len({s["outgoing_number"] for s in subs}), 1)


class TestJobsRecovery(InquiryTestBase):
    def test_stale_runner_lease_reclaimed_by_fence(self):
        self.seed_materials()
        inq = self.register()
        # 手工入队一个作业，让旧持有者租约过期
        from reg_inquiry import jobs
        with self.o.conn() as c, dbmod.transaction(c):
            job, _ = jobs.enqueue(
                c, job_type="VALIDATE", inquiry_id=inq["id"],
                idempotency_key="V:" + inq["id"],
                payload={"draft_id": "nonexistent"})
        # 节点 A 领取后“崩溃”（租约留在 RUNNING）
        with self.o.conn() as c:
            claimed = jobs.claim(c, lease_seconds=30,
                                 now=self.clock.now_iso(), owner="A")
            self.assertIsNotNone(claimed)
            self.assertIsNone(jobs.claim(c, lease_seconds=30,
                                         now=self.clock.now_iso(), owner="B"))
        # 时钟推进超过租约 -> B 接管（attempt 递增，旧 attempt 完成栅栏失效）
        self.clock.advance(seconds=60)
        with self.o.conn() as c:
            reclaimed = jobs.claim(c, lease_seconds=30,
                                   now=self.clock.now_iso(), owner="B")
        self.assertIsNotNone(reclaimed)
        self.assertEqual(reclaimed["attempt"], 2)
        # 旧持有者（attempt=1）试图完成 -> 被栅栏拒绝
        with self.o.conn() as c:
            ok = jobs.complete(c, claimed["id"], 1, {"stale": True},
                               now=self.clock.now_iso())
        self.assertFalse(ok)
