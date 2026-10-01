"""复核员身份规范化：双人会签必须稳定判定“同一复核员”。

首尾空白、大小写、Unicode 等价（NFKC）写法差异不得把一个人识别成多个
复核员；空白/非法身份必须明确拒绝。覆盖签署、租约（领取/释放）、撤回、
待办可见性、审计查询、原始提交值留痕与并发重复提交。
"""
import threading

from tests.test_countersign import CountersignBase
from pv_service.service import DomainError


def _run_barrier(fn, n, *, timeout=15):
    barrier = threading.Barrier(n)
    results, errors = [], []

    def worker(i):
        try:
            barrier.wait(timeout=timeout)
            results.append(fn(i))
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=timeout)
    return results, errors


class TestReviewerIdentitySignatures(CountersignBase):
    def setUp(self):
        self._make_app(countersign_required=True)
        self.r = self.serious_unlisted_intake()
        self.rid = self.r["report"]["id"]
        self.sid = self.app.reviews.create_sheet([self.rid])["sheet_id"]

    def _assert_still_blocked(self):
        sh = self.app.reviews.get_sheet(self.sid)
        self.assertEqual("OPEN", sh["status"])
        self.assertEqual(1, sh["approval_count"])
        self.assertEqual(1, len(sh["approver_keys"]))
        self.assertFalse(self.app.reviews.is_approved_for_dispatch(self.rid))
        with self.assertRaises(DomainError):
            self.app.reporting.send_one(self.rid)

    def test_whitespace_variants_are_same_reviewer(self):
        self.app.reviews.approve(self.sid, "  alice  ")
        for v in ("alice", " ALICE ", "\talice\n", "  Alice"):
            with self.assertRaises(DomainError) as cm:
                self.app.reviews.approve(self.sid, v)
            self.assertEqual("ALREADY_SIGNED", cm.exception.code)
        self._assert_still_blocked()

    def test_case_variants_are_same_reviewer(self):
        self.app.reviews.approve(self.sid, "Bob")
        for v in ("bob", "BOB", "BoB"):
            with self.assertRaises(DomainError) as cm:
                self.app.reviews.approve(self.sid, v)
            self.assertEqual("ALREADY_SIGNED", cm.exception.code)
        self._assert_still_blocked()

    def test_unicode_equivalent_forms_are_same_reviewer(self):
        # 全角拉丁字母 NFKC 折叠为 ASCII
        self.app.reviews.approve(self.sid, "revＡ")
        with self.assertRaises(DomainError) as cm:
            self.app.reviews.approve(self.sid, "revA")
        self.assertEqual("ALREADY_SIGNED", cm.exception.code)
        # Å U+212B（埃格斯特朗符）与 Å U+00C5 NFKC 等价
        self.app.reviews.withdraw(self.sid, "reva")
        self.app.reviews.approve(self.sid, "Ålex")
        with self.assertRaises(DomainError) as cm:
            self.app.reviews.approve(self.sid, "Ålex")
        self.assertEqual("ALREADY_SIGNED", cm.exception.code)
        self._assert_still_blocked()

    def test_two_distinct_reviewers_complete_and_gate_opens(self):
        self.app.reviews.approve(self.sid, "  alice  ")
        sh = self.app.reviews.approve(self.sid, "bob")
        self.assertEqual("APPROVED", sh["status"])
        self.assertEqual(2, len(sh["approver_keys"]))
        self.assertTrue(self.app.reviews.is_approved_for_dispatch(self.rid))
        pump = self.app.reporting.pump()
        self.assertEqual(1, len(pump["acked"]))

    def test_inner_whitespace_is_significant(self):
        # 仅去首尾空白："a b" 与 "ab" 是两个不同身份
        self.app.reviews.approve(self.sid, "a b")
        sh = self.app.reviews.approve(self.sid, "ab")
        self.assertEqual("APPROVED", sh["status"])

    def test_veto_variant_blocks_same_identity_and_ends_round(self):
        # 先赞成，再用等价写法否决：同一槽位冲突
        self.app.reviews.approve(self.sid, "carol")
        with self.assertRaises(DomainError) as cm:
            self.app.reviews.veto(self.sid, " CAROL ")
        self.assertEqual("ALREADY_SIGNED", cm.exception.code)
        # 另一人否决立即结束
        out = self.app.reviews.veto(self.sid, "dave", reason="x")
        self.assertEqual("REJECTED", out["status"])
        # 同一人等价写法不能重复否决
        with self.assertRaises(DomainError):
            self.app.reviews.veto(self.sid, "DAVE")

    def test_withdraw_works_with_any_equivalent_form(self):
        self.app.reviews.approve(self.sid, "  Eve  ")
        # 用等价写法撤回本人赞成
        self.app.reviews.withdraw(self.sid, "EVE")
        sh = self.app.reviews.get_sheet(self.sid)
        self.assertEqual(0, sh["approval_count"])
        # 撤回后同一身份可重新签署
        self.app.reviews.approve(self.sid, "eve")
        sh = self.app.reviews.approve(self.sid, "frank")
        self.assertEqual("APPROVED", sh["status"])

    def test_withdraw_other_identity_not_found(self):
        self.app.reviews.approve(self.sid, "gina")
        with self.assertRaises(DomainError) as cm:
            self.app.reviews.withdraw(self.sid, "henry")
        self.assertEqual("NOT_SIGNED", cm.exception.code)

    def test_blank_or_invalid_reviewer_rejected_on_all_writes(self):
        for bad in ("", "   ", "\t \n"):
            for op in ("approve", "veto", "claim", "release", "withdraw"):
                with self.assertRaises(DomainError) as cm:
                    getattr(self.app.reviews, op)(self.sid, bad)
                self.assertEqual("BLANK_REVIEWER", cm.exception.code,
                                 f"{op}({bad!r})")
                self.assertEqual(400, cm.exception.status)
        for bad in (None, 123, ["x"], {"x": 1}):
            with self.assertRaises(DomainError) as cm:
                self.app.reviews.approve(self.sid, bad)
            self.assertEqual("INVALID_REVIEWER", cm.exception.code)

    def test_blank_reviewer_rejected_on_todos_and_audit(self):
        for bad in ("", "   "):
            with self.assertRaises(DomainError) as cm:
                self.app.reviews.list_todos(reviewer=bad)
            self.assertEqual("BLANK_REVIEWER", cm.exception.code)
            with self.assertRaises(DomainError) as cm:
                self.app.reviews.audit_trail(actor=bad)
            self.assertEqual("BLANK_REVIEWER", cm.exception.code)
        with self.assertRaises(DomainError):
            self.app.reviews.list_todos(reviewer=7)

    def test_raw_submitted_value_preserved_for_traceability(self):
        self.app.reviews.approve(self.sid, "  Alice  ", comment="first")
        self.app.reviews.approve(self.sid, "BOB")
        sh = self.app.reviews.get_sheet(self.sid)
        by_key = {sig["reviewer_key"]: sig for sig in sh["signatures"]}
        # 规范化键用于身份判定，原始字符串逐字保留
        self.assertEqual("  Alice  ", by_key["alice"]["reviewer"])
        self.assertEqual("BOB", by_key["bob"]["reviewer"])
        # 事件流水 actor 保留原始值，并带 actor_key
        approved = [e for e in sh["events"] if e["event_type"] == "APPROVED"]
        actors = {e["actor"]: e["actor_key"] for e in approved}
        self.assertEqual("alice", actors["  Alice  "])
        self.assertEqual("bob", actors["BOB"])
        # 放行留档 approved_by_json 保留原始签署值
        gate = self.app.reviews.dispatch_approvals(self.rid)
        self.assertEqual({"  Alice  ", "BOB"}, set(gate[0]["approved_by"]))


class TestReviewerIdentityLeases(CountersignBase):
    def setUp(self):
        self._make_app(countersign_required=True, review_lease_ttl_seconds=3600)
        self.r = self.serious_unlisted_intake()
        self.sid = self.app.reviews.create_sheet([self.r["report"]["id"]])["sheet_id"]

    def test_claim_equivalent_form_is_self_renew_not_conflict(self):
        self.app.reviews.claim(self.sid, "  alice  ")
        # 等价写法再次领取 = 本人续租，幂等成功（不视为抢领冲突）
        out = self.app.reviews.claim(self.sid, "ALICE")
        self.assertEqual("alice", out["lease_holder_key"])
        self.assertEqual("  alice  ", out["lease_holder"])

    def test_equivalent_holder_blocks_other_and_controls_visibility(self):
        self.app.reviews.claim(self.sid, "  Alice  ")
        # 另一人（即便大小写不同也不是 alice）抢领失败
        with self.assertRaises(DomainError) as cm:
            self.app.reviews.claim(self.sid, "bob")
        self.assertEqual("TODO_ALREADY_CLAIMED", cm.exception.code)
        # alice 任意等价写法都能在待办里看到且 claimed_by_me
        todos = self.app.reviews.list_todos(reviewer="alice")
        self.assertEqual(1, len(todos))
        self.assertTrue(todos[0]["claimed_by_me"])
        # bob 看不到
        self.assertEqual([], self.app.reviews.list_todos(reviewer=" BOB "))

    def test_release_with_equivalent_form_succeeds(self):
        self.app.reviews.claim(self.sid, "  alice  ")
        out = self.app.reviews.release(self.sid, "ALICE")
        self.assertIsNone(out["lease_holder"])
        self.assertIsNone(out["lease_holder_key"])
        # 非持有者不能释放
        self.app.reviews.claim(self.sid, "alice")
        with self.assertRaises(DomainError) as cm:
            self.app.reviews.release(self.sid, "bob")
        self.assertEqual("NOT_LEASE_HOLDER", cm.exception.code)

    def test_lease_expiry_audit_tied_to_holder_key(self):
        self.app.reviews.claim(self.sid, "  Alice  ")
        self.clock.advance(seconds=3601)
        self.assertEqual(1, self.app.reviews.expire_leases())
        # 到期留痕可按规范化身份查到
        trail = self.app.reviews.audit_trail(sheet_id=self.sid,
                                             event_type="LEASE_EXPIRED",
                                             actor="alice")
        self.assertEqual(1, len(trail))
        self.assertEqual("  Alice  ", trail[0]["actor"])

    def test_invalidation_clears_holder_key(self):
        # 修订作废旧单必须同时清空 lease_holder 与 lease_holder_key，
        # 否则会残留一个指向旧持有者的身份（待办/恢复判定矛盾）。
        self.app.reviews.claim(self.sid, "  alice  ")
        self.app.cases.add_followup(self.r["case_id"], {"outcome": "死亡"})
        sh = self.app.reviews.get_sheet(self.sid)
        self.assertEqual("INVALIDATED", sh["status"])
        self.assertIsNone(sh["lease_holder"])
        self.assertIsNone(sh["lease_holder_key"])
        self.assertEqual([], self.app.reviews.list_todos(reviewer="alice"))


class TestReviewerIdentityAudit(CountersignBase):
    def setUp(self):
        self._make_app(countersign_required=True)

    def test_audit_query_matches_all_equivalent_forms(self):
        r = self.serious_unlisted_intake()
        sid = self.app.reviews.create_sheet([r["report"]["id"]])["sheet_id"]
        self.app.reviews.claim(sid, "  Alice  ")
        self.app.reviews.approve(sid, "  alice  ")
        self.app.reviews.release(sid, "ALICE")
        # 用任一等价写法查询都能命中同一复核员的全部动作
        for q in ("alice", "ALICE", " alice "):
            trail = self.app.reviews.audit_trail(actor=q)
            types = {e["event_type"] for e in trail}
            self.assertEqual({"CLAIMED", "APPROVED", "RELEASED"}, types,
                             f"query={q!r}")
        # 不同身份查不到 alice 的动作
        self.assertEqual([], self.app.reviews.audit_trail(actor="bob"))


class TestReviewerIdentityConcurrency(CountersignBase):
    def setUp(self):
        self._make_app(countersign_required=True)
        self.r = self.serious_unlisted_intake()
        self.rid = self.r["report"]["id"]
        self.sid = self.app.reviews.create_sheet([self.rid])["sheet_id"]

    def test_concurrent_equivalent_variants_one_slot(self):
        # 同一复核员用不同表面形式并发提交：只能占一个槽
        variants = ["alice", "ALICE", " alice ", "Alice", "Ａlice", "ALICE "]

        def approve(i):
            return self.app.reviews.approve(self.sid, variants[i])

        results, errors = _run_barrier(approve, len(variants))
        self.assertEqual(1, len(results))
        self.assertEqual(len(variants) - 1, len(errors))
        self.assertTrue(all(e.code == "ALREADY_SIGNED" for e in errors))
        sh = self.app.reviews.get_sheet(self.sid)
        self.assertEqual(1, sh["approval_count"])
        self.assertEqual("OPEN", sh["status"])
        self.assertFalse(self.app.reviews.is_approved_for_dispatch(self.rid))

    def test_concurrent_claim_equivalent_same_person_single_holder(self):
        variants = ["alice", "ALICE", " alice "]

        def claim(i):
            return self.app.reviews.claim(self.sid, variants[i])

        results, errors = _run_barrier(claim, len(variants))
        # 同一人多次领取要么成功（含续租幂等），不会把租约错误转移给“别人”；
        # 最终持有者始终是 alice 这一身份。
        self.assertEqual([], errors)
        self.assertEqual(len(variants), len(results))
        sh = self.app.reviews.get_sheet(self.sid)
        self.assertEqual("alice", sh["lease_holder_key"])

    def test_concurrent_first_approver_variants_then_second_person_once(self):
        # 多个 alice 变体并发抢第一槽 + 一个 bob：恰好两个不同身份完成一次
        inputs = ["alice", "ALICE", " alice ", "bob"]

        def approve(i):
            return self.app.reviews.approve(self.sid, inputs[i])

        results, errors = _run_barrier(approve, len(inputs))
        sh = self.app.reviews.get_sheet(self.sid)
        # alice 们合计只占 1 槽，bob 占 1 槽 -> APPROVED，且只放行一次
        self.assertEqual("APPROVED", sh["status"])
        self.assertEqual(2, sh["approval_count"])
        self.assertEqual({"alice", "bob"}, set(sh["approver_keys"]))
        completed = [e for e in sh["events"]
                     if e["event_type"] == "APPROVED_COMPLETED"]
        self.assertEqual(1, len(completed))
        self.assertEqual(1,
                         len(self.app.reviews.dispatch_approvals(self.rid)))
