"""双人会签的并发/租约/恢复不变量。

所有竞争场景都必须收敛到 *一个* 生效结论：
- 多人抢领同一待办只有一人成功；
- 同一身份并发重复签署只占一个意见槽，不累计票数；
- 批准与撤回同时发生只产生一个终态；
- 多人并发签署正好两名不同人时恰好 APPROVED 一次，闸门只解锁一次；
- 租约到期释放；服务恢复不产生结论。
"""
import threading

from tests.test_countersign import CountersignBase


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


class TestCountersignConcurrency(CountersignBase):
    def setUp(self):
        self._make_app(countersign_required=True)

    def test_concurrent_claim_only_one_holder(self):
        r = self.serious_unlisted_intake()
        s = self.app.reviews.create_sheet([r["report"]["id"]])
        n = 8

        def claim(i):
            return self.app.reviews.claim(s["sheet_id"], f"rev{i}")

        results, errors = _run_barrier(claim, n)
        self.assertEqual(1, len(results))
        self.assertEqual(n - 1, len(errors))
        codes = {e.code for e in errors}
        self.assertEqual({"TODO_ALREADY_CLAIMED"}, codes)
        sh = self.app.reviews.get_sheet(s["sheet_id"])
        self.assertIsNotNone(sh["lease_holder"])

    def test_concurrent_duplicate_signature_same_identity_no_accumulation(self):
        r = self.serious_unlisted_intake()
        s = self.app.reviews.create_sheet([r["report"]["id"]])
        n = 6

        def approve(i):
            return self.app.reviews.approve(s["sheet_id"], "revA")

        results, errors = _run_barrier(approve, n)
        self.assertEqual(1, len(results), "同一身份只有一次签署落库")
        self.assertEqual(n - 1, len(errors))
        self.assertTrue(all(e.code == "ALREADY_SIGNED" for e in errors))
        sh = self.app.reviews.get_sheet(s["sheet_id"])
        self.assertEqual(1, sh["approval_count"])
        self.assertEqual("OPEN", sh["status"])

    def test_concurrent_distinct_reviewers_exactly_one_approval_conclusion(self):
        # revA 已签；revB 与 revC 同时签第二个槽位（不同身份）。
        # 无论谁赢，结论只能是一次 APPROVED，且只放行一次。
        r = self.serious_unlisted_intake()
        rid = r["report"]["id"]
        s = self.app.reviews.create_sheet([rid])
        self.app.reviews.approve(s["sheet_id"], "revA")
        n = 8

        def approve(i):
            return self.app.reviews.approve(s["sheet_id"], f"rev{i}")

        results, errors = _run_barrier(approve, n)
        # rev0..rev7 中恰有一人成为第二签
        self.assertEqual(1, len(results))
        self.assertEqual(n - 1, len(errors))
        self.assertTrue(all(e.code in ("ALREADY_SIGNED", "SHEET_NOT_OPEN")
                            for e in errors))
        sh = self.app.reviews.get_sheet(s["sheet_id"])
        self.assertEqual("APPROVED", sh["status"])
        self.assertEqual(2, sh["approval_count"])
        approvals = self.app.reviews.dispatch_approvals(rid)
        self.assertEqual(1, len(approvals))
        completed = [e for e in sh["events"] if e["event_type"] == "APPROVED_COMPLETED"]
        self.assertEqual(1, len(completed))

    def test_concurrent_approve_and_veto_single_terminal_state(self):
        r = self.serious_unlisted_intake()
        rid = r["report"]["id"]
        s = self.app.reviews.create_sheet([rid])
        outcomes = {}

        def veto_side(_):
            try:
                self.app.reviews.veto(s["sheet_id"], "revV")
                outcomes["veto"] = "ok"
            except Exception as e:  # noqa: BLE001
                outcomes["veto"] = e.code

        def approve_side(i):
            try:
                self.app.reviews.approve(s["sheet_id"], f"revA{i}")
                outcomes[f"appr{i}"] = "ok"
            except Exception as e:  # noqa: BLE001
                outcomes[f"appr{i}"] = e.code

        barrier = threading.Barrier(3)
        threads = [
            threading.Thread(target=lambda: (barrier.wait(), veto_side(0))),
            threading.Thread(target=lambda: (barrier.wait(), approve_side(1))),
            threading.Thread(target=lambda: (barrier.wait(), approve_side(2))),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        sh = self.app.reviews.get_sheet(s["sheet_id"])
        self.assertIn(sh["status"], ("APPROVED", "REJECTED"))
        self.assertNotEqual("OPEN", sh["status"])
        # 被否决则一定没有放行；放行则一定不是否决
        self.assertEqual("REJECTED" not in sh["status"] or
                         not sh["released_for_dispatch"], True)
        if sh["status"] == "REJECTED":
            self.assertEqual([], self.app.reviews.dispatch_approvals(rid))
            with self.assertRaises(Exception):
                self.app.reporting.send_one(rid)
        else:
            self.assertEqual(1, len(self.app.reviews.dispatch_approvals(rid)))

    def test_concurrent_create_same_content_single_open_sheet(self):
        r = self.serious_unlisted_intake()
        rid = r["report"]["id"]
        n = 6

        def create(i):
            return self.app.reviews.create_sheet([rid])

        results, errors = _run_barrier(create, n)
        self.assertEqual([], errors)
        ids = {x["sheet_id"] for x in results}
        self.assertEqual(1, len(ids), "并发重复发起只产生一张 OPEN 单")
        from pv_service import db as dbmod
        conn = dbmod.connect(self.app.db_path)
        try:
            cnt = conn.execute(
                "SELECT COUNT(*) FROM review_sheets WHERE status='OPEN'").fetchone()[0]
            self.assertEqual(1, cnt)
        finally:
            conn.close()

    def test_concurrent_revision_bump_vs_approval(self):
        # 随访（修订推进/作废旧单）与第二签署并发：旧单绝不能放行
        r = self.serious_unlisted_intake()
        rid = r["report"]["id"]
        s = self.app.reviews.create_sheet([rid])
        self.app.reviews.approve(s["sheet_id"], "revA")
        outcomes = {}

        barrier = threading.Barrier(2)

        def followup():
            barrier.wait()
            try:
                self.app.cases.add_followup(r["case_id"], {"outcome": "死亡"})
                outcomes["followup"] = "ok"
            except Exception as e:  # noqa: BLE001
                outcomes["followup"] = e.code

        def approve_b():
            barrier.wait()
            try:
                self.app.reviews.approve(s["sheet_id"], "revB")
                outcomes["approve"] = "ok"
            except Exception as e:  # noqa: BLE001
                outcomes["approve"] = e.code

        t1 = threading.Thread(target=followup)
        t2 = threading.Thread(target=approve_b)
        t1.start(); t2.start(); t1.join(); t2.join()
        sh = self.app.reviews.get_sheet(s["sheet_id"])
        # 要么作废（无闸门），要么批准但…批准只能在未 bump 时发生；
        # bump 与批准在同一写锁串行，二者不可能同时成立。
        self.assertIn(sh["status"], ("APPROVED", "INVALIDATED"))
        if sh["status"] == "APPROVED":
            # 批准先于随访：放行的是旧决定；随访产生的新决定仍需新会签
            self.assertEqual(1, len(self.app.reviews.dispatch_approvals(rid)))
        else:
            self.assertEqual([], self.app.reviews.dispatch_approvals(rid))
            with self.assertRaises(Exception):
                self.app.reporting.send_one(rid)


class TestCountersignLease(CountersignBase):
    def setUp(self):
        # 短 TTL 便于到期测试
        self._make_app(countersign_required=True, review_lease_ttl_seconds=3600)
        self.r = self.serious_unlisted_intake()
        self.sheet_id = self.app.reviews.create_sheet([self.r["report"]["id"]])["sheet_id"]

    def test_claim_renewal_release_and_todos_visibility(self):
        self.app.reviews.claim(self.sheet_id, "revA")
        # 他人待办看不到、本人可见
        self.assertEqual([], self.app.reviews.list_todos(reviewer="revB"))
        todos = self.app.reviews.list_todos(reviewer="revA")
        self.assertEqual(1, len(todos))
        self.assertTrue(todos[0]["claimed_by_me"])
        # 非持有者不能释放
        from pv_service.service import DomainError
        with self.assertRaises(DomainError):
            self.app.reviews.release(self.sheet_id, "revB")
        self.app.reviews.release(self.sheet_id, "revA")
        todos = self.app.reviews.list_todos(reviewer="revB")
        self.assertEqual(1, len(todos))
        self.assertTrue(todos[0]["claimable"])

    def test_lease_expiry_allows_reclaim(self):
        self.app.reviews.claim(self.sheet_id, "revA")
        # 时钟推进超过 TTL
        self.clock.advance(seconds=3601)
        self.assertEqual(1, self.app.reviews.expire_leases())
        todos = self.app.reviews.list_todos(reviewer="revB")
        self.assertEqual(1, len(todos))
        self.app.reviews.claim(self.sheet_id, "revB")
        sh = self.app.reviews.get_sheet(self.sheet_id)
        self.assertEqual("revB", sh["lease_holder"])
        trail = self.app.reviews.audit_trail(sheet_id=self.sheet_id,
                                             event_type="LEASE_EXPIRED")
        self.assertEqual(1, len(trail))

    def test_recover_leases_only_releases_expired_no_conclusion(self):
        self.app.reviews.claim(self.sheet_id, "revA")
        self.clock.advance(seconds=3601)
        n = self.app.reviews.recover_leases()   # 模拟服务重启恢复
        self.assertEqual(1, n)
        sh = self.app.reviews.get_sheet(self.sheet_id)
        self.assertEqual("OPEN", sh["status"], "恢复不产生任何业务结论")
        self.assertIsNone(sh["lease_holder"])

    def test_unexpired_lease_survives_recover(self):
        self.app.reviews.claim(self.sheet_id, "revA")
        self.clock.advance(seconds=10)
        n = self.app.reviews.recover_leases()
        self.assertEqual(0, n)
        self.assertEqual("revA",
                         self.app.reviews.get_sheet(self.sheet_id)["lease_holder"])


if __name__ == "__main__":
    import unittest
    unittest.main()
