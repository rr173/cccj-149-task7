"""规则治理并发：并发启用/回滚/定时只有一个结果；请求幂等。"""
import threading
import unittest

from tests.base import BasePVTest
from pv_service.rule_pkg import build_content
from pv_service.service import DomainError

SECRET = "pv-demo-signing-secret"


def make_content(version, frm, to=None, **days):
    return build_content(jurisdiction="CN", version=version,
                         effective_from=frm, effective_to=to,
                         serious_days=10, listed_days=20,
                         routine_days=days.get("routine_days", 45))


class RuleConcurrencyTest(BasePVTest):
    def setUp(self):
        super().setUp()
        self.gov = self.app.rules_gov
        for i in range(4):
            self.app.cases.intake(
                "HOSPITAL", f"H-{i}",
                {"patient_name": f"P{i}", "dob": "1980-01-01",
                 "drugs": [{"name": "布洛芬", "causality": "可能"}],
                 "reactions": ["皮疹"], "seriousness": "不严重",
                 "event_date": "2026-09-07"},
                "2026-09-10T09:00:00")

    def _reg(self, *contents):
        for c in contents:
            self.gov.register_package(c, self.gov.sign(c, SECRET))

    def test_concurrent_activate_same_idempotency_key_one_result(self):
        self._reg(make_content("2026.1", "2026-10-01", "2027-01-01"))
        outcomes, lock = [], threading.Lock()

        def worker():
            try:
                r = self.gov.activate("CN", "2026.1",
                                      idempotency_key="same-key")
                with lock:
                    outcomes.append(("ok", r["activation_id"]))
            except Exception as e:  # noqa: BLE001
                with lock:
                    outcomes.append(("err", getattr(e, "code", str(e))))

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        ids = {oid for kind, oid in outcomes if kind == "ok"}
        self.assertEqual(len(ids), 1)
        # 同一逻辑启用只有一个作业
        acts = [a for a in self.gov.list_activations("CN")
                if a["idempotency_key"] == "same-key"]
        self.assertEqual(len(acts), 1)
        self.assertEqual(self.gov.current_version("CN")["version"], "2026.1")

    def test_concurrent_activate_distinct_requests_serialize(self):
        self._reg(make_content("2026.1", "2026-10-01", "2027-01-01"),
                  make_content("2027.1", "2027-01-01", routine_days=40))
        errors, lock = [], threading.Lock()

        def activate(ver):
            try:
                self.gov.activate("CN", ver)
            except Exception as e:  # noqa: BLE001
                with lock:
                    errors.append(getattr(e, "code", str(e)))

        t1 = threading.Thread(target=activate, args=("2026.1",))
        t2 = threading.Thread(target=activate, args=("2027.1",))
        t1.start(); t2.start(); t1.join(); t2.join()
        # 无未捕获异常；无论先后，最终只可能有一个确定的当前版本
        self.assertEqual(set(errors) - {"ACTIVATION_IN_PROGRESS"}, set())
        self.assertIn(self.gov.current_version("CN")["version"],
                      {"2026.1", "2027.1"})
        self.assertTrue(all(j["status"] in ("COMPLETED", "FAILED")
                            for j in self.gov.list_jobs("CN")))

    def test_activate_and_rollback_race_one_consistent_state(self):
        self._reg(make_content("2026.1", "2026-10-01"))
        results, lock = [], threading.Lock()

        def act():
            try:
                self.gov.activate("CN", "2026.1")
                with lock:
                    results.append("activated")
            except Exception:  # noqa: BLE001
                with lock:
                    results.append("act-busy")

        def rb():
            try:
                self.gov.rollback()
                with lock:
                    results.append("rolledback")
            except Exception:  # noqa: BLE001
                with lock:
                    results.append("rb-busy")

        t1 = threading.Thread(target=act)
        t2 = threading.Thread(target=rb)
        t1.start(); t2.start(); t1.join(); t2.join()
        # 两个操作串行化，最终版本是某个确定值（不会损坏）
        cur = self.gov.current_version("CN")
        self.assertIn(cur["version"], {"0", "2026.1"})
        self.assertTrue(all(j["status"] in ("COMPLETED", "FAILED")
                            for j in self.gov.list_jobs("CN")))
        self.assertTrue(self.gov.verify_audit_chain()["valid"])


if __name__ == "__main__":
    unittest.main()
