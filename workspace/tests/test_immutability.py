"""原始来源记录与历史版本只增不改（数据库触发器强制）。"""
import sqlite3

AppendOnlyError = sqlite3.IntegrityError

from tests.base import BasePVTest


class TestImmutability(BasePVTest):
    def test_source_record_cannot_be_updated_or_deleted(self):
        r = self.app.cases.intake("HOSPITAL", "H-IMM", self.zhang_patient())
        conn = self.db()
        with self.assertRaises((sqlite3.OperationalError, sqlite3.IntegrityError)):
            conn.execute("UPDATE source_records SET external_id='HACK' WHERE id=?",
                         (r["source_id"],))
        with self.assertRaises((sqlite3.OperationalError, sqlite3.IntegrityError)):
            conn.execute("DELETE FROM source_records WHERE id=?", (r["source_id"],))

    def test_case_versions_are_append_only(self):
        r = self.app.cases.intake("HOSPITAL", "H-IMM2", self.zhang_patient())
        self.app.cases.add_followup(r["case_id"], {"causality": "可能"})
        conn = self.db()
        versions = conn.execute(
            "SELECT version_no, snapshot_json FROM case_versions ORDER BY version_no"
        ).fetchall()
        self.assertEqual(2, len(versions))
        with self.assertRaises((sqlite3.OperationalError, sqlite3.IntegrityError)):
            conn.execute("UPDATE case_versions SET snapshot_json='{}' WHERE version_no=1")
        with self.assertRaises((sqlite3.OperationalError, sqlite3.IntegrityError)):
            conn.execute("DELETE FROM case_versions WHERE version_no=1")

    def test_followup_creates_new_version_history_preserved(self):
        r = self.app.cases.intake("HOSPITAL", "H-IMM3", self.zhang_patient())
        detail0 = self.app.cases.get_case(r["case_id"])
        v1 = detail0["versions"][0]
        self.app.cases.add_followup(r["case_id"], {"causality": "很可能",
                                                   "outcome": "恢复"})
        detail = self.app.cases.get_case(r["case_id"])
        self.assertEqual(2, len(detail["versions"]))
        self.assertEqual("INTAKE", detail["versions"][0]["change_type"])
        self.assertEqual("FOLLOWUP", detail["versions"][1]["change_type"])
        self.assertNotIn("causality", detail["versions"][0]["snapshot"])
        self.assertEqual("很可能", detail["versions"][1]["snapshot"]["causality"])
        # 原始来源报文内容从未改动
        conn = self.db()
        row = conn.execute("SELECT payload_json FROM source_records").fetchone()
        self.assertIn("不严重", row["payload_json"])

    def test_merged_source_record_still_readable(self):
        r1 = self.app.cases.intake("HOSPITAL", "H-M1", self.zhang_patient())
        r2 = self.app.cases.intake("HOTLINE", "HL-M1", self.zhang_multilang())
        cid = r2["duplicate_clusters"][0]["cluster_id"]
        m = self.app.cases.confirm_cluster(cid, actor="liu")
        conn = self.db()
        n = conn.execute("SELECT COUNT(*) FROM source_records").fetchone()[0]
        self.assertEqual(2, n)   # 原始记录一条不少
        memberships = conn.execute(
            "SELECT source_system, external_id, reason, active FROM source_records s "
            "JOIN case_membership m ON m.source_id=s.id ORDER BY s.external_id"
        ).fetchall()
        systems = {row[0] for row in memberships}
        self.assertEqual({"HOSPITAL", "HOTLINE"}, systems)
        # 被合并来源只有一行 active，归属 survivor；历史 INTAKE 行 active=0 保留
        active = conn.execute(
            "SELECT case_id FROM case_membership WHERE active=1").fetchall()
        self.assertEqual(2, len(active))
        self.assertTrue(all(row[0] == m["survivor_case_id"] for row in active))
        # 被转移来源：原始 INTAKE 行原样停用，新行 reason=MERGE 挂 survivor
        rows = conn.execute(
            "SELECT reason, active FROM case_membership ORDER BY created_at, rowid"
        ).fetchall()
        inactive = [r for r in rows if r[1] == 0]
        active_rows = [r for r in rows if r[1] == 1]
        self.assertEqual(1, len(inactive))
        self.assertEqual("INTAKE", inactive[0][0])
        self.assertEqual(2, len(active_rows))
        self.assertTrue(all(r[0] in ("INTAKE", "MERGE") for r in active_rows))
        self.assertIn("MERGE", {r[0] for r in active_rows})

    def test_events_append_only(self):
        r = self.app.cases.intake("HOSPITAL", "H-IMM4", self.zhang_patient())
        conn = self.db()
        with self.assertRaises((sqlite3.OperationalError, sqlite3.IntegrityError)):
            conn.execute("UPDATE case_events SET summary='x' WHERE case_id=?",
                         (r["case_id"],))
        with self.assertRaises((sqlite3.OperationalError, sqlite3.IntegrityError)):
            conn.execute("DELETE FROM case_events WHERE case_id=?", (r["case_id"],))

    def test_membership_cannot_be_mutated_only_deactivated(self):
        r = self.app.cases.intake("HOSPITAL", "H-IMM5", self.zhang_patient())
        conn = self.db()
        mid = conn.execute("SELECT id FROM case_membership WHERE active=1").fetchone()[0]
        # 改 case_id 等任何字段都被拒绝
        with self.assertRaises((sqlite3.OperationalError, sqlite3.IntegrityError)):
            conn.execute("UPDATE case_membership SET reason='HACK' WHERE id=?", (mid,))
        # active 1->0 合法
        conn.execute("UPDATE case_membership SET active=0 WHERE id=?", (mid,))
        self.assertEqual(0, conn.execute(
            "SELECT active FROM case_membership WHERE id=?", (mid,)).fetchone()[0])
        # 0->0 或 0->1 不允许（WHEN 条件 OLD.active=1）
        with self.assertRaises((sqlite3.OperationalError, sqlite3.IntegrityError)):
            conn.execute("UPDATE case_membership SET active=1 WHERE id=?", (mid,))
