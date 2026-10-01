"""审计哈希链：追加、逐帧核验、篡改检测、旧库迁移回填。"""
import json

from tests.base import BasePVTest
from pv_service import audit, db as dbmod


class TestAuditChain(BasePVTest):
    def test_events_are_chained_with_continuous_seq(self):
        r = self.app.cases.intake("HOSPITAL", "H-CHAIN-1", self.zhang_patient())
        cid = r["case_id"]
        self.app.cases.add_followup(cid, {"outcome": "好转"})
        conn = self.db()
        rows = conn.execute(
            "SELECT * FROM case_events WHERE case_id=? ORDER BY seq", (cid,)).fetchall()
        self.assertGreaterEqual(len(rows), 4)
        prev = audit.GENESIS
        for i, row in enumerate(rows, start=1):
            self.assertEqual(i, row["seq"])
            self.assertEqual(prev, row["prev_hash"])
            entry = {"case_id": row["case_id"], "seq": row["seq"],
                     "event_type": row["event_type"], "event_time": row["event_time"],
                     "actor": row["actor"], "summary": row["summary"],
                     "payload_json": row["payload_json"], "version_id": row["version_id"],
                     "report_id": row["report_id"], "cluster_id": row["cluster_id"],
                     "prev_hash": row["prev_hash"]}
            self.assertEqual(audit.hash_entry(entry), row["entry_hash"])
            prev = row["entry_hash"]

    def test_verify_chain_detects_tamper(self):
        r = self.app.cases.intake("HOSPITAL", "H-CHAIN-2", self.zhang_patient())
        events = self.app.cases.timeline(r["case_id"])
        audit.verify_chain(events)  # 不抛异常即通过

        tampered = [dict(e) for e in events]
        tampered[1]["summary"] = "被篡改的摘要"
        with self.assertRaises(audit.ChainBroken) as ctx:
            audit.verify_chain(tampered)
        self.assertTrue(any("entry_hash 不匹配" in f for f in ctx.exception.failures))

    def test_verify_chain_detects_deleted_frame(self):
        r = self.app.cases.intake("HOSPITAL", "H-CHAIN-3", self.zhang_patient())
        self.app.cases.add_followup(r["case_id"], {"outcome": "未好转"})
        events = self.app.cases.timeline(r["case_id"])
        del events[1]  # 删掉中间一帧
        with self.assertRaises(audit.ChainBroken) as ctx:
            audit.verify_chain(events)
        self.assertTrue(any("prev_hash 不匹配" in f or "断号" in f
                            for f in ctx.exception.failures))

    def test_seq_unique_per_case_independent_chains(self):
        r1 = self.app.cases.intake("HOSPITAL", "H-CHAIN-A", self.zhang_patient())
        r2 = self.app.cases.intake("HOSPITAL", "H-CHAIN-B",
                                   {**self.zhang_patient(), "patient_identifier": "OTHER"})
        e1 = self.app.cases.timeline(r1["case_id"])
        e2 = self.app.cases.timeline(r2["case_id"])
        # 两个病例各自从 seq=1 独立起链
        self.assertEqual(1, e1[0]["seq"])
        self.assertEqual(1, e2[0]["seq"])
        self.assertEqual(audit.GENESIS, e1[0]["prev_hash"])
        self.assertEqual(audit.GENESIS, e2[0]["prev_hash"])


class TestAuditMigration(BasePVTest):
    """旧库（无哈希链列）迁移回填。"""

    def test_migration_backfills_chain(self):
        # 先在旧 schema 上产生事件：直接构造一个无链列的库
        import shutil
        import sqlite3
        from pv_service.app import Application
        # 用当前 app 建数据后，模拟旧表结构（删列重建成旧表）
        r = self.app.cases.intake("HOSPITAL", "H-MIG-1", self.zhang_patient())
        self.app.cases.add_followup(r["case_id"], {"outcome": "好转"})
        self.app.shutdown()

        conn = sqlite3.connect(self.app.db_path)
        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DROP TRIGGER trg_events_no_update")
        conn.execute("DROP TRIGGER trg_events_no_delete")
        conn.execute("DROP INDEX ix_events_case")
        conn.execute("DROP INDEX ux_events_case_seq")
        conn.execute("ALTER TABLE case_events RENAME TO ce_old")
        conn.execute("""CREATE TABLE case_events (
            id TEXT PRIMARY KEY, case_id TEXT NOT NULL, event_type TEXT NOT NULL,
            event_time TEXT NOT NULL, actor TEXT, summary TEXT NOT NULL,
            payload_json TEXT NOT NULL DEFAULT '{}', version_id TEXT,
            report_id TEXT, cluster_id TEXT)""")
        conn.execute("""INSERT INTO case_events
            (id,case_id,event_type,event_time,actor,summary,payload_json,
             version_id,report_id,cluster_id)
            SELECT id,case_id,event_type,event_time,actor,summary,payload_json,
                   version_id,report_id,cluster_id FROM ce_old""")
        conn.execute("DROP TABLE ce_old")
        conn.execute("CREATE INDEX ix_events_case ON case_events(case_id,event_time)")
        conn.execute("COMMIT")
        conn.close()

        # 重新初始化 -> 迁移回填
        app2 = Application(self.dir, clock=self.clock)
        events = app2.cases.timeline(r["case_id"])
        audit.verify_chain(events)
        self.assertEqual(1, events[0]["seq"])
        # 迁移后新事件也能正常接链
        app2.cases.add_followup(r["case_id"], {"outcome": "治愈"})
        audit.verify_chain(app2.cases.timeline(r["case_id"]))
        app2.shutdown()
