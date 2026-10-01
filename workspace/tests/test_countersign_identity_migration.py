"""旧库迁移：把“原始字符串即身份”升级为规范化身份键。

场景（需求：服务重启与已有未决评审单下不能误放行，且保留可追溯的原始
提交值和既有审计记录）：

- 历史 OPEN 单上同一复核员用多种表面写法占了多个意见槽（旧漏洞产物）：
  迁移后按规范化键合并为一槽、重算 approval_count，单子仍 OPEN、不放行；
- 历史已终态（APPROVED/REJECTED）结论与 countersign_approvals 原样保留；
- 租约 lease_holder 回填规范化键；事件流水 actor_key 回填且 id/明细不丢；
- 重复执行 initialize 幂等；迁移后等价写法不能再占第二槽。
"""
import shutil
import sqlite3
import tempfile
import unittest
from datetime import datetime

from pv_service import db as dbmod
from pv_service.app import Application, FixedClock
from pv_service.service import DomainError


# 迁移前的最小旧 schema（review_signatures 以 (sheet_id, reviewer) 为主键，
# 各表都没有规范化身份键列）。
_OLD_SCHEMA = """
CREATE TABLE review_revision (scope TEXT PRIMARY KEY DEFAULT 'GLOBAL',
  revision INTEGER NOT NULL DEFAULT 0);
CREATE TABLE review_revision_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT, revision INTEGER NOT NULL UNIQUE,
  reason_code TEXT NOT NULL, detail_json TEXT NOT NULL DEFAULT '{}',
  actor TEXT, created_at TEXT NOT NULL);
CREATE TABLE review_sheets (
  id TEXT PRIMARY KEY, status TEXT NOT NULL DEFAULT 'OPEN'
    CHECK (status IN ('OPEN','APPROVED','REJECTED','INVALIDATED')),
  revision INTEGER NOT NULL, content_hash TEXT NOT NULL, title TEXT NOT NULL,
  requested_by TEXT, lease_holder TEXT, leased_at TEXT, lease_expires_at TEXT,
  approval_count INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL, decided_at TEXT,
  invalidated_reason TEXT, invalidated_at TEXT, invalidated_revision INTEGER);
CREATE TABLE review_sheet_items (
  id TEXT PRIMARY KEY, sheet_id TEXT NOT NULL, position INTEGER NOT NULL,
  item_type TEXT NOT NULL, ref_type TEXT, ref_id TEXT, snapshot_json TEXT NOT NULL,
  UNIQUE(sheet_id, position));
CREATE TABLE review_signatures (
  sheet_id TEXT NOT NULL, reviewer TEXT NOT NULL, verdict TEXT NOT NULL
    CHECK (verdict IN ('APPROVED','VETOED')),
  detail_json TEXT NOT NULL DEFAULT '{}', signed_at TEXT NOT NULL,
  PRIMARY KEY (sheet_id, reviewer));
CREATE TABLE review_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT, sheet_id TEXT NOT NULL,
  event_type TEXT NOT NULL, actor TEXT,
  detail_json TEXT NOT NULL DEFAULT '{}', created_at TEXT NOT NULL);
CREATE TABLE countersign_approvals (
  report_id TEXT PRIMARY KEY, sheet_id TEXT NOT NULL, revision INTEGER NOT NULL,
  approved_at TEXT NOT NULL, approved_by_json TEXT NOT NULL);
CREATE TRIGGER trg_review_events_no_update BEFORE UPDATE ON review_events
BEGIN SELECT RAISE(FAIL,'review_events is append-only'); END;
CREATE TRIGGER trg_review_events_no_delete BEFORE DELETE ON review_events
BEGIN SELECT RAISE(FAIL,'review_events is append-only'); END;
"""


class TestIdentityMigration(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="pv-migtest-")
        self.db_path = f"{self.dir}/pv.db"
        self.clock = FixedClock(datetime(2026, 9, 10, 9, 30, 0))

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def _build_old_db(self):
        conn = sqlite3.connect(self.db_path)
        try:
            conn.executescript(_OLD_SCHEMA)
            conn.execute("INSERT INTO review_revision(scope,revision) VALUES('GLOBAL',3)")
            # 1) 旧漏洞产物：OPEN 单上同一人用三种写法占了两个赞成槽
            conn.execute("""INSERT INTO review_sheets(id,status,revision,content_hash,
              title,lease_holder,leased_at,lease_expires_at,approval_count,created_at)
              VALUES('sheetFRAUD','OPEN',3,'h1','t','  Alice  ','2026-09-10T09:00:00',
              '2026-09-10T10:00:00',2,'2026-09-10T09:00:00')""")
            conn.execute("INSERT INTO review_signatures VALUES"
                         "('sheetFRAUD','  alice  ','APPROVED','{}','2026-09-10T09:01:00')")
            conn.execute("INSERT INTO review_signatures VALUES"
                         "('sheetFRAUD','ALICE','APPROVED','{}','2026-09-10T09:02:00')")
            for et, ac in [("CREATED", None), ("CLAIMED", "  Alice  "),
                           ("APPROVED", "  alice  "), ("APPROVED", "ALICE")]:
                conn.execute("INSERT INTO review_events(sheet_id,event_type,actor,"
                             "created_at) VALUES(?,?,?,?)",
                             ("sheetFRAUD", et, ac, "2026-09-10T09:02:00"))
            # 2) 合法的历史已完成单：两名不同复核员，结论与放行记录保留
            conn.execute("""INSERT INTO review_sheets(id,status,revision,content_hash,
              title,approval_count,created_at,decided_at)
              VALUES('sheetOK','APPROVED',2,'h2','t',2,'2026-09-09T09:00:00',
              '2026-09-09T09:05:00')""")
            conn.execute("INSERT INTO review_signatures VALUES"
                         "('sheetOK','carol','APPROVED','{}','2026-09-09T09:01:00')")
            conn.execute("INSERT INTO review_signatures VALUES"
                         "('sheetOK','dave','APPROVED','{}','2026-09-09T09:02:00')")
            conn.execute("INSERT INTO countersign_approvals VALUES"
                         "('rptOK','sheetOK',2,'2026-09-09T09:05:00','[\"carol\",\"dave\"]')")
            # 3) 被否决的历史单（保留）
            conn.execute("""INSERT INTO review_sheets(id,status,revision,content_hash,
              title,approval_count,created_at,decided_at)
              VALUES('sheetVETO','REJECTED',1,'h3','t',0,'2026-09-08T09:00:00',
              '2026-09-08T09:01:00')""")
            conn.execute("INSERT INTO review_signatures VALUES"
                         "('sheetVETO','zoe','VETOED','{}','2026-09-08T09:01:00')")
            conn.commit()
        finally:
            conn.close()

    def test_migration_dedups_fraudulent_open_sheet_without_release(self):
        self._build_old_db()
        app = Application(self.dir, clock=self.clock, countersign_required=True)
        try:
            fraud = app.reviews.get_sheet("sheetFRAUD")
            # 仍是未决单，但只剩一个意见槽，绝不能因迁移误放行
            self.assertEqual("OPEN", fraud["status"])
            self.assertEqual(1, fraud["approval_count"])
            self.assertEqual(["alice"], fraud["approver_keys"])
            self.assertFalse(fraud["released_for_dispatch"])
            # 历史合法放行记录保留，但欺诈未决单本身没有产生任何放行
            self.assertFalse(
                app.reviews.is_approved_for_dispatch("rptFRAUD"))
            self.assertEqual(
                [], [a for a in app.reviews.dispatch_approvals()
                     if a["sheet_id"] == "sheetFRAUD"])
            # 租约键已回填，原始持有者字符串保留
            self.assertEqual("alice", fraud["lease_holder_key"])
            self.assertEqual("  Alice  ", fraud["lease_holder"])
            # 等价写法不能再占第二槽；真正的第二人才能完成
            with self.assertRaises(DomainError) as cm:
                app.reviews.approve("sheetFRAUD", "Alice")
            self.assertEqual("ALREADY_SIGNED", cm.exception.code)
            sh = app.reviews.approve("sheetFRAUD", "bob")
            self.assertEqual("APPROVED", sh["status"])
        finally:
            app.shutdown()

    def test_migration_preserves_terminal_sheets_and_approvals(self):
        self._build_old_db()
        app = Application(self.dir, clock=self.clock, countersign_required=True)
        try:
            ok = app.reviews.get_sheet("sheetOK")
            self.assertEqual("APPROVED", ok["status"])
            self.assertEqual(["carol", "dave"], sorted(ok["approver_keys"]))
            gate = app.reviews.dispatch_approvals("rptOK")
            self.assertEqual(1, len(gate))
            self.assertEqual(["carol", "dave"], gate[0]["approved_by"])
            veto = app.reviews.get_sheet("sheetVETO")
            self.assertEqual("REJECTED", veto["status"])
        finally:
            app.shutdown()

    def test_migration_preserves_event_stream_and_backfills_actor_key(self):
        self._build_old_db()
        app = Application(self.dir, clock=self.clock, countersign_required=True)
        try:
            sh = app.reviews.get_sheet("sheetFRAUD")
            # 四条历史流水（含同一人的两种原始 actor 写法）全部保留
            self.assertEqual(4, len(sh["events"]))
            raw_actors = {e["actor"] for e in sh["events"] if e["actor"]}
            self.assertIn("  alice  ", raw_actors)
            self.assertIn("ALICE", raw_actors)
            self.assertIn("  Alice  ", raw_actors)
            # actor_key 全部归一到 alice
            for e in sh["events"]:
                if e["actor"] is not None:
                    self.assertEqual("alice", e["actor_key"])
            # 按规范化身份可跨多种原始写法查询
            trail = app.reviews.audit_trail(sheet_id="sheetFRAUD", actor="ALICE")
            types = {e["event_type"] for e in trail}
            self.assertEqual({"CLAIMED", "APPROVED"}, types)
        finally:
            app.shutdown()

    def test_migration_is_idempotent_across_repeated_initialize_and_restart(self):
        self._build_old_db()
        app = Application(self.dir, clock=self.clock, countersign_required=True)
        app.shutdown()
        # 再次初始化（模拟多次服务启动）不报错、不重复槽位
        dbmod.initialize(self.db_path)
        app2 = Application(self.dir, clock=self.clock, countersign_required=True)
        try:
            fraud = app2.reviews.get_sheet("sheetFRAUD")
            self.assertEqual(1, fraud["approval_count"])
            self.assertEqual(["alice"], fraud["approver_keys"])
            conn = dbmod.connect(self.db_path)
            try:
                n = conn.execute(
                    "SELECT COUNT(*) FROM review_signatures WHERE sheet_id='sheetFRAUD'"
                ).fetchone()[0]
                self.assertEqual(1, n)
            finally:
                conn.close()
        finally:
            app2.shutdown()


if __name__ == "__main__":
    unittest.main()
