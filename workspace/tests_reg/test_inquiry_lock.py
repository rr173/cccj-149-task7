"""问询登记 / 资料注册 / 精确版本锁定：缺失、越权、矛盾必须阻断。"""
from reg_inquiry.errors import (ContradictionError, MissingMaterialsError,
                                OutOfScopeError, VersionConflictError)

from .reg_base import InquiryTestBase


class TestInquiryRegistration(InquiryTestBase):
    def test_inquiry_questions_and_scope_frozen(self):
        inq = self.register()
        self.assertEqual([q["qid"] for q in inq["questions"]], ["Q1", "Q2"])
        self.assertEqual(set(inq["scope"]["allowed_material_ids"]),
                         {"MAT-CASE", "MAT-REG", "MAT-STAT", "MAT-CASE2"})
        self.assertEqual(inq["deadline"], "2026-10-20T00:00:00+00:00")
        # 问询登记后不可改（数据库触发器兜底）
        import sqlite3
        with self.assertRaises(sqlite3.Error):
            with self.o.conn() as c:
                c.execute("UPDATE inquiries SET deadline='2030-01-01' WHERE id=?",
                          (inq["id"],))

    def test_duplicate_qid_rejected(self):
        with self.assertRaises(ValueError):
            self.o.register_inquiry(
                regulator="R", jurisdiction="CN",
                questions=[{"qid": "Q1", "text": "a"},
                           {"qid": "Q1", "text": "b"}],
                deadline="2026-10-20T00:00:00+00:00",
                allowed_material_ids=[])

    def test_material_content_dedup_and_version_conflict(self):
        self.seed_materials()
        # 同内容重复登记（哪怕换版本号）-> 返回既有行
        again = self.o.register_material(
            material_id="MAT-CASE", version="9", category="CASE",
            content={"patient_name": "张三", "outcome": "恢复"})
        self.assertTrue(again["deduped"])
        self.assertEqual(again["version"], "1")
        # 同 (id,version) 不同内容 -> 冲突
        with self.assertRaises(VersionConflictError):
            self.o.register_material(
                material_id="MAT-CASE", version="1", category="CASE",
                content={"patient_name": "张三", "outcome": "死亡"})


class TestDraftLocking(InquiryTestBase):
    def setUp(self):
        super().setUp()
        self.seed_materials()

    def test_lock_pins_exact_versions(self):
        inq = self.register()
        d = self.o.lock_draft(inq["id"], ["CASE:MAT-CASE@1", "REG:MAT-REG@1"])
        refs = sorted(p["ref"] for p in d["pins"])
        self.assertEqual(refs, ["CASE:MAT-CASE@1", "REG:MAT-REG@1"])
        for p in d["pins"]:
            self.assertIsNotNone(p["content_hash"])

    def test_lock_idempotent_one_base(self):
        inq = self.register()
        d1 = self.o.lock_draft(inq["id"], ["CASE:MAT-CASE@1"])
        d2 = self.o.lock_draft(inq["id"], ["CASE:MAT-CASE@1"])
        self.assertEqual(d1["id"], d2["id"])
        drafts = self.o.list_drafts(inq["id"])
        self.assertEqual(len(drafts), 1)

    def test_missing_material_blocks_lock_and_lists_it(self):
        inq = self.register()
        with self.assertRaises(MissingMaterialsError) as cm:
            self.o.lock_draft(inq["id"], ["CASE:GHOST@7"])
        missing = cm.exception.details["missing"]
        self.assertEqual(missing[0]["material_id"], "GHOST")
        self.assertEqual(missing[0]["version"], "7")
        self.assertEqual(len(self.o.list_drafts(inq["id"])), 0)

    def test_out_of_scope_material_blocks(self):
        # 只允许 MAT-CASE；尝试锁定 MAT-REG（未在范围）-> 越权
        inq = self.register(allowed=("MAT-CASE",))
        with self.assertRaises(OutOfScopeError) as cm:
            self.o.lock_draft(inq["id"], ["REG:MAT-REG@1"])
        self.assertEqual(cm.exception.details["out_of_scope"][0]["material_id"],
                         "MAT-REG")

    def test_out_of_scope_category_blocks(self):
        inq = self.register(
            allowed=("MAT-CASE", "MAT-REG"),
            questions=[{"qid": "Q1", "text": "a"}],
        )
        # 收紧类别范围（手工改 scope 不允许；改为登记时限定类别）
        inq2 = self.o.register_inquiry(
            regulator="R2", jurisdiction="CN",
            questions=[{"qid": "Q1", "text": "a"}],
            deadline="2026-10-20T00:00:00+00:00",
            allowed_material_ids=["MAT-CASE", "MAT-REG"],
            allowed_categories=["CASE"])
        with self.assertRaises(OutOfScopeError):
            self.o.lock_draft(inq2["id"], ["REG:MAT-REG@1"])

    def test_contradicting_versions_block_lock(self):
        inq = self.register()
        with self.assertRaises(ContradictionError) as cm:
            self.o.lock_draft(inq["id"],
                              ["CASE:MAT-CASE@1", "CASE:MAT-CASE2@1"])
        c = cm.exception.details["contradictions"][0]
        self.assertEqual(c["fact_key"], "OUTCOME")
        self.assertEqual(c["value_a"], "恢复")
        self.assertEqual(c["value_b"], "未恢复")
        # 阻断后不留任何底稿
        self.assertEqual(len(self.o.list_drafts(inq["id"])), 0)
