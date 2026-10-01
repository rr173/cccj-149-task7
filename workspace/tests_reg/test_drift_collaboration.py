"""引用漂移候选 + 多撰写人章节协作（乐观修订号）。"""
from reg_inquiry.errors import DraftSealedError, StaleRevisionError

from .reg_base import InquiryTestBase


class TestDriftCandidates(InquiryTestBase):
    def setUp(self):
        super().setUp()
        self.seed_materials()
        self.inq = self.register()
        self.draft = self.o.lock_draft(
            self.inq["id"], ["CASE:MAT-CASE@1", "REG:MAT-REG@1"])

    def test_new_version_does_not_silently_enter_current_draft(self):
        # 组稿期间登记 MAT-CASE v2
        self.o.register_material(
            material_id="MAT-CASE", version="2", category="CASE",
            content={"patient_name": "张三", "outcome": "恢复，已随访"},
            fact_key="OUTCOME", fact_value="恢复-随访", registered_by="reg")
        # 当前底稿的 pin 一个字节都不变
        still = self.o.get_draft(self.draft["id"])
        self.assertEqual([p["version"] for p in still["pins"]
                          if p["material_id"] == "MAT-CASE"], ["1"])

    def test_drift_spawns_reasoned_candidate_with_cloned_work(self):
        a = self.o.add_attachment(self.draft["id"], filename="f.bin",
                                  content=b"x",
                                  derived_from_ref="CASE:MAT-CASE@1")
        self.o.submit_section(self.draft["id"], "Q1",
                              f"原文 [CASE:MAT-CASE@1] @attachment:{a['id']}",
                              expected_revision=0, submitted_by="alice")
        self.o.register_material(
            material_id="MAT-CASE", version="2", category="CASE",
            content={"outcome": "v2"}, fact_key="OUTCOME", fact_value="v2",
            registered_by="reg")
        drafts = [d for d in self.o.list_drafts(self.inq["id"])
                  if d["kind"] == "CANDIDATE"]
        self.assertEqual(len(drafts), 1)
        cand = self.o.get_draft(drafts[0]["id"])
        self.assertEqual(cand["parent_draft_id"], self.draft["id"])
        # 原因与变化明细
        self.assertIn("MATERIAL_CHANGED:MAT-CASE@2", cand["drift_reason"])
        chg = cand["material_changes"][0]
        self.assertEqual((chg["old_version"], chg["new_version"]), ("1", "2"))
        # 候选 pin 新版本，且未改变无关引用
        refs = {p["material_id"]: p["version"] for p in cand["pins"]}
        self.assertEqual(refs["MAT-CASE"], "2")
        self.assertEqual(refs["MAT-REG"], "1")
        # 章节与附件被克隆（正文引用锚延续；附件溯源随版本漂移重映射）
        self.assertEqual([s["qid"] for s in cand["sections"]], ["Q1"])
        self.assertEqual([x["id"] for x in cand["attachments"]], [a["id"]])
        self.assertEqual(cand["attachments"][0]["derived_from_ref"],
                         "CASE:MAT-CASE@2")

    def test_drift_event_recorded(self):
        self.o.register_material(
            material_id="MAT-CASE", version="2", category="CASE",
            content={"outcome": "v2"}, fact_key="OUTCOME", fact_value="v2")
        cf = self.o.conflicts(self.draft["id"])
        self.assertEqual(len(cf["drift_events"]), 1)
        ev = cf["drift_events"][0]
        self.assertEqual(ev["old_version"], "1")
        self.assertEqual(ev["new_version"], "2")
        self.assertTrue(cf["drift_warnings"])
        self.assertEqual(cf["drift_warnings"][0]["code"], "PIN_STALE")

    def test_repeated_new_version_one_candidate_per_draft_chain(self):
        self.o.register_material(
            material_id="MAT-CASE", version="2", category="CASE",
            content={"outcome": "v2"}, fact_key="OUTCOME", fact_value="v2")
        self.o.register_material(
            material_id="MAT-CASE", version="3", category="CASE",
            content={"outcome": "v3"}, fact_key="OUTCOME", fact_value="v3")
        # BASE -> v2 候选；v2 候选已 pin 新链，v3 再从 BASE 派生一个候选；
        # 但候选自身不会再链式生成候选。
        candidates = [d for d in self.o.list_drafts(self.inq["id"])
                      if d["kind"] == "CANDIDATE"]
        base_spawned = [c for c in candidates if c["parent_draft_id"]
                        == self.draft["id"]]
        self.assertEqual(len(base_spawned), 2)
        # 幂等：重复注册同一版本不产生新候选
        self.o.register_material(
            material_id="MAT-CASE", version="3", category="CASE",
            content={"outcome": "v3"}, fact_key="OUTCOME", fact_value="v3")
        self.assertEqual(len([d for d in self.o.list_drafts(self.inq["id"])
                              if d["kind"] == "CANDIDATE"]), 2)


class TestSectionCollaboration(InquiryTestBase):
    def setUp(self):
        super().setUp()
        self.seed_materials()
        inq = self.register()
        self.draft = self.o.lock_draft(inq["id"], ["CASE:MAT-CASE@1"])
        self.inq = inq

    def test_revision_increments_and_history_append_only(self):
        s1 = self.o.submit_section(self.draft["id"], "Q1", "v0 内容",
                                   expected_revision=0, submitted_by="alice")
        self.assertEqual(s1["revision"], 1)
        s2 = self.o.submit_section(self.draft["id"], "Q1", "v1 内容",
                                   expected_revision=1, submitted_by="alice")
        self.assertEqual(s2["revision"], 2)
        hist = self.o.section_history(s1["id"])
        self.assertEqual([h["revision"] for h in hist], [1, 2])
        self.assertEqual(hist[0]["body"], "v0 内容")

    def test_stale_submit_rejected_and_recorded(self):
        self.o.submit_section(self.draft["id"], "Q1", "alice 的内容",
                              expected_revision=0, submitted_by="alice")
        # bob 基于过期修订 0 提交 -> 拒绝且不覆盖
        with self.assertRaises(StaleRevisionError) as cm:
            self.o.submit_section(self.draft["id"], "Q1", "bob 的内容",
                                  expected_revision=0, submitted_by="bob")
        self.assertEqual(cm.exception.details,
                         {"qid": "Q1", "expected": 0, "actual": 1})
        sec = [s for s in self.o.get_draft(self.draft["id"])["sections"]
               if s["qid"] == "Q1"][0]
        self.assertEqual(sec["body"], "alice 的内容")
        conflicts = self.o.conflicts(self.draft["id"])["section_conflicts"]
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(conflicts[0]["actual_revision"], 1)
        self.assertIn("bob 的内容", conflicts[0]["body_preview"])

    def test_multiple_authors_distinct_sections_concurrent_ok(self):
        # 不同章节互不干扰（多人同时提交不同问题）
        import threading
        outcomes = []
        b = threading.Barrier(2)

        def write(qid, author):
            b.wait()
            try:
                self.o.submit_section(self.draft["id"], qid, f"by {author}",
                                      expected_revision=0, submitted_by=author)
                outcomes.append("ok")
            except Exception as e:  # noqa: BLE001
                outcomes.append(getattr(e, "code", type(e).__name__))

        t1 = threading.Thread(target=write, args=("Q1", "alice"))
        t2 = threading.Thread(target=write, args=("Q2", "bob"))
        t1.start(); t2.start(); t1.join(); t2.join()
        self.assertEqual(outcomes, ["ok", "ok"])

    def test_concurrent_same_section_single_winner(self):
        import threading
        b = threading.Barrier(5)
        outcomes = []

        def write(i):
            b.wait()
            node = self.new_node()
            try:
                node.submit_section(self.draft["id"], "Q1", f"body-{i}",
                                    expected_revision=0,
                                    submitted_by=f"a{i}")
                outcomes.append("ok")
            except Exception as e:  # noqa: BLE001
                outcomes.append(getattr(e, "code", type(e).__name__))

        ts = [threading.Thread(target=write, args=(i,)) for i in range(5)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        self.assertEqual(outcomes.count("ok"), 1)
        self.assertEqual(outcomes.count("STALE_REVISION"), 4)
        sec = [s for s in self.o.get_draft(self.draft["id"])["sections"]
               if s["qid"] == "Q1"][0]
        self.assertEqual(sec["revision"], 1)
        self.assertEqual(len(self.o.conflicts(self.draft["id"])
                          ["section_conflicts"]), 4)

    def test_no_edit_after_seal(self):
        self.o.submit_section(self.draft["id"], "Q1", "x [CASE:MAT-CASE@1]",
                              expected_revision=0, submitted_by="alice")
        self.o.submit_section(self.draft["id"], "Q2", "y [CASE:MAT-CASE@1]",
                              expected_revision=0, submitted_by="bob")
        self.o.finalize(self.draft["id"], sealed_by="lead")
        with self.assertRaises(DraftSealedError):
            self.o.submit_section(self.draft["id"], "Q1", "改",
                                  expected_revision=1, submitted_by="x")
        with self.assertRaises(DraftSealedError):
            self.o.add_attachment(self.draft["id"], filename="z",
                                  content=b"z")
