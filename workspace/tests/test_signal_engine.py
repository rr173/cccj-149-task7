"""引擎解析与四格归属测试：归并/拆解/撤回/未映射/观察期/背景计数。"""
import unittest

from pv_service import signal_engine as eng

TERMS = {"肝炎": "HEP", "肝功能异常": "HEP", "头痛": "HEAD", "皮疹": "RASH"}
CFG = eng.parse_config({
    "drug_groups": {"X": ["X"], "OTHERS": ["Y", "Z"]},
    "event_groups": {"HEP": ["HEP"], "HEAD": ["HEAD"]},
    "windows": [{"id": "W", "start": "2026-01-01", "end": "2026-04-01"}],
    "min_n": 1})
W = CFG["windows"][0]


class R(dict):
    """sqlite3.Row 风格的最小行对象（支持 r["x"] 与属性无关）。"""
    def __getattr__(self, name):
        return self[name]


def vrow(case_id, version_no, *, source_id=None, change_type="INTAKE",
         drugs=("X",), events=("肝炎",), report_date="2026-02-01", active=1,
         merged_from=(), split_from=None, vid=None, causality=None):
    import json
    return R(id=vid or f"v-{case_id}-{version_no}", case_id=case_id,
             version_no=version_no, change_type=change_type,
             source_id=source_id or case_id,
             merged_from_json=json.dumps(list(merged_from)),
             split_from=split_from, drugs_json=json.dumps(list(drugs)),
             events_json=json.dumps(list(events)),
             causality_json=json.dumps(causality or {}),
             report_date=report_date, active_case=active)


def by_source(states):
    return {s["source_id"]: s for s in states}


class TestResolve(unittest.TestCase):
    def test_basic_cells(self):
        rows = [
            vrow("c1", 1, drugs=("X",), events=("肝炎",)),                 # a
            vrow("c2", 1, drugs=("X",), events=("头痛",)),                 # b
            vrow("c3", 1, source_id="c3", drugs=("Y",), events=("肝炎",)),  # c
            vrow("c4", 1, drugs=("Z",), events=("皮疹",)),                 # d
        ]
        st = by_source(eng.resolve_states(rows, TERMS, CFG))
        for s in st.values():
            self.assertTrue(s["eligible"])
        cell = lambda sid: eng.assign_cell(st[sid], "X", ["HEP"], "HEP", W, CFG)
        self.assertEqual(cell("c1"), "a")
        self.assertEqual(cell("c2"), "b")
        self.assertEqual(cell("c3"), "c")
        self.assertEqual(cell("c4"), "d")

    def test_merge_absorbed_sources_do_not_double_count(self):
        rows = [
            vrow("c1", 1, source_id="s1", drugs=("X",), events=("肝炎",)),
            vrow("c2", 1, source_id="s2", drugs=("X",), events=("肝炎",),
                 change_type="MERGE"),
            # c1 在第 2 版吸收 s2
            vrow("c1", 2, source_id="s1", drugs=("X",), events=("肝炎",),
                 change_type="MERGE", merged_from=("s2",)),
        ]
        st = by_source(eng.resolve_states(rows, TERMS, CFG))
        self.assertTrue(st["s1"]["eligible"])
        self.assertFalse(st["s2"]["eligible"])
        self.assertEqual(st["s2"]["exclude_reason"], "MERGED_AWAY")

    def test_split_each_branch_counts_once(self):
        rows = [
            vrow("c1", 2, source_id="s1", drugs=("X",), events=("肝炎",),
                 change_type="SPLIT"),
            vrow("c9", 1, source_id="s9", drugs=("X",), events=("肝炎",),
                 change_type="SPLIT", split_from="s1"),
        ]
        st = by_source(eng.resolve_states(rows, TERMS, CFG))
        self.assertTrue(st["s1"]["eligible"])
        self.assertTrue(st["s9"]["eligible"])

    def test_withdraw_exits_sample(self):
        rows = [vrow("c1", 1, drugs=("X",), events=("肝炎",)),
                vrow("c1", 2, drugs=("X",), events=("肝炎",),
                     change_type="WITHDRAW", active=0)]
        st = by_source(eng.resolve_states(rows, TERMS, CFG))
        self.assertFalse(st["c1"]["eligible"])
        self.assertEqual(st["c1"]["exclude_reason"], "WITHDRAWN")

    def test_unmapped_event_excludes_entire_case(self):
        rows = [vrow("c1", 1, events=("未知反应",))]
        st = by_source(eng.resolve_states(rows, TERMS, CFG))
        self.assertEqual(st["c1"]["exclude_reason"], "EVENT_UNMAPPED")

    def test_mapped_but_unmonitored_event_is_background(self):
        # RASH 已归一但不在任何事件组；X+RASH 对 HEP 是 b，不是排除
        rows = [vrow("c1", 1, events=("皮疹",))]
        st = by_source(eng.resolve_states(rows, TERMS, CFG))
        self.assertTrue(st["c1"]["eligible"])
        self.assertEqual(eng.assign_cell(st["c1"], "X", ["HEP"], "HEP", W, CFG), "b")

    def test_unmonitored_drug_excluded(self):
        rows = [vrow("c1", 1, drugs=("W",), events=("肝炎",))]
        st = by_source(eng.resolve_states(rows, TERMS, CFG))
        self.assertEqual(st["c1"]["exclude_reason"], "DRUG_NOT_MONITORED")

    def test_window_half_open(self):
        st = by_source(eng.resolve_states([
            vrow("c1", 1, report_date="2026-01-01"),
            vrow("c2", 1, report_date="2026-03-31"),
            vrow("c3", 1, report_date="2026-04-01")], TERMS, CFG))
        self.assertTrue(eng.in_window(st["c1"], W))
        self.assertTrue(eng.in_window(st["c2"], W))
        self.assertFalse(eng.in_window(st["c3"], W))  # 右端排除

    def test_shard_artifacts_deterministic_and_partition(self):
        rows = [vrow(f"c{i}", 1, drugs=("X",), events=("肝炎",))
                for i in range(20)]
        states = eng.resolve_states(rows, TERMS, CFG)
        a1 = eng.build_shard_artifacts(states, CFG, 4)
        a2 = eng.build_shard_artifacts(states, CFG, 4)
        self.assertEqual(a1, a2)
        # 每个来源恰好落到一个分片
        seen = []
        for sh in a1:
            seen += list(sh["states"]["W"])
        self.assertEqual(sorted(seen), sorted(s["source_id"] for s in states))
        total = eng.aggregate(a1, CFG)
        self.assertEqual(total["X|HEP"]["a"], 20)
        # 不同分片数聚合同一结果
        total8 = eng.aggregate(eng.build_shard_artifacts(states, CFG, 8), CFG)
        self.assertEqual(total, total8)

    def test_changed_sources_classification(self):
        old = {"s1": {"eligible": True, "exclude_reason": None,
                      "drug_groups": ["X"], "event_groups": [],
                      "causality": {}, "version_no": 1, "case_id": "c1",
                      "version_id": "v1", "change_type": "FOLLOWUP",
                      "report_date": "2026-02-01"},
               "s2": {"eligible": True, "exclude_reason": None,
                      "drug_groups": ["X"], "event_groups": ["HEP"],
                      "causality": {}, "version_no": 1, "case_id": "c2",
                      "version_id": "v1", "change_type": "FOLLOWUP",
                      "report_date": "2026-02-01"}}
        new = dict(old)
        new["s1"] = dict(old["s1"], drug_groups=["OTHERS"])          # 跨药品组
        new["s2"] = dict(old["s2"], causality={"HEP": "UNLIKELY"})  # 因果修正
        new["s3"] = dict(old["s1"])                                   # 新增
        ch = eng.changed_sources(old, new)
        kinds = {c["source_id"]: c["kind"] for c in ch["changed"]}
        self.assertEqual(kinds["s1"], "DRUG_GROUP_CHANGE")
        self.assertEqual(kinds["s2"], "CAUSALITY_UPDATE")
        self.assertEqual(ch["added"], ["s3"])
        # 已有的 s2 撤回退出样本
        new["s2"] = dict(new["s2"], eligible=False,
                         exclude_reason="WITHDRAWN", event_groups=[])
        ch2 = eng.changed_sources(old, new)
        self.assertIn(("s2", "EXITED_SAMPLE"),
                      [(c["source_id"], c["kind"]) for c in ch2["changed"]])

    def test_duplicate_group_membership_rejected(self):
        with self.assertRaises(ValueError):
            eng.parse_config({
                "drug_groups": {"A": ["X"], "B": ["X"]},
                "event_groups": {"E": ["HEP"]},
                "windows": [{"id": "w", "start": "2026-01-01",
                             "end": "2026-02-01"}]})


if __name__ == "__main__":
    unittest.main()
