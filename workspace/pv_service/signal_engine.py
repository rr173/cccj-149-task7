"""信号窗口计算引擎（确定性纯函数层）。

输入：冻结的病例版本行（sig_run_case_snapshot 指向的 sig_case_versions）+
冻结术语映射 + 冻结监测定义配置；输出：每来源的解析状态、分片四格计数与
花名册。整个层不接触时钟、不产生随机性，相同输入字节必然得到相同输出。

来源状态解析规则（截止线 snapshot 已经把每病例选中的版本冻结好）：

- 每个存活病例最新版本的 ``source_id`` 是一个计入单位（一份报告=一个来源）；
- MERGE 版本 ``merged_from_json`` 里的被吸收来源 → 排除 MERGED_AWAY
  （去重后由存活来源代表整簇，绝不重复计数）；
- 撤回（active_case=0 且 WITHDRAW）→ 排除 WITHDRAWN（退出样本）；
- SPLIT：拆出方成为独立病例的最新版本，天然各自解析一次；
- 事件原文经 *冻结术语版本* 归一映射；任一事件无法映射 → EVENT_UNMAPPED
  （无法判断 E/非 E，整例排除）；映射成功但不属于任何被监测事件组 → 作为
  已知“非 E”进入背景；
- 药品码不属于任何被监测药品组 → DRUG_NOT_MONITORED 排除
  （无法判定 D/非 D 的归属，不能放进背景）；
- 观察期 [window.start, window.end) 半开区间按 report_date 判定。
"""
from __future__ import annotations

import hashlib
import json
import unicodedata
from collections import defaultdict
from typing import Any

from . import signal_metrics as metrics_mod

# ---------------------------------------------------------------------------
# 规范序列化 / 指纹
# ---------------------------------------------------------------------------

def canon(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":")).encode("utf-8")


def digest(obj: Any) -> str:
    return hashlib.sha256(canon(obj)).hexdigest()


def normalize_term(text: str) -> str:
    """事件原文规范键：NFKC 折叠 + casefold + 压缩空白。"""
    if not isinstance(text, str):
        raise ValueError("事件术语必须是字符串")
    return " ".join(unicodedata.normalize("NFKC", text).casefold().split())


# ---------------------------------------------------------------------------
# 定义配置解析与校验
# ---------------------------------------------------------------------------

def parse_config(config: dict) -> dict:
    """校验并归一化监测定义配置。"""
    drug_groups = config.get("drug_groups")
    event_groups = config.get("event_groups")
    windows = config.get("windows")
    if not isinstance(drug_groups, dict) or not drug_groups:
        raise ValueError("drug_groups 必须是非空 {组名: [药品码...]}")
    if not isinstance(event_groups, dict) or not event_groups:
        raise ValueError("event_groups 必须是非空 {组名: [事件码...]}")
    if not isinstance(windows, list) or not windows:
        raise ValueError("windows 必须是非空观察期列表")

    dg, eg = {}, {}
    for name, codes in drug_groups.items():
        if not isinstance(codes, list) or not codes:
            raise ValueError(f"药品组 {name} 药品码列表不能为空")
        dg[name] = sorted(str(c) for c in codes)
    for name, codes in event_groups.items():
        if not isinstance(codes, list) or not codes:
            raise ValueError(f"事件组 {name} 事件码列表不能为空")
        eg[name] = sorted(str(c) for c in codes)

    # 一个药品码/事件码在定义内至多属于一个组（否则四格归属不确定）
    _unique_membership(dg, "药品码")
    _unique_membership(eg, "事件码")

    norm_windows = []
    for w in windows:
        wid = str(w["id"])
        start, end = str(w["start"]), str(w["end"])
        if not start < end:
            raise ValueError(f"观察期 {wid} 非法：start 必须早于 end（半开区间）")
        norm_windows.append({"id": wid, "start": start, "end": end})
    ids = [w["id"] for w in norm_windows]
    if len(ids) != len(set(ids)):
        raise ValueError("观察期 id 必须唯一")

    out = {
        "drug_groups": dg,
        "event_groups": eg,
        "windows": norm_windows,
        "min_n": int(config.get("min_n", 0)),
        "min_a": int(config.get("min_a", 0)),
        "continuity_correction": bool(config.get("continuity_correction", False)),
        "require": str(config.get("require", "ALL")).upper(),
        "causality_exclude": sorted(str(x) for x in
                                    (config.get("causality_exclude") or [])),
    }
    for k in ("prr_min", "prr_ci_lower_than", "ror_min",
              "ror_ci_lower_than", "chi_square_min"):
        if config.get(k) is not None:
            out[k] = float(config[k])
    return out


def _unique_membership(groups: dict, label: str):
    seen = {}
    for gname, codes in groups.items():
        for code in codes:
            if code in seen:
                raise ValueError(f"{label} {code} 同时属于 {seen[code]} 与 {gname}")
            seen[code] = gname


def criteria_from_config(cfg: dict) -> dict:
    keys = ("min_n", "min_a", "prr_min", "prr_ci_lower_than", "ror_min",
            "ror_ci_lower_than", "chi_square_min", "require",
            "continuity_correction")
    return {k: cfg[k] for k in keys if k in cfg}


# ---------------------------------------------------------------------------
# 病例版本 -> 来源解析状态
# ---------------------------------------------------------------------------

def _loads(value, default):
    if value is None:
        return default
    if isinstance(value, (list, dict)):
        return value
    return json.loads(value)


def resolve_states(version_rows: list, term_mappings: dict, cfg: dict) -> list[dict]:
    """把冻结快照中的病例版本解析为 *按来源* 的状态列表（按 source_id 排序）。

    version_rows：快照 join sig_case_versions 的行（sqlite3.Row 或 dict 列表），
    每个病例一行（提交时已选好截止线下的最新版本）。
    """
    drug_groups = cfg["drug_groups"]
    event_groups = cfg["event_groups"]
    code2drug = {c: g for g, codes in drug_groups.items() for c in codes}
    code2event = {c: g for g, codes in event_groups.items() for c in codes}

    # 存活病例 -> 被它吸收的来源集合
    merged_into: dict[str, str] = {}
    survivor_version: dict[str, Any] = {}
    rows_by_case: dict[str, Any] = {}
    # 每个来源（source_id）的最新版本行：被吸收来源的花名册/台账指向其自身版本
    latest_by_source: dict[str, Any] = {}
    for r in version_rows:
        rows_by_case[r["case_id"]] = r
        latest_by_source[str(r["source_id"])] = r
        if int(r["active_case"]) == 1:
            absorbed = _loads(r["merged_from_json"], [])
            if absorbed:
                survivor_version[r["case_id"]] = r
                for src in absorbed:
                    merged_into[str(src)] = r["case_id"]

    states: list[dict] = []

    def base_state(source_id, case_id, version_row):
        return {
            "source_id": str(source_id),
            "case_id": str(case_id),
            "version_id": version_row["id"],
            "version_no": int(version_row["version_no"]),
            "change_type": version_row["change_type"],
            "report_date": version_row["report_date"],
            "active": bool(int(version_row["active_case"])),
            "eligible": False,
            "exclude_reason": None,
            "drug_groups": [],
            "event_groups": [],
            "causality": {},
            "merged_from": sorted(str(x) for x in
                                  _loads(version_row["merged_from_json"], [])),
            "split_from": version_row["split_from"],
        }

    # 1) 被吸收来源（去重后不单独计数；花名册保留 MERGED_AWAY 痕迹，
    #    指向被吸收来源自身的最新版本，而不是存活者版本）
    for source_id in sorted(merged_into):
        surv_case = merged_into[source_id]
        own = latest_by_source.get(source_id) or survivor_version[surv_case]
        st = base_state(source_id, own["case_id"], own)
        st["exclude_reason"] = "MERGED_AWAY"
        states.append(st)

    # 2) 每个病例最新版本代表的来源（已被吸收的来源不再作为存活来源重复出现）
    for case_id in sorted(rows_by_case):
        r = rows_by_case[case_id]
        source_id = str(r["source_id"])
        if source_id in merged_into:
            continue
        st = base_state(source_id, case_id, r)

        drugs = sorted(str(d) for d in _loads(r["drugs_json"], []))
        raw_events = [str(e) for e in _loads(r["events_json"], [])]
        causality_raw = _loads(r["causality_json"], {})

        drug_membership = sorted({code2drug[d] for d in drugs if d in code2drug})
        st["drug_groups"] = drug_membership

        # 术语归一（冻结版本）：任一原始事件无法映射 -> 整例不可判定
        mapped_events, unmapped = [], []
        for raw in raw_events:
            code = term_mappings.get(normalize_term(raw))
            if code is None:
                unmapped.append(raw)
            else:
                mapped_events.append(str(code))
        if unmapped:
            st["exclude_reason"] = "EVENT_UNMAPPED"
            st["unmapped_terms"] = sorted(set(unmapped))
            states.append(st)
            continue

        event_membership = sorted({code2event[c] for c in mapped_events
                                   if c in code2event})
        st["event_groups"] = event_membership
        # 因果评价以规范事件码为键保留（映射后的码）
        causality = {}
        for raw, val in causality_raw.items():
            code = term_mappings.get(normalize_term(raw), raw)
            causality[str(code)] = str(val)
        st["causality"] = causality

        if not int(r["active_case"]):
            st["exclude_reason"] = ("WITHDRAWN" if r["change_type"] == "WITHDRAW"
                                    else "INACTIVE_CASE")
            states.append(st)
            continue
        if drugs and not drug_membership:
            st["exclude_reason"] = "DRUG_NOT_MONITORED"
            states.append(st)
            continue

        st["eligible"] = True
        states.append(st)

    states.sort(key=lambda s: s["source_id"])
    return states


def in_window(state: dict, window: dict) -> bool:
    return window["start"] <= state["report_date"] < window["end"]


def _related_event(state: dict, event_group: dict, group_name: str,
                   cfg: dict) -> bool:
    """是否构成该事件组的“相关事件”：命中事件组且因果评价未被口径排除。"""
    codes = set(event_group)
    exclude = set(cfg.get("causality_exclude") or [])
    for code, val in state.get("causality", {}).items():
        if code in codes and val in exclude:
            return False
    return group_name in state["event_groups"]


def assign_cell(state: dict, drug_group: str, event_group_codes: list,
                event_group_name: str, window: dict, cfg: dict):
    """返回该来源在 (D=药品组, E=事件组, 窗口) 四格中的格；排除/不在窗口返回 None。"""
    if not state["eligible"] or not in_window(state, window):
        return None
    exposed = drug_group in state["drug_groups"]
    reacted = _related_event(state, event_group_codes, event_group_name, cfg)
    if exposed and reacted:
        return "a"
    if exposed and not reacted:
        return "b"
    if not exposed and reacted:
        return "c"
    return "d"


# ---------------------------------------------------------------------------
# 分片与聚合
# ---------------------------------------------------------------------------

def shard_of(source_id: str, shard_count: int) -> int:
    h = int(hashlib.sha256(str(source_id).encode("utf-8")).hexdigest(), 16)
    return h % shard_count


def empty_counts(cfg: dict) -> dict:
    return {f"{dg}|{eg}": {"a": 0, "b": 0, "c": 0, "d": 0}
            for dg in sorted(cfg["drug_groups"])
            for eg in sorted(cfg["event_groups"])}


def build_shard_artifacts(states: list, cfg: dict, shard_count: int) -> list[dict]:
    """把全部来源状态切成 shard_count 个确定性分片产物。

    每个分片：{shard_no, counts, roster, states, checksum}
    roster[window][pair] = {cell: [source_id...]}；
    states[window][source_id] = 该窗口的可比较属性（供增量/差异使用）。
    """
    shards = []
    for shard_no in range(shard_count):
        counts = empty_counts(cfg)
        roster: dict = {w["id"]: defaultdict(lambda: defaultdict(list))
                        for w in cfg["windows"]}
        exclusions: dict = {w["id"]: [] for w in cfg["windows"]}
        win_states: dict = {w["id"]: {} for w in cfg["windows"]}

        for st in states:
            if shard_of(st["source_id"], shard_count) != shard_no:
                continue
            for w in cfg["windows"]:
                inw = in_window(st, w) if st["eligible"] else False
                # 窗口状态只记录“落入该观察期”的来源（含被排除者，便于追溯退出）
                if not (inw or (not st["eligible"] and in_window(st, w))):
                    continue
                win_states[w["id"]][st["source_id"]] = _state_attrs(st)
                if not st["eligible"]:
                    exclusions[w["id"]].append(
                        {"source_id": st["source_id"], "case_id": st["case_id"],
                         "version_id": st["version_id"],
                         "reason": st["exclude_reason"]})
                    continue
                for dg_name, dg_codes in sorted(cfg["drug_groups"].items()):
                    for eg_name, eg_codes in sorted(cfg["event_groups"].items()):
                        cell = assign_cell(st, dg_name, eg_codes, eg_name, w, cfg)
                        if cell is None:
                            continue
                        counts[f"{dg_name}|{eg_name}"][cell] += 1
                        roster[w["id"]][f"{dg_name}|{eg_name}"][cell].append(
                            st["source_id"])

        roster_out = {}
        for wid, pairs in roster.items():
            roster_out[wid] = {}
            for pair, cells in sorted(pairs.items()):
                roster_out[wid][pair] = {k: sorted(v)
                                         for k, v in sorted(cells.items())}
            roster_out[wid]["__excluded__"] = sorted(
                exclusions[wid], key=lambda x: x["source_id"])
        artifact = {
            "shard_no": shard_no,
            "counts": counts,
            "roster": roster_out,
            "states": {wid: dict(sorted(s.items())) for wid, s in win_states.items()},
        }
        artifact["checksum"] = digest(
            {"counts": artifact["counts"], "roster": artifact["roster"]})
        shards.append(artifact)
    return shards


def _state_attrs(st: dict) -> dict:
    return {
        "case_id": st["case_id"],
        "version_id": st["version_id"],
        "version_no": st["version_no"],
        "eligible": st["eligible"],
        "exclude_reason": st["exclude_reason"],
        "drug_groups": sorted(st["drug_groups"]),
        "event_groups": sorted(st["event_groups"]),
        "causality": dict(sorted(st["causality"].items())),
        "change_type": st["change_type"],
        "report_date": st["report_date"],
    }

def merge_counts(total: dict, part: dict) -> dict:
    for pair, cells in part.items():
        tgt = total.setdefault(pair, {"a": 0, "b": 0, "c": 0, "d": 0})
        for k in ("a", "b", "c", "d"):
            tgt[k] += cells.get(k, 0)
    return total


def aggregate(shards: list, cfg: dict) -> dict:
    total = empty_counts(cfg)
    for sh in sorted(shards, key=lambda s: s["shard_no"]):
        merge_counts(total, sh["counts"])
    return total


def changed_sources(old_states: dict, new_states: dict) -> dict:
    """对比两个全量 states（{source: attrs}），分类差异来源。

    返回 {added:[...], removed:[...], changed:[{source, old, new, kind}...]}。
    kind: DRUG_GROUP_CHANGE / EVENT_GROUP_CHANGE / CAUSALITY_UPDATE / VERSION_REFRESH
    """
    old_keys, new_keys = set(old_states), set(new_states)
    added = sorted(new_keys - old_keys)
    removed = sorted(old_keys - new_keys)
    changed = []
    for src in sorted(old_keys & new_keys):
        o, n = old_states[src], new_states[src]
        if o == n:
            continue
        if o.get("eligible") != n.get("eligible") or \
                o.get("exclude_reason") != n.get("exclude_reason"):
            if not n.get("eligible"):
                kind = "EXITED_SAMPLE"
            elif not o.get("eligible"):
                kind = "REENTERED_SAMPLE"
            else:
                kind = "ELIGIBILITY_CHANGE"
        elif o.get("drug_groups") != n.get("drug_groups"):
            kind = "DRUG_GROUP_CHANGE"
        elif o.get("event_groups") != n.get("event_groups"):
            kind = "EVENT_GROUP_CHANGE"
        elif o.get("causality") != n.get("causality"):
            kind = "CAUSALITY_UPDATE"
        else:
            kind = "VERSION_REFRESH"
        changed.append({"source_id": src, "kind": kind, "old": o, "new": n})
    return {"added": added, "removed": removed, "changed": changed}


def source_window_ledger(state: dict, window: dict, cfg: dict) -> dict | None:
    """某来源在某窗口的台账视图；不在观察期返回 None。

    cells 给出它在该窗口每一个药品组×事件组四格中的归属（一次贡献的完整快照）。
    """
    if not in_window(state, window):
        return None
    cells = {}
    if state["eligible"]:
        for dg_name, dg_codes in sorted(cfg["drug_groups"].items()):
            for eg_name, eg_codes in sorted(cfg["event_groups"].items()):
                cell = assign_cell(state, dg_name, dg_codes, eg_name, window, cfg)
                if cell:
                    cells[f"{dg_name}|{eg_name}"] = cell
    return {
        "eligible": state["eligible"],
        "exclude_reason": state["exclude_reason"],
        "drug_groups": sorted(state["drug_groups"]),
        "event_groups": sorted(state["event_groups"]),
        "cells": cells,
    }


def evaluate_pair(counts: dict, cfg: dict) -> dict:
    cells = counts
    m = metrics_mod.compute_metrics(
        cells["a"], cells["b"], cells["c"], cells["d"],
        continuity_correction=cfg.get("continuity_correction", False))
    verdict = metrics_mod.evaluate(m, criteria_from_config(cfg))
    m["determination"] = verdict["determination"]
    m["criteria_detail"] = verdict
    return m
