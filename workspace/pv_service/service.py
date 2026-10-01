"""病例工作台领域服务：接收、随访新版本、合并、拆分、时间线。

并发模型：
- 进程内 threading.RLock + SQLite ``BEGIN IMMEDIATE`` 数据库写锁，
  合并/拆分/回执等跨表写操作整体串行化；
- 来源归属由部分唯一索引 ``ux_membership_active`` 兜底，任何来源
  在任何时刻只能归属一个当前病例（即使锁失效数据库也拒绝双归属）。
"""
from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from datetime import datetime
from typing import Any, Callable

from . import audit
from . import db as dbmod
from . import normalize
from . import revision as revmod
from .dedup import UnionFind, build_profile, score_profiles, CLUSTER_THRESHOLD
from .rules import RuleContext, RuleEngine, is_serious, significant_changes


class DomainError(Exception):
    """可映射到 HTTP 4xx 的领域错误。"""
    def __init__(self, code: str, message: str, status: int = 400):
        super().__init__(message)
        self.code = code
        self.status = status


class Clock:
    def now(self) -> datetime:
        return datetime.now()

    def now_iso(self) -> str:
        return self.now().isoformat(timespec="seconds")


def _uid() -> str:
    return uuid.uuid4().hex


def _json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, default=str)


def _latest_snapshot(conn: sqlite3.Connection, case_id: str) -> dict:
    row = conn.execute(
        "SELECT snapshot_json FROM case_versions WHERE case_id=? "
        "ORDER BY version_no DESC LIMIT 1", (case_id,)).fetchone()
    return json.loads(row["snapshot_json"]) if row else {}


def _latest_version(conn: sqlite3.Connection, case_id: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM case_versions WHERE case_id=? ORDER BY version_no DESC LIMIT 1",
        (case_id,)).fetchone()


# 随访补丁允许修改的“评价相关”字段路径（白名单，防止篡改原始标识）
FOLLOWUP_PATCH_PATHS = {
    "seriousness", "serious", "seriousness_criteria", "outcome", "causality",
    "narrative", "event_date", "onset_date",
}


def _build_snapshot(raw: dict) -> dict:
    """从来源原始 payload 构造病例首个规范化快照（原始 payload 不动）。"""
    patient = dict(raw.get("patient") or {})
    if raw.get("patient_name") and not patient.get("name"):
        patient["name"] = raw["patient_name"]
    # 强标识字段提升到 patient 节点（来源报文可能放在顶层）
    for k in ("dob", "gender", "patient_identifier", "national_id",
              "medical_record_no", "mrn", "id_card"):
        if raw.get(k) and not patient.get(k):
            patient[k] = raw[k]
    # 顶层其余未识别键不进快照，避免污染评价
    snap: dict = {
        "patient": patient,
        "drugs": raw.get("drugs") or ([raw["drug"]] if raw.get("drug") else []),
        "reactions": raw.get("reactions") or ([raw["reaction"]]
                                              if raw.get("reaction") else []),
    }
    for k in ("event_date", "seriousness", "seriousness_criteria", "outcome",
              "causality", "narrative"):
        if raw.get(k) is not None:
            snap[k] = raw[k]
    snap["event_date"] = snap.get("event_date") or raw.get("onset_date")
    snap["serious"] = is_serious(snap)
    return snap


def _merge_snapshots(primary: dict, others: list[dict]) -> dict:
    """合并多个病例快照：缺失字段相互补全，按更强评价取值。"""
    import copy
    merged = copy.deepcopy(primary)
    merged.setdefault("patient", {})

    def _merge_list(key):
        seen, acc = set(), []
        for item in merged.get(key) or []:
            term = (item.get("name") or item.get("term") or item.get("drug")
                    or item.get("reaction")) if isinstance(item, dict) else item
            c = (normalize.canonical_drug(term) if key == "drugs"
                 else normalize.canonical_reaction(term))
            if c and c not in seen:
                seen.add(c)
                acc.append(copy.deepcopy(item))
        for snap in others:
            for item in snap.get(key) or []:
                term = (item.get("name") or item.get("term") or item.get("drug")
                        or item.get("reaction")) if isinstance(item, dict) else item
                c = (normalize.canonical_drug(term) if key == "drugs"
                     else normalize.canonical_reaction(term))
                if c and c not in seen:
                    seen.add(c)
                    acc.append(copy.deepcopy(item))
        merged[key] = acc

    _merge_list("drugs")
    _merge_list("reactions")

    for snap in others:
        p = snap.get("patient") or {}
        for k, v in p.items():
            if v and not merged["patient"].get(k):
                merged["patient"][k] = v
        for k in ("event_date", "seriousness", "outcome", "causality", "narrative"):
            if snap.get(k) and not merged.get(k):
                merged[k] = snap[k]
        # 严重性取“更严重”，因果性取更强
        if is_serious(snap):
            merged["serious"] = True
        crit = set(merged.get("seriousness_criteria") or []) | \
            set(snap.get("seriousness_criteria") or [])
        if crit:
            merged["seriousness_criteria"] = sorted(crit)
        if normalize.CAUSALITY_RANK.get(normalize.canonical_causality(snap.get("causality")), 0) \
                > normalize.CAUSALITY_RANK.get(
                    normalize.canonical_causality(merged.get("causality")), 0):
            merged["causality"] = snap["causality"]
    merged["serious"] = is_serious(merged)
    return merged


def _apply_followup_patch(snapshot: dict, content: dict) -> dict:
    import copy
    out = copy.deepcopy(snapshot)
    applied: dict[str, Any] = {}
    # 标量字段白名单（点路径仅允许 patient.*）
    for path, value in content.items():
        if "." in path:
            head, sub = path.split(".", 1)
            if head != "patient":
                continue
            out.setdefault("patient", {})
            if isinstance(out["patient"], dict):
                out["patient"][sub] = value
                applied[path] = value
        elif path in FOLLOWUP_PATCH_PATHS:
            out[path] = value
            applied[path] = value

    # 反应列表：新增反应按 canonical 去重追加，不覆盖历史（旧反应可能已报送）
    extra_rx = content.get("new_reactions") or []
    if isinstance(extra_rx, str):
        extra_rx = [extra_rx]
    if extra_rx:
        acc = list(out.get("reactions") or [])
        have = set(normalize.canonical_reactions(out))
        added = []
        for r in extra_rx:
            term = r.get("term") if isinstance(r, dict) else r
            cr = normalize.canonical_reaction(term)
            if cr and cr not in have:
                have.add(cr)
                acc.append({"term": term} if not isinstance(r, dict) else r)
                added.append(cr)
        out["reactions"] = acc
        if added:
            applied["new_reactions"] = added

    extra_drugs = content.get("new_drugs") or []
    if isinstance(extra_drugs, str):
        extra_drugs = [extra_drugs]
    if extra_drugs:
        acc = list(out.get("drugs") or [])
        have = set(normalize.canonical_drugs(out))
        added = []
        for d in extra_drugs:
            name = d.get("name") if isinstance(d, dict) else d
            cd = normalize.canonical_drug(name)
            if cd and cd not in have:
                have.add(cd)
                acc.append({"name": name} if not isinstance(d, dict) else d)
                added.append(cd)
        out["drugs"] = acc
        if added:
            applied["new_drugs"] = added

    # 药品用药时间修正：drugs[].start_date
    drug_timing: dict[str, Any] = {}
    if isinstance(content.get("drug_start_dates"), dict):
        for dname, dt in content["drug_start_dates"].items():
            cd = normalize.canonical_drug(dname)
            for d in out.get("drugs") or []:
                if isinstance(d, dict) and normalize.canonical_drug(
                        d.get("name") or d.get("drug")) == cd:
                    d["start_date"] = dt
                    drug_timing[cd] = dt
    out["serious"] = is_serious(out)
    out["_applied_patch"] = applied
    if drug_timing:
        out["_applied_patch"]["drug_start_dates"] = drug_timing
    return out


class CaseService:
    def __init__(self, db_path: str, rule_engine: RuleEngine | None = None,
                 clock: Clock | None = None,
                 number_prefix: str = "REG", shared_lock: threading.RLock | None = None,
                 jurisdiction: str = "CN", rule_resolver: Callable | None = None):
        self.db_path = db_path
        self.engine = rule_engine or RuleEngine()
        self.clock = clock or Clock()
        self.number_prefix = number_prefix
        # 本服务接收病例的默认辖区（法规口径作用域）。规则治理层可按辖区
        # 把当前生效规则版本解析为具体规则引擎；未装配时回退内置历史口径。
        self.jurisdiction = jurisdiction
        self.rule_resolver = rule_resolver
        # 与报送派发共享：合并/拆分与“发送取报告”互斥，杜绝发送窗口内被合并
        self._lock = shared_lock or threading.RLock()
        dbmod.initialize(db_path)

    def resolve_rule(self, jurisdiction: str | None = None) -> tuple[str, str, RuleEngine]:
        """返回 (package_id, version, engine)：本次计算应固定使用的规则版本。

        - 装配了规则治理层（``rule_resolver``）时按辖区解析当前生效版本；
        - 否则固定为内置历史口径 builtin:legacy@0。
        报送规则计算、会签冻结、发送都从同一处取得版本，保证三处一致。
        """
        jur = jurisdiction or self.jurisdiction
        if self.rule_resolver is not None:
            return self.rule_resolver(jur)
        from .rule_pkg import LEGACY_PACKAGE_ID, LEGACY_VERSION
        return LEGACY_PACKAGE_ID, LEGACY_VERSION, self.engine

    @staticmethod
    def _rule_ref_view(rule_ref: tuple[str, str] | None) -> dict:
        if not rule_ref:
            from .rule_pkg import LEGACY_PACKAGE_ID, LEGACY_VERSION
            return {"package_id": LEGACY_PACKAGE_ID, "version": LEGACY_VERSION}
        return {"package_id": rule_ref[0], "version": rule_ref[1]}

    # -- 基础设施 -----------------------------------------------------------
    def _conn(self) -> sqlite3.Connection:
        return dbmod.connect(self.db_path)

    def is_listed(self, drug_canonical: str, reaction_canonical: str) -> bool:
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT 1 FROM ref_listed_reactions WHERE drug_canonical=? "
                "AND reaction_canonical=?", (drug_canonical, reaction_canonical)).fetchone()
            return row is not None
        finally:
            conn.close()

    def seed_listedness(self, pairs: list[tuple[str, str]]) -> None:
        conn = self._conn()
        try:
            with dbmod.transaction(conn):
                conn.executemany(
                    "INSERT OR IGNORE INTO ref_listed_reactions"
                    "(drug_canonical, reaction_canonical) VALUES(?,?)",
                    [(normalize.canonical_drug(d), normalize.canonical_reaction(r))
                     for d, r in pairs])
        finally:
            conn.close()

    def _event(self, conn, case_id, event_type, summary,
               payload=None, version_id=None, report_id=None, cluster_id=None,
               actor=None, when: str | None = None):
        return audit.append_event(
            conn, case_id, event_type, summary, payload,
            event_time=when or self.clock.now_iso(), actor=actor,
            version_id=version_id, report_id=report_id, cluster_id=cluster_id)

    # -- 监管编号 + 报告规划（与业务变更同一事务，杜绝重复编号）----------------
    def _allocate_number(self, conn) -> str:
        scope = self.clock.now().strftime("%Y")
        conn.execute(
            "INSERT INTO number_sequence(scope,last) VALUES(?,1) "
            "ON CONFLICT(scope) DO UPDATE SET last=last+1", (scope,))
        last = conn.execute("SELECT last FROM number_sequence WHERE scope=?",
                            (scope,)).fetchone()["last"]
        return f"{self.number_prefix}-{scope}-{last:05d}"

    def _plan_report(self, conn, case_id: str, version_row: sqlite3.Row,
                     decision, prior_version_snapshot: dict | None,
                     awareness_iso: str, material_id: str | None,
                     rule_ref: tuple[str, str] | None = None) -> dict | None:
        if not decision.should_report:
            self._event(conn, case_id, "REPORT_NOT_REQUIRED", decision.rationale,
                        {"decision": decision.as_dict(), "version_id": version_row["id"],
                         "rule": self._rule_ref_view(rule_ref)},
                        version_id=version_row["id"])
            return None
        # 同一版本幂等：唯一索引 ux_report_version 兜底
        exists = conn.execute("SELECT id FROM regulatory_reports WHERE case_version_id=?",
                              (version_row["id"],)).fetchone()
        if exists:
            return {"id": exists["id"], "dedup": True}
        followup_of = None
        superseded_numbers: list[str] = []
        if decision.report_type == "FOLLOWUP":
            # 关联到此前已发送的监管报告；合并后被合并病例的报告也在血缘内，
            # 以其为随访基准，保留同事件历史报告的连续追溯链。
            lineage_ids = [r["ancestor_id"] for r in conn.execute(
                "SELECT ancestor_id FROM case_lineage WHERE descendant_id=?",
                (case_id,))]
            if case_id not in lineage_ids:
                lineage_ids.append(case_id)
            placeholders = ",".join("?" * len(lineage_ids))
            prior = conn.execute(
                f"SELECT id FROM regulatory_reports WHERE case_id IN ({placeholders}) "
                f"AND status='ACKED' AND report_type='INITIAL' ORDER BY created_at DESC LIMIT 1",
                lineage_ids).fetchone()
            if not prior:
                prior = conn.execute(
                    f"SELECT id, regulatory_number FROM regulatory_reports "
                    f"WHERE case_id IN ({placeholders}) AND status<>'VOID' "
                    f"ORDER BY created_at DESC LIMIT 1", lineage_ids).fetchone()
            followup_of = prior["id"] if prior else None
            # 同事件其他病例已取得的监管编号随随访报告一并报送（监管连续性）
            for r in conn.execute(
                    f"SELECT regulatory_number FROM regulatory_reports "
                    f"WHERE case_id IN ({placeholders}) AND status='ACKED' AND id IS NOT ?",
                    (*lineage_ids, followup_of or "")):
                superseded_numbers.append(r["regulatory_number"])
        reg_no = self._allocate_number(conn)
        rid = _uid()
        snapshot = json.loads(version_row["snapshot_json"])
        rule_pkg_id, rule_ver = (rule_ref or ("builtin:legacy", "0"))
        conn.execute(
            "INSERT INTO regulatory_reports(id,case_id,case_version_id,followup_of_id,"
            "report_type,regulatory_number,status,expedited,days_due,due_date,"
            "decision_json,payload_json,jurisdiction,rule_package_id,rule_version,"
            "created_at,updated_at) "
            "VALUES(?,?,?,?,?,?,'QUEUED',?,?,?,?,?,?,?,?,?,?)",
            (rid, case_id, version_row["id"], followup_of, decision.report_type, reg_no,
             1 if decision.expedited else 0, decision.days_due, decision.due_date,
             _json(decision.as_dict()),
             _json({"regulatory_number": reg_no, "snapshot": snapshot,
                    "material_id": material_id, "awareness_date": awareness_iso,
                    "supersedes_regulatory_numbers": sorted(set(superseded_numbers)),
                    "rule_version": {"package_id": rule_pkg_id, "version": rule_ver}}),
             self.jurisdiction, rule_pkg_id, rule_ver,
             self.clock.now_iso(), self.clock.now_iso()))
        self._event(conn, case_id,
                    "REPORT_QUEUED" if not decision.expedited else "REPORT_QUEUED_EXPEDITED",
                    f"{'加急' if decision.expedited else '常规'}{'随访' if decision.report_type == 'FOLLOWUP' else '首次'}"
                    f"报送已排队 {reg_no}（{decision.rule_id}，截止 {decision.due_date}）",
                    {"report_id": rid, "regulatory_number": reg_no,
                     "decision": decision.as_dict(),
                     "rule_version": {"package_id": rule_pkg_id, "version": rule_ver}},
                    version_id=version_row["id"], report_id=rid)
        return {"id": rid, "regulatory_number": reg_no}

    def _new_version(self, conn, case_id, change_type, snapshot, summary,
                     awareness_date_iso: str, *, prior_snapshot=None,
                     material_id=None, actor=None, plan: bool = True,
                     change_summary=None):
        nxt = conn.execute(
            "SELECT COALESCE(MAX(version_no),0)+1 AS n FROM case_versions WHERE case_id=?",
            (case_id,)).fetchone()["n"]
        vid = _uid()
        clean_snap = {k: v for k, v in snapshot.items() if not k.startswith("_")}
        conn.execute(
            "INSERT INTO case_versions(id,case_id,version_no,change_type,snapshot_json,"
            "change_summary_json,material_id,actor,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (vid, case_id, nxt, change_type, _json(clean_snap),
             _json(change_summary or []), material_id, actor, self.clock.now_iso()))
        self._event(conn, case_id, f"VERSION_{change_type}",
                    f"v{nxt} {summary}", {"version_no": nxt},
                    version_id=vid, actor=actor)
        planned = None
        if plan:
            from datetime import date as _date
            aware = normalize.parse_date(awareness_date_iso) or self.clock.now().date()
            ctx = RuleContext(
                case_id=case_id, snapshot=clean_snap, awareness_date=aware,
                is_followup=change_type in ("FOLLOWUP", "MERGE"),
                prior_snapshot=prior_snapshot,
                is_listed_fn=self.is_listed, material_id=material_id)
            pkg_id, pkg_ver, engine = self.resolve_rule()
            decision = engine.evaluate(ctx)
            planned = self._plan_report(conn, case_id,
                                        {"id": vid, "snapshot_json": _json(clean_snap)},
                                        decision, prior_snapshot,
                                        awareness_date_iso, material_id,
                                        rule_ref=(pkg_id, pkg_ver))
        return vid, nxt, planned

    # -- 1) 接收来源报文（幂等）---------------------------------------------
    def intake(self, source_system: str, external_id: str, payload: dict,
               received_at: str | None = None) -> dict:
        if source_system not in ("HOSPITAL", "HOTLINE", "ENTERPRISE"):
            raise DomainError("BAD_SOURCE", f"未知来源渠道: {source_system}")
        sid = _uid()
        case_id = _uid()
        received = received_at or self.clock.now_iso()
        with self._lock:
            conn = self._conn()
            try:
                with dbmod.transaction(conn):
                    # 来源报文幂等：同一 (渠道, 外部号) 直接返回既有归属
                    dup_src = conn.execute(
                        "SELECT s.id AS sid, m.case_id FROM source_records s "
                        "LEFT JOIN case_membership m ON m.source_id=s.id AND m.active=1 "
                        "WHERE s.source_system=? AND s.external_id=?",
                        (source_system, external_id)).fetchone()
                    if dup_src:
                        return {"idempotent": True, "source_id": dup_src["sid"],
                                "case_id": dup_src["case_id"]}
                    conn.execute(
                        "INSERT INTO source_records(id,source_system,external_id,"
                        "received_at,payload_json,created_at) VALUES(?,?,?,?,?,?)",
                        (sid, source_system, external_id, received, _json(payload),
                         self.clock.now_iso()))
                    conn.execute(
                        "INSERT INTO cases(id,status,created_at) VALUES(?, 'OPEN', ?)",
                        (case_id, self.clock.now_iso()))
                    conn.execute(
                        "INSERT INTO case_membership(id,source_id,case_id,active,reason,"
                        "actor,created_at) VALUES(?,?,?,1,'INTAKE',NULL,?)",
                        (_uid(), sid, case_id, self.clock.now_iso()))
                    conn.execute("INSERT INTO case_lineage(ancestor_id,descendant_id,depth)"
                                 " VALUES(?,?,0)", (case_id, case_id))
                    snap = _build_snapshot(payload)
                    vid, vno, planned = self._new_version(
                        conn, case_id, "INTAKE", snap,
                        f"接收 {source_system} 报文 {external_id}",
                        received[:10], plan=True)
                    self._event(conn, case_id, "SOURCE_INTAKE",
                                f"来源 {source_system}/{external_id} 接入病例",
                                {"source_id": sid, "source_system": source_system,
                                 "external_id": external_id}, version_id=vid)
                    clusters = self._reconcile_duplicates(conn, case_id)
                    # 新材料入库：推进数据修订号，评审中的旧会签单立即失效
                    revmod.bump_revision(
                        conn, revmod.REV_NEW_MATERIAL,
                        detail={"case_id": case_id, "source_id": sid,
                                "source_system": source_system, "external_id": external_id},
                        actor=None, now_iso=self.clock.now_iso())
                return {"case_id": case_id, "source_id": sid, "version_no": vno,
                        "report": planned, "duplicate_clusters": clusters}
            finally:
                conn.close()

    # -- 疑似重复簇 ---------------------------------------------------------
    def _reconcile_duplicates(self, conn, new_case_id: str) -> list[dict]:
        """为新病例与全部 OPEN 病例打分，更新/生成待复核簇。

        - 被安全专员否决过的 *边* 永不复活（尊重人工决定）；
        - 仅对包含新病例、且实际由未否决边连通的分量建 PROPOSED 簇；
        - 触及的旧 PROPOSED 簇若成员集合扩大则 SUPERSEDED 后重建（审计链）。
        """
        rows = conn.execute(
            "SELECT c.id AS case_id, v.snapshot_json FROM cases c JOIN "
            "case_versions v ON v.case_id=c.id AND v.version_no=("
            "SELECT MAX(version_no) FROM case_versions WHERE case_id=c.id) "
            "WHERE c.status='OPEN'").fetchall()
        profiles = {}
        for r in rows:
            profiles[r["case_id"]] = build_profile(r["case_id"], json.loads(r["snapshot_json"]))
        if new_case_id not in profiles:
            return []

        # 已被人工否决的边（从 REJECTED 簇的 rationale 取）
        rejected_edges: set[frozenset] = set()
        for c in conn.execute("SELECT rationale_json FROM duplicate_clusters "
                              "WHERE status='REJECTED'"):
            for edge in json.loads(c["rationale_json"]).get("edges", []):
                rejected_edges.add(frozenset((edge["a"], edge["b"])))

        def _allowed(a, b) -> bool:
            return frozenset((a, b)) not in rejected_edges

        # 新病例出发的候选边
        new_edges: list[dict] = []
        for cid, prof in profiles.items():
            if cid == new_case_id:
                continue
            sc, reasons = score_profiles(profiles[new_case_id], prof)
            if sc >= CLUSTER_THRESHOLD and _allowed(new_case_id, cid):
                new_edges.append({"a": new_case_id, "b": cid,
                                  "score": sc, "reasons": reasons})
        if not new_edges:
            return []

        # 既有 PROPOSED 簇的边也参与连通（传递闭包），但被否决边不参与
        proposed_rows = conn.execute(
            "SELECT * FROM duplicate_clusters WHERE status='PROPOSED'").fetchall()
        existing_members: dict[str, set] = {}
        existing_edges: dict[str, list] = {}
        for crow in proposed_rows:
            cid = crow["id"]
            existing_members[cid] = {m["case_id"] for m in conn.execute(
                "SELECT case_id FROM cluster_members WHERE cluster_id=?", (cid,))}
            existing_edges[cid] = [e for e in json.loads(crow["rationale_json"])
                                   .get("edges", []) if _allowed(e["a"], e["b"])]

        uf = UnionFind(list(profiles))
        for cid, edges in existing_edges.items():
            for e in edges:
                if e["a"] in uf.parent and e["b"] in uf.parent:
                    uf.union(e["a"], e["b"])
        for e in new_edges:
            uf.union(e["a"], e["b"])

        component = {x for x in profiles if uf.find(x) == uf.find(new_case_id)}
        component.discard(new_case_id)
        if not component:
            return []
        component.add(new_case_id)

        # 分量内若任意两点存在被否决的边，人工已判定非同事件，不复活该簇
        if any(e <= component for e in rejected_edges):
            return []

        # 收集分量内全部 *未否决* 边（既有 + 新）
        all_edges = {(frozenset((e["a"], e["b"]))): e
                     for cid in existing_members if existing_members[cid] <= component
                     for e in existing_edges[cid]}
        for e in new_edges:
            all_edges[frozenset((e["a"], e["b"]))] = e
        comp_edges = list(all_edges.values())

        # 该分量触及哪些既有 PROPOSED 簇
        touched = [cid for cid, members in existing_members.items()
                   if members & component]
        # 只有分量确实扩大了某个既有簇才需要 supersede；纯新簇也正常建
        expanded = [cid for cid in touched if existing_members[cid] < component]
        new_cid = _uid()
        rationale = _json({"threshold": CLUSTER_THRESHOLD,
                           "edges": comp_edges, "members": sorted(component)})
        # 先插新簇（merged_into_id 外键要求目标已存在），再作废旧簇
        conn.execute(
            "INSERT INTO duplicate_clusters(id,status,rationale_json,created_at)"
            " VALUES(?,'PROPOSED',?,?)",
            (new_cid, rationale, self.clock.now_iso()))
        for cid in expanded:
            conn.execute(
                "UPDATE duplicate_clusters SET status='SUPERSEDED', merged_into_id=?,"
                "decided_at=? WHERE id=? AND status='PROPOSED'",
                (new_cid, self.clock.now_iso(), cid))
        if not expanded and touched:
            # 与既有簇成员完全相同：不重复提议，回滚刚插入的空新簇行
            conn.execute("DELETE FROM duplicate_clusters WHERE id=?", (new_cid,))
            return []
        conn.executemany("INSERT OR IGNORE INTO cluster_members(cluster_id,case_id,added_at)"
                         " VALUES(?,?,?)",
                         [(new_cid, m, self.clock.now_iso()) for m in sorted(component)])
        for m in sorted(component):
            self._event(conn, m, "DUPLICATE_PROPOSED",
                        f"疑似重复簇 {new_cid[:8]} 待复核（{len(component)} 个病例）",
                        {"cluster_id": new_cid, "score_edges": comp_edges},
                        cluster_id=new_cid)
        return [{"cluster_id": new_cid, "members": sorted(component),
                 "edges": comp_edges}]

    def list_clusters(self, status: str = "PROPOSED") -> list[dict]:
        conn = self._conn()
        try:
            out = []
            for c in conn.execute(
                    "SELECT * FROM duplicate_clusters WHERE status=? ORDER BY created_at",
                    (status,)):
                members = [r["case_id"] for r in conn.execute(
                    "SELECT case_id FROM cluster_members WHERE cluster_id=? ORDER BY case_id",
                    (c["id"],))]
                out.append({"id": c["id"], "status": c["status"],
                            "rationale": json.loads(c["rationale_json"]),
                            "members": members, "created_at": c["created_at"],
                            "decided_by": c["decided_by"], "reject_reason": c["reject_reason"]})
            return out
        finally:
            conn.close()

    # -- 2) 安全专员确认合并 / 否决 ------------------------------------------
    def confirm_cluster(self, cluster_id: str, survivor_case_id: str | None = None,
                        actor: str | None = None) -> dict:
        with self._lock:
            conn = self._conn()
            try:
                with dbmod.transaction(conn):
                    cluster = conn.execute(
                        "SELECT * FROM duplicate_clusters WHERE id=?",
                        (cluster_id,)).fetchone()
                    if not cluster:
                        raise DomainError("CLUSTER_NOT_FOUND", "疑似重复簇不存在", 404)
                    if cluster["status"] != "PROPOSED":
                        raise DomainError("CLUSTER_NOT_OPEN",
                                          f"簇状态为 {cluster['status']}，不能确认", 409)
                    members = [r["case_id"] for r in conn.execute(
                        "SELECT case_id FROM cluster_members WHERE cluster_id=? ORDER BY "
                        "added_at, case_id", (cluster_id,))]
                    cases = {r["id"]: r for r in conn.execute(
                        "SELECT * FROM cases WHERE id IN (%s)" % ",".join("?" * len(members)),
                        members)}
                    if any(c["status"] != "OPEN" for c in cases.values()):
                        bad = [k for k, c in cases.items() if c["status"] != "OPEN"]
                        raise DomainError("CASE_NOT_OPEN",
                                          f"病例 {bad} 已被合并/拆分，簇已过期", 409)
                    if survivor_case_id:
                        if survivor_case_id not in cases:
                            raise DomainError("BAD_SURVIVOR", "存活病例必须是簇成员")
                        survivor = survivor_case_id
                    else:
                        # 默认存活者 = 最早接入来源的病例；时间戳相同则按
                        # 病例行的物理插入顺序（rowid）定序，保证确定
                        first_seen = {}
                        for c in members:
                            row = conn.execute(
                                "SELECT MIN(s.received_at) AS earliest FROM case_membership m "
                                "JOIN source_records s ON s.id=m.source_id WHERE m.case_id=?",
                                (c,)).fetchone()
                            crow = conn.execute("SELECT rowid FROM cases WHERE id=?",
                                                (c,)).fetchone()["rowid"]
                            first_seen[c] = (row["earliest"], crow)
                        survivor = min(first_seen, key=first_seen.get)

                    # 归属转移：先校验全部来源当前确实挂在成员病例上（并发冲突 -> 409）
                    memberships = conn.execute(
                        "SELECT * FROM case_membership WHERE active=1 AND case_id IN (%s)"
                        % ",".join("?" * len(members)), members).fetchall()
                    if survivor not in members:
                        raise DomainError("BAD_SURVIVOR", "存活病例必须是簇成员")

                    prior_snap = _latest_snapshot(conn, survivor)
                    other_snaps = [_latest_snapshot(conn, m) for m in members
                                   if m != survivor]
                    merged_snap = _merge_snapshots(prior_snap, other_snaps)
                    merged_from = [m for m in members if m != survivor]

                    # 重复病例在合并前各自排队的未发送报告必须作废：
                    # 编号永不复用、行保留审计；已 ACKED 的报告保留并可追溯，
                    # 由 survivor 之后的随访报告承接（见 _plan_report 血缘关联）。
                    voided = conn.execute(
                        "SELECT id, regulatory_number, case_id FROM regulatory_reports "
                        "WHERE case_id IN (%s) AND status IN ('QUEUED','SENDING')"
                        % ",".join("?" * len(merged_from)), merged_from).fetchall() \
                        if merged_from else []
                    for vr in voided:
                        conn.execute(
                            "UPDATE regulatory_reports SET status='VOID', last_error=?,"
                            "updated_at=? WHERE id=?",
                            (f"病例合并入 {survivor}（重复簇 {cluster_id}），未发送报告作废，"
                             "监管编号保留不复用", self.clock.now_iso(), vr["id"]))
                        self._event(conn, vr["case_id"], "REPORT_VOIDED_BY_MERGE",
                                    f"报告 {vr['regulatory_number']} 因重复病例合并作废（未发送）",
                                    {"report_id": vr["id"], "regulatory_number": vr["regulatory_number"],
                                     "survivor_case_id": survivor, "cluster_id": cluster_id},
                                    report_id=vr["id"], cluster_id=cluster_id, actor=actor)
                        # 镜像到存活病例时间线：合并后只在 survivor 工作台即可追溯全部
                        self._event(conn, survivor, "REPORT_VOIDED_BY_MERGE",
                                    f"被合并病例报告 {vr['regulatory_number']} 作废（重复上报，未发送）",
                                    {"report_id": vr["id"], "regulatory_number": vr["regulatory_number"],
                                     "merged_from_case": vr["case_id"],
                                     "cluster_id": cluster_id},
                                    report_id=vr["id"], cluster_id=cluster_id, actor=actor)

                    for ms in memberships:
                        if ms["case_id"] != survivor:
                            cur = conn.execute(
                                "SELECT id FROM case_membership WHERE id=? AND active=1",
                                (ms["id"],)).fetchone()
                            if not cur:   # 并发：来源已被其他操作转移
                                raise DomainError("MEMBERSHIP_CHANGED",
                                                  f"来源归属已变化: {ms['source_id']}", 409)
                            conn.execute("UPDATE case_membership SET active=0 WHERE id=?",
                                         (ms["id"],))
                            conn.execute(
                                "INSERT INTO case_membership(id,source_id,case_id,active,"
                                "reason,actor,created_at) VALUES(?,?,?,1,'MERGE',?,?)",
                                (_uid(), ms["source_id"], survivor, actor,
                                 self.clock.now_iso()))

                    # 血缘闭包：合并病例的每个祖先 -> survivor 的每个后代，
                    # 深度 = depth(祖先,m)+1(合并边)+depth(survivor,后代)
                    for m in merged_from:
                        conn.execute(
                            "UPDATE cases SET status='MERGED', superseded_by_id=? WHERE id=?",
                            (survivor, m))
                        anc = conn.execute(
                            "SELECT ancestor_id, depth FROM case_lineage "
                            "WHERE descendant_id=?", (m,)).fetchall()
                        desc = conn.execute(
                            "SELECT descendant_id, depth FROM case_lineage "
                            "WHERE ancestor_id=?", (survivor,)).fetchall()
                        for a, da in anc:
                            for d, dd in desc:
                                conn.execute(
                                    "INSERT INTO case_lineage(ancestor_id,descendant_id,depth)"
                                    " VALUES(?,?,?) ON CONFLICT(ancestor_id,descendant_id) "
                                    "DO UPDATE SET depth=MIN(case_lineage.depth, excluded.depth)",
                                    (a, d, da + 1 + dd))

                    vid, vno, planned = self._new_version(
                        conn, survivor, "MERGE", merged_snap,
                        f"合并 {len(merged_from)} 个重复病例: {','.join(m[:8] for m in merged_from)}",
                        self.clock.now_iso()[:10], prior_snapshot=prior_snap,
                        actor=actor, plan=True,
                        change_summary=[{"merged_from": merged_from,
                                         "cluster_id": cluster_id}])
                    conn.execute(
                        "UPDATE duplicate_clusters SET status='CONFIRMED',decided_at=?,"
                        "decided_by=? WHERE id=?",
                        (self.clock.now_iso(), actor, cluster_id))
                    for m in members:
                        self._event(conn, m if m != survivor else survivor,
                                    "DUPLICATE_CONFIRMED" if m == survivor else "CASE_MERGED_AWAY",
                                    f"专员确认重复簇，存活病例 {survivor[:8]}"
                                    if m == survivor else f"病例合并入 {survivor[:8]}，原始记录保留",
                                    {"cluster_id": cluster_id, "survivor": survivor,
                                     "merged_from": merged_from},
                                    version_id=vid if m == survivor else None,
                                    cluster_id=cluster_id, actor=actor)
                    # 归并：评审中的会签单引用的归并候选/待发送决定已变化，立即失效
                    revmod.bump_revision(
                        conn, revmod.REV_MERGE,
                        detail={"cluster_id": cluster_id, "survivor": survivor,
                                "merged_from": merged_from},
                        actor=actor, now_iso=self.clock.now_iso())
                return {"survivor_case_id": survivor, "merged_from": merged_from,
                        "version_no": vno, "report": planned, "cluster_id": cluster_id,
                        "voided_reports": [{"id": v["id"],
                                            "regulatory_number": v["regulatory_number"]}
                                           for v in voided]}
            finally:
                conn.close()

    def reject_cluster(self, cluster_id: str, reason: str | None = None,
                       actor: str | None = None) -> dict:
        with self._lock:
            conn = self._conn()
            try:
                with dbmod.transaction(conn):
                    row = conn.execute("SELECT status FROM duplicate_clusters WHERE id=?",
                                       (cluster_id,)).fetchone()
                    if not row:
                        raise DomainError("CLUSTER_NOT_FOUND", "疑似重复簇不存在", 404)
                    if row["status"] != "PROPOSED":
                        raise DomainError("CLUSTER_NOT_OPEN",
                                          f"簇状态为 {row['status']}", 409)
                    conn.execute(
                        "UPDATE duplicate_clusters SET status='REJECTED',decided_at=?,"
                        "decided_by=?,reject_reason=? WHERE id=?",
                        (self.clock.now_iso(), actor, reason, cluster_id))
                    members = [r["case_id"] for r in conn.execute(
                        "SELECT case_id FROM cluster_members WHERE cluster_id=?",
                        (cluster_id,))]
                    for m in members:
                        self._event(conn, m, "DUPLICATE_REJECTED",
                                    f"专员否疑似重复簇 {cluster_id[:8]}: {reason or ''}",
                                    {"cluster_id": cluster_id, "reason": reason},
                                    cluster_id=cluster_id, actor=actor)
                return {"cluster_id": cluster_id, "status": "REJECTED"}
            finally:
                conn.close()

    # -- 3) 拆分 -------------------------------------------------------------
    def split_source(self, case_id: str, source_id: str, actor: str | None = None) -> dict:
        with self._lock:
            conn = self._conn()
            try:
                with dbmod.transaction(conn):
                    case = conn.execute("SELECT * FROM cases WHERE id=?",
                                        (case_id,)).fetchone()
                    if not case:
                        raise DomainError("CASE_NOT_FOUND", "病例不存在", 404)
                    if case["status"] != "OPEN":
                        raise DomainError("CASE_NOT_OPEN",
                                          f"病例状态 {case['status']}，不能拆分", 409)
                    ms = conn.execute(
                        "SELECT * FROM case_membership WHERE source_id=? AND active=1",
                        (source_id,)).fetchone()
                    if not ms or ms["case_id"] != case_id:
                        raise DomainError("SOURCE_NOT_IN_CASE",
                                          "该来源不归属此病例（可能已被并发操作转移）", 409)
                    src = conn.execute("SELECT * FROM source_records WHERE id=?",
                                       (source_id,)).fetchone()

                    new_case = _uid()
                    conn.execute("INSERT INTO cases(id,status,created_at) VALUES(?, 'OPEN', ?)",
                                 (new_case, self.clock.now_iso()))
                    conn.execute("UPDATE case_membership SET active=0 WHERE id=?", (ms["id"],))
                    conn.execute(
                        "INSERT INTO case_membership(id,source_id,case_id,active,reason,actor,"
                        "created_at) VALUES(?,?,?,1,'SPLIT',?,?)",
                        (_uid(), source_id, new_case, actor, self.clock.now_iso()))
                    # 血缘：父病例祖先 -> 新病例
                    conn.execute("INSERT INTO case_lineage(ancestor_id,descendant_id,depth)"
                                 " VALUES(?,?,0)", (new_case, new_case))
                    for r in conn.execute("SELECT ancestor_id,depth FROM case_lineage "
                                          "WHERE descendant_id=? AND ancestor_id<>?",
                                          (case_id, case_id)):
                        conn.execute("INSERT OR IGNORE INTO case_lineage(ancestor_id,"
                                     "descendant_id,depth) VALUES(?,?,?)",
                                     (r["ancestor_id"], new_case, r["depth"] + 1))
                    conn.execute("INSERT OR IGNORE INTO case_lineage(ancestor_id,descendant_id,"
                                 "depth) VALUES(?,?,1)", (case_id, new_case))

                    raw = json.loads(src["payload_json"])
                    snap = _build_snapshot(raw)
                    vid, vno, planned = self._new_version(
                        conn, new_case, "INTAKE", snap,
                        f"从病例 {case_id[:8]} 拆分出的独立病例",
                        src["received_at"][:10], actor=actor, plan=True)
                    # 父病例产生一个 SPLIT 版本（快照不变 -> 不触发报送）
                    parent_vid, parent_no, _ = self._new_version(
                        conn, case_id, "SPLIT", _latest_snapshot(conn, case_id),
                        f"拆分来源 {src['source_system']}/{src['external_id']} 至新病例",
                        self.clock.now_iso()[:10], actor=actor, plan=False,
                        change_summary=[{"split_source": source_id, "new_case": new_case}])
                    self._event(conn, case_id, "CASE_SPLIT",
                                f"来源 {source_id[:8]} 拆分至新病例 {new_case[:8]}",
                                {"source_id": source_id, "new_case_id": new_case},
                                version_id=parent_vid, actor=actor)
                    self._event(conn, new_case, "CASE_SPLIT_IN",
                                f"由病例 {case_id[:8]} 拆分建立",
                                {"parent_case_id": case_id, "source_id": source_id},
                                version_id=vid, actor=actor)
                    # 拆解：评审中的会签单立即失效
                    revmod.bump_revision(
                        conn, revmod.REV_SPLIT,
                        detail={"parent_case_id": case_id, "new_case_id": new_case,
                                "source_id": source_id},
                        actor=actor, now_iso=self.clock.now_iso())
                return {"new_case_id": new_case, "version_no": vno, "report": planned}
            finally:
                conn.close()

    # -- 4) 随访材料（新版本更正，不覆盖）------------------------------------
    def add_followup(self, case_id: str, content: dict,
                     received_at: str | None = None, actor: str | None = None) -> dict:
        with self._lock:
            conn = self._conn()
            try:
                with dbmod.transaction(conn):
                    case = conn.execute("SELECT * FROM cases WHERE id=?",
                                        (case_id,)).fetchone()
                    if not case:
                        raise DomainError("CASE_NOT_FOUND", "病例不存在", 404)
                    if case["status"] != "OPEN":
                        raise DomainError("CASE_NOT_OPEN",
                                          f"病例已 {case['status']}，随访应挂到存活病例", 409)
                    received = received_at or self.clock.now_iso()
                    mat_id = _uid()
                    conn.execute(
                        "INSERT INTO followup_materials(id,case_id,received_at,content_json,"
                        "actor,created_at) VALUES(?,?,?,?,?,?)",
                        (mat_id, case_id, received, _json(content), actor,
                         self.clock.now_iso()))
                    prior = _latest_version(conn, case_id)
                    prior_snap = json.loads(prior["snapshot_json"])
                    new_snap = _apply_followup_patch(prior_snap, content)
                    changes = significant_changes(prior_snap, new_snap)
                    vid, vno, planned = self._new_version(
                        conn, case_id, "FOLLOWUP", new_snap,
                        f"随访材料更正（{', '.join(changes) or '无实质评价变化'}）",
                        received[:10], prior_snapshot=prior_snap, material_id=mat_id,
                        actor=actor, plan=True, change_summary=changes)
                    self._event(conn, case_id, "FOLLOWUP_RECEIVED",
                                f"收到随访材料 {mat_id[:8]}",
                                {"material_id": mat_id, "changes": changes,
                                 "applied": new_snap.get("_applied_patch")},
                                version_id=vid, actor=actor)
                    # 新材料入库：严重等级变化/新待发送决定必须以最新修订号重新会签
                    revmod.bump_revision(
                        conn, revmod.REV_NEW_MATERIAL,
                        detail={"case_id": case_id, "material_id": mat_id,
                                "significant_changes": changes},
                        actor=actor, now_iso=self.clock.now_iso())
                return {"case_id": case_id, "material_id": mat_id, "version_no": vno,
                        "significant_changes": changes,
                        "report": planned}
            finally:
                conn.close()

    # -- 规则重新计算（参考数据 / 规则口径变化后重算评价）--------------------
    def recalculate_rules(self, actor: str | None = None) -> dict:
        """对全部 OPEN 病例以其最新版本快照重跑规则链（不新建/不改写报告）。

        用途：listedness 参考表更新或规则参数调整后，让待发送决定与严重
        等级评价重新一致。重算结果逐病例落审计事件，随后推进一次数据
        修订号 —— 评审中的会签单立即失效，须引用新修订号重新发起。
        已分配监管编号的报告行不动（编号安全），重算只影响“待发送决定”
        的评价依据，供新评审单冻结。
        """
        with self._lock:
            conn = self._conn()
            try:
                with dbmod.transaction(conn):
                    results = []
                    cases = conn.execute(
                        "SELECT id FROM cases WHERE status='OPEN' ORDER BY created_at,id"
                    ).fetchall()
                    for c in cases:
                        case_id = c["id"]
                        latest = _latest_version(conn, case_id)
                        if not latest:
                            continue
                        snap = json.loads(latest["snapshot_json"])
                        prev = conn.execute(
                            "SELECT snapshot_json FROM case_versions WHERE case_id=? "
                            "AND version_no=?", (case_id, latest["version_no"] - 1)
                        ).fetchone()
                        prior_snap = json.loads(prev["snapshot_json"]) if prev else None
                        aware = normalize.parse_date(latest["created_at"][:10]) \
                            or self.clock.now().date()
                        ctx = RuleContext(
                            case_id=case_id, snapshot=snap, awareness_date=aware,
                            is_followup=latest["version_no"] > 1,
                            prior_snapshot=prior_snap,
                            is_listed_fn=self.is_listed,
                            material_id=latest["material_id"])
                        decision = self.engine.evaluate(ctx)
                        # 既有排队决定（如有）与新评价的差异，供复核员判断
                        queued = conn.execute(
                            "SELECT id,regulatory_number,expedited,days_due,decision_json "
                            "FROM regulatory_reports WHERE case_version_id=?",
                            (latest["id"],)).fetchone()
                        diff = None
                        if queued:
                            old = json.loads(queued["decision_json"])
                            diff = {
                                "report_id": queued["id"],
                                "regulatory_number": queued["regulatory_number"],
                                "expedited": old.get("expedited") != decision.expedited,
                                "days_due": old.get("days_due") != decision.days_due,
                                "rule_id": old.get("rule_id") != decision.rule_id,
                                "old_rule_id": old.get("rule_id"),
                                "new_rule_id": decision.rule_id,
                            }
                        self._event(conn, case_id, "RULE_RECALCULATED",
                                    f"规则重新计算：{decision.rule_id}，"
                                    f"{'需报送' if decision.should_report else '无需报送'}",
                                    {"decision": decision.as_dict(),
                                     "version_id": latest["id"], "diff": diff},
                                    version_id=latest["id"], actor=actor)
                        results.append({"case_id": case_id,
                                        "version_id": latest["id"],
                                        "rule_id": decision.rule_id,
                                        "should_report": decision.should_report,
                                        "expedited": decision.expedited,
                                        "diff": diff})
                    rev = revmod.bump_revision(
                        conn, revmod.REV_RULE_RECALC,
                        detail={"cases_recalculated": len(results)},
                        actor=actor, now_iso=self.clock.now_iso())
                    return {"revision": rev, "cases_recalculated": len(results),
                            "results": results}
            finally:
                conn.close()

    # -- 查询：工作台 / 详情 / 时间线 ----------------------------------------
    def _active_source_ids(self, conn, case_id) -> list[str]:
        return [r["source_id"] for r in conn.execute(
            "SELECT source_id FROM case_membership WHERE case_id=? AND active=1", (case_id,))]

    def workbench(self) -> list[dict]:
        conn = self._conn()
        try:
            out = []
            for c in conn.execute("SELECT * FROM cases ORDER BY created_at"):
                snap = _latest_snapshot(conn, c["id"])
                sources = [dict(r) for r in conn.execute(
                    "SELECT s.id,s.source_system,s.external_id,s.received_at FROM "
                    "case_membership m JOIN source_records s ON s.id=m.source_id "
                    "WHERE m.case_id=? AND m.active=1 ORDER BY s.received_at", (c["id"],))]
                reports = [dict(r) for r in conn.execute(
                    "SELECT id,report_type,regulatory_number,status,expedited,due_date,"
                    "followup_of_id FROM regulatory_reports WHERE case_id=? ORDER BY created_at",
                    (c["id"],))]
                clusters = [r["id"] for r in conn.execute(
                    "SELECT dc.id FROM duplicate_clusters dc JOIN cluster_members cm "
                    "ON cm.cluster_id=dc.id WHERE cm.case_id=? AND dc.status='PROPOSED'",
                    (c["id"],))]
                out.append({
                    "case_id": c["id"], "status": c["status"],
                    "superseded_by_id": c["superseded_by_id"],
                    "patient": (snap.get("patient") or {}).get("name"),
                    "serious": snap.get("serious"),
                    "drugs": normalize.canonical_drugs(snap),
                    "reactions": normalize.canonical_reactions(snap),
                    "sources": sources,
                    "open_duplicate_clusters": clusters,
                    "reports": reports,
                })
            return out
        finally:
            conn.close()

    def get_case(self, case_id: str) -> dict:
        conn = self._conn()
        try:
            c = conn.execute("SELECT * FROM cases WHERE id=?", (case_id,)).fetchone()
            if not c:
                raise DomainError("CASE_NOT_FOUND", "病例不存在", 404)
            versions = [dict(r) for r in conn.execute(
                "SELECT id,version_no,change_type,snapshot_json,change_summary_json,"
                "material_id,actor,created_at FROM case_versions WHERE case_id=? "
                "ORDER BY version_no", (case_id,))]
            for v in versions:
                v["snapshot"] = json.loads(v.pop("snapshot_json"))
                v["change_summary"] = json.loads(v.pop("change_summary_json"))
            return {"case_id": case_id, "status": c["status"],
                    "superseded_by_id": c["superseded_by_id"],
                    "sources": self._active_source_ids(conn, case_id),
                    "versions": versions,
                    "timeline": self.timeline(case_id, conn)}
        finally:
            conn.close()

    def timeline(self, case_id: str, conn: sqlite3.Connection | None = None) -> list[dict]:
        own = conn is None
        conn = conn or self._conn()
        try:
            # 血缘：当前病例 + 其全部祖先病例的事件（合并历史可追溯）
            ids = [r["ancestor_id"] for r in conn.execute(
                "SELECT ancestor_id FROM case_lineage WHERE descendant_id=? ORDER BY depth DESC",
                (case_id,))]
            if case_id not in ids:
                ids.append(case_id)
            return audit.list_events(conn, ids)
        finally:
            if own:
                conn.close()
