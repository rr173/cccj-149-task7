"""问询登记与底稿（精确版本锁定 / 候选 / 章节协作 / 附件）。

- ``register_inquiry`` 冻结问题清单、截止时间与可引用资料范围（之后不可改）。
- ``lock_draft`` 把每个引用解析成 *精确版本* 的 draft_pins：资料缺失、引用
  越权、被锁版本之间事实矛盾都直接阻止锁定，并逐项列出未满足项。
- 组稿期间注册了资料新版本，``spawn_drift_candidates`` 为受影响的开放底稿
  生成带原因的 CANDIDATE（复制已写章节/附件），当前底稿的 pins 绝不被改写。
- 章节提交采用乐观修订号：必须带调用方看到的 expected_revision，过期提交
  不覆盖，并落 section_conflicts 留痕。
"""
from __future__ import annotations

import re
import uuid

from . import audit, materials
from .errors import (DraftSealedError, MissingMaterialsError, NotFoundError,
                     OutOfScopeError, StaleRevisionError)
from .util import loads, sha256_raw

CITATION_RE = re.compile(r"\[(CASE|SENT_REPORT|REG|STAT):([^\]\s@]+)(?:@([^\]\s]+))?\]")


# ---------------------------------------------------------------------------
# 问询登记
# ---------------------------------------------------------------------------
def register_inquiry(conn, *, regulator: str, jurisdiction: str, questions: list[dict],
                     deadline: str, allowed_material_ids: list[str] | None = None,
                     allowed_categories: list[str] | None = None,
                     title: str = "", registered_by: str | None = None,
                     created_at: str | None = None,
                     inquiry_id: str | None = None) -> dict:
    qids = [q["qid"] for q in questions]
    if len(qids) != len(set(qids)):
        raise ValueError("问题清单 qid 必须唯一")
    import json
    iid = inquiry_id or uuid.uuid4().hex
    now = created_at or _now()
    scope = {"allowed_material_ids": list(allowed_material_ids or []),
             "allowed_categories": list(allowed_categories or materials.CATEGORIES)}
    conn.execute(
        "INSERT INTO inquiries(id,regulator,jurisdiction,title,questions_json,"
        "deadline,scope_json,status,registered_by,created_at)"
        " VALUES(?,?,?,?,?,?,?,'OPEN',?,?)",
        (iid, regulator, jurisdiction, title,
         json.dumps(questions, ensure_ascii=False, sort_keys=True, default=str),
         deadline, json.dumps(scope, ensure_ascii=False, sort_keys=True),
         registered_by, now))
    audit.append_event(conn, "INQUIRY_REGISTERED",
                       f"问询登记 {regulator}/{jurisdiction}（{len(questions)} 问，"
                       f"截止 {deadline}）",
                       payload={"regulator": regulator, "jurisdiction": jurisdiction,
                                "questions": questions, "deadline": deadline,
                                "scope": scope, "title": title},
                       actor=registered_by, inquiry_id=iid, created_at=now)
    d = get_inquiry(conn, iid)
    d["deduped"] = False
    return d


def get_inquiry(conn, inquiry_id: str) -> dict:
    r = conn.execute("SELECT * FROM inquiries WHERE id=?", (inquiry_id,)).fetchone()
    if not r:
        raise NotFoundError(f"问询不存在: {inquiry_id}")
    return _inquiry_row(r)


def _inquiry_row(r) -> dict:
    d = dict(r)
    d["questions"] = loads(d.pop("questions_json"), [])
    d["scope"] = loads(d.pop("scope_json"), {})
    return d


def list_inquiries(conn) -> list[dict]:
    return [_inquiry_row(r) for r in conn.execute(
        "SELECT * FROM inquiries ORDER BY created_at")]


# ---------------------------------------------------------------------------
# 底稿锁定
# ---------------------------------------------------------------------------
def _check_scope(inquiry: dict, mat: dict) -> str | None:
    """返回越权原因；None=在范围内。范围在问询登记时冻结。"""
    scope = inquiry["scope"]
    allowed_ids = set(scope.get("allowed_material_ids") or [])
    allowed_cats = set(scope.get("allowed_categories") or [])
    if mat["material_id"] not in allowed_ids:
        return f"资料 {mat['material_id']} 不在可引用资料范围内"
    if mat["category"] not in allowed_cats:
        return f"资料类别 {mat['category']} 不在可引用类别范围内"
    return None


def pin_ref(ref: str) -> tuple[str, str, str | None]:
    """解析冻结引用锚 CASE:MAT-1@3 -> (category, material_id, version)。"""
    cat, rest = ref.split(":", 1) if ":" in ref else (ref, ref)
    if "@" in rest:
        mid, ver = rest.split("@", 1)
    else:
        mid, ver = rest, None
    category = materials.REF_PREFIX.get(cat, cat)
    return category, mid, ver


def make_ref(mat: dict) -> str:
    prefix = materials.CATEGORY_TO_PREFIX[mat["category"]]
    return f"{prefix}:{mat['material_id']}@{mat['version']}"


def detect_contradictions(pins: list[dict]) -> list[dict]:
    """同 fact_key 出现不同 fact_value（忽略大小写/首尾空白后比较）即矛盾。"""
    by_fact: dict[str, list[dict]] = {}
    for p in pins:
        if p.get("locked_fact_key"):
            by_fact.setdefault(p["locked_fact_key"], []).append(p)
    out = []
    for fk, group in by_fact.items():
        values = {("" if p.get("locked_fact_value") is None
                   else str(p["locked_fact_value"]).strip().casefold()): p
                  for p in group}
        if len(values) > 1:
            ps = group
            for i in range(len(ps)):
                for j in range(i + 1, len(ps)):
                    a, b = ps[i], ps[j]
                    va = "" if a.get("locked_fact_value") is None \
                        else str(a["locked_fact_value"]).strip().casefold()
                    vb = "" if b.get("locked_fact_value") is None \
                        else str(b["locked_fact_value"]).strip().casefold()
                    if va != vb:
                        out.append({"fact_key": fk,
                                    "ref_a": a["ref"], "value_a": a["locked_fact_value"],
                                    "ref_b": b["ref"], "value_b": b["locked_fact_value"]})
    return out


def _record_contradictions(conn, draft_id: str, found: list[dict], now: str) -> None:
    for c in found:
        conn.execute(
            "INSERT OR IGNORE INTO contradictions(id,draft_id,fact_key,ref_a,value_a,"
            "ref_b,value_b,detected_at) VALUES(?,?,?,?,?,?,?,?)",
            (uuid.uuid4().hex, draft_id, c["fact_key"], c["ref_a"],
             str(c["value_a"]), c["ref_b"], str(c["value_b"]), now))


def lock_draft(conn, inquiry_id: str, refs: list[str], *,
               kind: str = "BASE", parent_draft_id: str | None = None,
               correction_of_submission_id: str | None = None,
               drift_reason: str | None = None,
               material_changes: list[dict] | None = None,
               created_by: str | None = None, created_at: str | None = None,
               draft_id: str | None = None, clone_from_draft_id: str | None = None,
               audit_type: str = "DRAFT_LOCKED",
               audit_summary: str | None = None,
               ref_remap: dict | None = None) -> dict:
    """锁定底稿。refs 为冻结锚列表；失败（缺失/越权/矛盾）不落任何半成品。

    幂等：BASE 底稿对同一问询只创建一个（重复锁定返回既有 OPEN BASE）。
    clone_from_draft_id：候选/更正创建时复制来源底稿的章节历史与附件。
    """
    inquiry = get_inquiry(conn, inquiry_id)

    if kind == "BASE" and parent_draft_id is None and not clone_from_draft_id:
        existing = conn.execute(
            "SELECT * FROM drafts WHERE inquiry_id=? AND kind='BASE' "
            "AND parent_draft_id IS NULL ORDER BY locked_at",
            (inquiry_id,)).fetchone()
        if existing:
            # 重复锁定：即使引用集合不同（如资料已有新版本），也不静默创建第二
            # 个 BASE——调用方应在既有 BASE 与已生成的漂移候选之间显式选择。
            return get_draft(conn, existing["id"])

    # 1) 解析所有引用 -> 精确版本（缺失/越权逐项收集，不静默跳过）
    missing: list[dict] = []
    out_of_scope: list[dict] = []
    resolved: list[dict] = []
    seen = set()
    for ref in refs:
        category, mid, ver = pin_ref(ref)
        if (mid, ver) in seen:
            continue
        seen.add((mid, ver))
        mat = materials.resolve(conn, mid, ver)
        if mat is None:
            missing.append({"ref": ref, "material_id": mid,
                            "version": ver, "category": category,
                            "reason": "资料不存在" if ver else "资料无任何版本"})
            continue
        if category and mat["category"] != category:
            out_of_scope.append({"ref": ref, "material_id": mid,
                                 "version": mat["version"],
                                 "reason": f"引用类别 {category} 与实际类别 "
                                           f"{mat['category']} 不符"})
            continue
        scope_reason = _check_scope(inquiry, mat)
        if scope_reason:
            out_of_scope.append({"ref": make_ref(mat), "material_id": mid,
                                 "version": mat["version"],
                                 "reason": scope_reason})
            continue
        resolved.append(mat)

    if missing:
        raise MissingMaterialsError("底稿锁定被阻止：存在缺失资料",
                                    details={"missing": missing})
    if out_of_scope:
        raise OutOfScopeError("底稿锁定被阻止：引用越权",
                              details={"out_of_scope": out_of_scope})

    # 2) 版本之间事实矛盾检查：锁定即阻断（不得把已知矛盾锁进底稿）
    pin_dicts = [{"ref": make_ref(m), "locked_fact_key": m["fact_key"],
                  "locked_fact_value": m["fact_value"]} for m in resolved]
    contradictions = detect_contradictions(pin_dicts)
    if contradictions:
        from .errors import ContradictionError
        raise ContradictionError(
            "底稿锁定被阻止：被引用版本在事实上互相矛盾（%d 处）"
            % len(contradictions),
            details={"contradictions": contradictions})

    # 克隆附件溯源重映射：来源 pin -> 本草 pin（按逻辑资料 id 对齐），
    # 使“病例 v1 的附件”在候选/更正里自动指向“病例 v2 的溯源锚”。
    if ref_remap is None and clone_from_draft_id:
        ref_remap = {}
        for p in list_pins(conn, clone_from_draft_id):
            new = next((make_ref(m) for m in resolved
                        if m["material_id"] == p["material_id"]
                        and make_ref(m) != p["ref"]), None)
            if new:
                ref_remap[p["ref"]] = new

    now = created_at or _now()
    did = draft_id or uuid.uuid4().hex
    conn.execute(
        "INSERT INTO drafts(id,inquiry_id,kind,status,parent_draft_id,"
        "correction_of_submission_id,drift_reason,"
        "created_from_material_changes_json,locked_at,created_by)"
        " VALUES(?,?,?,'OPEN',?,?,?,? ,?,?)",
        (did, inquiry_id, kind, parent_draft_id, correction_of_submission_id,
         drift_reason,
         __import__("json").dumps(material_changes or [], ensure_ascii=False,
                                  sort_keys=True, default=str),
         now, created_by))
    for m in resolved:
        conn.execute(
            "INSERT INTO draft_pins(id,draft_id,material_id,material_row_id,version,"
            "category,ref,content_hash,locked_fact_key,locked_fact_value)"
            " VALUES(?,?,?,?,?,?,?,?,?,?)",
            (uuid.uuid4().hex, did, m["material_id"], m["id"], m["version"],
             m["category"], make_ref(m), m["content_hash"], m["fact_key"],
             m["fact_value"]))
    _record_contradictions(conn, did, contradictions, now)

    # 3) 克隆来源底稿的章节（含历史）与附件（独立锚 id，附件锚保留）。
    # ref_remap：候选/更正替换了资料版本时，把克隆附件的溯源锚改指新版本。
    if clone_from_draft_id:
        _clone_sections_and_attachments(conn, clone_from_draft_id, did, now,
                                        created_by, ref_remap=ref_remap)

    audit.append_event(conn, audit_type,
                       audit_summary or f"底稿锁定 {kind}（{len(resolved)} 个精确版本）",
                       payload={"draft_id": did, "kind": kind,
                                "refs": [make_ref(m) for m in resolved],
                                "parent_draft_id": parent_draft_id,
                                "correction_of_submission_id":
                                    correction_of_submission_id,
                                "drift_reason": drift_reason,
                                "material_changes": material_changes,
                                "contradictions": contradictions},
                       actor=created_by, inquiry_id=inquiry_id, draft_id=did,
                       created_at=now)
    return get_draft(conn, did)


def _clone_sections_and_attachments(conn, src_draft_id: str, dst_draft_id: str,
                                    now: str, actor: str | None,
                                    ref_remap: dict | None = None) -> None:
    for s in conn.execute("SELECT * FROM sections WHERE draft_id=? ORDER BY rowid",
                          (src_draft_id,)):
        nsid = uuid.uuid4().hex
        history = conn.execute(
            "SELECT * FROM section_revisions WHERE section_id=? ORDER BY revision",
            (s["id"],)).fetchall()
        # 新底稿沿用来源当前修订号：克隆内容就是调用方“看到的修订”，
        # 首次提交必须带 expected_revision=该修订号（与来源当前版本一致），
        # 历史修订原样复制，新版本号在其之后递增，无重号。
        conn.execute(
            "INSERT INTO sections(id,draft_id,qid,title,body,revision,updated_by,"
            "updated_at) VALUES(?,?,?,?,?,?,?,?)",
            (nsid, dst_draft_id, s["qid"], s["title"], s["body"], s["revision"],
             actor, now))
        for rv in history:
            conn.execute(
                "INSERT INTO section_revisions(id,section_id,revision,body,title,"
                "submitted_by,submitted_at) VALUES(?,?,?,?,?,?,?)",
                (uuid.uuid4().hex, nsid, rv["revision"], rv["body"], rv["title"],
                 rv["submitted_by"], rv["submitted_at"]))
        # 克隆附件：附件锚 id（filename 之外的稳定 id）保留，便于正文引用延续
    for a in conn.execute("SELECT * FROM attachments WHERE draft_id=? ORDER BY rowid",
                          (src_draft_id,)):
        derived = a["derived_from_ref"]
        if ref_remap and derived in ref_remap:
            derived = ref_remap[derived]
        conn.execute(
            "INSERT INTO attachments(id,draft_id,filename,content_type,content_blob,"
            "sha256,derived_from_ref,uploaded_by,created_at)"
            " VALUES(?,?,?,?,?,?,?,?,?)",
            (a["id"], dst_draft_id, a["filename"], a["content_type"],
             a["content_blob"], a["sha256"], derived, actor, now))


def get_draft(conn, draft_id: str, *, include: bool = True) -> dict:
    r = conn.execute("SELECT * FROM drafts WHERE id=?", (draft_id,)).fetchone()
    if not r:
        raise NotFoundError(f"底稿不存在: {draft_id}")
    d = dict(r)
    d["material_changes"] = loads(d.pop("created_from_material_changes_json"), [])
    if include:
        d["pins"] = list_pins(conn, draft_id)
        d["sections"] = list_sections(conn, draft_id)
        d["attachments"] = list_attachments(conn, draft_id)
    return d


def list_pins(conn, draft_id: str) -> list[dict]:
    return [dict(r) for r in conn.execute(
        "SELECT * FROM draft_pins WHERE draft_id=? ORDER BY ref", (draft_id,))]


def list_drafts(conn, inquiry_id: str) -> list[dict]:
    out = []
    for r in conn.execute("SELECT * FROM drafts WHERE inquiry_id=? ORDER BY locked_at",
                          (inquiry_id,)):
        d = dict(r)
        d["material_changes"] = loads(d.pop("created_from_material_changes_json"), [])
        out.append(d)
    return out


# ---------------------------------------------------------------------------
# 漂移 -> 带原因的新底稿候选
# ---------------------------------------------------------------------------
def spawn_drift_candidates(conn, *, material_row_id: str, created_at: str | None = None
                           ) -> list[str]:
    """资料新版本注册后：为所有 pin 了该资料旧版本的开放底稿生成候选。

    - 当前底稿的 pins 一个字节都不改（引用变化绝不悄悄混入）；
    - 候选 pin 住 *新版本* 并克隆章节/附件，携带 drift_reason 与变化明细；
    - 同一来源底稿针对同一新版本只生成一次候选（幂等）。
    返回新建候选 draft_id 列表。
    """
    mat = materials.resolve_row(conn, material_row_id)
    if mat is None:
        return []
    now = created_at or _now()
    # 只从 pin 住该资料旧版本的 *BASE/CORRECTION* 开放底稿派生候选；
    # 既存 CANDIDATE 不再链式派生（同一新版本对一条底稿链只产生一个候选）。
    rows = conn.execute(
        "SELECT dp.* FROM draft_pins dp JOIN drafts d ON d.id=dp.draft_id "
        "WHERE dp.material_id=? AND dp.version<>? AND d.status='OPEN' "
        "AND d.kind IN ('BASE','CORRECTION')",
        (mat["material_id"], mat["version"])).fetchall()
    created: list[str] = []
    for pin in rows:
        src_id = pin["draft_id"]
        reason = f"MATERIAL_CHANGED:{mat['material_id']}@{mat['version']}"
        dup = conn.execute(
            "SELECT id FROM drafts WHERE parent_draft_id=? AND kind='CANDIDATE' "
            "AND drift_reason=?", (src_id, reason)).fetchone()
        if dup:
            continue
        src = get_draft(conn, src_id, include=False)
        # 候选只替换 *发生漂移的那一个* 引用，其余精确版本原样保留，
        # 绝不把无关版本/资料带进候选。
        new_refs = []
        for p in list_pins(conn, src_id):
            if p["material_id"] == mat["material_id"]:
                new_refs.append(make_ref(mat))
            else:
                new_refs.append(p["ref"])
        change = {"material_id": mat["material_id"], "category": mat["category"],
                  "old_version": pin["version"], "new_version": mat["version"],
                  "old_ref": pin["ref"], "new_ref": make_ref(mat)}
        cand = lock_draft(
            conn, src["inquiry_id"], new_refs, kind="CANDIDATE",
            parent_draft_id=src_id,
            drift_reason=f"MATERIAL_CHANGED:{mat['material_id']}@{mat['version']}",
            material_changes=[change], created_by="system", created_at=now,
            clone_from_draft_id=src_id, audit_type="DRAFT_CANDIDATE_SPAWNED",
            audit_summary=(f"引用漂移生成候选：{mat['material_id']} "
                           f"{pin['version']} -> {mat['version']}（当前底稿不变）"))
        conn.execute(
            "INSERT INTO drift_events(id,inquiry_id,source_draft_id,"
            "candidate_draft_id,material_id,old_version,new_version,reason,created_at)"
            " VALUES(?,?,?,?,?,?,?,?,?)",
            (uuid.uuid4().hex, src["inquiry_id"], src_id, cand["id"],
             mat["material_id"], pin["version"], mat["version"],
             f"资料 {mat['material_id']} 在组稿期间发布新版本 {mat['version']}，"
             f"当前底稿维持锁定的 {pin['version']}，另立候选", now))
        created.append(cand["id"])
    return created


# ---------------------------------------------------------------------------
# 章节协作（乐观修订号）
# ---------------------------------------------------------------------------
def record_stale_conflict(conn, draft_id: str, qid: str, expected_revision: int,
                          body: str, attempted_by: str | None, *,
                          now: str | None = None, section_row=None) -> None:
    """在独立事务中记录一次过期章节提交（竞争冲突留痕）。

    业务事务回滚后由编排器调用本函数，保证冲突不丢失；重复记录无害（每次
    过期尝试都应可追溯）。
    """
    now = now or _now()
    if section_row is None:
        section_row = conn.execute(
            "SELECT * FROM sections WHERE draft_id=? AND qid=?",
            (draft_id, qid)).fetchone()
    if section_row is None:
        return
    conn.execute(
        "INSERT INTO section_conflicts(id,section_id,expected_revision,"
        "actual_revision,attempted_by,body_preview,reason,created_at)"
        " VALUES(?,?,?,?,?,?,?,?)",
        (uuid.uuid4().hex, section_row["id"], expected_revision,
         section_row["revision"], attempted_by, (body or "")[:200],
         f"过期提交：基于修订 {expected_revision}，当前已为 "
         f"{section_row['revision']}，拒绝覆盖", now))
    draft = get_draft(conn, draft_id, include=False)
    audit.append_event(conn, "SECTION_CONFLICT",
                       f"章节竞争更新被拒绝 {qid}（期望 rev={expected_revision}，"
                       f"当前 rev={section_row['revision']}）",
                       payload={"draft_id": draft_id, "qid": qid,
                                "expected_revision": expected_revision,
                                "actual_revision": section_row["revision"]},
                       actor=attempted_by, inquiry_id=draft["inquiry_id"],
                       draft_id=draft_id, created_at=now)


def submit_section(conn, draft_id: str, qid: str, body: str, *,
                   expected_revision: int, title: str = "",
                   submitted_by: str | None = None,
                   created_at: str | None = None,
                   section_id: str | None = None) -> dict:
    """提交章节内容。

    前提：expected_revision 必须等于调用方看到的当前修订号；
    过期提交不覆盖新内容，落冲突记录并抛 STALE_REVISION。
    """
    draft = get_draft(conn, draft_id, include=False)
    if draft["status"] != "OPEN":
        raise DraftSealedError(f"底稿 {draft_id} 已{draft['status']}，"
                               "章节不可改；请基于更正件修改")
    inquiry = get_inquiry(conn, draft["inquiry_id"])
    valid_qids = {q["qid"] for q in inquiry["questions"]}
    if qid not in valid_qids:
        raise NotFoundError(f"章节对应的问题 {qid} 不在问询清单中")

    now = created_at or _now()
    s = conn.execute("SELECT * FROM sections WHERE draft_id=? AND qid=?",
                     (draft_id, qid)).fetchone()
    if s is None:
        if expected_revision != 0:
            raise StaleRevisionError(
                f"章节 {qid} 尚不存在，期望修订号应为 0",
                details={"qid": qid, "expected": expected_revision, "actual": 0})
        sid = section_id or uuid.uuid4().hex
        conn.execute(
            "INSERT INTO sections(id,draft_id,qid,title,body,revision,updated_by,"
            "updated_at) VALUES(?,?,?,?,?,1,?,?)",
            (sid, draft_id, qid, title, body, submitted_by, now))
        conn.execute(
            "INSERT INTO section_revisions(id,section_id,revision,body,title,"
            "submitted_by,submitted_at) VALUES(?,?,1,?,?,?,?)",
            (uuid.uuid4().hex, sid, body, title, submitted_by, now))
        audit.append_event(conn, "SECTION_SUBMITTED",
                           f"章节创建 {qid}（rev=1）",
                           payload={"draft_id": draft_id, "qid": qid,
                                    "revision": 1, "length": len(body)},
                           actor=submitted_by, inquiry_id=draft["inquiry_id"],
                           draft_id=draft_id, created_at=now)
        return dict(conn.execute("SELECT * FROM sections WHERE id=?",
                                 (sid,)).fetchone())
    if s["revision"] != expected_revision:
        record_stale_conflict(conn, draft_id, qid, expected_revision, body,
                              submitted_by, now=now, section_row=s)
        raise StaleRevisionError(
            f"章节 {qid} 已被他人更新（当前修订 {s['revision']}）",
            details={"qid": qid, "expected": expected_revision,
                     "actual": s["revision"]})

    new_rev = s["revision"] + 1
    conn.execute(
        "UPDATE sections SET title=?, body=?, revision=?, updated_by=?, updated_at=?"
        " WHERE id=? AND revision=?",
        (title, body, new_rev, submitted_by, now, s["id"], expected_revision))
    conn.execute(
        "INSERT INTO section_revisions(id,section_id,revision,body,title,"
        "submitted_by,submitted_at) VALUES(?,?,?,?,?,?,?)",
        (uuid.uuid4().hex, s["id"], new_rev, body, title, submitted_by, now))
    audit.append_event(conn, "SECTION_SUBMITTED",
                       f"章节更新 {qid}（rev={new_rev}）",
                       payload={"draft_id": draft_id, "qid": qid,
                                "revision": new_rev, "length": len(body)},
                       actor=submitted_by, inquiry_id=draft["inquiry_id"],
                       draft_id=draft_id, created_at=now)
    return dict(conn.execute("SELECT * FROM sections WHERE id=?",
                             (s["id"],)).fetchone())


def list_sections(conn, draft_id: str) -> list[dict]:
    return [dict(r) for r in conn.execute(
        "SELECT * FROM sections WHERE draft_id=? ORDER BY qid", (draft_id,))]


def section_history(conn, section_id: str) -> list[dict]:
    return [dict(r) for r in conn.execute(
        "SELECT * FROM section_revisions WHERE section_id=? ORDER BY revision",
        (section_id,))]


def list_conflicts(conn, draft_id: str | None = None) -> list[dict]:
    if draft_id:
        rows = conn.execute(
            "SELECT sc.*, s.draft_id, s.qid FROM section_conflicts sc "
            "JOIN sections s ON s.id=sc.section_id WHERE s.draft_id=? ORDER BY sc.rowid",
            (draft_id,)).fetchall()
    else:
        rows = conn.execute(
            "SELECT sc.*, s.draft_id, s.qid FROM section_conflicts sc "
            "JOIN sections s ON s.id=sc.section_id ORDER BY sc.rowid").fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# 附件
# ---------------------------------------------------------------------------
def add_attachment(conn, draft_id: str, *, filename: str, content: bytes,
                   content_type: str = "application/octet-stream",
                   derived_from_ref: str | None = None,
                   uploaded_by: str | None = None,
                   created_at: str | None = None,
                   attachment_id: str | None = None) -> dict:
    draft = get_draft(conn, draft_id, include=False)
    if draft["status"] != "OPEN":
        raise DraftSealedError(f"底稿 {draft_id} 已{draft['status']}，不可新增附件")
    if not isinstance(content, (bytes, bytearray)):
        raise ValueError("附件内容必须是 bytes")
    # 附件必须能溯源到本草锁定的精确资料（正文引用一致的同一套锚）
    if derived_from_ref is not None:
        hit = conn.execute(
            "SELECT 1 FROM draft_pins WHERE draft_id=? AND ref=?",
            (draft_id, derived_from_ref)).fetchone()
        if not hit:
            raise OutOfScopeError(
                f"附件溯源 {derived_from_ref} 未锁定在本草底稿中",
                details={"ref": derived_from_ref})
    now = created_at or _now()
    aid = attachment_id or uuid.uuid4().hex
    digest = sha256_raw(content)
    conn.execute(
        "INSERT INTO attachments(id,draft_id,filename,content_type,content_blob,"
        "sha256,derived_from_ref,uploaded_by,created_at)"
        " VALUES(?,?,?,?,?,?,?,?,?)",
        (aid, draft_id, filename, content_type, bytes(content), digest,
         derived_from_ref, uploaded_by, now))
    audit.append_event(conn, "ATTACHMENT_ADDED",
                       f"附件上传 {filename}（{len(content)} 字节）",
                       payload={"draft_id": draft_id, "filename": filename,
                                "attachment_id": aid, "sha256": digest,
                                "derived_from_ref": derived_from_ref},
                       actor=uploaded_by, inquiry_id=draft["inquiry_id"],
                       draft_id=draft_id, created_at=now)
    return dict(conn.execute("SELECT id,draft_id,filename,content_type,sha256,"
                             "derived_from_ref,uploaded_by,created_at,"
                             "length(content_blob) AS size FROM attachments WHERE id=?",
                             (aid,)).fetchone())


def list_attachments(conn, draft_id: str, *, with_content: bool = False) -> list[dict]:
    cols = ("id,draft_id,filename,content_type,sha256,derived_from_ref,uploaded_by,"
            "created_at,length(content_blob) AS size")
    rows = conn.execute(
        f"SELECT {cols}{', content_blob' if with_content else ''} FROM attachments "
        "WHERE draft_id=? ORDER BY filename", (draft_id,)).fetchall()
    return [dict(r) for r in rows]


def get_attachment_content(conn, draft_id: str, attachment_id: str) -> dict:
    r = conn.execute("SELECT * FROM attachments WHERE draft_id=? AND id=?",
                     (draft_id, attachment_id)).fetchone()
    if not r:
        raise NotFoundError(f"附件不存在: {attachment_id}")
    return dict(r)


def extract_citations(text: str) -> list[dict]:
    """提取正文中的全部引用锚（含位置）。"""
    return [{"prefix": m.group(1), "material_id": m.group(2),
             "version": m.group(3), "raw": m.group(0),
             "category": materials.REF_PREFIX[m.group(1)]}
            for m in CITATION_RE.finditer(text or "")]


def _now() -> str:
    from .util import utc_now_iso
    return utc_now_iso()
