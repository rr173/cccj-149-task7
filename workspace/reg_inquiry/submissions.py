"""不可变提交件：定稿封存 / 发送重试 / 回执 / 更正关系。

- ``seal``：定稿校验通过后在 *单个 IMMEDIATE 事务* 内分配外发编号、生成
  不可变清单 manifest（每个问题带唯一 answer_uid 与正文/附件哈希）、提交件
  落库并把底稿置 FINALIZED（同问询其余开放底稿置 SUPERSEDED）。
- 提交件行除发送状态机外不可改（触发器兜底）；之后任何修改只能产生
  CORRECTION 更正件，原提交件与监管回执原样保留。
- 发送是条件 UPDATE 领取的状态机：重复请求/双节点同时发送，只有一方能把
  QUEUED/FAILED 推进到 SENDING；SENT/DELIVERED 直接幂等返回。
"""
from __future__ import annotations

import json
import uuid

from . import audit, drafts, validation
from .errors import DraftSealedError, InquiryError, NotFoundError, ValidationFailed
from .util import sha256_hex, utc_now_iso


# ---------------------------------------------------------------------------
# manifest
# ---------------------------------------------------------------------------
def _answer_uid(draft_id: str, qid: str, section_id: str, revision: int) -> str:
    return sha256_hex([draft_id, qid, section_id, revision])[:20]


def build_manifest(conn, draft: dict, *, submission_id: str, kind: str,
                   corrects_submission_id: str | None, outgoing_number: str,
                   sealed_at: str, policy_version: int | None) -> dict:
    per_q = {}
    for s in draft["sections"]:
        per_q[s["qid"]] = s
    answers = []
    attach_by_id = {a["id"]: a for a in draft["attachments"]}
    for sec in sorted(draft["sections"], key=lambda s: s["qid"]):
        cite_refs = sorted({f"{c['prefix']}:{c['material_id']}@{c['version']}"
                            for c in drafts.extract_citations(
                                sec["body"] + "\n" + sec["title"])
                            if c["version"]})
        attach_refs = validation._extract_attach_refs(sec["body"])
        answers.append({
            "qid": sec["qid"],
            "answer_uid": _answer_uid(draft["id"], sec["qid"], sec["id"],
                                      sec["revision"]),
            "section_id": sec["id"],
            "section_revision": sec["revision"],
            "title": sec["title"],
            "body_sha256": sha256_hex(sec["body"]),
            "citations": cite_refs,
            "attachments": [{
                "attachment_id": aid,
                "filename": attach_by_id[aid]["filename"],
                "sha256": attach_by_id[aid]["sha256"],
                "derived_from_ref": attach_by_id[aid]["derived_from_ref"],
            } for aid in sorted(set(attach_refs)) if aid in attach_by_id],
        })
    return {
        "submission_id": submission_id,
        "inquiry_id": draft["inquiry_id"],
        "kind": kind,
        "corrects_submission_id": corrects_submission_id,
        "outgoing_number": outgoing_number,
        "sealed_at": sealed_at,
        "policy_version": policy_version,
        "pins": sorted([{
            "ref": p["ref"], "material_id": p["material_id"],
            "version": p["version"], "category": p["category"],
            "content_hash": p["content_hash"],
            "fact_key": p["locked_fact_key"],
            "fact_value": p["locked_fact_value"],
        } for p in draft["pins"]], key=lambda p: p["ref"]),
        "answers": answers,
    }


def _allocate_outgoing_number(conn, jurisdiction: str, year: str, now: str) -> str:
    scope = f"{jurisdiction}:{year}"
    # upsert + 条件自增在同一写事务内，SQLite 写锁全局串行 -> 编号不重复
    conn.execute(
        "INSERT INTO outgoing_number_seq(scope,last_no) VALUES(?,0) "
        "ON CONFLICT(scope) DO NOTHING", (scope,))
    row = conn.execute(
        "UPDATE outgoing_number_seq SET last_no=last_no+1 WHERE scope=? "
        "RETURNING last_no", (scope,)).fetchone()
    no = row["last_no"]
    return f"{jurisdiction}-{year}-{no:05d}"


def seal(conn, draft_id: str, *, sealed_by: str | None = None,
         sealed_at: str | None = None) -> dict:
    """定稿：校验 -> 分配编号 -> 不可变 manifest -> 提交件落库（同一事务）。

    调用方须已开启 BEGIN IMMEDIATE 事务。重复定稿已 FINALIZED 的底稿返回
    既有提交件（幂等）；SUPERSEDED 底稿拒绝。
    """
    draft = drafts.get_draft(conn, draft_id)
    now = sealed_at or utc_now_iso()

    existing = conn.execute("SELECT * FROM submissions WHERE draft_id=?",
                            (draft_id,)).fetchone()
    if existing:
        return _submission_dict(existing)
    if draft["status"] == "SUPERSEDED":
        raise DraftSealedError(f"底稿 {draft_id} 已被取代，不能定稿；"
                               "请基于提交件创建更正件")
    if draft["status"] != "OPEN":
        raise DraftSealedError(f"底稿 {draft_id} 状态 {draft['status']}，不能定稿")

    report = validation.build_report(conn, draft_id)
    if not report["ok"]:
        raise ValidationFailed(report)

    inquiry = drafts.get_inquiry(conn, draft["inquiry_id"])
    year = now[:4]

    corrects = draft["correction_of_submission_id"]
    kind = "CORRECTION" if corrects or draft["kind"] == "CORRECTION" else "ORIGINAL"

    sid = uuid.uuid4().hex
    outgoing_number = _allocate_outgoing_number(conn, inquiry["jurisdiction"], year, now)
    manifest = build_manifest(
        conn, draft, submission_id=sid, kind=kind,
        corrects_submission_id=corrects, outgoing_number=outgoing_number,
        sealed_at=now, policy_version=report["policy_version"])
    manifest_json = json.dumps(manifest, ensure_ascii=False, sort_keys=True,
                               default=str)
    manifest_hash = sha256_hex(manifest)

    conn.execute(
        "INSERT INTO submissions(id,draft_id,inquiry_id,kind,"
        "corrects_submission_id,jurisdiction,outgoing_number,status,manifest_json,"
        "manifest_hash,sealed_by,sealed_at,created_at)"
        " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (sid, draft_id, draft["inquiry_id"], kind, corrects,
         inquiry["jurisdiction"], outgoing_number, "SEALED", manifest_json,
         manifest_hash, sealed_by, now, now))

    # 底稿状态机推进（触发器只允许 OPEN -> FINALIZED 且 submission_id 非空）
    conn.execute(
        "UPDATE drafts SET status='FINALIZED', finalized_at=?, submission_id=? "
        "WHERE id=? AND status='OPEN'", (now, sid, draft_id))
    # 同问询其余开放底稿（候选等）一律置 SUPERSEDED；它们的内容仍保留可查
    conn.execute(
        "UPDATE drafts SET status='SUPERSEDED' WHERE inquiry_id=? AND id<>? "
        "AND status='OPEN'", (draft["inquiry_id"], draft_id))

    audit.append_event(conn, "SUBMISSION_SEALED",
                       f"定稿封存 {outgoing_number}（{kind}，"
                       f"{report['answered_count']}/{report['question_count']} 问）",
                       payload={"submission_id": sid, "draft_id": draft_id,
                                "outgoing_number": outgoing_number, "kind": kind,
                                "corrects_submission_id": corrects,
                                "manifest_hash": manifest_hash,
                                "warnings": report["warnings"]},
                       actor=sealed_by, inquiry_id=draft["inquiry_id"],
                       draft_id=draft_id, submission_id=sid,
                       idempotency_key=f"SEAL:{draft_id}", created_at=now)
    return get_submission(conn, sid)


# ---------------------------------------------------------------------------
# 发送（领取式状态机；与外部传输的调用由 orchestrator 放在事务之间）
# ---------------------------------------------------------------------------
def claim_for_sending(conn, submission_id: str, *, now: str | None = None,
                      actor: str | None = None) -> dict | None:
    """条件领取：QUEUED/FAILED/SEALED -> SENDING，attempts+1。

    SENDING（崩溃残留）需先 recover；SENT/DELIVERED/REJECTED 返回 None
    （调用方据此幂等跳过，不产生任何新履历）。
    """
    sub = get_submission(conn, submission_id)
    if sub["status"] in ("SENT", "DELIVERED", "REJECTED"):
        return None
    if sub["status"] == "SENDING":
        return None
    next_attempt = sub["attempts"] + 1
    cur = conn.execute(
        "UPDATE submissions SET status='SENDING', attempts=?, last_error=NULL "
        "WHERE id=? AND status IN ('SEALED','QUEUED','FAILED')",
        (next_attempt, submission_id))
    if cur.rowcount != 1:
        return None
    _add_event(conn, submission_id, "SENDING",
               {"attempt": next_attempt}, now=now, actor=actor)
    # 同事务回读确认：并发双节点时只有一个条件 UPDATE 命中
    fresh = conn.execute("SELECT status FROM submissions WHERE id=?",
                         (submission_id,)).fetchone()
    if fresh is None or fresh["status"] != "SENDING":
        return None
    return get_submission(conn, submission_id)


def mark_sent(conn, submission_id: str, ack_hint: dict | None = None, *,
              now: str | None = None, actor: str | None = None) -> dict:
    """标记 SENT。只在状态确有推进时落履历，避免恢复路径产生重复 SENT。"""
    now = now or utc_now_iso()
    cur = conn.execute(
        "UPDATE submissions SET status='SENT', sent_at=COALESCE(sent_at,?), "
        "last_error=NULL WHERE id=? AND status IN ('SENDING','QUEUED','SEALED')",
        (now, submission_id))
    if cur.rowcount:
        _add_event(conn, submission_id, "SENT", {"ack_hint": ack_hint or {}},
                   now=now, actor=actor)
    return get_submission(conn, submission_id)


def mark_failed(conn, submission_id: str, error_code: str, message: str, *,
                retryable: bool = True, now: str | None = None,
                actor: str | None = None) -> dict:
    now = now or utc_now_iso()
    conn.execute(
        "UPDATE submissions SET status=?, last_error=? WHERE id=?",
        ("FAILED" if retryable else "REJECTED", f"{error_code}: {message}",
         submission_id))
    _add_event(conn, submission_id, "FAILED" if retryable else "REJECTED",
               {"error_code": error_code, "message": message},
               now=now, actor=actor)
    return get_submission(conn, submission_id)


def recover_sending(conn) -> int:
    """崩溃恢复：把卡在 SENDING 的提交件重置回 QUEUED（编号/manifest 不变）。

    传输侧以外发编号为幂等键，重放只取回同一受理结果。
    """
    rows = conn.execute(
        "UPDATE submissions SET status='QUEUED' WHERE status='SENDING'")
    n = rows.rowcount
    if n:
        audit.append_event(conn, "SEND_RECOVERED",
                           f"发送恢复：{n} 个卡在 SENDING 的提交件重置为 QUEUED",
                           payload={"count": n})
    return n


def send_payload(sub: dict) -> dict:
    """构造外发载荷（含 manifest；传输以 outgoing_number 为幂等键）。"""
    return {"outgoing_number": sub["outgoing_number"],
            "inquiry_id": sub["inquiry_id"], "kind": sub["kind"],
            "corrects_outgoing_number": None,
            "manifest_hash": sub["manifest_hash"],
            "manifest": sub["manifest"]}


def _add_event(conn, submission_id: str, event_type: str, detail: dict, *,
               now: str | None = None, actor: str | None = None) -> None:
    # (submission_id,event_type,detail_json) UNIQUE：重放不产生重复履历
    conn.execute(
        "INSERT OR IGNORE INTO submission_events(id,submission_id,event_type,"
        "detail_json,actor,created_at) VALUES(?,?,?,?,?,?)",
        (uuid.uuid4().hex, submission_id, event_type,
         json.dumps(detail or {}, ensure_ascii=False, sort_keys=True, default=str),
         actor, now or utc_now_iso()))


def events(conn, submission_id: str) -> list[dict]:
    return [dict(r) for r in conn.execute(
        "SELECT * FROM submission_events WHERE submission_id=? ORDER BY id",
        (submission_id,))]


# ---------------------------------------------------------------------------
# 回执
# ---------------------------------------------------------------------------
def register_receipt(conn, submission_id: str, *, receipt_number: str,
                     payload: dict, received_at: str | None = None,
                     registered_by: str | None = None) -> dict:
    """登记监管回执（回执号幂等）。交叉核对：回执外发编号必须属于该提交件。"""
    sub = get_submission(conn, submission_id)
    existing = conn.execute("SELECT * FROM receipts WHERE receipt_number=?",
                            (receipt_number,)).fetchone()
    if existing:
        if existing["submission_id"] != submission_id:
            raise InquiryError(
                f"回执 {receipt_number} 已登记于另一提交件 "
                f"{existing['submission_id']}",
                details={"receipt_number": receipt_number,
                         "submission_id": existing["submission_id"]})
        return dict(existing)

    out_no = (payload or {}).get("outgoing_number")
    if out_no and out_no != sub["outgoing_number"]:
        raise InquiryError(
            f"回执外发编号 {out_no} 与提交件 {sub['outgoing_number']} 不一致",
            details={"receipt_number": receipt_number, "expected": sub["outgoing_number"],
                     "actual": out_no})
    now = utc_now_iso()
    rid = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO receipts(id,receipt_number,submission_id,outgoing_number,"
        "regulator,payload_json,received_at,registered_by,created_at)"
        " VALUES(?,?,?,?,?,?,?,?,?)",
        (rid, receipt_number, submission_id, sub["outgoing_number"],
         (payload or {}).get("regulator", ""),
         json.dumps(payload or {}, ensure_ascii=False, sort_keys=True, default=str),
         received_at or now, registered_by, now))
    conn.execute(
        "UPDATE submissions SET status='DELIVERED' WHERE id=? AND status IN "
        "('SENT','DELIVERED')", (submission_id,))
    _add_event(conn, submission_id, "RECEIPT_REGISTERED",
               {"receipt_number": receipt_number}, now=now, actor=registered_by)
    audit.append_event(conn, "RECEIPT_REGISTERED",
                       f"监管回执登记 {receipt_number} -> {sub['outgoing_number']}",
                       payload={"submission_id": submission_id,
                                "receipt_number": receipt_number},
                       actor=registered_by, inquiry_id=sub["inquiry_id"],
                       submission_id=submission_id,
                       idempotency_key=f"RECEIPT:{receipt_number}", created_at=now)
    return dict(conn.execute("SELECT * FROM receipts WHERE id=?", (rid,)).fetchone())


def list_receipts(conn, submission_id: str) -> list[dict]:
    return [dict(r) for r in conn.execute(
        "SELECT * FROM receipts WHERE submission_id=? ORDER BY created_at",
        (submission_id,))]


# ---------------------------------------------------------------------------
# 更正关系
# ---------------------------------------------------------------------------
def create_correction_draft(conn, submission_id: str, *, refs: list[str] | None = None,
                            reason: str = "", created_by: str | None = None,
                            created_at: str | None = None) -> dict:
    """基于已封存提交件创建更正底稿（克隆章节/附件；原提交件永不改变）。

    refs 默认沿用原底稿锁定的精确版本；要纳入新证据时显式给出新锚集合
    （缺失/越权/矛盾仍由 lock_draft 阻断）。
    """
    sub = get_submission(conn, submission_id)
    if sub["status"] not in ("SENT", "DELIVERED", "SEALED", "QUEUED", "FAILED"):
        raise DraftSealedError(f"提交件状态 {sub['status']}，不可创建更正件")
    src_draft = drafts.get_draft(conn, sub["draft_id"], include=False)
    if refs is None:
        refs = [p["ref"] for p in drafts.list_pins(conn, src_draft["id"])]
    now = created_at or utc_now_iso()
    draft = drafts.lock_draft(
        conn, sub["inquiry_id"], refs, kind="CORRECTION",
        parent_draft_id=src_draft["id"], correction_of_submission_id=submission_id,
        drift_reason=f"CORRECTION:{reason or '提交后更正'}",
        created_by=created_by, created_at=now,
        clone_from_draft_id=src_draft["id"],
        audit_type="CORRECTION_DRAFT_CREATED",
        audit_summary=(f"更正底稿创建（更正 {sub['outgoing_number']}：{reason}）"))
    audit.append_event(conn, "CORRECTION_LINKED",
                       f"更正关系：新底稿 -> {sub['outgoing_number']}",
                       payload={"draft_id": draft["id"],
                                "submission_id": submission_id,
                                "outgoing_number": sub["outgoing_number"],
                                "reason": reason},
                       actor=created_by, inquiry_id=sub["inquiry_id"],
                       draft_id=draft["id"], created_at=now)
    return draft


def correction_chain(conn, submission_id: str) -> dict:
    """返回 {original, corrections:[...]}：原始提交件 + 其全部更正件。"""
    sub = get_submission(conn, submission_id)
    root_id = submission_id
    cur = sub
    seen = set()
    while cur["kind"] == "CORRECTION" and cur["corrects_submission_id"] \
            and cur["corrects_submission_id"] not in seen:
        seen.add(cur["id"])
        cur = get_submission(conn, cur["corrects_submission_id"])
        root_id = cur["id"]
    root = get_submission(conn, root_id)
    corrections = [_submission_dict(r) for r in conn.execute(
        "SELECT * FROM submissions WHERE corrects_submission_id=? "
        "ORDER BY sealed_at", (root_id,))]
    return {"original": root, "corrections": corrections}


# ---------------------------------------------------------------------------
def get_submission(conn, submission_id: str) -> dict:
    r = conn.execute("SELECT * FROM submissions WHERE id=?",
                     (submission_id,)).fetchone()
    if not r:
        raise NotFoundError(f"提交件不存在: {submission_id}")
    return _submission_dict(r)


def _submission_dict(r) -> dict:
    d = dict(r)
    d["manifest"] = json.loads(d.pop("manifest_json"))
    return d


def list_submissions(conn, inquiry_id: str) -> list[dict]:
    return [_submission_dict(r) for r in conn.execute(
        "SELECT * FROM submissions WHERE inquiry_id=? ORDER BY sealed_at",
        (inquiry_id,))]
