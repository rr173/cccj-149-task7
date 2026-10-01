"""全局操作留痕哈希链（append-only）。

每个审计帧含连续 ``seq``、上一帧 ``prev_hash``（首帧 GENESIS）与本帧
``entry_hash``。帧哈希覆盖全部业务字段，任何一帧被改写/删除/重排都会导致
断链，``independent`` 核对模块重放整条链即可发现。

``idempotency_key`` 唯一：可恢复作业重放、重复请求不产生重复履历。
所有写入必须在调用方已开启的 BEGIN IMMEDIATE 事务内进行。
"""
from __future__ import annotations

import uuid

from .util import GENESIS, loads, sha256_hex

_CHAIN_FIELDS = ("event_type", "actor", "inquiry_id", "draft_id", "submission_id",
                 "job_id", "summary", "payload_json", "prev_hash")


def _frame_hash(frame: dict) -> str:
    return sha256_hex({k: frame[k] for k in _CHAIN_FIELDS})


def append_event(conn, event_type: str, summary: str, *,
                 payload: dict | None = None, actor: str | None = None,
                 inquiry_id: str | None = None, draft_id: str | None = None,
                 submission_id: str | None = None, job_id: str | None = None,
                 idempotency_key: str | None = None,
                 created_at: str | None = None) -> str | None:
    """追加审计帧。带 ``idempotency_key`` 且已存在时返回既有帧 id（不重复留痕）。"""
    if idempotency_key is not None:
        existing = conn.execute(
            "SELECT id FROM audit_events WHERE idempotency_key=?",
            (idempotency_key,)).fetchone()
        if existing:
            return existing["id"]
    seq = conn.execute("SELECT COALESCE(MAX(seq),0)+1 AS n FROM audit_events") \
        .fetchone()["n"]
    prev = conn.execute("SELECT entry_hash FROM audit_events WHERE seq=?",
                        (seq - 1,)).fetchone()
    prev_hash = prev["entry_hash"] if prev else GENESIS
    import json
    frame = {
        "event_type": event_type, "actor": actor, "inquiry_id": inquiry_id,
        "draft_id": draft_id, "submission_id": submission_id, "job_id": job_id,
        "summary": summary,
        "payload_json": json.dumps(payload or {}, ensure_ascii=False, sort_keys=True,
                                   default=str),
        "prev_hash": prev_hash,
    }
    entry_hash = _frame_hash(frame)
    eid = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO audit_events(id,seq,event_type,actor,inquiry_id,draft_id,"
        "submission_id,job_id,summary,payload_json,idempotency_key,prev_hash,"
        "entry_hash,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (eid, seq, event_type, actor, inquiry_id, draft_id, submission_id, job_id,
         summary, frame["payload_json"], idempotency_key, prev_hash, entry_hash,
         created_at or _now()))
    return eid


def _now() -> str:
    from .util import utc_now_iso
    return utc_now_iso()


def list_events(conn, *, inquiry_id: str | None = None, event_type: str | None = None,
                limit: int | None = None) -> list[dict]:
    where, params = [], []
    if inquiry_id:
        where.append("inquiry_id=?")
        params.append(inquiry_id)
    if event_type:
        where.append("event_type=?")
        params.append(event_type)
    sql = "SELECT * FROM audit_events"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY seq"
    if limit:
        sql += " LIMIT %d" % int(limit)
    out = []
    for r in conn.execute(sql, params):
        d = dict(r)
        d["payload"] = loads(d.pop("payload_json"), {})
        out.append(d)
    return out


def verify_chain(events: list[dict]) -> list[str]:
    """重放哈希链，返回全部断点（空列表=完好）。供独立核对模块复用。"""
    failures: list[str] = []
    prev_hash = GENESIS
    for i, e in enumerate(events, start=1):
        tag = f"audit seq={e.get('seq')}"
        if e.get("seq") != i:
            failures.append(f"{tag}: 序号断号/重号（期望 {i}）")
        if e.get("prev_hash") != prev_hash:
            failures.append(f"{tag}: prev_hash 不匹配（链断裂或帧被删改）")
        import json
        payload_json = e.get("payload_json")
        if payload_json is None:
            payload_json = json.dumps(e.get("payload") or {}, ensure_ascii=False,
                                      sort_keys=True, default=str)
        frame = {
            "event_type": e.get("event_type"), "actor": e.get("actor"),
            "inquiry_id": e.get("inquiry_id"), "draft_id": e.get("draft_id"),
            "submission_id": e.get("submission_id"), "job_id": e.get("job_id"),
            "summary": e.get("summary"), "payload_json": payload_json,
            "prev_hash": e.get("prev_hash"),
        }
        missing = [k for k in _CHAIN_FIELDS if k not in frame]
        if missing:
            failures.append(f"{tag}: 缺少字段 {missing}")
        elif _frame_hash(frame) != e.get("entry_hash"):
            failures.append(f"{tag}: entry_hash 不匹配（帧内容被篡改）")
        prev_hash = e.get("entry_hash") or prev_hash
    return failures
