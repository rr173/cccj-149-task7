"""信号子系统全局审计哈希链（append-only）。

与病例库的 *每病例* 链不同，信号审计是 *全局单链*：seq 连续递增、prev_hash
指向上一帧。帧哈希覆盖事件类型、操作人、摘要、payload、幂等键与前驱哈希，
任何改写/删除/重排都会在重放时暴露为断链。

调用方必须在已开启的 ``BEGIN IMMEDIATE`` 事务内调用 ``append_event``。
"""
from __future__ import annotations

import json
import uuid

from .signal_engine import canon, digest

GENESIS = "GENESIS"

_FIELDS = ("seq", "event_type", "actor", "summary", "payload_json",
           "idempotency_key", "prev_hash")


def append_event(conn, event_type: str, summary: str, *,
                 payload: dict | None = None, actor: str | None = None,
                 idempotency_key: str | None = None,
                 created_at: str) -> str:
    seq = conn.execute("SELECT COALESCE(MAX(seq),0)+1 AS n FROM sig_audit_events"
                       ).fetchone()["n"]
    prev = conn.execute("SELECT entry_hash FROM sig_audit_events WHERE seq=?",
                        (seq - 1,)).fetchone()
    prev_hash = prev["entry_hash"] if prev else GENESIS
    payload_json = json.dumps(payload or {}, ensure_ascii=False,
                              sort_keys=True, default=str)
    eid = uuid.uuid4().hex
    entry = {"seq": seq, "event_type": event_type, "actor": actor,
             "summary": summary, "payload_json": payload_json,
             "idempotency_key": idempotency_key, "prev_hash": prev_hash}
    entry_hash = digest(entry)
    conn.execute(
        "INSERT INTO sig_audit_events(id,seq,event_type,actor,summary,payload_json,"
        "idempotency_key,prev_hash,entry_hash,created_at) "
        "VALUES(?,?,?,?,?,?,?,?,?,?)",
        (eid, seq, event_type, actor, summary, payload_json, idempotency_key,
         prev_hash, entry_hash, created_at))
    return eid


def list_events(conn, *, event_type: str | None = None, limit: int = 500):
    sql = "SELECT * FROM sig_audit_events"
    params = []
    if event_type:
        sql += " WHERE event_type=?"
        params.append(event_type)
    sql += " ORDER BY seq DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(sql, params).fetchall()
    out = [dict(r) for r in rows]
    for d in out:
        d["payload"] = json.loads(d.pop("payload_json"))
    return list(reversed(out))


def verify_chain(conn) -> dict:
    rows = conn.execute("SELECT * FROM sig_audit_events ORDER BY seq").fetchall()
    failures, prev_hash = [], GENESIS
    for i, r in enumerate(rows, start=1):
        tag = f"seq={r['seq']}"
        if r["seq"] != i:
            failures.append(f"{tag}: 序号断号/重号（期望 {i}）")
        if r["prev_hash"] != prev_hash:
            failures.append(f"{tag}: prev_hash 不匹配（链断裂或帧被删改）")
        entry = {"seq": r["seq"], "event_type": r["event_type"], "actor": r["actor"],
                 "summary": r["summary"], "payload_json": r["payload_json"],
                 "idempotency_key": r["idempotency_key"], "prev_hash": r["prev_hash"]}
        if digest(entry) != r["entry_hash"]:
            failures.append(f"{tag}: entry_hash 不匹配（帧内容被篡改）")
        prev_hash = r["entry_hash"]
    return {"valid": not failures, "events": len(rows), "failures": failures,
            "head": rows[-1]["entry_hash"] if rows else GENESIS}
