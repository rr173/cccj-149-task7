"""规则治理审计哈希链（全局只增）。

与病例审计链（``audit.py``，按病例分链）不同，规则治理事件——
登记 / 启用 / 回滚 / 定时 / 试算 / 恢复 / 更正候选——汇成一条全局链：
每帧含连续 ``seq``、上一帧 ``prev_hash``（首帧 GENESIS）与本帧
``entry_hash``。任何一帧被增删改都会导致后续 prev_hash 对不上，
``verify_rule_chain`` 可在不依赖业务状态的情况下逐项核验。

所有追加都在调用方已开启的 ``BEGIN IMMEDIATE`` 事务内完成（seq 分配
与业务结论同生共死）。``idempotency_key`` 有 UNIQUE 约束 + 应用层先查：
同一请求重放（重复启用、恢复续跑）命中既有帧，绝不产生第二条审计事件。
"""
from __future__ import annotations

import hashlib
import json
import uuid

GENESIS = "GENESIS"

_CHAIN_FIELDS = ("seq", "event_type", "actor", "jurisdiction", "summary",
                 "payload_json", "idempotency_key", "prev_hash", "created_at")


def _canonical(entry: dict) -> bytes:
    return json.dumps(entry, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), default=str).encode("utf-8")


def hash_entry(entry: dict) -> str:
    missing = [f for f in _CHAIN_FIELDS if f not in entry]
    if missing:
        raise ValueError(f"治理审计帧缺少字段: {missing}")
    return hashlib.sha256(_canonical(entry)).hexdigest()


def append_event(conn, event_type: str, summary: str, *,
                 payload: dict | None = None, actor: str | None = None,
                 jurisdiction: str | None = None, when: str | None = None,
                 idempotency_key: str | None = None) -> dict:
    """在调用方写事务内追加一帧。

    返回 ``{"id","seq","deduped"}``：``deduped=True`` 表示命中同
    idempotency_key 的既有帧（请求重放，不重复落审计）。
    """
    now = when or __import__("datetime").datetime.now().isoformat(timespec="seconds")
    payload_json = json.dumps(payload or {}, ensure_ascii=False, sort_keys=True,
                              default=str)
    if idempotency_key:
        existing = conn.execute(
            "SELECT id,seq FROM rule_audit_events WHERE idempotency_key=?",
            (idempotency_key,)).fetchone()
        if existing:
            return {"id": existing["id"], "seq": existing["seq"], "deduped": True}
    nxt = conn.execute(
        "SELECT COALESCE(MAX(seq),0)+1 AS n FROM rule_audit_events").fetchone()["n"]
    prev = conn.execute("SELECT entry_hash FROM rule_audit_events WHERE seq=?",
                        (nxt - 1,)).fetchone()
    prev_hash = prev["entry_hash"] if prev else GENESIS
    entry = {
        "seq": nxt, "event_type": event_type, "actor": actor,
        "jurisdiction": jurisdiction, "summary": summary,
        "payload_json": payload_json, "idempotency_key": idempotency_key,
        "prev_hash": prev_hash, "created_at": now,
    }
    entry["entry_hash"] = hash_entry(entry)
    eid = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO rule_audit_events(id,seq,event_type,actor,jurisdiction,summary,"
        "payload_json,idempotency_key,prev_hash,entry_hash,created_at) "
        "VALUES(:id,:seq,:event_type,:actor,:jurisdiction,:summary,:payload_json,"
        ":idempotency_key,:prev_hash,:entry_hash,:created_at)",
        {"id": eid, **entry})
    return {"id": eid, "seq": nxt, "deduped": False}


def list_events(conn, *, event_type: str | None = None,
                jurisdiction: str | None = None, limit: int = 500) -> list[dict]:
    where, params = [], []
    if event_type:
        where.append("event_type=?")
        params.append(event_type)
    if jurisdiction:
        where.append("jurisdiction=?")
        params.append(jurisdiction)
    sql = "SELECT * FROM rule_audit_events"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY seq DESC LIMIT ?"
    params.append(limit)
    out = []
    for r in conn.execute(sql, params):
        d = dict(r)
        d["payload"] = json.loads(d.pop("payload_json"))
        out.append(d)
    return out


class RuleChainBroken(Exception):
    def __init__(self, failures: list[str]):
        super().__init__("; ".join(failures) or "规则治理审计链断裂")
        self.failures = failures


def verify_chain(conn) -> dict:
    """重放全局治理审计链；返回核验报告，失败逐条列出。"""
    rows = conn.execute("SELECT * FROM rule_audit_events ORDER BY seq").fetchall()
    failures: list[str] = []
    prev_hash = GENESIS
    for i, r in enumerate(rows, start=1):
        tag = f"seq={r['seq']}"
        if r["seq"] != i:
            failures.append(f"{tag}: 序号断号/重号（期望 {i}）")
        if r["prev_hash"] != prev_hash:
            failures.append(f"{tag}: prev_hash 不匹配（链断裂或帧被删改）")
        recomputed = hash_entry({
            "seq": r["seq"], "event_type": r["event_type"], "actor": r["actor"],
            "jurisdiction": r["jurisdiction"], "summary": r["summary"],
            "payload_json": r["payload_json"],
            "idempotency_key": r["idempotency_key"],
            "prev_hash": r["prev_hash"], "created_at": r["created_at"],
        })
        if recomputed != r["entry_hash"]:
            failures.append(f"{tag}: entry_hash 不匹配（帧内容被篡改）")
        prev_hash = r["entry_hash"]
    return {"valid": not failures, "events": len(rows),
            "head": rows[-1]["entry_hash"] if rows else GENESIS,
            "failures": failures}
