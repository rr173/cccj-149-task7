"""病例审计哈希链（append-only）。

case_events 是病例时间线的权威来源，同时是一条 *每病例* 的哈希链：
每个事件帧包含连续序号 ``seq``（从 1 开始）、上一帧哈希 ``prev_hash``
（首帧为常量 ``GENESIS``）和本帧哈希 ``entry_hash``。

帧哈希覆盖事件的全部业务字段（含类型、时间、操作人、摘要、payload、
版本/报告/簇引用），因此：
- 任何一帧被改写 / 删除 / 重排，下一帧的 prev_hash 就对不上；
- 证据包在另一进程中无需访问数据库，逐帧重算即可发现断链；
- ``ux_events_case_seq`` 唯一索引在数据库层拒绝断号/重号。

所有写事件的代码必须走 ``append_event``（它依赖调用方已开启的
``BEGIN IMMEDIATE`` 事务：取 max(seq) 与插入必须与业务变更原子提交）。
"""
from __future__ import annotations

import hashlib
import json
import uuid
from typing import Any

GENESIS = "GENESIS"

# 帧哈希覆盖的字段（顺序无关，序列化时 sort_keys）
_CHAIN_FIELDS = ("case_id", "seq", "event_type", "event_time", "actor", "summary",
                 "payload_json", "version_id", "report_id", "cluster_id", "prev_hash")


def canonical_entry(entry: dict) -> bytes:
    """审计帧的规范 JSON 字节（紧凑、键排序、UTF-8）。"""
    return json.dumps(entry, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":")).encode("utf-8")


def hash_entry(entry: dict) -> str:
    missing = [f for f in _CHAIN_FIELDS if f not in entry]
    if missing:
        raise ValueError(f"审计帧缺少字段: {missing}")
    return hashlib.sha256(canonical_entry(entry)).hexdigest()


def append_event(conn, case_id: str, event_type: str, summary: str,
                 payload: dict | None = None, *, event_time: str | None = None,
                 actor: str | None = None, version_id: str | None = None,
                 report_id: str | None = None, cluster_id: str | None = None) -> str:
    """在 *已开启写事务* 的连接上追加一个审计事件，返回事件 id。

    必须在 BEGIN IMMEDIATE 事务内调用：seq 分配与业务写入同生共死，
    并发写者在 SQLite 写锁上串行，序号无空洞。
    """
    seq = conn.execute(
        "SELECT COALESCE(MAX(seq),0)+1 AS n FROM case_events WHERE case_id=?",
        (case_id,)).fetchone()["n"]
    prev = conn.execute(
        "SELECT entry_hash FROM case_events WHERE case_id=? AND seq=?",
        (case_id, seq - 1)).fetchone()
    prev_hash = prev["entry_hash"] if prev else GENESIS
    entry = {
        "case_id": case_id, "seq": seq, "event_type": event_type,
        "event_time": event_time, "actor": actor, "summary": summary,
        "payload_json": json.dumps(payload or {}, ensure_ascii=False, sort_keys=True,
                                   default=str),
        "version_id": version_id, "report_id": report_id,
        "cluster_id": cluster_id, "prev_hash": prev_hash,
    }
    entry["entry_hash"] = hash_entry(entry)
    eid = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO case_events(id,case_id,seq,event_type,event_time,actor,summary,"
        "payload_json,prev_hash,entry_hash,version_id,report_id,cluster_id) "
        "VALUES(:id,:case_id,:seq,:event_type,:event_time,:actor,:summary,"
        ":payload_json,:prev_hash,:entry_hash,:version_id,:report_id,:cluster_id)",
        {"id": eid, **entry})
    return eid


def list_events(conn, case_ids, *, max_rowid: int | None = None,
                per_case_cut: dict | None = None) -> list[dict]:
    """读取事件链（按 event_time,rowid 排序的时间线视图）。

    - max_rowid：只取 rowid <= 截止线的帧（证据包绑定某时刻）；
    - per_case_cut：{case_id: rowid} 更细的逐病例截止线（合并/拆分血缘）。
    """
    if not case_ids:
        return []
    where = ["case_id IN (%s)" % ",".join("?" * len(case_ids))]
    params = list(case_ids)
    if max_rowid is not None:
        where.append("rowid<=?")
        params.append(max_rowid)
    rows = conn.execute(
        "SELECT rowid AS rowid,* FROM case_events WHERE %s "
        "ORDER BY event_time, rowid" % " AND ".join(where), params).fetchall()
    cuts = per_case_cut or {}
    out = []
    for r in rows:
        cut = cuts.get(r["case_id"])
        if cut is not None and r["rowid"] > cut:
            continue
        d = dict(r)
        d["payload"] = json.loads(d.pop("payload_json"))
        out.append(d)
    return out


class ChainBroken(Exception):
    """审计链核验失败。``failures`` 列出全部断点（而非只报第一个）。"""

    def __init__(self, failures: list[str]):
        super().__init__("; ".join(failures) or "审计链断裂")
        self.failures = failures


def verify_chain(events: list[dict], *, label: str = "病例") -> None:
    """对一组按 seq 有序的同一病例事件重算哈希链。

    失败时抛出 ChainBroken，failures 收集所有发现的问题：
    断号 / 首帧前驱错误 / 前驱不匹配 / 帧摘要不匹配。
    """
    failures: list[str] = []
    case_ids = {e.get("case_id") for e in events}
    if len(case_ids) > 1:
        raise ValueError("verify_chain 只能核验单一病例的事件")
    cid = next(iter(case_ids), None)
    prev_hash = GENESIS
    for i, e in enumerate(events, start=1):
        tag = f"{label} {str(cid)[:8]} seq={e.get('seq')}"
        if e.get("seq") != i:
            failures.append(f"{tag}: 序号断号/重号（期望 {i}，实际 {e.get('seq')}）")
        if e.get("prev_hash") != prev_hash:
            failures.append(f"{tag}: prev_hash 不匹配（链断裂或事件被删改）")
        stored = e.get("entry_hash")
        payload = e.get("payload")
        payload_json = e.get("payload_json")
        if payload_json is None:
            payload_json = json.dumps(payload if payload is not None else {},
                                      ensure_ascii=False, sort_keys=True, default=str)
        recomputed = hash_entry({
            "case_id": e.get("case_id"), "seq": e.get("seq"),
            "event_type": e.get("event_type"), "event_time": e.get("event_time"),
            "actor": e.get("actor"), "summary": e.get("summary"),
            "payload_json": payload_json, "version_id": e.get("version_id"),
            "report_id": e.get("report_id"), "cluster_id": e.get("cluster_id"),
            "prev_hash": e.get("prev_hash"),
        })
        if stored != recomputed:
            failures.append(f"{tag}: entry_hash 不匹配（帧内容被篡改）")
        prev_hash = stored or prev_hash
    if failures:
        raise ChainBroken(failures)


def chain_head(events: list[dict]) -> str:
    """有序事件链的链头哈希；空链为 GENESIS。"""
    return events[-1]["entry_hash"] if events else GENESIS
