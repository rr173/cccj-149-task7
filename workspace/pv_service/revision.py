"""数据修订号原语（双人会签的失效机制基础）。

*纯 SQL、不依赖任何领域服务*，可被 ``service.py`` 在其 ``BEGIN IMMEDIATE``
事务内直接调用，避免循环导入。

修订号语义（评审需求）：

- 评审期间发生「新材料入库、归并或拆解、规则重新计算」之一即推进修订号；
- 推进与业务变更在同一个写事务内提交 —— SQLite 写事务全局串行，
  评审单看到的「冻结修订号」与业务状态永远一致，不存在中间窗口；
- 推进时原子地把所有 ``OPEN`` 评审单置为 ``INVALIDATED``（记录原因与
  新修订号）：旧单立即失效，已有签署意见保留（``review_events`` /
  ``review_signatures`` 不删），但再不能推动放行；
- 再次发起评审只能引用最新修订号（``ReviewService.create_sheet`` 强制）。
"""
from __future__ import annotations

# 修订原因码
REV_NEW_MATERIAL = "NEW_MATERIAL"          # 新材料入库（接收/随访）
REV_MERGE = "MERGED"                       # 候选归并确认
REV_SPLIT = "SPLIT"                        # 拆解
REV_RULE_RECALC = "RULE_RECALCULATED"      # 规则重新计算
REV_REFERENCE_CHANGED = "REFERENCE_CHANGED"  # listedness 等参考数据变更


def current_revision(conn) -> int:
    """读取当前数据修订号（未初始化时为 0）。"""
    row = conn.execute(
        "SELECT revision FROM review_revision WHERE scope='GLOBAL'").fetchone()
    return int(row["revision"]) if row else 0


def bump_revision(conn, reason_code: str, *, detail: dict | None = None,
                  actor: str | None = None, now_iso: str | None = None,
                  invalidate_open_sheets: bool = True) -> int:
    """在 *已开启的写事务* 内推进修订号并作废旧评审单。

    返回新修订号。必须由调用方持有 ``BEGIN IMMEDIATE`` 事务：
    业务写入、修订号自增、旧单失效三者同生共死。
    """
    import json as _json
    now = now_iso or __import__("datetime").datetime.now().isoformat(timespec="seconds")
    conn.execute(
        "INSERT INTO review_revision(scope,revision) VALUES('GLOBAL',1) "
        "ON CONFLICT(scope) DO UPDATE SET revision=review_revision.revision+1")
    rev = current_revision(conn)
    conn.execute(
        "INSERT INTO review_revision_events(id,revision,reason_code,detail_json,"
        "actor,created_at) VALUES(NULL,?,?,?,?,?)",
        (rev, reason_code,
         _json.dumps(detail or {}, ensure_ascii=False, sort_keys=True, default=str),
         actor, now))
    if invalidate_open_sheets:
        invalidate_open(conn, rev, reason_code=reason_code, now_iso=now,
                        detail=detail)
    return rev


def invalidate_open(conn, new_revision: int, *, reason_code: str,
                    now_iso: str, detail: dict | None = None) -> int:
    """把所有 OPEN 评审单置为 INVALIDATED，返回作废单数。

    活意见槽与意见流水原样保留（“已有意见保留”）；租约清空，待办列表
    立即不再展示旧单。每单补一条 INVALIDATED 意见流水。
    """
    import json as _json
    # 先取 OPEN 单 id 再统一更新，保证整个操作仍在调用方事务内。
    open_ids = [r["id"] for r in conn.execute(
        "SELECT id FROM review_sheets WHERE status='OPEN'").fetchall()]
    if open_ids:
        conn.execute(
            "UPDATE review_sheets SET status='INVALIDATED', invalidated_reason=?, "
            "invalidated_at=?, invalidated_revision=?, lease_holder=NULL, "
            "lease_holder_key=NULL, lease_expires_at=NULL, leased_at=NULL "
            "WHERE status='OPEN'",
            (reason_code, now_iso, new_revision))
        for sid in open_ids:
            conn.execute(
                "INSERT INTO review_events(id,sheet_id,event_type,actor,actor_key,"
                "detail_json,created_at) VALUES(NULL,?,?,NULL,NULL,?,?)",
                (sid, "INVALIDATED",
                 _json.dumps({"reason_code": reason_code,
                              "new_revision": new_revision,
                              "detail": detail or {}}, ensure_ascii=False,
                             sort_keys=True, default=str), now_iso))
    return len(open_ids)
