"""药物安全决定的双人会签服务。

评审单生命周期（状态机单向流转，终态不可回退）::

    OPEN ──两名不同复核员 APPROVED──▶ APPROVED      （放行待发送决定）
      ├──任一名复核员 VETOED────────▶ REJECTED      （本轮立即结束）
      └──数据修订号推进─────────────▶ INVALIDATED   （旧单立即失效）

不变量如何成立（数据库约束 + 写事务串行，而非只靠应用判断）：

1. *不可变评审单*：候选归并关系、严重等级变化、待发送决定在建单时
   冻结进 ``review_sheet_items``（触发器拒绝 UPDATE/DELETE），并按
   内容 SHA-256 去重；同修订号+同内容只允许一张 OPEN 单
   （部分唯一索引兜底，并发重复发起只返回同一张）。
2. *同一修订号*：单子冻结 ``revision``；新材料/归并/拆解/规则重算在
   业务事务内推进修订号并原子作废旧单（见 ``revision.py``），再次
   发起只能引用最新修订号。
3. *两名不同人、同人不累计*：``review_signatures`` 主键
   ``(sheet_id, reviewer_key)`` 使同一身份只能占一个意见槽；身份键由
   ``identity.normalize_reviewer`` 做 NFKC + casefold + 去首尾空白，
   因此 "alice"/"ALICE"/" alice "/全角等价写法都命中同一槽，单人无法
   靠表面写法差异占满两槽。重复签署直接冲突，两个不同身份的 APPROVED
   槽才完成会签；原始提交值仍逐次留痕（``reviewer`` / ``actor``）。
4. *空白身份明确拒绝*：领取/释放/赞成/否决/撤回的 reviewer 为非字符串、
   空白或超长时返回 400（``INVALID_REVIEWER`` / ``BLANK_REVIEWER`` /
   ``REVIEWER_TOO_LONG``），绝不把缺失身份静默当作某个复核员。
5. *意见全程保留*：签署/否决/撤回/失效/领取/释放全部追加进
   ``review_events``（只增触发器）；撤回只删活意见槽，流水不丢。
6. *租约*：领取是带条件的原子 UPDATE（未被持有或已过期才能成功），
   多人抢领只有一人成功；到期由 ``expire_leases`` 释放；服务重启
   恢复时统一清理过期租约。租约比较同样用规范化身份键。租约只是待办
   工作台的协作提示，不限制签署权（真正的并发安全由 3 的槽位约束保证）。
7. *一个生效结论*：批准完成时按冻结的待发送决定写
   ``countersign_approvals``（report_id 主键，幂等重放只解锁一次）；
   批准与撤回/否决同时发生时，所有写操作在共享 RLock +
   BEGIN IMMEDIATE 上串行，状态机只允许一个终态。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import uuid
from typing import Any

from . import audit
from . import db as dbmod
from . import identity
from . import revision as revmod
from .service import Clock, DomainError

REQUIRED_APPROVALS = 2
DEFAULT_LEASE_TTL_SECONDS = 900

_ITEM_TYPES = ("MERGE_CANDIDATE", "SERIOUSNESS_CHANGE", "PENDING_REPORT")


def _uid() -> str:
    return uuid.uuid4().hex


def _json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, default=str)


def _require_reviewer(reviewer) -> tuple[str, str]:
    """校验写操作的复核员身份，返回 ``(规范化键, 原始提交值)``。

    空白 / 非字符串 / 超长身份一律明确拒绝（400），绝不把缺失身份
    静默当成某个复核员。
    """
    try:
        key = identity.validate_reviewer(reviewer)
    except ValueError as exc:
        code, _, message = str(exc).partition(":")
        raise DomainError(code, message, 400)
    return key, reviewer


class ReviewService:
    def __init__(self, db_path: str, clock: Clock | None = None,
                 shared_lock: threading.RLock | None = None,
                 lease_ttl_seconds: int = DEFAULT_LEASE_TTL_SECONDS,
                 required_approvals: int = REQUIRED_APPROVALS):
        self.db_path = db_path
        self.clock = clock or Clock()
        self._lock = shared_lock or threading.RLock()
        self.lease_ttl_seconds = lease_ttl_seconds
        self.required_approvals = required_approvals
        dbmod.initialize(db_path)
        self.recover_leases()

    # -- 基础设施 -----------------------------------------------------------
    def _conn(self) -> sqlite3.Connection:
        return dbmod.connect(self.db_path)

    def _now(self) -> str:
        return self.clock.now_iso()

    def _lease_expiry(self) -> str:
        from datetime import timedelta
        return (self.clock.now()
                + timedelta(seconds=self.lease_ttl_seconds)).isoformat(timespec="seconds")

    def current_revision(self) -> int:
        conn = self._conn()
        try:
            return revmod.current_revision(conn)
        finally:
            conn.close()

    def _log(self, conn, sheet_id: str, event_type: str, actor: str | None = None,
             detail: dict | None = None, when: str | None = None,
             actor_key: str | None = None) -> None:
        """追加只增事件流水。``actor`` 存原始值（追溯），``actor_key`` 存
        规范化键（过滤/比较）；未显式给键时从 actor 软推导。"""
        if actor is not None and actor_key is None:
            actor_key, _ = identity.coerce_reviewer(actor)
        conn.execute(
            "INSERT INTO review_events(id,sheet_id,event_type,actor,actor_key,"
            "detail_json,created_at) VALUES(NULL,?,?,?,?,?,?)",
            (sheet_id, event_type, actor, actor_key, _json(detail or {}),
             when or self._now()))

    def recover_leases(self) -> int:
        """服务恢复：释放已到期租约（不产生任何业务结论）。

        未到期的租约在进程崩溃后也可能失去持有者，按 TTL 自然到期；
        此方法幂等，可在启动时与任何时刻重复执行。
        """
        with self._lock:
            conn = self._conn()
            try:
                with dbmod.transaction(conn):
                    return self._expire_leases(conn)
            finally:
                conn.close()

    def _expire_leases(self, conn, now: str | None = None) -> int:
        """在调用方事务内释放到期租约并逐单留痕。返回释放数量。"""
        ts = now or self._now()
        rows = conn.execute(
            "SELECT id, lease_holder, lease_holder_key FROM review_sheets "
            "WHERE status='OPEN' AND lease_holder IS NOT NULL "
            "AND lease_expires_at<=?", (ts,)).fetchall()
        for r in rows:
            conn.execute(
                "UPDATE review_sheets SET lease_holder=NULL, lease_holder_key=NULL,"
                "leased_at=NULL,lease_expires_at=NULL WHERE id=? AND status='OPEN'",
                (r["id"],))
            self._log(conn, r["id"], "LEASE_EXPIRED", r["lease_holder"],
                      {"lease_expired_at": ts}, when=ts,
                      actor_key=r["lease_holder_key"])
        return len(rows)

    # -- 评审单内容收集（冻结快照）------------------------------------------
    def _collect_items(self, conn, report_ids: list[str] | None) -> list[dict]:
        items: list[dict] = []

        # 1) 待发送决定（QUEUED 报告；含规则决策、加急与截止日）
        if report_ids is not None:
            if not report_ids:
                raise DomainError("EMPTY_REVIEW", "评审单至少包含一个待发送决定")
            placeholders = ",".join("?" * len(report_ids))
            rpts = conn.execute(
                f"SELECT * FROM regulatory_reports WHERE id IN ({placeholders}) "
                f"AND status IN ('QUEUED','SENDING','FAILED') ORDER BY id",
                report_ids).fetchall()
            found = {r["id"] for r in rpts}
            missing = [x for x in report_ids if x not in found]
            if missing:
                raise DomainError("REPORT_NOT_PENDING",
                                  f"报告不存在或已无待发送状态: {missing}", 409)
        else:
            rpts = conn.execute(
                "SELECT * FROM regulatory_reports WHERE status='QUEUED' "
                "ORDER BY expedited DESC, due_date, id").fetchall()
        for r in rpts:
            items.append({
                "item_type": "PENDING_REPORT",
                "ref_type": "report", "ref_id": r["id"],
                "snapshot": {
                    "report_id": r["id"], "case_id": r["case_id"],
                    "case_version_id": r["case_version_id"],
                    "report_type": r["report_type"],
                    "regulatory_number": r["regulatory_number"],
                    "status_at_freeze": r["status"],
                    "expedited": bool(r["expedited"]), "days_due": r["days_due"],
                    "due_date": r["due_date"],
                    "rule_version": {"package_id": r["rule_package_id"],
                                     "version": r["rule_version"]},
                    "decision": json.loads(r["decision_json"]),
                },
            })

        # 2) 候选归并关系（全部 PROPOSED 疑似重复簇，含分值与命中理由）
        for c in conn.execute(
                "SELECT * FROM duplicate_clusters WHERE status='PROPOSED' "
                "ORDER BY created_at, id"):
            rationale = json.loads(c["rationale_json"])
            members = [m["case_id"] for m in conn.execute(
                "SELECT case_id FROM cluster_members WHERE cluster_id=? "
                "ORDER BY added_at, case_id", (c["id"],))]
            items.append({
                "item_type": "MERGE_CANDIDATE",
                "ref_type": "cluster", "ref_id": c["id"],
                "snapshot": {"cluster_id": c["id"], "members": members,
                             "rationale": rationale, "created_at": c["created_at"]},
            })

        # 3) 严重等级变化（每个 OPEN 病例最近两个版本之间的严重性翻转）
        for row in conn.execute(
                "SELECT v.case_id, v.id AS new_vid, v.version_no AS new_no, "
                "v.snapshot_json AS new_snap, p.id AS old_vid, "
                "p.version_no AS old_no, p.snapshot_json AS old_snap "
                "FROM case_versions v JOIN case_versions p "
                "ON p.case_id=v.case_id AND p.version_no=v.version_no-1 "
                "JOIN cases c ON c.id=v.case_id WHERE c.status='OPEN' "
                "AND v.version_no=(SELECT MAX(version_no) FROM case_versions "
                "WHERE case_id=v.case_id) ORDER BY v.case_id"):
            old_serious = bool(json.loads(row["old_snap"]).get("serious"))
            new_serious = bool(json.loads(row["new_snap"]).get("serious"))
            if old_serious != new_serious:
                items.append({
                    "item_type": "SERIOUSNESS_CHANGE",
                    "ref_type": "case", "ref_id": row["case_id"],
                    "snapshot": {
                        "case_id": row["case_id"],
                        "from_version_id": row["old_vid"],
                        "to_version_id": row["new_vid"],
                        "from_version_no": row["old_no"],
                        "to_version_no": row["new_no"],
                        "from_serious": old_serious, "to_serious": new_serious,
                    },
                })
        # 确定性排序 -> 内容哈希稳定（与收集顺序无关）
        items.sort(key=lambda x: (x["item_type"], x["ref_id"] or ""))
        return items

    @staticmethod
    def _content_hash(items: list[dict]) -> str:
        body = _json(items)
        return hashlib.sha256(body.encode("utf-8")).hexdigest()

    # -- 发起评审 -----------------------------------------------------------
    def create_sheet(self, report_ids: list[str] | None = None, *,
                     requested_by: str | None = None,
                     title: str | None = None) -> dict:
        """对当前最新修订号冻结一张不可变评审单。

        内容 = 待发送决定（可指定 report_ids 子集）+ 候选归并关系 +
        严重等级变化。同修订号同内容的 OPEN 单已存在时幂等返回旧单。
        """
        # requested_by 仅用于留档，不是会签身份；软规范化，空白记为 NULL。
        _, requested_by = identity.coerce_reviewer(requested_by)
        with self._lock:
            conn = self._conn()
            try:
                with dbmod.transaction(conn):
                    self._expire_leases(conn)
                    rev = revmod.current_revision(conn)
                    items = self._collect_items(conn, report_ids)
                    if not items:
                        raise DomainError("EMPTY_REVIEW",
                                          "当前没有可评审内容（无待发送决定/"
                                          "候选归并/严重等级变化）", 409)
                    content_hash = self._content_hash(items)
                    existing = conn.execute(
                        "SELECT id FROM review_sheets WHERE revision=? AND content_hash=? "
                        "AND status='OPEN'", (rev, content_hash)).fetchone()
                    if existing:
                        return self._load_sheet(conn, existing["id"],
                                                idempotent=True)
                    sid = _uid()
                    n_pending = sum(1 for i in items if i["item_type"] == "PENDING_REPORT")
                    n_merge = sum(1 for i in items if i["item_type"] == "MERGE_CANDIDATE")
                    n_serious = sum(1 for i in items
                                    if i["item_type"] == "SERIOUSNESS_CHANGE")
                    sheet_title = title or (
                        f"安全决定会签 r{rev}：待发送 {n_pending} / 归并候选 {n_merge}"
                        f" / 严重等级变化 {n_serious}")
                    conn.execute(
                        "INSERT INTO review_sheets(id,status,revision,content_hash,title,"
                        "requested_by,created_at) VALUES(?, 'OPEN', ?,?,?,?,?)",
                        (sid, rev, content_hash, sheet_title, requested_by, self._now()))
                    for pos, item in enumerate(items):
                        conn.execute(
                            "INSERT INTO review_sheet_items(id,sheet_id,position,item_type,"
                            "ref_type,ref_id,snapshot_json) VALUES(?,?,?,?,?,?,?)",
                            (_uid(), sid, pos, item["item_type"], item["ref_type"],
                             item["ref_id"], _json(item["snapshot"])))
                    self._log(conn, sid, "CREATED", requested_by,
                              {"revision": rev, "content_hash": content_hash,
                               "counts": {"pending_report": n_pending,
                                          "merge_candidate": n_merge,
                                          "seriousness_change": n_serious}})
                    return self._load_sheet(conn, sid)
            except sqlite3.IntegrityError:
                # 并发发起穿透了应用层检查：部分唯一索引保证只留一张 OPEN 单。
                # rev/content_hash 已在本线程前面计算完成（局部变量仍在）。
                with dbmod.transaction(conn):
                    row = conn.execute(
                        "SELECT id FROM review_sheets WHERE revision=? AND content_hash=? "
                        "AND status='OPEN'", (rev, content_hash)).fetchone()
                    if row:
                        return self._load_sheet(conn, row["id"], idempotent=True)
                    raise
            finally:
                conn.close()

    # -- 待办领取 / 释放 -----------------------------------------------------
    def list_todos(self, *, reviewer: str | None = None) -> list[dict]:
        """待办工作台：OPEN 单（未被领取、租约已到期、或本人已领取）。"""
        # 显式但空白/非法的 reviewer 明确拒绝；None 表示不过滤（看全部）。
        reviewer_key = None
        if reviewer is not None:
            try:
                reviewer_key = identity.validate_reviewer(reviewer)
            except ValueError as exc:
                code, _, message = str(exc).partition(":")
                raise DomainError(code, message, 400)
        with self._lock:
            conn = self._conn()
            try:
                with dbmod.transaction(conn):
                    self._expire_leases(conn)
                    rows = conn.execute(
                        "SELECT * FROM review_sheets WHERE status='OPEN' ORDER BY created_at"
                    ).fetchall()
                    now = self._now()
                    held_ids = {r["id"]: (r["lease_holder_key"] is not None
                                          and r["lease_expires_at"] > now)
                                for r in rows}
                    sheet_ids = [r["id"] for r in rows]
                # 只读装载在写事务外进行（_load_sheet 不开事务）
                out = []
                for sid in sheet_ids:
                    d = self._load_sheet(conn, sid)
                    holder_key = d["lease_holder_key"]
                    held = held_ids[sid]
                    if reviewer_key is None:
                        # 不带 reviewer：工作台只呈现可领取（未被持有）的单，
                        # 与历史行为保持一致。
                        if held:
                            continue
                        d["claimed_by_me"] = False
                    else:
                        if held and holder_key != reviewer_key:
                            continue
                        d["claimed_by_me"] = (holder_key == reviewer_key)
                    d["claimable"] = not held
                    out.append(d)
                return out
            finally:
                conn.close()

    def claim(self, sheet_id: str, reviewer: str) -> dict:
        """领取待办（租约）。多人抢领只有一人成功，其余 409。"""
        reviewer_key, reviewer = _require_reviewer(reviewer)
        with self._lock:
            conn = self._conn()
            try:
                with dbmod.transaction(conn):
                    self._expire_leases(conn)
                    sheet = conn.execute("SELECT * FROM review_sheets WHERE id=?",
                                         (sheet_id,)).fetchone()
                    if not sheet:
                        raise DomainError("SHEET_NOT_FOUND", "评审单不存在", 404)
                    if sheet["status"] != "OPEN":
                        raise DomainError("SHEET_NOT_OPEN",
                                          f"评审单状态为 {sheet['status']}，不能领取", 409)
                    if sheet["lease_holder_key"] == reviewer_key and \
                            sheet["lease_expires_at"] > self._now():
                        return self._load_sheet(conn, sheet_id)  # 本人续租幂等
                    now = self._now()
                    expires = self._lease_expiry()
                    cur = conn.execute(
                        "UPDATE review_sheets SET lease_holder=?, lease_holder_key=?, "
                        "leased_at=?, lease_expires_at=? WHERE id=? AND status='OPEN' "
                        "AND (lease_holder_key IS NULL OR lease_expires_at<=?)",
                        (reviewer, reviewer_key, now, expires, sheet_id, now))
                    if cur.rowcount == 0:
                        holder = conn.execute(
                            "SELECT lease_holder FROM review_sheets WHERE id=?",
                            (sheet_id,)).fetchone()["lease_holder"]
                        raise DomainError("TODO_ALREADY_CLAIMED",
                                          f"待办已被 {holder} 领取且租约未到期", 409)
                    self._log(conn, sheet_id, "CLAIMED", reviewer,
                              {"lease_expires_at": expires}, actor_key=reviewer_key)
                    return self._load_sheet(conn, sheet_id)
            finally:
                conn.close()

    def release(self, sheet_id: str, reviewer: str) -> dict:
        """主动释放租约（仅持有者可释放）。"""
        reviewer_key, reviewer = _require_reviewer(reviewer)
        with self._lock:
            conn = self._conn()
            try:
                with dbmod.transaction(conn):
                    sheet = self._get_open_sheet(conn, sheet_id)
                    if sheet["lease_holder_key"] != reviewer_key:
                        raise DomainError("NOT_LEASE_HOLDER",
                                          "只有租约持有者可以释放", 409)
                    conn.execute(
                        "UPDATE review_sheets SET lease_holder=NULL,"
                        "lease_holder_key=NULL, leased_at=NULL,"
                        "lease_expires_at=NULL WHERE id=?", (sheet_id,))
                    self._log(conn, sheet_id, "RELEASED", reviewer,
                              actor_key=reviewer_key)
                    return self._load_sheet(conn, sheet_id)
            finally:
                conn.close()

    # -- 签署 / 否决 / 撤回 --------------------------------------------------
    def _get_open_sheet(self, conn, sheet_id: str) -> sqlite3.Row:
        sheet = conn.execute("SELECT * FROM review_sheets WHERE id=?",
                             (sheet_id,)).fetchone()
        if not sheet:
            raise DomainError("SHEET_NOT_FOUND", "评审单不存在", 404)
        if sheet["status"] == "INVALIDATED":
            raise DomainError("SHEET_INVALIDATED",
                              f"评审单因数据修订 {sheet['invalidated_revision']} 已失效"
                              f"（{sheet['invalidated_reason']}），请引用最新修订号重新发起",
                              409)
        if sheet["status"] != "OPEN":
            raise DomainError("SHEET_NOT_OPEN",
                              f"评审单状态为 {sheet['status']}，本轮已结束", 409)
        # 防御：正常路径下修订推进已在同一串行序列里把单子作废
        if sheet["revision"] != revmod.current_revision(conn):
            raise DomainError("SHEET_STALE",
                              "数据修订号已前进，评审单已过期", 409)
        return sheet

    def approve(self, sheet_id: str, reviewer: str, comment: str | None = None) -> dict:
        """赞成签署。同一身份重复提交不累计；两名不同复核员赞成即放行。

        身份按规范化键判定：首尾空白、大小写、Unicode 等价写法都视为
        同一复核员，只能占一个意见槽；原始提交值原样留痕。
        """
        reviewer_key, reviewer = _require_reviewer(reviewer)
        with self._lock:
            conn = self._conn()
            try:
                with dbmod.transaction(conn):
                    sheet = self._get_open_sheet(conn, sheet_id)
                    existing = conn.execute(
                        "SELECT reviewer, verdict FROM review_signatures "
                        "WHERE sheet_id=? AND reviewer_key=?",
                        (sheet_id, reviewer_key)).fetchone()
                    if existing:
                        raise DomainError("ALREADY_SIGNED",
                                          f"{reviewer} 与 {existing['reviewer']} "
                                          f"（{existing['verdict']}）为同一复核员身份，"
                                          "不能重复累计票数（可先撤回再改签）", 409)
                    try:
                        conn.execute(
                            "INSERT INTO review_signatures(sheet_id,reviewer_key,"
                            "reviewer,verdict,detail_json,signed_at) "
                            "VALUES(?,?,?,'APPROVED',?,?)",
                            (sheet_id, reviewer_key, reviewer,
                             _json({"comment": comment}), self._now()))
                    except sqlite3.IntegrityError:
                        # 与并发的同一人提交相撞：规范化键只有一槽，后到者不累计
                        raise DomainError("ALREADY_SIGNED",
                                          f"{reviewer} 已签署，同一身份不能重复累计票数",
                                          409)
                    conn.execute(
                        "UPDATE review_sheets SET approval_count=approval_count+1 WHERE id=?",
                        (sheet_id,))
                    self._log(conn, sheet_id, "APPROVED", reviewer,
                              {"comment": comment}, actor_key=reviewer_key)
                    return self._maybe_complete(conn, sheet_id)
            finally:
                conn.close()

    def veto(self, sheet_id: str, reviewer: str, reason: str | None = None) -> dict:
        """否决：任一名复核员否决即结束本轮（结论 REJECTED）。"""
        reviewer_key, reviewer = _require_reviewer(reviewer)
        with self._lock:
            conn = self._conn()
            try:
                with dbmod.transaction(conn):
                    sheet = self._get_open_sheet(conn, sheet_id)
                    existing = conn.execute(
                        "SELECT reviewer, verdict FROM review_signatures "
                        "WHERE sheet_id=? AND reviewer_key=?",
                        (sheet_id, reviewer_key)).fetchone()
                    if existing:
                        raise DomainError("ALREADY_SIGNED",
                                          f"{reviewer} 与 {existing['reviewer']} "
                                          f"（{existing['verdict']}）为同一复核员身份，"
                                          "不能重复签署", 409)
                    try:
                        conn.execute(
                            "INSERT INTO review_signatures(sheet_id,reviewer_key,"
                            "reviewer,verdict,detail_json,signed_at) "
                            "VALUES(?,?,?,'VETOED',?,?)",
                            (sheet_id, reviewer_key, reviewer,
                             _json({"reason": reason}), self._now()))
                    except sqlite3.IntegrityError:
                        raise DomainError("ALREADY_SIGNED",
                                          f"{reviewer} 已签署，同一身份不能重复签署",
                                          409)
                    conn.execute(
                        "UPDATE review_sheets SET status='REJECTED', decided_at=?, "
                        "lease_holder=NULL, lease_holder_key=NULL, leased_at=NULL, "
                        "lease_expires_at=NULL WHERE id=?",
                        (self._now(), sheet_id))
                    self._log(conn, sheet_id, "VETOED", reviewer,
                              {"reason": reason}, actor_key=reviewer_key)
                    self._log(conn, sheet_id, "VETOED_COMPLETED", reviewer,
                              {"reason": reason, "decided_at": self._now()},
                              actor_key=reviewer_key)
                    self._mirror_case_event(
                        conn, sheet_id, "REVIEW_VETOED",
                        f"会签评审单被 {reviewer} 否决，本轮发送终止"
                        + (f"：{reason}" if reason else ""),
                        {"sheet_id": sheet_id, "reason": reason}, actor=reviewer)
                    return self._load_sheet(conn, sheet_id)
            finally:
                conn.close()

    def withdraw(self, sheet_id: str, reviewer: str) -> dict:
        """撤回本人此前的赞成意见（仅 OPEN 单可撤；撤回不影响他人意见）。

        否决是终态（REJECTED），不可撤回；撤回只移除活意见槽，
        历史流水永久保留。
        """
        reviewer_key, reviewer = _require_reviewer(reviewer)
        with self._lock:
            conn = self._conn()
            try:
                with dbmod.transaction(conn):
                    sheet = conn.execute("SELECT * FROM review_sheets WHERE id=?",
                                         (sheet_id,)).fetchone()
                    if not sheet:
                        raise DomainError("SHEET_NOT_FOUND", "评审单不存在", 404)
                    sig = conn.execute(
                        "SELECT verdict FROM review_signatures "
                        "WHERE sheet_id=? AND reviewer_key=?",
                        (sheet_id, reviewer_key)).fetchone()
                    if sig and sig["verdict"] == "VETOED":
                        raise DomainError("VETO_FINAL",
                                          "否决即结束本轮，不能撤回否决意见", 409)
                    if sheet["status"] == "INVALIDATED":
                        raise DomainError("SHEET_INVALIDATED",
                                          f"评审单因数据修订 {sheet['invalidated_revision']} "
                                          f"已失效（{sheet['invalidated_reason']}）", 409)
                    if sheet["status"] != "OPEN":
                        raise DomainError("SHEET_NOT_OPEN",
                                          f"评审单状态为 {sheet['status']}，本轮已结束",
                                          409)
                    if not sig:
                        raise DomainError("NOT_SIGNED",
                                          f"{reviewer} 在本单没有可撤回的活意见", 409)
                    conn.execute(
                        "DELETE FROM review_signatures WHERE sheet_id=? AND reviewer_key=?",
                        (sheet_id, reviewer_key))
                    conn.execute(
                        "UPDATE review_sheets SET approval_count=MAX(approval_count-1,0) "
                        "WHERE id=?", (sheet_id,))
                    self._log(conn, sheet_id, "WITHDRAWN", reviewer,
                              actor_key=reviewer_key)
                    return self._load_sheet(conn, sheet_id)
            finally:
                conn.close()

    def _maybe_complete(self, conn, sheet_id: str) -> dict:
        """检查活赞成槽是否达到两名不同复核员；达到则产生唯一 APPROVED 结论。"""
        approvals = conn.execute(
            "SELECT DISTINCT reviewer_key, reviewer, signed_at "
            "FROM review_signatures WHERE sheet_id=? AND verdict='APPROVED' "
            "ORDER BY signed_at, rowid",
            (sheet_id,)).fetchall()
        if len(approvals) < self.required_approvals:
            return self._load_sheet(conn, sheet_id)
        # 以规范化身份键去重：杜绝空白/大小写/Unicode 等价写法冒充第二名复核员
        keys: list[str] = []
        reviewers: list[str] = []
        for r in approvals:
            if r["reviewer_key"] in keys:
                continue
            keys.append(r["reviewer_key"])
            reviewers.append(r["reviewer"])
        if len(keys) < self.required_approvals:
            # 主键 (sheet_id, reviewer_key) 已保证不重复，这里双保险
            return self._load_sheet(conn, sheet_id)
        rev = conn.execute("SELECT revision FROM review_sheets WHERE id=?",
                           (sheet_id,)).fetchone()["revision"]
        conn.execute(
            "UPDATE review_sheets SET status='APPROVED', decided_at=?, lease_holder=NULL,"
            "lease_holder_key=NULL, leased_at=NULL, lease_expires_at=NULL "
            "WHERE id=? AND status='OPEN'",
            (self._now(), sheet_id))
        self._log(conn, sheet_id, "APPROVED_COMPLETED", None,
                  {"reviewers": reviewers, "reviewer_keys": keys,
                   "decided_at": self._now()})
        # 放行闸门：只对建单时冻结、且当前仍未发送/未作废的决定生效。
        # 修订后的新报告不包含在 items 内，必须重新会签。
        frozen_reports = [json.loads(r["snapshot_json"]) for r in conn.execute(
            "SELECT snapshot_json FROM review_sheet_items WHERE sheet_id=? "
            "AND item_type='PENDING_REPORT'", (sheet_id,))]
        released = []
        for snap in frozen_reports:
            rid = snap["report_id"]
            cur = conn.execute("SELECT status, case_id FROM regulatory_reports WHERE id=?",
                               (rid,)).fetchone()
            if not cur or cur["status"] in ("ACKED", "VOID"):
                continue
            conn.execute(
                "INSERT OR IGNORE INTO countersign_approvals(report_id,sheet_id,revision,"
                "approved_at,approved_by_json) VALUES(?,?,?,?,?)",
                (rid, sheet_id, rev, self._now(), _json(reviewers)))
            released.append({"report_id": rid, "case_id": cur["case_id"]})
        for item in released:
            audit.append_event(
                conn, item["case_id"], "REVIEW_COUNTERSIGNED",
                f"双人会签通过，待发送决定解锁（评审单 {sheet_id[:8]}）",
                {"sheet_id": sheet_id, "report_id": item["report_id"],
                 "reviewers": reviewers, "revision": rev},
                event_time=self._now(), report_id=item["report_id"])
        return self._load_sheet(conn, sheet_id)

    def _mirror_case_event(self, conn, sheet_id: str, event_type: str,
                           summary: str, payload: dict, actor: str | None) -> None:
        """把会签终态镜像到受影响病例时间线（审计链可追溯）。"""
        case_ids = {r["ref_id"] for r in conn.execute(
            "SELECT DISTINCT ref_id FROM review_sheet_items "
            "WHERE sheet_id=? AND item_type='SERIOUSNESS_CHANGE'", (sheet_id,))}
        for r in conn.execute(
                "SELECT snapshot_json FROM review_sheet_items WHERE sheet_id=? "
                "AND item_type='PENDING_REPORT'", (sheet_id,)):
            case_ids.add(json.loads(r["snapshot_json"]).get("case_id"))
        for cid in sorted(c for c in case_ids if c):
            audit.append_event(conn, cid, event_type, summary, payload,
                               event_time=self._now(), actor=actor)

    # -- 租约维护 ------------------------------------------------------------
    def expire_leases(self) -> int:
        """主动释放所有到期租约（也可由后台周期调用）。"""
        with self._lock:
            conn = self._conn()
            try:
                with dbmod.transaction(conn):
                    return self._expire_leases(conn)
            finally:
                conn.close()

    # -- 查询：详情 / 历史 / 审计 --------------------------------------------
    def _load_sheet(self, conn, sheet_id: str, *, idempotent: bool = False) -> dict:
        s = conn.execute("SELECT * FROM review_sheets WHERE id=?",
                         (sheet_id,)).fetchone()
        if not s:
            raise DomainError("SHEET_NOT_FOUND", "评审单不存在", 404)
        items = []
        for r in conn.execute(
                "SELECT item_type,ref_type,ref_id,snapshot_json FROM review_sheet_items "
                "WHERE sheet_id=? ORDER BY position", (sheet_id,)):
            items.append({"item_type": r["item_type"], "ref_type": r["ref_type"],
                          "ref_id": r["ref_id"],
                          "snapshot": json.loads(r["snapshot_json"])})
        signatures = [dict(r) for r in conn.execute(
            "SELECT reviewer,reviewer_key,verdict,detail_json,signed_at "
            "FROM review_signatures WHERE sheet_id=? ORDER BY signed_at, rowid",
            (sheet_id,))]
        for sig in signatures:
            sig["detail"] = json.loads(sig.pop("detail_json"))
        events = [dict(r) for r in conn.execute(
            "SELECT event_type,actor,actor_key,detail_json,created_at FROM review_events "
            "WHERE sheet_id=? ORDER BY id", (sheet_id,))]
        for e in events:
            e["detail"] = json.loads(e.pop("detail_json"))
        approvers = [sig["reviewer"] for sig in signatures if sig["verdict"] == "APPROVED"]
        approver_keys = sorted({sig["reviewer_key"] for sig in signatures
                                if sig["verdict"] == "APPROVED"})
        gate = conn.execute(
            "SELECT 1 FROM countersign_approvals WHERE sheet_id=? LIMIT 1",
            (sheet_id,)).fetchone()
        return {
            "sheet_id": s["id"], "status": s["status"], "revision": s["revision"],
            "current_revision": revmod.current_revision(conn),
            "content_hash": s["content_hash"], "title": s["title"],
            "requested_by": s["requested_by"],
            "required_approvals": self.required_approvals,
            "approval_count": len(approver_keys),
            "approvers": approvers, "approver_keys": approver_keys,
            "signatures": signatures,
            "lease_holder": s["lease_holder"],
            "lease_holder_key": s["lease_holder_key"],
            "lease_expires_at": s["lease_expires_at"],
            "created_at": s["created_at"], "decided_at": s["decided_at"],
            "invalidated_reason": s["invalidated_reason"],
            "invalidated_at": s["invalidated_at"],
            "invalidated_revision": s["invalidated_revision"],
            "released_for_dispatch": bool(gate),
            "items": items, "events": events,
            **({"idempotent": True} if idempotent else {}),
        }

    def get_sheet(self, sheet_id: str) -> dict:
        conn = self._conn()
        try:
            return self._load_sheet(conn, sheet_id)
        finally:
            conn.close()

    def decision_history(self, *, report_id: str | None = None,
                         case_id: str | None = None,
                         status: str | None = None, limit: int = 100) -> list[dict]:
        """决定历史：评审单及其结论（可按报告/病例/状态过滤）。"""
        conn = self._conn()
        try:
            where, params = [], []
            if status:
                if status not in ("OPEN", "APPROVED", "REJECTED", "INVALIDATED"):
                    raise DomainError("BAD_STATUS", f"未知状态: {status}")
                where.append("s.status=?")
                params.append(status)
            sheets = [r for r in conn.execute(
                "SELECT id FROM review_sheets s "
                + ("WHERE " + " AND ".join(where) if where else "")
                + " ORDER BY created_at DESC, id DESC LIMIT ?",
                (*params, limit)).fetchall()]
            out = []
            for r in sheets:
                d = self._load_sheet(conn, r["id"])
                if report_id and not any(
                        i["item_type"] == "PENDING_REPORT" and i["ref_id"] == report_id
                        for i in d["items"]):
                    continue
                if case_id and not any(
                        (i["item_type"] == "SERIOUSNESS_CHANGE" and i["ref_id"] == case_id)
                        or (i["item_type"] == "PENDING_REPORT"
                            and (i["snapshot"] or {}).get("case_id") == case_id)
                        for i in d["items"]):
                    continue
                d.pop("events", None)
                out.append(d)
            return out
        finally:
            conn.close()

    def audit_trail(self, *, sheet_id: str | None = None,
                    event_type: str | None = None, actor: str | None = None,
                    limit: int = 200) -> list[dict]:
        """审计查询：会签意见流水（领取/签署/否决/撤回/失效全程）。

        ``actor`` 按规范化身份匹配：空白/大小写/Unicode 等价写法都能稳定
        命中同一复核员；显式传空白/非法身份明确拒绝（与写操作一致）。
        """
        where, params = [], []
        if sheet_id:
            where.append("e.sheet_id=?")
            params.append(sheet_id)
        if event_type:
            where.append("e.event_type=?")
            params.append(event_type)
        if actor is not None:
            try:
                actor_key = identity.validate_reviewer(actor)
            except ValueError as exc:
                code, _, message = str(exc).partition(":")
                raise DomainError(code, message, 400)
            where.append("e.actor_key=?")
            params.append(actor_key)
        sql = ("SELECT e.id,e.sheet_id,e.event_type,e.actor,e.actor_key,"
               "e.detail_json,e.created_at,"
               "s.status AS sheet_status,s.revision FROM review_events e "
               "JOIN review_sheets s ON s.id=e.sheet_id "
               + ("WHERE " + " AND ".join(where) if where else "")
               + " ORDER BY e.id DESC LIMIT ?")
        conn = self._conn()
        try:
            out = []
            for r in conn.execute(sql, (*params, limit)).fetchall():
                d = dict(r)
                d["detail"] = json.loads(d.pop("detail_json"))
                out.append(d)
            return out
        finally:
            conn.close()

    def revision_history(self, limit: int = 100) -> list[dict]:
        """失效原因查询：修订号推进事件（旧单为何失效）。"""
        conn = self._conn()
        try:
            out = []
            for r in conn.execute(
                    "SELECT revision,reason_code,detail_json,actor,created_at "
                    "FROM review_revision_events ORDER BY revision DESC LIMIT ?",
                    (limit,)).fetchall():
                d = dict(r)
                d["detail"] = json.loads(d.pop("detail_json"))
                out.append(d)
            return out
        finally:
            conn.close()

    def dispatch_approvals(self, report_id: str | None = None) -> list[dict]:
        """查询会签放行记录（决定历史的发送侧视图）。"""
        conn = self._conn()
        try:
            if report_id:
                rows = conn.execute(
                    "SELECT * FROM countersign_approvals WHERE report_id=?",
                    (report_id,)).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM countersign_approvals ORDER BY approved_at DESC, "
                    "report_id").fetchall()
            out = []
            for r in rows:
                d = dict(r)
                d["approved_by"] = json.loads(d.pop("approved_by_json"))
                out.append(d)
            return out
        finally:
            conn.close()

    def is_approved_for_dispatch(self, report_id: str) -> bool:
        """发送闸门：该待发送决定是否已取得双人会签放行。"""
        conn = self._conn()
        try:
            return conn.execute(
                "SELECT 1 FROM countersign_approvals WHERE report_id=?",
                (report_id,)).fetchone() is not None
        finally:
            conn.close()
