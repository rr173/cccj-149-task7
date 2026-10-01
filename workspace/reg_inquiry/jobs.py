"""可恢复作业：组稿（锁定）、校验、定稿、发送统一走作业。

- 业务幂等键唯一（``jobs.idempotency_key UNIQUE``）：重复请求只入队一次；
- 领取是条件 UPDATE：``PENDING`` 或租约过期的 ``RUNNING`` 才可能被领取，
  两个工作节点同时处理同一答复键只有一个成功；
- ``attempt`` 是领取栅栏：完成/失败必须带回自己的 attempt，过期工作者
  （租约已被别人接管）的结果写不进去，不会覆盖新一轮执行；
- 崩溃恢复把过期 RUNNING 重置回 PENDING，并把卡在 SENDING 的提交件
  重置回 QUEUED；处理器全部写成幂等，从 checkpoint 继续未完成步骤。
"""
from __future__ import annotations

import json
import uuid
from typing import Callable

from . import submissions
from . import db as dbmod
from .errors import InquiryError, TransportTempError
from .util import loads, utc_now_iso

JOB_TYPES = ("LOCK_DRAFT", "VALIDATE", "FINALIZE", "SEND")


def _lease_expiry(now_iso: str, lease_seconds: float) -> str:
    from datetime import datetime, timedelta
    t = datetime.fromisoformat(now_iso)
    return (t + timedelta(seconds=lease_seconds)).isoformat()


def enqueue(conn, *, job_type: str, inquiry_id: str, idempotency_key: str,
            payload: dict | None = None, target_draft_id: str | None = None,
            target_submission_id: str | None = None, created_at: str | None = None,
            reopen_failed: bool = False) -> tuple[dict, bool]:
    """入队（幂等）。返回 (job, newly_created)。

    - 已存在 COMPLETED/RUNNING/PENDING 作业：原样返回（重复请求不重复执行）；
    - 已存在 FAILED 作业且 reopen_failed=True：保留旧失败行留痕，新建同键
      作业重新执行（校验失败补齐内容后重新定稿的场景）。UNIQUE 约束要求先把
      旧键改名（加 #failed-<id> 后缀），原行仍可查询、不可删除。
    """
    if job_type not in JOB_TYPES:
        raise ValueError(f"未知作业类型: {job_type}")
    existing = conn.execute("SELECT * FROM jobs WHERE idempotency_key=?",
                            (idempotency_key,)).fetchone()
    if existing:
        if existing["status"] == "FAILED" and reopen_failed:
            conn.execute(
                "UPDATE jobs SET idempotency_key=? WHERE id=?",
                (f"{idempotency_key}#failed-{existing['id']}", existing["id"]))
        else:
            return (dict(existing), False)
    now = created_at or utc_now_iso()
    jid = uuid.uuid4().hex
    conn.execute(
        "INSERT INTO jobs(id,job_type,inquiry_id,idempotency_key,target_draft_id,"
        "target_submission_id,payload_json,status,attempt,created_at,updated_at)"
        " VALUES(?,?,?,?,?,?,?,'PENDING',0,?,?)",
        (jid, job_type, inquiry_id, idempotency_key, target_draft_id,
         target_submission_id,
         json.dumps(payload or {}, ensure_ascii=False, sort_keys=True, default=str),
         now, now))
    return (dict(conn.execute("SELECT * FROM jobs WHERE id=?", (jid,)).fetchone()),
            True)


def claim(conn, *, lease_seconds: float = 30.0, now: str | None = None,
          owner: str | None = None) -> dict | None:
    """原子领取一个待执行作业（含租约过期的孤儿作业）。

    领取本身是一个独立 IMMEDIATE 短事务：两个节点同时抢同一作业时，
    SQLite 写锁串行 + ``WHERE status='PENDING' OR 租约过期`` 条件保证
    只有一方的条件 UPDATE 命中。
    """
    now = now or utc_now_iso()
    expires = _lease_expiry(now, lease_seconds)
    with dbmod.transaction(conn):
        row = conn.execute(
            "UPDATE jobs SET status='RUNNING', attempt=attempt+1, lease_owner=?,"
            "leased_at=?, lease_expires_at=?, updated_at=? "
            "WHERE id=(SELECT id FROM jobs WHERE status='PENDING' OR "
            "(status='RUNNING' AND lease_expires_at IS NOT NULL AND "
            "lease_expires_at<=?) ORDER BY created_at,id LIMIT 1) RETURNING *",
            (owner or uuid.uuid4().hex[:8], now, expires, now, now)).fetchone()
        return _job_dict(row) if row else None


def heartbeat(conn, job_id: str, attempt: int, *, checkpoint: dict | None = None,
              lease_seconds: float = 30.0, now: str | None = None) -> bool:
    """续租并写检查点（仅当前 attempt 的持有者成功）。"""
    now = now or utc_now_iso()
    expires = _lease_expiry(now, lease_seconds)
    row = conn.execute(
        "UPDATE jobs SET lease_expires_at=?, updated_at=?,"
        "checkpoint_json=COALESCE(?,checkpoint_json) WHERE id=? AND attempt=? "
        "AND status='RUNNING' RETURNING id",
        (expires, now,
         json.dumps(checkpoint, ensure_ascii=False, sort_keys=True)
         if checkpoint is not None else None, job_id, attempt)).fetchone()
    return row is not None


def complete(conn, job_id: str, attempt: int, result: dict, *,
             now: str | None = None) -> bool:
    now = now or utc_now_iso()
    row = conn.execute(
        "UPDATE jobs SET status='COMPLETED', result_json=?, finished_at=?, "
        "updated_at=?, lease_owner=NULL WHERE id=? AND attempt=? "
        "AND status='RUNNING' RETURNING id",
        (json.dumps(result or {}, ensure_ascii=False, sort_keys=True, default=str),
         now, now, job_id, attempt)).fetchone()
    return row is not None


def fail(conn, job_id: str, attempt: int, error_code: str, message: str, *,
         permanent: bool = False, now: str | None = None) -> bool:
    now = now or utc_now_iso()
    if permanent:
        row = conn.execute(
            "UPDATE jobs SET status='FAILED', error_code=?, error_message=?,"
            "permanent=1, finished_at=?, updated_at=?, lease_owner=NULL "
            "WHERE id=? AND attempt=? AND status='RUNNING' RETURNING id",
            (error_code, message, now, now, job_id, attempt)).fetchone()
    else:
        # 临时失败：回到 PENDING 等待恢复/重试（attempt 保留，作为本轮栅栏）
        row = conn.execute(
            "UPDATE jobs SET status='PENDING', error_code=?, error_message=?,"
            "updated_at=?, lease_owner=NULL WHERE id=? AND attempt=? "
            "AND status='RUNNING' RETURNING id",
            (error_code, message, now, job_id, attempt)).fetchone()
    return row is not None


def get_job(conn, job_id: str) -> dict:
    r = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    if not r:
        raise InquiryError(f"作业不存在: {job_id}")
    return _job_dict(r)


def _job_dict(r) -> dict:
    d = dict(r)
    d["payload"] = loads(d.pop("payload_json"), {})
    d["checkpoint"] = loads(d.pop("checkpoint_json"), {})
    d["result"] = loads(d.pop("result_json"), None)
    return d


def list_jobs(conn, *, status: str | None = None, inquiry_id: str | None = None
              ) -> list[dict]:
    where, params = [], []
    if status:
        where.append("status=?")
        params.append(status)
    if inquiry_id:
        where.append("inquiry_id=?")
        params.append(inquiry_id)
    sql = "SELECT * FROM jobs"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY created_at"
    return [_job_dict(r) for r in conn.execute(sql, params)]


def recover(conn, *, now: str | None = None) -> dict:
    """进程/节点恢复：孤儿作业回 PENDING；SENDING 提交件回 QUEUED。"""
    now = now or utc_now_iso()
    orphan_jobs = conn.execute(
        "UPDATE jobs SET status='PENDING', lease_owner=NULL, updated_at=? "
        "WHERE status='RUNNING' AND lease_expires_at IS NOT NULL "
        "AND lease_expires_at<=?", (now, now)).rowcount
    sending = submissions.recover_sending(conn)
    return {"requeued_jobs": orphan_jobs, "requeued_sending": sending}


# ---------------------------------------------------------------------------
# 执行器
# ---------------------------------------------------------------------------
Handler = Callable[[dict, "JobContext"], dict]


class JobContext:
    def __init__(self, job: dict, conn, *, lease_seconds: float, now_fn,
                 owner: str | None):
        self.job = job
        self.conn = conn
        self.lease_seconds = lease_seconds
        self._now_fn = now_fn
        self.owner = owner

    def tx(self):
        """处理器内开启一个 IMMEDIATE 短事务（可多次；步骤级原子/续跑）。"""
        from . import db as dbmod
        return dbmod.transaction(self.conn)

    def checkpoint(self, data: dict) -> None:
        with self.tx():
            heartbeat(self.conn, self.job["id"], self.job["attempt"],
                      checkpoint=data, lease_seconds=self.lease_seconds,
                      now=self._now_fn())


class JobRunner:
    """把作业类型映射到处理器。

    领取在独立短事务内完成；处理器通过 ``ctx.tx()`` 自行管理步骤级事务
    （SEND 需要在事务之间进行外部网络 I/O，不能包在单个长事务里）。
    完成/失败带 attempt 栅栏：租约过期被他人接管后，旧持有者的结果写不回。
    """

    def __init__(self, db_path: str, handlers: dict[str, Handler], *,
                 clock=None, lease_seconds: float = 30.0, owner: str | None = None):
        self.db_path = db_path
        self.handlers = handlers
        self.clock = clock
        self.lease_seconds = lease_seconds
        self.owner = owner

    def _now(self) -> str:
        return self.clock.now_iso() if self.clock else utc_now_iso()

    def run_once(self, conn) -> dict | None:
        """领取并执行至多一个作业，返回 {job, result}；无作业返回 None。"""
        job = claim(conn, lease_seconds=self.lease_seconds, now=self._now(),
                    owner=self.owner)
        if job is None:
            return None
        handler = self.handlers.get(job["job_type"])
        ctx = JobContext(job, conn, lease_seconds=self.lease_seconds,
                         now_fn=self._now, owner=self.owner)
        if handler is None:
            with dbmod.transaction(conn):
                fail(conn, job["id"], job["attempt"], "NO_HANDLER",
                     f"作业类型 {job['job_type']} 无处理器", permanent=True,
                     now=self._now())
            raise InquiryError(f"无处理器: {job['job_type']}")
        try:
            result = handler(job, ctx) or {}
        except Exception as exc:  # noqa: BLE001 - 作业失败要分类落库
            self._classify_and_fail(conn, job, exc)
            raise
        with dbmod.transaction(conn):
            ok = complete(conn, job["id"], job["attempt"], result, now=self._now())
        if not ok:
            return {"job": job, "stale": True}
        return {"job": job, "result": result}

    def _classify_and_fail(self, conn, job: dict, exc: Exception) -> None:
        # 校验失败/领域错误/传输拒收=永久；TransportTemp=可恢复（回 PENDING）
        permanent = not isinstance(exc, TransportTempError)
        code = getattr(exc, "code", exc.__class__.__name__)
        with dbmod.transaction(conn):
            fail(conn, job["id"], job["attempt"], code, str(exc),
                 permanent=permanent, now=self._now())

    def drain(self, conn, max_jobs: int = 100) -> list[dict]:
        out = []
        for _ in range(max_jobs):
            r = self.run_once(conn)
            if r is None:
                break
            if not r.get("stale"):
                out.append(r)
        return out
