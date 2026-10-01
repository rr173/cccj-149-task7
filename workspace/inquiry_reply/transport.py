"""外发传输通道。

默认实现把提交件写入本地发件箱表（outbound_no 为幂等键）。真实网关适配器
应实现同样的 ``send(conn, submission, now) -> bool`` 契约：

- 返回 True  表示本次调用真正外发（此前未发）；
- 返回 False 表示该 outbound_no 已外发过（幂等去重）；
- 抛出 DomainError("TRANSPORT_ERROR", ...) 表示瞬时故障，作业将标记 FAILED
  并可由 retry_job / run_pending_jobs 恢复重试 —— 重试不得产生重复外发。
"""
from __future__ import annotations


class OutboxTransport:
    """本地发件箱通道（库内记录即“已外发”的权威凭证）。"""

    def send(self, conn, submission: dict, now: str) -> bool:
        cur = conn.execute(
            "INSERT OR IGNORE INTO outbox(outbound_no, submission_id, sent_at) "
            "VALUES(?,?,?)",
            (submission["outbound_no"], submission["submission_id"], now))
        return cur.rowcount > 0
