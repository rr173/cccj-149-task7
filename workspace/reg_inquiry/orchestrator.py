"""监管问询答复编排器（对外门面）。

一个 InquiryOrchestrator 对应一个独立 inquiry.db。所有多步写操作都以
可恢复作业执行（LOCK_DRAFT/VALIDATE/FINALIZE/SEND），处理器幂等；
构造时自动恢复孤儿作业与 SENDING 残留。

关键保证（数据库约束 + 条件 UPDATE + 同事务审计链共同成立）：
- 精确版本锁定；引用变化只另立带原因候选，绝不混入当前底稿；
- 章节乐观修订号，过期提交不覆盖并留痕；
- 定稿校验失败准确列出未满足问题，成功才生成不可变提交件；
- 发送/重试/双节点/重复请求不产生重复提交件、外发编号或履历；
- 后续修改只能产生更正件，原提交件与监管回执原样保留。
"""
from __future__ import annotations

import threading
from typing import Any, Callable

from . import audit, db as dbmod, drafts, jobs, materials, submissions, validation
from .errors import JobFailed, TransportRejectError, TransportTempError
from .transport import RegulatorTransport
from .util import Clock


class InquiryOrchestrator:
    def __init__(self, db_path: str, *, transport: RegulatorTransport | None = None,
                 gateway_db: str | None = None, clock=None,
                 lease_seconds: float = 30.0, owner: str | None = None,
                 auto_recover: bool = True):
        self.db_path = db_path
        dbmod.initialize(db_path)
        self.clock = clock or Clock()
        self.lease_seconds = lease_seconds
        self.owner = owner
        # 传输端可注入（默认在同级目录建独立网关库）
        self._transport = transport
        self._gateway_db = gateway_db
        self._lock = threading.RLock()
        self.handlers: dict[str, Callable] = {
            "LOCK_DRAFT": self._handle_lock,
            "VALIDATE": self._handle_validate,
            "FINALIZE": self._handle_finalize,
            "SEND": self._handle_send,
        }
        if auto_recover:
            self.recover()

    # -- 连接 ----------------------------------------------------------------
    def conn(self):
        return dbmod.connect(self.db_path)

    def _now(self) -> str:
        return self.clock.now_iso()

    @property
    def transport(self) -> RegulatorTransport:
        # 传输端有自己的连接：惰性创建（每个 orchestrator 实例一个）
        if self._transport is None:
            gw = self._gateway_db or (
                ":memory:" if self.db_path == ":memory:"
                else str(self.db_path).rsplit(".", 1)[0] + "_gateway.db")
            self._transport = RegulatorTransport(gw, clock=self.clock)
        return self._transport

    # =======================================================================
    # 1) 问询登记
    # =======================================================================
    def register_inquiry(self, *, regulator: str, jurisdiction: str,
                         questions: list[dict], deadline: str,
                         allowed_material_ids: list[str] | None = None,
                         allowed_categories: list[str] | None = None,
                         title: str = "", registered_by: str | None = None,
                         inquiry_id: str | None = None) -> dict:
        with self._lock, self.conn() as c, dbmod.transaction(c):
            return drafts.register_inquiry(
                c, regulator=regulator, jurisdiction=jurisdiction,
                questions=questions, deadline=deadline,
                allowed_material_ids=allowed_material_ids,
                allowed_categories=allowed_categories, title=title,
                registered_by=registered_by, inquiry_id=inquiry_id)

    def get_inquiry(self, inquiry_id: str) -> dict:
        with self.conn() as c:
            return drafts.get_inquiry(c, inquiry_id)

    def list_inquiries(self) -> list[dict]:
        with self.conn() as c:
            return drafts.list_inquiries(c)

    # =======================================================================
    # 资料注册 / 披露策略（漂移在此联动生成候选）
    # =======================================================================
    def register_material(self, *, material_id: str, version: str, category: str,
                          content: dict, title: str = "",
                          fact_key: str | None = None, fact_value: Any = None,
                          registered_by: str | None = None) -> dict:
        """资料新版本登记。若有开放底稿引用旧版本，同事务生成漂移候选。"""
        with self._lock, self.conn() as c, dbmod.transaction(c):
            row = materials.register_material(
                c, material_id=material_id, version=version, category=category,
                content=content, title=title, fact_key=fact_key,
                fact_value=fact_value, registered_by=registered_by)
            candidates = []
            if not row.get("deduped"):
                candidates = drafts.spawn_drift_candidates(
                    c, material_row_id=row["id"])
            row["spawned_candidates"] = candidates
            return row

    def put_disclosure_policy(self, *, jurisdiction: str, rules: dict,
                              created_by: str | None = None) -> dict:
        with self._lock, self.conn() as c, dbmod.transaction(c):
            return materials.put_policy(c, jurisdiction=jurisdiction, rules=rules,
                                        created_by=created_by)

    # =======================================================================
    # 2) 底稿锁定（可恢复作业）
    # =======================================================================
    def lock_draft(self, inquiry_id: str, refs: list[str], *,
                   created_by: str | None = None, wait: bool = True,
                   idempotency_key: str | None = None) -> dict:
        # 直接执行（幂等：BASE 重复锁定返回既有 OPEN BASE；作业路径同样可用）
        with self._lock, self.conn() as c, dbmod.transaction(c):
            return drafts.lock_draft(c, inquiry_id, refs, created_by=created_by)

    def lock_draft_job(self, inquiry_id: str, refs: list[str], *,
                       created_by: str | None = None, wait: bool = True,
                       idempotency_key: str | None = None) -> dict:
        payload = {"refs": refs, "created_by": created_by}
        key = idempotency_key or f"LOCK:{inquiry_id}:{__import__('hashlib').sha256(','.join(sorted(refs)).encode()).hexdigest()[:16]}"
        return self._enqueue_and_run("LOCK_DRAFT", inquiry_id, key, payload, wait=wait)

    def get_draft(self, draft_id: str) -> dict:
        with self.conn() as c:
            return drafts.get_draft(c, draft_id)

    def list_drafts(self, inquiry_id: str) -> list[dict]:
        with self.conn() as c:
            return drafts.list_drafts(c, inquiry_id)

    def _handle_lock(self, job, ctx) -> dict:
        p = job["payload"]
        with ctx.tx():
            d = drafts.lock_draft(ctx.conn, job["inquiry_id"], p["refs"],
                                  created_by=p.get("created_by"))
        return {"draft_id": d["id"]}

    # =======================================================================
    # 3) 章节协作（乐观修订号，直接提交）
    # =======================================================================
    def submit_section(self, draft_id: str, qid: str, body: str, *,
                       expected_revision: int, title: str = "",
                       submitted_by: str | None = None) -> dict:
        """乐观并发提交。

        过期提交不覆盖新内容；冲突留痕在独立事务中保留（业务事务回滚不影响
        留痕），随后把 STALE_REVISION 抛给调用方。
        """
        from .errors import StaleRevisionError
        with self._lock:
            c = self.conn()
            try:
                with dbmod.transaction(c):
                    return drafts.submit_section(
                        c, draft_id, qid, body, expected_revision=expected_revision,
                        title=title, submitted_by=submitted_by)
            except StaleRevisionError:
                with dbmod.transaction(c):
                    drafts.record_stale_conflict(
                        c, draft_id, qid, expected_revision, body, submitted_by)
                raise

    def section_history(self, section_id: str) -> list[dict]:
        with self.conn() as c:
            return drafts.section_history(c, section_id)

    def add_attachment(self, draft_id: str, *, filename: str, content: bytes,
                       content_type: str = "application/octet-stream",
                       derived_from_ref: str | None = None,
                       uploaded_by: str | None = None,
                       attachment_id: str | None = None) -> dict:
        with self._lock, self.conn() as c, dbmod.transaction(c):
            return drafts.add_attachment(
                c, draft_id, filename=filename, content=content,
                content_type=content_type, derived_from_ref=derived_from_ref,
                uploaded_by=uploaded_by, attachment_id=attachment_id)

    def get_attachment(self, draft_id: str, attachment_id: str) -> dict:
        with self.conn() as c:
            return drafts.get_attachment_content(c, draft_id, attachment_id)

    # =======================================================================
    # 4) 冲突查询
    # =======================================================================
    def conflicts(self, draft_id: str | None = None) -> dict:
        """返回章节竞争冲突 + 被锁版本矛盾 + 引用漂移警告。"""
        with self.conn() as c:
            sec_conflicts = drafts.list_conflicts(c, draft_id)
            out = {"section_conflicts": sec_conflicts}
            if draft_id:
                d = drafts.get_draft(c, draft_id)
                out["contradictions"] = [dict(r) for r in c.execute(
                    "SELECT * FROM contradictions WHERE draft_id=? ORDER BY rowid",
                    (draft_id,))]
                out["pins"] = d["pins"]
                out["drift_warnings"] = validation.check_drift(c, d)
                drifts = c.execute(
                    "SELECT * FROM drift_events WHERE source_draft_id=? "
                    "ORDER BY created_at", (draft_id,)).fetchall()
                out["drift_events"] = [dict(r) for r in drifts]
            else:
                out["contradictions"] = [dict(r) for r in c.execute(
                    "SELECT * FROM contradictions ORDER BY rowid")]
            return out

    # =======================================================================
    # 5) 定稿校验（只读报告 / 可恢复作业）
    # =======================================================================
    def validate(self, draft_id: str) -> dict:
        with self.conn() as c:
            return validation.build_report(c, draft_id)

    def validate_job(self, draft_id: str, *, wait: bool = True) -> dict:
        with self.conn() as c:
            d = drafts.get_draft(c, draft_id, include=False)
        return self._enqueue_and_run(
            "VALIDATE", d["inquiry_id"], f"VALIDATE:{draft_id}",
            {"draft_id": draft_id}, wait=wait)

    def _handle_validate(self, job, ctx) -> dict:
        return {"report": validation.build_report(ctx.conn, job["payload"]["draft_id"])}

    # =======================================================================
    # 6) 定稿封存（可恢复作业）
    # =======================================================================
    def finalize(self, draft_id: str, *, sealed_by: str | None = None,
                 wait: bool = True) -> dict:
        with self.conn() as c:
            d = drafts.get_draft(c, draft_id, include=False)
        return self._enqueue_and_run(
            "FINALIZE", d["inquiry_id"], f"FINALIZE:{draft_id}",
            {"draft_id": draft_id, "sealed_by": sealed_by},
            target_draft_id=draft_id, wait=wait)

    def _handle_finalize(self, job, ctx) -> dict:
        p = job["payload"]
        with ctx.tx():
            sub = submissions.seal(ctx.conn, p["draft_id"],
                                   sealed_by=p.get("sealed_by"))
        return {"submission_id": sub["id"],
                "outgoing_number": sub["outgoing_number"]}

    # =======================================================================
    # 7) 发送与重试（可恢复作业；事务间调用外部传输）
    # =======================================================================
    def send(self, submission_id: str, *, wait: bool = True) -> dict:
        with self.conn() as c:
            sub = submissions.get_submission(c, submission_id)
        return self._enqueue_and_run(
            "SEND", sub["inquiry_id"], f"SEND:{submission_id}",
            {"submission_id": submission_id},
            target_submission_id=submission_id, wait=wait)

    def _handle_send(self, job, ctx) -> dict:
        sid = job["payload"]["submission_id"]
        # 步骤一：事务内领取（SEALED/QUEUED/FAILED -> SENDING）。
        # 重复请求虽然因幂等键复用同一作业，但可能在租约过期后再次被领取；
        # 因此以 *提交件当前状态* 为最终判据：已 SENT/DELIVERED/REJECTED
        # 直接返回现状，绝不重复外发。
        with ctx.tx():
            current = submissions.get_submission(ctx.conn, sid)
            if current["status"] in ("SENT", "DELIVERED", "REJECTED"):
                return {"submission_id": sid, "status": current["status"],
                        "outgoing_number": current["outgoing_number"],
                        "skipped": True}
            # 崩溃恢复窗口：回执已经登记（监管侧已受理），直接回到 DELIVERED，
            # 绝不再次外发产生第二条受理。
            existing_receipt = ctx.conn.execute(
                "SELECT * FROM receipts WHERE submission_id=? ORDER BY created_at "
                "LIMIT 1", (sid,)).fetchone()
            if existing_receipt:
                ctx.conn.execute(
                    "UPDATE submissions SET status='DELIVERED' WHERE id=?", (sid,))
                return {"submission_id": sid, "status": "DELIVERED",
                        "outgoing_number": current["outgoing_number"],
                        "recovered_from_receipt": True, "skipped": True}
            sub = submissions.claim_for_sending(ctx.conn, sid, now=self._now())
        if sub is None:
            # SENDING 正由其他节点持有：不抢，如实返回（调用方可稍后再查）
            fresh = submissions.get_submission(ctx.conn, sid)
            return {"submission_id": sid, "status": fresh["status"],
                    "outgoing_number": fresh["outgoing_number"], "skipped": False}

        ctx.checkpoint({"step": "SENDING", "attempt": sub["attempts"]})

        # 步骤二：事务外调用监管传输（以编号为幂等键；重复外发只受理一次）
        payload = submissions.send_payload(sub)
        try:
            response = self.transport.submit(payload)
        except TransportTempError as exc:
            with ctx.tx():
                submissions.mark_failed(ctx.conn, sid, "TRANSPORT_TEMP", str(exc),
                                        retryable=True, now=self._now())
            raise
        except TransportRejectError as exc:
            with ctx.tx():
                submissions.mark_failed(ctx.conn, sid, "TRANSPORT_REJECT", str(exc),
                                        retryable=False, now=self._now())
            raise

        # 步骤三：事务内落 SENT（幂等履历 + 状态栅栏）
        with ctx.tx():
            submissions.mark_sent(ctx.conn, sid, ack_hint=response, now=self._now())
            audit.append_event(
                ctx.conn, "SUBMISSION_SENT",
                f"外发成功 {sub['outgoing_number']}",
                payload={"submission_id": sid, "attempt": sub["attempts"],
                         "response": response},
                submission_id=sid, inquiry_id=sub["inquiry_id"],
                idempotency_key=f"SENT:{sid}:{sub['attempts']}",
                created_at=self._now())
        return {"submission_id": sid, "status": "SENT",
                "outgoing_number": sub["outgoing_number"], "response": response}

    def submission_status(self, submission_id: str) -> dict:
        with self.conn() as c:
            sub = submissions.get_submission(c, submission_id)
            sub["events"] = submissions.events(c, submission_id)
            return sub

    def list_submissions(self, inquiry_id: str) -> list[dict]:
        with self.conn() as c:
            return submissions.list_submissions(c, inquiry_id)

    # =======================================================================
    # 8) 回执登记
    # =======================================================================
    def register_receipt(self, submission_id: str, *, receipt_number: str,
                         payload: dict, received_at: str | None = None,
                         registered_by: str | None = None) -> dict:
        with self._lock, self.conn() as c, dbmod.transaction(c):
            return submissions.register_receipt(
                c, submission_id, receipt_number=receipt_number, payload=payload,
                received_at=received_at, registered_by=registered_by)

    def list_receipts(self, submission_id: str) -> list[dict]:
        with self.conn() as c:
            return submissions.list_receipts(c, submission_id)

    # =======================================================================
    # 9) 更正关系
    # =======================================================================
    def create_correction_draft(self, submission_id: str, *,
                                refs: list[str] | None = None, reason: str = "",
                                created_by: str | None = None) -> dict:
        with self._lock, self.conn() as c, dbmod.transaction(c):
            return submissions.create_correction_draft(
                c, submission_id, refs=refs, reason=reason, created_by=created_by)

    def correction_chain(self, submission_id: str) -> dict:
        with self.conn() as c:
            return submissions.correction_chain(c, submission_id)

    # =======================================================================
    # 10) 独立核对
    # =======================================================================
    def verify_submission(self, submission_id: str) -> dict:
        from .verifier import verify_submission
        with self.conn() as c:
            return verify_submission(c, submission_id)

    def verify_all(self) -> dict:
        from .verifier import verify_all
        with self.conn() as c:
            return verify_all(c)

    # =======================================================================
    # 11) 操作留痕
    # =======================================================================
    def audit_trail(self, *, inquiry_id: str | None = None,
                    event_type: str | None = None, limit: int | None = None
                    ) -> list[dict]:
        with self.conn() as c:
            return audit.list_events(c, inquiry_id=inquiry_id,
                                     event_type=event_type, limit=limit)

    def verify_audit_chain(self) -> dict:
        with self.conn() as c:
            failures = audit.verify_chain(audit.list_events(c))
            return {"ok": not failures, "failures": failures}

    # =======================================================================
    # 作业与恢复
    # =======================================================================
    def recover(self) -> dict:
        """节点启动/人工恢复：孤儿作业回 PENDING，SENDING 回 QUEUED。"""
        with self._lock, self.conn() as c, dbmod.transaction(c):
            return jobs.recover(c, now=self._now())

    def _enqueue_and_run(self, job_type: str, inquiry_id: str, key: str,
                         payload: dict, *, wait: bool = True,
                         target_draft_id: str | None = None,
                         target_submission_id: str | None = None) -> dict:
        """入队并（默认）同步运行到完成；重复键返回同一作业结果。

        校验/定稿类作业曾永久失败（如 VALIDATION_FAILED）后，调用方补齐内容
        重新请求时：旧 FAILED 行改名留痕，新作业占用同一幂等键重跑。
        """
        with self._lock, self.conn() as c, dbmod.transaction(c):
            job, created = jobs.enqueue(
                c, job_type=job_type, inquiry_id=inquiry_id,
                idempotency_key=key, payload=payload,
                target_draft_id=target_draft_id,
                target_submission_id=target_submission_id, reopen_failed=True)
        if not wait:
            return {"job_id": job["id"], "status": job["status"], "enqueued": created}
        return self.run_job(job["id"])

    def run_job(self, job_id: str) -> dict:
        """运行一个作业到终态（永久失败抛 JobFailed）。

        - 进入时若作业已 COMPLETED：这是重复请求，重放处理器取幂等结果
          （不产生第二条提交件/编号/履历）；
        - 否则领取并执行；临时失败作业回 PENDING 后原地重试，成功结果直接返回；
        - 被其他节点持有时短轮询等待其终态，绝不重复执行副作用。
        """
        import time
        runner = jobs.JobRunner(self.db_path, self.handlers, clock=self.clock,
                                lease_seconds=self.lease_seconds,
                                owner=self.owner)
        for _ in range(1000):
            with self._lock, self.conn() as c:
                j = jobs.get_job(c, job_id)
                if j["status"] == "COMPLETED":
                    return self._replay_completed(c, j)
                if j["status"] == "FAILED":
                    raise JobFailed(j)
                completed_result = None
                try:
                    r = runner.run_once(c)
                except Exception:
                    # 处理器已分类落库（FAILED 永久 / PENDING 临时）
                    j2 = jobs.get_job(c, job_id)
                    if j2["status"] == "FAILED":
                        raise JobFailed(j2) from None
                    # 临时失败：作业已回 PENDING，下一轮重新领取重试
                    continue
                if r is not None and not r.get("stale"):
                    j2 = jobs.get_job(c, job_id)
                    if j2["status"] == "COMPLETED":
                        completed_result = j2["result"]
            if completed_result is not None:
                return completed_result
            # r is None：没有领到（被其他节点持有）或结果过期；等待其终态
            time.sleep(0.02)
            with self.conn() as c:
                j = jobs.get_job(c, job_id)
                if j["status"] == "COMPLETED":
                    # 区分：本轮自己刚完成（结果已在上面返回）；能走到这里说明
                    # 是其他节点完成的 -> 幂等重放取当前状态
                    return self._replay_completed(c, j)
                if j["status"] == "FAILED":
                    raise JobFailed(j)
        with self.conn() as c:
            raise JobFailed(jobs.get_job(c, job_id))

    def _replay_completed(self, c, job: dict) -> dict:
        """对已 COMPLETED 的幂等作业重新执行处理器以取回当前幂等结果。

        处理器均以当前状态为判据短路（seal/send/lock 天然幂等，validate 只读），
        因此不会产生第二条提交件、编号或履历。
        """
        from .jobs import JobContext
        ctx = JobContext(job, c, lease_seconds=self.lease_seconds,
                         now_fn=self._now, owner=self.owner)
        return self.handlers[job["job_type"]](job, ctx) or {}

    def pump(self, max_jobs: int = 100) -> list[dict]:
        """工作节点主循环一步：领取并执行所有当前可执行作业。"""
        runner = jobs.JobRunner(self.db_path, self.handlers, clock=self.clock,
                                lease_seconds=self.lease_seconds,
                                owner=self.owner)
        with self._lock, self.conn() as c:
            return runner.drain(c, max_jobs=max_jobs)

    def list_jobs(self, *, status: str | None = None,
                  inquiry_id: str | None = None) -> list[dict]:
        with self.conn() as c:
            return jobs.list_jobs(c, status=status, inquiry_id=inquiry_id)
