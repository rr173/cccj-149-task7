"""监管报送服务。

编号安全（需求硬约束：失败重试 / 进程重启绝不产生重复监管编号）：
1. regulatory_number 在病例变更同一 IMMEDIATE 事务内由序列表分配，
   reports.regulatory_number 有 UNIQUE 约束；
2. 发送只是状态机 QUEUED -> SENDING -> ACKED/FAILED，编号在创建时已固定，
   任何重试都复用同一行同一编号；
3. 网关以编号为幂等键：崩溃在“网关已受理、回执未落库”窗口时，
   重启重放只会取回同一个 ack；
4. 重启恢复把卡在 SENDING 的报告重置回 QUEUED（at-least-once 发送 +
   幂等接收 = effectively-once 且编号唯一）。
"""
from __future__ import annotations

import json
import threading
import time
from datetime import timedelta

from . import audit
from . import db as dbmod
from .gateway import RegulatorGateway, GatewayError

BACKOFF_SECONDS = [5, 30, 120, 600, 1800]
MAX_ATTEMPTS = 5


class ReportingService:
    def __init__(self, db_path: str, gateway=None, gateway_fn=None, clock=None,
                 auto_start: bool = False, poll_seconds: float = 1.0,
                 shared_lock: threading.RLock | None = None,
                 countersign_required: bool = False,
                 countersign_gate=None,
                 allow_countersign_compat: bool = False):
        self.db_path = db_path
        self._gateway = gateway
        self._gateway_fn = gateway_fn
        self.gateway_db = getattr(gateway, "conn", None)
        self._gw_local = threading.local()
        self.clock = clock
        self.poll_seconds = poll_seconds
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        # 与 CaseService 共享同一把 RLock：发送取报告与合并/拆分互斥
        self._local_lock = shared_lock or threading.RLock()
        # 双人会签闸门：countersign_gate(report_id) -> bool（是否已会签放行）。
        # countersign_required=True 时未放行的待发送决定禁止派发；
        # allow_countersign_compat=True 时原有无需会签的调用方可显式
        # 传 allow_ungated=True 继续使用（兼容开关）。
        self.countersign_required = countersign_required
        self._countersign_gate = countersign_gate
        self.allow_countersign_compat = allow_countersign_compat
        self.recover_stuck()
        if auto_start:
            self.start_dispatcher()

    @property
    def gateway(self) -> RegulatorGateway:
        """每个线程使用自己的网关连接（sqlite 连接不可跨线程共享）。"""
        gw = getattr(self._gw_local, "gw", None)
        if gw is None:
            gw = self._gateway_fn() if self._gateway_fn else self._gateway
            self._gw_local.gw = gw
        return gw

    def _conn(self):
        return dbmod.connect(self.db_path)

    def _now_iso(self) -> str:
        return self.clock.now_iso() if self.clock else \
            __import__("datetime").datetime.now().isoformat(timespec="seconds")

    # -- 崩溃恢复 ------------------------------------------------------------
    def recover_stuck(self) -> int:
        """进程重启：把 SENDING 重置为 QUEUED。编号不变、网关侧幂等。"""
        conn = self._conn()
        try:
            with dbmod.transaction(conn):
                rows = conn.execute(
                    "UPDATE regulatory_reports SET status='QUEUED', next_retry_after=NULL,"
                    "updated_at=? WHERE status='SENDING' RETURNING id",
                    (self._now_iso(),)).fetchall()
                for r in rows:
                    case_id = conn.execute(
                        "SELECT case_id FROM regulatory_reports WHERE id=?",
                        (r["id"],)).fetchone()["case_id"]
                    audit.append_event(
                        conn, case_id, "REPORT_RECOVERED",
                        "进程重启恢复：发送中断的报告重新入队（监管编号不变）",
                        {"report_id": r["id"]},
                        event_time=self._now_iso(), report_id=r["id"])
                return len(rows)
        finally:
            conn.close()

    # -- 查询 ---------------------------------------------------------------
    def list_reports(self, status: str | None = None) -> list[dict]:
        conn = self._conn()
        try:
            if status:
                rows = conn.execute("SELECT * FROM regulatory_reports WHERE status=? "
                                    "ORDER BY expedited DESC, due_date", (status,)).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM regulatory_reports ORDER BY expedited DESC, due_date, "
                    "created_at").fetchall()
            out = []
            for r in rows:
                d = dict(r)
                d.pop("priority", None)
                d["decision"] = json.loads(d.pop("decision_json"))
                d["payload"] = json.loads(d.pop("payload_json"))
                d.pop("ack_json", None)
                out.append(d)
            return out
        finally:
            conn.close()

    def get_report(self, report_id: str) -> dict:
        conn = self._conn()
        try:
            r = conn.execute("SELECT * FROM regulatory_reports WHERE id=?",
                             (report_id,)).fetchone()
            if not r:
                from .service import DomainError
                raise DomainError("REPORT_NOT_FOUND", "报告不存在", 404)
            d = dict(r)
            d["decision"] = json.loads(d.pop("decision_json"))
            d["payload"] = json.loads(d.pop("payload_json"))
            d["ack"] = json.loads(d.pop("ack_json")) if d.get("ack_json") else None
            return d
        finally:
            conn.close()

    def due(self) -> list[dict]:
        """即将到期/已逾期未确认的报告（工作台加急视图）。"""
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT id,case_id,report_type,regulatory_number,expedited,days_due,due_date,"
                "status,attempts,last_error FROM regulatory_reports "
                "WHERE status IN ('QUEUED','SENDING','FAILED') "
                "ORDER BY expedited DESC, due_date").fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    # -- 双人会签发送闸门 -----------------------------------------------------
    def _gated(self, report_id: str, allow_ungated: bool) -> bool:
        """该报告是否被会签闸门拦截。

        - 未启用强制会签：不拦截；
        - 已取得双人会签放行（countersign_approvals）：不拦截；
        - 未放行且调用方显式携带兼容开关（且系统允许兼容）：不拦截；
        - 其余：拦截。
        """
        if not self.countersign_required:
            return False
        if self._countersign_gate and self._countersign_gate(report_id):
            return False
        if allow_ungated and self.allow_countersign_compat:
            return False
        return True

    # -- 发送状态机 ----------------------------------------------------------
    def _claim(self, conn, *, allow_ungated: bool = False,
               gated_returns_none: bool = False) -> object | None:
        now = self._now_iso()
        row = conn.execute(
            "SELECT * FROM regulatory_reports WHERE status='QUEUED' "
            "AND (next_retry_after IS NULL OR next_retry_after<=?) "
            "ORDER BY expedited DESC, due_date, created_at, rowid LIMIT 1",
            (now,)).fetchone()
        if not row:
            return None
        if self._gated(row["id"], allow_ungated):
            # 队列头未会签放行：跳过它，继续在队列里寻找已放行的报告。
            remaining = conn.execute(
                "SELECT id FROM regulatory_reports WHERE status='QUEUED' AND id<>? "
                "AND (next_retry_after IS NULL OR next_retry_after<=?) "
                "ORDER BY expedited DESC, due_date, created_at, rowid",
                (row["id"], now)).fetchall()
            for cand in remaining:
                if not self._gated(cand["id"], allow_ungated):
                    row = conn.execute("SELECT * FROM regulatory_reports WHERE id=?",
                                       (cand["id"],)).fetchone()
                    break
            else:
                # 后台派发线程：整个队列都在等会签，安静等待下一周期；
                # 显式调用（send_one/pump/HTTP）：抛 403 让调用方知道原因。
                if gated_returns_none:
                    return None
                from .service import DomainError
                raise DomainError(
                    "COUNTERSIGN_REQUIRED",
                    "待发送决定尚未取得双人会签放行；请先发起并完成会签评审，"
                    "或由显式兼容开关放行", 403)
        conn.execute(
            "UPDATE regulatory_reports SET status='SENDING', attempts=attempts+1, "
            "updated_at=? WHERE id=? AND status='QUEUED'", (now, row["id"]))
        return row

    def send_one(self, report_id: str | None = None, *,
                 allow_ungated: bool = False,
                 gated_returns_none: bool = False) -> dict | None:
        """发送一份报告。返回结果字典；没有可发的则 None。

        allow_ungated=True 为显式兼容开关：仅当服务以
        allow_countersign_compat 启动时才允许跳过会签闸门。
        gated_returns_none=True 时，队列全部等待会签返回 None（后台线程用），
        否则抛 403（显式调用用）。
        """
        with self._local_lock:
            conn = self._conn()
            try:
                with dbmod.transaction(conn):
                    if report_id:
                        row = conn.execute(
                            "SELECT * FROM regulatory_reports WHERE id=?",
                            (report_id,)).fetchone()
                        if not row:
                            from .service import DomainError
                            raise DomainError("REPORT_NOT_FOUND", "报告不存在", 404)
                        if row["status"] == "VOID":
                            return {"report_id": report_id, "result": "VOID",
                                    "regulatory_number": row["regulatory_number"]}
                        if row["status"] not in ("QUEUED", "SENDING", "FAILED"):
                            return {"report_id": report_id, "result": row["status"],
                                    "regulatory_number": row["regulatory_number"]}
                        if self._gated(report_id, allow_ungated):
                            if gated_returns_none:
                                return None
                            from .service import DomainError
                            raise DomainError(
                                "COUNTERSIGN_REQUIRED",
                                f"报告 {row['regulatory_number']} 尚未取得双人会签放行；"
                                "请先完成会签评审，或由显式兼容开关放行", 403)
                        conn.execute(
                            "UPDATE regulatory_reports SET status='SENDING', "
                            "attempts=attempts+1, updated_at=? WHERE id=?",
                            (self._now_iso(), report_id))
                    else:
                        row = self._claim(conn, allow_ungated=allow_ungated,
                                          gated_returns_none=gated_returns_none)
                        if not row:
                            return None
                rid, number = row["id"], row["regulatory_number"]
                payload = json.loads(row["payload_json"])
                try:
                    resp = self.gateway.submit(number, payload)
                except GatewayError as e:
                    return self._record_failure(rid, e)
                return self._record_ack(rid, resp)
            finally:
                conn.close()

    def _record_failure(self, rid: str, err: GatewayError) -> dict:
        conn = self._conn()
        try:
            with dbmod.transaction(conn):
                r = conn.execute("SELECT * FROM regulatory_reports WHERE id=?",
                                 (rid,)).fetchone()
                attempts = r["attempts"]
                if not err.retryable or attempts >= MAX_ATTEMPTS:
                    conn.execute(
                        "UPDATE regulatory_reports SET status='FAILED', last_error=?, "
                        "updated_at=? WHERE id=?", (str(err), self._now_iso(), rid))
                    audit.append_event(
                        conn, r["case_id"], "REPORT_FAILED",
                        f"报送失败（监管编号 {r['regulatory_number']} 保留，等待人工处理）: {err}",
                        {"report_id": rid, "attempts": attempts,
                         "retryable": err.retryable, "error": str(err)},
                        event_time=self._now_iso(), report_id=rid)
                    return {"report_id": rid, "result": "FAILED", "error": str(err),
                            "regulatory_number": r["regulatory_number"]}
                backoff = BACKOFF_SECONDS[min(attempts - 1, len(BACKOFF_SECONDS) - 1)]
                if self.clock:
                    nxt = (self.clock.now() + timedelta(seconds=backoff)).isoformat(timespec="seconds")
                else:
                    from datetime import datetime
                    nxt = (datetime.now() + timedelta(seconds=backoff)).isoformat(timespec="seconds")
                conn.execute(
                    "UPDATE regulatory_reports SET status='QUEUED', last_error=?, "
                    "next_retry_after=?, updated_at=? WHERE id=?",
                    (str(err), nxt, self._now_iso(), rid))
                audit.append_event(
                    conn, r["case_id"], "REPORT_RETRY_SCHEDULED",
                    f"报送失败将重试（编号 {r['regulatory_number']} 不变，{backoff}s 后）: {err}",
                    {"report_id": rid, "attempt": attempts,
                     "retry_after": nxt, "error": str(err)},
                    event_time=self._now_iso(), report_id=rid)
                return {"report_id": rid, "result": "RETRY_SCHEDULED",
                        "retry_after": nxt, "regulatory_number": r["regulatory_number"]}
        finally:
            conn.close()

    def _record_ack(self, rid: str, resp: dict) -> dict:
        conn = self._conn()
        try:
            with dbmod.transaction(conn):
                r = conn.execute("SELECT * FROM regulatory_reports WHERE id=?",
                                 (rid,)).fetchone()
                if r["status"] == "ACKED":   # 并发/重放：保持既有回执
                    return {"report_id": rid, "result": "ALREADY_ACKED",
                            "regulatory_number": r["regulatory_number"],
                            "ack_id": r["ack_id"]}
                conn.execute(
                    "UPDATE regulatory_reports SET status='ACKED', ack_id=?, ack_json=?, "
                    "acked_at=?, last_error=NULL, next_retry_after=NULL, updated_at=? "
                    "WHERE id=?", (resp["ack_id"], json.dumps(resp, ensure_ascii=False),
                                   self._now_iso(), self._now_iso(), rid))
                audit.append_event(
                    conn, r["case_id"], "REPORT_ACKED",
                    f"监管回执 {resp['ack_id']}（编号 {r['regulatory_number']}，"
                    f"{'幂等重放' if resp.get('idempotent_replay') else '首次受理'}）",
                    {"report_id": rid, "ack": resp},
                    event_time=self._now_iso(), report_id=rid)
                return {"report_id": rid, "result": "ACKED",
                        "regulatory_number": r["regulatory_number"],
                        "ack_id": resp["ack_id"],
                        "idempotent_replay": bool(resp.get("idempotent_replay"))}
        finally:
            conn.close()

    def requeue(self, report_id: str) -> dict:
        conn = self._conn()
        try:
            with dbmod.transaction(conn):
                r = conn.execute("SELECT * FROM regulatory_reports WHERE id=?",
                                 (report_id,)).fetchone()
                if not r:
                    from .service import DomainError
                    raise DomainError("REPORT_NOT_FOUND", "报告不存在", 404)
                conn.execute(
                    "UPDATE regulatory_reports SET status='QUEUED', next_retry_after=NULL,"
                    "updated_at=? WHERE id=?", (self._now_iso(), report_id))
                audit.append_event(
                    conn, r["case_id"], "REPORT_REQUEUED",
                    f"人工重置报告 {r['regulatory_number']} 重新发送",
                    {"report_id": report_id},
                    event_time=self._now_iso(), report_id=report_id)
            return {"report_id": report_id, "status": "QUEUED"}
        finally:
            conn.close()

    def pump(self, max_batches: int = 100, *, allow_ungated: bool = False) -> dict:
        """同步排空队列（测试/脚本用）。

        allow_ungated=True 为显式兼容开关（需服务端允许兼容），
        让原有无需会签的调用继续发送。
        """
        sent, failed, retried = [], [], []
        for _ in range(max_batches):
            res = self.send_one(allow_ungated=allow_ungated)
            if res is None:
                break
            if res["result"] == "ACKED" or res["result"] == "ALREADY_ACKED":
                sent.append(res)
            elif res["result"] == "RETRY_SCHEDULED":
                retried.append(res)
            else:
                failed.append(res)
        return {"acked": sent, "retried": retried, "failed": failed}

    # -- 后台派发线程 --------------------------------------------------------
    def start_dispatcher(self):
        if self._thread and self._thread.is_alive():
            return

        def loop():
            while not self._stop.is_set():
                try:
                    # 队列全部等待会签时返回 None 安静等待，不刷错误日志
                    self.send_one(gated_returns_none=True)
                except Exception as e:   # 后台线程永不因单次异常退出
                    print(f"[dispatcher] error: {e}")
                self._stop.wait(self.poll_seconds)

        self._thread = threading.Thread(target=loop, name="pv-dispatcher", daemon=True)
        self._thread.start()

    def stop_dispatcher(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
