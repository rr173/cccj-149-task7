"""模拟监管方接收端（独立 SQLite，物理隔离）。

- 以 ``outgoing_number`` 为幂等键：重复提交永远返回同一受理号，
  与业务库的编号 UNIQUE + 发送履历去重构成双端幂等；
- 可脚本化临时故障/明确拒收，验证重试不产生重复提交件或履历；
- 受理计数 ``accept_count`` 让测试精确断言“只外发一次”。
"""
from __future__ import annotations

import uuid
from pathlib import Path

from .errors import TransportRejectError, TransportTempError
from .util import connect, utc_now_iso

GATEWAY_SCHEMA = """
CREATE TABLE IF NOT EXISTS gateway_submissions (
    outgoing_number TEXT PRIMARY KEY,
    accepted_at     TEXT NOT NULL,
    response_json   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS gateway_attempts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    outgoing_number TEXT NOT NULL,
    outcome         TEXT NOT NULL,
    detail          TEXT,
    attempted_at    TEXT NOT NULL
);
"""


class RegulatorTransport:
    def __init__(self, gateway_db: str, *, clock=None, fail_times: int = 0,
                 reject: bool = False, reject_reason: str = "REJECTED"):
        if gateway_db != ":memory:":
            Path(gateway_db).parent.mkdir(parents=True, exist_ok=True)
        self.conn = connect(gateway_db)
        self.conn.executescript(GATEWAY_SCHEMA)
        self.fail_times = fail_times      # 前 N 次提交临时失败
        self.reject = reject
        self.reject_reason = reject_reason
        self.clock = clock

    def _now(self) -> str:
        return self.clock.now_iso() if self.clock else utc_now_iso()

    def submit(self, payload: dict) -> dict:
        number = payload["outgoing_number"]
        existing = self.conn.execute(
            "SELECT response_json FROM gateway_submissions WHERE outgoing_number=?",
            (number,)).fetchone()
        if existing:
            import json
            self._record(number, "DUPLICATE_OK", "返回既有受理（幂等）")
            return {**json.loads(existing["response_json"]),
                    "idempotent_replay": True}
        if self.reject:
            self._record(number, "REJECTED", self.reject_reason)
            raise TransportRejectError(f"监管方拒收：{self.reject_reason}")
        if self.fail_times > 0:
            self.fail_times -= 1
            self._record(number, "TEMP_FAIL", "模拟临时故障")
            raise TransportTempError("监管网关临时不可用，请重试")
        import json
        accepted = self._now()
        receipt_number = f"ACK-{uuid.uuid4().hex[:12].upper()}"
        response = {"receipt_number": receipt_number, "accepted_at": accepted,
                    "outgoing_number": number, "status": "ACCEPTED"}
        self.conn.execute(
            "INSERT INTO gateway_submissions(outgoing_number,accepted_at,"
            "response_json) VALUES(?,?,?)",
            (number, accepted, json.dumps(response, ensure_ascii=False,
                                          sort_keys=True)))
        self._record(number, "ACCEPTED", receipt_number)
        return response

    def _record(self, number: str, outcome: str, detail: str) -> None:
        self.conn.execute(
            "INSERT INTO gateway_attempts(outgoing_number,outcome,detail,"
            "attempted_at) VALUES(?,?,?,?)",
            (number, outcome, detail, self._now()))

    def accept_count(self, number: str) -> int:
        return self.conn.execute(
            "SELECT COUNT(*) AS n FROM gateway_attempts WHERE outgoing_number=? "
            "AND outcome='ACCEPTED'", (number,)).fetchone()["n"]

    def total_attempts(self, number: str) -> int:
        return self.conn.execute(
            "SELECT COUNT(*) AS n FROM gateway_attempts WHERE outgoing_number=?",
            (number,)).fetchone()["n"]
