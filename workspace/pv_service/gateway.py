"""模拟监管机构网关。

- 独立的 SQLite 库，与主业务库物理隔离（真实世界是外部系统）。
- 以 regulatory_number 为幂等键：重复提交永远返回同一个 ack，
  配合业务库编号 UNIQUE 约束，构成“客户端+服务端”双端幂等。
- 可脚本化故障（前 N 次 500/超时），用于验证失败重试不会产生重复编号。
"""
from __future__ import annotations

import json
import uuid
from pathlib import Path

from . import db as dbmod

GATEWAY_SCHEMA = """
CREATE TABLE IF NOT EXISTS gateway_receipts (
    regulatory_number TEXT PRIMARY KEY,
    ack_id            TEXT NOT NULL,
    received_at       TEXT NOT NULL,
    response_json     TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS gateway_attempts (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    regulatory_number TEXT NOT NULL,
    outcome           TEXT NOT NULL,
    detail            TEXT,
    attempted_at      TEXT NOT NULL
);
"""


class GatewayError(Exception):
    def __init__(self, detail: str, retryable: bool = True):
        super().__init__(detail)
        self.retryable = retryable


class RegulatorGateway:
    def __init__(self, gateway_db: str, clock=None, fail_script: dict | None = None):
        Path(gateway_db).parent.mkdir(parents=True, exist_ok=True)
        self.conn = dbmod.connect(gateway_db)
        self.conn.executescript(GATEWAY_SCHEMA)
        self.fail_script = fail_script or {}
        self.clock = clock

    def _now(self) -> str:
        return self.clock.now_iso() if self.clock else __import__("datetime").datetime.now().isoformat(timespec="seconds")

    def submit(self, regulatory_number: str, payload: dict) -> dict:
        # 幂等：编号已经受理过 -> 原样返回既有 ack（重试/崩溃恢复的关键）
        existing = self.conn.execute(
            "SELECT ack_id, response_json FROM gateway_receipts WHERE regulatory_number=?",
            (regulatory_number,)).fetchone()
        if existing:
            self._record_attempt(regulatory_number, "DUPLICATE_OK",
                                 "返回既有回执（幂等）")
            return {"ack_id": existing["ack_id"], "idempotent_replay": True,
                    **json.loads(existing["response_json"])}

        attempts = self.conn.execute(
            "SELECT COUNT(*) AS n FROM gateway_attempts WHERE regulatory_number=? "
            "AND outcome='FAILED'", (regulatory_number,)).fetchone()["n"]
        fail_times = self.fail_script.get("fail_times", 0)
        fail_numbers = set(self.fail_script.get("fail_numbers", []))
        if attempts < fail_times or regulatory_number in fail_numbers:
            # fail_numbers 永久失败（验证重试上限与人工介入）；fail_times 为临时故障
            if regulatory_number in fail_numbers:
                self._record_attempt(regulatory_number, "FAILED", "永久拒收 500")
                raise GatewayError("gateway 500: permanent rejection", retryable=False)
            self._record_attempt(regulatory_number, "FAILED", f"第 {attempts + 1} 次 500/超时")
            raise GatewayError(f"gateway 500 (attempt {attempts + 1})", retryable=True)

        ack_id = "ACK-" + uuid.uuid4().hex[:12].upper()
        response = {"status": "RECEIVED", "regulatory_number": regulatory_number}
        self.conn.execute(
            "INSERT INTO gateway_receipts(regulatory_number,ack_id,received_at,response_json)"
            " VALUES(?,?,?,?)",
            (regulatory_number, ack_id, self._now(), json.dumps(response, ensure_ascii=False)))
        self._record_attempt(regulatory_number, "OK", "受理成功")
        return {"ack_id": ack_id, "idempotent_replay": False, **response}

    def _record_attempt(self, number, outcome, detail):
        self.conn.execute(
            "INSERT INTO gateway_attempts(regulatory_number,outcome,detail,attempted_at)"
            " VALUES(?,?,?,?)", (number, outcome, detail, self._now()))

    def receipt_count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) AS n FROM gateway_receipts").fetchone()["n"]

    def close(self):
        self.conn.close()
