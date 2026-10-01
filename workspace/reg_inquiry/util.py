"""通用原语：时钟、规范 JSON、内容指纹与连接工厂。"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

GENESIS = "GENESIS"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class Clock:
    """可替换时钟（测试用固定时钟/步进时钟）。"""

    def now_iso(self) -> str:
        return utc_now_iso()


class FixedClock:
    def __init__(self, start: datetime | str):
        if isinstance(start, str):
            start = datetime.fromisoformat(start)
        self.t = start

    def now_iso(self) -> str:
        return self.t.isoformat()

    def advance(self, **kwargs) -> None:
        self.t = self.t + timedelta(**kwargs)


def canonical_bytes(obj: Any) -> bytes:
    """规范 JSON：键排序、紧凑分隔、ensure_ascii=False、UTF-8。"""
    return json.dumps(obj, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), default=str).encode("utf-8")


def canonical_json(obj: Any) -> str:
    return canonical_bytes(obj).decode("utf-8")


def sha256_hex(obj: Any) -> str:
    """对 *规范序列化后的对象* 取 SHA-256 hex（内容寻址/指纹/哈希链统一入口）。"""
    return hashlib.sha256(canonical_bytes(obj)).hexdigest()


def sha256_raw(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def loads(s: str | bytes | None, default=None):
    if s is None or s == "":
        return default
    return json.loads(s)


def connect(db_path: str | Path) -> sqlite3.Connection:
    """短连接工厂（连接不跨线程共享）；WAL + busy_timeout 支撑多工作节点。"""
    if str(db_path) != ":memory:":
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=30, isolation_level=None,
                           check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn
