"""可引用资料库：病例 / 已发送报文 / 适用法规 / 统计结论的版本化存储。

该组件模拟编排器外部的资料源：资料以只增版本演进，任何修改都产生新版本，
旧版本永远可取回 —— 底稿因此能锁定 *精确版本*（版本号 + 内容哈希），
独立核对因此能在事后重算哈希验证提交件未被篡改。
"""
from __future__ import annotations

import hashlib

from . import db as dbmod
from .errors import DomainError

KINDS = ("CASE", "MESSAGE", "REGULATION", "STATISTIC")


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def fetch(conn, evidence_id: str, version: int | None = None):
    """读取资料版本；version=None 取当前最新版本。不存在返回 None。"""
    if version is None:
        row = conn.execute(
            "SELECT * FROM evidence_versions WHERE evidence_id=? "
            "ORDER BY version DESC LIMIT 1", (evidence_id,)).fetchone()
    else:
        row = conn.execute(
            "SELECT * FROM evidence_versions WHERE evidence_id=? AND version=?",
            (evidence_id, version)).fetchone()
    return dict(row) if row else None


def current_version(conn, evidence_id: str) -> int | None:
    row = conn.execute(
        "SELECT MAX(version) AS v FROM evidence_versions WHERE evidence_id=?",
        (evidence_id,)).fetchone()
    return row["v"]


class EvidenceStore:
    """资料源写入/读取入口（每次 upsert 产生一个新版本）。"""

    def __init__(self, db_path: str, *, clock=None):
        self.db_path = str(db_path)
        self.clock = clock

    def _now(self) -> str:
        from .service import Clock  # 延迟导入避免环
        return (self.clock or Clock()).now_iso()

    def upsert(self, evidence_id: str, kind: str, content: str) -> dict:
        """写入资料新版本，返回版本记录（含 version 与 content_hash）。"""
        if kind not in KINDS:
            raise DomainError("BAD_FIELD", f"未知资料类型 {kind}，应为 {KINDS}")
        conn = dbmod.connect(self.db_path)
        try:
            with dbmod.transaction(conn):
                version = (current_version(conn, evidence_id) or 0) + 1
                record = {
                    "evidence_id": evidence_id, "version": version, "kind": kind,
                    "content": content, "content_hash": sha256_text(content),
                    "updated_at": self._now(),
                }
                conn.execute(
                    "INSERT INTO evidence_versions(evidence_id,version,kind,content,"
                    "content_hash,updated_at) VALUES(?,?,?,?,?,?)",
                    (evidence_id, version, kind, content,
                     record["content_hash"], record["updated_at"]))
                return record
        finally:
            conn.close()

    def get(self, evidence_id: str, version: int | None = None) -> dict | None:
        conn = dbmod.connect(self.db_path)
        try:
            return fetch(conn, evidence_id, version)
        finally:
            conn.close()

    def current(self, evidence_id: str) -> dict | None:
        return self.get(evidence_id, None)

    def versions(self, evidence_id: str) -> list[int]:
        conn = dbmod.connect(self.db_path)
        try:
            rows = conn.execute(
                "SELECT version FROM evidence_versions WHERE evidence_id=? "
                "ORDER BY version", (evidence_id,)).fetchall()
            return [r["version"] for r in rows]
        finally:
            conn.close()
