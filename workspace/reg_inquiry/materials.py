"""可引用资料注册表与辖区披露策略。

资料四类：CASE（病例）/ SENT_REPORT（已发送报文）/ REGULATION（适用法规）/
STATISTIC（统计结论）。每个资料逻辑 id 可以有多个 *只增* 版本；
``(material_id, version)`` 唯一，内容指纹 ``content_hash`` 唯一
（同内容重复登记返回原行 ``deduped=True``）。

``fact_key/fact_value``：同一 fact_key 下不同 fact_value 即“版本互相矛盾”，
由底稿锁定与定稿校验统一识别并阻断。

披露策略按辖区只增版本化；定稿使用问询登记时确定的辖区的 *当前* 策略。
"""
from __future__ import annotations

import uuid
from typing import Any

from . import audit
from .errors import VersionConflictError
from .util import loads, sha256_hex

CATEGORIES = ("CASE", "SENT_REPORT", "REGULATION", "STATISTIC")

# 章节正文/附件中的引用锚前缀 -> 资料类别
REF_PREFIX = {"CASE": "CASE", "SENT_REPORT": "SENT_REPORT",
              "REG": "REGULATION", "STAT": "STATISTIC"}
CATEGORY_TO_PREFIX = {v: k for k, v in REF_PREFIX.items()}


def register_material(conn, *, material_id: str, version: str, category: str,
                      content: dict, title: str = "", fact_key: str | None = None,
                      fact_value: Any = None, registered_by: str | None = None,
                      created_at: str | None = None) -> dict:
    """登记一个资料的新版本。

    - 同 (material_id, version) 不同内容 -> 409 VERSION_CONFLICT；
    - 同内容重复登记（任意版本号）-> 返回既有行 deduped=True；
    - 自动接上同逻辑 id 最新版本的 supersedes 链。
    若存在引用该资料的开放底稿，调用方（orchestrator）在同事务内生成候选；
    本函数只负责注册并返回行。
    """
    if category not in CATEGORIES:
        raise ValueError(f"未知资料类别: {category}")
    if not material_id or not str(material_id).strip():
        raise ValueError("material_id 不能为空")
    if not version or not str(version).strip():
        raise ValueError("version 不能为空")
    import json
    content_json = json.dumps(content, ensure_ascii=False, sort_keys=True,
                              default=str)
    content_hash = sha256_hex(content)

    conflict = conn.execute(
        "SELECT id, content_hash FROM materials WHERE material_id=? AND version=?",
        (material_id, version)).fetchone()
    if conflict and conflict["content_hash"] != content_hash:
        raise VersionConflictError(
            f"资料 {material_id} 版本 {version} 已被不同内容占用",
            details={"material_id": material_id, "version": version})

    # 同一资料逻辑 id 下相同内容重复登记（哪怕换版本号）-> 返回既有行；
    # 不同资料 id 内容相同不算重复（例如两个病例恰好字段一致）。
    same = conn.execute(
        "SELECT * FROM materials WHERE material_id=? AND content_hash=?",
        (material_id, content_hash)).fetchone()
    if same:
        row = dict(same)
        row["deduped"] = True
        return row

    prev = conn.execute(
        "SELECT id FROM materials WHERE material_id=? ORDER BY rowid DESC LIMIT 1",
        (material_id,)).fetchone()
    mid = uuid.uuid4().hex
    now = created_at or _now()
    fv = None if fact_value is None else str(fact_value)
    conn.execute(
        "INSERT INTO materials(id,material_id,version,category,title,content_json,"
        "fact_key,fact_value,content_hash,supersedes_id,registered_by,created_at)"
        " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
        (mid, material_id, version, category, title, content_json, fact_key, fv,
         content_hash, prev["id"] if prev else None, registered_by, now))
    audit.append_event(conn, "MATERIAL_REGISTERED",
                       f"资料登记 {category}:{material_id}@{version}",
                       payload={"material_id": material_id, "version": version,
                                "category": category, "content_hash": content_hash,
                                "fact_key": fact_key, "fact_value": fv,
                                "supersedes": prev["id"] if prev else None},
                       actor=registered_by, created_at=now)
    row = dict(conn.execute("SELECT * FROM materials WHERE id=?", (mid,)).fetchone())
    row["deduped"] = False
    return row


def resolve(conn, material_id: str, version: str | None = None) -> dict | None:
    """解析资料精确版本；version=None 取最新版本。返回 None 表示不存在。"""
    if version is not None:
        r = conn.execute(
            "SELECT * FROM materials WHERE material_id=? AND version=?",
            (material_id, version)).fetchone()
    else:
        r = conn.execute(
            "SELECT * FROM materials WHERE material_id=? ORDER BY rowid DESC LIMIT 1",
            (material_id,)).fetchone()
    return dict(r) if r else None


def resolve_row(conn, material_row_id: str) -> dict | None:
    r = conn.execute("SELECT * FROM materials WHERE id=?",
                     (material_row_id,)).fetchone()
    return dict(r) if r else None


def list_materials(conn, *, category: str | None = None) -> list[dict]:
    sql = "SELECT * FROM materials"
    rows = conn.execute(sql + (" WHERE category=?" if category else ""),
                        (category,) if category else ()).fetchall()
    return [dict(r) for r in rows]


def put_policy(conn, *, jurisdiction: str, rules: dict,
               created_by: str | None = None, created_at: str | None = None) -> dict:
    """登记辖区披露策略的新版本（版本号自动递增）。

    rules 形如 {"sensitive_fields": {"patient_name": "REDACT",
    "patient_identifier": "PROHIBIT", "age": "ALLOW"}}；
    策略模式：ALLOW 放行 / REDACT 必须脱敏（出现原值即违规）/
    PROHIBIT 禁止出现（任何出现即违规）。未列出字段按 DEFAULT（缺省 ALLOW）。
    """
    version = conn.execute(
        "SELECT COALESCE(MAX(version),0)+1 AS n FROM disclosure_policies "
        "WHERE jurisdiction=?", (jurisdiction,)).fetchone()["n"]
    import json
    pid = uuid.uuid4().hex
    now = created_at or _now()
    rules_json = json.dumps(rules, ensure_ascii=False, sort_keys=True, default=str)
    conn.execute(
        "INSERT INTO disclosure_policies(id,jurisdiction,version,rules_json,"
        "created_at,created_by) VALUES(?,?,?,?,?,?)",
        (pid, jurisdiction, version, rules_json, now, created_by))
    audit.append_event(conn, "POLICY_VERSIONED",
                       f"披露策略新版本 {jurisdiction}@v{version}",
                       payload={"jurisdiction": jurisdiction, "version": version,
                                "rules": rules},
                       actor=created_by, created_at=now)
    return {"id": pid, "jurisdiction": jurisdiction, "version": version,
            "rules": rules}


def current_policy(conn, jurisdiction: str) -> dict:
    """取辖区当前策略；从未登记则全部 ALLOW（宽松缺省，可被显式策略收紧）。"""
    r = conn.execute(
        "SELECT * FROM disclosure_policies WHERE jurisdiction=? "
        "ORDER BY version DESC LIMIT 1", (jurisdiction,)).fetchone()
    if not r:
        return {"jurisdiction": jurisdiction, "version": 0,
                "rules": {"sensitive_fields": {}, "default": "ALLOW"}}
    d = dict(r)
    d["rules"] = loads(d.pop("rules_json"), {})
    return d


def _now() -> str:
    from .util import utc_now_iso
    return utc_now_iso()
