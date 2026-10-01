"""药物安全法规规则包：内容规范、签名、内容指纹、版本化规则引擎。

规则包是「带辖区、生效区间、依赖版本、内容摘要」的不可变报送口径单元。
登记前必须通过三道校验（见 :class:`pv_service.rule_gov.RuleGovernanceService`）：

1. **签名校验**：``signature`` 是对规范内容字节的 HMAC-SHA256（hex），
   密钥按 ``signer_id`` 在密钥注册表中解析；签名失败 / 未知签名人一律拒绝。
2. **依赖校验**：``depends_on`` 列出的 (package/version) 必须已经登记，
   形成可追溯的规则血缘；缺失依赖拒绝。
3. **时间区间冲突**：同一辖区内任意两条已登记包的声明生效区间
   ``[effective_from, effective_to)`` 不得相交（``effective_to=null`` 视为
   正无穷），冲突拒绝。

内容指纹 ``content_hash`` = 规范内容 JSON 的 SHA-256：

- **相同内容重复登记**：指纹命中既有包，直接返回原版本，不产生新版本；
- **不同内容占用同一版本号**：``(jurisdiction, version)`` 唯一约束兜底，
  应用层先给出明确 409，绝不允许两个不同内容共用一个版本号。

版本化规则引擎在不改既有 ``rules.py`` 规则链的前提下，用包内容里的
天数口径（``serious_days/listed_days/routine_days``）构造同一套规则链 ——
不同法规版本因此会对同一份病例快照产生*可比较但不同*的决定。
``builtin:legacy`` 代表服务内置的历史口径（15/30/90），用于：

- 旧报告（治理上线前）固定的规则版本；
- 试算时未显式指定基线版本时的对照口径。
"""
from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any

from .rules import RuleEngine

LEGACY_PACKAGE_ID = "builtin:legacy"
LEGACY_VERSION = "0"

# 内置历史口径（与 rules.py 默认值一致）
LEGACY_DAYS = {"serious_days": 15, "listed_days": 30, "routine_days": 90}

# 内容中允许出现的规则参数（白名单），其余字段不影响决定口径
_PARAM_KEYS = ("serious_days", "listed_days", "routine_days")


class RulePackageError(ValueError):
    """规则包内容/签名/区间错误。``code`` 可直接映射到 4xx。"""

    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


def canonical_content_bytes(content: dict) -> bytes:
    """规则包内容的规范字节（紧凑、键排序、UTF-8）。

    指纹与签名都基于这一字节序列，因此字段书写顺序不影响同一性判定。
    """
    if not isinstance(content, dict):
        raise RulePackageError("BAD_CONTENT", "规则包内容必须是 JSON 对象")
    return json.dumps(content, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), default=str).encode("utf-8")


def content_hash(content: dict) -> str:
    return hashlib.sha256(canonical_content_bytes(content)).hexdigest()


def sign_content(content: dict, secret: bytes | str) -> str:
    """用对称密钥对规范内容做 HMAC-SHA256（hex）。

    纯标准库实现：部署侧通过带外渠道把 ``signer_id -> secret`` 注册进
    服务密钥注册表；验签只认注册表中的密钥，常量时间比较防时序侧信道。
    """
    if isinstance(secret, str):
        secret = secret.encode("utf-8")
    return hmac.new(secret, canonical_content_bytes(content),
                    hashlib.sha256).hexdigest()


def verify_signature(content: dict, signature: str, secret: bytes | str) -> bool:
    if not isinstance(signature, str) or not signature:
        return False
    expected = sign_content(content, secret)
    return hmac.compare_digest(expected, signature)


# -- 生效区间 ---------------------------------------------------------------

def _iso_date(value: Any, field: str) -> str | None:
    """生效边界：接受 ISO 日期/日期时间，规范化为 'YYYY-MM-DD'；null 放行。"""
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise RulePackageError("BAD_INTERVAL", f"{field} 必须是 ISO 日期字符串或 null")
    s = value.strip()
    # 仅取日期前缀做比较（含时间也合法，但区间按日历日判定）
    date_part = s[:10]
    if len(date_part) != 10 or date_part[4] != "-" or date_part[7] != "-":
        raise RulePackageError("BAD_INTERVAL", f"{field} 不是合法 ISO 日期: {value}")
    try:
        from datetime import date as _date
        _date.fromisoformat(date_part)
    except ValueError:
        raise RulePackageError("BAD_INTERVAL", f"{field} 不是合法日历日: {value}")
    return date_part


def normalize_interval(effective_from: Any, effective_to: Any) -> tuple[str, str | None]:
    start = _iso_date(effective_from, "effective_from")
    end = _iso_date(effective_to, "effective_to")
    if start is None:
        raise RulePackageError("BAD_INTERVAL", "effective_from 不允许为空")
    if end is not None and end <= start:
        raise RulePackageError("BAD_INTERVAL",
                              "生效区间无效：effective_to 必须晚于 effective_from")
    return start, end


def intervals_overlap(a_from: str, a_to: str | None,
                      b_from: str, b_to: str | None) -> bool:
    """半开区间 [from, to) 相交判定；None 端 = 正无穷。"""
    if a_to is not None and b_from >= a_to:
        return False
    if b_to is not None and a_from >= b_to:
        return False
    return True


# -- 版本号 -----------------------------------------------------------------

def version_key(version: str) -> tuple:
    """把点分数字版本号解析为可比较元组；非数字段按字典序兜底。

    用于依赖顺序与回滚目标判断，不强制 semver 前缀。
    """
    if not isinstance(version, str) or not version.strip():
        raise RulePackageError("BAD_VERSION", "版本号不能为空")
    parts = []
    for seg in version.strip().split("."):
        if seg.isdigit():
            parts.append((0, int(seg)))
        else:
            parts.append((1, seg))
    return tuple(parts)


# -- 规则包内容校验 ----------------------------------------------------------

def build_content(*, jurisdiction: str, effective_from: str,
                  effective_to: str | None = None, version: str,
                  depends_on: list[dict] | None = None,
                  summary: str = "",
                  serious_days: int | None = None,
                  listed_days: int | None = None,
                  routine_days: int | None = None,
                  parameters: dict | None = None) -> dict:
    """构造一份规范规则包内容（调用方随后签名再登记）。"""
    if not isinstance(jurisdiction, str) or not jurisdiction.strip():
        raise RulePackageError("BAD_JURISDICTION", "辖区不能为空")
    start, end = normalize_interval(effective_from, effective_to)
    version_key(version)
    deps = depends_on or []
    if not isinstance(deps, list):
        raise RulePackageError("BAD_DEPENDENCY", "depends_on 必须是数组")
    norm_deps = []
    for d in deps:
        if not isinstance(d, dict) or not d.get("version"):
            raise RulePackageError("BAD_DEPENDENCY",
                                   "每个依赖必须含 package 与 version")
        norm_deps.append({
            "package": d.get("package") or jurisdiction,
            "version": str(d["version"]),
        })
    norm_deps.sort(key=lambda d: (d["package"], version_key(d["version"])))

    days = {}
    raw_params = dict(parameters or {})
    explicit = {"serious_days": serious_days, "listed_days": listed_days,
                "routine_days": routine_days}
    for k in _PARAM_KEYS:
        val = explicit[k] if explicit[k] is not None else raw_params.get(k)
        if val is None:
            continue
        if not isinstance(val, int) or isinstance(val, bool) or val <= 0:
            raise RulePackageError("BAD_PARAMETER", f"{k} 必须是正整数")
        days[k] = val
    # 未识别参数原样保留（可携带未来口径），但不参与当前引擎构造
    extra = {k: v for k, v in raw_params.items() if k not in _PARAM_KEYS}

    content = {
        "jurisdiction": jurisdiction.strip(),
        "version": str(version).strip(),
        "effective_from": start,
        "effective_to": end,
        "depends_on": norm_deps,
        "summary": summary or "",
        "parameters": {**days, **extra},
    }
    return content


def engine_days(content: dict) -> dict:
    """从包内容取出报送天数口径（缺省回退内置历史口径）。"""
    params = content.get("parameters") or {}
    return {
        "serious_days": int(params.get("serious_days", LEGACY_DAYS["serious_days"])),
        "listed_days": int(params.get("listed_days", LEGACY_DAYS["listed_days"])),
        "routine_days": int(params.get("routine_days", LEGACY_DAYS["routine_days"])),
    }


def build_engine(content: dict | None) -> RuleEngine:
    """按包内容的天数口径构造规则链引擎。

    ``content=None`` 即内置历史口径（builtin:legacy）。
    """
    if content is None:
        return RuleEngine(**LEGACY_DAYS)
    return RuleEngine(**engine_days(content))


def legacy_view() -> dict:
    return {
        "package_id": LEGACY_PACKAGE_ID,
        "jurisdiction": "*",
        "version": LEGACY_VERSION,
        "summary": "服务内置历史报送口径（15/30/90 日）",
        "parameters": dict(LEGACY_DAYS),
        "builtin": True,
    }
