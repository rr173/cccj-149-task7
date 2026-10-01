"""收件辖区披露策略：定稿前扫描正文与附件中的敏感字段。

每个辖区登记一组 ``SensitiveRule(field, pattern)``：凡在章节正文或附件
内容中命中模式的，定稿校验一律阻止（SENSITIVE_FIELD），并在报告中给出
字段名与位置（命中值脱敏后留痕，不把敏感原文写进报告/审计）。
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SensitiveRule:
    field: str      # 敏感字段名（用于报告定位）
    pattern: str    # 正则


@dataclass(frozen=True)
class DisclosurePolicy:
    jurisdiction: str
    rules: tuple


NATIONAL_ID = SensitiveRule("national_id", r"\b\d{17}[\dXx]\b")
PHONE_CN = SensitiveRule("phone", r"\b1[3-9]\d{9}\b")
EMAIL = SensitiveRule("email", r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
SSN_US = SensitiveRule("ssn", r"\b\d{3}-\d{2}-\d{4}\b")
MRN = SensitiveRule("medical_record_no", r"\bMRN-\d{6,}\b")

#: 未登记辖区使用的保守兜底策略
BASE_POLICY = DisclosurePolicy("BASE", (NATIONAL_ID, PHONE_CN, EMAIL))

DEFAULT_POLICIES = {
    "CN": DisclosurePolicy("CN", (NATIONAL_ID, PHONE_CN, MRN)),
    "EU": DisclosurePolicy("EU", (EMAIL, NATIONAL_ID, MRN)),
    "US": DisclosurePolicy("US", (SSN_US, EMAIL, MRN)),
}
