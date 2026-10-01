"""监管问询答复编排器（Regulatory Inquiry Response Orchestrator）。

独立子包：接收监管方问题清单/截止时间/可引用资料范围，把病例、已发送报文、
适用法规、统计结论的 *精确版本* 锁定为答复底稿；组稿期间引用变化只产生带
原因的新底稿候选，绝不悄悄混入当前答复。

能力一览（见 ``InquiryOrchestrator``）：
问询登记 / 底稿锁定 / 章节协作 / 冲突查询 / 定稿校验 / 发送与重试 /
回执登记 / 更正关系 / 独立核对 / 操作留痕。
"""
from .orchestrator import InquiryOrchestrator
from .errors import (
    InquiryError, ValidationFailed, MissingMaterialsError, OutOfScopeError,
    StaleRevisionError, DraftSealedError, VersionConflictError,
    JobFailed, TransportTempError, TransportRejectError,
)

__all__ = [
    "InquiryOrchestrator",
    "InquiryError",
    "ValidationFailed",
    "MissingMaterialsError",
    "OutOfScopeError",
    "StaleRevisionError",
    "DraftSealedError",
    "VersionConflictError",
    "JobFailed",
    "TransportTempError",
    "TransportRejectError",
]
