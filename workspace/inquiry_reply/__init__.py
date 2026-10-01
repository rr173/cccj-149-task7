"""监管问询答复编排器（独立模块，纯 Python 标准库 + SQLite）。

能力总览：
  问询登记   register_inquiry
  底稿锁定   lock_draft / refresh_draft
  章节协作   submit_section / put_attachment / get_sections / get_attachments
  冲突查询   query_conflicts
  定稿校验   validate_draft
  发送与重试 finalize_and_send / retry_job / run_pending_jobs / run_job
  回执登记   register_receipt
  更正关系   create_correction / correction_chain
  独立核对   independent_check
  操作留痕   audit_trail
"""
from .errors import DomainError
from .evidence import EvidenceStore
from .policy import BASE_POLICY, DEFAULT_POLICIES, DisclosurePolicy, SensitiveRule
from .service import Clock, FixedClock, InquiryReplyService
from .transport import OutboxTransport

__all__ = [
    "DomainError", "EvidenceStore", "InquiryReplyService",
    "Clock", "FixedClock", "OutboxTransport",
    "DisclosurePolicy", "SensitiveRule", "DEFAULT_POLICIES", "BASE_POLICY",
]

__version__ = "0.1.0"
