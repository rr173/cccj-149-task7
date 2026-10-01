"""领域异常：全部带稳定错误码，调用方可据此重试或阻断。"""
from __future__ import annotations


class InquiryError(Exception):
    """编排器领域错误基类。"""

    code = "INQUIRY_ERROR"

    def __init__(self, message: str = "", *, details=None):
        super().__init__(message or self.code)
        self.details = details if details is not None else {}


class NotFoundError(InquiryError):
    code = "NOT_FOUND"


class MissingMaterialsError(InquiryError):
    """底稿锁定/更正引用了尚不存在（或尚无该精确版本）的资料。

    ``details['missing']`` 逐项给出资料逻辑 id / 要求版本 / 类别与原因；
    缺失未补齐前不得锁定、不得定稿。
    """

    code = "MISSING_MATERIALS"


class OutOfScopeError(InquiryError):
    """引用越权：资料不在问询登记的可引用资料范围内。"""

    code = "OUT_OF_SCOPE"


class VersionConflictError(InquiryError):
    """调用方基于过期修订提交章节（乐观并发）。详情含当前修订号。"""

    code = "STALE_REVISION"


class StaleRevisionError(VersionConflictError):
    """VersionConflictError 的旧名兼容别名。"""


class DraftSealedError(InquiryError):
    """底稿已定稿/被取代：章节与附件不可再改，只能走更正件。"""

    code = "DRAFT_SEALED"


class ContradictionError(InquiryError):
    """底稿锁定/定稿时被引用版本之间存在互相矛盾的事实。"""

    code = "CONTRADICTION"


class ValidationFailed(InquiryError):
    """定稿校验未通过。``details['report']`` 是完整结构化校验报告，
    ``details['unsatisfied']`` 精确列出尚未满足的问题。"""

    code = "VALIDATION_FAILED"

    def __init__(self, report: dict):
        super().__init__("定稿校验未通过：%d 个阻断项"
                         % len(report.get("blocking", [])),
                         details={"report": report,
                                  "unsatisfied": report.get("unsatisfied", [])})
        self.report = report


class JobFailed(InquiryError):
    """可恢复作业最终失败（永久错误或重试耗尽）。"""

    code = "JOB_FAILED"

    def __init__(self, job: dict, *, cause: Exception | None = None):
        super().__init__("作业 %s 失败：%s" % (job.get("id"), job.get("error_code")))
        self.job = job
        self.cause = cause
        self.code = job.get("error_code") or "JOB_FAILED"


class TransportTempError(InquiryError):
    """外发传输临时失败（可重试）。"""

    code = "TRANSPORT_TEMP"


class TransportRejectError(InquiryError):
    """监管网关明确拒收（终态，不重试）。"""

    code = "TRANSPORT_REJECT"
