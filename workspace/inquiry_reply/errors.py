"""领域错误。"""


class DomainError(Exception):
    """可映射到 HTTP 4xx/5xx 的领域错误。

    常用 code：
      NOT_FOUND          实体不存在（404）
      BAD_FIELD          入参非法（400）
      STALE_REVISION     章节/附件乐观修订号冲突（409，附 current_revision）
      DRAFT_NOT_OPEN     底稿已不可编辑（409）
      VALIDATION_FAILED  定稿校验未通过（422，附 report：尚未满足的问题清单）
      JOB_IN_FLIGHT      作业正由其他工作节点处理（409）
      JOB_FAILED         作业执行失败（500）
      TRANSPORT_ERROR    外发通道瞬时故障（503，供传输适配器抛出）
    """

    def __init__(self, code: str, message: str, status: int = 400):
        super().__init__(message)
        self.code = code
        self.status = status
