"""复核员身份的规范化与校验（双人会签的身份判定基础）。

为什么需要它
------------
``review_signatures`` / 租约 / 审计流水若直接以调用方提交的原始字符串作为
身份，会把 *同一复核员* 因首尾空白、大小写或 Unicode 等价形式的不同写法
识别成多个身份：

    "alice"  "  alice  "  "ALICE"  "revＡ"（全角）  "Å"(U+212B)/"Å"(U+00C5)

规范化后这些写法得到同一个稳定身份键，双人会签、待办领取/释放/撤回、
审计查询对“是否同一复核员”给出一致结论，单人无法占满两个意见槽放行。

规范化规则（确定性、无外部依赖）
--------------------------------
1. 必须是字符串（数字等先由调用方转成字符串）；
2. NFKC 兼容性分解组合 —— 全角/半角、兼容字符、组合序列等价折叠；
3. ``str.casefold`` —— 大小写不敏感（比 lower 更彻底，覆盖 ß 等）；
4. 仅去除 *首尾* 空白（``str.strip``），保留内部空白；规范化后为空即
   “空白身份”，必须明确拒绝。

原始提交值不丢
--------------
规范化键只用于身份判定与存储索引；调用方每次提交的原始字符串仍原样写入
意见明细与事件流水（``raw_*`` 字段），保证可追溯。
"""
from __future__ import annotations

import unicodedata

# 身份键长度上限：防止超长输入拖垮索引/日志（与常见账号长度对齐）。
MAX_REVIEWER_LEN = 256


def normalize_reviewer(value: str) -> str:
    """把复核员提交值规范化为稳定身份键。

    调用方应先用 :func:`validate_reviewer`（或 :func:`coerce_reviewer`）
    排除非字符串与空白身份；本函数假定 *value* 是非空字符串。
    """
    folded = unicodedata.normalize("NFKC", value).casefold()
    return folded.strip()


def validate_reviewer(value) -> str:
    """校验并规范化复核员身份，返回稳定身份键。

    - 非字符串（``None``/数字/列表等）→ ``ValueError``（调用方映射为
      400 ``INVALID_REVIEWER``）；
    - 空串或仅含空白 → ``ValueError``（``BLANK_REVIEWER``）；
    - 超长 → ``ValueError``（``REVIEWER_TOO_LONG``）。

    规范化只去首尾空白，不改动内部空白，因此 " a b " 与 "a b" 同身份，
    但 "ab" 是另一个身份。
    """
    if not isinstance(value, str):
        raise ValueError("INVALID_REVIEWER:reviewer 必须是字符串")
    if not value.strip():
        raise ValueError("BLANK_REVIEWER:缺少 reviewer 身份（不能为空白）")
    key = normalize_reviewer(value)
    if len(key) > MAX_REVIEWER_LEN:
        raise ValueError("REVIEWER_TOO_LONG:reviewer 身份超长")
    return key


def coerce_reviewer(value):
    """软校验：规范化成功返回 ``(key, raw)``，空白/非法返回 ``(None, None)``。

    用于不强制要求身份的可选字段（如 ``requested_by``）以及只读查询的
    可选过滤项；写操作（领取/释放/赞成/否决/撤回）必须使用
    :func:`validate_reviewer` 明确拒绝无效身份。
    """
    if not isinstance(value, str) or not value.strip():
        return None, None
    key = normalize_reviewer(value)
    if len(key) > MAX_REVIEWER_LEN:
        return None, None
    return key, value


def is_same_identity(a: str, b: str) -> bool:
    """两个提交值是否指向同一复核员（空白/非法一律按不同身份处理）。"""
    try:
        return validate_reviewer(a) == validate_reviewer(b)
    except ValueError:
        return False
