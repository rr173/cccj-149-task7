"""定稿校验：每个问题都有唯一可追踪回答 + 引用/附件一致 + 敏感披露合规。

校验 *只读* 组装结构化报告：
- ``blocking``：阻断项（任一存在即不得定稿）；
- ``unsatisfied``：尚未满足的问题清单（精确到 qid 与原因码）；
- ``warnings``：非阻断（如引用已漂移到更新版本、临近/超过截止时间）。

所有阻断原因码稳定：NO_ANSWER / EMPTY_ANSWER / MULTIPLE_ANSWERS /
CITATION_NOT_LOCKED / CITATION_VERSION_MISMATCH / ATTACHMENT_NOT_CITED /
ATTACHMENT_REF_NOT_LOCKED / SENSITIVE_* / PIN_CONTRADICTION。
"""
from __future__ import annotations

from datetime import datetime

from . import drafts, materials
from .util import loads

# 附件在正文中的引用形式：@attachment:<id>
ATTACH_CITATION_PREFIX = "@attachment:"


def _issue(code: str, message: str, *, qid: str | None = None,
           severity: str = "blocking", **extra) -> dict:
    d = {"severity": severity, "code": code, "message": message}
    if qid is not None:
        d["qid"] = qid
    d.update(extra)
    return d


def check_drift(conn, draft: dict) -> list[dict]:
    """本草锁定的版本是否已有更新版本到达（只警告，不阻断——
    底稿锁定的精确版本仍可定稿，漂移候选已另行生成）。"""
    warnings = []
    for pin in draft["pins"]:
        latest = materials.resolve(conn, pin["material_id"])
        if latest and latest["version"] != pin["version"]:
            warnings.append({
                "code": "PIN_STALE", "severity": "warning",
                "material_id": pin["material_id"],
                "locked_version": pin["version"],
                "latest_version": latest["version"], "ref": pin["ref"],
                "message": f"引用 {pin['ref']} 已有新版本 "
                           f"{latest['version']}（本草仍按锁定版本定稿；"
                           "如须采用请切换到漂移候选）"})
    return warnings


def _sensitive_violations(text: str, policy: dict, *, where: str,
                          qid: str | None) -> list[dict]:
    rules = policy.get("rules") or {}
    fields = rules.get("sensitive_fields") or {}
    default = rules.get("default", "ALLOW")
    out = []
    for field, mode in fields.items():
        if mode == "ALLOW":
            continue
        # 字段以 "字段名:值" 或嵌套 JSON 键出现；大小写不敏感匹配键，
        # 值出现且未以 «REDACTED:…» 占位即视为原值泄露。
        patterns = [f'"{field}"', f"{field}:", f"{field}："]
        if not any(p.lower() in (text or "").lower() for p in patterns):
            continue
        if _raw_value_present(text, field):
            code = ("SENSITIVE_PROHIBITED" if mode == "PROHIBIT"
                    else "SENSITIVE_NOT_REDACTED")
            out.append(_issue(code, f"{where}出现敏感字段 {field}（策略 {mode}）",
                              qid=qid, field=field, policy=mode))
    if default == "PROHIBIT" and not fields:
        # 不采用此分支（显式字段策略才有意义），保留保守空行为
        pass
    return out


def _raw_value_present(text: str, field: str) -> bool:
    """粗略但保守：含该字段键且同一行/同一 JSON 项里值不是 REDACTED 占位。"""
    lowered_field = field.lower()
    # JSON 形态
    try:
        obj = _find_key(loads(text, None), lowered_field)
        for v in obj:
            if v is not None and not _is_redaction(v):
                return True
    except Exception:
        pass
    # 文本形态：field: value / field：value
    for line in (text or "").splitlines():
        low = line.lower()
        idx = low.find(lowered_field)
        if idx == -1:
            continue
        for sep in (":", "："):
            p = low.find(sep, idx)
            if p != -1:
                val = line[p + 1:].strip().strip('",')
                if val and not _is_redaction(val):
                    return True
    return False


def _find_key(obj, key: str):
    found = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if str(k).lower() == key:
                found.append(v)
            found.extend(_find_key(v, key))
    elif isinstance(obj, list):
        for it in obj:
            found.extend(_find_key(it, key))
    return found


def _is_redaction(v) -> bool:
    s = str(v).strip()
    return s.startswith("«REDACTED") or s.upper() in ("***", "[REDACTED]", "NULL",
                                                      "NONE", "")


def build_report(conn, draft_id: str) -> dict:
    """组装完整校验报告（只读）。"""
    draft = drafts.get_draft(conn, draft_id)
    inquiry = drafts.get_inquiry(conn, draft["inquiry_id"])
    policy = materials.current_policy(conn, inquiry["jurisdiction"])

    blocking: list[dict] = []
    warnings: list[dict] = []
    unsatisfied: list[dict] = []
    per_question: dict[str, dict] = {}

    pins_by_ref = {p["ref"]: p for p in draft["pins"]}
    sections_by_qid = {s["qid"]: s for s in draft["sections"]}
    attachments = draft["attachments"]
    attach_ids = {a["id"] for a in attachments}
    attach_by_id = {a["id"]: a for a in attachments}

    # 1) 全局：锁定版本之间矛盾（定稿时复核一次，防止矛盾状态入库）
    contradictions = drafts.detect_contradictions(draft["pins"])
    for c in contradictions:
        blocking.append(_issue("PIN_CONTRADICTION",
                               f"事实 {c['fact_key']} 在 {c['ref_a']} 与 "
                               f"{c['ref_b']} 中取值矛盾："
                               f"{c['value_a']!r} ≠ {c['value_b']!r}",
                               fact_key=c["fact_key"], ref_a=c["ref_a"],
                               ref_b=c["ref_b"]))

    # 2) 逐问题：唯一章节 / 非空回答 / 引用全部锁定且版本一致 / 敏感合规
    for q in inquiry["questions"]:
        qid = q["qid"]
        matches = [s for s in draft["sections"] if s["qid"] == qid]
        pq = {"qid": qid, "issues": []}
        if len(matches) > 1:
            it = _issue("MULTIPLE_ANSWERS",
                        f"问题 {qid} 存在 {len(matches)} 个竞争回答，必须唯一",
                        qid=qid)
            blocking.append(it)
            pq["issues"].append(it)
            unsatisfied.append({"qid": qid, "code": it["code"],
                                "message": it["message"]})
            per_question[qid] = pq
            continue
        if not matches:
            it = _issue("NO_ANSWER", f"问题 {qid} 尚无回答章节", qid=qid)
            blocking.append(it)
            pq["issues"].append(it)
            unsatisfied.append({"qid": qid, "code": it["code"],
                                "message": it["message"]})
            per_question[qid] = pq
            continue
        sec = matches[0]
        if not sec["body"] or not sec["body"].strip():
            it = _issue("EMPTY_ANSWER", f"问题 {qid} 的回答正文为空", qid=qid)
            blocking.append(it)
            pq["issues"].append(it)
            unsatisfied.append({"qid": qid, "code": it["code"],
                                "message": it["message"]})

        # 2a) 正文引用：必须全部在 pins 中，且版本与锁定一致
        cited_refs = set()
        for c in drafts.extract_citations(sec["body"] + "\n" + sec["title"]):
            ref = f"{c['prefix']}:{c['material_id']}" + (
                f"@{c['version']}" if c["version"] else "")
            if c["version"] is None:
                it = _issue("CITATION_VERSION_MISMATCH",
                            f"问题 {qid} 的引用 {c['raw']} 未指定精确版本，"
                            "无法追踪", qid=qid, raw=c["raw"])
                blocking.append(it)
                pq["issues"].append(it)
                continue
            pin = pins_by_ref.get(ref)
            if pin is None:
                it = _issue("CITATION_NOT_LOCKED",
                            f"问题 {qid} 引用了未在本草锁定的 {ref}",
                            qid=qid, ref=ref)
                blocking.append(it)
                pq["issues"].append(it)
                continue
            if pin["version"] != c["version"]:
                it = _issue("CITATION_VERSION_MISMATCH",
                            f"问题 {qid} 引用版本 {c['version']} 与锁定版本 "
                            f"{pin['version']} 不一致", qid=qid, ref=ref)
                blocking.append(it)
                pq["issues"].append(it)
            cited_refs.add(ref)

        # 2b) 正文中声明的附件引用必须存在；附件都必须在某章节正文中被引用
        for token in _extract_attach_refs(sec["body"]):
            if token not in attach_ids:
                it = _issue("ATTACHMENT_REF_MISSING",
                            f"问题 {qid} 引用了不存在的附件 {token}",
                            qid=qid, attachment_id=token)
                blocking.append(it)
                pq["issues"].append(it)
        pq["cited_refs"] = sorted(cited_refs)
        pq["attachment_refs"] = _extract_attach_refs(sec["body"])

        # 2c) 敏感字段披露策略
        for v in _sensitive_violations(sec["body"], policy,
                                      where=f"问题 {qid} 正文", qid=qid):
            blocking.append(v)
            pq["issues"].append(v)
        if pq["issues"]:
            unsatisfied.append({"qid": qid,
                                "codes": sorted({i["code"] for i in pq["issues"]}),
                                "message": "；".join(i["message"]
                                                     for i in pq["issues"])})
        per_question[qid] = pq

    # 3) 附件一致性：每个附件至少被一个章节引用；溯源必须指向已锁定资料
    referenced_attach = set()
    for s in draft["sections"]:
        referenced_attach.update(_extract_attach_refs(s["body"]))
    for a in attachments:
        if a["id"] not in referenced_attach:
            it = _issue("ATTACHMENT_NOT_CITED",
                        f"附件 {a['filename']}（{a['id']}）未被任何章节正文引用",
                        attachment_id=a["id"], filename=a["filename"])
            blocking.append(it)
        dref = a.get("derived_from_ref")
        if dref and dref not in pins_by_ref:
            it = _issue("ATTACHMENT_REF_NOT_LOCKED",
                        f"附件 {a['filename']} 溯源 {dref} 未在本草锁定",
                        attachment_id=a["id"], ref=dref)
            blocking.append(it)

    # 4) 多出来的章节（对应不上任何问题）
    for s in draft["sections"]:
        if s["qid"] not in sections_by_qid:
            pass
        if s["qid"] not in {q["qid"] for q in inquiry["questions"]}:
            blocking.append(_issue("ORPHAN_SECTION",
                                   f"章节 {s['qid']} 不对应问询清单中的任何问题",
                                   qid=s["qid"]))

    # 5) 漂移 / 截止时间 警告（非阻断）
    warnings.extend(check_drift(conn, draft))
    try:
        deadline = datetime.fromisoformat(inquiry["deadline"])
        now = datetime.fromisoformat(_now_iso())
        if now > deadline:
            warnings.append({"code": "PAST_DEADLINE", "severity": "warning",
                             "message": f"已超过问询截止时间 {inquiry['deadline']}"})
    except Exception:
        pass

    return {
        "draft_id": draft_id,
        "inquiry_id": draft["inquiry_id"],
        "jurisdiction": inquiry["jurisdiction"],
        "policy_version": policy.get("version"),
        "ok": not blocking,
        "blocking": blocking,
        "warnings": warnings,
        "unsatisfied": unsatisfied,
        "per_question": per_question,
        "question_count": len(inquiry["questions"]),
        "answered_count": sum(
            1 for q in inquiry["questions"]
            if q["qid"] in sections_by_qid
            and sections_by_qid[q["qid"]]["body"].strip()),
    }


def _extract_attach_refs(text: str) -> list[str]:
    import re
    return re.findall(r"@attachment:([A-Za-z0-9_\-]+)", text or "")


def _now_iso() -> str:
    from .util import utc_now_iso
    return utc_now_iso()
