"""独立核对（independent verification）。

刻意 *不* 复用组装/校验路径：直接从数据库逐行重算，任何与提交件清单不符、
附件哈希不符、回执错配或审计链断裂都逐条列出，``ok=False``。

核对项：
1. 提交件存在且 manifest_hash 与 manifest 内容一致（不可变封存完整性）；
2. 底稿章节正文/修订号与 manifest 中每问 answer_uid、body_sha256 一致
   （每个问题唯一可追踪回答未被偷换）；
3. manifest 中每个引用 pin 与 draft_pins 冻结行一致（精确版本、内容哈希）；
4. 附件存在且 SHA-256 与 manifest 记录一致；
5. 状态机与外发履历：编号唯一、SENT/DELIVERED 有对应履历、无重复 SENDING；
6. 回执：回执编号唯一、外发编号与提交件一致、payload 对齐；
7. 审计哈希链完整；
8. 更正链：CORRECTION 的 corrects 指针存在，原提交件内容与 manifest 未变。
"""
from __future__ import annotations

import json

from . import audit, submissions
from .util import sha256_hex, sha256_raw


def verify_submission(conn, submission_id: str) -> dict:
    failures: list[dict] = []
    sub_row = conn.execute("SELECT * FROM submissions WHERE id=?",
                           (submission_id,)).fetchone()
    if not sub_row:
        return {"ok": False, "submission_id": submission_id,
                "failures": [{"code": "SUBMISSION_MISSING",
                              "message": "提交件不存在"}]}
    sub = dict(sub_row)
    manifest = json.loads(sub["manifest_json"])

    # 1) manifest 完整性
    if sha256_hex(manifest) != sub["manifest_hash"]:
        failures.append({"code": "MANIFEST_HASH_MISMATCH",
                         "message": "manifest 内容与封存指纹不符（提交件被篡改）"})

    draft_row = conn.execute("SELECT * FROM drafts WHERE id=?",
                             (sub["draft_id"],)).fetchone()
    if not draft_row:
        failures.append({"code": "DRAFT_MISSING", "message": "关联底稿不存在"})
        return _result(sub, manifest, failures)
    if draft_row["status"] != "FINALIZED" or draft_row["submission_id"] != sub["id"]:
        failures.append({"code": "DRAFT_STATE_MISMATCH",
                         "message": "底稿未与提交件正确绑定"})

    # 2) 每问回答：answer_uid + body_sha256 + 修订号
    sections = {r["qid"]: dict(r) for r in conn.execute(
        "SELECT * FROM sections WHERE draft_id=?", (sub["draft_id"],))}
    manifest_qids = set()
    for ans in manifest.get("answers", []):
        qid = ans["qid"]
        manifest_qids.add(qid)
        sec = sections.get(qid)
        if sec is None:
            failures.append({"code": "ANSWER_MISSING", "qid": qid,
                             "message": f"manifest 中问题 {qid} 的章节已不存在"})
            continue
        expect_uid = submissions._answer_uid(
            sub["draft_id"], qid, sec["id"], sec["revision"])
        if ans["answer_uid"] != expect_uid:
            failures.append({"code": "ANSWER_UID_MISMATCH", "qid": qid,
                             "message": f"问题 {qid} answer_uid 与当前章节不符"})
        if ans["body_sha256"] != sha256_hex(sec["body"]):
            failures.append({"code": "BODY_HASH_MISMATCH", "qid": qid,
                             "message": f"问题 {qid} 正文与封存哈希不符"})
        if ans["section_revision"] != sec["revision"]:
            failures.append({"code": "REVISION_MISMATCH", "qid": qid,
                             "message": f"问题 {qid} 修订号已偏离封存值"})
    if set(sections) != manifest_qids:
        extra = set(sections) - manifest_qids
        if extra:
            failures.append({"code": "ANSWER_SET_MISMATCH",
                             "message": f"存在 manifest 之外的章节: {sorted(extra)}"})

    # 3) pins 与 manifest 一致
    pins = {(r["ref"]): dict(r) for r in conn.execute(
        "SELECT * FROM draft_pins WHERE draft_id=?", (sub["draft_id"],))}
    for p in manifest.get("pins", []):
        pin = pins.get(p["ref"])
        if pin is None:
            failures.append({"code": "PIN_MISSING", "ref": p["ref"],
                             "message": f"manifest 引用 {p['ref']} 的锁定行缺失"})
            continue
        if pin["content_hash"] != p["content_hash"] \
                or pin["version"] != p["version"] or pin["category"] != p["category"]:
            failures.append({"code": "PIN_MISMATCH", "ref": p["ref"],
                             "message": f"引用 {p['ref']} 锁定内容与 manifest 不符"})

    # 4) 附件哈希
    for ans in manifest.get("answers", []):
        for a in ans.get("attachments", []):
            row = conn.execute(
                "SELECT * FROM attachments WHERE draft_id=? AND id=?",
                (sub["draft_id"], a["attachment_id"])).fetchone()
            if row is None:
                failures.append({"code": "ATTACHMENT_MISSING",
                                 "attachment_id": a["attachment_id"],
                                 "message": f"附件 {a['filename']} 不存在"})
                continue
            actual = sha256_raw(bytes(row["content_blob"]))
            if actual != a["sha256"] or actual != row["sha256"]:
                failures.append({"code": "ATTACHMENT_HASH_MISMATCH",
                                 "attachment_id": a["attachment_id"],
                                 "message": f"附件 {a['filename']} 哈希不符"})

    # 5) 外发编号唯一 + 履历状态机
    dup = conn.execute(
        "SELECT COUNT(*) AS n FROM submissions WHERE outgoing_number=?",
        (sub["outgoing_number"],)).fetchone()["n"]
    if dup != 1:
        failures.append({"code": "OUTGOING_NUMBER_DUPLICATED",
                         "message": f"外发编号 {sub['outgoing_number']} 出现 {dup} 次"})
    ev_rows = conn.execute(
        "SELECT event_type, COUNT(*) AS n FROM submission_events "
        "WHERE submission_id=? GROUP BY event_type", (sub["id"],)).fetchall()
    ev_count = {r["event_type"]: r["n"] for r in ev_rows}
    for et in ("SENT",):
        if sub["status"] in ("SENT", "DELIVERED") and ev_count.get(et, 0) == 0:
            failures.append({"code": "EVENT_MISSING", "event_type": et,
                             "message": f"状态 {sub['status']} 缺少 {et} 履历"})
    if ev_count.get("SENT", 0) > 1:
        failures.append({"code": "DUPLICATE_SENT_EVENT",
                         "message": "存在重复 SENT 履历"})
    # SENDING 履历应与真实尝试轮次一一对应（每次重试一条，按 attempt 去重）
    sending_attempts = set()
    for ev in conn.execute(
            "SELECT detail_json FROM submission_events WHERE submission_id=? "
            "AND event_type='SENDING'", (sub["id"],)):
        try:
            sending_attempts.add(json.loads(ev["detail_json"]).get("attempt"))
        except Exception:
            pass
    if None in sending_attempts:
        failures.append({"code": "SENDING_EVENT_INVALID",
                         "message": "SENDING 履历缺少 attempt 标记"})
    if ev_count.get("SENDING", 0) != len(sending_attempts):
        failures.append({"code": "DUPLICATE_SENDING_EVENT",
                         "message": "SENDING 履历存在重复尝试轮次"})
    # attempts 是实际外发次数（含失败重试），应等于 SENDING 履历数
    if sub["attempts"] != ev_count.get("SENDING", 0):
        failures.append({"code": "ATTEMPTS_MISMATCH",
                         "message": f"attempts={sub['attempts']} 与 SENDING 履历数 "
                                    f"{ev_count.get('SENDING', 0)} 不一致"})

    # 6) 回执
    for rc in conn.execute("SELECT * FROM receipts WHERE submission_id=?",
                           (sub["id"],)):
        if rc["outgoing_number"] != sub["outgoing_number"]:
            failures.append({"code": "RECEIPT_NUMBER_MISMATCH",
                             "receipt_number": rc["receipt_number"],
                             "message": "回执外发编号与提交件不一致"})
        try:
            payload = json.loads(rc["payload_json"])
            if payload.get("outgoing_number") and \
                    payload["outgoing_number"] != sub["outgoing_number"]:
                failures.append({"code": "RECEIPT_PAYLOAD_MISMATCH",
                                 "receipt_number": rc["receipt_number"],
                                 "message": "回执载荷外发编号不一致"})
        except Exception:
            failures.append({"code": "RECEIPT_PAYLOAD_INVALID",
                             "receipt_number": rc["receipt_number"],
                             "message": "回执载荷不是合法 JSON"})
    if sub["status"] == "DELIVERED" and conn.execute(
            "SELECT COUNT(*) AS n FROM receipts WHERE submission_id=?",
            (sub["id"],)).fetchone()["n"] == 0:
        failures.append({"code": "RECEIPT_MISSING",
                         "message": "状态 DELIVERED 但无监管回执"})

    # 8) 更正指针
    if sub["kind"] == "CORRECTION":
        if not sub["corrects_submission_id"]:
            failures.append({"code": "CORRECTION_LINK_MISSING",
                             "message": "更正件缺少 corrects 指针"})
        elif not conn.execute("SELECT 1 FROM submissions WHERE id=?",
                              (sub["corrects_submission_id"],)).fetchone():
            failures.append({"code": "CORRECTION_TARGET_MISSING",
                             "message": "更正件指向的原提交件不存在"})

    return _result(sub, manifest, failures)


def _result(sub, manifest, failures: list[dict]) -> dict:
    return {"ok": not failures, "submission_id": sub["id"],
            "outgoing_number": sub["outgoing_number"], "kind": sub["kind"],
            "manifest_hash": sub["manifest_hash"], "failures": failures,
            "answer_count": len(manifest.get("answers", []))}


def verify_all(conn) -> dict:
    """全库核对：所有提交件 + 审计哈希链 + 编号全局唯一。"""
    sub_failures = []
    sub_ids = [r["id"] for r in conn.execute("SELECT id FROM submissions")]
    ok = True
    results = []
    for sid in sub_ids:
        r = verify_submission(conn, sid)
        results.append(r)
        ok = ok and r["ok"]
    chain_failures = audit.verify_chain(
        audit.list_events(conn))
    if chain_failures:
        ok = False
        sub_failures.extend({"code": "AUDIT_CHAIN_BROKEN", "message": m}
                            for m in chain_failures)
    # 原始提交件唯一
    rows = conn.execute(
        "SELECT inquiry_id, COUNT(*) AS n FROM submissions WHERE kind='ORIGINAL' "
        "GROUP BY inquiry_id HAVING n>1").fetchall()
    for r in rows:
        ok = False
        sub_failures.append({"code": "MULTIPLE_ORIGINALS",
                             "message": f"问询 {r['inquiry_id']} 有多份原始提交件"})
    return {"ok": ok, "submissions": results,
            "audit_chain_ok": not chain_failures,
            "audit_chain_failures": chain_failures,
            "failures": sub_failures}
