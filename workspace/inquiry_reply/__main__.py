"""端到端演示：python3 -m inquiry_reply [db_path]

覆盖完整生命周期：问询登记 → 底稿锁定 → 章节协作 → 冲突查询 → 定稿校验 →
可恢复发送 → 回执登记 → 资料变化产生更正件 → 独立核对 → 操作留痕。
"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

from .service import InquiryReplyService


def main() -> int:
    db_path = sys.argv[1] if len(sys.argv) > 1 else str(
        Path(tempfile.mkdtemp(prefix="inquiry-reply-")) / "inquiry_reply.db")
    svc = InquiryReplyService(db_path, worker_id="demo-node")

    print("== 0. 准备可引用资料（病例/已发送报文/法规/统计结论）")
    svc.evidence.upsert("case:1001", "CASE", "患者甲，女，62岁，2型糖尿病，用药后皮疹。")
    svc.evidence.upsert("msg:2001", "MESSAGE", "2026-09-01 已报送 AE-2026-001（初始报告）。")
    svc.evidence.upsert("reg:3001", "REGULATION", "《药品不良反应报告和监测管理办法》第二十一条。")
    svc.evidence.upsert("stat:4001", "STATISTIC", "2026H1 共收到 AE 报告 128 例，其中严重 12 例。")

    print("== 1. 问询登记")
    inq = svc.register_inquiry(
        regulator="NMPA", jurisdiction="CN", deadline="2026-10-15",
        questions=[{"question_id": "Q1", "text": "说明病例情况与已报送情况"},
                   {"question_id": "Q2", "text": "说明统计口径与法规依据"}],
        allowed_scope=["case:*", "msg:*", "reg:*", "stat:*"],
        actor="pm", idempotency_key="REQ-2026-0901")
    print("   inquiry:", inq["inquiry_id"], "问题数:", len(inq["questions"]))

    print("== 2. 底稿锁定（精确版本冻结）")
    draft = svc.lock_draft(inquiry_id=inq["inquiry_id"],
                           evidence_ids=["case:1001", "msg:2001", "reg:3001",
                                         "stat:4001"], actor="pm")
    for c in draft["citations"]:
        print(f"   锁定 {c['evidence_id']}@v{c['version']} hash={c['content_hash'][:12]}…")

    print("== 3. 章节协作（两名撰写人并行）")
    svc.submit_section(draft_id=draft["draft_id"], section_key="sec-q1",
                       question_id="Q1", author="alice", base_revision=0,
                       content="病例与报送情况见 [[ev:case:1001]]、[[ev:msg:2001]]，"
                               "明细见附件 [[att:A1]]。")
    svc.submit_section(draft_id=draft["draft_id"], section_key="sec-q2",
                       question_id="Q2", author="bob", base_revision=0,
                       content="统计口径见 [[ev:stat:4001]]，法规依据见 [[ev:reg:3001]]。")
    svc.put_attachment(draft_id=draft["draft_id"], attachment_id="A1",
                       name="病例清单", content="病例清单：case:1001（随访 2 次）",
                       author="alice", base_revision=0)
    print("   sections:", [s["section_key"] for s in svc.get_sections(draft["draft_id"])])

    print("== 4. 组稿期间资料变化 → 冲突查询 → 带原因的新候选")
    svc.evidence.upsert("stat:4001", "STATISTIC",
                        "2026H1 共收到 AE 报告 131 例，其中严重 13 例。")
    conflicts = svc.query_conflicts(draft["draft_id"])
    print("   引用漂移:", conflicts["evidence_drift"])
    report = svc.validate_draft(draft["draft_id"])
    print("   旧底稿定稿校验:", report["ok"], report["unmet"])
    new_draft = svc.refresh_draft(draft_id=draft["draft_id"], actor="pm")
    print("   新候选:", new_draft["draft_id"][:12], "原因:", new_draft["reasons"])

    print("== 5. 定稿校验 + 可恢复发送")
    result = svc.finalize_and_send(draft_id=new_draft["draft_id"], actor="pm")
    print("   提交件:", result["submission_id"][:12], "外发编号:", result["outbound_no"])

    print("== 6. 回执登记（幂等）")
    r1 = svc.register_receipt(submission_id=result["submission_id"],
                              regulator_ref="NMPA-ACK-001",
                              payload={"verdict": "received"}, actor="pm")
    r2 = svc.register_receipt(submission_id=result["submission_id"],
                              regulator_ref="NMPA-ACK-001",
                              payload={"verdict": "received"}, actor="pm")
    print("   回执:", r1["receipt_id"][:12], "重复登记去重:", r2["deduped"])

    print("== 7. 定稿后修改只能走更正件")
    corr_draft = svc.create_correction(submission_id=result["submission_id"],
                                       reason="统计口径勘误", actor="pm")
    svc.submit_section(draft_id=corr_draft["draft_id"], section_key="sec-q2",
                       author="bob", base_revision=1,
                       content="统计口径见 [[ev:stat:4001]]（已按 131 例口径勘误），"
                               "法规依据见 [[ev:reg:3001]]。")
    corr_result = svc.finalize_and_send(draft_id=corr_draft["draft_id"], actor="pm")
    print("   更正件:", corr_result["submission_id"][:12],
          "外发编号:", corr_result["outbound_no"])
    for link in svc.correction_chain(result["submission_id"]):
        print(f"   链: {link['outbound_no']} status={link['status']} "
              f"回执数={len(link['receipts'])}")

    print("== 8. 独立核对")
    for sid in (result["submission_id"], corr_result["submission_id"]):
        check = svc.independent_check(sid)
        print(f"   {sid[:12]} ok={check['ok']} "
              f"({sum(c['ok'] for c in check['checks'])}/{len(check['checks'])} 项通过)")

    print("== 9. 操作留痕")
    trail = svc.audit_trail()
    print(f"   共 {len(trail)} 条；首尾:",
          trail[0]["action"], "→", trail[-1]["action"])
    print("演示完成。数据库:", db_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
