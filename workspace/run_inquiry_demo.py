"""监管问询答复编排器 —— 端到端剧本演示（无需起任何服务）。

覆盖：问询登记 -> 精确版本锁定 -> 多人章节协作 -> 引用漂移另立候选 ->
定稿校验阻断/补齐 -> 不可变提交件 -> 发送重试/幂等 -> 回执 -> 更正件 ->
独立核对 -> 操作留痕。
"""
from __future__ import annotations

import os
import tempfile

from reg_inquiry import InquiryOrchestrator
from reg_inquiry.transport import RegulatorTransport
from reg_inquiry.util import FixedClock


def main() -> None:
    d = tempfile.mkdtemp(prefix="inquiry-demo-")
    clock = FixedClock("2026-10-01T09:00:00+00:00")
    gw = RegulatorTransport(os.path.join(d, "regulator.db"), clock=clock,
                            fail_times=1)  # 首次外发临时故障，验证自动重试
    o = InquiryOrchestrator(os.path.join(d, "inquiry.db"), transport=gw,
                            clock=clock)

    def step(t): print(f"\n=== {t} ===")

    # 1) 可引用资料（四类）登记，带精确版本与事实键
    step("1. 资料登记（病例/法规/统计结论，精确版本）")
    o.register_material(material_id="CASE-7788", version="1", category="CASE",
                        content={"patient_name": "张三", "event": "皮疹",
                                 "outcome": "恢复"},
                        fact_key="OUTCOME", fact_value="恢复",
                        registered_by="pv-officer")
    o.register_material(material_id="RULE-EXP", version="2026-1",
                        category="REGULATION",
                        content={"expedited_days": 15, "basis": "严重非预期"})
    o.register_material(material_id="STAT-2026Q3", version="3",
                        category="STATISTIC", content={"prr": 2.3, "ci": [1.1, 4.8]})
    print("  三个资料版本已注册")

    # 2) 问询登记：问题清单 + 截止时间 + 可引用资料范围（冻结）
    step("2. 监管问询登记（NMPA/CN，两问，截止 10-20）")
    inq = o.register_inquiry(
        regulator="NMPA", jurisdiction="CN", title="2026 年第 18 号问询",
        questions=[{"qid": "Q1", "text": "病例 7788 的事件与转归？"},
                   {"qid": "Q2", "text": "加急判定依据与统计结论？"}],
        deadline="2026-10-20T00:00:00+00:00",
        allowed_material_ids=["CASE-7788", "RULE-EXP", "STAT-2026Q3"],
        registered_by="intake-desk")
    print("  inquiry:", inq["id"][:12], "scope:",
          inq["scope"]["allowed_material_ids"])

    # 3) 底稿锁定：精确版本
    step("3. 底稿锁定（把引用冻结到精确版本）")
    draft = o.lock_draft(
        inq["id"],
        ["CASE:CASE-7788@1", "REG:RULE-EXP@2026-1", "STAT:STAT-2026Q3@3"],
        created_by="lead")
    for p in draft["pins"]:
        print("  pin:", p["ref"])

    # 4) 缺失/越权/矛盾会阻断（演示一次越权：另一问询范围只含法规）
    step("4. 引用越权被阻止")
    o.register_material(material_id="OUT-OF-SCOPE", version="1",
                        category="CASE", content={"secret": 1})
    narrow = o.register_inquiry(
        regulator="NMPA", jurisdiction="CN", title="范围受限问询",
        questions=[{"qid": "Q1", "text": "仅可引用法规"}],
        deadline="2026-10-20T00:00:00+00:00",
        allowed_material_ids=["RULE-EXP"])
    try:
        o.lock_draft(narrow["id"], ["CASE:OUT-OF-SCOPE@1"])
    except Exception as e:
        print("  阻断:", e.code, "-",
              e.details["out_of_scope"][0]["reason"])

    # 5) 多人协作撰写不同章节
    step("5. 两名撰写人并行提交不同章节")
    att = o.add_attachment(draft["id"], filename="病例7788.pdf",
                           content=b"%PDF-1.4 ...case source document...",
                           derived_from_ref="CASE:CASE-7788@1",
                           uploaded_by="alice")
    o.submit_section(
        draft["id"], "Q1",
        f"病例 7788：2026-09-07 皮疹，转归恢复 [CASE:CASE-7788@1]。"
        f"原件见附件 @attachment:{att['id']}",
        expected_revision=0, submitted_by="alice")
    o.submit_section(
        draft["id"], "Q2",
        "严重且非预期，按 15 日加急 [REG:RULE-EXP@2026-1]；"
        "本季度 PRR=2.3（95%CI 1.1-4.8）[STAT:STAT-2026Q3@3]。",
        expected_revision=0, submitted_by="bob")
    print("  Q1 by alice, Q2 by bob 已提交")

    # 6) 乐观修订号：bob 基于过期修订覆盖 Q1 被拒
    step("6. 竞争更新：过期提交不得覆盖")
    try:
        o.submit_section(draft["id"], "Q1", "我要用旧视图覆盖",
                         expected_revision=0, submitted_by="bob")
    except Exception as e:
        print("  拒绝:", e.code, e.details)
    print("  冲突留痕:", len(o.conflicts(draft["id"])["section_conflicts"]), "条")

    # 7) 组稿期间引用漂移：病例新版本到达，不混入当前底稿，另立候选
    step("7. 引用漂移：病例 v2 到达 -> 带原因的新底稿候选")
    o.register_material(material_id="CASE-7788", version="2", category="CASE",
                        content={"patient_name": "张三", "event": "皮疹",
                                 "outcome": "随访仍有色素沉着"},
                        fact_key="OUTCOME", fact_value="色素沉着",
                        registered_by="pv-officer")
    base_pins = [p["ref"] for p in o.get_draft(draft["id"])["pins"]]
    cands = [d for d in o.list_drafts(inq["id"]) if d["kind"] == "CANDIDATE"]
    print("  当前底稿仍锁定:", [r for r in base_pins if "CASE-7788" in r])
    cand = o.get_draft(cands[0]["id"])
    print("  候选已锁新版本:", [p["ref"] for p in cand["pins"]
                               if "CASE-7788" in p["ref"]])
    print("  原因:", cand["drift_reason"], "（章节/附件已克隆，可独立继续）")

    # 8) 定稿校验：先故意缺一个附件引用，再补齐（这里 BASE 已齐备，演示校验报告）
    step("8. 定稿校验（每问唯一回答 / 引用一致 / 敏感字段）")
    o.put_disclosure_policy(jurisdiction="CN", rules={
        "sensitive_fields": {"patient_identifier": "PROHIBIT"}})
    report = o.validate(draft["id"])
    print("  ok =", report["ok"], "| answered =",
          f"{report['answered_count']}/{report['question_count']}",
        "| warnings =", [w["code"] for w in report["warnings"]])

    # 9) 定稿 -> 不可变提交件 + 外发编号
    step("9. 定稿封存（不可变提交件，分配外发编号）")
    sealed = o.finalize(draft["id"], sealed_by="lead")
    sub = o.submission_status(sealed["submission_id"])
    print("  outgoing_number:", sub["outgoing_number"], "| kind:", sub["kind"])
    print("  manifest_hash:", sub["manifest_hash"][:24], "...")
    print("  每问 answer_uid:",
          [(a["qid"], a["answer_uid"][:8]) for a in sub["manifest"]["answers"]])

    # 10) 发送（网关首次临时故障，自动重试；重复请求不重复外发）
    step("10. 发送（首次临时故障->重试；重复请求幂等）")
    r = o.send(sub["id"])
    print("  发送:", r["status"], "受理号:", r["response"]["receipt_number"],
          "| attempts:", o.submission_status(sub["id"])["attempts"])
    o.send(sub["id"])
    print("  重复发送后网关受理次数 =", gw.accept_count(sub["outgoing_number"]),
          "(必须为 1)")

    # 11) 回执登记
    step("11. 监管回执登记")
    o.register_receipt(sub["id"], receipt_number=r["response"]["receipt_number"],
                       payload={"outgoing_number": sub["outgoing_number"],
                                "regulator": "NMPA"}, registered_by="gateway-bot")
    print("  状态:", o.submission_status(sub["id"])["status"])

    # 12) 定稿后不可改；新情况只能产生更正件（原提交件与回执保留）
    step("12. 提交后更正：产生更正件，原件保留")
    corr_draft = o.create_correction_draft(
        sub["id"], refs=["CASE:CASE-7788@2", "REG:RULE-EXP@2026-1",
                         "STAT:STAT-2026Q3@3"], reason="随访补充转归 v2",
        created_by="alice")
    a1 = corr_draft["attachments"][0]
    o.submit_section(
        corr_draft["id"], "Q1",
        f"更正：随访仍有色素沉着 [CASE:CASE-7788@2] @attachment:{a1['id']}",
        expected_revision=1, submitted_by="alice")
    o.submit_section(corr_draft["id"], "Q2",
                     "依据不变 [REG:RULE-EXP@2026-1]；"
                     "[STAT:STAT-2026Q3@3]", expected_revision=1,
                     submitted_by="bob")
    csealed = o.finalize(corr_draft["id"], sealed_by="lead")
    csub = o.submission_status(csealed["submission_id"])
    cr = o.send(csub["id"])
    print("  更正编号:", csub["outgoing_number"], "| corrects:",
          sub["outgoing_number"], "| 发送:", cr["status"])
    chain = o.correction_chain(sub["id"])
    print("  链: 原件", chain["original"]["outgoing_number"], "-> 更正",
          [c["outgoing_number"] for c in chain["corrections"]])

    # 13) 独立核对 + 审计链
    step("13. 独立核对（提交件/哈希/编号/回执/审计链）")
    v = o.verify_all()
    print("  全库核对 ok =", v["ok"], "| 审计链完好 =", v["audit_chain_ok"])

    step("14. 操作留痕（只增哈希链）")
    types = [e["event_type"] for e in o.audit_trail(inquiry_id=inq["id"])]
    print("  事件序列:")
    for t in dict.fromkeys(types):
        print("   -", t)
    print("\n演示数据库目录:", d)


if __name__ == "__main__":
    main()
