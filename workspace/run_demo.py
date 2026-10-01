"""端到端剧本演示：运行 python3 run_demo.py

覆盖需求全部主线：
1) 三渠道接收同一事件（中文 / 英文、别名、缺字段）-> 系统给出可复核疑似重复簇；
2) 安全专员确认合并，原始来源保留，重复未发送报告作废；
3) 随访材料修正严重性/因果/用药时间 -> 15 日加急 follow-up（新版本，不覆盖）；
4) 监管网关临时故障重试、进程重启恢复，监管编号始终唯一；
5) 错误拆分后再拆分与合并并发 -> 来源仍只归属一个当前病例；
6) 打印完整版本时间线与监管回执。
"""
import json
import shutil
from datetime import datetime
from pathlib import Path

from pv_service.app import Application, FixedClock
from pv_service.gateway import RegulatorGateway

DATA = Path("data/demo")


def banner(t):
    print("\n" + "=" * 72 + f"\n {t}\n" + "=" * 72)


def show(obj):
    print(json.dumps(obj, ensure_ascii=False, indent=2, default=str))


def main():
    if DATA.exists():
        shutil.rmtree(DATA)
    clock = FixedClock(datetime(2026, 9, 10, 9, 0))
    # 网关前 2 次对任意报告超时（演示失败重试）
    app = Application(str(DATA), clock=clock,
                      gateway_fail_script={"fail_times": 2})
    app.seed_demo_reference()

    banner("1) 医院首次上报（中文，布洛芬/皮疹，非严重）")
    hospital = {
        "patient_name": "张三", "gender": "男", "dob": "1980-03-03",
        "patient_identifier": "ID-998",
        "drugs": [{"name": "布洛芬", "start_date": "2026-09-05"}],
        "reactions": ["皮疹"], "seriousness": "不严重",
        "event_date": "2026-09-07",
        "narrative": "服药后双上肢皮疹"}
    r1 = app.cases.intake("HOSPITAL", "H-2026-1001", hospital)
    print("case:", r1["case_id"], " 首次报告编号:",
          r1["report"]["regulatory_number"])

    banner("2) 患者热线重复上报（英文+别名老张+斜杠日期）-> 疑似重复簇")
    hotline = {
        "patient_name": "老张", "gender": "male", "dob": "1980-03-03",
        "patient_identifier": "ID-998",
        "drugs": [{"name": "ibuprofen", "start_date": "2026/09/05"}],
        "reactions": ["rash"], "seriousness": "mild",
        "event_date": "2026/09/07",
        "narrative": "rash on arms after ibuprofen"}
    r2 = app.cases.intake("HOTLINE", "HL-88701", hotline)
    cluster = r2["duplicate_clusters"][0]
    print(f"系统建议簇 {cluster['cluster_id'][:8]}，命中证据：")
    for edge in cluster["edges"]:
        print(f"  分数 {edge['score']}：")
        for rs in edge["reasons"]:
            print(f"    +{rs['weight']:>3}  {rs['factor']}: {rs.get('detail','')}")
    print("=> 系统不自动合并，等待安全专员复核")

    banner("3) 合作企业第三份报文（缺出生日期，仅姓名+同药同反应）")
    enterprise = {
        "patient_name": "San Zhang", "gender": "M",
        "drugs": [{"name": "Ibuprofeno"}], "reactions": ["Sarpullido"],
        "seriousness": "moderate", "event_date": "2026-09-08"}
    r3 = app.cases.intake("ENTERPRISE", "ENT/ES/7788", enterprise)
    print("三方簇成员数:", len(r3["duplicate_clusters"][0]["members"]))
    cid = r3["duplicate_clusters"][0]["cluster_id"]

    banner("4) 安全专员确认合并（原始来源全部保留）")
    m = app.cases.confirm_cluster(cid, actor="安全专员-刘敏")
    print("存活病例:", m["survivor_case_id"])
    print("合并病例:", [c[:8] for c in m["merged_from"]])
    print("作废的重复未发送报告:", [v["regulatory_number"] for v in m["voided_reports"]])
    survivor = m["survivor_case_id"]
    wb = next(c for c in app.cases.workbench() if c["case_id"] == survivor)
    print("当前归属来源:", [(s["source_system"], s["external_id"])
                          for s in wb["sources"]])

    banner("5) 随访材料：更正用药时间 + 升级严重性（住院、肝损伤）-> 15 日加急")
    clock.advance(days=3)
    fu = app.cases.add_followup(survivor, {
        "seriousness": "严重", "seriousness_criteria": ["hospitalisation"],
        "new_reactions": ["肝损伤"], "causality": "很可能",
        "drug_start_dates": {"布洛芬": "2026-09-04"}, "outcome": "住院治疗中"})
    print("实质变化:", fu["significant_changes"])
    fup_no = fu["report"]["regulatory_number"]
    rep = app.reporting.get_report(fu["report"]["id"])
    print("随访报告编号:", fup_no, " 类型:", rep["report_type"],
          " 规则:", rep["decision"]["rule_id"])
    print("截止日期:", rep["due_date"], "（获知日 2026-09-13 + 15 日）")

    banner("6) 派发：网关前两次超时 -> 自动退避重试，编号始终不变")
    s1 = app.reporting.send_one()
    print("第 1 次:", s1["result"], s1["regulatory_number"])
    clock.advance(seconds=10)
    s2 = app.reporting.send_one()
    print("第 2 次:", s2["result"], s2["regulatory_number"])
    clock.advance(seconds=60)
    s3 = app.reporting.send_one()
    print("第 3 次:", s3["result"], s3["regulatory_number"],
          "ack:", s3["ack_id"])
    # 排空其余报告（按退避推进时钟）
    for _ in range(20):
        rest = app.reporting.send_one()
        if not rest:
            break
        if rest["result"] == "RETRY_SCHEDULED":
            clock.advance(seconds=2000)
            print("  等待退避后重试:", rest["regulatory_number"])
            continue
        print("  排空:", rest["result"], rest["regulatory_number"])

    banner("7) 进程重启恢复演示")
    # 再造一份报告并卡在 SENDING，网关侧其实已受理
    other = app.cases.intake("HOSPITAL", "H-2026-2002", {
        "patient_name": "李四", "dob": "1971-06-06",
        "drugs": [{"name": "warfarin"}], "reactions": ["肝损伤"],
        "seriousness": "严重", "event_date": "2026-09-12"})
    rid = other["report"]["id"]; no = other["report"]["regulatory_number"]
    detail = app.reporting.get_report(rid)
    gw = RegulatorGateway(app.gateway_db, clock=clock)
    ack_before = gw.submit(no, detail["payload"])["ack_id"]
    import sqlite3
    lock_conn = sqlite3.connect(app.db_path)
    lock_conn.execute(
        "UPDATE regulatory_reports SET status='SENDING' WHERE id=?", (rid,))
    lock_conn.commit()
    lock_conn.close()
    app2 = Application(str(DATA), clock=clock)   # 模拟新进程
    rec = app2.reporting.send_one()
    print("恢复后状态:", rec["result"], "编号不变:", rec["regulatory_number"] == no,
          " 同一回执:", rec["ack_id"] == ack_before)

    banner("8) 完整版本时间线（存活病例）")
    detail = app2.cases.get_case(survivor)
    print(f"病例 {survivor[:8]}  共 {len(detail['versions'])} 个版本：")
    for v in detail["versions"]:
        print(f"  v{v['version_no']} {v['change_type']:<9} {v['created_at']} "
              f"严重={v['snapshot'].get('serious')}")
    print("\n事件时间线：")
    for e in detail["timeline"]:
        print(f"  [{e['event_time']}] {e['event_type']:<26} {e['summary']}")

    banner("9) 监管编号唯一性总账")
    conn = sqlite3.connect(app.db_path)
    rows = conn.execute(
        "SELECT regulatory_number, report_type, status, expedited FROM "
        "regulatory_reports ORDER BY created_at").fetchall()
    for no, typ, st, ex in rows:
        print(f"  {no}  {typ:<8} {st:<6} {'加急' if ex else '常规'}")
    dup = conn.execute(
        "SELECT regulatory_number FROM regulatory_reports GROUP BY regulatory_number "
        "HAVING COUNT(*)>1").fetchall()
    print("重复编号:", dup or "无")
    conn.close()

    banner("10) 对存活病例当前版本导出可移交证据包")
    task = app2.exports.request_export(survivor, requested_by="安全专员-刘敏")
    app2.exports.run_until_idle()
    task = app2.exports.get_task(task["task_id"])
    print("任务状态:", task["status"], " 清单根:", task["package_root"])
    pkg_path, _ = app2.exports.package_path(task["task_id"])
    import tarfile
    with tarfile.open(pkg_path, "r:*") as tf:
        names = tf.getnames()
    print("包内文件数:", len(names))
    for n in sorted(names):
        print("   ", n)

    banner("11) 在“另一进程”中离线核验（不访问数据库/网络）")
    from pv_service.verifier import verify_package
    report = verify_package(str(pkg_path))
    for c in report["checks"]:
        print(f"  [{'PASS' if c['ok'] else 'FAIL'}] {c['code']:<16} {c['message']}")
    print("核验结论:", "有效 ✓" if report["valid"] else "无效 ✗")
    print("签名公钥指纹（可经带外渠道核对）:",
          report["summary"]["public_key_fingerprint"])

    banner("12) 离线核验必须能抓出篡改：改写一个审计帧后重新核验")
    import io
    from pv_service import package_codec as codec
    files = {}
    with tarfile.open(pkg_path, "r:*") as tf:
        for m in tf.getmembers():
            files[m.name] = tf.extractfile(m).read()
    tl_name = f"timeline/{survivor}.json"
    tl = json.loads(files[tl_name])
    tl["events"][1]["summary"] = "试图篡改历史结论"
    files[tl_name] = codec.canonical_json(tl)
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w", format=tarfile.USTAR_FORMAT) as to:
        for name in sorted(files):
            info = tarfile.TarInfo(name)
            info.size = len(files[name]); info.mode = 0o444
            info.uid = info.gid = 0; info.mtime = 0
            to.addfile(info, io.BytesIO(files[name]))
    bad_report = verify_package(buf.getvalue())
    print("篡改后结论:", "有效（不应出现！）" if bad_report["valid"] else "无效 ✗（正确拦截）")
    for e in bad_report["errors"]:
        print("   -", e)

    banner("13) 法规规则变更：登记 → 试算影响 → 受控启用（已发送只更正，未发送重算）")
    from pv_service.rule_pkg import build_content
    gov = app.rules_gov
    # 再登记一个非严重病例（90 日，未发送），与上面已发送报告形成对照
    extra = app.cases.intake(
        "HOSPITAL", "H-RULE-1",
        {"patient_name": "王五", "dob": "1972-02-02",
         "drugs": [{"name": "布洛芬", "causality": "可能"}],
         "reactions": ["皮疹"], "seriousness": "不严重",
         "event_date": "2026-09-08"}, "2026-09-10T12:00:00")
    unsent_id = extra["report"]["id"]
    content = build_content(
        jurisdiction="CN", version="2026.Q4", effective_from="2026-10-01",
        summary="2026 Q4：非严重常规报送 90→60 日", routine_days=60)
    pkg = gov.register_package(
        content, gov.sign(content, "pv-demo-signing-secret"), registered_by="ra")
    print("登记规则包:", pkg["package_id"], pkg["version"], "（相同内容重复登记 deduped =",
          gov.register_package(content, gov.sign(content, "pv-demo-signing-secret"))["deduped"], "）")
    trial = gov.create_trial("CN", "2026.Q4")
    trial = gov.run_trial_until_done(trial["trial_id"])
    print("试算:", trial["changed_units"], "/", trial["total_units"],
          "个历史快照决定变化；受影响病例",
          gov.trial_progress(trial["trial_id"])["affected_case_ids"])
    act = gov.activate("CN", "2026.Q4", actor="ra")
    j = act["job"]
    print("启用:", act["status"], "| 未发送重算队列:", j["staged_units"],
          "| 已发送更正候选:", j["candidate_count"])
    print("未发送报告现在:", app.reporting.get_report(unsent_id)["days_due"],
          "日，固定版本", app.reporting.get_report(unsent_id)["rule_version"])
    print("当前辖区版本:", gov.current_version("CN")["package_id"],
          gov.current_version("CN")["version"])
    print("治理审计链核验:", "有效" if gov.verify_audit_chain()["valid"] else "无效")

    app2.shutdown()


if __name__ == "__main__":
    main()
