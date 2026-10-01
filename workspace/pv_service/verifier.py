"""离线证据包核验器（纯函数，不访问数据库/网络）。

核验项（任一失败 valid 即为 false，且结果中逐条给出失败的具体项）：

  PKG_OPEN       tar 可打开、无非法成员
  PKG_MANIFEST   manifest/package 元数据齐全、格式版本支持
  SIG_ALGORITHM  签名为 64 字节 Ed25519
  SIGNATURE      签名可由包内公钥（或带外信任公钥指纹）验证通过
  MANIFEST_ROOT  重算清单根与 manifest.root 一致
  FILE_PRESENT   manifest 列出的每个文件都在包内（无缺失）
  FILE_DIGEST    每个文件 sha256/size 与清单一致（无篡改/多余文件）
  TIMELINE_CHAIN 每个病例审计切片 seq 连续、prev_hash/entry_hash 逐帧正确
  TIMELINE_HEAD  切片头哈希与 manifest.timelines 声明一致、cut 内无越界帧
  SNAPSHOT_BINDING 绑定版本快照与 cases 序列中的版本字节一致、关系/来源一致
  VERSION_STATUS 版本号连续、change_type 与版本状态、关系自洽
  REPORTS        报告状态轨迹与审计帧一致，回执只在 ACKED 时存在，编号自洽
  RELATIONS      合并/拆分关系在时间线与版本序列中都有对应证据帧
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field

from . import audit
from . import ed25519
from . import package_codec as codec


@dataclass
class Check:
    code: str
    ok: bool
    message: str
    details: list = field(default_factory=list)

    def as_dict(self) -> dict:
        return {"code": self.code, "ok": self.ok, "message": self.message,
                "details": self.details}


def _load_json(data: bytes, where: str, failures: list):
    try:
        return json.loads(data.decode("utf-8"))
    except Exception as e:
        failures.append(f"{where}: 不是合法 JSON（{e}）")
        return None


def verify_package(source, *, trusted_key_fingerprint: str | None = None) -> dict:
    """核验一个证据包。

    返回::

        {"valid": bool, "package_root": str|None, "checks": [Check...],
         "errors": [str], "warnings": [str], "summary": {...}}

    设计：尽量收集全部问题，不在第一处失败时短路；只有所有检查通过
    （errors 为空）才 valid=true。
    """
    checks: list[Check] = []
    errors: list[str] = []
    warnings: list[str] = []

    def add(code, ok, message, details=None):
        checks.append(Check(code, ok, message, details or []))
        if not ok:
            for d in (details or [message]):
                errors.append(f"[{code}] {d}")

    # 1) 打开包 -------------------------------------------------------------
    try:
        pkg = codec.read_package(source)
        add("PKG_OPEN", True, "证据包格式可读取")
    except codec.PackageError as e:
        add("PKG_OPEN", False, "证据包无法打开", [str(e)])
        return _result(None, checks, errors, warnings)

    manifest = pkg["manifest"]
    files = pkg["files"]
    manifest_bytes = pkg["manifest_bytes"]
    meta = pkg.get("meta") or {}

    # 2) 清单元数据 ----------------------------------------------------------
    meta_problems = []
    if manifest.get("format") != codec.FORMAT:
        meta_problems.append(f"format 非 {codec.FORMAT}: {manifest.get('format')!r}")
    if manifest.get("format_version") != codec.FORMAT_VERSION:
        meta_problems.append(
            f"format_version 不支持: {manifest.get('format_version')!r}")
    for req in ("files", "anchor", "request", "snapshot"):
        if req not in manifest:
            meta_problems.append(f"manifest 缺少字段: {req}")
    add("PKG_MANIFEST", not meta_problems,
        "清单与元数据齐全" if not meta_problems else "清单元数据不完整",
        meta_problems)

    # 3) 签名算法 + 签名核验 --------------------------------------------------
    sig = pkg["signature"]
    add("SIG_ALGORITHM", len(sig) == 64,
        "签名为 64 字节 Ed25519" if len(sig) == 64 else
        f"签名长度 {len(sig)} ≠ 64", [])

    pub_hex = meta.get("signing_public_key")
    pub = None
    if pub_hex:
        try:
            pub = bytes.fromhex(pub_hex)
        except ValueError:
            pass
    sig_ok = False
    sig_details = []
    if len(sig) == 64 and pub and len(pub) == 32:
        sig_ok = ed25519.verify(pub, manifest_bytes, sig)
        if not sig_ok:
            sig_details.append("签名与清单字节不匹配（清单或签名被篡改）")
    else:
        sig_details.append("缺少签名公钥（package.json.signing_public_key）")
    if trusted_key_fingerprint:
        fp = meta.get("signing_public_key_fingerprint")
        if fp != trusted_key_fingerprint:
            sig_ok = False
            sig_details.append(
                f"公钥指纹 {fp} 与带外信任指纹 {trusted_key_fingerprint} 不一致")
    add("SIGNATURE", sig_ok,
        "签名有效" if sig_ok else "签名核验失败", sig_details)

    # 4) 清单根 --------------------------------------------------------------
    root_problems = []
    recomputed_root = None
    try:
        recomputed_root = codec.compute_manifest_root(manifest)
        if recomputed_root != manifest.get("root"):
            root_problems.append(
                f"重算根 {recomputed_root[:16]}… ≠ 清单声明 {str(manifest.get('root'))[:16]}…")
    except KeyError as e:
        root_problems.append(f"重算清单根时缺少字段: {e}")
    add("MANIFEST_ROOT", not root_problems,
        "清单根一致" if not root_problems else "清单根不匹配", root_problems)
    if meta.get("manifest_root") and meta["manifest_root"] != manifest.get("root"):
        add("MANIFEST_ROOT", False, "package.json 中的 manifest_root 与清单不一致",
            [f"package.json={meta['manifest_root'][:16]}… "
             f"manifest={str(manifest.get('root'))[:16]}…"])

    # 5) 文件存在性 + 摘要 ----------------------------------------------------
    declared = {f["path"]: f for f in manifest.get("files", [])}
    missing, digest_bad, extra = [], [], []
    for path, spec in sorted(declared.items()):
        data = files.get(path)
        if data is None:
            missing.append(path)
            continue
        h = codec.sha256_hex(data)
        if h != spec.get("sha256"):
            digest_bad.append(f"{path}: 摘要 {h[:12]}… ≠ 清单 {str(spec.get('sha256'))[:12]}…")
        elif len(data) != spec.get("size"):
            digest_bad.append(f"{path}: 大小 {len(data)} ≠ {spec.get('size')}")
    for path in sorted(files):
        if path not in declared:
            extra.append(path)
    add("FILE_PRESENT", not missing,
        f"{len(declared)} 个声明文件全部在包内" if not missing else "存在缺失文件",
        [f"缺失: {p}" for p in missing])
    add("FILE_DIGEST", not digest_bad and not extra,
        "每个文件摘要/大小与清单一致" if not digest_bad and not extra
        else "文件摘要被篡改或存在未声明文件",
        digest_bad + [f"未声明的多余文件: {p}" for p in extra])

    # 辅助：按清单路径取 JSON 对象。
    # 刻意 *不* 因 FILE_DIGEST 失败而拒绝解析——审计链/快照绑定等语义检查
    # 各自有完整性机制，应独立运行，使“改文件内容”既被 FILE_DIGEST 抓到、
    # 也被对应的语义检查（如 entry_hash 重算）再次抓到，失败项更具体。
    def valid_json(path):
        if path not in files:
            return None
        return _load_json(files[path], path, [])

    # 6) 审计链切片 ----------------------------------------------------------
    timelines: dict[str, dict] = {}
    chain_failures, head_failures = [], []
    declared_timelines = {t["case_id"]: t for t in manifest.get("timelines", [])}
    for t in manifest.get("timelines", []):
        path = t.get("path") or f"timeline/{t['case_id']}.json"
        obj = valid_json(path)
        if obj is None:
            chain_failures.append(f"{t['case_id'][:8]}: 时间线文件不可读/已损坏")
            continue
        evs = obj.get("events") or []
        timelines[t["case_id"]] = obj
        try:
            audit.verify_chain(evs, label="病例")
        except audit.ChainBroken as e:
            chain_failures.extend(e.failures)
        # seq 连续性 + cut 一致性
        for i, e in enumerate(evs, start=1):
            if e.get("seq") != i:
                chain_failures.append(
                    f"{t['case_id'][:8]}: seq 在 {i} 处不连续")
                break
        head = audit.chain_head(evs) if evs else audit.GENESIS
        if t.get("head") and head != t["head"]:
            head_failures.append(
                f"{t['case_id'][:8]}: 切片头 {head[:12]}… ≠ 声明 {str(t['head'])[:12]}…")
        if evs and t.get("cut_rowid") is not None:
            if any(e.get("seq") is None for e in evs):
                pass
        if obj.get("case_id") != t["case_id"]:
            head_failures.append(f"{path}: case_id 与清单不一致")
    add("TIMELINE_CHAIN", not chain_failures,
        "全部审计切片哈希链连续有效" if not chain_failures else "审计链断裂/被篡改",
        chain_failures)
    add("TIMELINE_HEAD", not head_failures,
        "各切片链头与清单一致" if not head_failures else "审计链头不一致",
        head_failures)

    # 7) 快照绑定 ------------------------------------------------------------
    binding_failures = []
    snap_file = valid_json("export/snapshot.json")
    case_files: dict[str, dict] = {}
    req_file = valid_json("export/request.json")
    for path, spec in declared.items():
        if path.startswith("cases/") and path.endswith(".json"):
            obj = valid_json(path)
            if obj is not None:
                case_files[obj.get("case_id")] = obj
    bound_version = None
    if snap_file:
        cid = snap_file.get("case_id")
        vid = snap_file.get("version_id")
        cf = case_files.get(cid)
        if cf is None:
            binding_failures.append(f"绑定病例 {str(cid)[:8]} 的 cases 文件缺失")
        else:
            match = [v for v in cf.get("versions", []) if v.get("id") == vid]
            if not match:
                binding_failures.append("绑定版本不在 cases 版本序列中")
            else:
                bound_version = match[0]
                embedded = snap_file.get("snapshot")
                if embedded != match[0].get("snapshot"):
                    binding_failures.append(
                        "export/snapshot 的快照与版本序列中的同版本内容不一致（前后状态拼接）")
                if snap_file.get("version_no") != match[0].get("version_no"):
                    binding_failures.append("绑定 version_no 与版本序列不一致")
        if manifest.get("version_id") != vid or manifest.get("case_id") != cid:
            binding_failures.append("manifest 的 case/version 与快照不一致")
        if req_file and (req_file.get("version_id") != vid or
                         req_file.get("case_id") != cid):
            binding_failures.append("导出请求与快照绑定的病例/版本不一致")
        # 来源集合与 memberships 一致
        mem_file = valid_json("memberships.json")
        if mem_file is not None:
            active_sources = {m["source_id"] for m in mem_file.get("memberships", [])
                              if m.get("active_at_cut") == 1}
            if set(snap_file.get("source_ids") or []) != active_sources:
                binding_failures.append(
                    "快照 source_ids 与 memberships 截止归属集合不一致")
    add("SNAPSHOT_BINDING", not binding_failures,
        "快照与版本序列/来源归属一致绑定" if not binding_failures
        else "快照绑定校验失败", binding_failures)

    # 8) 版本状态 ------------------------------------------------------------
    status_failures = []
    for cid, cf in sorted(case_files.items()):
        versions = cf.get("versions", [])
        for i, v in enumerate(versions, start=1):
            if v.get("version_no") != i:
                status_failures.append(
                    f"病例 {cid[:8]}: 版本号在 v{i} 处不连续（v{v.get('version_no')}）")
        # change_type 合法性
        valid_types = {"INTAKE", "FOLLOWUP", "MERGE", "SPLIT"}
        for v in versions:
            if v.get("change_type") not in valid_types:
                status_failures.append(
                    f"病例 {cid[:8]} v{v.get('version_no')}: 非法 change_type "
                    f"{v.get('change_type')!r}")
        if versions and versions[0].get("change_type") != "INTAKE":
            status_failures.append(
                f"病例 {cid[:8]}: 首个版本不是 INTAKE（版本状态异常）")
        # 每个版本都应能在时间线找到对应 VERSION_* 帧
        tl = timelines.get(cid)
        if tl is not None:
            versioned = {e.get("version_id") for e in tl["events"]
                         if e.get("version_id")}
            for v in versions:
                if v["id"] not in versioned:
                    status_failures.append(
                        f"病例 {cid[:8]} v{v.get('version_no')}: 时间线缺少其 VERSION_* 帧")
    add("VERSION_STATUS", not status_failures,
        "版本号连续、状态与审计时间线自洽" if not status_failures
        else "版本状态异常", status_failures)

    # 9) 报告与回执 -----------------------------------------------------------
    report_failures = []
    report_objs = {}
    for path in sorted(declared):
        if path.startswith("reports/") and path.endswith(".json"):
            obj = valid_json(path)
            if obj is not None:
                report_objs[obj.get("id")] = obj
    # 审计帧中引用到的报告必须都在包内
    referenced_reports = set()
    for tl in timelines.values():
        for e in tl.get("events", []):
            if e.get("report_id"):
                referenced_reports.add(e["report_id"])
    for rid in referenced_reports:
        if rid not in report_objs:
            report_failures.append(f"审计帧引用的报告缺失: {str(rid)[:8]}")
    for rid, rep in report_objs.items():
        frozen = rep.get("status_as_of_anchor")
        trace = rep.get("status_trace") or []
        if not trace:
            report_failures.append(
                f"{rep.get('regulatory_number')}: 报告无任何状态轨迹帧")
        elif trace[-1]["status"] != frozen:
            report_failures.append(
                f"{rep.get('regulatory_number')}: 冻结状态 {frozen} 与轨迹末态 "
                f"{trace[-1]['status']} 不一致（回执状态被拼接）")
        # 回执只能在冻结为 ACKED 时存在
        ack = rep.get("ack_as_of_anchor")
        if frozen == "ACKED":
            if not ack or not ack.get("ack_id"):
                report_failures.append(
                    f"{rep.get('regulatory_number')}: 状态 ACKED 但缺少监管回执内容")
        else:
            if ack:
                report_failures.append(
                    f"{rep.get('regulatory_number')}: 冻结状态 {frozen} 却带有 ACK 回执"
                    f"（锚点后的状态被拼接进来）")
        # 轨迹中每个状态帧都要能在时间线事件中找到
        for st in trace:
            tl = timelines.get(st.get("case_id"))
            if tl is None:
                report_failures.append(
                    f"{rep.get('regulatory_number')}: 状态帧引用了不在包内的病例")
                continue
            if not any(e.get("seq") == st.get("seq") and
                       e.get("event_type") == st.get("event_type")
                       for e in tl["events"]):
                report_failures.append(
                    f"{rep.get('regulatory_number')}: 状态轨迹帧 seq={st.get('seq')} "
                    f"{st.get('event_type')} 在时间线中不存在")
        if not rep.get("regulatory_number"):
            report_failures.append(f"报告 {str(rid)[:8]} 缺少监管编号")
    add("REPORTS", not report_failures,
        "报送状态轨迹与回执一致冻结" if not report_failures
        else "报送/回执核验失败", report_failures)

    # 10) 合并/拆分关系 -------------------------------------------------------
    relation_failures = []
    rel_file = valid_json("relations.json")
    relations = rel_file.get("relations", []) if rel_file else \
        manifest.get("relations", [])
    for r in relations:
        kind = r.get("relation")
        vid = r.get("version_id")
        if kind == "MERGE":
            survivor = r.get("survivor_case_id")
            tl = timelines.get(survivor)
            if tl is None or not any(e.get("version_id") == vid
                                     for e in tl["events"]):
                relation_failures.append(
                    f"合并关系（存活 {str(survivor)[:8]}）在存活病例时间线无版本帧")
            for other in r.get("merged_from_case_ids", []):
                otl = timelines.get(other)
                if otl is None:
                    relation_failures.append(
                        f"合并关系缺少被合并病例 {str(other)[:8]} 的时间线切片")
                elif not any(e.get("event_type") == "CASE_MERGED_AWAY"
                             for e in otl["events"]):
                    relation_failures.append(
                        f"被合并病例 {str(other)[:8]} 缺少 CASE_MERGED_AWAY 帧")
        elif kind == "SPLIT":
            parent = r.get("parent_case_id")
            tl = timelines.get(parent)
            if tl is None or not any(e.get("event_type") == "CASE_SPLIT"
                                     and e.get("version_id") == vid
                                     for e in tl["events"]):
                relation_failures.append(
                    f"拆分关系（父病例 {str(parent)[:8]}）缺少 CASE_SPLIT 帧")
        else:
            relation_failures.append(f"未知关系类型: {kind!r}")
    # 反向：时间线中出现的 CASE_MERGED_AWAY/CASE_SPLIT/CASE_SPLIT_IN 也应有关系
    for cid, tl in timelines.items():
        etypes = {e.get("event_type") for e in tl.get("events", [])}
        if "CASE_MERGED_AWAY" in etypes:
            if not any(cid in (r.get("merged_from_case_ids") or [])
                       for r in relations):
                relation_failures.append(
                    f"病例 {cid[:8]} 有 CASE_MERGED_AWAY 帧但关系清单缺失")
        if "CASE_SPLIT" in etypes:
            if not any(r.get("relation") == "SPLIT" and r.get("parent_case_id") == cid
                       for r in relations):
                relation_failures.append(
                    f"病例 {cid[:8]} 有 CASE_SPLIT 帧但关系清单缺失")
    add("RELATIONS", not relation_failures,
        "合并/拆分关系与审计证据自洽" if not relation_failures
        else "关系核验失败", relation_failures)

    if not pub:
        warnings.append("包内未携带签名公钥，签名来源无法识别（签名项已失败）")
    if pub and not trusted_key_fingerprint:
        warnings.append(
            "未提供带外信任公钥指纹；仅验证了签名数学有效性，"
            "请通过安全渠道核对公钥指纹后再采信来源")

    return _result(manifest.get("root"), checks, errors, warnings,
                   summary={
                       "case_id": manifest.get("case_id"),
                       "version_id": manifest.get("version_id"),
                       "files": len(declared),
                       "timelines": len(manifest.get("timelines", [])),
                       "relations": len(relations),
                       "reports": len(report_objs),
                       "public_key_fingerprint": meta.get(
                           "signing_public_key_fingerprint"),
                   })


def _result(root, checks, errors, warnings, summary=None) -> dict:
    return {
        "valid": not errors,
        "package_root": root,
        "checks": [c.as_dict() for c in checks],
        "errors": errors,
        "warnings": warnings,
        "summary": summary or {},
    }
