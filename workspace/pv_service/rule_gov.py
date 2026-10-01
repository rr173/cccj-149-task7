"""药物安全法规规则变更的影响分析与受控启用服务。

在既有报送计算 / 双人会签 / 发送路径之外提供一个*治理层*，把法规口径
当作不可变、可签名、可追溯版本的资产来管理，并保证「启用只有一个结果、
已发送不被覆盖、批量可续跑、请求幂等」。

核心对象
--------
- **规则包**（``rule_packages``，不可变）：辖区 + 版本 + 生效区间 +
  依赖 + 内容（天数口径等）+ HMAC 签名。登记三校验：签名、依赖、
  同辖区生效区间冲突；相同内容（content_hash）重复登记返回原版本，
  不同内容占用同版本号被拒绝。
- **辖区当前版本**（``rule_jurisdiction_state``）：计算路径通过
  ``CaseService.resolve_rule`` 读取，决定新报告固定哪个规则版本。
- **试算任务**（``rule_trials`` + units）：对创建时冻结的一组历史快照
  分别用基线版本与候选版本重算决定，逐分片落差异。试算*只读病例*，
  绝不改写病例 / 会签 / 发送状态。
- **启用作业**（``rule_jobs`` + units）：两阶段——
  阶段一逐分片把每个受影响报告的新决定暂存（``rule_job_units``）；
  阶段二在*单事务*内原子应用（未发送重算/作废、已发送只生成更正候选）。
  任一分片失败 -> 作业 FAILED，版本指针不动，无部分启用。

已发送 vs 未发送
-----------------
- 未发送（QUEUED/SENDING/FAILED）：启用成功后按新版本重算，更新决定/
  截止日/固定版本，监管编号不变；新版本判定无需报送则作废旧未发送报告
  （VOID，编号保留），并推进数据修订号作废旧会签单（须重新会签）。
- 已发送（ACKED）：报告行、原始规则版本、历史结果*永不改动*，只生成
  一条可复核的 ``rule_correction_candidates`` 更正候选。

幂等 / 续跑 / 并发
------------------
- 批量单元表对 (job/trial, report/version) 有唯一约束，分片续跑靠
  ``next_shard`` 检查点 + 「只插入尚不存在的单元」，重复请求不重复
  生成差异、候选、审计事件（审计帧带 idempotency_key UNIQUE）。
- 启用/回滚/定时在共享 RLock + 辖区状态行 CAS 上串行；同 idempotency_key
  重放返回同一结论，因此并发启用/回滚/定时只有一个结果。
"""
from __future__ import annotations

import json
import sqlite3
import threading
import uuid

from . import audit
from . import db as dbmod
from . import rule_audit
from . import revision as revmod
from .rule_pkg import (
    LEGACY_DAYS, LEGACY_PACKAGE_ID, LEGACY_VERSION,
    RulePackageError, build_engine, content_hash, intervals_overlap,
    normalize_interval, sign_content, verify_signature, version_key,
)
from .rules import RuleContext
from .service import Clock, DomainError

# 演示签名人（便于端到端跑通；生产应由部署带外注册真实密钥）
DEMO_SIGNER_ID = "demo-regulator"
DEMO_SIGNER_SECRET = "pv-demo-signing-secret"  # noqa: S105 仅演示


def _uid() -> str:
    return uuid.uuid4().hex


def _json(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, default=str)


class RuleGovernanceError(DomainError):
    pass


class ProcessInterrupted(RuntimeError):
    """测试钩子模拟的「进程中断」：作业停在检查点（RUNNING），等待恢复续跑。"""


def _decision_diff(old: dict, new: dict) -> dict:
    """两份决定的可复核差异（字段级）。"""
    fields = ("should_report", "expedited", "days_due", "due_date", "rule_id")
    changes = {}
    for f in fields:
        if old.get(f) != new.get(f):
            changes[f] = {"old": old.get(f), "new": new.get(f)}
    return {
        "changed": bool(changes),
        "fields": changes,
        "old_rule_id": old.get("rule_id"),
        "new_rule_id": new.get("rule_id"),
        "old_should_report": old.get("should_report"),
        "new_should_report": new.get("should_report"),
    }


class RuleGovernanceService:
    def __init__(self, db_path: str, cases, *, clock: Clock | None = None,
                 shared_lock: threading.RLock | None = None,
                 shard_size: int = 50, auto_start: bool = False,
                 poll_seconds: float = 0.2, seed_demo_signer: bool = True,
                 crash_hook=None):
        self.db_path = db_path
        self.cases = cases                       # CaseService（取 is_listed / 引擎装配）
        self.clock = clock or Clock()
        self.shard_size = shard_size
        # 测试钩子：在作业分片暂存过程中按 (phase, job_id, shard_no) 抛错，
        # 模拟进程在分片中途崩溃（验证无部分启用 + 检查点续跑 + 不重复）。
        self.crash_hook = crash_hook
        # 测试钩子：强制用指定引擎重算（验证“新版本判定无需报送 -> VOID”
        # 等规则链分支）；生产路径恒为 None，按包内容构造引擎。
        self.engine_override = None
        self._lock = shared_lock or threading.RLock()
        self.poll_seconds = poll_seconds
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        dbmod.initialize(db_path)
        if seed_demo_signer:
            self.register_signing_key(DEMO_SIGNER_ID, DEMO_SIGNER_SECRET,
                                      created_by="system-bootstrap", if_missing=True)
        self._bootstrap_legacy()
        self.recover()
        if auto_start:
            self.start_scheduler()

    def _bootstrap_legacy(self) -> None:
        """登记内置历史口径 sentinel（builtin:legacy@0，辖区 '*'）。

        它让「回滚到历史口径」也能被外键引用并参与审计；真实业务辖区
        （CN/...）不会与其产生区间冲突（辖区不同）。幂等，可重复执行。
        """
        from .rule_pkg import legacy_view
        view = legacy_view()
        content = {
            "jurisdiction": "*", "package_id": LEGACY_PACKAGE_ID,
            "version": LEGACY_VERSION, "effective_from": "1970-01-01",
            "effective_to": None, "depends_on": [], "summary": view["summary"],
            "parameters": dict(LEGACY_DAYS),
        }
        chash = content_hash(content)
        with self._lock:
            conn = self._conn()
            try:
                with dbmod.transaction(conn):
                    if conn.execute("SELECT 1 FROM rule_packages WHERE id=?",
                                    (LEGACY_PACKAGE_ID,)).fetchone():
                        return
                    conn.execute(
                        "INSERT INTO rule_packages(id,package_id,jurisdiction,version,"
                        "effective_from,effective_to,content_json,content_hash,signature,"
                        "signer_id,depends_json,summary,registered_by,created_at) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (LEGACY_PACKAGE_ID, LEGACY_PACKAGE_ID, "*", LEGACY_VERSION,
                         "1970-01-01", None, _json(content), chash,
                         "builtin", "builtin", "[]", view["summary"],
                         "system-bootstrap", self._now()))
            finally:
                conn.close()

    # -- 基础设施 ------------------------------------------------------------
    def _conn(self) -> sqlite3.Connection:
        return dbmod.connect(self.db_path)

    def _now(self) -> str:
        return self.clock.now_iso()

    # -- 签名密钥注册表 ------------------------------------------------------
    def register_signing_key(self, signer_id: str, secret: str, *,
                             created_by: str | None = None,
                             if_missing: bool = False) -> dict:
        if not signer_id or not isinstance(signer_id, str):
            raise RuleGovernanceError("BAD_SIGNER", "signer_id 不能为空")
        if not secret or not isinstance(secret, str):
            raise RuleGovernanceError("BAD_SIGNER", "secret 不能为空")
        with self._lock:
            conn = self._conn()
            try:
                with dbmod.transaction(conn):
                    existing = conn.execute(
                        "SELECT signer_id FROM rule_signing_keys WHERE signer_id=?",
                        (signer_id,)).fetchone()
                    if existing:
                        if if_missing:
                            return {"signer_id": signer_id, "deduped": True}
                        raise RuleGovernanceError(
                            "SIGNER_EXISTS",
                            "签名人已注册；密钥只增不可改，请使用新的 signer_id", 409)
                    conn.execute(
                        "INSERT INTO rule_signing_keys(signer_id,secret,created_at,created_by)"
                        " VALUES(?,?,?,?)",
                        (signer_id, secret, self._now(), created_by))
                return {"signer_id": signer_id, "deduped": False}
            finally:
                conn.close()

    def _signer_secret(self, conn, signer_id: str) -> str:
        row = conn.execute("SELECT secret FROM rule_signing_keys WHERE signer_id=?",
                           (signer_id,)).fetchone()
        if not row:
            raise RuleGovernanceError("UNKNOWN_SIGNER",
                                      f"未知签名人: {signer_id}（未在密钥注册表登记）", 422)
        return row["secret"]

    @staticmethod
    def sign(content: dict, secret: str) -> str:
        """便捷封装：部署/测试侧用对称密钥对规则包内容签名。"""
        return sign_content(content, secret)

    # -- 规则包登记与校验 ----------------------------------------------------
    def register_package(self, content: dict, signature: str, *,
                         signer_id: str = DEMO_SIGNER_ID,
                         registered_by: str | None = None) -> dict:
        """登记一个不可变规则包。

        - content 必须含 jurisdiction/version/effective_from/effective_to；
        - 签名、依赖、生效区间三校验全过才入库；
        - 相同内容重复登记 -> 返回既有包（deduped=True）；
        - 不同内容占用同 (辖区,版本) -> 409 VERSION_CONFLICT。
        """
        if not isinstance(content, dict):
            raise RuleGovernanceError("BAD_CONTENT", "规则包内容必须是 JSON 对象")
        jurisdiction = str(content.get("jurisdiction") or "").strip()
        version = str(content.get("version") or "").strip()
        if not jurisdiction:
            raise RuleGovernanceError("BAD_JURISDICTION", "内容缺少 jurisdiction")
        try:
            normalize_interval(content.get("effective_from"), content.get("effective_to"))
            version_key(version)
        except RulePackageError as e:
            raise RuleGovernanceError(e.code, e.message, 400)
        eff_from = content["effective_from"]
        eff_to = content.get("effective_to")
        depends = content.get("depends_on") or []
        package_id = str(content.get("package_id") or jurisdiction)
        chash = content_hash(content)

        with self._lock:
            conn = self._conn()
            try:
                with dbmod.transaction(conn):
                    # 1) 相同内容 -> 返回原版本（不论辖区/版本字段是否一致，
                    #    内容指纹相同即同一不可变制品）
                    same = conn.execute(
                        "SELECT * FROM rule_packages WHERE content_hash=?",
                        (chash,)).fetchone()
                    if same:
                        return self._package_view(same, deduped=True)

                    # 2) 不同内容占用同一版本号 -> 拒绝
                    occ = conn.execute(
                        "SELECT id,content_hash FROM rule_packages "
                        "WHERE jurisdiction=? AND version=?",
                        (jurisdiction, version)).fetchone()
                    if occ:
                        raise RuleGovernanceError(
                            "VERSION_CONFLICT",
                            f"辖区 {jurisdiction} 版本 {version} 已被不同内容占用"
                            f"（包 {occ['id'][:8]}）；不可用不同内容复用同一版本号", 409)
                    occ2 = conn.execute(
                        "SELECT id FROM rule_packages WHERE package_id=? AND version=?",
                        (package_id, version)).fetchone()
                    if occ2:
                        raise RuleGovernanceError(
                            "VERSION_CONFLICT",
                            f"规则族 {package_id} 版本 {version} 已存在且内容不同", 409)

                    # 3) 签名校验（未知签名人/签名不符 -> 拒绝）
                    secret = self._signer_secret(conn, signer_id)
                    if not verify_signature(content, signature, secret):
                        raise RuleGovernanceError(
                            "BAD_SIGNATURE", "规则包签名校验失败（内容被改动或密钥不符）", 422)

                    # 4) 依赖校验：depends_on 的每个 (package,version) 必须已登记
                    for dep in depends:
                        d_pkg = str(dep.get("package") or jurisdiction)
                        d_ver = str(dep.get("version") or "")
                        found = conn.execute(
                            "SELECT 1 FROM rule_packages WHERE package_id=? AND version=?",
                            (d_pkg, d_ver)).fetchone()
                        if not found:
                            raise RuleGovernanceError(
                                "DEPENDENCY_MISSING",
                                f"依赖规则未登记：{d_pkg}@{d_ver}", 422)

                    # 5) 同辖区生效区间冲突（半开区间，null 端为正无穷）
                    for other in conn.execute(
                            "SELECT version,effective_from,effective_to FROM rule_packages "
                            "WHERE jurisdiction=?", (jurisdiction,)).fetchall():
                        if intervals_overlap(eff_from, eff_to,
                                             other["effective_from"],
                                             other["effective_to"]):
                            raise RuleGovernanceError(
                                "INTERVAL_CONFLICT",
                                f"生效区间与既有版本 {other['version']} "
                                f"[{other['effective_from']}, {other['effective_to']}) 相交", 409)

                    pid = _uid()
                    conn.execute(
                        "INSERT INTO rule_packages(id,package_id,jurisdiction,version,"
                        "effective_from,effective_to,content_json,content_hash,signature,"
                        "signer_id,depends_json,summary,registered_by,created_at) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (pid, package_id, jurisdiction, version, eff_from, eff_to,
                         _json(content), chash, signature, signer_id, _json(depends),
                         str(content.get("summary") or ""), registered_by, self._now()))
                    row = conn.execute("SELECT * FROM rule_packages WHERE id=?",
                                       (pid,)).fetchone()
                    rule_audit.append_event(
                        conn, "RULE_PACKAGE_REGISTERED",
                        f"登记规则包 {package_id}@{version}（{jurisdiction}，"
                        f"生效 {eff_from}..{eff_to or '∞'}）",
                        payload={"package_row_id": pid, "package_id": package_id,
                                 "jurisdiction": jurisdiction, "version": version,
                                 "content_hash": chash, "signer_id": signer_id,
                                 "effective_from": eff_from, "effective_to": eff_to,
                                 "depends_on": depends},
                        actor=registered_by, jurisdiction=jurisdiction, when=self._now(),
                        idempotency_key=f"register:{chash}")
                    return self._package_view(row, deduped=False)
            finally:
                conn.close()

    def _package_view(self, row: sqlite3.Row, *, deduped: bool = False) -> dict:
        return {
            "id": row["id"],
            "package_id": row["package_id"],
            "jurisdiction": row["jurisdiction"],
            "version": row["version"],
            "effective_from": row["effective_from"],
            "effective_to": row["effective_to"],
            "content": json.loads(row["content_json"]),
            "content_hash": row["content_hash"],
            "signature": row["signature"],
            "signer_id": row["signer_id"],
            "depends_on": json.loads(row["depends_json"]),
            "summary": row["summary"],
            "registered_by": row["registered_by"],
            "created_at": row["created_at"],
            "deduped": deduped,
        }

    def get_package(self, *, package_row_id: str | None = None,
                    package_id: str | None = None, version: str | None = None) -> dict:
        conn = self._conn()
        try:
            row = self._load_package_row(conn, package_row_id=package_row_id,
                                         package_id=package_id, version=version)
            return self._package_view(row)
        finally:
            conn.close()

    @staticmethod
    def _load_package_row(conn, *, package_row_id: str | None = None,
                          package_id: str | None = None, version: str | None = None,
                          allow_legacy: bool = False) -> sqlite3.Row | None:
        if package_row_id:
            row = conn.execute("SELECT * FROM rule_packages WHERE id=?",
                               (package_row_id,)).fetchone()
            if not row:
                raise RuleGovernanceError("PACKAGE_NOT_FOUND", "规则包不存在", 404)
            return row
        if package_id:
            row = conn.execute(
                "SELECT * FROM rule_packages WHERE package_id=? AND version=? "
                "ORDER BY rowid DESC LIMIT 1", (package_id, str(version))).fetchone()
            if not row and not (allow_legacy
                                and package_id == LEGACY_PACKAGE_ID
                                and str(version) == LEGACY_VERSION):
                raise RuleGovernanceError(
                    "PACKAGE_NOT_FOUND",
                    f"规则包不存在: {package_id}@{version}", 404)
            return row
        raise RuleGovernanceError("BAD_PACKAGE_REF",
                                  "需提供 package_row_id 或 package_id+version")

    def list_packages(self, jurisdiction: str | None = None) -> list[dict]:
        conn = self._conn()
        try:
            if jurisdiction:
                rows = conn.execute(
                    "SELECT * FROM rule_packages WHERE jurisdiction=? "
                    "ORDER BY effective_from, rowid", (jurisdiction,)).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM rule_packages ORDER BY jurisdiction, effective_from"
                ).fetchall()
            return [self._package_view(r) for r in rows]
        finally:
            conn.close()

    # -- 引擎解析（供计算路径固定版本）--------------------------------------
    def resolve(self, jurisdiction: str) -> tuple[str, str, object]:
        """返回辖区当前生效版本 (package_id, version, engine)。

        无启用记录的辖区 -> 内置历史口径 builtin:legacy@0。
        """
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT s.package_id AS package_id, s.version AS version, p.content_json "
                "AS content_json FROM rule_jurisdiction_state s "
                "LEFT JOIN rule_packages p ON p.id=s.current_pkg_id "
                "WHERE s.jurisdiction=?", (jurisdiction,)).fetchone()
            if not row:
                return LEGACY_PACKAGE_ID, LEGACY_VERSION, build_engine(None)
            return row["package_id"], row["version"], \
                build_engine(json.loads(row["content_json"]) if row["content_json"] else None)
        finally:
            conn.close()

    def current_version(self, jurisdiction: str) -> dict:
        pkg_id, ver, _ = self.resolve(jurisdiction)
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT * FROM rule_jurisdiction_state WHERE jurisdiction=?",
                (jurisdiction,)).fetchone()
            if not row:
                return {"jurisdiction": jurisdiction, "package_id": pkg_id,
                        "version": ver, "builtin": True, "current_seq": 0}
            return {"jurisdiction": jurisdiction, "package_id": row["package_id"],
                    "version": row["version"], "builtin": False,
                    "current_seq": row["current_seq"], "activated_at": row["activated_at"],
                    "activated_by": row["activated_by"]}
        finally:
            conn.close()

    def list_current_versions(self) -> list[dict]:
        conn = self._conn()
        try:
            jurs = {r["jurisdiction"] for r in conn.execute(
                "SELECT DISTINCT jurisdiction FROM rule_packages")}
            jurs |= {r["jurisdiction"] for r in conn.execute(
                "SELECT jurisdiction FROM rule_jurisdiction_state")}
            jurs.add(self.cases.jurisdiction)
            return [self.current_version(j) for j in sorted(jurs)]
        finally:
            conn.close()

    # -- 版本化决定计算（试算 / 作业共用；纯读病例快照）----------------------
    def _engine_for(self, conn, package_row_id: str | None) -> object:
        if self.engine_override is not None:
            return self.engine_override
        if package_row_id is None:
            return build_engine(None)
        row = conn.execute("SELECT content_json FROM rule_packages WHERE id=?",
                           (package_row_id,)).fetchone()
        if not row:
            raise RuleGovernanceError("PACKAGE_NOT_FOUND", "规则包不存在", 404)
        return build_engine(json.loads(row["content_json"]))

    def _decide(self, conn, snapshot: dict, awareness_iso: str,
                engine, *, is_followup: bool = False,
                prior_snapshot: dict | None = None) -> dict:
        from .normalize import parse_date
        aware = parse_date(awareness_iso) or self.clock.now().date()
        ctx = RuleContext(
            case_id=snapshot.get("case_id") or "trial",
            snapshot=snapshot, awareness_date=aware,
            is_followup=is_followup, prior_snapshot=prior_snapshot,
            is_listed_fn=self.cases.is_listed)
        return engine.evaluate(ctx).as_dict()

    # ======================================================================
    # 试算任务（只读，不改病例/会签/发送）
    # ======================================================================
    def create_trial(self, candidate_package_id: str, candidate_version: str, *,
                     selector: dict | None = None,
                     baseline_package_id: str | None = None,
                     baseline_version: str | None = None,
                     idempotency_key: str | None = None,
                     requested_by: str | None = None,
                     shard_size: int | None = None) -> dict:
        selector = selector or {}
        with self._lock:
            conn = self._conn()
            try:
                with dbmod.transaction(conn):
                    cand = self._load_package_row(
                        conn, package_id=candidate_package_id,
                        version=candidate_version)
                    jurisdiction = cand["jurisdiction"]
                    base_row = None
                    if baseline_package_id:
                        base_row = self._load_package_row(
                            conn, package_id=baseline_package_id,
                            version=str(baseline_version or LEGACY_VERSION),
                            allow_legacy=True)
                    base_row_id = base_row["id"] if base_row else None

                    # 幂等：同辖区+候选包+幂等键只产生一个试算任务
                    idem = idempotency_key or f"auto:{cand['id']}:{content_hash(selector)}"
                    existing = conn.execute(
                        "SELECT * FROM rule_trials WHERE jurisdiction=? "
                        "AND candidate_pkg_id=? AND idempotency_key=?",
                        (jurisdiction, cand["id"], idem)).fetchone()
                    if existing:
                        return self._trial_view(existing, conn, deduped=True)

                    scope = self._select_scope(conn, jurisdiction, selector)
                    tid = _uid()
                    conn.execute(
                        "INSERT INTO rule_trials(id,jurisdiction,candidate_pkg_id,"
                        "baseline_pkg_id,selector_json,scope_json,status,shard_size,"
                        "total_units,idempotency_key,requested_by,created_at) "
                        "VALUES(?,?,?,?,?,?,'PENDING',?,?,?,?,?)",
                        (tid, jurisdiction, cand["id"], base_row_id, _json(selector),
                         _json([u["case_version_id"] for u in scope]),
                         shard_size or self.shard_size, len(scope), idem,
                         requested_by, self._now()))
                    # 冻结单元快照（试算期间病例再变化也不影响试算输入）
                    for pos, u in enumerate(scope):
                        conn.execute(
                            "INSERT INTO rule_trial_units(id,trial_id,shard_no,position,"
                            "case_id,case_version_id,snapshot_json,awareness_date) "
                            "VALUES(?,?,?,?,?,?,?,?)",
                            (_uid(), tid, pos // (shard_size or self.shard_size), pos,
                             u["case_id"], u["case_version_id"], _json(u["snapshot"]),
                             u["awareness_date"]))
                    rule_audit.append_event(
                        conn, "RULE_TRIAL_CREATED",
                        f"创建试算任务：{candidate_package_id}@{candidate_version}，"
                        f"{len(scope)} 个历史快照",
                        payload={"trial_id": tid, "jurisdiction": jurisdiction,
                                 "candidate": f"{candidate_package_id}@{candidate_version}",
                                 "baseline": (f"{baseline_package_id}@{baseline_version}"
                                              if baseline_package_id else LEGACY_PACKAGE_ID),
                                 "selector": selector, "total_units": len(scope)},
                        actor=requested_by, jurisdiction=jurisdiction, when=self._now(),
                        idempotency_key=f"trial-create:{tid}")
                    row = conn.execute("SELECT * FROM rule_trials WHERE id=?",
                                       (tid,)).fetchone()
                    return self._trial_view(row, conn, deduped=False)
            finally:
                conn.close()

    def _select_scope(self, conn, jurisdiction: str, selector: dict) -> list[dict]:
        """根据选择条件冻结一组历史病例版本快照（创建时锁定输入）。

        selector:
          case_ids:    显式病例 id 列表
          version_ids: 显式版本 id 列表
          as_of:       仅取 created_at <= as_of 的最新版本
          limit:       最多取多少条（确定性按病例创建时间）
        未给条件：取辖区全部 OPEN 病例的最新版本。
        """
        case_ids = selector.get("case_ids")
        version_ids = selector.get("version_ids")
        as_of = selector.get("as_of")
        limit = selector.get("limit")
        units: list[dict] = []
        if version_ids:
            rows = conn.execute(
                f"SELECT * FROM case_versions WHERE id IN "
                f"({','.join('?' * len(version_ids))}) ORDER BY case_id,version_no",
                [str(x) for x in version_ids]).fetchall()
        elif case_ids:
            placeholders = ",".join("?" * len(case_ids))
            sql = (f"SELECT v.* FROM case_versions v JOIN cases c ON c.id=v.case_id "
                   f"WHERE v.case_id IN ({placeholders}) AND v.version_no=("
                   f"SELECT MAX(version_no) FROM case_versions WHERE case_id=v.case_id)")
            params = [str(x) for x in case_ids]
            if as_of:
                sql = ("SELECT v.* FROM case_versions v WHERE v.case_id IN "
                       f"({placeholders}) AND v.created_at<=? "
                       "AND v.version_no=(SELECT MAX(version_no) FROM case_versions "
                       "WHERE case_id=v.case_id AND created_at<=?)")
                params = [str(x) for x in case_ids] + [as_of, as_of]
            sql += " ORDER BY v.case_id"
            rows = conn.execute(sql, params).fetchall()
        else:
            sql = ("SELECT v.* FROM case_versions v JOIN cases c ON c.id=v.case_id "
                   "WHERE c.status='OPEN' AND v.version_no=("
                   "SELECT MAX(version_no) FROM case_versions WHERE case_id=v.case_id)")
            params: list = []
            if as_of:
                sql = ("SELECT v.* FROM case_versions v JOIN cases c ON c.id=v.case_id "
                       "WHERE c.status='OPEN' AND v.created_at<=? AND v.version_no=("
                       "SELECT MAX(version_no) FROM case_versions WHERE case_id=v.case_id "
                       "AND created_at<=?)")
                params = [as_of, as_of]
            sql += " ORDER BY c.created_at, v.case_id"
            rows = conn.execute(sql, params).fetchall()
        for r in rows:
            units.append({
                "case_id": r["case_id"], "case_version_id": r["id"],
                "snapshot": json.loads(r["snapshot_json"]),
                "awareness_date": r["created_at"][:10],
            })
        if isinstance(limit, int) and limit > 0:
            units = units[:limit]
        return units

    def run_trial(self, trial_id: str, *, max_shards: int | None = None) -> dict:
        """推进试算（从检查点 next_shard 续跑）。重复调用不重复生成差异。"""
        with self._lock:
            conn = self._conn()
            try:
                trial = conn.execute("SELECT * FROM rule_trials WHERE id=?",
                                     (trial_id,)).fetchone()
                if not trial:
                    raise RuleGovernanceError("TRIAL_NOT_FOUND", "试算任务不存在", 404)
                if trial["status"] == "COMPLETED":
                    return self._trial_view(trial, conn)
                if trial["status"] == "FAILED":
                    raise RuleGovernanceError("TRIAL_FAILED",
                                              f"试算已失败：{trial['error_code']}", 409)
                with dbmod.transaction(conn):
                    conn.execute("UPDATE rule_trials SET status='RUNNING', started_at=? "
                                 "WHERE id=? AND status IN ('PENDING','RUNNING')",
                                 (self._now(), trial_id))
                base_engine = self._engine_for(conn, trial["baseline_pkg_id"])
                cand_engine = self._engine_for(conn, trial["candidate_pkg_id"])
                shard_size = trial["shard_size"]
                nshards = (trial["total_units"] + shard_size - 1) // shard_size
                start = trial["next_shard"]
                end = nshards if max_shards is None else min(nshards, start + max_shards)
                for shard in range(start, end):
                    self._run_trial_shard(conn, trial_id, shard, shard_size,
                                          base_engine, cand_engine)
                with dbmod.transaction(conn):
                    t = conn.execute("SELECT * FROM rule_trials WHERE id=?",
                                     (trial_id,)).fetchone()
                    if t["next_shard"] >= nshards:
                        conn.execute(
                            "UPDATE rule_trials SET status='COMPLETED',finished_at=? "
                            "WHERE id=?", (self._now(), trial_id))
                        rule_audit.append_event(
                            conn, "RULE_TRIAL_COMPLETED",
                            f"试算任务完成：{t['changed_units']}/{t['total_units']} "
                            "个快照决定有差异",
                            payload={"trial_id": trial_id,
                                     "changed_units": t["changed_units"],
                                     "total_units": t["total_units"]},
                            jurisdiction=t["jurisdiction"], when=self._now(),
                            idempotency_key=f"trial-done:{trial_id}")
                row = conn.execute("SELECT * FROM rule_trials WHERE id=?",
                                   (trial_id,)).fetchone()
                return self._trial_view(row, conn)
            finally:
                conn.close()

    def _run_trial_shard(self, conn, trial_id, shard, shard_size,
                         base_engine, cand_engine) -> None:
        """处理一个分片：逐单元短事务提交（检查点），仅处理尚无结果的单元。

        进程可在任意单元后中断：已提交单元的差异/计数已落库，恢复续跑跳过
        它们，只补未完成单元，不重复生成差异或计数。
        """
        units = conn.execute(
            "SELECT * FROM rule_trial_units WHERE trial_id=? AND shard_no=? "
            "ORDER BY position", (trial_id, shard)).fetchall()
        pending = [u for u in units if u["baseline_json"] is None]
        if not pending:
            return
        for u in pending:
            if self.crash_hook:
                self.crash_hook("TRIAL_UNIT", trial_id, shard, u["position"])
            snapshot = json.loads(u["snapshot_json"])
            old = self._decide(conn, snapshot, u["awareness_date"], base_engine)
            new = self._decide(conn, snapshot, u["awareness_date"], cand_engine)
            diff = _decision_diff(old, new)
            with dbmod.transaction(conn):
                conn.execute(
                    "UPDATE rule_trial_units SET baseline_json=?,candidate_json=?,"
                    "changed=?,diff_json=? WHERE id=? AND baseline_json IS NULL",
                    (_json(old), _json(new), 1 if diff["changed"] else 0,
                     _json(diff), u["id"]))
                if diff["changed"]:
                    conn.execute(
                        "UPDATE rule_trials SET changed_units=changed_units+1 WHERE id=?",
                        (trial_id,))
                conn.execute(
                    "UPDATE rule_trials SET done_units=done_units+1 WHERE id=?",
                    (trial_id,))
        # 分片整批完成后推进检查点
        with dbmod.transaction(conn):
            conn.execute(
                "UPDATE rule_trials SET next_shard=? WHERE id=? AND next_shard=?",
                (shard + 1, trial_id, shard))

    def run_trial_until_done(self, trial_id: str) -> dict:
        view = self.run_trial(trial_id)
        while view["status"] in ("PENDING", "RUNNING"):
            view = self.run_trial(trial_id)
        return view

    def _trial_view(self, row: sqlite3.Row, conn=None, *, deduped: bool = False) -> dict:
        return {
            "trial_id": row["id"], "jurisdiction": row["jurisdiction"],
            "candidate_pkg_id": row["candidate_pkg_id"],
            "baseline_pkg_id": row["baseline_pkg_id"],
            "selector": json.loads(row["selector_json"]),
            "scope_size": len(json.loads(row["scope_json"])),
            "status": row["status"], "total_units": row["total_units"],
            "done_units": row["done_units"], "changed_units": row["changed_units"],
            "next_shard": row["next_shard"],
            "error_code": row["error_code"], "error_message": row["error_message"],
            "idempotency_key": row["idempotency_key"],
            "requested_by": row["requested_by"], "created_at": row["created_at"],
            "finished_at": row["finished_at"], "deduped": deduped,
        }

    def get_trial(self, trial_id: str) -> dict:
        conn = self._conn()
        try:
            row = conn.execute("SELECT * FROM rule_trials WHERE id=?",
                               (trial_id,)).fetchone()
            if not row:
                raise RuleGovernanceError("TRIAL_NOT_FOUND", "试算任务不存在", 404)
            return self._trial_view(row)
        finally:
            conn.close()

    def trial_progress(self, trial_id: str) -> dict:
        conn = self._conn()
        try:
            row = conn.execute("SELECT * FROM rule_trials WHERE id=?",
                               (trial_id,)).fetchone()
            if not row:
                raise RuleGovernanceError("TRIAL_NOT_FOUND", "试算任务不存在", 404)
            view = self._trial_view(row)
            view["affected_case_ids"] = [r["case_id"] for r in conn.execute(
                "SELECT DISTINCT case_id FROM rule_trial_units WHERE trial_id=? "
                "AND changed=1 ORDER BY case_id", (trial_id,))]
            view["affected_report_count"] = conn.execute(
                "SELECT COUNT(*) AS n FROM rule_trial_units WHERE trial_id=? AND changed=1",
                (trial_id,)).fetchone()["n"]
            return view
        finally:
            conn.close()

    def trial_diffs(self, trial_id: str, *, changed_only: bool = True,
                    limit: int = 500) -> list[dict]:
        conn = self._conn()
        try:
            sql = ("SELECT * FROM rule_trial_units WHERE trial_id=? "
                   "AND baseline_json IS NOT NULL")
            if changed_only:
                sql += " AND changed=1"
            sql += " ORDER BY shard_no,position LIMIT ?"
            out = []
            for r in conn.execute(sql, (trial_id, limit)).fetchall():
                out.append({
                    "case_id": r["case_id"], "case_version_id": r["case_version_id"],
                    "shard_no": r["shard_no"], "changed": bool(r["changed"]),
                    "baseline": json.loads(r["baseline_json"]),
                    "candidate": json.loads(r["candidate_json"]),
                    "diff": json.loads(r["diff_json"]),
                })
            return out
        finally:
            conn.close()

    def list_trials(self, jurisdiction: str | None = None) -> list[dict]:
        conn = self._conn()
        try:
            if jurisdiction:
                rows = conn.execute(
                    "SELECT * FROM rule_trials WHERE jurisdiction=? ORDER BY created_at",
                    (jurisdiction,)).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM rule_trials ORDER BY created_at").fetchall()
            return [self._trial_view(r) for r in rows]
        finally:
            conn.close()

    # ======================================================================
    # 受控启用 / 回滚（两阶段作业；失败不留部分启用）
    # ======================================================================
    def _already_activated(self, conn, idem: str):
        return conn.execute(
            "SELECT * FROM rule_activations WHERE idempotency_key=?",
            (idem,)).fetchone()

    def activate(self, package_id: str, version: str, *,
                 actor: str | None = None,
                 expected_package_id: str | None = None,
                 expected_version: str | None = None,
                 idempotency_key: str | None = None,
                 wait: bool = True) -> dict:
        """受控启用某辖区的某规则版本（绑定调用方看到的当前版本）。

        expected_* 为乐观并发条件：提供时若辖区当前版本不匹配则 409
        （调用方基于过期视图启用被拒绝）。并发启用/回滚/定时只有一个结果。
        """
        return self._activate(
            package_id, version, kind="ACTIVATE", actor=actor,
            expected_package_id=expected_package_id, expected_version=expected_version,
            idempotency_key=idempotency_key, wait=wait)

    def rollback(self, *, target_package_id: str | None = None,
                 target_version: str | None = None,
                 actor: str | None = None,
                 expected_package_id: str | None = None,
                 expected_version: str | None = None,
                 idempotency_key: str | None = None,
                 wait: bool = True) -> dict:
        """回滚到该辖区此前登记过的版本（缺省=上一启用版本）。"""
        with self._lock:
            conn = self._conn()
            try:
                jurisdiction = self.cases.jurisdiction
                cur = conn.execute(
                    "SELECT * FROM rule_jurisdiction_state WHERE jurisdiction=?",
                    (jurisdiction,)).fetchone()
                if target_package_id is None or target_version is None:
                    if not cur:
                        raise RuleGovernanceError(
                            "NO_HISTORY", "该辖区从未启用任何规则包，无可回滚目标", 409)
                    prev = conn.execute(
                        "SELECT package_id,version FROM rule_activations "
                        "WHERE jurisdiction=? AND status='COMPLETED' "
                        "AND NOT (package_id=? AND version=?) "
                        "ORDER BY created_at DESC, rowid DESC LIMIT 1",
                        (jurisdiction, cur["package_id"], cur["version"])).fetchone()
                    if not prev:
                        target_package_id, target_version = LEGACY_PACKAGE_ID, LEGACY_VERSION
                    else:
                        target_package_id, target_version = prev["package_id"], prev["version"]
            finally:
                conn.close()
        return self._activate(
            target_package_id, str(target_version), kind="ROLLBACK", actor=actor,
            expected_package_id=expected_package_id, expected_version=expected_version,
            idempotency_key=idempotency_key, wait=wait)

    def _activate(self, package_id: str, version: str, *, kind: str,
                  actor, expected_package_id, expected_version,
                  idempotency_key, wait: bool) -> dict:
        # 只有调用方*显式*携带同一 idempotency_key 才算请求重放（返回原结论）；
        # 未携带键的每次启用/回滚/定时都是独立操作（回滚后可再启用同版本），
        # 由作业/辖区状态行的串行与各种唯一约束保证“只有一个生效结果”。
        idem = idempotency_key or f"{kind.lower()}:{self.cases.jurisdiction}:" \
                                 f"{package_id}@{version}:{_uid()}"
        with self._lock:
            conn = self._conn()
            try:
                # 幂等：同一请求重放返回原结论（不重复作业/审计）
                prior = self._already_activated(conn, idem)
                if prior:
                    return self._activation_view(prior, conn, deduped=True)
                with dbmod.transaction(conn):
                    target = self._load_package_row(
                        conn, package_id=package_id, version=str(version),
                        allow_legacy=True)
                    # 内置历史口径可回滚到任意辖区；真实包辖区以包内容为准
                    jurisdiction = (self.cases.jurisdiction
                                    if package_id == LEGACY_PACKAGE_ID
                                    else target["jurisdiction"])

                    # 绑定调用方看到的当前版本（乐观并发）
                    cur_pkg, cur_ver, _ = self.resolve(jurisdiction)
                    if expected_package_id is not None and \
                            expected_package_id != cur_pkg:
                        raise RuleGovernanceError(
                            "VERSION_MOVED",
                            f"当前版本已为 {cur_pkg}@{cur_ver}，与调用方视图 "
                            f"{expected_package_id}@{expected_version} 不一致", 409)
                    if expected_version is not None and str(expected_version) != cur_ver:
                        raise RuleGovernanceError("VERSION_MOVED",
                                                  "辖区当前版本与调用方视图不一致", 409)

                    # 同辖区已有在途作业（启用/回滚）-> 只有一个结果
                    inflight = conn.execute(
                        "SELECT id FROM rule_jobs WHERE jurisdiction=? AND status IN "
                        "('PENDING','RUNNING') ORDER BY rowid LIMIT 1",
                        (jurisdiction,)).fetchone()
                    if inflight:
                        raise RuleGovernanceError(
                            "ACTIVATION_IN_PROGRESS",
                            f"辖区 {jurisdiction} 已有在途启用/回滚作业 {inflight['id'][:8]}，"
                            "请待其结束或查询恢复", 409)

                    if package_id == cur_pkg and str(version) == cur_ver:
                        # 目标即当前：记录一条 SKIPPED（幂等、可审计），不跑作业
                        act_id = _uid()
                        conn.execute(
                            "INSERT INTO rule_activations(id,jurisdiction,kind,package_id,"
                            "version,rule_pkg_id,from_package_id,from_version,status,"
                            "idempotency_key,actor,detail_json,created_at) "
                            "VALUES(?,?,?,?,?,?,?,?,'SKIPPED_ALREADY_CURRENT',?,?,?,?)",
                            (act_id, jurisdiction, kind, package_id, str(version),
                             target["id"], cur_pkg, cur_ver,
                             idem, actor, _json({"reason": "target already current"}),
                             self._now()))
                        rule_audit.append_event(
                            conn, "RULE_ACTIVATION_SKIPPED",
                            f"{kind} {package_id}@{version}：目标即当前版本，跳过",
                            payload={"jurisdiction": jurisdiction, "package_id": package_id,
                                     "version": str(version)},
                            actor=actor, jurisdiction=jurisdiction, when=self._now(),
                            idempotency_key=f"act-skip:{idem}")
                        row = conn.execute("SELECT * FROM rule_activations WHERE id=?",
                                           (act_id,)).fetchone()
                        return self._activation_view(row, conn)

                    # 即时启用/回滚使尚未触发的定时计划失效；但定时计划*自身*
                    # 到点触发（kind=SCHEDULED）时不能作废它自己（要置为 FIRED）。
                    if kind in ("ACTIVATE", "ROLLBACK"):
                        conn.execute(
                            "UPDATE rule_schedules SET status='SUPERSEDED' "
                            "WHERE jurisdiction=? AND status='PENDING'",
                            (jurisdiction,))

                    job_id = _uid()
                    units = self._collect_job_units(conn, jurisdiction, target,
                                                    package_id, str(version))
                    n_unsent = sum(1 for u in units if u["action"] != "CORRECTION_SENT")
                    n_sent = sum(1 for u in units if u["action"] == "CORRECTION_SENT")
                    conn.execute(
                        "INSERT INTO rule_jobs(id,jurisdiction,kind,target_pkg_id,package_id,"
                        "version,idem_key,status,total_units,shard_size,unsent_count,"
                        "sent_count,actor,created_at,started_at) "
                        "VALUES(?,?,?,?,?,?,?,'PENDING',?,?,?,?,?,?,?)",
                        (job_id, jurisdiction, kind, target["id"],
                         package_id, str(version), idem, len(units), self.shard_size,
                         n_unsent, n_sent, actor, self._now(), self._now()))
                    for pos, u in enumerate(units):
                        conn.execute(
                            "INSERT INTO rule_job_units(id,job_id,shard_no,position,report_id,"
                            "case_id,case_version_id,report_status,old_package_id,old_version,"
                            "new_package_id,new_version,action,changed) "
                            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            (_uid(), job_id, pos // self.shard_size, pos, u["report_id"],
                             u["case_id"], u["case_version_id"], u["report_status"],
                             u["old_package_id"], u["old_version"], package_id,
                             str(version), u["action"], 0))
                # 阶段一逐分片暂存；全部分片成功后阶段二在单事务内原子应用，
                # 并在该事务内写入 COMPLETED 启用历史（失败则无历史、指针不动）。
                job_view = self.run_job(job_id, wait=wait)
                conn = self._conn()
                try:
                    row = conn.execute(
                        "SELECT * FROM rule_activations WHERE idempotency_key=?",
                        (idem,)).fetchone()
                    view = self._activation_view(row, conn)
                finally:
                    conn.close()
                view["job"] = job_view
                return view
            finally:
                conn.close()

    def _collect_job_units(self, conn, jurisdiction: str, target_row,
                           new_package_id: str, new_version: str) -> list[dict]:
        """收集辖区内全部受影响报告（未发送重算、已发送更正候选）。

        VOID 报告已终结，不参与。
        """
        rows = conn.execute(
            "SELECT * FROM regulatory_reports WHERE jurisdiction=? AND status<>'VOID' "
            "ORDER BY created_at, rowid", (jurisdiction,)).fetchall()
        units = []
        for r in rows:
            if r["status"] == "ACKED":
                action = "CORRECTION_SENT"
            else:
                # QUEUED / SENDING / FAILED：未发送
                action = "RECALC_UNSENT"
            units.append({
                "report_id": r["id"], "case_id": r["case_id"],
                "case_version_id": r["case_version_id"], "report_status": r["status"],
                "old_package_id": r["rule_package_id"], "old_version": r["rule_version"],
                "action": action,
            })
        return units

    def run_job(self, job_id: str, *, wait: bool = True,
                max_shards: int | None = None) -> dict:
        """执行启用作业。阶段一分片暂存；全部分片成功后阶段二原子应用。

        阶段一中途异常（含进程中断模拟）：标记作业失败/恢复后从检查点续跑，
        阶段二尚未发生，业务状态与辖区版本指针保持原样（无部分启用）。
        """
        with self._lock:
            job = self._job_row(job_id)
            if job["status"] == "COMPLETED":
                return self._job_view(job)
            if job["status"] == "FAILED":
                raise RuleGovernanceError("JOB_FAILED",
                                          f"作业已失败：{job['error_code']}", 409)
            try:
                self._stage_job(job_id, max_shards=max_shards)
                job = self._job_row(job_id)
                if job["staged_units"] >= job["total_units"] and not job["applied"]:
                    self._apply_job(job_id)
            except RuleGovernanceError:
                raise
            except ProcessInterrupted:
                # 进程中断：作业留在 RUNNING 检查点（recover 会置 PENDING 续跑），
                # 不标记业务失败、不应用。
                raise
            except Exception as e:  # noqa: BLE001 阶段一任何意外都不允许部分启用
                self._fail_job(job_id, "SHARD_FAILED", f"{type(e).__name__}: {e}")
                raise RuleGovernanceError(
                    "SHARD_FAILED",
                    f"启用作业分片失败，未应用（无部分启用，可恢复续跑）: {e}", 500)
            return self._job_view(self._job_row(job_id))

    def _job_row(self, job_id: str) -> sqlite3.Row:
        conn = self._conn()
        try:
            job = conn.execute("SELECT * FROM rule_jobs WHERE id=?",
                               (job_id,)).fetchone()
            if not job:
                raise RuleGovernanceError("JOB_NOT_FOUND", "启用作业不存在", 404)
            return job
        finally:
            conn.close()

    def _stage_job(self, job_id: str, *, max_shards: int | None = None) -> None:
        """阶段一：逐分片把新决定暂存到 rule_job_units（不改任何业务行）。

        崩溃/异常向上传播（由 run_job 统一记失败）；已提交单元成为检查点。
        """
        conn = self._conn()
        try:
            job = conn.execute("SELECT * FROM rule_jobs WHERE id=?",
                               (job_id,)).fetchone()
            exists = conn.execute("SELECT 1 FROM rule_packages WHERE id=?",
                                  (job["target_pkg_id"],)).fetchone()
            engine = self._engine_for(conn, job["target_pkg_id"] if exists else None)
            nshards = (job["total_units"] + job["shard_size"] - 1) // job["shard_size"]
            start = job["next_shard"]
            end = nshards if max_shards is None else min(nshards, start + max_shards)
            for shard in range(start, end):
                self._stage_shard(conn, job, shard, engine)
        finally:
            conn.close()

    def _stage_shard(self, conn, job, shard, engine) -> None:
        """阶段一单个分片：逐单元暂存（短事务检查点）。

        崩溃可在任意单元后发生：已暂存单元保留，恢复续跑只补未暂存单元。
        阶段二应用是独立的单事务，故阶段一中途崩溃绝不改变业务状态。
        """
        units = conn.execute(
            "SELECT * FROM rule_job_units WHERE job_id=? AND shard_no=? ORDER BY position",
            (job["id"], shard)).fetchall()
        pending = [u for u in units if u["new_decision_json"] is None]
        if not pending:
            with dbmod.transaction(conn):
                conn.execute(
                    "UPDATE rule_jobs SET next_shard=? WHERE id=? AND next_shard=?",
                    (shard + 1, job["id"], shard))
            return
        for u in pending:
            if self.crash_hook:
                self.crash_hook("JOB_UNIT", job["id"], shard, u["position"])
            vrow = conn.execute(
                "SELECT snapshot_json,created_at FROM case_versions WHERE id=?",
                (u["case_version_id"],)).fetchone()
            snapshot = json.loads(vrow["snapshot_json"])
            old_report = conn.execute(
                "SELECT * FROM regulatory_reports WHERE id=?",
                (u["report_id"],)).fetchone()
            old_decision = json.loads(old_report["decision_json"])
            is_followup = old_report["report_type"] == "FOLLOWUP"
            prior = None
            if is_followup:
                pv = conn.execute(
                    "SELECT snapshot_json FROM case_versions WHERE case_id=? "
                    "AND id<>? ORDER BY version_no DESC LIMIT 1",
                    (u["case_id"], u["case_version_id"])).fetchone()
                prior = json.loads(pv["snapshot_json"]) if pv else None
            new_decision = self._decide(conn, snapshot, vrow["created_at"][:10], engine,
                                        is_followup=is_followup, prior_snapshot=prior)
            diff = _decision_diff(old_decision, new_decision)
            action = u["action"]
            if not new_decision["should_report"] and action == "RECALC_UNSENT":
                action = "VOID_UNSENT"
            with dbmod.transaction(conn):
                conn.execute(
                    "UPDATE rule_job_units SET new_decision_json=?,changed=?,action=? "
                    "WHERE id=? AND new_decision_json IS NULL",
                    (_json(new_decision), 1 if diff["changed"] else 0, action, u["id"]))
                conn.execute(
                    "UPDATE rule_jobs SET staged_units=staged_units+1 WHERE id=?",
                    (job["id"],))
        with dbmod.transaction(conn):
            conn.execute(
                "UPDATE rule_jobs SET status='RUNNING',next_shard=? "
                "WHERE id=? AND next_shard=?", (shard + 1, job["id"], shard))

    def _fail_job(self, job_id, code, message) -> None:
        conn = self._conn()
        try:
            with dbmod.transaction(conn):
                conn.execute(
                    "UPDATE rule_jobs SET status='FAILED',finished_at=?,error_code=?,"
                    "error_message=? WHERE id=? AND status IN ('PENDING','RUNNING')",
                    (self._now(), code, message, job_id))
                job = conn.execute("SELECT jurisdiction FROM rule_jobs WHERE id=?",
                                   (job_id,)).fetchone()
                rule_audit.append_event(
                    conn, "RULE_JOB_FAILED",
                    f"启用作业失败 {job_id[:8]}：{code}（无部分启用，版本指针不变）",
                    payload={"job_id": job_id, "error_code": code, "message": message},
                    jurisdiction=job["jurisdiction"] if job else None, when=self._now(),
                    idempotency_key=f"job-fail:{job_id}")
        finally:
            conn.close()

    def _apply_job(self, job_id: str) -> None:
        """阶段二：单事务原子应用全部暂存结果。

        - 未发送：按新版本重算（更新决定/截止/固定版本，编号不变），
          新版本判定无需报送则 VOID；推进数据修订号作废旧会签单；
        - 已发送：报告行不动，只插入更正候选；
        - 切换辖区当前版本指针；
        - 审计事件带作业级幂等键，重放不重复。
        """
        conn = self._conn()
        try:
            with dbmod.transaction(conn):
                job = conn.execute("SELECT * FROM rule_jobs WHERE id=?",
                                   (job_id,)).fetchone()
                # COMPLETED 启用历史在*同一*应用事务写入：应用与结论同生共死，
                # 任何中途失败都会整笔回滚（无 COMPLETED 历史、无部分启用）。
                act_id = _uid()
                state_before = conn.execute(
                    "SELECT * FROM rule_jurisdiction_state WHERE jurisdiction=?",
                    (job["jurisdiction"],)).fetchone()
                from_pkg = state_before["package_id"] if state_before else LEGACY_PACKAGE_ID
                from_ver = state_before["version"] if state_before else LEGACY_VERSION
                conn.execute(
                    "INSERT OR IGNORE INTO rule_activations(id,jurisdiction,kind,package_id,"
                    "version,rule_pkg_id,from_package_id,from_version,job_id,status,"
                    "idempotency_key,actor,detail_json,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?, 'COMPLETED',?,?,?,?)",
                    (act_id, job["jurisdiction"], job["kind"], job["package_id"],
                     job["version"], job["target_pkg_id"], from_pkg, from_ver, job_id,
                     job["idem_key"], job["actor"],
                     _json({"job_id": job_id}), self._now()))
                act = conn.execute(
                    "SELECT * FROM rule_activations WHERE idempotency_key=?",
                    (job["idem_key"],)).fetchone()
                units = conn.execute(
                    "SELECT * FROM rule_job_units WHERE job_id=? ORDER BY position",
                    (job_id,)).fetchall()
                n_recalc = n_void = n_corr = 0
                for u in units:
                    new_decision = json.loads(u["new_decision_json"])
                    if u["action"] in ("RECALC_UNSENT", "VOID_UNSENT"):
                        if u["action"] == "VOID_UNSENT":
                            n_void += 1
                            old_payload_row = conn.execute(
                                "SELECT payload_json FROM regulatory_reports WHERE id=?",
                                (u["report_id"],)).fetchone()
                            void_payload = json.loads(old_payload_row["payload_json"])
                            void_payload["rule_version"] = {
                                "package_id": u["new_package_id"],
                                "version": u["new_version"]}
                            conn.execute(
                                "UPDATE regulatory_reports SET status='VOID',"
                                "decision_json=?,payload_json=?,"
                                "rule_package_id=?,rule_version=?,"
                                "last_error=?,updated_at=? WHERE id=?",
                                (_json(new_decision), _json(void_payload),
                                 u["new_package_id"], u["new_version"],
                                 f"规则启用 {u['new_package_id']}@{u['new_version']} "
                                 "后新版本判定无需报送，未发送报告作废（编号保留）",
                                 self._now(), u["report_id"]))
                            audit.append_event(
                                conn, u["case_id"], "REPORT_VOIDED_BY_RULE_CHANGE",
                                f"规则启用后新版本判定无需报送，报告作废旧（编号保留）",
                                {"report_id": u["report_id"],
                                 "old_rule": {"package_id": u["old_package_id"],
                                              "version": u["old_version"]},
                                 "new_rule": {"package_id": u["new_package_id"],
                                              "version": u["new_version"]}},
                                event_time=self._now(), report_id=u["report_id"])
                        else:
                            n_recalc += 1
                            payload = json.loads(conn.execute(
                                "SELECT payload_json FROM regulatory_reports WHERE id=?",
                                (u["report_id"],)).fetchone()["payload_json"])
                            payload["rule_version"] = {"package_id": u["new_package_id"],
                                                       "version": u["new_version"]}
                            conn.execute(
                                "UPDATE regulatory_reports SET expedited=?,days_due=?,"
                                "due_date=?,decision_json=?,payload_json=?,"
                                "rule_package_id=?,rule_version=?,updated_at=? WHERE id=?",
                                (1 if new_decision["expedited"] else 0,
                                 new_decision["days_due"], new_decision["due_date"],
                                 _json(new_decision), _json(payload),
                                 u["new_package_id"], u["new_version"],
                                 self._now(), u["report_id"]))
                            audit.append_event(
                                conn, u["case_id"], "REPORT_RECALCULATED_BY_RULE",
                                f"规则启用 {u['new_package_id']}@{u['new_version']}："
                                f"未发送决定重算（{new_decision['rule_id']}，"
                                f"截止 {new_decision['due_date']}）",
                                {"report_id": u["report_id"],
                                 "old_rule": {"package_id": u["old_package_id"],
                                              "version": u["old_version"]},
                                 "new_rule": {"package_id": u["new_package_id"],
                                              "version": u["new_version"]},
                                 "decision": new_decision},
                                event_time=self._now(), report_id=u["report_id"])
                        conn.execute("UPDATE rule_job_units SET applied=1 WHERE id=?",
                                     (u["id"],))
                    elif u["action"] == "CORRECTION_SENT":
                        old_report = conn.execute(
                            "SELECT * FROM regulatory_reports WHERE id=?",
                            (u["report_id"],)).fetchone()
                        old_decision = json.loads(old_report["decision_json"])
                        diff = _decision_diff(old_decision, new_decision)
                        # 同一报告 + 同一目标版本的候选只留一条（回滚后再次启用
                        # 同版本属正常操作，不能因候选唯一键导致整笔应用回滚）。
                        already = conn.execute(
                            "SELECT 1 FROM rule_correction_candidates WHERE report_id=? "
                            "AND candidate_package_id=? AND candidate_version=?",
                            (u["report_id"], u["new_package_id"],
                             u["new_version"])).fetchone()
                        if diff["changed"] and not already:
                            n_corr += 1
                            conn.execute(
                                "INSERT INTO rule_correction_candidates(id,jurisdiction,"
                                "report_id,case_id,regulatory_number,job_id,activation_id,"
                                "original_package_id,original_version,candidate_package_id,"
                                "candidate_version,original_decision_json,"
                                "candidate_decision_json,diff_json,created_at) "
                                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                (_uid(), job["jurisdiction"], u["report_id"], u["case_id"],
                                 old_report["regulatory_number"], job_id,
                                 act["id"] if act else None, u["old_package_id"],
                                 u["old_version"], u["new_package_id"], u["new_version"],
                                 _json(old_decision), _json(new_decision), _json(diff),
                                 self._now()))
                            audit.append_event(
                                conn, u["case_id"], "REPORT_CORRECTION_CANDIDATE",
                                f"已发送报告 {old_report['regulatory_number']} 生成"
                                f"更正候选（原 {u['old_package_id']}@{u['old_version']}"
                                f" -> 新 {u['new_package_id']}@{u['new_version']}，原记录保留）",
                                {"report_id": u["report_id"],
                                 "regulatory_number": old_report["regulatory_number"],
                                 "old_rule": {"package_id": u["old_package_id"],
                                              "version": u["old_version"]},
                                 "new_rule": {"package_id": u["new_package_id"],
                                              "version": u["new_version"]},
                                 "diff": diff},
                                event_time=self._now(), report_id=u["report_id"])
                        conn.execute("UPDATE rule_job_units SET applied=1 WHERE id=?",
                                     (u["id"],))

                # 切换辖区当前版本指针
                if state_before:
                    conn.execute(
                        "UPDATE rule_jurisdiction_state SET current_pkg_id=?,package_id=?,"
                        "version=?,current_seq=current_seq+1,activated_at=?,activated_by=? "
                        "WHERE jurisdiction=?",
                        (job["target_pkg_id"], job["package_id"], job["version"],
                         self._now(), job["actor"], job["jurisdiction"]))
                else:
                    conn.execute(
                        "INSERT INTO rule_jurisdiction_state(jurisdiction,current_pkg_id,"
                        "package_id,version,current_seq,activated_at,activated_by) "
                        "VALUES(?,?,?,?,1,?,?)",
                        (job["jurisdiction"], job["target_pkg_id"], job["package_id"],
                         job["version"], self._now(), job["actor"]))

                # 数据修订号：未发送决定变化必须重新会签
                rev = revmod.bump_revision(
                    conn, revmod.REV_RULE_RECALC,
                    detail={"jurisdiction": job["jurisdiction"], "job_id": job_id,
                            "package_id": job["package_id"], "version": job["version"],
                            "recalculated": n_recalc, "voided": n_void,
                            "correction_candidates": n_corr,
                            "from": f"{from_pkg}@{from_ver}"},
                    actor=job["actor"], now_iso=self._now())

                conn.execute(
                    "UPDATE rule_jobs SET status='COMPLETED',applied=1,candidate_count=?,"
                    "finished_at=? WHERE id=?", (n_corr, self._now(), job_id))
                rule_audit.append_event(
                    conn, "RULE_ACTIVATED" if job["kind"] == "ACTIVATE"
                    else "RULE_ROLLED_BACK",
                    f"{job['kind']} 生效 {job['package_id']}@{job['version']}"
                    f"（{job['jurisdiction']}）：未发送重算 {n_recalc}、作废 {n_void}、"
                    f"更正候选 {n_corr}",
                    payload={"job_id": job_id, "jurisdiction": job["jurisdiction"],
                             "kind": job["kind"], "package_id": job["package_id"],
                             "version": job["version"],
                             "from": f"{from_pkg}@{from_ver}",
                             "recalculated": n_recalc, "voided": n_void,
                             "correction_candidates": n_corr, "revision": rev},
                    actor=job["actor"], jurisdiction=job["jurisdiction"], when=self._now(),
                    idempotency_key=f"job-apply:{job_id}")
        finally:
            conn.close()

    def _job_view(self, row: sqlite3.Row) -> dict:
        return {
            "job_id": row["id"], "jurisdiction": row["jurisdiction"],
            "kind": row["kind"], "package_id": row["package_id"],
            "version": row["version"], "status": row["status"],
            "total_units": row["total_units"], "staged_units": row["staged_units"],
            "applied": bool(row["applied"]), "next_shard": row["next_shard"],
            "shard_size": row["shard_size"], "unsent_count": row["unsent_count"],
            "sent_count": row["sent_count"], "candidate_count": row["candidate_count"],
            "error_code": row["error_code"], "error_message": row["error_message"],
            "created_at": row["created_at"], "finished_at": row["finished_at"],
        }

    def get_job(self, job_id: str) -> dict:
        return self._job_view(self._job_row(job_id))

    def list_jobs(self, jurisdiction: str | None = None) -> list[dict]:
        conn = self._conn()
        try:
            if jurisdiction:
                rows = conn.execute(
                    "SELECT * FROM rule_jobs WHERE jurisdiction=? ORDER BY created_at",
                    (jurisdiction,)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM rule_jobs ORDER BY created_at").fetchall()
            return [self._job_view(r) for r in rows]
        finally:
            conn.close()

    def _activation_view(self, row: sqlite3.Row, conn, *, deduped: bool = False) -> dict:
        view = {
            "activation_id": row["id"], "jurisdiction": row["jurisdiction"],
            "kind": row["kind"], "package_id": row["package_id"],
            "version": row["version"], "from_package_id": row["from_package_id"],
            "from_version": row["from_version"], "job_id": row["job_id"],
            "status": row["status"], "actor": row["actor"],
            "idempotency_key": row["idempotency_key"],
            "created_at": row["created_at"], "deduped": deduped,
        }
        if row["job_id"] and isinstance(conn, sqlite3.Connection):
            jr = conn.execute("SELECT status FROM rule_jobs WHERE id=?",
                              (row["job_id"],)).fetchone()
            view["job_status"] = jr["status"] if jr else None
        return view

    def list_activations(self, jurisdiction: str | None = None) -> list[dict]:
        conn = self._conn()
        try:
            if jurisdiction:
                rows = conn.execute(
                    "SELECT * FROM rule_activations WHERE jurisdiction=? ORDER BY rowid",
                    (jurisdiction,)).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM rule_activations ORDER BY rowid").fetchall()
            return [self._activation_view(r, conn) for r in rows]
        finally:
            conn.close()

    def correction_candidates(self, *, report_id: str | None = None,
                              jurisdiction: str | None = None,
                              limit: int = 500) -> list[dict]:
        conn = self._conn()
        try:
            where, params = [], []
            if report_id:
                where.append("report_id=?")
                params.append(report_id)
            if jurisdiction:
                where.append("jurisdiction=?")
                params.append(jurisdiction)
            sql = "SELECT * FROM rule_correction_candidates"
            if where:
                sql += " WHERE " + " AND ".join(where)
            sql += " ORDER BY rowid DESC LIMIT ?"
            params.append(limit)
            out = []
            for r in conn.execute(sql, params).fetchall():
                out.append({
                    "id": r["id"], "jurisdiction": r["jurisdiction"],
                    "report_id": r["report_id"], "case_id": r["case_id"],
                    "regulatory_number": r["regulatory_number"], "job_id": r["job_id"],
                    "original": {"package_id": r["original_package_id"],
                                 "version": r["original_version"],
                                 "decision": json.loads(r["original_decision_json"])},
                    "candidate": {"package_id": r["candidate_package_id"],
                                  "version": r["candidate_version"],
                                  "decision": json.loads(r["candidate_decision_json"])},
                    "diff": json.loads(r["diff_json"]), "created_at": r["created_at"],
                })
            return out
        finally:
            conn.close()

    # ======================================================================
    # 定时生效
    # ======================================================================
    def schedule(self, package_id: str, version: str, effective_at: str, *,
                 requested_by: str | None = None) -> dict:
        with self._lock:
            conn = self._conn()
            try:
                with dbmod.transaction(conn):
                    target = self._load_package_row(conn, package_id=package_id,
                                                    version=str(version))
                    jurisdiction = target["jurisdiction"]
                    pend = conn.execute(
                        "SELECT id FROM rule_schedules WHERE jurisdiction=? AND status='PENDING'",
                        (jurisdiction,)).fetchone()
                    if pend:
                        raise RuleGovernanceError(
                            "SCHEDULE_EXISTS",
                            f"辖区 {jurisdiction} 已有待触发的定时计划 {pend['id'][:8]}，"
                            "请先取消", 409)
                    sid = _uid()
                    conn.execute(
                        "INSERT INTO rule_schedules(id,jurisdiction,rule_pkg_id,package_id,"
                        "version,effective_at,status,requested_by,created_at) "
                        "VALUES(?,?,?,?,?,?,'PENDING',?,?)",
                        (sid, jurisdiction, target["id"], package_id, str(version),
                         effective_at, requested_by, self._now()))
                    rule_audit.append_event(
                        conn, "RULE_SCHEDULE_CREATED",
                        f"定时生效计划 {package_id}@{version} @ {effective_at}",
                        payload={"schedule_id": sid, "jurisdiction": jurisdiction,
                                 "package_id": package_id, "version": str(version),
                                 "effective_at": effective_at},
                        actor=requested_by, jurisdiction=jurisdiction, when=self._now(),
                        idempotency_key=f"sched-create:{sid}")
                    return self._schedule_view(conn.execute(
                        "SELECT * FROM rule_schedules WHERE id=?", (sid,)).fetchone())
            finally:
                conn.close()

    def cancel_schedule(self, schedule_id: str, *, actor: str | None = None) -> dict:
        with self._lock:
            conn = self._conn()
            try:
                with dbmod.transaction(conn):
                    row = conn.execute("SELECT * FROM rule_schedules WHERE id=?",
                                       (schedule_id,)).fetchone()
                    if not row:
                        raise RuleGovernanceError("SCHEDULE_NOT_FOUND",
                                                  "定时计划不存在", 404)
                    if row["status"] != "PENDING":
                        raise RuleGovernanceError("SCHEDULE_NOT_PENDING",
                                                  f"计划状态为 {row['status']}，不能取消",
                                                  409)
                    conn.execute(
                        "UPDATE rule_schedules SET status='CANCELLED' WHERE id=?",
                        (schedule_id,))
                    rule_audit.append_event(
                        conn, "RULE_SCHEDULE_CANCELLED",
                        f"取消定时计划 {row['package_id']}@{row['version']}",
                        payload={"schedule_id": schedule_id},
                        actor=actor, jurisdiction=row["jurisdiction"], when=self._now(),
                        idempotency_key=f"sched-cancel:{schedule_id}")
                    return {"schedule_id": schedule_id, "status": "CANCELLED"}
            finally:
                conn.close()

    def list_schedules(self, status: str | None = None) -> list[dict]:
        conn = self._conn()
        try:
            if status:
                rows = conn.execute(
                    "SELECT * FROM rule_schedules WHERE status=? ORDER BY effective_at",
                    (status,)).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM rule_schedules ORDER BY effective_at").fetchall()
            return [self._schedule_view(r) for r in rows]
        finally:
            conn.close()

    @staticmethod
    def _schedule_view(r: sqlite3.Row) -> dict:
        return {"schedule_id": r["id"], "jurisdiction": r["jurisdiction"],
                "package_id": r["package_id"], "version": r["version"],
                "effective_at": r["effective_at"], "status": r["status"],
                "requested_by": r["requested_by"], "created_at": r["created_at"],
                "fired_at": r["fired_at"], "activation_id": r["activation_id"]}

    def fire_due_schedules(self, *, now: str | None = None) -> list[dict]:
        """触发所有到点的 PENDING 计划。到点时目标已为当前版本则跳过。

        在共享锁 + 计划行 CAS 上执行，与即时启用/回滚串行，只有一个结果。
        """
        now = now or self._now()
        fired = []
        with self._lock:
            conn = self._conn()
            try:
                due = conn.execute(
                    "SELECT * FROM rule_schedules WHERE status='PENDING' "
                    "AND effective_at<=? ORDER BY effective_at,rowid", (now,)).fetchall()
            finally:
                conn.close()
        for sch in due:
            try:
                res = self._activate(
                    sch["package_id"], sch["version"], kind="SCHEDULED",
                    actor="scheduler", expected_package_id=None,
                    expected_version=None,
                    idempotency_key=f"scheduled:{sch['id']}", wait=True)
                fired.append(res)
                fire_status = "FIRED"
                with self._lock:
                    c2 = self._conn()
                    try:
                        with dbmod.transaction(c2):
                            c2.execute(
                                "UPDATE rule_schedules SET status=?,fired_at=?,"
                                "activation_id=? WHERE id=? AND status='PENDING'",
                                (fire_status, self._now(), res["activation_id"], sch["id"]))
                    finally:
                        c2.close()
            except RuleGovernanceError as e:
                # 在途作业 / 版本已移动：计划保留 PENDING 等待下轮或人工处理
                if e.code in ("ACTIVATION_IN_PROGRESS", "VERSION_MOVED"):
                    continue
                raise
        return fired

    # ======================================================================
    # 崩溃恢复 / 调度线程
    # ======================================================================
    def recover(self) -> dict:
        """进程重启/中断后恢复：把 RUNNING 的试算/作业退回可续跑状态。

        单元结果已带唯一约束且按检查点续跑，恢复后重跑只补未完成分片，
        不重复生成差异/候选/审计。未应用的作业绝不视为已启用（指针未动）。
        """
        with self._lock:
            conn = self._conn()
            try:
                with dbmod.transaction(conn):
                    trials = conn.execute(
                        "UPDATE rule_trials SET status='PENDING' "
                        "WHERE status='RUNNING' RETURNING id").fetchall()
                    jobs = conn.execute(
                        "UPDATE rule_jobs SET status='PENDING' WHERE status='RUNNING' "
                        "AND applied=0 RETURNING id").fetchall()
                    n = rule_audit.append_event(
                        conn, "RULE_GOVERNANCE_RECOVERED",
                        "规则治理恢复：中断的试算/启用作业重置为可续跑",
                        payload={"trials_reset": len(trials), "jobs_reset": len(jobs),
                                 "trial_ids": [r["id"] for r in trials],
                                 "job_ids": [r["id"] for r in jobs]},
                        when=self._now(),
                        idempotency_key=f"recover:{self._now()}")
                return {"trials_reset": len(trials), "jobs_reset": len(jobs),
                        "audit_seq": n["seq"], "deduped": n["deduped"]}
            finally:
                conn.close()

    def resume_pending(self, *, max_shards: int | None = None) -> dict:
        """恢复接口：续跑所有未完成试算与启用作业，并触发到点定时计划。"""
        result = {"trials": [], "jobs": [], "schedules_fired": []}
        conn = self._conn()
        try:
            pending_trials = [r["id"] for r in conn.execute(
                "SELECT id FROM rule_trials WHERE status IN ('PENDING','RUNNING') "
                "ORDER BY rowid").fetchall()]
            pending_jobs = [r["id"] for r in conn.execute(
                "SELECT id FROM rule_jobs WHERE status IN ('PENDING','RUNNING') "
                "AND applied=0 ORDER BY rowid").fetchall()]
        finally:
            conn.close()
        for tid in pending_trials:
            result["trials"].append(self.run_trial(tid, max_shards=max_shards))
        for jid in pending_jobs:
            result["jobs"].append(self.run_job(jid, max_shards=max_shards))
        result["schedules_fired"] = self.fire_due_schedules()
        return result

    def start_scheduler(self):
        if self._thread and self._thread.is_alive():
            return

        def loop():
            while not self._stop.is_set():
                try:
                    self.resume_pending()
                except Exception as e:  # noqa: BLE001
                    print(f"[rule-scheduler] {e}")
                self._stop.wait(self.poll_seconds)

        self._thread = threading.Thread(target=loop, name="pv-rule-scheduler", daemon=True)
        self._thread.start()

    def stop_scheduler(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    # ======================================================================
    # 审计
    # ======================================================================
    def audit_trail(self, *, event_type: str | None = None,
                    jurisdiction: str | None = None, limit: int = 500) -> list[dict]:
        conn = self._conn()
        try:
            return rule_audit.list_events(conn, event_type=event_type,
                                          jurisdiction=jurisdiction, limit=limit)
        finally:
            conn.close()

    def verify_audit_chain(self) -> dict:
        conn = self._conn()
        try:
            return rule_audit.verify_chain(conn)
        finally:
            conn.close()
