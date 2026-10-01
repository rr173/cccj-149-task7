"""证据包导出服务。

围绕“病例的某个版本快照”构建自包含证据包，并保证四条硬约束：

1. 快照绑定 —— 包锚定具体版本的 ``VERSION_*`` 审计帧（anchor rowid）。
   包内来源、版本、关系、回执、时间线全部按 rowid 截止线从 *同一个*
   BEGIN IMMEDIATE 事务中读出。导出期间的随访/合并/拆分只会产生新行
   （append-only），永远不可能混进包里；拿不到完整快照（引用行缺失、
   审计链断裂）则任务 FAILED，错误码 SNAPSHOT_INCOMPLETE /
   AUDIT_CHAIN_BROKEN，不拼接、不留半成品包。

2. 同请求唯一 —— export_tasks(case_id, case_version_id, idempotency_key)
   有 UNIQUE 约束：并发发起相同导出只产生一个任务，其余请求返回同一任务；
   包文件按 manifest 根哈希内容寻址存储，确定性构建保证重启重建也只落
   同一份字节，临时目录在启动时清走。

3. 崩溃恢复 —— 启动时 BUILDING 重置为 PENDING（任务可重跑）；
   COMPLETED 但文件丢失的同样回 PENDING；重复文件只保留内容寻址那份。

4. 可核验 —— 包带 Ed25519 签名的清单根，离线核验见 verifier.py。
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3
import threading
import time
import uuid
from pathlib import Path

from . import audit
from . import db as dbmod
from . import ed25519
from . import package_codec as codec
from .service import Clock, DomainError

EXPORT_PROGRESS_STAGES = ["PENDING", "RESOLVE_VERSION", "ASSEMBLE_SNAPSHOT",
                          "BUILD_PACKAGE", "FINALIZE", "COMPLETED"]


class ExportError(DomainError):
    pass


def default_idempotency_key(case_id: str, version_id: str) -> str:
    import hashlib
    return hashlib.sha256(f"{case_id}|{version_id}".encode("utf-8")).hexdigest()[:32]


def _is_shutdown_error(exc: BaseException) -> bool:
    """关闭/删除数据目录后的守护线程轮询错误，属于良性竞态。"""
    import sqlite3
    if isinstance(exc, sqlite3.OperationalError) and (
            "no such table" in str(exc) or "unable to open database" in str(exc)):
        return True
    return isinstance(exc, (FileNotFoundError,))


class ExportService:
    def __init__(self, db_path: str, data_dir: str, clock: Clock | None = None,
                 signer_seed: bytes | None = None, auto_start: bool = True,
                 poll_seconds: float = 0.1, crash_hook=None, build_events=None):
        self.db_path = db_path
        self.data_dir = Path(data_dir)
        self.packages_dir = self.data_dir / "packages"
        self.tmp_dir = self.data_dir / "export_tmp"
        self.packages_dir.mkdir(parents=True, exist_ok=True)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)
        self.clock = clock or Clock()
        self.poll_seconds = poll_seconds
        # 测试钩子：crash_hook 在指定阶段抛出（模拟崩溃）；
        # build_events(stage, task_id) 在阶段点同步触发外部业务变更
        # （随访/合并/拆分），用于验证构建期间状态推进不污染快照。
        self.crash_hook = crash_hook
        self.build_events = build_events
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.signer_seed = signer_seed or self._load_or_create_signer()
        dbmod.initialize(db_path)
        self.recover()
        if auto_start:
            self.start_worker()
    # -- 签名密钥（部署时可注入固定种子；缺省首次启动生成）------------------
    def _load_or_create_signer(self) -> bytes:
        key_path = self.data_dir / "signing_key.bin"
        if key_path.exists():
            seed = bytes.fromhex(key_path.read_text().strip())
            if len(seed) == 32:
                return seed
        seed = os.urandom(32)
        key_path.write_text(seed.hex())
        try:
            key_path.chmod(0o600)
        except OSError:
            pass
        return seed

    @property
    def public_key(self) -> bytes:
        return ed25519.publickey(self.signer_seed)

    def _conn(self):
        return dbmod.connect(self.db_path)

    def _now(self) -> str:
        return self.clock.now_iso()

    # -- 崩溃/重启恢复 --------------------------------------------------------
    def recover(self) -> dict:
        with self._lock:
            conn = self._conn()
            try:
                with dbmod.transaction(conn):
                    reset = conn.execute(
                        "UPDATE export_tasks SET status='PENDING', started_at=NULL,"
                        "progress_json=? WHERE status='BUILDING' RETURNING id",
                        (json.dumps({"stage": "PENDING", "note": "重启恢复，重新排队"}),)
                    ).fetchall()
                    reset_ids = [r["id"] for r in reset]
                    # COMPLETED 但文件已不存在（内容寻址文件被外部清掉）-> 重建
                    requeue_missing = []
                    for r in conn.execute(
                            "SELECT id, package_path FROM export_tasks WHERE status='COMPLETED'"):
                        if not r["package_path"] or not (self.data_dir / r["package_path"]).exists():
                            conn.execute(
                                "UPDATE export_tasks SET status='PENDING', package_path=NULL,"
                                "package_root=NULL, package_size=NULL, started_at=NULL,"
                                "progress_json=? WHERE id=?",
                                (json.dumps({"stage": "PENDING",
                                             "note": "包文件丢失，重新排队"}), r["id"]))
                            requeue_missing.append(r["id"])
                # 临时目录的归属任务行必然先于目录存在（先认领任务、后建目录）。
                # 因此：属于被重置任务的旧尝试目录、或查无任务行的无主目录，
                # 都是崩溃残留，可安全清理；绝不触碰其他运行进程的活跃构建。
                known_ids = {r["id"] for r in
                             conn.execute("SELECT id FROM export_tasks")}
                affected = set(reset_ids) | set(requeue_missing)
                for child in list(self.tmp_dir.iterdir()):
                    if not child.is_dir():
                        continue
                    owner = child.name.split("-", 1)[0]
                    if owner in affected or owner not in known_ids:
                        shutil.rmtree(child, ignore_errors=True)
                return {"reset_building": len(reset),
                        "requeue_missing": len(requeue_missing)}
            finally:
                conn.close()

    # -- 发起导出（幂等）-----------------------------------------------------
    def request_export(self, case_id: str, *, version_id: str | None = None,
                       requested_by: str | None = None,
                       idempotency_key: str | None = None) -> dict:
        with self._lock:
            conn = self._conn()
            try:
                with dbmod.transaction(conn):
                    case = conn.execute("SELECT id FROM cases WHERE id=?",
                                        (case_id,)).fetchone()
                    if not case:
                        raise ExportError("CASE_NOT_FOUND", "病例不存在", 404)
                    if version_id is None:
                        row = conn.execute(
                            "SELECT id FROM case_versions WHERE case_id=? "
                            "ORDER BY version_no DESC LIMIT 1", (case_id,)).fetchone()
                        if not row:
                            raise ExportError("NO_VERSION", "病例尚无任何版本", 409)
                        version_id = row["id"]
                    else:
                        row = conn.execute(
                            "SELECT id FROM case_versions WHERE id=? AND case_id=?",
                            (version_id, case_id)).fetchone()
                        if not row:
                            raise ExportError("VERSION_NOT_FOUND",
                                              "该版本不存在或不属于此病例", 404)
                    key = idempotency_key or default_idempotency_key(case_id, version_id)
                    existing = conn.execute(
                        "SELECT * FROM export_tasks WHERE case_id=? AND case_version_id=? "
                        "AND idempotency_key=?", (case_id, version_id, key)).fetchone()
                    now = self._now()
                    if existing:
                        task = dict(existing)
                        task["deduped"] = True
                        # FAILED 的同请求重试：重新入队（仍是同一个任务，不新增行）
                        if task["status"] == "FAILED":
                            conn.execute(
                                "UPDATE export_tasks SET status='PENDING', error_code=NULL,"
                                "error_message=NULL, started_at=NULL, finished_at=NULL,"
                                "progress_json=? WHERE id=?",
                                (json.dumps({"stage": "PENDING",
                                             "note": "失败后重试，重新排队"}), task["id"]))
                            task["status"] = "PENDING"
                        return self._task_view(task)
                    tid = uuid.uuid4().hex
                    conn.execute(
                        "INSERT INTO export_tasks(id,case_id,case_version_id,idempotency_key,"
                        "status,progress_json,requested_by,requested_at) "
                        "VALUES(?,?,?,?,'PENDING',?,?,?)",
                        (tid, case_id, version_id, key,
                         json.dumps({"stage": "RESOLVE_VERSION"}), requested_by, now))
                    task = conn.execute("SELECT * FROM export_tasks WHERE id=?",
                                        (tid,)).fetchone()
                    return self._task_view(dict(task), deduped=False)
            finally:
                conn.close()

    def _task_view(self, row: dict, deduped: bool = True) -> dict:
        progress = json.loads(row.get("progress_json") or "{}")
        return {
            "task_id": row["id"],
            "case_id": row["case_id"],
            "case_version_id": row["case_version_id"],
            "idempotency_key": row["idempotency_key"],
            "status": row["status"],
            "progress": progress,
            "requested_by": row.get("requested_by"),
            "requested_at": row["requested_at"],
            "started_at": row.get("started_at"),
            "finished_at": row.get("finished_at"),
            "error_code": row.get("error_code"),
            "error_message": row.get("error_message"),
            "package_root": row.get("package_root"),
            "package_path": row.get("package_path"),
            "package_size": row.get("package_size"),
            "deduped": deduped,
        }

    def get_task(self, task_id: str) -> dict:
        conn = self._conn()
        try:
            row = conn.execute("SELECT * FROM export_tasks WHERE id=?",
                               (task_id,)).fetchone()
            if not row:
                raise ExportError("TASK_NOT_FOUND", "导出任务不存在", 404)
            return self._task_view(dict(row))
        finally:
            conn.close()

    def list_tasks(self, case_id: str | None = None) -> list[dict]:
        conn = self._conn()
        try:
            if case_id:
                rows = conn.execute(
                    "SELECT * FROM export_tasks WHERE case_id=? ORDER BY requested_at, rowid",
                    (case_id,)).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM export_tasks ORDER BY requested_at, rowid").fetchall()
            return [self._task_view(dict(r)) for r in rows]
        finally:
            conn.close()

    def package_path(self, task_id: str) -> tuple[Path, dict]:
        conn = self._conn()
        try:
            row = conn.execute("SELECT * FROM export_tasks WHERE id=?",
                               (task_id,)).fetchone()
            if not row:
                raise ExportError("TASK_NOT_FOUND", "导出任务不存在", 404)
            if row["status"] != "COMPLETED" or not row["package_path"]:
                raise ExportError("PACKAGE_NOT_READY",
                                  f"任务状态为 {row['status']}，包尚不可下载", 409)
            path = self.data_dir / row["package_path"]
            if not path.exists():
                raise ExportError("PACKAGE_MISSING",
                                  "清单记录的包文件已丢失，请重新发起导出", 410)
            return path, self._task_view(dict(row))
        finally:
            conn.close()

    # -- 核验（服务端复用同一套离线核验纯函数，结果留档）---------------------
    def verify_task(self, task_id: str, *, trusted_key_fingerprint: str | None = None,
                    verifier: str = "server") -> dict:
        path, task = self.package_path(task_id)
        from . import verifier as vmod
        report = vmod.verify_package(str(path),
                                     trusted_key_fingerprint=trusted_key_fingerprint)
        conn = self._conn()
        try:
            with dbmod.transaction(conn):
                conn.execute(
                    "INSERT INTO verification_runs(id,export_task_id,package_root,ok,"
                    "report_json,verifier,created_at) VALUES(?,?,?,?,?,?,?)",
                    (uuid.uuid4().hex, task_id, task["package_root"],
                     1 if report["valid"] else 0,
                     json.dumps(report, ensure_ascii=False), verifier, self._now()))
        finally:
            conn.close()
        return report

    def verification_runs(self, task_id: str | None = None) -> list[dict]:
        conn = self._conn()
        try:
            if task_id:
                rows = conn.execute(
                    "SELECT * FROM verification_runs WHERE export_task_id=? "
                    "ORDER BY created_at,rowid", (task_id,)).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM verification_runs ORDER BY created_at,rowid").fetchall()
            out = []
            for r in rows:
                d = {"run_id": r["id"], "export_task_id": r["export_task_id"],
                     "package_root": r["package_root"], "ok": bool(r["ok"]),
                     "verifier": r["verifier"], "created_at": r["created_at"],
                     "report": json.loads(r["report_json"])}
                out.append(d)
            return out
        finally:
            conn.close()

    # -- 后台工作线程 --------------------------------------------------------
    def start_worker(self):
        if self._thread and self._thread.is_alive():
            return

        def loop():
            while not self._stop.is_set():
                try:
                    if self.run_next() is None:
                        self._stop.wait(self.poll_seconds)
                except Exception as e:  # 工作线程绝不停
                    # 关闭/删除数据目录后的轮询竞态：静默，不再刷日志
                    if not self._stop.is_set() and not _is_shutdown_error(e):
                        print(f"[export-worker] {e}")
                    self._stop.wait(self.poll_seconds)

        self._thread = threading.Thread(target=loop, name="pv-export-worker", daemon=True)
        self._thread.start()

    def stop_worker(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def run_next(self) -> str | None:
        """认领并执行一个 PENDING 任务；没有则返回 None。"""
        with self._lock:
            conn = self._conn()
            try:
                task_id = None
                with dbmod.transaction(conn):
                    row = conn.execute(
                        "SELECT * FROM export_tasks WHERE status='PENDING' "
                        "ORDER BY requested_at, rowid LIMIT 1").fetchone()
                    if not row:
                        return None
                    task_id = row["id"]
                    conn.execute(
                        "UPDATE export_tasks SET status='BUILDING', started_at=? "
                        "WHERE id=? AND status='PENDING'", (self._now(), task_id))
            finally:
                conn.close()
        if task_id:
            self._build(task_id)
        return task_id

    def run_until_idle(self, timeout: float = 30.0) -> None:
        """同步排空队列（测试/脚本）。"""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.run_next() is None:
                conn = self._conn()
                try:
                    pending = conn.execute(
                        "SELECT COUNT(*) AS n FROM export_tasks WHERE status IN "
                        "('PENDING','BUILDING')").fetchone()["n"]
                finally:
                    conn.close()
                if pending == 0:
                    return
            time.sleep(0.01)
        raise TimeoutError("导出队列未在时限内排空")

    def _set_progress(self, task_id, stage, **extra):
        conn = self._conn()
        try:
            with dbmod.transaction(conn):
                conn.execute("UPDATE export_tasks SET progress_json=? WHERE id=?",
                             (json.dumps({"stage": stage, **extra}), task_id))
        finally:
            conn.close()

    def _fail(self, task_id, code, message):
        conn = self._conn()
        try:
            with dbmod.transaction(conn):
                conn.execute(
                    "UPDATE export_tasks SET status='FAILED', finished_at=?, error_code=?,"
                    "error_message=?, progress_json=? WHERE id=?",
                    (self._now(), code, message,
                     json.dumps({"stage": "FAILED", "error_code": code}), task_id))
        finally:
            conn.close()

    # ======================================================================
    # 快照组装 + 构建（核心）
    # ======================================================================
    def _build(self, task_id: str) -> None:
        attempt = uuid.uuid4().hex[:12]
        try:
            if self.build_events:
                self.build_events("BEFORE_ASSEMBLE", task_id)
            self._set_progress(task_id, "ASSEMBLE_SNAPSHOT", attempt=attempt)
            if self.crash_hook:
                self.crash_hook("CLAIMED", task_id)
            bundle = self._assemble(task_id)
            if self.crash_hook:
                self.crash_hook("ASSEMBLED", task_id)
            # 快照已在单事务内固定；此刻并发的随访/合并/拆分只能追加新版本，
            # 无法改动 bundle。在最终化前触发业务变更，证明落盘的仍是旧快照。
            if self.build_events:
                self.build_events("AFTER_ASSEMBLE", task_id)
            self._set_progress(task_id, "BUILD_PACKAGE", attempt=attempt)
            self._finalize(task_id, bundle, attempt)
        except ExportError as e:
            self._fail(task_id, e.code, str(e))
        except audit.ChainBroken as e:
            self._fail(task_id, "AUDIT_CHAIN_BROKEN", str(e))
        except Exception as e:  # noqa: BLE001
            self._fail(task_id, "BUILD_ERROR", f"{type(e).__name__}: {e}")

    def _assemble(self, task_id: str) -> dict:
        """在单一事务中按锚点截止线收集全部证据，返回可直接落盘的 bundle。

        整个读取在一个 BEGIN IMMEDIATE 事务内：SQLite 快照 + append-only
        表结构保证读到的来源/版本/关系/回执/事件互相对应；任何引用缺失即
        SNAPSHOT_INCOMPLETE，绝不从前/后版本拼数据补齐。
        """
        conn = self._conn()
        try:
            with dbmod.transaction(conn):
                task = conn.execute("SELECT * FROM export_tasks WHERE id=?",
                                    (task_id,)).fetchone()
                case_id, version_id = task["case_id"], task["case_version_id"]

                version = conn.execute(
                    "SELECT * FROM case_versions WHERE id=? AND case_id=?",
                    (version_id, case_id)).fetchone()
                if not version:
                    raise ExportError("SNAPSHOT_INCOMPLETE",
                                      "目标版本行缺失（无法取得发起时版本快照）", 409)

                # 锚点：该版本对应的 VERSION_* 审计帧。
                # 注意一个版本在同一事务里先写 VERSION_* 帧，再写 SOURCE_INTAKE /
                # REPORT_QUEUED 等帧，因此版本的截止线不是 VERSION 帧自身 rowid，
                # 而是“下一版本 VERSION 帧之前”的最后一行 —— 这样同版本的报告
                # 排队/回执帧都在窗口内，而下一版本（随访/合并/拆分）的帧被排除。
                anchor_evt = conn.execute(
                    "SELECT rowid AS rowid,* FROM case_events WHERE version_id=? "
                    "AND event_type LIKE 'VERSION\\_%' ESCAPE '\\' ORDER BY rowid LIMIT 1",
                    (version_id,)).fetchone()
                if not anchor_evt:
                    raise ExportError("SNAPSHOT_INCOMPLETE",
                                      "版本缺少 VERSION_* 锚点事件，快照不完整", 409)
                next_version_evt = conn.execute(
                    "SELECT e.rowid AS rowid FROM case_events e JOIN case_versions v "
                    "ON v.id=e.version_id "
                    "WHERE v.case_id=? AND e.event_type LIKE 'VERSION\\_%' ESCAPE '\\' "
                    "AND v.version_no>? ORDER BY e.rowid LIMIT 1",
                    (case_id, version["version_no"])).fetchone()
                anchor_cut = (next_version_evt["rowid"] - 1
                              if next_version_evt else self._max_event_rowid(conn))

                # -- 血缘截止：沿合并/拆分历史求“截止时”的病例集合与逐病例 cutoff
                per_case_cut = self._lineage_as_of(
                    conn, case_id, anchor_evt["rowid"], anchor_cut)

                # 全部相关版本（含被合并病例在其截止前的版本）
                cases_version_rows = {}
                for cid, cut in per_case_cut.items():
                    # 版本表 rowid 与事件表 rowid 不可跨表比较：用每个版本的
                    # VERSION_* 帧 rowid 判定其是否在截止线之前。
                    vrows = [v for v in conn.execute(
                        "SELECT * FROM case_versions WHERE case_id=? ORDER BY version_no",
                        (cid,))
                        if conn.execute(
                            "SELECT rowid FROM case_events WHERE version_id=? "
                            "AND event_type LIKE 'VERSION\\_%' ESCAPE '\\' "
                            "ORDER BY rowid LIMIT 1", (v["id"],)).fetchone()["rowid"] <= cut]
                    cases_version_rows[cid] = vrows
                    if not vrows:
                        raise ExportError("SNAPSHOT_INCOMPLETE",
                                          f"病例 {cid[:8]} 在截止点无版本（快照不完整）", 409)
                if not any(v["id"] == version_id for v in cases_version_rows[case_id]):
                    raise ExportError("SNAPSHOT_INCOMPLETE",
                                      "锚点版本不在截止版本序列中（状态被拼接）", 409)

                # -- 审计链切片 + 逐病例链核验 ------------------------------
                timelines_files = []
                events_by_case: dict[str, list] = {}
                for cid, cut in per_case_cut.items():
                    ev_rows = conn.execute(
                        "SELECT rowid AS rowid,* FROM case_events WHERE case_id=? "
                        "AND rowid<=? ORDER BY seq", (cid, cut)).fetchall()
                    events_dicts = [dict(r) for r in ev_rows]
                    try:
                        audit.verify_chain(events_dicts, label="病例")
                    except audit.ChainBroken as e:
                        raise audit.ChainBroken(
                            [f"病例 {cid[:8]}: {f}" for f in e.failures])
                    events_by_case[cid] = events_dicts
                    timelines_files.append({
                        "case_id": cid,
                        "path": f"timeline/{cid}.json",
                        "first_seq": events_dicts[0]["seq"] if events_dicts else 0,
                        "last_seq": events_dicts[-1]["seq"] if events_dicts else 0,
                        "n_events": len(events_dicts),
                        "cut_rowid": cut,
                        "head": audit.chain_head(events_dicts),
                    })

                # -- 来源归属 as-of + 来源原文 --------------------------------
                source_ids, memberships = self._sources_as_of(conn, per_case_cut)
                source_rows = {}
                for sid in source_ids:
                    sr = conn.execute("SELECT * FROM source_records WHERE id=?",
                                      (sid,)).fetchone()
                    if not sr:
                        raise ExportError("SNAPSHOT_INCOMPLETE",
                                          f"来源原文缺失: {sid[:8]}（快照引用悬空）", 409)
                    source_rows[sid] = sr

                # -- 合并/拆分关系（截止锚点）--------------------------------
                relations = self._relations_as_of(conn, per_case_cut,
                                                  cases_version_rows, events_by_case)

                # -- 报送 + 回执（截止链上引用到的报告）-----------------------
                report_ids = set()
                for evs in events_by_case.values():
                    for e in evs:
                        if e.get("report_id"):
                            report_ids.add(e["report_id"])
                reports = {}
                for rid in sorted(report_ids):
                    rr = conn.execute("SELECT * FROM regulatory_reports WHERE id=?",
                                      (rid,)).fetchone()
                    if not rr:
                        raise ExportError("SNAPSHOT_INCOMPLETE",
                                          f"报告记录缺失: {rid[:8]}（审计引用悬空）", 409)
                    # 截止时状态由审计帧重放冻结（不取当前 mutable 状态，
                    # 避免把锚点之后的 ACK 拼进来）
                    reports[rid] = self._frozen_report(conn, rr, events_by_case)

                snapshot_json = version["snapshot_json"]
                snapshot_obj = json.loads(snapshot_json)

                # 组装文件对象（调用方负责序列化）
                files: dict[str, dict] = {}
                files["export/request.json"] = {"object": {
                    "case_id": case_id, "version_id": version_id,
                    "version_no": version["version_no"],
                    "requested_at": task["requested_at"],
                    "requested_by": task["requested_by"],
                    "idempotency_key": task["idempotency_key"],
                }}
                files["export/snapshot.json"] = {"object": {
                    "case_id": case_id,
                    "version_id": version_id,
                    "version_no": version["version_no"],
                    "change_type": version["change_type"],
                    "created_at": version["created_at"],
                    "actor": version["actor"],
                    "material_id": version["material_id"],
                    "change_summary": json.loads(version["change_summary_json"]),
                    "snapshot": snapshot_obj,
                    "snapshot_sha256": codec.sha256_hex(
                        codec.canonical_json(snapshot_obj)),
                    "source_ids": sorted(source_ids),
                    "case_scope": sorted(per_case_cut.keys()),
                }}
                for cid, vrows in cases_version_rows.items():
                    files[f"cases/{cid}.json"] = {"object": {
                        "case_id": cid,
                        "versions": [{
                            "id": v["id"], "version_no": v["version_no"],
                            "change_type": v["change_type"],
                            "created_at": v["created_at"], "actor": v["actor"],
                            "material_id": v["material_id"],
                            "snapshot_sha256": codec.sha256_hex(
                                codec.canonical_json(json.loads(v["snapshot_json"]))),
                            "snapshot": json.loads(v["snapshot_json"]),
                            "change_summary": json.loads(v["change_summary_json"]),
                        } for v in vrows],
                    }}
                for sid, sr in source_rows.items():
                    files[f"sources/{sid}.json"] = {"object": {
                        "id": sr["id"], "source_system": sr["source_system"],
                        "external_id": sr["external_id"], "received_at": sr["received_at"],
                        "created_at": sr["created_at"],
                        "payload": json.loads(sr["payload_json"]),
                    }}
                files["memberships.json"] = {"object": {
                    "as_of_anchor_rowid": anchor_cut,
                    "memberships": [{
                        "id": m["id"], "source_id": m["source_id"],
                        "case_id": m["case_id"], "active_at_cut": m["active_at_cut"],
                        "reason": m["reason"], "actor": m["actor"],
                        "created_event_rowid": m["created_event_rowid"],
                    } for m in memberships],
                }}
                files["relations.json"] = {"object": {
                    "as_of_anchor_rowid": anchor_cut, "relations": relations}}
                for rid, rep in reports.items():
                    files[f"reports/{rid}.json"] = {"object": rep}
                for cid, evs in events_by_case.items():
                    files[f"timeline/{cid}.json"] = {"object": {
                        "case_id": cid, "cut_rowid": per_case_cut[cid],
                        "head": audit.chain_head(evs),
                        "events": [{
                            "id": e["id"], "case_id": e["case_id"], "seq": e["seq"],
                            "event_type": e["event_type"], "event_time": e["event_time"],
                            "actor": e["actor"], "summary": e["summary"],
                            "payload": json.loads(e["payload_json"]),
                            "version_id": e["version_id"], "report_id": e["report_id"],
                            "cluster_id": e["cluster_id"],
                            "prev_hash": e["prev_hash"], "entry_hash": e["entry_hash"],
                        } for e in evs],
                    }}

                return {
                    "task": dict(task),
                    "case_id": case_id,
                    "version_id": version_id,
                    "version_no": version["version_no"],
                    "change_type": version["change_type"],
                    "snapshot_obj": snapshot_obj,
                    "anchor": {"version_id": version_id,
                               "anchor_event_id": anchor_evt["id"],
                               "anchor_event_type": anchor_evt["event_type"],
                               "anchor_event_rowid": anchor_evt["rowid"],
                               "anchor_seq": anchor_evt["seq"],
                               "cut_rowid": anchor_cut},
                    "per_case_cut": per_case_cut,
                    "timelines_files": timelines_files,
                    "relations": relations,
                    "files": files,
                }
        finally:
            conn.close()

    # -- 血缘截止：锚定版本窗口内该病例“看得到”的病例集合 --------------------
    def _version_window_end(self, conn, version_id: str) -> int:
        """某版本窗口的事件截止 rowid = 下一版本 VERSION 帧 rowid - 1。

        同一版本的全部事件在一个事务内提交（VERSION_* 帧最先写，
        SOURCE_INTAKE/REPORT_QUEUED 等随后写），而下一版本的 VERSION 帧是
        下一个事务。用“下一版本帧前一行”作窗口边界，恰好包含本版本全部事件、
        排除下一版本（之后的随访/合并/拆分）的任何事件。
        """
        v = conn.execute("SELECT case_id, version_no FROM case_versions WHERE id=?",
                         (version_id,)).fetchone()
        nxt = conn.execute(
            "SELECT e.rowid AS rowid FROM case_events e JOIN case_versions cv "
            "ON cv.id=e.version_id WHERE cv.case_id=? "
            "AND e.event_type LIKE 'VERSION\\_%' ESCAPE '\\' "
            "AND cv.version_no>? ORDER BY e.rowid LIMIT 1",
            (v["case_id"], v["version_no"])).fetchone()
        if nxt:
            return nxt["rowid"] - 1
        return self._max_event_rowid(conn)

    @staticmethod
    def _max_event_rowid(conn) -> int:
        row = conn.execute("SELECT COALESCE(MAX(rowid),0) AS m FROM case_events").fetchone()
        return row["m"]

    def _lineage_as_of(self, conn, case_id, anchor_frame_rowid, anchor_cut):
        """求锚定版本窗口内的病例作用域与逐病例 rowid 截止线。

        - 主病例 cutoff = 其锚定版本窗口末（anchor_cut）；
        - 合并进来的病例：沿窗口内 MERGE 版本的 change_summary 找，
          cutoff = 其 CASE_MERGED_AWAY 帧所在版本窗口末（含同事务镜像帧）；
        - 拆分母病例：沿 CASE_SPLIT_IN 反查，cutoff = 父病例 SPLIT 版本窗口末。
        只依据窗口内已存在的版本/事件，不使用当前 case_lineage 闭包，
        因此锚点之后的合并/拆分不会拼接进包；BFS 覆盖先拆分后合并等组合。
        """
        cuts = {case_id: anchor_cut}
        frontier = [case_id]
        visited: set[str] = set()
        while frontier:
            cid = frontier.pop()
            if cid in visited:
                continue
            visited.add(cid)
            cut = cuts[cid]

            all_vrows = conn.execute(
                "SELECT * FROM case_versions WHERE case_id=? ORDER BY version_no",
                (cid,)).fetchall()
            for v in all_vrows:
                ver = conn.execute(
                    "SELECT rowid AS rowid FROM case_events WHERE version_id=? "
                    "AND event_type LIKE 'VERSION\\_%' ESCAPE '\\' ORDER BY rowid LIMIT 1",
                    (v["id"],)).fetchone()
                if not ver or ver["rowid"] > cut:
                    continue
                for item in json.loads(v["change_summary_json"] or "[]"):
                    if not isinstance(item, dict) or "merged_from" not in item:
                        continue
                    for other in item["merged_from"]:
                        away = conn.execute(
                            "SELECT rowid AS rowid FROM case_events "
                            "WHERE case_id=? AND event_type='CASE_MERGED_AWAY' "
                            "ORDER BY rowid LIMIT 1", (other,)).fetchone()
                        if not away:
                            raise ExportError(
                                "SNAPSHOT_INCOMPLETE",
                                f"被合并病例 {other[:8]} 缺少 CASE_MERGED_AWAY 帧", 409)
                        # 被合并病例在此帧后不再产生任何事件，away 帧即其精确截止线
                        # （它最后一个版本窗口的事务末尾）。
                        if other not in cuts:
                            cuts[other] = away["rowid"]
                            frontier.append(other)

            # 若作用域中的病例本身是拆分产物，引入父病例（cutoff=父 SPLIT 窗口末）
            split_in = conn.execute(
                "SELECT rowid AS rowid,payload_json FROM case_events "
                "WHERE case_id=? AND event_type='CASE_SPLIT_IN' AND rowid<=? "
                "ORDER BY rowid LIMIT 1", (cid, cut)).fetchone()
            if split_in:
                payload = json.loads(split_in["payload_json"])
                parent = payload.get("parent_case_id")
                if parent and parent not in cuts:
                    match = conn.execute(
                        "SELECT rowid AS rowid,version_id FROM case_events "
                        "WHERE case_id=? AND event_type='CASE_SPLIT' AND rowid<=? "
                        "ORDER BY rowid", (parent, cut)).fetchall()
                    parent_version = None
                    for r in match:
                        pe = conn.execute(
                            "SELECT payload_json FROM case_events WHERE rowid=?",
                            (r["rowid"],)).fetchone()
                        if json.loads(pe["payload_json"]).get("new_case_id") == cid:
                            parent_version = r["version_id"]
                            break
                    if parent_version is None:
                        raise ExportError(
                            "SNAPSHOT_INCOMPLETE",
                            "拆分母病例的 CASE_SPLIT 帧缺失（快照不完整）", 409)
                    cuts[parent] = self._version_window_end(conn, parent_version)
                    frontier.append(parent)
        return cuts

    # -- 来源归属 as-of -------------------------------------------------------
    def _sources_as_of(self, conn, per_case_cut):
        """逐病例按其 cutoff 重放只增归属表，得到截止时仍归属该病例的来源。

        case_membership 只增：来源转移时旧行 active 1->0、新病例插入新行。
        判定规则：对每一条 active=1 的归属插入行（id 唯一），它在某病例
        cutoff 时仍有效，当且仅当同一来源不存在 rowid<=cutoff 的更晚 active=1
        行（即之后没有被转走）。这样锚点之后发生的拆分/合并不会影响判定。
        """
        all_sources: set[str] = set()
        memberships_out = []
        for cid, cut in per_case_cut.items():
            rows = conn.execute(
                "SELECT rowid AS rowid,* FROM case_membership WHERE case_id=? "
                "AND active=1 AND rowid<=? ORDER BY rowid", (cid, cut)).fetchall()
            for r in rows:
                newer = conn.execute(
                    "SELECT 1 FROM case_membership WHERE source_id=? AND active=1 "
                    "AND rowid>? AND rowid<=? LIMIT 1",
                    (r["source_id"], r["rowid"], cut)).fetchone()
                active_at_cut = 0 if newer else 1
                memberships_out.append({
                    "id": r["id"], "source_id": r["source_id"], "case_id": cid,
                    "active_at_cut": active_at_cut, "reason": r["reason"],
                    "actor": r["actor"], "created_event_rowid": r["rowid"],
                })
                if active_at_cut:
                    all_sources.add(r["source_id"])
        memberships_out.sort(key=lambda m: (m["case_id"], m["created_event_rowid"]))
        return all_sources, memberships_out

    # -- 关系 as-of -----------------------------------------------------------
    def _relations_as_of(self, conn, per_case_cut, cases_version_rows, events_by_case):
        relations = []
        for cid, cut in per_case_cut.items():
            for v in cases_version_rows.get(cid, []):
                if v["change_type"] == "MERGE":
                    for item in json.loads(v["change_summary_json"] or "[]"):
                        if isinstance(item, dict) and "merged_from" in item:
                            relations.append({
                                "relation": "MERGE",
                                "survivor_case_id": cid,
                                "merged_from_case_ids": item["merged_from"],
                                "cluster_id": item.get("cluster_id"),
                                "version_id": v["id"], "version_no": v["version_no"],
                                "at": v["created_at"],
                            })
                elif v["change_type"] == "SPLIT":
                    for item in json.loads(v["change_summary_json"] or "[]"):
                        if isinstance(item, dict) and item.get("new_case"):
                            relations.append({
                                "relation": "SPLIT",
                                "parent_case_id": cid,
                                "source_id": item.get("split_source"),
                                "new_case_id": item.get("new_case"),
                                "version_id": v["id"], "version_no": v["version_no"],
                                "at": v["created_at"],
                            })
        relations.sort(key=lambda r: (r["at"], r.get("version_id") or ""))
        return relations

    # -- 报告截止状态重放 -----------------------------------------------------
    def _frozen_report(self, conn, report_row, events_by_case):
        rid = report_row["id"]
        # 收集截止链上与该报告相关的全部事件，按 rowid 排序重放状态
        evs = []
        for cid, es in events_by_case.items():
            for e in es:
                if e["report_id"] == rid:
                    evs.append((e["rowid"], cid, e))
        evs.sort(key=lambda x: x[0])
        status = "QUEUED"
        last_ack = None
        status_changes = []
        for _, cid, e in evs:
            t = e["event_type"]
            mapped = None
            if t in ("REPORT_QUEUED", "REPORT_QUEUED_EXPEDITED"):
                mapped = "QUEUED"
            elif t == "REPORT_RETRY_SCHEDULED" or t == "REPORT_RECOVERED" \
                    or t == "REPORT_REQUEUED":
                mapped = "QUEUED"
            elif t == "REPORT_FAILED":
                mapped = "FAILED"
            elif t == "REPORT_ACKED":
                mapped = "ACKED"
                payload = json.loads(e["payload_json"])
                last_ack = payload.get("ack")
            elif t == "REPORT_VOIDED_BY_MERGE":
                mapped = "VOID"
            if mapped:
                status = mapped
                status_changes.append({"seq": e["seq"], "case_id": cid,
                                       "event_type": t, "status": mapped,
                                       "event_time": e["event_time"]})
        d = dict(report_row)
        return {
            "id": d["id"], "case_id": d["case_id"], "case_version_id": d["case_version_id"],
            "followup_of_id": d["followup_of_id"], "report_type": d["report_type"],
            "regulatory_number": d["regulatory_number"],
            "status_as_of_anchor": status,
            "expedited": bool(d["expedited"]), "days_due": d["days_due"],
            "due_date": d["due_date"],
            "decision": json.loads(d["decision_json"]),
            "payload": json.loads(d["payload_json"]),
            "created_at": d["created_at"],
            "ack_as_of_anchor": last_ack,
            "status_trace": status_changes,
        }

    # -- 落盘 + 原子提交 ------------------------------------------------------
    def _finalize(self, task_id: str, bundle: dict, attempt: str) -> None:
        task = bundle["task"]
        # 每次构建尝试使用独立临时目录：崩溃恢复后新旧尝试互不删除对方文件。
        tmp_dir = self.tmp_dir / f"{task_id}-{attempt}"
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir, ignore_errors=True)
        tmp_dir.mkdir(parents=True)
        tmp_file = tmp_dir / "package.tar"

        request_obj = bundle["files"]["export/request.json"]["object"]
        snapshot_view = {
            "case_id": bundle["case_id"],
            "version_id": bundle["version_id"],
            "version_no": bundle["version_no"],
            "change_type": bundle.get("change_type"),
            "snapshot_sha256": codec.sha256_hex(
                codec.canonical_json(bundle["snapshot_obj"])),
        }
        # 先不带 package_id 构建以得到内容根，再由根派生确定性 package_id，
        # 保证同一请求在任意进程重build字节完全一致（内容寻址只留一份）。
        provisional_id = "0" * 32
        with open(tmp_file, "wb") as fh:
            manifest = codec.build_package(
                fh,
                package_meta={"package_id": provisional_id,
                              "created_at": task["requested_at"],
                              "service": "pv_service"},
                request=request_obj,
                snapshot_view=snapshot_view,
                files=bundle["files"],
                timelines=bundle["timelines_files"],
                relations=bundle["relations"],
                anchor=bundle["anchor"],
                signer_seed=self.signer_seed)
        package_id = "pkg" + manifest["root"][:29]
        # package_id 位于 package.json（不参与清单根），重写一次即为最终字节
        with open(tmp_file, "wb") as fh:
            manifest = codec.build_package(
                fh,
                package_meta={"package_id": package_id,
                              "created_at": task["requested_at"],
                              "service": "pv_service"},
                request=request_obj,
                snapshot_view=snapshot_view,
                files=bundle["files"],
                timelines=bundle["timelines_files"],
                relations=bundle["relations"],
                anchor=bundle["anchor"],
                signer_seed=self.signer_seed)

        data = tmp_file.read_bytes()
        size = len(data)
        root = manifest["root"]
        # 构建自检：回读包并确认清单根/签名自洽（不调用会落库的 verify_task）
        from . import verifier as vmod
        self_check = vmod.verify_package(data)
        if not self_check["valid"]:
            raise ExportError("BUILD_ERROR",
                              "构建自检失败: " + "; ".join(self_check["errors"][:5]))

        rel_path = Path("packages") / f"{root}.tar"
        final_path = self.data_dir / rel_path
        # 内容寻址：同根即同一包；已存在则直接复用（绝不覆盖/复制第二份）
        final_path.parent.mkdir(parents=True, exist_ok=True)
        if not final_path.exists():
            os.replace(tmp_file, final_path)
        shutil.rmtree(tmp_dir, ignore_errors=True)

        conn = self._conn()
        try:
            with dbmod.transaction(conn):
                # 条件提交：只有任务仍处于本次 BUILDING 才标记完成。
                # 恢复重排队后旧尝试不得把已经被新尝试推进的任务改坏。
                cur = conn.execute(
                    "SELECT status FROM export_tasks WHERE id=?", (task_id,)).fetchone()
                if cur is None:
                    return
                if cur["status"] == "COMPLETED":
                    return  # 另一尝试已完成（内容寻址，同一根），幂等返回
                conn.execute(
                    "UPDATE export_tasks SET status='COMPLETED', finished_at=?,"
                    "package_root=?,package_path=?,package_size=?,progress_json=? "
                    "WHERE id=? AND status='BUILDING'",
                    (self._now(), root, str(rel_path), size,
                     json.dumps({"stage": "COMPLETED", "package_root": root,
                                 "files": len(manifest["files"])}),
                     task_id))
        finally:
            conn.close()
