"""药物安全信号窗口计算服务（可复演运行 + 只增信号修订）。

一次完整生命周期：

1. 分析师登记 *术语归一版本*（sig_terms）与 *监测定义*
   （药品集合/事件分组/观察期/最小样本量/PRR/ROR 阈值，sig_definitions）；
2. 病例版本随随访/归并/拆解/撤回/因果修正只增录入（sig_case_versions）；
3. ``submit_run`` 用 (定义, 病例截止线, 术语版本, 模式, 基线运行) 确定性导出
   computation_key：同键并发只产生一个作业；提交事务里冻结病例快照；
4. ``run_shards`` / ``run_until_published`` 分片执行，每片独立短事务提交
   （检查点），崩溃恢复只补未提交片；任一片失败不发布半成品；
5. ``_publish`` 在 *单个 IMMEDIATE 事务* 内：撤销旧贡献、汇总四格、追加
   信号修订（指标列不可变）、冻结入选清单、生成修订差异、落贡献台账；
6. 分析师对修订做 CONFIRMED/REFUTED/WATCH 处置：必须引用最新修订号
   （过期号 409 STALE_REVISION），旧修订处置永久保留。
"""
from __future__ import annotations

import json
import threading
import uuid
from datetime import datetime

from . import signal_audit as audit
from . import signal_db as db
from . import signal_engine as eng
from .service import Clock, DomainError


def _uid() -> str:
    return uuid.uuid4().hex


def _json(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), default=str)


class SignalService:
    def __init__(self, db_path: str, *, clock: Clock | None = None,
                 shared_lock=None, shard_size: int = 200, shard_count: int = 4):
        self.db_path = db_path
        self.clock = clock or Clock()
        self._lock = shared_lock or threading.RLock()
        self.default_shard_size = shard_size
        self.default_shard_count = shard_count
        db.initialize(db_path)
        # 测试钩子：在分片提交前注入故障（模拟崩溃/坏片）
        self.shard_fault_hook = None

    def _conn(self):
        return db.connect(self.db_path)

    def _now(self) -> str:
        return self.clock.now().isoformat(timespec="seconds")

    # ------------------------------------------------------------------ 术语
    def create_term_version(self, mappings: dict, *, term_version: str,
                            note: str = "", created_by: str | None = None) -> dict:
        """登记术语归一版本。相同映射内容幂等返回原版本。"""
        if not isinstance(mappings, dict) or not mappings:
            raise DomainError("BAD_FIELD", "mappings 必须是非空 {原文: 规范事件码}")
        norm = {eng.normalize_term(k): str(v) for k, v in mappings.items()}
        content_hash = eng.digest({"mappings": norm})
        with self._lock, self._conn() as conn:
            row = conn.execute("SELECT * FROM sig_terms WHERE content_hash=?",
                               (content_hash,)).fetchone()
            if row:
                view = self._term_view(row)
                view["deduped"] = True
                return view
            tid = _uid()
            try:
                conn.execute(
                    "INSERT INTO sig_terms(id,term_version,mappings_json,content_hash,"
                    "note,created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                    (tid, term_version, _json(norm), content_hash, note,
                     created_by, self._now()))
            except Exception as e:  # UNIQUE(term_version)
                raise DomainError("VERSION_CONFLICT",
                                  f"术语版本号 {term_version} 已被不同内容占用",
                                  409) from e
            row = conn.execute("SELECT * FROM sig_terms WHERE id=?", (tid,)).fetchone()
            audit.append_event(conn, "TERM_VERSION_REGISTERED",
                               f"登记术语归一版本 {term_version}",
                               payload={"term_version": term_version,
                                        "content_hash": content_hash,
                                        "mappings": len(norm)},
                               actor=created_by, created_at=self._now())
            view = self._term_view(row)
            view["deduped"] = False
            return view

    def list_term_versions(self) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute("SELECT * FROM sig_terms ORDER BY created_at").fetchall()
            return [self._term_view(r) for r in rows]

    def get_term_version(self, term_version_id: str) -> dict:
        with self._conn() as conn:
            row = self._term_row(conn, term_version_id)
            return self._term_view(row)

    def _term_row(self, conn, tid):
        row = conn.execute("SELECT * FROM sig_terms WHERE id=?", (tid,)).fetchone()
        if not row:
            raise DomainError("NOT_FOUND", f"术语版本不存在: {tid}", 404)
        return row

    @staticmethod
    def _term_view(row) -> dict:
        return {"term_version_id": row["id"], "term_version": row["term_version"],
                "mappings": json.loads(row["mappings_json"]),
                "content_hash": row["content_hash"], "note": row["note"],
                "created_by": row["created_by"], "created_at": row["created_at"]}

    # ------------------------------------------------------------------ 定义
    def create_definition(self, config: dict, *, definition_code: str,
                          created_by: str | None = None) -> dict:
        """创建监测定义（内容冻结）。相同配置幂等返回原定义。"""
        try:
            parsed = eng.parse_config(config)
        except ValueError as e:
            raise DomainError("BAD_FIELD", str(e), 400) from e
        content_hash = eng.digest({"code": definition_code, "config": parsed})
        with self._lock, self._conn() as conn:
            row = conn.execute("SELECT * FROM sig_definitions WHERE content_hash=?",
                               (content_hash,)).fetchone()
            if row:
                view = self._def_view(row, parsed)
                view["deduped"] = True
                return view
            version = (conn.execute(
                "SELECT COALESCE(MAX(version),0)+1 v FROM sig_definitions "
                "WHERE definition_code=?", (definition_code,)).fetchone()["v"])
            did = _uid()
            conn.execute(
                "INSERT INTO sig_definitions(id,definition_code,version,config_json,"
                "content_hash,created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                (did, definition_code, version, _json(parsed), content_hash,
                 created_by, self._now()))
            row = conn.execute("SELECT * FROM sig_definitions WHERE id=?",
                               (did,)).fetchone()
            audit.append_event(conn, "DEFINITION_CREATED",
                               f"创建监测定义 {definition_code} v{version}",
                               payload={"definition_id": did,
                                        "definition_code": definition_code,
                                        "version": version,
                                        "content_hash": content_hash},
                               actor=created_by, created_at=self._now())
            view = self._def_view(row, parsed)
            view["deduped"] = False
            return view

    def list_definitions(self) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM sig_definitions ORDER BY definition_code,version"
            ).fetchall()
            return [self._def_view(r) for r in rows]

    def get_definition(self, definition_id: str) -> dict:
        with self._conn() as conn:
            row, cfg = self._def_row(conn, definition_id)
            return self._def_view(row, cfg)

    def _def_row(self, conn, did):
        row = conn.execute("SELECT * FROM sig_definitions WHERE id=?",
                           (did,)).fetchone()
        if not row:
            raise DomainError("NOT_FOUND", f"监测定义不存在: {did}", 404)
        return row, json.loads(row["config_json"])

    @staticmethod
    def _def_view(row, parsed: dict | None = None) -> dict:
        cfg = parsed or json.loads(row["config_json"])
        return {"definition_id": row["id"],
                "definition_code": row["definition_code"],
                "version": row["version"], "config": cfg,
                "content_hash": row["content_hash"],
                "created_by": row["created_by"], "created_at": row["created_at"]}

    # ---------------------------------------------------------------- 病例版本
    def add_case_version(self, *, case_id: str, version_no: int | None = None,
                         change_type: str = "INTAKE", source_id: str | None = None,
                         drugs: list | None = None, events: list | None = None,
                         causality: dict | None = None, report_date: str,
                         merged_from: list | None = None, split_from: str | None = None,
                         active: bool = True) -> dict:
        """录入（或从病例库装载）一个只增病例版本。

        version_no 缺省按病例自增。事件以 *原文* 存储，运行时按冻结术语版本
        归一；drugs 存规范药品码（或在定义里登记的码）。
        """
        if change_type not in ("INTAKE", "FOLLOWUP", "MERGE", "SPLIT",
                               "WITHDRAW", "CAUSALITY"):
            raise DomainError("BAD_FIELD", f"未知变更类型 {change_type}")
        source_id = source_id or case_id
        with self._lock, self._conn() as conn:
            if version_no is None:
                version_no = conn.execute(
                    "SELECT COALESCE(MAX(version_no),0)+1 v FROM sig_case_versions "
                    "WHERE case_id=?", (case_id,)).fetchone()["v"]
            vid = _uid()
            try:
                conn.execute(
                    "INSERT INTO sig_case_versions(id,case_id,version_no,change_type,"
                    "source_id,merged_from_json,split_from,drugs_json,events_json,"
                    "causality_json,report_date,active_case,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (vid, case_id, version_no, change_type, source_id,
                     _json(sorted(str(x) for x in (merged_from or []))),
                     split_from,
                     _json(sorted(str(x) for x in (drugs or []))),
                     _json([str(x) for x in (events or [])]),
                     _json(causality or {}), report_date, 1 if active else 0,
                     self._now()))
            except Exception as e:
                raise DomainError("VERSION_CONFLICT",
                                  f"病例 {case_id} 版本号 {version_no} 已存在",
                                  409) from e
            return {"version_id": vid, "case_id": case_id, "version_no": version_no,
                    "change_type": change_type, "source_id": source_id}

    # ------------------------------------------------------------- 运行提交
    def computation_key(self, *, definition_id: str, cutoff: str,
                        term_version_id: str, mode: str,
                        base_run_id: str | None) -> str:
        raw = f"{definition_id}|{cutoff}|{term_version_id}|{mode}|{base_run_id or ''}"
        return eng.digest({"computation": raw})[:32]

    def submit_run(self, *, definition_id: str, cutoff: str,
                   term_version_id: str, mode: str = "FULL",
                   base_run_id: str | None = None, shard_count: int | None = None,
                   requested_by: str | None = None) -> dict:
        """提交一次窗口计算（幂等：同一计算键并发只产生一个作业）。"""
        if mode not in ("FULL", "INCREMENTAL"):
            raise DomainError("BAD_FIELD", "mode 必须是 FULL/INCREMENTAL")
        shard_count = int(shard_count or self.default_shard_count)
        if shard_count < 1:
            raise DomainError("BAD_FIELD", "shard_count 必须 >=1")
        key = self.computation_key(definition_id=definition_id, cutoff=cutoff,
                                   term_version_id=term_version_id, mode=mode,
                                   base_run_id=base_run_id)
        with self._lock, self._conn() as conn:
            existing = conn.execute("SELECT * FROM sig_runs WHERE computation_key=?",
                                    (key,)).fetchone()
            if existing:
                return self._run_view(existing, deduped=True)

            _, cfg = self._def_row(conn, definition_id)
            self._term_row(conn, term_version_id)
            if mode == "INCREMENTAL" and not base_run_id:
                raise DomainError("BAD_FIELD", "INCREMENTAL 运行必须提供 base_run_id")
            if base_run_id:
                base = conn.execute("SELECT * FROM sig_runs WHERE id=?",
                                    (base_run_id,)).fetchone()
                if not base:
                    raise DomainError("NOT_FOUND",
                                      f"基线运行不存在: {base_run_id}", 404)
                if base["definition_id"] != definition_id:
                    raise DomainError("BAD_FIELD", "基线运行的定义必须一致")

            # 冻结病例快照：截止线（按版本落库时间）内每病例的最新版本
            rows = conn.execute(
                "SELECT v.* FROM sig_case_versions v JOIN ("
                "  SELECT case_id, MAX(version_no) mx FROM sig_case_versions "
                "  WHERE created_at <= ? GROUP BY case_id"
                ") t ON v.case_id=t.case_id AND v.version_no=t.mx "
                "WHERE v.created_at <= ? ORDER BY v.case_id",
                (cutoff, cutoff)).fetchall()
            input_fp = eng.digest({
                "cutoff": cutoff, "term": term_version_id, "definition": definition_id,
                "cases": [{"case_id": r["case_id"], "version_id": r["id"],
                           "version_no": r["version_no"],
                           "created_at": r["created_at"]} for r in rows]})

            run_id = _uid()
            conn.execute(
                "INSERT INTO sig_runs(id,computation_key,definition_id,mode,cutoff,"
                "term_version_id,base_run_id,shard_count,shard_size,input_fingerprint,"
                "status,requested_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (run_id, key, definition_id, mode, cutoff, term_version_id,
                 base_run_id, shard_count, self.default_shard_size, input_fp,
                 "PENDING", requested_by, self._now()))
            for r in rows:
                conn.execute(
                    "INSERT INTO sig_run_case_snapshot(run_id,case_id,version_id,"
                    "version_no) VALUES(?,?,?,?)",
                    (run_id, r["case_id"], r["id"], r["version_no"]))
            for shard_no in range(shard_count):
                conn.execute(
                    "INSERT INTO sig_run_shards(id,run_id,shard_no,status) "
                    "VALUES(?,?,?,?)", (_uid(), run_id, shard_no, "PENDING"))
            audit.append_event(
                conn, "RUN_SUBMITTED",
                f"提交信号计算 {mode} cutoff={cutoff} 分片={shard_count}",
                payload={"run_id": run_id, "computation_key": key,
                         "definition_id": definition_id, "cutoff": cutoff,
                         "term_version_id": term_version_id, "mode": mode,
                         "base_run_id": base_run_id, "frozen_cases": len(rows),
                         "input_fingerprint": input_fp},
                actor=requested_by, idempotency_key=key, created_at=self._now())
            run = conn.execute("SELECT * FROM sig_runs WHERE id=?",
                               (run_id,)).fetchone()
            return self._run_view(run, deduped=False)

    def list_runs(self, *, definition_id: str | None = None,
                  status: str | None = None) -> list[dict]:
        sql, params = "SELECT * FROM sig_runs", []
        where = []
        if definition_id:
            where.append("definition_id=?")
            params.append(definition_id)
        if status:
            where.append("status=?")
            params.append(status)
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY created_at"
        with self._conn() as conn:
            return [self._run_view(r) for r in conn.execute(sql, params).fetchall()]

    def get_run(self, run_id: str) -> dict:
        with self._conn() as conn:
            return self._run_view(self._run_row(conn, run_id))

    def _run_row(self, conn, run_id):
        row = conn.execute("SELECT * FROM sig_runs WHERE id=?",
                           (run_id,)).fetchone()
        if not row:
            raise DomainError("NOT_FOUND", f"运行不存在: {run_id}", 404)
        return row

    def _run_view(self, row, *, deduped: bool = False) -> dict:
        d = {"run_id": row["id"], "computation_key": row["computation_key"],
             "definition_id": row["definition_id"], "mode": row["mode"],
             "cutoff": row["cutoff"], "term_version_id": row["term_version_id"],
             "base_run_id": row["base_run_id"],
             "shard_count": row["shard_count"],
             "committed_shards": row["committed_shards"],
             "input_fingerprint": row["input_fingerprint"], "status": row["status"],
             "error_code": row["error_code"], "error_message": row["error_message"],
             "requested_by": row["requested_by"], "created_at": row["created_at"],
             "started_at": row["started_at"], "published_at": row["published_at"]}
        if deduped:
            d["deduped"] = True
        return d

    # ------------------------------------------------------------- 冻结输入
    def _frozen_inputs(self, conn, run_row):
        """读取该运行冻结的病例版本 + 术语映射 + 定义配置。"""
        _, cfg = self._def_row(conn, run_row["definition_id"])
        term = self._term_row(conn, run_row["term_version_id"])
        mappings = json.loads(term["mappings_json"])
        rows = conn.execute(
            "SELECT v.* FROM sig_run_case_snapshot s JOIN sig_case_versions v "
            "ON v.id=s.version_id WHERE s.run_id=? ORDER BY s.case_id",
            (run_row["id"],)).fetchall()
        return rows, mappings, cfg

    # ------------------------------------------------------------- 分片执行
    def run_shards(self, run_id: str, *, max_shards: int | None = None) -> dict:
        """执行未提交分片（每片独立短事务；崩溃后重跑安全，已提交片跳过）。

        冻结输入与解析在只读连接上完成（结果完全确定性），每个分片在自己的
        IMMEDIATE 事务里提交检查点；钩子注入故障时该分片 FAILED 且整作业
        FAILED，已提交片保留、不发布任何半成品。
        """
        processed = 0
        with self._lock:
            read_conn = self._conn()
            try:
                run = self._run_row(read_conn, run_id)
                if run["status"] == "PUBLISHED":
                    return self._run_view(run)
                rows, mappings, cfg = self._frozen_inputs(read_conn, run)
                pending_shards = [r["shard_no"] for r in read_conn.execute(
                    "SELECT shard_no FROM sig_run_shards WHERE run_id=? AND "
                    "status!='COMMITTED' ORDER BY shard_no", (run_id,)).fetchall()]
            finally:
                read_conn.close()

            # 标记作业开始（幂等；已是 RUNNING/FAILED 无妨）
            mark = self._conn()
            try:
                with db.transaction(mark):
                    mark.execute(
                        "UPDATE sig_runs SET status='RUNNING',started_at=COALESCE("
                        "started_at,?) WHERE id=? AND status IN ('PENDING','FAILED')",
                        (self._now(), run_id))
            finally:
                mark.close()

            states = eng.resolve_states(rows, mappings, cfg)
            all_artifacts = eng.build_shard_artifacts(
                states, cfg, run["shard_count"])

            for shard_no in pending_shards:
                if max_shards is not None and processed >= max_shards:
                    break
                artifact = all_artifacts[shard_no]
                try:
                    if self.shard_fault_hook:
                        self.shard_fault_hook(run_id, shard_no, artifact)
                    self._commit_shard(run_id, shard_no, artifact)
                    processed += 1
                except _ShardFault as f:
                    self._fail_shard(run_id, shard_no, f)
                    raise

        view = self.get_run(run_id)
        if view["committed_shards"] == view["shard_count"] and view["status"] != "PUBLISHED":
            self._publish_if_ready(run_id)
            view = self.get_run(run_id)
        return view

    def _commit_shard(self, run_id, shard_no, artifact):
        conn = self._conn()
        try:
            with db.transaction(conn):
                shard_pk = conn.execute(
                    "SELECT id,status FROM sig_run_shards WHERE run_id=? AND shard_no=?",
                    (run_id, shard_no)).fetchone()
                if shard_pk["status"] == "COMMITTED":
                    return
                conn.execute(
                    "UPDATE sig_run_shards SET status='COMMITTED',counts_json=?,"
                    "roster_json=?,checksum=?,committed_at=? WHERE id=?",
                    (_json(artifact["counts"]),
                     _json({"roster": artifact["roster"],
                            "states": artifact["states"]}),
                     artifact["checksum"], self._now(), shard_pk["id"]))
                conn.execute(
                    "UPDATE sig_runs SET committed_shards=("
                    "SELECT COUNT(*) FROM sig_run_shards WHERE run_id=? AND "
                    "status='COMMITTED'), status='RUNNING' WHERE id=?",
                    (run_id, run_id))
        finally:
            conn.close()

    def _fail_shard(self, run_id, shard_no, fault):
        conn = self._conn()
        try:
            with db.transaction(conn):
                conn.execute(
                    "UPDATE sig_run_shards SET status='FAILED',error_code=?,"
                    "error_message=? WHERE run_id=? AND shard_no=?",
                    (fault.code, str(fault), run_id, shard_no))
                conn.execute(
                    "UPDATE sig_runs SET status='FAILED',error_code=?,error_message=?",
                    (fault.code, f"分片 {shard_no} 失败: {fault}"))
                audit.append_event(
                    conn, "RUN_SHARD_FAILED",
                    f"运行 {run_id[:8]} 分片 {shard_no} 失败",
                    payload={"run_id": run_id, "shard_no": shard_no,
                             "error_code": fault.code},
                    created_at=self._now())
        finally:
            conn.close()

    def run_until_published(self, run_id: str, *, max_shards: int | None = None):
        """持续执行分片直到发布；全部提交后在单事务内原子发布。"""
        view = self.get_run(run_id)
        while view["status"] not in ("PUBLISHED", "FAILED"):
            before = view["committed_shards"]
            view = self.run_shards(run_id, max_shards=max_shards)
            # 没有任何待处理分片却仍未发布（理论上不该发生）时停止避免空转
            if view["committed_shards"] == before and \
                    view["committed_shards"] != view["shard_count"]:
                break
        return view

    # ------------------------------------------------------------- 发布
    def _publish_if_ready(self, run_id: str):
        with self._lock, self._conn() as conn:
            run = self._run_row(conn, run_id)
            if run["status"] == "PUBLISHED":
                return
            total_shards = conn.execute(
                "SELECT COUNT(*) c FROM sig_run_shards WHERE run_id=? AND "
                "status='COMMITTED'", (run_id,)).fetchone()["c"]
            if total_shards != run["shard_count"]:
                return
            self._publish(conn, run)

    def _load_committed_artifacts(self, conn, run):
        rows = conn.execute(
            "SELECT shard_no,counts_json,roster_json,checksum FROM sig_run_shards "
            "WHERE run_id=? AND status='COMMITTED' ORDER BY shard_no",
            (run["id"],)).fetchall()
        artifacts = []
        for r in rows:
            payload = json.loads(r["roster_json"])
            artifacts.append({"shard_no": r["shard_no"],
                              "counts": json.loads(r["counts_json"]),
                              "roster": payload["roster"],
                              "states": payload["states"],
                              "checksum": r["checksum"]})
        return artifacts

    def _previous_published(self, conn, run):
        """同定义下、cutoff 更早的最近一次已发布运行（增量基线或全量前驱）。"""
        if run["mode"] == "INCREMENTAL" and run["base_run_id"]:
            base = conn.execute("SELECT * FROM sig_runs WHERE id=? AND status='PUBLISHED'",
                                (run["base_run_id"],)).fetchone()
            if base:
                return base
        return conn.execute(
            "SELECT * FROM sig_runs WHERE definition_id=? AND status='PUBLISHED' "
            "AND cutoff<=? AND id!=? ORDER BY cutoff DESC, created_at DESC LIMIT 1",
            (run["definition_id"], run["cutoff"], run["id"])).fetchone()

    def _publish(self, conn, run):
        """单 IMMEDIATE 事务：汇总 -> 追加修订 -> 清单 -> 差异 -> 贡献台账。"""
        _, cfg = self._def_row(conn, run["definition_id"])
        artifacts = self._load_committed_artifacts(conn, run)
        total = eng.aggregate(artifacts, cfg)

        # 合并花名册 / states（窗口 -> pair -> cell -> sources）
        roster_windows, states_windows = {}, {}
        excluded_windows = {}
        for w in cfg["windows"]:
            roster_windows[w["id"]] = {pair: {"a": [], "b": [], "c": [], "d": []}
                                       for pair in total}
            states_windows[w["id"]] = {}
            excluded_windows[w["id"]] = {}
        for art in artifacts:
            for wid, pairs in art["roster"].items():
                exc = pairs.get("__excluded__", [])
                bucket = excluded_windows[wid]
                for item in exc:
                    bucket[item["source_id"]] = item
                for pair, cells in pairs.items():
                    if pair == "__excluded__":
                        continue
                    for cell, srcs in cells.items():
                        roster_windows[wid][pair][cell].extend(srcs)
            for wid, sm in art["states"].items():
                states_windows[wid].update(sm)

        prev_run = self._previous_published(conn, run)
        prev_artifacts = self._load_committed_artifacts(conn, prev_run) if prev_run else []
        prev_states_windows = {w["id"]: {} for w in cfg["windows"]}
        prev_roster_windows = {w["id"]: {pair: {"a": [], "b": [], "c": [], "d": []}
                                         for pair in total}
                               for w in cfg["windows"]}
        for art in prev_artifacts:
            for wid, sm in art["states"].items():
                if wid in prev_states_windows:
                    prev_states_windows[wid].update(sm)
            for wid, pairs in art["roster"].items():
                if wid not in prev_roster_windows:
                    continue
                for pair, cells in pairs.items():
                    if pair == "__excluded__":
                        continue
                    if pair not in prev_roster_windows[wid]:
                        continue
                    for cell in ("a", "b", "c", "d"):
                        prev_roster_windows[wid][pair][cell].extend(cells.get(cell, []))

        created_at = self._now()
        new_revision_ids = []  # (rev_id, pair, window) for roster
        with db.transaction(conn):
            for w in cfg["windows"]:
                for pair in sorted(total):
                    dg_name, eg_name = pair.split("|", 1)
                    cells = total[pair]
                    m = eng.evaluate_pair(cells, cfg)
                    next_rev = conn.execute(
                        "SELECT COALESCE(MAX(revision_no),0)+1 v FROM sig_signal_revisions "
                        "WHERE definition_id=? AND window_id=? AND drug_group=? AND "
                        "event_group=?",
                        (run["definition_id"], w["id"], dg_name, eg_name)
                    ).fetchone()["v"]
                    prev_rev_row = conn.execute(
                        "SELECT * FROM sig_signal_revisions WHERE definition_id=? AND "
                        "window_id=? AND drug_group=? AND event_group=? ORDER BY "
                        "revision_no DESC LIMIT 1",
                        (run["definition_id"], w["id"], dg_name, eg_name)
                    ).fetchone()
                    criteria = eng.criteria_from_config(cfg)
                    content_hash = eng.digest({
                        "run": run["id"], "window": w["id"], "pair": pair,
                        "cells": cells, "metrics": _metrics_for_hash(m)})
                    rev_id = _uid()
                    conn.execute(
                        "INSERT INTO sig_signal_revisions(id,run_id,definition_id,"
                        "window_id,window_start,window_end,drug_group,event_group,"
                        "revision_no,a,b,c,d,n,prr,prr_ci_low,prr_ci_high,ror,"
                        "ror_ci_low,ror_ci_high,chi_square,determination,criteria_json,"
                        "content_hash,created_at) VALUES("
                        "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (rev_id, run["id"], run["definition_id"], w["id"],
                         w["start"], w["end"], dg_name, eg_name, next_rev,
                         cells["a"], cells["b"], cells["c"], cells["d"], m["n"],
                         m["prr"], m["prr_ci_low"], m["prr_ci_high"],
                         m["ror"], m["ror_ci_low"], m["ror_ci_high"],
                         m["chi_square"], m["determination"], _json(criteria),
                         content_hash, created_at))
                    new_revision_ids.append((rev_id, w["id"], pair, cells))

                    # 冻结入选病例清单（a/b/c/d）+ 该信号键的窗口内排除痕迹
                    roster = roster_windows[w["id"]][pair]
                    order = {"a": 0, "b": 1, "c": 2, "d": 3}
                    for cell in ("a", "b", "c", "d"):
                        for src in sorted(roster[cell]):
                            attrs = states_windows[w["id"]].get(src, {})
                            conn.execute(
                                "INSERT INTO sig_signal_roster(id,revision_id,source_id,"
                                "case_id,version_id,included,cell,reason,detail_json) "
                                "VALUES(?,?,?,?,?,?,?,?,?)",
                                (_uid(), rev_id, src,
                                 attrs.get("case_id", ""), attrs.get("version_id", ""),
                                 1, cell, None,
                                 _json({"causality": attrs.get("causality", {})})))

                    # ---- 修订差异（相对上一修订）----
                    old_cells = None
                    old_metrics = None
                    if prev_rev_row:
                        old_cells = {k: prev_rev_row[k] for k in ("a", "b", "c", "d")}
                        old_metrics = {k: prev_rev_row[k] for k in (
                            "prr", "prr_ci_low", "prr_ci_high", "ror",
                            "ror_ci_low", "ror_ci_high", "chi_square",
                            "determination")}
                    diff = self._build_diff(
                        pair, cells, m, old_cells, old_metrics,
                        prev_states_windows[w["id"]], states_windows[w["id"]],
                        prev_roster_windows[w["id"]][pair],
                        roster_windows[w["id"]][pair],
                        excluded_windows[w["id"]])
                    conn.execute(
                        "INSERT INTO sig_signal_diffs(id,revision_id,prev_revision_id,"
                        "run_id,prev_run_id,old_counts_json,new_counts_json,"
                        "metrics_delta_json,case_changes_json,created_at) "
                        "VALUES(?,?,?,?,?,?,?,?,?,?)",
                        (_uid(), rev_id,
                         prev_rev_row["id"] if prev_rev_row else None,
                         run["id"], prev_run["id"] if prev_run else None,
                         _json(old_cells), _json(cells),
                         _json(diff["metrics_delta"]), _json(diff["case_changes"]),
                         created_at))

            # ---- 贡献台账：撤销旧窗口贡献 + 写入新贡献（同来源窗口只一条有效）
            if prev_run:
                conn.execute(
                    "UPDATE sig_case_contributions SET revoked=1, revoked_by_run_id=? "
                    "WHERE definition_id=? AND revoked=0 AND window_id IN (%s)"
                    % ",".join("?" * len(cfg["windows"])),
                    [run["id"], run["definition_id"]] + [w["id"] for w in cfg["windows"]])
            for w in cfg["windows"]:
                for src in sorted(states_windows[w["id"]]):
                    attrs = states_windows[w["id"]][src]
                    state = {
                        "eligible": attrs.get("eligible"),
                        "exclude_reason": attrs.get("exclude_reason"),
                        "drug_groups": attrs.get("drug_groups", []),
                        "event_groups": attrs.get("event_groups", []),
                        "causality": attrs.get("causality", {}),
                        "report_date": attrs.get("report_date")}
                    led = eng.source_window_ledger(state, w, cfg)
                    if led is None:
                        continue
                    conn.execute(
                        "INSERT INTO sig_case_contributions(id,definition_id,"
                        "window_id,source_id,case_id,version_id,run_id,"
                        "drug_groups_json,event_groups_json,cells_json,eligible,"
                        "exclude_reason,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (_uid(), run["definition_id"], w["id"], src,
                         attrs["case_id"], attrs["version_id"], run["id"],
                         _json(led["drug_groups"]), _json(led["event_groups"]),
                         _json(led["cells"]), 1 if led["eligible"] else 0,
                         led["exclude_reason"], created_at))

            conn.execute("UPDATE sig_runs SET status='PUBLISHED',published_at=? "
                         "WHERE id=?", (created_at, run["id"]))
            audit.append_event(
                conn, "RUN_PUBLISHED",
                f"发布信号计算 {run['id'][:8]}（{len(new_revision_ids)} 个信号修订）",
                payload={"run_id": run["id"], "mode": run["mode"],
                         "cutoff": run["cutoff"], "prev_run_id":
                         prev_run["id"] if prev_run else None,
                         "signals": _signal_summary(conn, run)},
                actor=run["requested_by"], created_at=created_at)

    @staticmethod
    def _build_diff(pair, new_cells, new_metrics, old_cells, old_metrics,
                    old_states, new_states, old_roster, new_roster_cells,
                    excluded):
        """生成单个信号键的可追踪差异（病例级 + 指标级）。"""
        change = eng.changed_sources(old_states, new_states)
        new_roster_set = {s for cell in ("a", "b", "c", "d")
                          for s in new_roster_cells[cell]}
        old_roster_set = {s for cell in ("a", "b", "c", "d")
                          for s in old_roster[cell]}
        case_changes = []
        for src in change["added"]:
            attrs = new_states.get(src, {})
            case_changes.append({
                "source_id": src, "kind": "NEW_ENTERED" if src in new_roster_set
                else "NEW_EXCLUDED",
                "in_signal": src in new_roster_set,
                "new": attrs,
                "reason": excluded.get(src, {}).get("reason")})
        for src in change["removed"]:
            attrs = old_states.get(src, {})
            case_changes.append({
                "source_id": src,
                "kind": "EXITED_SAMPLE" if attrs.get("eligible") and
                src in old_roster_set else "REMOVED",
                "in_signal_before": src in old_roster_set,
                "old": attrs})
        for ch in change["changed"]:
            src = ch["source_id"]
            before_cell = next((cell for cell in ("a", "b", "c", "d")
                                if src in old_roster.get(cell, [])), None)
            after_cell = next((cell for cell in ("a", "b", "c", "d")
                               if src in new_roster_cells.get(cell, [])), None)
            case_changes.append({
                "source_id": src, "kind": ch["kind"],
                "old_cell": before_cell, "new_cell": after_cell,
                "in_signal_before": src in old_roster_set,
                "in_signal_after": src in new_roster_set,
                "old": ch["old"], "new": ch["new"]})
        # 对该信号键格归属发生变化的来源（跨组体现在 DRUG/EVENT_GROUP_CHANGE）
        metrics_delta = {"determination": None}
        if old_metrics is not None:
            metrics_delta["determination"] = {
                "from": old_metrics["determination"],
                "to": new_metrics["determination"]}
            for k in ("prr", "ror", "chi_square"):
                ov, nv = old_metrics[k], new_metrics[k]
                metrics_delta[k] = {"from": ov, "to": nv,
                                    "delta": (round(nv - ov, 6)
                                              if ov is not None and nv is not None
                                              else None)}
        else:
            metrics_delta["determination"] = {"from": None,
                                              "to": new_metrics["determination"]}
        return {"metrics_delta": metrics_delta,
                "case_changes": sorted(case_changes, key=lambda x: x["source_id"])}

    # ------------------------------------------------------------- 查询：窗口结果
    def window_results(self, definition_id: str, *, window_id: str | None = None,
                       run_id: str | None = None, determination: str | None = None,
                       head_only: bool = True) -> list[dict]:
        """窗口信号结果（默认只看每信号键链头修订；指定 run_id 看该次快照）。"""
        with self._conn() as conn:
            sql = ("SELECT r.* FROM sig_signal_revisions r WHERE r.definition_id=?")
            params = [definition_id]
            if window_id:
                sql += " AND r.window_id=?"
                params.append(window_id)
            if run_id:
                sql += " AND r.run_id=?"
                params.append(run_id)
            if determination:
                sql += " AND r.determination=?"
                params.append(determination)
            if head_only and not run_id:
                sql += (" AND r.revision_no=(SELECT MAX(revision_no) FROM "
                        "sig_signal_revisions h WHERE h.definition_id=r.definition_id "
                        "AND h.window_id=r.window_id AND h.drug_group=r.drug_group "
                        "AND h.event_group=r.event_group)")
            sql += " ORDER BY r.window_id,r.drug_group,r.event_group,r.revision_no"
            return [self._revision_view(r) for r in conn.execute(sql, params).fetchall()]

    def get_revision(self, revision_id: str) -> dict:
        with self._conn() as conn:
            row = self._revision_row(conn, revision_id)
            view = self._revision_view(row)
            view["roster"] = self._roster(conn, revision_id)
            view["diff"] = self._diff_view(conn, revision_id)
            return view

    def _revision_row(self, conn, rev_id):
        row = conn.execute("SELECT * FROM sig_signal_revisions WHERE id=?",
                           (rev_id,)).fetchone()
        if not row:
            raise DomainError("NOT_FOUND", f"信号修订不存在: {rev_id}", 404)
        return row

    @staticmethod
    def _revision_view(r) -> dict:
        return {
            "revision_id": r["id"], "run_id": r["run_id"],
            "definition_id": r["definition_id"], "window_id": r["window_id"],
            "window_start": r["window_start"], "window_end": r["window_end"],
            "drug_group": r["drug_group"], "event_group": r["event_group"],
            "revision_no": r["revision_no"],
            "counts": {"a": r["a"], "b": r["b"], "c": r["c"], "d": r["d"],
                       "n": r["n"]},
            "metrics": {"prr": r["prr"], "prr_ci_low": r["prr_ci_low"],
                        "prr_ci_high": r["prr_ci_high"], "ror": r["ror"],
                        "ror_ci_low": r["ror_ci_low"], "ror_ci_high": r["ror_ci_high"],
                        "chi_square": r["chi_square"]},
            "determination": r["determination"],
            "criteria": json.loads(r["criteria_json"]),
            "content_hash": r["content_hash"],
            "disposition": r["disposition"], "disposition_by": r["disposition_by"],
            "disposition_at": r["disposition_at"],
            "disposition_note": r["disposition_note"], "created_at": r["created_at"]}

    def _roster(self, conn, rev_id) -> dict:
        rows = conn.execute(
            "SELECT * FROM sig_signal_roster WHERE revision_id=? ORDER BY cell,source_id",
            (rev_id,)).fetchall()
        out = {"a": [], "b": [], "c": [], "d": []}
        for r in rows:
            out[r["cell"]].append({
                "source_id": r["source_id"], "case_id": r["case_id"],
                "version_id": r["version_id"], "included": bool(r["included"]),
                "detail": json.loads(r["detail_json"])})
        return out

    def _diff_view(self, conn, rev_id):
        row = conn.execute("SELECT * FROM sig_signal_diffs WHERE revision_id=?",
                           (rev_id,)).fetchone()
        if not row:
            return None
        return {"prev_revision_id": row["prev_revision_id"],
                "prev_run_id": row["prev_run_id"],
                "old_counts": json.loads(row["old_counts_json"]),
                "new_counts": json.loads(row["new_counts_json"]),
                "metrics_delta": json.loads(row["metrics_delta_json"]),
                "case_changes": json.loads(row["case_changes_json"])}

    # ----------------------------------------------------- 病例贡献追踪
    def contribution_trace(self, *, source_id: str | None = None,
                           definition_id: str | None = None,
                           window_id: str | None = None,
                           include_revoked: bool = True) -> list[dict]:
        sql, params = "SELECT * FROM sig_case_contributions", []
        where = []
        if source_id:
            where.append("source_id=?")
            params.append(source_id)
        if definition_id:
            where.append("definition_id=?")
            params.append(definition_id)
        if window_id:
            where.append("window_id=?")
            params.append(window_id)
        if not include_revoked:
            where.append("revoked=0")
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY created_at,source_id"
        with self._conn() as conn:
            return [{"contribution_id": r["id"], "definition_id": r["definition_id"],
                     "window_id": r["window_id"], "source_id": r["source_id"],
                     "case_id": r["case_id"], "version_id": r["version_id"],
                     "run_id": r["run_id"],
                     "drug_groups": json.loads(r["drug_groups_json"]),
                     "event_groups": json.loads(r["event_groups_json"]),
                     "cells": json.loads(r["cells_json"]),
                     "eligible": bool(r["eligible"]),
                     "exclude_reason": r["exclude_reason"],
                     "revoked": bool(r["revoked"]),
                     "revoked_by_run_id": r["revoked_by_run_id"],
                     "created_at": r["created_at"]}
                    for r in conn.execute(sql, params).fetchall()]

    def run_exclusions(self, run_id: str) -> dict:
        """该运行各窗口被排除来源（MERGED_AWAY/WITHDRAWN/UNMAPPED/...）。"""
        with self._conn() as conn:
            run = self._run_row(conn, run_id)
            artifacts = self._load_committed_artifacts(conn, run)
        out = {}
        for art in artifacts:
            for wid, pairs in art["roster"].items():
                out.setdefault(wid, {})
                for item in pairs.get("__excluded__", []):
                    out[wid][item["source_id"]] = item
        return {wid: sorted(items.values(), key=lambda x: x["source_id"])
                for wid, items in out.items()}

    # ------------------------------------------------------------- 处置
    def dispose(self, revision_id: str, disposition: str, *,
                expected_revision_no: int, analyst: str,
                note: str | None = None) -> dict:
        """分析师确认/驳回/观察信号修订。

        - 必须引用该信号键链头修订号；基于过期号操作 -> 409 STALE_REVISION；
        - 已处置的旧修订结论保留，不允许改写 -> 409 ALREADY_DISPOSED；
          新修订是独立行，需要重新处置（PENDING）。
        """
        if disposition not in ("CONFIRMED", "REFUTED", "WATCH"):
            raise DomainError("BAD_FIELD",
                              "disposition 必须是 CONFIRMED/REFUTED/WATCH")
        if not analyst or not str(analyst).strip():
            raise DomainError("BLANK_ANALYST", "缺少分析师身份")
        with self._lock, self._conn() as conn:
            row = self._revision_row(conn, revision_id)
            head = conn.execute(
                "SELECT revision_no,id FROM sig_signal_revisions WHERE "
                "definition_id=? AND window_id=? AND drug_group=? AND event_group=? "
                "ORDER BY revision_no DESC LIMIT 1",
                (row["definition_id"], row["window_id"], row["drug_group"],
                 row["event_group"])).fetchone()
            if int(expected_revision_no) != head["revision_no"]:
                raise DomainError(
                    "STALE_REVISION",
                    f"过期修订号 {expected_revision_no}：链头已到 "
                    f"{head['revision_no']}（修订 {head['id']}），请基于最新修订处置",
                    409)
            if row["id"] != head["id"]:
                raise DomainError("STALE_REVISION",
                                  "该修订不是链头修订，不能处置（旧结论保留）", 409)
            if row["disposition"] != "PENDING":
                raise DomainError(
                    "ALREADY_DISPOSED",
                    f"修订已被 {row['disposition_by']} 处置为 {row['disposition']}，"
                    "结论保留不可改写；新修订需重新处置", 409)
            ts = self._now()
            with db.transaction(conn):
                conn.execute(
                    "UPDATE sig_signal_revisions SET disposition=?,disposition_by=?,"
                    "disposition_at=?,disposition_note=? WHERE id=?",
                    (disposition, analyst, ts, note, revision_id))
                audit.append_event(
                    conn, "SIGNAL_DISPOSITION",
                    f"{analyst} {disposition} 信号 "
                    f"{row['drug_group']}/{row['event_group']} rev{row['revision_no']}",
                    payload={"revision_id": revision_id,
                             "run_id": row["run_id"],
                             "window_id": row["window_id"],
                             "drug_group": row["drug_group"],
                             "event_group": row["event_group"],
                             "revision_no": row["revision_no"],
                             "disposition": disposition, "note": note},
                    actor=analyst, created_at=ts)
            new_row = conn.execute("SELECT * FROM sig_signal_revisions WHERE id=?",
                                   (revision_id,)).fetchone()
            return self._revision_view(new_row)

    def list_dispositions(self, definition_id: str, *,
                          disposition: str | None = None) -> list[dict]:
        sql = ("SELECT * FROM sig_signal_revisions WHERE definition_id=? "
               "AND disposition!='PENDING'")
        params = [definition_id]
        if disposition:
            sql += " AND disposition=?"
            params.append(disposition)
        sql += " ORDER BY disposition_at"
        with self._conn() as conn:
            return [self._revision_view(r)
                    for r in conn.execute(sql, params).fetchall()]

    def revision_diffs(self, definition_id: str, *, window_id: str | None = None,
                       drug_group: str | None = None, event_group: str | None = None,
                       limit: int = 200) -> list[dict]:
        sql = ("SELECT d.*, r.drug_group,r.event_group,r.window_id,r.revision_no "
               "FROM sig_signal_diffs d JOIN sig_signal_revisions r "
               "ON r.id=d.revision_id WHERE r.definition_id=?")
        params = [definition_id]
        if window_id:
            sql += " AND r.window_id=?"
            params.append(window_id)
        if drug_group:
            sql += " AND r.drug_group=?"
            params.append(drug_group)
        if event_group:
            sql += " AND r.event_group=?"
            params.append(event_group)
        sql += " ORDER BY r.revision_no DESC LIMIT ?"
        params.append(limit)
        with self._conn() as conn:
            out = []
            for r in conn.execute(sql, params).fetchall():
                out.append({
                    "revision_id": r["revision_id"],
                    "prev_revision_id": r["prev_revision_id"],
                    "run_id": r["run_id"], "prev_run_id": r["prev_run_id"],
                    "window_id": r["window_id"],
                    "drug_group": r["drug_group"], "event_group": r["event_group"],
                    "revision_no": r["revision_no"],
                    "old_counts": json.loads(r["old_counts_json"]),
                    "new_counts": json.loads(r["new_counts_json"]),
                    "metrics_delta": json.loads(r["metrics_delta_json"]),
                    "case_changes": json.loads(r["case_changes_json"])})
            return out

    # ------------------------------------------------------------- 恢复
    def recover(self) -> dict:
        """崩溃恢复：RUNNING 但未发布的作业保持可续跑（分片是检查点）。

        已 COMMITTED 的分片不重算；PENDING/FAILED 分片在下次 run 时补跑。
        返回每个非终态运行的续跑进度。
        """
        with self._lock, self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM sig_runs WHERE status IN ('PENDING','RUNNING','FAILED')"
            ).fetchall()
            result = {"recoverable": [], "failed": []}
            for r in rows:
                committed = conn.execute(
                    "SELECT COUNT(*) c FROM sig_run_shards WHERE run_id=? AND "
                    "status='COMMITTED'", (r["id"],)).fetchone()["c"]
                failed_shards = [s["shard_no"] for s in conn.execute(
                    "SELECT shard_no FROM sig_run_shards WHERE run_id=? AND "
                    "status='FAILED' ORDER BY shard_no", (r["id"],)).fetchall()]
                # FAILED 分片重置为 PENDING 以便续跑（检查点之后的补算）
                if failed_shards:
                    with db.transaction(conn):
                        conn.execute(
                            "UPDATE sig_run_shards SET status='PENDING',error_code=NULL,"
                            "error_message=NULL WHERE run_id=? AND status='FAILED'",
                            (r["id"],))
                        conn.execute(
                            "UPDATE sig_runs SET status='RUNNING',error_code=NULL,"
                            "error_message=NULL WHERE id=?", (r["id"],))
                        audit.append_event(
                            conn, "RUN_RECOVERED",
                            f"恢复运行 {r['id'][:8]}：已提交 {committed}/"
                            f"{r['shard_count']}，重置失败分片 {failed_shards}",
                            payload={"run_id": r["id"], "committed": committed,
                                     "reset_shards": failed_shards},
                            created_at=self._now())
                entry = {"run_id": r["id"], "status": "RUNNING" if failed_shards
                         else r["status"],
                         "committed_shards": committed,
                         "shard_count": r["shard_count"],
                         "reset_failed_shards": failed_shards}
                (result["failed"] if failed_shards else result["recoverable"]
                 ).append(entry)
            return result

    def resume_pending(self, *, max_shards: int | None = None) -> list[dict]:
        runs = self.recover()
        todo = runs["recoverable"] + runs["failed"]
        out = []
        for item in todo:
            out.append(self.run_until_published(item["run_id"],
                                                max_shards=max_shards))
        return out

    # ------------------------------------------------------------- 审计
    def audit_trail(self, *, event_type: str | None = None,
                    limit: int = 500) -> list[dict]:
        with self._conn() as conn:
            return audit.list_events(conn, event_type=event_type, limit=limit)

    def verify_audit_chain(self) -> dict:
        with self._conn() as conn:
            return audit.verify_chain(conn)

    # ------------------------------------------------------------- 复算核验
    def verify_run_reproducibility(self, run_id: str) -> dict:
        """只读重算：重新解析冻结输入并与已提交分片/发布指标比对。"""
        with self._conn() as conn:
            run = self._run_row(conn, run_id)
            rows, mappings, cfg = self._frozen_inputs(conn, run)
            artifacts = self._load_committed_artifacts(conn, run)
        states = eng.resolve_states(rows, mappings, cfg)
        rebuilt = eng.build_shard_artifacts(states, cfg, run["shard_count"])
        failures = []
        for art in artifacts:
            fresh = rebuilt[art["shard_no"]]
            if art["checksum"] != fresh["checksum"]:
                failures.append(f"分片 {art['shard_no']} 校验和不一致")
            if art["counts"] != fresh["counts"]:
                failures.append(f"分片 {art['shard_no']} 四格计数不一致")
        return {"run_id": run_id, "valid": not failures, "failures": failures,
                "status": run["status"],
                "input_fingerprint": run["input_fingerprint"]}


class _ShardFault(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def _metrics_for_hash(m: dict) -> dict:
    return {k: m[k] for k in ("a", "b", "c", "d", "n", "prr", "prr_ci_low",
                              "prr_ci_high", "ror", "ror_ci_low", "ror_ci_high",
                              "chi_square", "determination")}


def _signal_summary(conn, run) -> dict:
    rows = conn.execute(
        "SELECT determination,COUNT(*) c FROM sig_signal_revisions WHERE run_id=? "
        "GROUP BY determination", (run["id"],)).fetchall()
    return {r["determination"]: r["c"] for r in rows}
