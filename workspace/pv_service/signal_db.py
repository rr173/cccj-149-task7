"""药物安全信号窗口计算子系统的 SQLite 持久化。

与既有病例库（pv.db）分离，放在同一数据目录的 ``signal.db``：病例库负责
病例接收/版本/合并拆分，信号库只读引用其 *已冻结* 的版本快照，不反向改动
病例库，也不需要对既有库做结构迁移。

关键持久化不变量（由数据库约束而不是仅由应用代码保证）：

1. sig_terms / sig_definitions / sig_case_versions 只增（触发器拒绝改删）；
   术语归一与监测定义一经发布即冻结为新版本，永不就地修改。
2. sig_runs.computation_key UNIQUE —— 并发/重复提交同一计算键只有一个作业。
3. sig_run_shards(run_id,shard_no) UNIQUE + COMMITTED 分片独立短事务提交，
   崩溃后只补未提交分片；发布要求全部分片 COMMITTED，失败不发布半成品。
4. sig_signal_revisions 只增：发布只能为每个 (定义,窗口,药品组,事件组) 追加
   一个新修订号；触发器只放行处置列（确认/驳回/观察）的状态机更新，
   指标/四格计数等计算列永不被后台计算覆盖。
5. sig_case_contributions 是 *只增* 的贡献台账：新版本进入后续窗口时
   旧行 revoked=1 并插入新行；部分唯一索引保证同一 (窗口,来源) 至多一条
   有效贡献 —— “同一来源在一个窗口中只贡献一次”。
6. sig_audit_events 是全局只增哈希链（seq/prev_hash/entry_hash）。
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

--------------------------------------------------------------------------------
-- 术语归一版本：一次运行冻结一个术语版本，事件原文经该版本映射到规范事件码。
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sig_terms (
    id             TEXT PRIMARY KEY,            -- 内部行 id（hex）
    term_version   TEXT NOT NULL,               -- 逻辑版本号（如 2026Q1@1）
    mappings_json  TEXT NOT NULL,               -- {事件原文(规范键): 规范事件码}
    content_hash   TEXT NOT NULL UNIQUE,        -- 相同内容重复登记返回原版本
    note           TEXT NOT NULL DEFAULT '',
    created_by     TEXT,
    created_at     TEXT NOT NULL,
    UNIQUE(term_version)
);
CREATE INDEX IF NOT EXISTS ix_sig_terms_time ON sig_terms(created_at);

--------------------------------------------------------------------------------
-- 监测定义：药品集合、事件分组、观察期、最小样本量、PRR/ROR 阈值。
-- 内容冻结；修改口径 = 新建一个定义（新 definition_code 或新版本行）。
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sig_definitions (
    id              TEXT PRIMARY KEY,
    definition_code TEXT NOT NULL,              -- 逻辑编码（分析师命名）
    version         INTEGER NOT NULL DEFAULT 1,
    config_json     TEXT NOT NULL,              -- 完整冻结配置（见 signal_service）
    content_hash    TEXT NOT NULL UNIQUE,       -- 相同内容重复创建返回原定义
    created_by      TEXT,
    created_at      TEXT NOT NULL,
    UNIQUE(definition_code, version)
);

--------------------------------------------------------------------------------
-- 病例版本（从病例库只增投影；信号子系统的权威病例输入）。
-- 直接通过信号服务录入或从 pv.db 装载，均在此冻结；触发器拒绝改删。
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sig_case_versions (
    id              TEXT PRIMARY KEY,
    case_id         TEXT NOT NULL,
    version_no      INTEGER NOT NULL,
    change_type     TEXT NOT NULL
                    CHECK (change_type IN ('INTAKE','FOLLOWUP','MERGE','SPLIT',
                                           'WITHDRAW','CAUSALITY')),
    source_id       TEXT NOT NULL,              -- 本版本归属来源（病例聚合单位）
    merged_from_json TEXT NOT NULL DEFAULT '[]',-- MERGE：被吸收的来源列表
    split_from      TEXT,                       -- SPLIT：拆出方来源
    drugs_json      TEXT NOT NULL DEFAULT '[]', -- ["X",...] 规范药品码
    events_json     TEXT NOT NULL DEFAULT '[]', -- ["HEP",...] 规范事件码
    causality_json  TEXT NOT NULL DEFAULT '{}', -- {事件码: "CERTAIN"/...}
    report_date     TEXT NOT NULL,              -- 报告/事件计入日期(ISO 日期)
    active_case     INTEGER NOT NULL DEFAULT 1, -- 0 = 已退出样本（被吸收/撤回）
    created_at      TEXT NOT NULL,              -- 版本落库时间（截止线过滤用）
    UNIQUE(case_id, version_no)
);
CREATE INDEX IF NOT EXISTS ix_sigcv_cut ON sig_case_versions(created_at);
CREATE INDEX IF NOT EXISTS ix_sigcv_case ON sig_case_versions(case_id, version_no);

--------------------------------------------------------------------------------
-- 计算作业（一次“可复演窗口计算”）。
-- computation_key 由 (定义, 病例截止线, 术语版本, 模式, 基线运行) 确定性导出，
-- UNIQUE 兜底：并发提交同一计算键只产生一个作业。
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sig_runs (
    id                  TEXT PRIMARY KEY,
    computation_key     TEXT NOT NULL UNIQUE,
    definition_id       TEXT NOT NULL REFERENCES sig_definitions(id),
    mode                TEXT NOT NULL CHECK (mode IN ('FULL','INCREMENTAL')),
    cutoff              TEXT NOT NULL,          -- 病例版本截止线(ISO)
    term_version_id     TEXT NOT NULL REFERENCES sig_terms(id),
    base_run_id         TEXT REFERENCES sig_runs(id),
    shard_count         INTEGER NOT NULL,
    shard_size          INTEGER NOT NULL,
    input_fingerprint   TEXT NOT NULL,          -- 冻结输入整体指纹
    status              TEXT NOT NULL DEFAULT 'PENDING'
                        CHECK (status IN ('PENDING','RUNNING','COMMITTED',
                                          'PUBLISHED','FAILED')),
    committed_shards    INTEGER NOT NULL DEFAULT 0,
    error_code          TEXT,
    error_message       TEXT,
    requested_by        TEXT,
    created_at          TEXT NOT NULL,
    started_at          TEXT,
    published_at        TEXT
);
CREATE INDEX IF NOT EXISTS ix_sigruns_status ON sig_runs(status);
CREATE INDEX IF NOT EXISTS ix_sigruns_def ON sig_runs(definition_id, created_at);

-- 运行在提交时冻结的病例版本快照（每病例选中的版本）。重放只看这张表，
-- 截止线之后新到的病例版本绝不混入。
CREATE TABLE IF NOT EXISTS sig_run_case_snapshot (
    run_id          TEXT NOT NULL REFERENCES sig_runs(id),
    case_id         TEXT NOT NULL,
    version_id      TEXT NOT NULL REFERENCES sig_case_versions(id),
    version_no      INTEGER NOT NULL,
    PRIMARY KEY (run_id, case_id)
);

-- 分片：每片独立短事务把计数/花名册/台账落库后置 COMMITTED（检查点）。
CREATE TABLE IF NOT EXISTS sig_run_shards (
    id            TEXT PRIMARY KEY,
    run_id        TEXT NOT NULL REFERENCES sig_runs(id),
    shard_no      INTEGER NOT NULL,
    status        TEXT NOT NULL DEFAULT 'PENDING'
                  CHECK (status IN ('PENDING','COMMITTED','FAILED')),
    counts_json   TEXT NOT NULL DEFAULT '{}',   -- {(drug_grp|event_grp): {a,b,c,d}}
    roster_json   TEXT NOT NULL DEFAULT '{}',   -- 每信号键的入选/排除来源明细
    checksum      TEXT,                         -- 本分片结果指纹
    error_code    TEXT,
    error_message TEXT,
    committed_at  TEXT,
    UNIQUE(run_id, shard_no)
);

--------------------------------------------------------------------------------
-- 信号修订：发布一次追加一组（笛卡尔积 药品组×事件组 每个键一条）。
-- 指标列与四格计数永不更新（触发器只放行处置状态机列）。
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sig_signal_revisions (
    id                TEXT PRIMARY KEY,
    run_id            TEXT NOT NULL REFERENCES sig_runs(id),
    definition_id     TEXT NOT NULL REFERENCES sig_definitions(id),
    window_id         TEXT NOT NULL,            -- 观察期标识（定义内）
    window_start      TEXT NOT NULL,
    window_end        TEXT NOT NULL,
    drug_group        TEXT NOT NULL,
    event_group       TEXT NOT NULL,
    revision_no       INTEGER NOT NULL,         -- 该信号键窗口链上的单调修订号
    a INTEGER NOT NULL, b INTEGER NOT NULL,
    c INTEGER NOT NULL, d INTEGER NOT NULL,
    n INTEGER NOT NULL,
    prr               REAL,
    prr_ci_low        REAL,
    prr_ci_high       REAL,
    ror               REAL,
    ror_ci_low        REAL,
    ror_ci_high       REAL,
    chi_square        REAL,
    determination     TEXT NOT NULL
                      CHECK (determination IN ('SIGNAL','NO_SIGNAL','INSUFFICIENT')),
    criteria_json     TEXT NOT NULL,
    content_hash      TEXT NOT NULL,            -- 本修订计算内容指纹（只增校验）
    disposition       TEXT NOT NULL DEFAULT 'PENDING'
                      CHECK (disposition IN ('PENDING','CONFIRMED','REFUTED','WATCH')),
    disposition_by    TEXT,
    disposition_at    TEXT,
    disposition_note  TEXT,
    created_at        TEXT NOT NULL,
    UNIQUE(definition_id, window_id, drug_group, event_group, revision_no)
);
CREATE INDEX IF NOT EXISTS ix_sigrev_run ON sig_signal_revisions(run_id);
CREATE INDEX IF NOT EXISTS ix_sigrev_head
    ON sig_signal_revisions(definition_id, window_id, drug_group, event_group, revision_no);

-- 入选病例清单（修订快照的一部分，永久冻结；发布时整组插入）。
CREATE TABLE IF NOT EXISTS sig_signal_roster (
    id            TEXT PRIMARY KEY,
    revision_id   TEXT NOT NULL REFERENCES sig_signal_revisions(id),
    source_id     TEXT NOT NULL,
    case_id       TEXT NOT NULL,
    version_id    TEXT NOT NULL,
    included      INTEGER NOT NULL CHECK (included IN (0,1)),
    cell          TEXT NOT NULL,                 -- a/b/c/d/excluded
    reason        TEXT,                          -- 排除/计入原因
    detail_json   TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS ix_sigroster_rev ON sig_signal_roster(revision_id);
CREATE INDEX IF NOT EXISTS ix_sigroster_source ON sig_signal_roster(source_id);

-- 修订差异：一次发布相对同信号键窗口链上一修订的可追踪差异。
CREATE TABLE IF NOT EXISTS sig_signal_diffs (
    id                TEXT PRIMARY KEY,
    revision_id       TEXT NOT NULL REFERENCES sig_signal_revisions(id),
    prev_revision_id  TEXT REFERENCES sig_signal_revisions(id),
    run_id            TEXT NOT NULL REFERENCES sig_runs(id),
    prev_run_id       TEXT,
    old_counts_json   TEXT NOT NULL,
    new_counts_json   TEXT NOT NULL,
    metrics_delta_json TEXT NOT NULL,
    case_changes_json TEXT NOT NULL,            -- NEW/REMOVED/DRUG_CHANGE/...
    created_at        TEXT NOT NULL,
    UNIQUE(revision_id)
);
CREATE INDEX IF NOT EXISTS ix_sigdiff_prev ON sig_signal_diffs(prev_revision_id);

--------------------------------------------------------------------------------
-- 病例贡献台账（只增；撤销-重计）。
-- 一个来源在某窗口只占一条 *有效* 贡献行；后续窗口新版本：旧行 revoked=1 +
-- 新行。它在该窗口各药品组×事件组四格中的归属存 cells_json
-- （{"药品组|事件组": "a/b/c/d"}），一次撤销/重计即整体替换，绝不部分残留。
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sig_case_contributions (
    id              TEXT PRIMARY KEY,
    definition_id   TEXT NOT NULL REFERENCES sig_definitions(id),
    window_id       TEXT NOT NULL,
    source_id       TEXT NOT NULL,
    case_id         TEXT NOT NULL,
    version_id      TEXT NOT NULL,
    run_id          TEXT NOT NULL REFERENCES sig_runs(id),
    drug_groups_json TEXT NOT NULL DEFAULT '[]', -- 该版本命中的药品组
    event_groups_json TEXT NOT NULL DEFAULT '[]',-- 命中的事件组
    cells_json      TEXT NOT NULL DEFAULT '{}',  -- 每信号键的四格归属
    eligible        INTEGER NOT NULL,
    exclude_reason  TEXT,                        -- 非入选原因（MERGED_AWAY/...）
    revoked         INTEGER NOT NULL DEFAULT 0,
    revoked_by_run_id   TEXT REFERENCES sig_runs(id),
    created_at      TEXT NOT NULL
);
-- 同一来源在同一窗口至多一条有效贡献（核心去重不变量）。
CREATE UNIQUE INDEX IF NOT EXISTS ux_sigcontrib_active
    ON sig_case_contributions(definition_id, window_id, source_id)
    WHERE revoked = 0;
CREATE INDEX IF NOT EXISTS ix_sigcontrib_source
    ON sig_case_contributions(source_id, window_id);
CREATE INDEX IF NOT EXISTS ix_sigcontrib_run ON sig_case_contributions(run_id);

--------------------------------------------------------------------------------
-- 信号子系统审计哈希链（全局只增）。
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sig_audit_events (
    id              TEXT PRIMARY KEY,
    seq             INTEGER NOT NULL UNIQUE,
    event_type      TEXT NOT NULL,
    actor           TEXT,
    summary         TEXT NOT NULL,
    payload_json    TEXT NOT NULL DEFAULT '{}',
    idempotency_key TEXT UNIQUE,
    prev_hash       TEXT NOT NULL,
    entry_hash      TEXT NOT NULL,
    created_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_sigaudit_type ON sig_audit_events(event_type, seq);

--------------------------------------------------------------------------------
-- 不可变性触发器
--------------------------------------------------------------------------------
CREATE TRIGGER IF NOT EXISTS trg_sigterms_no_update BEFORE UPDATE ON sig_terms
BEGIN SELECT RAISE(FAIL, 'sig_terms is append-only (publish a new term version)'); END;
CREATE TRIGGER IF NOT EXISTS trg_sigterms_no_delete BEFORE DELETE ON sig_terms
BEGIN SELECT RAISE(FAIL, 'sig_terms is append-only'); END;

CREATE TRIGGER IF NOT EXISTS trg_sigdef_no_update BEFORE UPDATE ON sig_definitions
BEGIN SELECT RAISE(FAIL, 'sig_definitions is append-only (create a new definition)'); END;
CREATE TRIGGER IF NOT EXISTS trg_sigdef_no_delete BEFORE DELETE ON sig_definitions
BEGIN SELECT RAISE(FAIL, 'sig_definitions is append-only'); END;

CREATE TRIGGER IF NOT EXISTS trg_sigcv_no_update BEFORE UPDATE ON sig_case_versions
BEGIN SELECT RAISE(FAIL, 'sig_case_versions is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_sigcv_no_delete BEFORE DELETE ON sig_case_versions
BEGIN SELECT RAISE(FAIL, 'sig_case_versions is append-only'); END;

-- 信号修订：只放行分析师处置列的状态机更新；计算列/计数/修订号一律拒绝改写。
CREATE TRIGGER IF NOT EXISTS trg_sigrev_guard BEFORE UPDATE ON sig_signal_revisions
WHEN NOT (
    NEW.id IS OLD.id AND NEW.run_id IS OLD.run_id AND
    NEW.definition_id IS OLD.definition_id AND NEW.window_id IS OLD.window_id AND
    NEW.window_start IS OLD.window_start AND NEW.window_end IS OLD.window_end AND
    NEW.drug_group IS OLD.drug_group AND NEW.event_group IS OLD.event_group AND
    NEW.revision_no IS OLD.revision_no AND
    NEW.a IS OLD.a AND NEW.b IS OLD.b AND NEW.c IS OLD.c AND NEW.d IS OLD.d AND
    NEW.n IS OLD.n AND
    NEW.prr IS OLD.prr AND NEW.prr_ci_low IS OLD.prr_ci_low AND
    NEW.prr_ci_high IS OLD.prr_ci_high AND
    NEW.ror IS OLD.ror AND NEW.ror_ci_low IS OLD.ror_ci_low AND
    NEW.ror_ci_high IS OLD.ror_ci_high AND
    NEW.chi_square IS OLD.chi_square AND
    NEW.determination IS OLD.determination AND
    NEW.criteria_json IS OLD.criteria_json AND
    NEW.content_hash IS OLD.content_hash AND
    NEW.created_at IS OLD.created_at AND
    -- 仅以下四列允许变化（分析师处置）
    NEW.disposition IS NOT OLD.disposition
)
BEGIN SELECT RAISE(FAIL, 'signal revision metrics are immutable; only disposition may change'); END;

CREATE TRIGGER IF NOT EXISTS trg_sigrev_no_delete BEFORE DELETE ON sig_signal_revisions
BEGIN SELECT RAISE(FAIL, 'sig_signal_revisions is append-only'); END;

CREATE TRIGGER IF NOT EXISTS trg_sigroster_no_update BEFORE UPDATE ON sig_signal_roster
BEGIN SELECT RAISE(FAIL, 'sig_signal_roster is immutable'); END;
CREATE TRIGGER IF NOT EXISTS trg_sigroster_no_delete BEFORE DELETE ON sig_signal_roster
BEGIN SELECT RAISE(FAIL, 'sig_signal_roster is immutable'); END;

CREATE TRIGGER IF NOT EXISTS trg_sigdiff_no_update BEFORE UPDATE ON sig_signal_diffs
BEGIN SELECT RAISE(FAIL, 'sig_signal_diffs is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_sigdiff_no_delete BEFORE DELETE ON sig_signal_diffs
BEGIN SELECT RAISE(FAIL, 'sig_signal_diffs is append-only'); END;

-- 贡献台账：只允许 revoked 0->1（撤销），其余更新拒绝。
CREATE TRIGGER IF NOT EXISTS trg_sigcontrib_guard BEFORE UPDATE ON sig_case_contributions
WHEN NOT (
    NEW.id IS OLD.id AND NEW.definition_id IS OLD.definition_id AND
    NEW.window_id IS OLD.window_id AND NEW.source_id IS OLD.source_id AND
    NEW.case_id IS OLD.case_id AND NEW.version_id IS OLD.version_id AND
    NEW.run_id IS OLD.run_id AND NEW.drug_groups_json IS OLD.drug_groups_json AND
    NEW.event_groups_json IS OLD.event_groups_json AND
    NEW.cells_json IS OLD.cells_json AND NEW.eligible IS OLD.eligible AND
    NEW.exclude_reason IS OLD.exclude_reason AND
    NEW.created_at IS OLD.created_at AND
    OLD.revoked = 0 AND NEW.revoked = 1
)
BEGIN SELECT RAISE(FAIL, 'contributions are append-only; only revoke 0->1 allowed'); END;
CREATE TRIGGER IF NOT EXISTS trg_sigcontrib_no_delete BEFORE DELETE ON sig_case_contributions
BEGIN SELECT RAISE(FAIL, 'sig_case_contributions is append-only'); END;

CREATE TRIGGER IF NOT EXISTS trg_sigaudit_no_update BEFORE UPDATE ON sig_audit_events
BEGIN SELECT RAISE(FAIL, 'sig_audit_events is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_sigaudit_no_delete BEFORE DELETE ON sig_audit_events
BEGIN SELECT RAISE(FAIL, 'sig_audit_events is append-only'); END;
"""


def connect(db_path: str | Path) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=15, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=15000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def initialize(db_path: str | Path) -> None:
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()


@contextmanager
def transaction(conn: sqlite3.Connection):
    """BEGIN IMMEDIATE 写事务：所有写者在 SQLite 写锁上串行化。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
