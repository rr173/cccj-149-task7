"""SQLite 持久化：schema、连接工厂、事务工具。

关键持久化不变量（由数据库约束保证，而不是只靠应用代码）：

1. source_records 原始来源记录只增不改（触发器拒绝 UPDATE/DELETE）。
2. case_versions / case_events 只增不改 —— 历史版本永不被覆盖。
3. case_membership 归属关系只增：来源转移时旧行 active 1->0 并新插一行，
   部分唯一索引保证同一时刻一个来源只能有一条 active 归属
   （“任何来源只能归属一个当前病例”）。
4. regulatory_reports.regulatory_number UNIQUE + number_sequence 在同一个
   IMMEDIATE 事务内推进 —— 重试/重启不可能产生重复监管编号。
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- 原始来源记录（医院 / 患者热线 / 合作企业）--------------------------------
CREATE TABLE IF NOT EXISTS source_records (
    id            TEXT PRIMARY KEY,
    source_system TEXT NOT NULL CHECK (source_system IN ('HOSPITAL','HOTLINE','ENTERPRISE')),
    external_id   TEXT NOT NULL,
    received_at   TEXT NOT NULL,          -- 获知日期/时间（监管时钟起点）
    payload_json  TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    UNIQUE(source_system, external_id)    -- 来源方报文幂等
);

-- 病例（聚合根）-----------------------------------------------------------
CREATE TABLE IF NOT EXISTS cases (
    id               TEXT PRIMARY KEY,
    status           TEXT NOT NULL DEFAULT 'OPEN'
                     CHECK (status IN ('OPEN','MERGED','SPLIT_OFF')),
    created_at       TEXT NOT NULL,
    superseded_by_id TEXT REFERENCES cases(id)
);
CREATE INDEX IF NOT EXISTS ix_cases_status ON cases(status);

-- 来源 <-> 当前病例 的归属（只增；转移 = 停用旧行 + 插入新行）----------------
CREATE TABLE IF NOT EXISTS case_membership (
    id         TEXT PRIMARY KEY,
    source_id  TEXT NOT NULL REFERENCES source_records(id),
    case_id    TEXT NOT NULL REFERENCES cases(id),
    active     INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0,1)),
    reason     TEXT NOT NULL,             -- INTAKE / MERGE / SPLIT
    actor      TEXT,
    created_at TEXT NOT NULL
);
-- 同一来源至多一个当前病例（核心不变量）
CREATE UNIQUE INDEX IF NOT EXISTS ux_membership_active
    ON case_membership(source_id) WHERE active = 1;
CREATE INDEX IF NOT EXISTS ix_membership_case ON case_membership(case_id) WHERE active = 1;

-- 病例版本（完整快照，只增不改；随访/合并/拆分都产生新版本）------------------
CREATE TABLE IF NOT EXISTS case_versions (
    id                  TEXT PRIMARY KEY,
    case_id             TEXT NOT NULL REFERENCES cases(id),
    version_no          INTEGER NOT NULL,
    change_type         TEXT NOT NULL
                        CHECK (change_type IN ('INTAKE','FOLLOWUP','MERGE','SPLIT')),
    snapshot_json       TEXT NOT NULL,
    change_summary_json TEXT NOT NULL DEFAULT '[]',
    material_id         TEXT,
    actor               TEXT,
    created_at          TEXT NOT NULL,
    UNIQUE(case_id, version_no)
);
CREATE INDEX IF NOT EXISTS ix_versions_case ON case_versions(case_id, version_no);

-- 随访材料（只增）
CREATE TABLE IF NOT EXISTS followup_materials (
    id          TEXT PRIMARY KEY,
    case_id     TEXT NOT NULL REFERENCES cases(id),
    received_at TEXT NOT NULL,
    content_json TEXT NOT NULL,
    actor       TEXT,
    created_at  TEXT NOT NULL
);

-- 统一事件日志（时间线的权威来源，只增；带每病例哈希链，供离线证据包核验）
CREATE TABLE IF NOT EXISTS case_events (
    id          TEXT PRIMARY KEY,
    case_id     TEXT NOT NULL REFERENCES cases(id),
    seq         INTEGER NOT NULL,          -- 病例内事件序号，从 1 开始
    event_type  TEXT NOT NULL,
    event_time  TEXT NOT NULL,
    actor       TEXT,
    summary     TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    prev_hash   TEXT NOT NULL,             -- 上一帧 entry_hash（首帧=genesis）
    entry_hash  TEXT NOT NULL,             -- 本帧 SHA-256（规范 JSON）
    version_id  TEXT REFERENCES case_versions(id),
    report_id   TEXT,
    cluster_id  TEXT
);
CREATE INDEX IF NOT EXISTS ix_events_case ON case_events(case_id, event_time);
-- ux_events_case_seq 由 initialize 在末尾创建（旧库需先迁移补 seq 列）

-- 病例血缘闭包（合并/拆分后历史可追溯到当前存活病例）
CREATE TABLE IF NOT EXISTS case_lineage (
    ancestor_id   TEXT NOT NULL REFERENCES cases(id),
    descendant_id TEXT NOT NULL REFERENCES cases(id),
    depth         INTEGER NOT NULL,
    PRIMARY KEY (ancestor_id, descendant_id)
);
CREATE INDEX IF NOT EXISTS ix_lineage_desc ON case_lineage(descendant_id);

-- 疑似重复簇（PROPOSED -> CONFIRMED/REJECTED，安全专员决定）----------------
CREATE TABLE IF NOT EXISTS duplicate_clusters (
    id            TEXT PRIMARY KEY,
    status        TEXT NOT NULL DEFAULT 'PROPOSED'
                  CHECK (status IN ('PROPOSED','CONFIRMED','REJECTED','SUPERSEDED')),
    rationale_json TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    decided_at    TEXT,
    decided_by    TEXT,
    reject_reason TEXT,
    merged_into_id TEXT REFERENCES duplicate_clusters(id)
);
CREATE TABLE IF NOT EXISTS cluster_members (
    cluster_id TEXT NOT NULL REFERENCES duplicate_clusters(id),
    case_id    TEXT NOT NULL REFERENCES cases(id),
    added_at   TEXT NOT NULL,
    PRIMARY KEY (cluster_id, case_id)
);

-- 监管参考：已报告/预期 (药品, 反应) 对（listedness / expectedness）
CREATE TABLE IF NOT EXISTS ref_listed_reactions (
    drug_canonical     TEXT NOT NULL,
    reaction_canonical TEXT NOT NULL,
    PRIMARY KEY (drug_canonical, reaction_canonical)
);

-- 监管编号序列（按年度作用域），UPDATE...RETURNING 原子推进
CREATE TABLE IF NOT EXISTS number_sequence (
    scope TEXT PRIMARY KEY,
    last  INTEGER NOT NULL DEFAULT 0
);

-- 监管报告（指向发送时的不可变版本；历史报告永不被改写）
CREATE TABLE IF NOT EXISTS regulatory_reports (
    id                  TEXT PRIMARY KEY,
    case_id             TEXT NOT NULL REFERENCES cases(id),
    case_version_id     TEXT NOT NULL REFERENCES case_versions(id),
    followup_of_id      TEXT REFERENCES regulatory_reports(id),
    report_type         TEXT NOT NULL CHECK (report_type IN ('INITIAL','FOLLOWUP')),
    regulatory_number   TEXT NOT NULL UNIQUE,
    status              TEXT NOT NULL
                        CHECK (status IN ('QUEUED','SENDING','ACKED','FAILED','VOID')),
    expedited           INTEGER NOT NULL,
    days_due            INTEGER NOT NULL,
    due_date            TEXT NOT NULL,
    decision_json       TEXT NOT NULL,
    payload_json        TEXT NOT NULL,
    -- 本次决定固定使用的法规规则版本（计算路径写入；试算/重算不改已发送行）
    jurisdiction        TEXT NOT NULL DEFAULT 'CN',
    rule_package_id     TEXT NOT NULL DEFAULT 'builtin:legacy',
    rule_version        TEXT NOT NULL DEFAULT '0',
    attempts            INTEGER NOT NULL DEFAULT 0,
    last_error          TEXT,
    next_retry_after    TEXT,
    ack_id              TEXT,
    ack_json            TEXT,
    acked_at            TEXT,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);
-- 一个病例版本至多生成一份报告（规则幂等）
CREATE UNIQUE INDEX IF NOT EXISTS ux_report_version
    ON regulatory_reports(case_version_id);
CREATE INDEX IF NOT EXISTS ix_reports_status ON regulatory_reports(status, next_retry_after);
CREATE INDEX IF NOT EXISTS ix_reports_jurisdiction ON regulatory_reports(jurisdiction, status);

-- 证据包导出任务（同一 case+version+幂等键 全局唯一）-----------------------
CREATE TABLE IF NOT EXISTS export_tasks (
    id              TEXT PRIMARY KEY,
    case_id         TEXT NOT NULL REFERENCES cases(id),
    case_version_id TEXT NOT NULL REFERENCES case_versions(id),
    idempotency_key TEXT NOT NULL,
    status          TEXT NOT NULL
                    CHECK (status IN ('PENDING','BUILDING','COMPLETED','FAILED')),
    progress_json   TEXT NOT NULL DEFAULT '{}',
    requested_by    TEXT,
    requested_at    TEXT NOT NULL,
    started_at      TEXT,
    finished_at     TEXT,
    error_code      TEXT,
    error_message   TEXT,
    package_root    TEXT,                   -- manifest root（hex）
    package_path    TEXT,                   -- 数据目录内相对路径
    package_size    INTEGER,
    UNIQUE(case_id, case_version_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS ix_exports_case ON export_tasks(case_id, requested_at);
CREATE INDEX IF NOT EXISTS ix_exports_status ON export_tasks(status);

-- 离线核验结果（服务端复跑同一套纯函数核验器，历史结果可追溯）
CREATE TABLE IF NOT EXISTS verification_runs (
    id           TEXT PRIMARY KEY,
    export_task_id TEXT REFERENCES export_tasks(id),
    package_root TEXT,
    ok           INTEGER NOT NULL,
    report_json  TEXT NOT NULL,
    verifier     TEXT,
    created_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_verif_task ON verification_runs(export_task_id, created_at);

--------------------------------------------------------------------------------
-- 药物安全决定双人会签
--------------------------------------------------------------------------------
-- 数据修订号（全局单行）：新材料入库 / 归并 / 拆解 / 规则重新计算时 +1。
-- 评审单创建时冻结 revision；业务变更在同一 IMMEDIATE 事务内推进修订号并
-- 原子作废旧单，因此“旧单立即失效、不能推动后续发送”由数据库串行化保证。
CREATE TABLE IF NOT EXISTS review_revision (
    scope     TEXT PRIMARY KEY DEFAULT 'GLOBAL',
    revision  INTEGER NOT NULL DEFAULT 0
);

-- 每次修订推进的原因（只增；评审单失效原因可回溯到具体修订事件）
CREATE TABLE IF NOT EXISTS review_revision_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    revision    INTEGER NOT NULL UNIQUE,
    reason_code TEXT NOT NULL,
    detail_json TEXT NOT NULL DEFAULT '{}',
    actor       TEXT,
    created_at  TEXT NOT NULL
);

-- 会签评审单（不可变评审单）。
-- 单子：创建后冻结 revision 与内容；状态机只允许单向流转：
-- OPEN -> APPROVED / REJECTED / INVALIDATED（终态；作废单永不复活，
-- 只能以最新修订号重新发起）。
CREATE TABLE IF NOT EXISTS review_sheets (
    id             TEXT PRIMARY KEY,
    status         TEXT NOT NULL DEFAULT 'OPEN'
                   CHECK (status IN ('OPEN','APPROVED','REJECTED','INVALIDATED')),
    revision       INTEGER NOT NULL,             -- 创建时冻结的数据修订号
    content_hash   TEXT NOT NULL,                -- items 快照内容 SHA-256
    title          TEXT NOT NULL,
    requested_by   TEXT,
    lease_holder   TEXT,                         -- 租约持有者原始提交值（展示/追溯）
    lease_holder_key TEXT,                       -- 规范化身份键（判定/比较用）
    leased_at      TEXT,
    lease_expires_at TEXT,
    approval_count INTEGER NOT NULL DEFAULT 0,
    created_at     TEXT NOT NULL,
    decided_at     TEXT,
    invalidated_reason TEXT,
    invalidated_at     TEXT,
    invalidated_revision INTEGER
);
-- 同一修订号 + 同一内容：只允许存在一张未决（OPEN）单 ——
-- 并发重复发起、服务恢复重试在数据库层幂等。
CREATE UNIQUE INDEX IF NOT EXISTS ux_review_open_content
    ON review_sheets(revision, content_hash) WHERE status='OPEN';
CREATE INDEX IF NOT EXISTS ix_review_status ON review_sheets(status, lease_expires_at);
CREATE INDEX IF NOT EXISTS ix_review_revision ON review_sheets(revision);

-- 评审单项（不可变快照：候选归并关系 / 严重等级变化 / 待发送决定）。
-- 禁止 UPDATE/DELETE（见触发器）：旧单里的意见与冻结内容永久可查。
CREATE TABLE IF NOT EXISTS review_sheet_items (
    id          TEXT PRIMARY KEY,
    sheet_id    TEXT NOT NULL REFERENCES review_sheets(id),
    position    INTEGER NOT NULL,               -- 冻结顺序
    item_type   TEXT NOT NULL
                CHECK (item_type IN ('MERGE_CANDIDATE','SERIOUSNESS_CHANGE','PENDING_REPORT')),
    ref_type    TEXT,                            -- cluster / case / report
    ref_id      TEXT,
    snapshot_json TEXT NOT NULL,
    UNIQUE(sheet_id, position)
);
CREATE INDEX IF NOT EXISTS ix_review_items_sheet ON review_sheet_items(sheet_id);

-- 会签意见流水（只增不改：签署 / 否决 / 撤回 / 失效全部留痕，
-- “已有意见保留但不能推动后续发送”由此成立）。
CREATE TABLE IF NOT EXISTS review_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    sheet_id    TEXT NOT NULL REFERENCES review_sheets(id),
    event_type  TEXT NOT NULL
                CHECK (event_type IN ('CREATED','CLAIMED','RELEASED',
                                      'APPROVED','VETOED','WITHDRAWN',
                                      'APPROVED_COMPLETED','VETOED_COMPLETED',
                                      'INVALIDATED','LEASE_EXPIRED')),
    actor       TEXT,                            -- 操作人原始提交值（追溯）
    actor_key   TEXT,                            -- 规范化身份键（过滤/比较用）
    detail_json TEXT NOT NULL DEFAULT '{}',
    created_at  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_review_events_sheet ON review_events(sheet_id, id);

-- 当前活意见投影（一个复核员在一张单上至多占一个意见槽）。
-- 主键 (sheet_id, reviewer_key) 以 *规范化身份* 保证“同一身份的再次提交
-- 不能累计票数”：首尾空白/大小写/Unicode 等价写法都命中同一槽位，
-- 单人无法用 "alice"/"ALICE"/" alice " 占满两个槽完成双签。
-- reviewer 保留每次提交的原始字符串（可追溯）；撤回 = 删槽（流水仍在
-- review_events）。APPROVED 槽数即当前有效赞成票；两名不同复核员赞成
-- 才完成会签。
CREATE TABLE IF NOT EXISTS review_signatures (
    sheet_id     TEXT NOT NULL REFERENCES review_sheets(id),
    reviewer_key TEXT NOT NULL,                  -- 规范化身份键（槽位归属）
    reviewer     TEXT NOT NULL,                  -- 原始提交值（追溯/展示）
    verdict      TEXT NOT NULL CHECK (verdict IN ('APPROVED','VETOED')),
    detail_json  TEXT NOT NULL DEFAULT '{}',
    signed_at    TEXT NOT NULL,
    PRIMARY KEY (sheet_id, reviewer_key)
);
-- ix_review_sigs_key 在 initialize 末尾（迁移之后）创建：旧库的该表在
-- 迁移时重建，索引必须等 reviewer_key 列就位后才能建立。

-- 会签放行闸门：评审单 APPROVED 后按待发送决定（报告行）解锁。
-- 主键 report_id 保证一份待发送决定无论经几张单/多少次重放只解锁一次，
-- 且只对创建单时冻结的那批决定生效；修订后新排队的报告必须重新会签。
CREATE TABLE IF NOT EXISTS countersign_approvals (
    report_id       TEXT PRIMARY KEY,
    sheet_id        TEXT NOT NULL REFERENCES review_sheets(id),
    revision        INTEGER NOT NULL,
    approved_at     TEXT NOT NULL,
    approved_by_json TEXT NOT NULL              -- 两名签署人（留档）
);

--------------------------------------------------------------------------------
-- 药物安全法规规则治理：登记 / 试算 / 受控启用 / 定时 / 回滚 / 更正候选 / 审计
--------------------------------------------------------------------------------
-- 不可变规则包。登记后永不修改/删除（触发器兜底）。
--  (jurisdiction, version) 唯一：不同内容占用同一版本号由数据库拒绝；
--  content_hash 唯一：相同内容重复登记直接命中原包（返回原版本）。
CREATE TABLE IF NOT EXISTS rule_packages (
    id             TEXT PRIMARY KEY,            -- 内部行 id（hex）
    package_id     TEXT NOT NULL,               -- 规则族逻辑 id（缺省=辖区）
    jurisdiction   TEXT NOT NULL,
    version        TEXT NOT NULL,
    effective_from TEXT NOT NULL,
    effective_to   TEXT,                        -- null = 开放区间（正无穷）
    content_json   TEXT NOT NULL,               -- 规范内容（含参数/依赖/摘要）
    content_hash   TEXT NOT NULL UNIQUE,
    signature      TEXT NOT NULL,
    signer_id      TEXT NOT NULL,
    depends_json   TEXT NOT NULL DEFAULT '[]',
    summary        TEXT NOT NULL DEFAULT '',
    registered_by  TEXT,
    created_at     TEXT NOT NULL,
    UNIQUE(jurisdiction, version),
    UNIQUE(package_id, version)
);
CREATE INDEX IF NOT EXISTS ix_rule_pkg_jur ON rule_packages(jurisdiction, effective_from);

-- 规则包签名密钥注册表（signer_id -> secret）。
-- 验签只认此表中的密钥；密钥行只增（轮换=新增 signer_id，旧密钥保留可验历史包）。
CREATE TABLE IF NOT EXISTS rule_signing_keys (
    signer_id   TEXT PRIMARY KEY,
    secret      TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    created_by  TEXT
);
CREATE TRIGGER IF NOT EXISTS trg_rule_keys_no_update BEFORE UPDATE ON rule_signing_keys
BEGIN SELECT RAISE(FAIL, 'rule_signing_keys is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_rule_keys_no_delete BEFORE DELETE ON rule_signing_keys
BEGIN SELECT RAISE(FAIL, 'rule_signing_keys is append-only'); END;

-- 辖区当前生效规则版本（单行/辖区；受控启用/回滚/定时在此 CAS 切换）。
-- 不存在的辖区视为内置历史口径 builtin:legacy@0。
CREATE TABLE IF NOT EXISTS rule_jurisdiction_state (
    jurisdiction     TEXT PRIMARY KEY,
    current_pkg_id   TEXT NOT NULL,             -- rule_packages.id
    package_id       TEXT NOT NULL,
    version          TEXT NOT NULL,
    current_seq      INTEGER NOT NULL DEFAULT 0,-- 累计启用次数（单调递增）
    activated_at     TEXT NOT NULL,
    activated_by     TEXT
);

-- 启用/回滚历史（只增）。idempotency_key 唯一：并发/重试的同一请求只产生一条结论。
CREATE TABLE IF NOT EXISTS rule_activations (
    id              TEXT PRIMARY KEY,
    jurisdiction    TEXT NOT NULL,
    kind            TEXT NOT NULL CHECK (kind IN ('ACTIVATE','ROLLBACK','SCHEDULED')),
    package_id      TEXT NOT NULL,
    version         TEXT NOT NULL,
    rule_pkg_id     TEXT REFERENCES rule_packages(id),
    from_package_id TEXT,
    from_version    TEXT,
    job_id          TEXT,                       -- 关联重算/更正作业
    status          TEXT NOT NULL                -- COMPLETED / FAILED / SKIPPED
                    CHECK (status IN ('COMPLETED','FAILED','SUPERSEDED',
                                      'SKIPPED_ALREADY_CURRENT')),
    idempotency_key TEXT NOT NULL UNIQUE,
    actor           TEXT,
    detail_json     TEXT NOT NULL DEFAULT '{}',
    created_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_rule_act_jur ON rule_activations(jurisdiction, created_at);

-- 定时生效计划。只有一个 PENDING 计划可作用于同一辖区（部分唯一索引）。
CREATE TABLE IF NOT EXISTS rule_schedules (
    id             TEXT PRIMARY KEY,
    jurisdiction   TEXT NOT NULL,
    rule_pkg_id    TEXT NOT NULL REFERENCES rule_packages(id),
    package_id     TEXT NOT NULL,
    version        TEXT NOT NULL,
    effective_at   TEXT NOT NULL,               -- 到点自动启用
    status         TEXT NOT NULL
                   CHECK (status IN ('PENDING','FIRED','CANCELLED','SUPERSEDED')),
    requested_by   TEXT,
    created_at     TEXT NOT NULL,
    fired_at       TEXT,
    activation_id  TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_rule_sched_pending
    ON rule_schedules(jurisdiction) WHERE status='PENDING';
CREATE INDEX IF NOT EXISTS ix_rule_sched_due ON rule_schedules(effective_at)
    WHERE status='PENDING';

-- 批量试算任务（选择一组历史数据快照，比较基线 vs 候选规则的决定差异）。
CREATE TABLE IF NOT EXISTS rule_trials (
    id               TEXT PRIMARY KEY,
    jurisdiction     TEXT NOT NULL,
    candidate_pkg_id TEXT NOT NULL REFERENCES rule_packages(id),
    baseline_pkg_id  TEXT REFERENCES rule_packages(id),  -- null = builtin:legacy
    selector_json    TEXT NOT NULL DEFAULT '{}', -- 快照选择条件（创建时冻结）
    scope_json       TEXT NOT NULL DEFAULT '[]', -- 冻结的 case_version 集合
    status           TEXT NOT NULL
                     CHECK (status IN ('PENDING','RUNNING','COMPLETED','FAILED')),
    total_units      INTEGER NOT NULL DEFAULT 0,
    done_units       INTEGER NOT NULL DEFAULT 0,
    changed_units    INTEGER NOT NULL DEFAULT 0,
    shard_size       INTEGER NOT NULL DEFAULT 50,
    next_shard       INTEGER NOT NULL DEFAULT 0, -- 检查点：下一个待处理分片序号
    error_code       TEXT,
    error_message    TEXT,
    idempotency_key  TEXT,
    requested_by     TEXT,
    created_at       TEXT NOT NULL,
    started_at       TEXT,
    finished_at      TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_rule_trial_idem
    ON rule_trials(jurisdiction, candidate_pkg_id, idempotency_key);
CREATE INDEX IF NOT EXISTS ix_rule_trial_status ON rule_trials(status);

-- 试算分片单元（每条历史快照一行；唯一约束保证恢复续跑不重复生成差异）。
CREATE TABLE IF NOT EXISTS rule_trial_units (
    id               TEXT PRIMARY KEY,
    trial_id         TEXT NOT NULL REFERENCES rule_trials(id),
    shard_no         INTEGER NOT NULL,
    position         INTEGER NOT NULL,
    case_id          TEXT NOT NULL,
    case_version_id  TEXT NOT NULL,
    snapshot_json    TEXT NOT NULL,              -- 创建时冻结的快照副本（只读）
    awareness_date   TEXT NOT NULL,
    baseline_json    TEXT,                       -- 基线决定（可空=尚未处理）
    candidate_json   TEXT,
    changed          INTEGER NOT NULL DEFAULT 0,
    diff_json        TEXT,
    UNIQUE(trial_id, case_version_id)
);
CREATE INDEX IF NOT EXISTS ix_rule_trial_unit ON rule_trial_units(trial_id, shard_no, position);

-- 启用/回滚的批量重算与更正作业（分片、检查点续跑；失败不留部分启用）。
CREATE TABLE IF NOT EXISTS rule_jobs (
    id              TEXT PRIMARY KEY,
    jurisdiction    TEXT NOT NULL,
    kind            TEXT NOT NULL CHECK (kind IN ('ACTIVATE','ROLLBACK','SCHEDULED')),
    target_pkg_id   TEXT NOT NULL REFERENCES rule_packages(id),
    package_id      TEXT NOT NULL,
    version         TEXT NOT NULL,
    idem_key        TEXT NOT NULL,
    status          TEXT NOT NULL
                    CHECK (status IN ('PENDING','RUNNING','COMPLETED','FAILED','CANCELLED')),
    total_units     INTEGER NOT NULL DEFAULT 0,
    staged_units    INTEGER NOT NULL DEFAULT 0,  -- 已暂存（阶段一进度）
    applied         INTEGER NOT NULL DEFAULT 0,  -- 是否已原子应用（阶段二）
    next_shard      INTEGER NOT NULL DEFAULT 0,
    shard_size      INTEGER NOT NULL DEFAULT 50,
    unsent_count    INTEGER NOT NULL DEFAULT 0,
    sent_count      INTEGER NOT NULL DEFAULT 0,
    candidate_count INTEGER NOT NULL DEFAULT 0,
    error_code      TEXT,
    error_message   TEXT,
    actor           TEXT,
    created_at      TEXT NOT NULL,
    started_at      TEXT,
    finished_at     TEXT
);
CREATE INDEX IF NOT EXISTS ix_rule_job_status ON rule_jobs(status);

-- 作业分片单元：阶段一把每个受影响报告的新决定/更正候选暂存于此，
-- 全部分片成功后阶段二在单事务内原子应用；任一分片失败 -> 作业 FAILED，
-- 暂存仅为待应用草稿，绝不改写任何报告/会签/发送状态。
CREATE TABLE IF NOT EXISTS rule_job_units (
    id               TEXT PRIMARY KEY,
    job_id           TEXT NOT NULL REFERENCES rule_jobs(id),
    shard_no         INTEGER NOT NULL,
    position         INTEGER NOT NULL,
    report_id        TEXT NOT NULL,
    case_id          TEXT NOT NULL,
    case_version_id  TEXT NOT NULL,
    report_status    TEXT NOT NULL,             -- 处理时刻的报告状态（ACKED/QUEUED/...）
    old_package_id   TEXT NOT NULL,
    old_version      TEXT NOT NULL,
    new_package_id   TEXT NOT NULL,
    new_version      TEXT NOT NULL,
    action           TEXT NOT NULL
                     CHECK (action IN ('RECALC_UNSENT','VOID_UNSENT','CORRECTION_SENT')),
    new_decision_json TEXT,
    changed          INTEGER NOT NULL DEFAULT 0,
    applied          INTEGER NOT NULL DEFAULT 0,
    UNIQUE(job_id, report_id)
);
CREATE INDEX IF NOT EXISTS ix_rule_job_unit ON rule_job_units(job_id, shard_no, position);

-- 已发送报告的更正候选（只增、可复核；永不覆盖原报告/原始历史结果）。
-- 同一报告在同一目标版本至多一条候选，且一条报告对一次作业至多一条（幂等）。
CREATE TABLE IF NOT EXISTS rule_correction_candidates (
    id               TEXT PRIMARY KEY,
    jurisdiction     TEXT NOT NULL,
    report_id        TEXT NOT NULL,
    case_id          TEXT NOT NULL,
    regulatory_number TEXT NOT NULL,
    job_id           TEXT NOT NULL REFERENCES rule_jobs(id),
    activation_id    TEXT REFERENCES rule_activations(id),
    original_package_id TEXT NOT NULL,
    original_version TEXT NOT NULL,
    candidate_package_id TEXT NOT NULL,
    candidate_version TEXT NOT NULL,
    original_decision_json TEXT NOT NULL,
    candidate_decision_json TEXT NOT NULL,
    diff_json        TEXT NOT NULL,
    created_at       TEXT NOT NULL,
    UNIQUE(job_id, report_id),
    UNIQUE(report_id, candidate_package_id, candidate_version)
);
CREATE INDEX IF NOT EXISTS ix_corr_report ON rule_correction_candidates(report_id);

-- 规则治理审计事件（全局只增哈希链；idempotency_key 唯一保证不重复审计事件）。
CREATE TABLE IF NOT EXISTS rule_audit_events (
    id              TEXT PRIMARY KEY,
    seq             INTEGER NOT NULL UNIQUE,
    event_type      TEXT NOT NULL,
    actor           TEXT,
    jurisdiction    TEXT,
    summary         TEXT NOT NULL,
    payload_json    TEXT NOT NULL DEFAULT '{}',
    idempotency_key TEXT UNIQUE,
    prev_hash       TEXT NOT NULL,
    entry_hash      TEXT NOT NULL,
    created_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_rule_audit_type ON rule_audit_events(event_type, seq);

--------------------------------------------------------------------------------
-- 不可变性触发器
--------------------------------------------------------------------------------
CREATE TRIGGER IF NOT EXISTS trg_sources_no_update BEFORE UPDATE ON source_records
BEGIN SELECT RAISE(FAIL, 'source_records is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_sources_no_delete BEFORE DELETE ON source_records
BEGIN SELECT RAISE(FAIL, 'source_records is append-only'); END;

CREATE TRIGGER IF NOT EXISTS trg_versions_no_update BEFORE UPDATE ON case_versions
BEGIN SELECT RAISE(FAIL, 'case_versions is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_versions_no_delete BEFORE DELETE ON case_versions
BEGIN SELECT RAISE(FAIL, 'case_versions is append-only'); END;

CREATE TRIGGER IF NOT EXISTS trg_events_no_update BEFORE UPDATE ON case_events
BEGIN SELECT RAISE(FAIL, 'case_events is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_events_no_delete BEFORE DELETE ON case_events
BEGIN SELECT RAISE(FAIL, 'case_events is append-only'); END;

CREATE TRIGGER IF NOT EXISTS trg_materials_no_update BEFORE UPDATE ON followup_materials
BEGIN SELECT RAISE(FAIL, 'followup_materials is append-only'); END;

-- 归属行只允许 active: 1 -> 0（转移时停用），其余任何字段修改都被拒绝
CREATE TRIGGER IF NOT EXISTS trg_membership_guard BEFORE UPDATE ON case_membership
WHEN NOT (
    NEW.id IS OLD.id
    AND NEW.source_id IS OLD.source_id
    AND NEW.case_id IS OLD.case_id
    AND NEW.reason IS OLD.reason
    AND NEW.actor IS OLD.actor
    AND NEW.created_at IS OLD.created_at
    AND OLD.active = 1 AND NEW.active = 0
)
BEGIN SELECT RAISE(FAIL, 'case_membership is append-only; only active 1->0 allowed'); END;

CREATE TRIGGER IF NOT EXISTS trg_membership_no_delete BEFORE DELETE ON case_membership
BEGIN SELECT RAISE(FAIL, 'case_membership is append-only'); END;

-- 评审单项：冻结快照，任何 UPDATE/DELETE 都拒绝（不可变评审单）
CREATE TRIGGER IF NOT EXISTS trg_review_items_no_update BEFORE UPDATE ON review_sheet_items
BEGIN SELECT RAISE(FAIL, 'review_sheet_items is immutable'); END;
CREATE TRIGGER IF NOT EXISTS trg_review_items_no_delete BEFORE DELETE ON review_sheet_items
BEGIN SELECT RAISE(FAIL, 'review_sheet_items is immutable'); END;

-- 会签意见流水：只增
CREATE TRIGGER IF NOT EXISTS trg_review_events_no_update BEFORE UPDATE ON review_events
BEGIN SELECT RAISE(FAIL, 'review_events is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_review_events_no_delete BEFORE DELETE ON review_events
BEGIN SELECT RAISE(FAIL, 'review_events is append-only'); END;

-- 修订事件：只增
CREATE TRIGGER IF NOT EXISTS trg_revision_events_no_update BEFORE UPDATE ON review_revision_events
BEGIN SELECT RAISE(FAIL, 'review_revision_events is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_revision_events_no_delete BEFORE DELETE ON review_revision_events
BEGIN SELECT RAISE(FAIL, 'review_revision_events is append-only'); END;

-- 放行闸门：只增（解锁不可撤销，撤回只对未完成单有效）
CREATE TRIGGER IF NOT EXISTS trg_cs_approvals_no_update BEFORE UPDATE ON countersign_approvals
BEGIN SELECT RAISE(FAIL, 'countersign_approvals is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_cs_approvals_no_delete BEFORE DELETE ON countersign_approvals
BEGIN SELECT RAISE(FAIL, 'countersign_approvals is append-only'); END;

-- 规则包：登记后不可变（只能新增新版本，不能改/删既有版本）
CREATE TRIGGER IF NOT EXISTS trg_rule_pkg_no_update BEFORE UPDATE ON rule_packages
BEGIN SELECT RAISE(FAIL, 'rule_packages is append-only (register a new version)'); END;
CREATE TRIGGER IF NOT EXISTS trg_rule_pkg_no_delete BEFORE DELETE ON rule_packages
BEGIN SELECT RAISE(FAIL, 'rule_packages is append-only'); END;

-- 启用/回滚历史：只增
CREATE TRIGGER IF NOT EXISTS trg_rule_act_no_update BEFORE UPDATE ON rule_activations
BEGIN SELECT RAISE(FAIL, 'rule_activations is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_rule_act_no_delete BEFORE DELETE ON rule_activations
BEGIN SELECT RAISE(FAIL, 'rule_activations is append-only'); END;

-- 更正候选：只增（已发送报告永不被覆盖，候选永久可复核）
CREATE TRIGGER IF NOT EXISTS trg_corr_no_update BEFORE UPDATE ON rule_correction_candidates
BEGIN SELECT RAISE(FAIL, 'rule_correction_candidates is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_corr_no_delete BEFORE DELETE ON rule_correction_candidates
BEGIN SELECT RAISE(FAIL, 'rule_correction_candidates is append-only'); END;

-- 治理审计事件：只增哈希链
CREATE TRIGGER IF NOT EXISTS trg_rule_audit_no_update BEFORE UPDATE ON rule_audit_events
BEGIN SELECT RAISE(FAIL, 'rule_audit_events is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_rule_audit_no_delete BEFORE DELETE ON rule_audit_events
BEGIN SELECT RAISE(FAIL, 'rule_audit_events is append-only'); END;
"""


def connect(db_path: str | Path) -> sqlite3.Connection:
    """打开一个数据库连接（短连接，线程间不共享）。"""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=15, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=15000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _migrate(conn: sqlite3.Connection) -> None:
    """旧库迁移：为 case_events 补审计哈希链列并回填历史帧。

    回填顺序与 append_event 一致（病例内按 rowid），回填后再加唯一索引；
    整个过程在一个事务内完成，新库（列已存在）直接跳过。
    """
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(case_events)")}
    if "entry_hash" in cols:
        return
    import hashlib
    import json as _json

    conn.execute("PRAGMA foreign_keys=OFF")
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            # 临时摘掉旧表的只增触发器（本迁移是唯一允许的结构性更新）
            conn.execute("DROP TRIGGER IF EXISTS trg_events_no_update")
            conn.execute("ALTER TABLE case_events RENAME TO case_events_old")
            conn.execute("""
                CREATE TABLE case_events (
                    id          TEXT PRIMARY KEY,
                    case_id     TEXT NOT NULL REFERENCES cases(id),
                    seq         INTEGER NOT NULL,
                    event_type  TEXT NOT NULL,
                    event_time  TEXT NOT NULL,
                    actor       TEXT,
                    summary     TEXT NOT NULL,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    prev_hash   TEXT NOT NULL,
                    entry_hash  TEXT NOT NULL,
                    version_id  TEXT REFERENCES case_versions(id),
                    report_id   TEXT,
                    cluster_id  TEXT
                )""")
            cases = [r["case_id"] for r in conn.execute(
                "SELECT DISTINCT case_id FROM case_events_old ORDER BY case_id")]
            for cid in cases:
                prev = "GENESIS"
                rows = conn.execute(
                    "SELECT * FROM case_events_old WHERE case_id=? ORDER BY rowid",
                    (cid,)).fetchall()
                for seq, r in enumerate(rows, start=1):
                    entry = {
                        "case_id": cid, "seq": seq,
                        "event_type": r["event_type"], "event_time": r["event_time"],
                        "actor": r["actor"], "summary": r["summary"],
                        "payload_json": r["payload_json"],
                        "version_id": r["version_id"], "report_id": r["report_id"],
                        "cluster_id": r["cluster_id"], "prev_hash": prev,
                    }
                    h = hashlib.sha256(
                        _json.dumps(entry, ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":")).encode("utf-8")).hexdigest()
                    conn.execute(
                        "INSERT INTO case_events(id,case_id,seq,event_type,event_time,actor,"
                        "summary,payload_json,prev_hash,entry_hash,version_id,report_id,"
                        "cluster_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (r["id"], cid, seq, r["event_type"], r["event_time"], r["actor"],
                         r["summary"], r["payload_json"], prev, h, r["version_id"],
                         r["report_id"], r["cluster_id"]))
                    prev = h
            conn.execute("DROP TABLE case_events_old")
            conn.execute("CREATE INDEX IF NOT EXISTS ix_events_case "
                         "ON case_events(case_id, event_time)")
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_events_case_seq "
                         "ON case_events(case_id, seq)")
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
    finally:
        conn.execute("PRAGMA foreign_keys=ON")


def _table_columns(conn, table: str) -> set[str]:
    return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}


def _migrate_reviewer_identity(conn: sqlite3.Connection) -> None:
    """旧库迁移：复核员身份规范化键（reviewer_key / actor_key / lease_holder_key）。

    历史库直接以原始字符串作身份，同一复核员的空白/大小写/Unicode 等价
    写法会被当成多人。迁移把身份判定统一到规范化键：

    - ``review_sheets`` 补 ``lease_holder_key`` 并按现存租约回填；
    - ``review_signatures`` 改为以 ``(sheet_id, reviewer_key)`` 为主键，
      回填时把历史上 *同键重复* 的意见槽合并保留一条（确定性保留最后一次
      提交），并按真实 APPROVED 槽数重算 ``approval_count``；
    - ``review_events`` 补 ``actor_key``（重建表，id/顺序/明细原样保留）。

    迁移只改变“身份如何被索引/比较”，绝不删除评审单与历史流水，已产生的
    终态（APPROVED/REJECTED/INVALIDATED）结论原样保留。
    """
    from .identity import normalize_reviewer

    def _norm_actor(value):
        # 历史 actor 可能是空白串（非 NULL）：规范化后为空应记为 NULL 键，
        # 与“无身份的系统事件（CREATED/INVALIDATED）”保持一致。
        if not value or not value.strip():
            return None
        return normalize_reviewer(value)

    # 整个身份迁移在一个 IMMEDIATE 事务内：崩溃不会留下半迁移结构
    # （SQLite 的 DDL 也受事务保护，重跑 initialize 时整段重来）。
    conn.execute("BEGIN IMMEDIATE")
    try:
        # 1) review_sheets.lease_holder_key（直接加列 + 回填，该表无触发器）
        sheet_cols = _table_columns(conn, "review_sheets")
        if sheet_cols and "lease_holder_key" not in sheet_cols:
            conn.execute("ALTER TABLE review_sheets ADD COLUMN lease_holder_key TEXT")
            rows = conn.execute(
                "SELECT id, lease_holder FROM review_sheets "
                "WHERE lease_holder IS NOT NULL").fetchall()
            for r in rows:
                key = _norm_actor(r["lease_holder"])
                conn.execute(
                    "UPDATE review_sheets SET lease_holder_key=? WHERE id=?",
                    (key, r["id"]))

        # 2) review_signatures：规范化键 + 新主键（重建表）
        sig_cols = _table_columns(conn, "review_signatures")
        if sig_cols and "reviewer_key" not in sig_cols:
            conn.execute("ALTER TABLE review_signatures RENAME TO review_signatures_old")
            conn.execute("""
                CREATE TABLE review_signatures (
                    sheet_id     TEXT NOT NULL REFERENCES review_sheets(id),
                    reviewer_key TEXT NOT NULL,
                    reviewer     TEXT NOT NULL,
                    verdict      TEXT NOT NULL CHECK (verdict IN ('APPROVED','VETOED')),
                    detail_json  TEXT NOT NULL DEFAULT '{}',
                    signed_at    TEXT NOT NULL,
                    PRIMARY KEY (sheet_id, reviewer_key)
                )""")
            # 回填（Python 侧规范化，避免在 SQL 里重写 NFKC/casefold 规则）
            old_rows = conn.execute(
                "SELECT sheet_id, reviewer, verdict, detail_json, signed_at, rowid "
                "FROM review_signatures_old ORDER BY rowid").fetchall()
            # 同键重复（历史漏洞产物）确定性保留最后一次提交（rowid 最大），
            # 其余槽位丢弃但事件流水仍在 review_events 中可追溯。
            winner: dict[tuple, int] = {}
            for r in old_rows:
                winner[(r["sheet_id"], normalize_reviewer(r["reviewer"]))] = r["rowid"]
            for r in old_rows:
                key = normalize_reviewer(r["reviewer"])
                if winner[(r["sheet_id"], key)] != r["rowid"]:
                    continue
                conn.execute(
                    "INSERT INTO review_signatures(sheet_id,reviewer_key,reviewer,"
                    "verdict,detail_json,signed_at) VALUES(?,?,?,?,?,?)",
                    (r["sheet_id"], key, r["reviewer"], r["verdict"],
                     r["detail_json"], r["signed_at"]))
            conn.execute("DROP TABLE review_signatures_old")
            conn.execute("CREATE INDEX IF NOT EXISTS ix_review_sigs_key "
                         "ON review_signatures(reviewer_key)")
            # 以真实 APPROVED 槽数重算计数（终态状态不回退，只修正投影计数）
            conn.execute("""
                UPDATE review_sheets SET approval_count = (
                    SELECT COUNT(*) FROM review_signatures s
                    WHERE s.sheet_id = review_sheets.id AND s.verdict = 'APPROVED')
            """)

        # 3) review_events.actor_key（重建表，id/流水原样保留）
        ev_cols = _table_columns(conn, "review_events")
        if ev_cols and "actor_key" not in ev_cols:
            conn.execute("DROP TRIGGER IF EXISTS trg_review_events_no_update")
            conn.execute("DROP TRIGGER IF EXISTS trg_review_events_no_delete")
            conn.execute("ALTER TABLE review_events RENAME TO review_events_old")
            conn.execute("""
                CREATE TABLE review_events (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    sheet_id    TEXT NOT NULL REFERENCES review_sheets(id),
                    event_type  TEXT NOT NULL
                                CHECK (event_type IN ('CREATED','CLAIMED','RELEASED',
                                                      'APPROVED','VETOED','WITHDRAWN',
                                                      'APPROVED_COMPLETED','VETOED_COMPLETED',
                                                      'INVALIDATED','LEASE_EXPIRED')),
                    actor       TEXT,
                    actor_key   TEXT,
                    detail_json TEXT NOT NULL DEFAULT '{}',
                    created_at  TEXT NOT NULL
                )""")
            old_events = conn.execute(
                "SELECT id,sheet_id,event_type,actor,detail_json,created_at "
                "FROM review_events_old ORDER BY id").fetchall()
            for r in old_events:
                conn.execute(
                    "INSERT INTO review_events(id,sheet_id,event_type,actor,actor_key,"
                    "detail_json,created_at) VALUES(?,?,?,?,?,?,?)",
                    (r["id"], r["sheet_id"], r["event_type"], r["actor"],
                     _norm_actor(r["actor"]), r["detail_json"], r["created_at"]))
            conn.execute("DROP TABLE review_events_old")
            conn.execute("CREATE INDEX IF NOT EXISTS ix_review_events_sheet "
                         "ON review_events(sheet_id, id)")
            conn.execute("""
                CREATE TRIGGER IF NOT EXISTS trg_review_events_no_update
                BEFORE UPDATE ON review_events
                BEGIN SELECT RAISE(FAIL, 'review_events is append-only'); END
            """)
            conn.execute("""
                CREATE TRIGGER IF NOT EXISTS trg_review_events_no_delete
                BEFORE DELETE ON review_events
                BEGIN SELECT RAISE(FAIL, 'review_events is append-only'); END
            """)
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise


def _migrate_report_rule_pinning(conn: sqlite3.Connection) -> None:
    """旧库迁移：监管报告固定本次使用的规则版本列。

    治理上线前的历史报告统一回填为内置历史口径 builtin:legacy@0；
    新库在 CREATE TABLE 中已含这些列，直接跳过。
    """
    cols = _table_columns(conn, "regulatory_reports")
    if cols and "rule_package_id" in cols:
        return
    conn.execute("BEGIN IMMEDIATE")
    try:
        for ddl, default in (
            ("ALTER TABLE regulatory_reports ADD COLUMN jurisdiction TEXT "
             "NOT NULL DEFAULT 'CN'", None),
            ("ALTER TABLE regulatory_reports ADD COLUMN rule_package_id TEXT "
             "NOT NULL DEFAULT 'builtin:legacy'", None),
            ("ALTER TABLE regulatory_reports ADD COLUMN rule_version TEXT "
             "NOT NULL DEFAULT '0'", None),
        ):
            conn.execute(ddl)
        conn.execute("CREATE INDEX IF NOT EXISTS ix_reports_jurisdiction "
                     "ON regulatory_reports(jurisdiction, status)")
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise


def initialize(db_path: str | Path) -> None:
    """建库建表（可重复执行）。"""
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
        _migrate(conn)
        _migrate_reviewer_identity(conn)
        _migrate_report_rule_pinning(conn)
        # 规范化身份键索引（迁移把旧 review_signatures 重建为带键新表后再建）
        conn.execute("CREATE INDEX IF NOT EXISTS ix_review_sigs_key "
                     "ON review_signatures(reviewer_key)")
        # 新库与迁移后的旧库都在此时补上审计链唯一索引
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_events_case_seq "
                     "ON case_events(case_id, seq)")
    finally:
        conn.close()


@contextmanager
def transaction(conn: sqlite3.Connection):
    """BEGIN IMMEDIATE 写事务：所有写者在数据库层串行化，避免写偏差。

    SQLite 同一时刻只允许一个写事务；配合 busy_timeout，并发的
    合并/拆分/回执写入会排队而不是互相覆盖。
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
