"""SQLite 持久化 schema 与事务工具（独立 inquiry.db）。

持久化不变量由数据库约束兜底（而非只靠应用代码）：

1. 资料 ``materials``、底稿锁定快照 ``draft_pins``、章节修订
   ``section_revisions``、不可变提交件 ``submissions``、回执 ``receipts``、
   审计 ``audit_events``、漂移/矛盾/发送履历全部 *只增*（触发器拒绝
   UPDATE/DELETE；少数状态机列由触发器白名单放行）。
2. 每个问题在一份底稿内至多对应一个章节（``ux_sections_draft_qid``），
   定稿时“每问唯一可追踪回答”由该约束 + 校验共同成立。
3. 外发编号在同一 IMMEDIATE 事务内由序列表分配且
   ``submissions.outgoing_number UNIQUE``；发送幂等履历以
   ``ux_send_event_unique`` 去重——重试/双节点/重启都不产生重复编号、
   重复提交件或重复履历。
4. 作业 ``jobs`` 用条件 UPDATE 领取（``WHERE status='PENDING'`` 或租约过期），
   两个节点同时抢同一答复键只有一个成功；完成时带 attempt 栅栏，
   过期工作者不能覆盖新执行结果。
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path

from .util import connect

SCHEMA = """
PRAGMA foreign_keys=ON;

--------------------------------------------------------------------------------
-- 问询登记
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS inquiries (
    id              TEXT PRIMARY KEY,
    regulator       TEXT NOT NULL,            -- 监管方
    jurisdiction    TEXT NOT NULL,            -- 收件辖区（决定披露策略）
    title           TEXT NOT NULL DEFAULT '',
    questions_json  TEXT NOT NULL,            -- [{qid, text, required_materials?}]
    deadline        TEXT NOT NULL,            -- 截止时间（ISO8601）
    -- 可引用资料范围（登记即冻结）：
    --   allowed_material_ids: ["MAT-1", ...]（空/null=不允许任何资料，显式）
    --   allowed_categories:   ["CASE","SENT_REPORT","REGULATION","STATISTIC"]
    scope_json      TEXT NOT NULL DEFAULT '{}',
    status          TEXT NOT NULL DEFAULT 'OPEN'
                    CHECK (status IN ('OPEN','CLOSED')),
    registered_by   TEXT,
    created_at      TEXT NOT NULL
);

--------------------------------------------------------------------------------
-- 可引用资料注册表（病例 / 已发送报文 / 适用法规 / 统计结论）
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS materials (
    id              TEXT PRIMARY KEY,        -- 本次登记行 id（hex）
    material_id     TEXT NOT NULL,           -- 资料逻辑 id（同一资料多版本共用）
    version         TEXT NOT NULL,
    category        TEXT NOT NULL
                    CHECK (category IN ('CASE','SENT_REPORT','REGULATION','STATISTIC')),
    title           TEXT NOT NULL DEFAULT '',
    content_json    TEXT NOT NULL,
    -- 事实指纹：同 fact_key 不同 fact_value 即为“版本之间互相矛盾”
    fact_key        TEXT,
    fact_value      TEXT,
    content_hash    TEXT NOT NULL,           -- 规范内容 SHA-256
    supersedes_id   TEXT REFERENCES materials(id),  -- 上一版本行
    registered_by   TEXT,
    created_at      TEXT NOT NULL,
    UNIQUE(material_id, version)
);
CREATE INDEX IF NOT EXISTS ix_mat_lookup ON materials(material_id, category);
CREATE INDEX IF NOT EXISTS ix_mat_fact ON materials(fact_key) WHERE fact_key IS NOT NULL;

-- 辖区披露策略（只增版本化：{sensitive_fields:{field: REDACT|PROHIBIT|ALLOW}}）
CREATE TABLE IF NOT EXISTS disclosure_policies (
    id              TEXT PRIMARY KEY,
    jurisdiction    TEXT NOT NULL,
    version         INTEGER NOT NULL,
    rules_json      TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    created_by      TEXT,
    UNIQUE(jurisdiction, version)
);
CREATE INDEX IF NOT EXISTS ix_policy_jur ON disclosure_policies(jurisdiction, version);

--------------------------------------------------------------------------------
-- 答复底稿（草稿；精确版本锁定）
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS drafts (
    id              TEXT PRIMARY KEY,
    inquiry_id      TEXT NOT NULL REFERENCES inquiries(id),
    kind            TEXT NOT NULL CHECK (kind IN ('BASE','CANDIDATE','CORRECTION')),
    status          TEXT NOT NULL
                    CHECK (status IN ('OPEN','FINALIZED','SUPERSEDED')),
    parent_draft_id TEXT REFERENCES drafts(id),       -- CANDIDATE/CORRECTION 的来源
    correction_of_submission_id TEXT REFERENCES submissions(id), -- 更正件指向
    drift_reason    TEXT,                             -- 候选/更正产生原因
    created_from_material_changes_json TEXT NOT NULL DEFAULT '[]',
    locked_at       TEXT NOT NULL,
    created_by      TEXT,
    finalized_at    TEXT,
    submission_id   TEXT
);
CREATE INDEX IF NOT EXISTS ix_drafts_inquiry ON drafts(inquiry_id, status);
CREATE INDEX IF NOT EXISTS ix_drafts_parent ON drafts(parent_draft_id);

-- 底稿锁定的精确资料版本（不可变快照）。ref 与 content_hash 在锁定时冻结，
-- 此后资料注册表再登记新版本不影响本草——引用在组稿期间变化不会悄悄混入。
CREATE TABLE IF NOT EXISTS draft_pins (
    id              TEXT PRIMARY KEY,
    draft_id        TEXT NOT NULL REFERENCES drafts(id),
    material_id     TEXT NOT NULL,           -- 逻辑 id
    material_row_id TEXT NOT NULL REFERENCES materials(id),
    version         TEXT NOT NULL,
    category        TEXT NOT NULL,
    ref             TEXT NOT NULL,           -- 冻结引用锚：CASE:<mid>@<ver>
    content_hash    TEXT NOT NULL,
    locked_fact_key TEXT,
    locked_fact_value TEXT,
    UNIQUE(draft_id, material_id)
);
CREATE INDEX IF NOT EXISTS ix_pins_draft ON draft_pins(draft_id);
CREATE INDEX IF NOT EXISTS ix_pins_material ON draft_pins(material_row_id);

-- 章节（可独立编辑；一章节回答一个问题）
CREATE TABLE IF NOT EXISTS sections (
    id              TEXT PRIMARY KEY,
    draft_id        TEXT NOT NULL REFERENCES drafts(id),
    qid             TEXT NOT NULL,
    title           TEXT NOT NULL DEFAULT '',
    body            TEXT NOT NULL DEFAULT '',
    revision        INTEGER NOT NULL DEFAULT 0,   -- 乐观修订号（每次提交 +1）
    updated_by      TEXT,
    updated_at      TEXT NOT NULL,
    UNIQUE(draft_id, qid)
);
CREATE INDEX IF NOT EXISTS ix_sections_draft ON sections(draft_id);

-- 章节修订历史（只增；过期提交被拒绝的冲突也留痕）
CREATE TABLE IF NOT EXISTS section_revisions (
    id              TEXT PRIMARY KEY,
    section_id      TEXT NOT NULL REFERENCES sections(id),
    revision        INTEGER NOT NULL,
    body            TEXT NOT NULL,
    title           TEXT NOT NULL DEFAULT '',
    submitted_by    TEXT,
    submitted_at    TEXT NOT NULL,
    UNIQUE(section_id, revision)
);

-- 竞争更新冲突：以调用方看到的修订为前提，过期提交不覆盖，记录于此
CREATE TABLE IF NOT EXISTS section_conflicts (
    id              TEXT PRIMARY KEY,
    section_id      TEXT NOT NULL REFERENCES sections(id),
    expected_revision INTEGER NOT NULL,
    actual_revision INTEGER NOT NULL,
    attempted_by    TEXT,
    body_preview    TEXT NOT NULL DEFAULT '',
    reason          TEXT NOT NULL,
    created_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_conflict_section ON section_conflicts(section_id);

-- 附件（按底稿冻结；derived_from_material 必须指向已锁定资料）
CREATE TABLE IF NOT EXISTS attachments (
    id              TEXT NOT NULL,           -- 稳定附件锚（跨候选/更正克隆保留，
                                             -- 正文 @attachment:<id> 引用不断）
    draft_id        TEXT NOT NULL REFERENCES drafts(id),
    filename        TEXT NOT NULL,
    content_type    TEXT NOT NULL DEFAULT 'application/octet-stream',
    content_blob    BLOB NOT NULL,
    sha256          TEXT NOT NULL,
    derived_from_ref TEXT,                   -- 如 CASE:MAT-1@3
    uploaded_by     TEXT,
    created_at      TEXT NOT NULL,
    PRIMARY KEY (draft_id, id)
);
CREATE INDEX IF NOT EXISTS ix_attach_draft ON attachments(draft_id);

-- 组稿期间引用漂移事件（资料新版本到达 -> 自动产生带原因的新底稿候选）
CREATE TABLE IF NOT EXISTS drift_events (
    id              TEXT PRIMARY KEY,
    inquiry_id      TEXT NOT NULL REFERENCES inquiries(id),
    source_draft_id TEXT NOT NULL REFERENCES drafts(id),
    candidate_draft_id TEXT NOT NULL REFERENCES drafts(id),
    material_id     TEXT NOT NULL,
    old_version     TEXT NOT NULL,
    new_version     TEXT NOT NULL,
    reason          TEXT NOT NULL,
    created_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_drift_inquiry ON drift_events(inquiry_id);

-- 底稿内被引用版本之间的矛盾登记（只增；锁定与定稿都会复核）
CREATE TABLE IF NOT EXISTS contradictions (
    id              TEXT PRIMARY KEY,
    draft_id        TEXT NOT NULL REFERENCES drafts(id),
    fact_key        TEXT NOT NULL,
    ref_a           TEXT NOT NULL,
    value_a         TEXT NOT NULL,
    ref_b           TEXT NOT NULL,
    value_b         TEXT NOT NULL,
    detected_at     TEXT NOT NULL,
    UNIQUE(draft_id, fact_key, ref_a, ref_b)
);

--------------------------------------------------------------------------------
-- 不可变提交件 / 发送履历 / 回执
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS submissions (
    id                  TEXT PRIMARY KEY,
    draft_id            TEXT NOT NULL UNIQUE REFERENCES drafts(id),
    inquiry_id          TEXT NOT NULL REFERENCES inquiries(id),
    kind                TEXT NOT NULL CHECK (kind IN ('ORIGINAL','CORRECTION')),
    corrects_submission_id TEXT REFERENCES submissions(id),
    jurisdiction        TEXT NOT NULL,
    outgoing_number     TEXT NOT NULL UNIQUE,
    status              TEXT NOT NULL
                        CHECK (status IN ('SEALED','QUEUED','SENDING','SENT',
                                          'DELIVERED','FAILED','REJECTED')),
    manifest_json       TEXT NOT NULL,      -- 不可变清单（含每问 answer_uid）
    manifest_hash       TEXT NOT NULL,      -- 提交件内容指纹
    sealed_by           TEXT,
    sealed_at           TEXT NOT NULL,
    attempts            INTEGER NOT NULL DEFAULT 0,
    last_error          TEXT,
    sent_at             TEXT,
    created_at          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_sub_inquiry ON submissions(inquiry_id);
CREATE INDEX IF NOT EXISTS ix_sub_corrects ON submissions(corrects_submission_id);
-- 每个问询至多一份“原始提交件”（更正件另走 CORRECTION 链）
CREATE UNIQUE INDEX IF NOT EXISTS ux_sub_original_per_inquiry
    ON submissions(inquiry_id) WHERE kind='ORIGINAL';

-- 发送履历（去重：同一提交件同一阶段只落一条，重试不制造重复履历）
CREATE TABLE IF NOT EXISTS submission_events (
    id              TEXT PRIMARY KEY,
    submission_id   TEXT NOT NULL REFERENCES submissions(id),
    event_type      TEXT NOT NULL
                    CHECK (event_type IN ('QUEUED','SENDING','SENT','FAILED',
                                          'RETRY','REJECTED','RECEIPT_REGISTERED')),
    detail_json     TEXT NOT NULL DEFAULT '{}',
    actor           TEXT,
    created_at      TEXT NOT NULL,
    UNIQUE(submission_id, event_type, detail_json)
);
CREATE INDEX IF NOT EXISTS ix_sub_ev ON submission_events(submission_id, id);

-- 监管回执（以回执号幂等；登记时与提交件交叉核对）
CREATE TABLE IF NOT EXISTS receipts (
    id                  TEXT PRIMARY KEY,
    receipt_number      TEXT NOT NULL UNIQUE,
    submission_id       TEXT NOT NULL REFERENCES submissions(id),
    outgoing_number     TEXT NOT NULL,
    regulator           TEXT NOT NULL,
    payload_json        TEXT NOT NULL,
    received_at         TEXT NOT NULL,
    registered_by       TEXT,
    created_at          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_receipt_sub ON receipts(submission_id);

--------------------------------------------------------------------------------
-- 可恢复作业 / 外发编号序列
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS jobs (
    id              TEXT PRIMARY KEY,
    job_type        TEXT NOT NULL CHECK (job_type IN ('LOCK_DRAFT','VALIDATE',
                                                      'FINALIZE','SEND')),
    inquiry_id      TEXT NOT NULL REFERENCES inquiries(id),
    -- 业务幂等键（如 SEND:<submission_id>）：重复请求/双节点只产生一个作业
    idempotency_key TEXT NOT NULL UNIQUE,
    target_draft_id TEXT,
    target_submission_id TEXT,
    payload_json    TEXT NOT NULL DEFAULT '{}',
    status          TEXT NOT NULL
                    CHECK (status IN ('PENDING','RUNNING','COMPLETED','FAILED')),
    attempt         INTEGER NOT NULL DEFAULT 0,    -- 领取栅栏（每次领取 +1）
    lease_owner     TEXT,
    leased_at       TEXT,
    lease_expires_at TEXT,
    checkpoint_json TEXT NOT NULL DEFAULT '{}',
    result_json     TEXT,
    error_code      TEXT,
    error_message   TEXT,
    permanent       INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    finished_at     TEXT
);
CREATE INDEX IF NOT EXISTS ix_jobs_status ON jobs(status, lease_expires_at);
CREATE INDEX IF NOT EXISTS ix_jobs_type ON jobs(job_type, inquiry_id);

CREATE TABLE IF NOT EXISTS outgoing_number_seq (
    scope   TEXT PRIMARY KEY,   -- <jurisdiction>:<year>
    last_no INTEGER NOT NULL DEFAULT 0
);

--------------------------------------------------------------------------------
-- 操作留痕（全局只增哈希链）
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit_events (
    id              TEXT PRIMARY KEY,
    seq             INTEGER NOT NULL UNIQUE,
    event_type      TEXT NOT NULL,
    actor           TEXT,
    inquiry_id      TEXT,
    draft_id        TEXT,
    submission_id   TEXT,
    job_id          TEXT,
    summary         TEXT NOT NULL,
    payload_json    TEXT NOT NULL DEFAULT '{}',
    idempotency_key TEXT UNIQUE,
    prev_hash       TEXT NOT NULL,
    entry_hash      TEXT NOT NULL,
    created_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_audit_inquiry ON audit_events(inquiry_id, seq);
CREATE INDEX IF NOT EXISTS ix_audit_type ON audit_events(event_type, seq);

--------------------------------------------------------------------------------
-- 不可变性触发器
--------------------------------------------------------------------------------
CREATE TRIGGER IF NOT EXISTS trg_inquiries_no_update BEFORE UPDATE ON inquiries
BEGIN SELECT RAISE(FAIL, 'inquiries is append-only (scope/questions are frozen)'); END;
CREATE TRIGGER IF NOT EXISTS trg_inquiries_no_delete BEFORE DELETE ON inquiries
BEGIN SELECT RAISE(FAIL, 'inquiries is append-only'); END;

CREATE TRIGGER IF NOT EXISTS trg_materials_no_update BEFORE UPDATE ON materials
BEGIN SELECT RAISE(FAIL, 'materials is append-only (register a new version)'); END;
CREATE TRIGGER IF NOT EXISTS trg_materials_no_delete BEFORE DELETE ON materials
BEGIN SELECT RAISE(FAIL, 'materials is append-only'); END;

CREATE TRIGGER IF NOT EXISTS trg_policies_no_update BEFORE UPDATE ON disclosure_policies
BEGIN SELECT RAISE(FAIL, 'disclosure_policies is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_policies_no_delete BEFORE DELETE ON disclosure_policies
BEGIN SELECT RAISE(FAIL, 'disclosure_policies is append-only'); END;

CREATE TRIGGER IF NOT EXISTS trg_pins_no_update BEFORE UPDATE ON draft_pins
BEGIN SELECT RAISE(FAIL, 'draft_pins are immutable'); END;
CREATE TRIGGER IF NOT EXISTS trg_pins_no_delete BEFORE DELETE ON draft_pins
BEGIN SELECT RAISE(FAIL, 'draft_pins are immutable'); END;

-- 底稿只允许状态机推进：OPEN -> FINALIZED/SUPERSESED；
-- 定稿时写 finalized_at/submission_id，其余字段禁改。
CREATE TRIGGER IF NOT EXISTS trg_drafts_guard BEFORE UPDATE ON drafts
WHEN NOT (
    NEW.id IS OLD.id AND NEW.inquiry_id IS OLD.inquiry_id AND NEW.kind IS OLD.kind
    AND NEW.parent_draft_id IS OLD.parent_draft_id
    AND NEW.correction_of_submission_id IS OLD.correction_of_submission_id
    AND NEW.drift_reason IS OLD.drift_reason
    AND NEW.created_from_material_changes_json IS OLD.created_from_material_changes_json
    AND NEW.locked_at IS OLD.locked_at AND NEW.created_by IS OLD.created_by
    AND OLD.status='OPEN' AND NEW.status IN ('FINALIZED','SUPERSEDED')
    AND (NEW.submission_id IS NOT NULL OR NEW.status='SUPERSEDED')
)
BEGIN SELECT RAISE(FAIL, 'drafts: only OPEN -> FINALIZED/SUPERSEDED allowed'); END;
CREATE TRIGGER IF NOT EXISTS trg_drafts_no_delete BEFORE DELETE ON drafts
BEGIN SELECT RAISE(FAIL, 'drafts is append-only'); END;

CREATE TRIGGER IF NOT EXISTS trg_section_rev_no_update BEFORE UPDATE ON section_revisions
BEGIN SELECT RAISE(FAIL, 'section_revisions is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_section_rev_no_delete BEFORE DELETE ON section_revisions
BEGIN SELECT RAISE(FAIL, 'section_revisions is append-only'); END;

CREATE TRIGGER IF NOT EXISTS trg_conflicts_no_update BEFORE UPDATE ON section_conflicts
BEGIN SELECT RAISE(FAIL, 'section_conflicts is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_conflicts_no_delete BEFORE DELETE ON section_conflicts
BEGIN SELECT RAISE(FAIL, 'section_conflicts is append-only'); END;

CREATE TRIGGER IF NOT EXISTS trg_attach_no_update BEFORE UPDATE ON attachments
BEGIN SELECT RAISE(FAIL, 'attachments are immutable (re-upload on a new draft)'); END;
CREATE TRIGGER IF NOT EXISTS trg_attach_no_delete BEFORE DELETE ON attachments
BEGIN SELECT RAISE(FAIL, 'attachments are immutable'); END;

CREATE TRIGGER IF NOT EXISTS trg_drift_no_update BEFORE UPDATE ON drift_events
BEGIN SELECT RAISE(FAIL, 'drift_events is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_drift_no_delete BEFORE DELETE ON drift_events
BEGIN SELECT RAISE(FAIL, 'drift_events is append-only'); END;

CREATE TRIGGER IF NOT EXISTS trg_contrad_no_update BEFORE UPDATE ON contradictions
BEGIN SELECT RAISE(FAIL, 'contradictions is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_contrad_no_delete BEFORE DELETE ON contradictions
BEGIN SELECT RAISE(FAIL, 'contradictions is append-only'); END;

-- 提交件不可变：只允许发送状态机与 attempts/last_error/sent_at 更新；
-- manifest/outgoing_number/kind 等任何定稿内容禁改（更正只能产生新提交件）。
CREATE TRIGGER IF NOT EXISTS trg_submissions_guard BEFORE UPDATE ON submissions
WHEN NOT (
    NEW.id IS OLD.id AND NEW.draft_id IS OLD.draft_id
    AND NEW.inquiry_id IS OLD.inquiry_id AND NEW.kind IS OLD.kind
    AND NEW.corrects_submission_id IS OLD.corrects_submission_id
    AND NEW.jurisdiction IS OLD.jurisdiction
    AND NEW.outgoing_number IS OLD.outgoing_number
    AND NEW.manifest_json IS OLD.manifest_json AND NEW.manifest_hash IS OLD.manifest_hash
    AND NEW.sealed_by IS OLD.sealed_by AND NEW.sealed_at IS OLD.sealed_at
    AND NEW.created_at IS OLD.created_at
    AND NEW.status IN ('SEALED','QUEUED','SENDING','SENT','DELIVERED','FAILED','REJECTED')
)
BEGIN SELECT RAISE(FAIL, 'submissions are immutable except delivery state'); END;
CREATE TRIGGER IF NOT EXISTS trg_submissions_no_delete BEFORE DELETE ON submissions
BEGIN SELECT RAISE(FAIL, 'submissions are immutable'); END;

CREATE TRIGGER IF NOT EXISTS trg_sub_events_no_update BEFORE UPDATE ON submission_events
BEGIN SELECT RAISE(FAIL, 'submission_events is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_sub_events_no_delete BEFORE DELETE ON submission_events
BEGIN SELECT RAISE(FAIL, 'submission_events is append-only'); END;

CREATE TRIGGER IF NOT EXISTS trg_receipts_no_update BEFORE UPDATE ON receipts
BEGIN SELECT RAISE(FAIL, 'receipts is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_receipts_no_delete BEFORE DELETE ON receipts
BEGIN SELECT RAISE(FAIL, 'receipts is append-only'); END;

-- 作业只允许状态机字段更新（领取/心跳/完成）与 FAILED 行的幂等键改名留痕
-- （校验失败补齐后重新入队：旧 FAILED 行改名保留可查，新行占用原幂等键），
-- 其余冻结。
CREATE TRIGGER IF NOT EXISTS trg_jobs_guard BEFORE UPDATE ON jobs
WHEN NOT (
    (
        NEW.id IS OLD.id AND NEW.job_type IS OLD.job_type
        AND NEW.inquiry_id IS OLD.inquiry_id
        AND NEW.idempotency_key IS OLD.idempotency_key
        AND NEW.target_draft_id IS OLD.target_draft_id
        AND NEW.target_submission_id IS OLD.target_submission_id
        AND NEW.payload_json IS OLD.payload_json AND NEW.created_at IS OLD.created_at
    ) OR (
        OLD.status='FAILED' AND NEW.status IS OLD.status
        AND NEW.id IS OLD.id AND NEW.job_type IS OLD.job_type
        AND NEW.inquiry_id IS OLD.inquiry_id
        AND NEW.target_draft_id IS OLD.target_draft_id
        AND NEW.target_submission_id IS OLD.target_submission_id
        AND NEW.payload_json IS OLD.payload_json
        AND NEW.idempotency_key LIKE OLD.idempotency_key || '#failed-%'
        AND NEW.created_at IS OLD.created_at
    )
)
BEGIN SELECT RAISE(FAIL, 'jobs: only execution-state fields may change'); END;
CREATE TRIGGER IF NOT EXISTS trg_jobs_no_delete BEFORE DELETE ON jobs
BEGIN SELECT RAISE(FAIL, 'jobs is append-only'); END;

CREATE TRIGGER IF NOT EXISTS trg_audit_no_update BEFORE UPDATE ON audit_events
BEGIN SELECT RAISE(FAIL, 'audit_events is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_audit_no_delete BEFORE DELETE ON audit_events
BEGIN SELECT RAISE(FAIL, 'audit_events is append-only'); END;
"""


def initialize(db_path: str | Path) -> None:
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()


@contextmanager
def transaction(conn: sqlite3.Connection):
    """BEGIN IMMEDIATE 写事务：多工作节点在数据库写锁上全局串行。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
