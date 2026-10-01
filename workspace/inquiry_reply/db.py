"""SQLite 持久化：schema、连接工厂、事务工具。

关键持久化不变量（由数据库约束保证，而不是只靠应用代码）：

1. evidence_versions 资料版本只增不改 —— 底稿锁定的精确版本永远可取回核对。
2. drafts 锁定内容不可改：触发器只允许 status 推进（OPEN→SUPERSEDED/FINALIZED），
   citations_json 等锁定内容任何 UPDATE 都被数据库拒绝 —— “组稿期间引用发生
   变化不能悄悄混入当前答复”由数据库兜底。
3. submissions 提交件不可变：触发器只允许 status 推进，payload/外发编号永不
   改写；draft_id UNIQUE 保证同一底稿至多一份提交件；outbound_no UNIQUE
   保证外发编号不重复。
4. receipts / outbox / job_steps / audit_log 只增不改 —— 回执、发件记录、
   作业履历、操作留痕永不被覆盖。
5. jobs.job_key UNIQUE：同一答复键的重复请求 / 两个工作节点并发只产生一个
   作业；外发编号在 finalize 步骤事务内由 sequences 表分配 —— 重试、崩溃
   恢复、双节点竞争都不可能产生重复提交件或重复外发编号。
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- 可引用资料库（病例 / 已发送报文 / 适用法规 / 统计结论），版本只增 ----------
CREATE TABLE IF NOT EXISTS evidence_versions (
    evidence_id  TEXT NOT NULL,
    version      INTEGER NOT NULL,
    kind         TEXT NOT NULL CHECK (kind IN ('CASE','MESSAGE','REGULATION','STATISTIC')),
    content      TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    updated_at   TEXT NOT NULL,
    PRIMARY KEY (evidence_id, version)
);

-- 问询登记（问题清单 + 截止时间 + 可引用资料范围）----------------------------
CREATE TABLE IF NOT EXISTS inquiries (
    inquiry_id      TEXT PRIMARY KEY,
    regulator       TEXT NOT NULL,
    jurisdiction    TEXT NOT NULL,          -- 收件辖区（决定披露策略）
    deadline        TEXT NOT NULL,
    questions_json  TEXT NOT NULL,          -- [{question_id, text}]
    scope_json      TEXT NOT NULL,          -- 可引用范围：精确 id 或 "前缀*"
    status          TEXT NOT NULL DEFAULT 'OPEN' CHECK (status IN ('OPEN','ANSWERED')),
    idempotency_key TEXT UNIQUE,            -- 重复登记请求幂等
    created_at      TEXT NOT NULL
);

-- 答复底稿（引用版本在锁定时冻结；资料变化只能产生带原因的新候选）------------
CREATE TABLE IF NOT EXISTS drafts (
    draft_id         TEXT PRIMARY KEY,
    inquiry_id       TEXT NOT NULL REFERENCES inquiries(inquiry_id),
    seq              INTEGER NOT NULL,
    kind             TEXT NOT NULL CHECK (kind IN ('ORIGINAL','REFRESH','CORRECTION')),
    status           TEXT NOT NULL DEFAULT 'OPEN'
                     CHECK (status IN ('OPEN','SUPERSEDED','FINALIZED')),
    citations_json   TEXT NOT NULL,         -- [{evidence_id, kind, version, content_hash}]
    reasons_json     TEXT NOT NULL DEFAULT '[]',  -- 产生本候选的原因
    based_on_draft_id TEXT,
    corrects_submission_id TEXT,
    created_at       TEXT NOT NULL,
    UNIQUE(inquiry_id, seq)
);

-- 章节（多人协作；revision 乐观并发，过期提交不得覆盖新内容）----------------
CREATE TABLE IF NOT EXISTS sections (
    draft_id    TEXT NOT NULL REFERENCES drafts(draft_id),
    section_key TEXT NOT NULL,
    question_id TEXT,                       -- 该章节回答的问询问题（可空=非答题章节）
    content     TEXT NOT NULL,
    revision    INTEGER NOT NULL,
    author      TEXT NOT NULL,
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (draft_id, section_key)
);

-- 附件（正文以 [[att:id]] 引用，定稿校验双向一致）--------------------------
CREATE TABLE IF NOT EXISTS attachments (
    draft_id      TEXT NOT NULL REFERENCES drafts(draft_id),
    attachment_id TEXT NOT NULL,
    name          TEXT NOT NULL,
    content       TEXT NOT NULL,
    content_hash  TEXT NOT NULL,
    revision      INTEGER NOT NULL,
    updated_at    TEXT NOT NULL,
    PRIMARY KEY (draft_id, attachment_id)
);

-- 不可变提交件（定稿即冻结；后续改动只能产生更正件）------------------------
CREATE TABLE IF NOT EXISTS submissions (
    submission_id  TEXT PRIMARY KEY,
    draft_id       TEXT NOT NULL UNIQUE REFERENCES drafts(draft_id),
    inquiry_id     TEXT NOT NULL REFERENCES inquiries(inquiry_id),
    outbound_no    TEXT NOT NULL UNIQUE,    -- 外发编号（序列表事务内分配）
    corrects_submission_id TEXT REFERENCES submissions(submission_id),
    payload_json   TEXT NOT NULL,
    payload_hash   TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'FINALIZED'
                   CHECK (status IN ('FINALIZED','SENT','ACKED','CORRECTED')),
    created_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_submissions_inquiry ON submissions(inquiry_id, created_at);

-- 监管回执（只增；按监管方引用号幂等）---------------------------------------
CREATE TABLE IF NOT EXISTS receipts (
    receipt_id    TEXT PRIMARY KEY,
    submission_id TEXT NOT NULL REFERENCES submissions(submission_id),
    regulator_ref TEXT NOT NULL,
    payload_json  TEXT NOT NULL,
    received_at   TEXT NOT NULL,
    UNIQUE(submission_id, regulator_ref)
);

-- 外发编号序列（在 finalize 步骤事务内原子推进）-----------------------------
CREATE TABLE IF NOT EXISTS sequences (
    name  TEXT PRIMARY KEY,
    value INTEGER NOT NULL DEFAULT 0
);

-- 可恢复作业（组稿定稿发送；检查点续跑；job_key 幂等；租约原子领取）----------
CREATE TABLE IF NOT EXISTS jobs (
    job_id       TEXT PRIMARY KEY,
    job_key      TEXT NOT NULL UNIQUE,      -- 同一答复键全局唯一
    type         TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'PENDING'
                 CHECK (status IN ('PENDING','RUNNING','DONE','FAILED')),
    checkpoint   INTEGER NOT NULL DEFAULT 0, -- 下一个待执行步骤序号
    payload_json TEXT NOT NULL,
    result_json  TEXT,
    error_json   TEXT,
    attempts     INTEGER NOT NULL DEFAULT 0,
    claimed_by   TEXT,
    lease_until  TEXT,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_jobs_status ON jobs(status);

-- 作业步骤履历（只增；与步骤效果、检查点同事务提交）
CREATE TABLE IF NOT EXISTS job_steps (
    job_id       TEXT NOT NULL REFERENCES jobs(job_id),
    step_index   INTEGER NOT NULL,
    step_name    TEXT NOT NULL,
    detail_json  TEXT NOT NULL DEFAULT '{}',
    completed_at TEXT NOT NULL,
    PRIMARY KEY (job_id, step_index)
);

-- 发件箱（默认传输通道；outbound_no 为幂等键）-------------------------------
CREATE TABLE IF NOT EXISTS outbox (
    outbound_no   TEXT PRIMARY KEY,
    submission_id TEXT NOT NULL UNIQUE REFERENCES submissions(submission_id),
    sent_at       TEXT NOT NULL
);

-- 操作留痕（只增）-----------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT NOT NULL,
    actor       TEXT NOT NULL,
    action      TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id   TEXT NOT NULL,
    detail_json TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS ix_audit_entity ON audit_log(entity_type, entity_id, id);

--------------------------------------------------------------------------------
-- 不可变性触发器
--------------------------------------------------------------------------------
CREATE TRIGGER IF NOT EXISTS trg_evidence_no_update BEFORE UPDATE ON evidence_versions
BEGIN SELECT RAISE(FAIL, 'evidence_versions is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_evidence_no_delete BEFORE DELETE ON evidence_versions
BEGIN SELECT RAISE(FAIL, 'evidence_versions is append-only'); END;

-- 底稿：锁定内容不可改，仅允许 status 推进
CREATE TRIGGER IF NOT EXISTS trg_drafts_guard BEFORE UPDATE ON drafts
WHEN NOT (
    NEW.draft_id IS OLD.draft_id
    AND NEW.inquiry_id IS OLD.inquiry_id
    AND NEW.seq IS OLD.seq
    AND NEW.kind IS OLD.kind
    AND NEW.citations_json IS OLD.citations_json
    AND NEW.reasons_json IS OLD.reasons_json
    AND NEW.based_on_draft_id IS OLD.based_on_draft_id
    AND NEW.corrects_submission_id IS OLD.corrects_submission_id
    AND NEW.created_at IS OLD.created_at
)
BEGIN SELECT RAISE(FAIL, 'drafts: locked citations are immutable; only status may advance'); END;
CREATE TRIGGER IF NOT EXISTS trg_drafts_no_delete BEFORE DELETE ON drafts
BEGIN SELECT RAISE(FAIL, 'drafts is append-only'); END;

-- 提交件：payload 与外发编号不可变，仅允许 status 推进
CREATE TRIGGER IF NOT EXISTS trg_submissions_guard BEFORE UPDATE ON submissions
WHEN NOT (
    NEW.submission_id IS OLD.submission_id
    AND NEW.draft_id IS OLD.draft_id
    AND NEW.inquiry_id IS OLD.inquiry_id
    AND NEW.outbound_no IS OLD.outbound_no
    AND NEW.corrects_submission_id IS OLD.corrects_submission_id
    AND NEW.payload_json IS OLD.payload_json
    AND NEW.payload_hash IS OLD.payload_hash
    AND NEW.created_at IS OLD.created_at
)
BEGIN SELECT RAISE(FAIL, 'submissions: payload is immutable; only status may advance'); END;
CREATE TRIGGER IF NOT EXISTS trg_submissions_no_delete BEFORE DELETE ON submissions
BEGIN SELECT RAISE(FAIL, 'submissions is append-only'); END;

CREATE TRIGGER IF NOT EXISTS trg_receipts_no_update BEFORE UPDATE ON receipts
BEGIN SELECT RAISE(FAIL, 'receipts is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_receipts_no_delete BEFORE DELETE ON receipts
BEGIN SELECT RAISE(FAIL, 'receipts is append-only'); END;

CREATE TRIGGER IF NOT EXISTS trg_outbox_no_update BEFORE UPDATE ON outbox
BEGIN SELECT RAISE(FAIL, 'outbox is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_outbox_no_delete BEFORE DELETE ON outbox
BEGIN SELECT RAISE(FAIL, 'outbox is append-only'); END;

CREATE TRIGGER IF NOT EXISTS trg_job_steps_no_update BEFORE UPDATE ON job_steps
BEGIN SELECT RAISE(FAIL, 'job_steps is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_job_steps_no_delete BEFORE DELETE ON job_steps
BEGIN SELECT RAISE(FAIL, 'job_steps is append-only'); END;

CREATE TRIGGER IF NOT EXISTS trg_audit_no_update BEFORE UPDATE ON audit_log
BEGIN SELECT RAISE(FAIL, 'audit_log is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_audit_no_delete BEFORE DELETE ON audit_log
BEGIN SELECT RAISE(FAIL, 'audit_log is append-only'); END;
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


def initialize(db_path: str | Path) -> None:
    """建库建表（可重复执行）。"""
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()


@contextmanager
def transaction(conn: sqlite3.Connection):
    """BEGIN IMMEDIATE 写事务：所有写者在数据库层串行化，避免写偏差。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
