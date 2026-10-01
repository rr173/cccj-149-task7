"""监管问询答复编排器（独立模块）。

一次完整生命周期：

1. **问询登记** ``register_inquiry``：记录监管方问题清单、截止时间与
   可引用资料范围（精确 id 或 ``前缀*``）；幂等键去重重复登记；
2. **底稿锁定** ``lock_draft``：把病例 / 已发送报文 / 适用法规 / 统计结论的
   *精确版本*（版本号 + 内容哈希）冻结成答复底稿；组稿期间资料一旦变化，
   ``refresh_draft`` 生成 **带原因的新底稿候选**，旧底稿锁定内容原样保留
   （数据库触发器兜底），变化绝不悄悄混入当前答复；
3. **章节协作** ``submit_section`` / ``put_attachment``：多名撰写人按
   section_key 独立提交；乐观修订号（base_revision）保证同一章节的过期
   提交不能覆盖新内容（409 STALE_REVISION）；
4. **冲突查询** ``query_conflicts``：引用漂移 / 资料缺失 / 引用越权 /
   版本矛盾 / 未锁定引用，一屏列出；
5. **定稿校验** ``validate_draft``：资料缺失、引用越权、版本矛盾、问题未
   唯一可追踪回答、附件与正文引用不一致、敏感字段违反收件辖区披露策略 ——
   任一不满足即阻止定稿，并准确列出尚未满足的问题；
6. **发送与重试** ``finalize_and_send``：可恢复作业（validate → finalize →
   send），每步与其检查点在同一事务提交，意外中断后从断点续跑；
   ``jobs.job_key`` 幂等 + 租约原子领取，重复请求或两个工作节点同时处理
   同一答复键不会产生重复提交件 / 外发编号 / 履历记录；
7. **回执登记** ``register_receipt``：监管回执按 (提交件, 监管方引用号)
   幂等留档；
8. **更正关系** ``create_correction`` / ``correction_chain``：定稿后的任何
   改动只能产生更正件（新底稿 → 新提交件，关联原提交件），原提交件与
   监管回执永久保留；
9. **独立核对** ``independent_check``：不信任服务侧标志位，从存储快照
   重算哈希、重跑校验规则、核对作业履历与审计轨迹；
10. **操作留痕** ``audit_trail``：全部写操作只增留痕（触发器兜底）。

并发模型：进程内共享 RLock + SQLite ``BEGIN IMMEDIATE`` 数据库写锁；
作业领取是单条原子 UPDATE，跨进程 / 跨节点也只会有一个获胜者。
"""
from __future__ import annotations

import json
import re
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta

from . import db as dbmod
from .errors import DomainError
from .evidence import EvidenceStore, current_version as ev_current_version
from .evidence import fetch as ev_fetch
from .evidence import sha256_text
from .policy import BASE_POLICY, DEFAULT_POLICIES, DisclosurePolicy, SensitiveRule
from .transport import OutboxTransport

# 正文内联引用标记：[[ev:资料id]] / [[ev:资料id@版本]] / [[att:附件id]]
EV_TOKEN = re.compile(r"\[\[ev:([0-9A-Za-z_.:-]+?)(?:@(\d+))?\]\]")
ATT_TOKEN = re.compile(r"\[\[att:([0-9A-Za-z_.:-]+?)\]\]")

#: 可恢复作业的步骤编排
JOB_STEPS = {"FINALIZE_AND_SEND": ("validate", "finalize", "send")}

_UNSET = object()  # submit_section 的 question_id 缺省哨兵（区分“未传”与“置空”）


def _uid() -> str:
    return uuid.uuid4().hex


def _json(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), default=str)


def _issue(code: str, message: str, **detail) -> dict:
    return {"code": code, "message": message, "detail": detail}


def _mask(value: str) -> str:
    return (value[:3] + "***") if len(value) > 3 else "***"


def _parse_ev_tokens(content: str) -> list[tuple[str, int | None]]:
    return [(m.group(1), int(m.group(2)) if m.group(2) else None)
            for m in EV_TOKEN.finditer(content or "")]


def _parse_att_tokens(content: str) -> list[str]:
    return [m.group(1) for m in ATT_TOKEN.finditer(content or "")]


def _scope_allows(scope: list[str], evidence_id: str) -> bool:
    for entry in scope:
        if entry.endswith("*"):
            if evidence_id.startswith(entry[:-1]):
                return True
        elif evidence_id == entry:
            return True
    return False


class Clock:
    def now(self) -> datetime:
        return datetime.now()

    def now_iso(self) -> str:
        return self.now().isoformat(timespec="seconds")


class FixedClock(Clock):
    """可推进的确定性时钟（测试/演示保证行为可复现）。"""

    def __init__(self, start: datetime):
        self._now = start

    def now(self) -> datetime:
        return self._now

    def advance(self, **kwargs):
        self._now += timedelta(**kwargs)

    def set(self, dt: datetime):
        self._now = dt


class InquiryReplyService:
    """监管问询答复编排器。"""

    def __init__(self, db_path: str, *, clock: Clock | None = None,
                 transport=None, worker_id: str | None = None,
                 lease_seconds: int = 30, shared_lock=None, policies=None):
        self.db_path = str(db_path)
        self.clock = clock or Clock()
        self.transport = transport or OutboxTransport()
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.lease_seconds = lease_seconds
        self._lock = shared_lock or threading.RLock()
        self._policies = dict(DEFAULT_POLICIES)
        if policies:
            self._policies.update(policies)
        dbmod.initialize(self.db_path)
        self.evidence = EvidenceStore(self.db_path, clock=self.clock)

    # ------------------------------------------------------------------ 基础
    @contextmanager
    def _conn(self):
        conn = dbmod.connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def _write_tx(self):
        with self._lock, self._conn() as conn:
            with dbmod.transaction(conn):
                yield conn

    def _now(self) -> str:
        return self.clock.now_iso()

    def _lease_until(self) -> str:
        return (self.clock.now() + timedelta(seconds=self.lease_seconds)) \
            .isoformat(timespec="seconds")

    def _audit(self, conn, actor: str, action: str,
               entity_type: str, entity_id: str, detail=None):
        conn.execute(
            "INSERT INTO audit_log(ts,actor,action,entity_type,entity_id,detail_json)"
            " VALUES(?,?,?,?,?,?)",
            (self._now(), actor, action, entity_type, entity_id,
             _json(detail or {})))

    @staticmethod
    def _next_seq(conn, name: str) -> int:
        conn.execute("INSERT OR IGNORE INTO sequences(name,value) VALUES(?,0)", (name,))
        conn.execute("UPDATE sequences SET value=value+1 WHERE name=?", (name,))
        return conn.execute(
            "SELECT value FROM sequences WHERE name=?", (name,)).fetchone()["value"]

    # ------------------------------------------------------------------ 披露策略
    def register_policy(self, jurisdiction: str, rules: list[tuple[str, str]]):
        """登记/覆盖某辖区的披露策略：rules = [(字段名, 正则), ...]。"""
        self._policies[jurisdiction] = DisclosurePolicy(
            jurisdiction, tuple(SensitiveRule(f, p) for f, p in rules))

    def _policy(self, jurisdiction: str) -> DisclosurePolicy:
        return self._policies.get(jurisdiction, BASE_POLICY)

    def _scan_sensitive(self, jurisdiction: str, texts: list[tuple[str, str]]):
        findings = []
        for where, text in texts:
            for rule in self._policy(jurisdiction).rules:
                m = re.search(rule.pattern, text or "")
                if m:
                    findings.append({"field": rule.field, "where": where,
                                     "match": _mask(m.group(0))})
        return findings

    # ------------------------------------------------------------------ 问询登记
    def register_inquiry(self, *, regulator: str, jurisdiction: str,
                         deadline: str, questions: list, allowed_scope: list[str],
                         actor: str, idempotency_key: str | None = None) -> dict:
        """登记监管问询（问题清单 + 截止时间 + 可引用资料范围）。

        idempotency_key 相同的重复登记返回原问询，不产生新记录。
        """
        qs = self._normalize_questions(questions)
        if not regulator or not jurisdiction or not deadline:
            raise DomainError("BAD_FIELD", "regulator/jurisdiction/deadline 均必填")
        if not isinstance(allowed_scope, list) or not allowed_scope:
            raise DomainError("BAD_FIELD", "allowed_scope 必须是非空清单")
        if idempotency_key is not None:
            with self._conn() as conn:
                hit = conn.execute(
                    "SELECT inquiry_id FROM inquiries WHERE idempotency_key=?",
                    (idempotency_key,)).fetchone()
                if hit:
                    view = self.get_inquiry(hit["inquiry_id"])
                    view["deduped"] = True
                    return view
        inquiry_id = _uid()
        with self._write_tx() as conn:
            if idempotency_key is not None:
                cur = conn.execute(
                    "INSERT OR IGNORE INTO inquiries(inquiry_id,regulator,jurisdiction,"
                    "deadline,questions_json,scope_json,status,idempotency_key,"
                    "created_at) VALUES(?,?,?,?,?,?,'OPEN',?,?)",
                    (inquiry_id, regulator, jurisdiction, deadline, _json(qs),
                     _json(list(allowed_scope)), idempotency_key, self._now()))
                if cur.rowcount == 0:  # 并发下同键已被别的请求占用
                    exist = conn.execute(
                        "SELECT inquiry_id FROM inquiries WHERE idempotency_key=?",
                        (idempotency_key,)).fetchone()
                    inquiry_id = exist["inquiry_id"]
                    deduped = True
                else:
                    deduped = False
            else:
                conn.execute(
                    "INSERT INTO inquiries(inquiry_id,regulator,jurisdiction,deadline,"
                    "questions_json,scope_json,status,idempotency_key,created_at)"
                    " VALUES(?,?,?,?,?,?,'OPEN',NULL,?)",
                    (inquiry_id, regulator, jurisdiction, deadline, _json(qs),
                     _json(list(allowed_scope)), self._now()))
                deduped = False
            if not deduped:
                self._audit(conn, actor, "INQUIRY_REGISTERED", "inquiry", inquiry_id,
                            {"regulator": regulator, "jurisdiction": jurisdiction,
                             "deadline": deadline, "questions": len(qs),
                             "allowed_scope": list(allowed_scope)})
        view = self.get_inquiry(inquiry_id)
        view["deduped"] = deduped
        return view

    @staticmethod
    def _normalize_questions(questions) -> list[dict]:
        if not isinstance(questions, list) or not questions:
            raise DomainError("BAD_FIELD", "questions 必须是非空清单")
        out = []
        for i, q in enumerate(questions, start=1):
            if isinstance(q, str):
                out.append({"question_id": f"Q{i}", "text": q})
            else:
                qid, text = q.get("question_id"), q.get("text")
                if not qid or not text:
                    raise DomainError("BAD_FIELD", "每个问题需要 question_id 与 text")
                out.append({"question_id": str(qid), "text": str(text)})
        ids = [q["question_id"] for q in out]
        if len(set(ids)) != len(ids):
            raise DomainError("BAD_FIELD", "question_id 重复")
        return out

    def get_inquiry(self, inquiry_id: str) -> dict:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM inquiries WHERE inquiry_id=?",
                               (inquiry_id,)).fetchone()
            if not row:
                raise DomainError("NOT_FOUND", f"问询不存在: {inquiry_id}", 404)
            return self._inquiry_view(row)

    @staticmethod
    def _inquiry_view(row) -> dict:
        return {"inquiry_id": row["inquiry_id"], "regulator": row["regulator"],
                "jurisdiction": row["jurisdiction"], "deadline": row["deadline"],
                "questions": json.loads(row["questions_json"]),
                "allowed_scope": json.loads(row["scope_json"]),
                "status": row["status"], "created_at": row["created_at"]}

    # ------------------------------------------------------------------ 底稿锁定
    def lock_draft(self, *, inquiry_id: str, evidence_ids: list[str], actor: str,
                   kind: str = "ORIGINAL", based_on_draft_id: str | None = None,
                   reasons: list | None = None,
                   corrects_submission_id: str | None = None) -> dict:
        """把指定资料的 *当前精确版本* 锁定成答复底稿。

        锁定内容（版本号 + 内容哈希）由数据库触发器保证不可改；资料后续
        变化不会混入本底稿，只能经 refresh_draft / create_correction 产生
        带原因的新候选。
        """
        self.get_inquiry(inquiry_id)
        with self._write_tx() as conn:
            return self._lock_draft(
                conn, inquiry_id=inquiry_id, evidence_ids=evidence_ids, actor=actor,
                kind=kind, based_on_draft_id=based_on_draft_id,
                reasons=list(reasons or []),
                corrects_submission_id=corrects_submission_id)

    def _lock_draft(self, conn, *, inquiry_id, evidence_ids, actor, kind,
                    based_on_draft_id, reasons, corrects_submission_id) -> dict:
        seq = conn.execute(
            "SELECT COALESCE(MAX(seq),0)+1 AS s FROM drafts WHERE inquiry_id=?",
            (inquiry_id,)).fetchone()["s"]
        citations, seen = [], set()
        for eid in evidence_ids:
            if eid in seen:
                continue
            seen.add(eid)
            rec = ev_fetch(conn, eid)  # 当前最新版本；不存在则记为缺失
            citations.append({
                "evidence_id": eid,
                "kind": rec["kind"] if rec else None,
                "version": rec["version"] if rec else None,
                "content_hash": rec["content_hash"] if rec else None,
            })
        draft_id = _uid()
        conn.execute(
            "INSERT INTO drafts(draft_id,inquiry_id,seq,kind,status,citations_json,"
            "reasons_json,based_on_draft_id,corrects_submission_id,created_at)"
            " VALUES(?,?,?,?,'OPEN',?,?,?,?,?)",
            (draft_id, inquiry_id, seq, kind, _json(citations), _json(reasons),
             based_on_draft_id, corrects_submission_id, self._now()))
        self._audit(conn, actor, "DRAFT_LOCKED", "draft", draft_id,
                    {"inquiry_id": inquiry_id, "kind": kind, "seq": seq,
                     "citations": citations, "reasons": reasons})
        row = conn.execute("SELECT * FROM drafts WHERE draft_id=?",
                           (draft_id,)).fetchone()
        return self._draft_view(row)

    def refresh_draft(self, *, draft_id: str, actor: str,
                      extra_reasons: list | None = None) -> dict | None:
        """组稿期间引用发生变化时，生成带原因的新底稿候选。

        旧底稿置为 SUPERSEDED（锁定内容原样保留），章节/附件原样复制到
        新候选；无漂移且无附加原因时返回 None。
        """
        with self._write_tx() as conn:
            draft = self._draft_view(self._draft_row(conn, draft_id))
            if draft["status"] != "OPEN":
                raise DomainError("DRAFT_NOT_OPEN",
                                  f"底稿状态 {draft['status']} 不能刷新", 409)
            conflicts = self._collect_conflicts(conn, draft)
            reasons = [{"type": "EVIDENCE_UPDATED", **d}
                       for d in conflicts["evidence_drift"]]
            reasons += [{"type": "EVIDENCE_MISSING", **m}
                        for m in conflicts["missing_evidence"]]
            reasons += list(extra_reasons or [])
            if not reasons:
                return None
            new_draft = self._lock_draft(
                conn, inquiry_id=draft["inquiry_id"],
                evidence_ids=[c["evidence_id"] for c in draft["citations"]],
                actor=actor, kind="REFRESH", based_on_draft_id=draft_id,
                reasons=reasons, corrects_submission_id=None)
            self._copy_sections(conn, draft_id, new_draft["draft_id"])
            conn.execute("UPDATE drafts SET status='SUPERSEDED' WHERE draft_id=?",
                         (draft_id,))
            self._audit(conn, actor, "DRAFT_SUPERSEDED", "draft", draft_id,
                        {"successor": new_draft["draft_id"], "reasons": reasons})
            return new_draft

    def _copy_sections(self, conn, src_draft_id: str, dst_draft_id: str):
        for s in conn.execute("SELECT * FROM sections WHERE draft_id=?",
                              (src_draft_id,)).fetchall():
            conn.execute(
                "INSERT INTO sections(draft_id,section_key,question_id,content,"
                "revision,author,updated_at) VALUES(?,?,?,?,?,?,?)",
                (dst_draft_id, s["section_key"], s["question_id"], s["content"],
                 s["revision"], s["author"], self._now()))
        for a in conn.execute("SELECT * FROM attachments WHERE draft_id=?",
                              (src_draft_id,)).fetchall():
            conn.execute(
                "INSERT INTO attachments(draft_id,attachment_id,name,content,"
                "content_hash,revision,updated_at) VALUES(?,?,?,?,?,?,?)",
                (dst_draft_id, a["attachment_id"], a["name"], a["content"],
                 a["content_hash"], a["revision"], self._now()))

    def _draft_row(self, conn, draft_id: str):
        row = conn.execute("SELECT * FROM drafts WHERE draft_id=?",
                           (draft_id,)).fetchone()
        if not row:
            raise DomainError("NOT_FOUND", f"底稿不存在: {draft_id}", 404)
        return row

    @staticmethod
    def _draft_view(row) -> dict:
        return {"draft_id": row["draft_id"], "inquiry_id": row["inquiry_id"],
                "seq": row["seq"], "kind": row["kind"], "status": row["status"],
                "citations": json.loads(row["citations_json"]),
                "reasons": json.loads(row["reasons_json"]),
                "based_on_draft_id": row["based_on_draft_id"],
                "corrects_submission_id": row["corrects_submission_id"],
                "created_at": row["created_at"]}

    def get_draft(self, draft_id: str) -> dict:
        with self._conn() as conn:
            return self._draft_view(self._draft_row(conn, draft_id))

    def list_drafts(self, inquiry_id: str) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM drafts WHERE inquiry_id=? ORDER BY seq",
                (inquiry_id,)).fetchall()
            return [self._draft_view(r) for r in rows]

    # ------------------------------------------------------------------ 章节协作
    def submit_section(self, *, draft_id: str, section_key: str, content: str,
                       author: str, base_revision: int, question_id=_UNSET) -> dict:
        """提交章节内容（多名撰写人可并行编辑不同章节）。

        乐观并发：base_revision 必须等于调用方看到的当前修订号（新建=0），
        过期提交返回 409 STALE_REVISION，绝不覆盖他人已提交的新内容。
        """
        with self._write_tx() as conn:
            draft = self._draft_row(conn, draft_id)
            self._require_open(draft)
            row = conn.execute(
                "SELECT * FROM sections WHERE draft_id=? AND section_key=?",
                (draft_id, section_key)).fetchone()
            if row is None:
                if base_revision != 0:
                    exc = DomainError("STALE_REVISION",
                                      f"章节 {section_key} 尚不存在，base_revision 应为 0",
                                      409)
                    exc.current_revision = 0
                    raise exc
                qid = None if question_id is _UNSET else question_id
                new_rev = 1
                conn.execute(
                    "INSERT INTO sections(draft_id,section_key,question_id,content,"
                    "revision,author,updated_at) VALUES(?,?,?,?,1,?,?)",
                    (draft_id, section_key, qid, content, author, self._now()))
            else:
                if row["revision"] != base_revision:
                    exc = DomainError(
                        "STALE_REVISION",
                        f"章节 {section_key} 当前修订号为 {row['revision']}，"
                        f"提交基于过期的 {base_revision}", 409)
                    exc.current_revision = row["revision"]
                    raise exc
                qid = row["question_id"] if question_id is _UNSET else question_id
                new_rev = row["revision"] + 1
                conn.execute(
                    "UPDATE sections SET content=?,question_id=?,revision=?,author=?,"
                    "updated_at=? WHERE draft_id=? AND section_key=? AND revision=?",
                    (content, qid, new_rev, author, self._now(),
                     draft_id, section_key, base_revision))
            self._audit(conn, author, "SECTION_SUBMITTED", "draft", draft_id,
                        {"section_key": section_key, "revision": new_rev,
                         "question_id": qid})
            return {"draft_id": draft_id, "section_key": section_key,
                    "revision": new_rev, "question_id": qid}

    def put_attachment(self, *, draft_id: str, attachment_id: str, name: str,
                       content: str, author: str, base_revision: int) -> dict:
        """提交附件（与章节相同的乐观修订号语义）。"""
        with self._write_tx() as conn:
            draft = self._draft_row(conn, draft_id)
            self._require_open(draft)
            row = conn.execute(
                "SELECT * FROM attachments WHERE draft_id=? AND attachment_id=?",
                (draft_id, attachment_id)).fetchone()
            if row is None:
                if base_revision != 0:
                    exc = DomainError("STALE_REVISION",
                                      f"附件 {attachment_id} 尚不存在，base_revision 应为 0",
                                      409)
                    exc.current_revision = 0
                    raise exc
                new_rev = 1
                conn.execute(
                    "INSERT INTO attachments(draft_id,attachment_id,name,content,"
                    "content_hash,revision,updated_at) VALUES(?,?,?,?,?,1,?)",
                    (draft_id, attachment_id, name, content, sha256_text(content),
                     self._now()))
            else:
                if row["revision"] != base_revision:
                    exc = DomainError(
                        "STALE_REVISION",
                        f"附件 {attachment_id} 当前修订号为 {row['revision']}，"
                        f"提交基于过期的 {base_revision}", 409)
                    exc.current_revision = row["revision"]
                    raise exc
                new_rev = row["revision"] + 1
                conn.execute(
                    "UPDATE attachments SET name=?,content=?,content_hash=?,revision=?,"
                    "updated_at=? WHERE draft_id=? AND attachment_id=? AND revision=?",
                    (name, content, sha256_text(content), new_rev, self._now(),
                     draft_id, attachment_id, base_revision))
            self._audit(conn, author, "ATTACHMENT_PUT", "draft", draft_id,
                        {"attachment_id": attachment_id, "revision": new_rev})
            return {"draft_id": draft_id, "attachment_id": attachment_id,
                    "revision": new_rev}

    @staticmethod
    def _require_open(draft_row):
        if draft_row["status"] != "OPEN":
            raise DomainError(
                "DRAFT_NOT_OPEN",
                f"底稿 {draft_row['draft_id']} 状态为 {draft_row['status']}，"
                "已不可编辑；定稿后的修改请走更正件", 409)

    def get_sections(self, draft_id: str) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM sections WHERE draft_id=? ORDER BY section_key",
                (draft_id,)).fetchall()
            return [dict(r) for r in rows]

    def get_attachments(self, draft_id: str) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM attachments WHERE draft_id=? ORDER BY attachment_id",
                (draft_id,)).fetchall()
            return [dict(r) for r in rows]

    # ------------------------------------------------------------------ 冲突查询
    def query_conflicts(self, draft_id: str) -> dict:
        """列出底稿当前全部冲突：引用漂移 / 资料缺失 / 引用越权 / 版本矛盾。"""
        with self._conn() as conn:
            draft = self._draft_view(self._draft_row(conn, draft_id))
            return self._collect_conflicts(conn, draft)

    def _collect_conflicts(self, conn, draft: dict) -> dict:
        inquiry = self.get_inquiry(draft["inquiry_id"])
        scope = inquiry["allowed_scope"]
        drift, missing, scope_violations = [], [], []
        version_conflicts, unlocked = [], []
        locked = {c["evidence_id"]: c for c in draft["citations"]}
        for c in draft["citations"]:
            eid, ver = c["evidence_id"], c["version"]
            if not _scope_allows(scope, eid):
                scope_violations.append({"evidence_id": eid})
            if ver is None or ev_fetch(conn, eid, ver) is None:
                missing.append({"evidence_id": eid, "locked_version": ver})
                continue
            cur = ev_current_version(conn, eid)
            if cur != ver:
                drift.append({"evidence_id": eid, "locked_version": ver,
                              "current_version": cur})
        explicit: dict[str, set] = {}
        for s in conn.execute("SELECT * FROM sections WHERE draft_id=?",
                              (draft["draft_id"],)).fetchall():
            for eid, ver in _parse_ev_tokens(s["content"]):
                lock = locked.get(eid)
                if lock is None:
                    unlocked.append({"evidence_id": eid,
                                     "section_key": s["section_key"]})
                    continue
                if ver is not None:
                    explicit.setdefault(eid, set()).add(ver)
                    if lock["version"] is not None and ver != lock["version"]:
                        version_conflicts.append(
                            {"evidence_id": eid, "section_key": s["section_key"],
                             "locked_version": lock["version"], "cited_version": ver})
        for eid, vers in explicit.items():
            if len(vers) > 1:
                version_conflicts.append(
                    {"evidence_id": eid, "cited_versions": sorted(vers),
                     "locked_version": locked[eid]["version"]})
        succ = conn.execute(
            "SELECT draft_id FROM drafts WHERE based_on_draft_id=? "
            "ORDER BY seq DESC LIMIT 1", (draft["draft_id"],)).fetchone()
        out = {"draft_id": draft["draft_id"], "status": draft["status"],
               "evidence_drift": drift, "missing_evidence": missing,
               "scope_violations": scope_violations,
               "version_conflicts": version_conflicts,
               "unlocked_citations": unlocked,
               "superseded_by": succ["draft_id"] if succ else None}
        out["has_conflicts"] = any(
            [drift, missing, scope_violations, version_conflicts, unlocked])
        return out

    # ------------------------------------------------------------------ 定稿校验
    def validate_draft(self, draft_id: str) -> dict:
        """定稿校验：任一硬性问题未满足即阻止定稿，并准确列出问题清单。"""
        with self._conn() as conn:
            return self._validate(conn, draft_id)

    def _validate(self, conn, draft_id: str) -> dict:
        draft = self._draft_view(self._draft_row(conn, draft_id))
        inquiry = self.get_inquiry(draft["inquiry_id"])
        issues, warnings = [], []
        add = lambda code, msg, **kw: issues.append(_issue(code, msg, **kw))

        if draft["status"] == "SUPERSEDED":
            succ = conn.execute(
                "SELECT draft_id FROM drafts WHERE based_on_draft_id=? "
                "ORDER BY seq DESC LIMIT 1", (draft_id,)).fetchone()
            add("DRAFT_SUPERSEDED", "底稿已被新候选取代，不能定稿",
                successor=succ["draft_id"] if succ else None)
        if draft["status"] == "FINALIZED":
            add("DRAFT_ALREADY_FINALIZED", "底稿已定稿，后续修改须走更正件")

        # -- 引用：越权 / 缺失 / 篡改 / 漂移 --------------------------------
        scope = inquiry["allowed_scope"]
        locked = {c["evidence_id"]: c for c in draft["citations"]}
        for c in draft["citations"]:
            eid, ver = c["evidence_id"], c["version"]
            if not _scope_allows(scope, eid):
                add("SCOPE_VIOLATION", f"资料 {eid} 超出问询允许引用范围",
                    evidence_id=eid)
            if ver is None:
                add("MISSING_EVIDENCE", f"资料 {eid} 在锁定时不存在", evidence_id=eid)
                continue
            rec = ev_fetch(conn, eid, ver)
            if rec is None:
                add("MISSING_EVIDENCE", f"资料 {eid} 版本 v{ver} 已不存在",
                    evidence_id=eid, version=ver)
                continue
            if rec["content_hash"] != c["content_hash"]:
                add("EVIDENCE_TAMPERED", f"资料 {eid} v{ver} 内容与锁定哈希不符",
                    evidence_id=eid, version=ver)
            cur = ev_current_version(conn, eid)
            if cur != ver:
                add("STALE_CITATION",
                    f"资料 {eid} 组稿期间已更新（锁定 v{ver}，当前 v{cur}），"
                    "须生成带原因的新底稿候选",
                    evidence_id=eid, locked_version=ver, current_version=cur)

        # -- 正文内联引用：未锁定 / 版本矛盾 --------------------------------
        sections = conn.execute(
            "SELECT * FROM sections WHERE draft_id=? ORDER BY section_key",
            (draft_id,)).fetchall()
        explicit: dict[str, set] = {}
        for s in sections:
            for eid, ver in _parse_ev_tokens(s["content"]):
                lock = locked.get(eid)
                if lock is None:
                    add("UNLOCKED_CITATION",
                        f"章节 {s['section_key']} 引用了未锁定进底稿的资料 {eid}",
                        evidence_id=eid, section_key=s["section_key"])
                    continue
                if ver is not None:
                    explicit.setdefault(eid, set()).add(ver)
                    if lock["version"] is not None and ver != lock["version"]:
                        add("VERSION_CONFLICT",
                            f"章节 {s['section_key']} 引用 {eid}@v{ver} "
                            f"与底稿锁定 v{lock['version']} 矛盾",
                            evidence_id=eid, section_key=s["section_key"],
                            locked_version=lock["version"], cited_version=ver)
        for eid, vers in explicit.items():
            if len(vers) > 1:
                add("VERSION_CONFLICT",
                    f"资料 {eid} 在章节间引用版本互相矛盾：{sorted(vers)}",
                    evidence_id=eid, cited_versions=sorted(vers))

        # -- 每个问题都有唯一可追踪的回答 -----------------------------------
        by_q: dict[str, list] = {}
        for s in sections:
            if s["question_id"]:
                by_q.setdefault(s["question_id"], []).append(s)
        qids = [q["question_id"] for q in inquiry["questions"]]
        for q in inquiry["questions"]:
            qid = q["question_id"]
            matches = by_q.get(qid, [])
            if not matches:
                add("QUESTION_UNANSWERED", f"问题 {qid} 尚无对应回答章节",
                    question_id=qid, text=q["text"])
            elif len(matches) > 1:
                add("QUESTION_AMBIGUOUS",
                    f"问题 {qid} 被多个章节回答，无法唯一追踪",
                    question_id=qid, sections=[m["section_key"] for m in matches])
            elif not matches[0]["content"].strip():
                add("EMPTY_ANSWER", f"问题 {qid} 的回答章节为空",
                    question_id=qid, section_key=matches[0]["section_key"])
        for qid in by_q:
            if qid not in qids:
                add("UNKNOWN_QUESTION", f"章节回答了问询中不存在的问题 {qid}",
                    question_id=qid)

        # -- 附件与正文引用双向一致 ------------------------------------------
        atts = conn.execute("SELECT * FROM attachments WHERE draft_id=?",
                            (draft_id,)).fetchall()
        att_ids = {a["attachment_id"] for a in atts}
        refs: set[str] = set()
        for s in sections:
            refs.update(_parse_att_tokens(s["content"]))
        for r in sorted(refs - att_ids):
            add("ATTACHMENT_MISSING", f"正文引用了不存在的附件 {r}", attachment_id=r)
        for a in sorted(att_ids - refs):
            add("ATTACHMENT_UNREFERENCED", f"附件 {a} 未在正文中被引用",
                attachment_id=a)

        # -- 敏感字段符合收件辖区披露策略 ------------------------------------
        texts = [(f"section:{s['section_key']}", s["content"]) for s in sections]
        texts += [(f"attachment:{a['attachment_id']}", a["content"]) for a in atts]
        for f in self._scan_sensitive(inquiry["jurisdiction"], texts):
            add("SENSITIVE_FIELD",
                f"检出敏感字段 {f['field']}（{f['where']}），"
                f"违反 {inquiry['jurisdiction']} 辖区披露策略", **f)

        # -- 截止时间（仅提醒，不阻止）---------------------------------------
        try:
            from datetime import date as _date
            if self.clock.now().date() > _date.fromisoformat(
                    str(inquiry["deadline"])[:10]):
                warnings.append(_issue(
                    "DEADLINE_PASSED", f"已超过监管截止时间 {inquiry['deadline']}"))
        except ValueError:
            warnings.append(_issue("DEADLINE_UNPARSEABLE",
                                   f"截止时间无法解析: {inquiry['deadline']}"))

        return {"draft_id": draft_id, "ok": not issues, "issues": issues,
                "warnings": warnings,
                "unmet": [i["code"] for i in issues]}

    # ------------------------------------------------------------------ 发送与重试
    def finalize_and_send(self, *, draft_id: str, actor: str,
                          await_timeout: float = 30.0) -> dict:
        """定稿校验 + 生成不可变提交件 + 外发（可恢复作业，幂等）。

        同一底稿的重复调用 / 多节点并发只产生一个作业、一份提交件、
        一个外发编号。校验未通过抛 VALIDATION_FAILED（附尚未满足的问题）。
        """
        with self._write_tx() as conn:
            self._draft_row(conn, draft_id)
            job_id = self._ensure_job(
                conn, f"finalize:{draft_id}", "FINALIZE_AND_SEND",
                {"draft_id": draft_id, "actor": actor}, actor)
        job = self.run_job(job_id)
        if job is None:  # 被其他节点领取：等待其完成
            job = self._await_job(job_id, await_timeout)
        if job["status"] == "FAILED":
            err = job["error"] or {}
            exc = DomainError(err.get("code", "JOB_FAILED"),
                              err.get("message", "作业执行失败"),
                              422 if err.get("code") == "VALIDATION_FAILED" else 500)
            if err.get("report"):
                exc.report = err["report"]
            raise exc
        if job["status"] != "DONE":
            raise DomainError("JOB_IN_FLIGHT",
                              f"作业 {job_id} 正由其他工作节点处理", 409)
        return job["result"]

    def _ensure_job(self, conn, job_key: str, job_type: str, payload: dict,
                    actor: str) -> str:
        cur = conn.execute(
            "INSERT OR IGNORE INTO jobs(job_id,job_key,type,status,checkpoint,"
            "payload_json,attempts,created_at,updated_at)"
            " VALUES(?,?,?,'PENDING',0,?,0,?,?)",
            (_uid(), job_key, job_type, _json(payload), self._now(), self._now()))
        row = conn.execute("SELECT job_id FROM jobs WHERE job_key=?",
                           (job_key,)).fetchone()
        if cur.rowcount:
            self._audit(conn, actor, "JOB_CREATED", "job", row["job_id"],
                        {"job_key": job_key, "type": job_type})
        return row["job_id"]

    def run_job(self, job_id: str) -> dict | None:
        """领取并执行作业；已被其他节点持有时返回 None。

        每个步骤与其检查点在同一事务提交：崩溃后从断点续跑，已提交的
        步骤不会重跑，不会产生重复提交件 / 外发编号 / 履历记录。
        """
        if not self._claim(job_id):
            return None
        job = self.get_job(job_id)
        steps = JOB_STEPS[job["type"]]
        try:
            idx = job["checkpoint"]
            while idx < len(steps):
                with self._write_tx() as conn:
                    done = conn.execute(
                        "SELECT 1 FROM job_steps WHERE job_id=? AND step_index=?",
                        (job_id, idx)).fetchone()
                    if not done:
                        detail = self._exec_step(conn, job, steps[idx])
                        conn.execute(
                            "INSERT INTO job_steps(job_id,step_index,step_name,"
                            "detail_json,completed_at) VALUES(?,?,?,?,?)",
                            (job_id, idx, steps[idx], _json(detail or {}),
                             self._now()))
                    conn.execute(
                        "UPDATE jobs SET checkpoint=?,updated_at=? WHERE job_id=?",
                        (idx + 1, self._now(), job_id))
                idx += 1
            result = self._job_result(job)
            with self._write_tx() as conn:
                conn.execute(
                    "UPDATE jobs SET status='DONE',result_json=?,updated_at=?"
                    " WHERE job_id=?", (_json(result), self._now(), job_id))
                self._audit(conn, job["payload"].get("actor", "system"),
                            "JOB_DONE", "job", job_id, {"result": result})
            return self.get_job(job_id)
        except Exception as e:  # 标记 FAILED，保留检查点等待恢复
            err = self._error_payload(e)
            with self._write_tx() as conn:
                conn.execute(
                    "UPDATE jobs SET status='FAILED',error_json=?,updated_at=?"
                    " WHERE job_id=?", (_json(err), self._now(), job_id))
                self._audit(conn, job["payload"].get("actor", "system"),
                            "JOB_FAILED", "job", job_id, err)
            return self.get_job(job_id)

    @staticmethod
    def _error_payload(e: Exception) -> dict:
        if isinstance(e, DomainError):
            err = {"code": e.code, "message": str(e)}
            report = getattr(e, "report", None)
            if report:
                err["report"] = report
            return err
        return {"code": "INTERNAL", "message": str(e)}

    def _claim(self, job_id: str) -> bool:
        with self._write_tx() as conn:
            cur = conn.execute(
                "UPDATE jobs SET status='RUNNING',claimed_by=?,lease_until=?,"
                "attempts=attempts+1,updated_at=? WHERE job_id=? AND "
                "(status IN ('PENDING','FAILED') OR "
                " (status='RUNNING' AND lease_until < ?))",
                (self.worker_id, self._lease_until(), self._now(), job_id,
                 self._now()))
            if cur.rowcount:
                self._audit(conn, self.worker_id, "JOB_CLAIMED", "job", job_id,
                            {"worker": self.worker_id})
            return cur.rowcount == 1

    def _exec_step(self, conn, job: dict, step: str) -> dict:
        payload = job["payload"]
        if step == "validate":
            # 崩溃恢复重放：提交件已产生说明校验此前已通过（finalize 只可能
            # 在 validate 成功之后提交），直接幂等跳过
            if conn.execute("SELECT 1 FROM submissions WHERE draft_id=?",
                            (payload["draft_id"],)).fetchone():
                return {"replayed": True}
            report = self._validate(conn, payload["draft_id"])
            if not report["ok"]:
                exc = DomainError(
                    "VALIDATION_FAILED",
                    "定稿校验未通过: " + ",".join(report["unmet"]), 422)
                exc.report = report
                raise exc
            return {"report": report}
        if step == "finalize":
            return self._step_finalize(conn, payload)
        if step == "send":
            return self._step_send(conn, payload)
        raise DomainError("INTERNAL", f"未知作业步骤 {step}", 500)

    def _step_finalize(self, conn, payload: dict) -> dict:
        draft_id = payload["draft_id"]
        actor = payload.get("actor", "system")
        existing = conn.execute("SELECT * FROM submissions WHERE draft_id=?",
                                (draft_id,)).fetchone()
        if existing:  # 崩溃恢复重放：同一底稿复用原提交件与外发编号
            return {"submission_id": existing["submission_id"],
                    "outbound_no": existing["outbound_no"], "reused": True}
        draft = self._draft_view(self._draft_row(conn, draft_id))
        outbound_no = f"OUT-{self._next_seq(conn, 'outbound'):06d}"
        snapshot = self._build_snapshot(conn, draft)
        payload_json = _json(snapshot)
        sub_id = _uid()
        conn.execute(
            "INSERT INTO submissions(submission_id,draft_id,inquiry_id,outbound_no,"
            "corrects_submission_id,payload_json,payload_hash,status,created_at)"
            " VALUES(?,?,?,?,?,?,?,'FINALIZED',?)",
            (sub_id, draft_id, draft["inquiry_id"], outbound_no,
             draft["corrects_submission_id"], payload_json,
             sha256_text(payload_json), self._now()))
        conn.execute("UPDATE drafts SET status='FINALIZED' WHERE draft_id=?",
                     (draft_id,))
        conn.execute("UPDATE inquiries SET status='ANSWERED' WHERE inquiry_id=?",
                     (draft["inquiry_id"],))
        if draft["corrects_submission_id"]:
            conn.execute(
                "UPDATE submissions SET status='CORRECTED'"
                " WHERE submission_id=? AND status<>'CORRECTED'",
                (draft["corrects_submission_id"],))
            self._audit(conn, actor, "SUBMISSION_CORRECTED", "submission",
                        draft["corrects_submission_id"],
                        {"correction_submission": sub_id})
        self._audit(conn, actor, "SUBMISSION_FINALIZED", "submission", sub_id,
                    {"draft_id": draft_id, "outbound_no": outbound_no,
                     "corrects": draft["corrects_submission_id"]})
        return {"submission_id": sub_id, "outbound_no": outbound_no,
                "reused": False}

    def _step_send(self, conn, payload: dict) -> dict:
        actor = payload.get("actor", "system")
        sub = conn.execute("SELECT * FROM submissions WHERE draft_id=?",
                           (payload["draft_id"],)).fetchone()
        if not sub:
            raise DomainError("INTERNAL", "finalize 步骤尚未产生提交件", 500)
        sent_now = self.transport.send(conn, dict(sub), self._now())
        if sent_now:
            conn.execute(
                "UPDATE submissions SET status='SENT'"
                " WHERE submission_id=? AND status='FINALIZED'",
                (sub["submission_id"],))
            self._audit(conn, actor, "SUBMISSION_SENT", "submission",
                        sub["submission_id"], {"outbound_no": sub["outbound_no"]})
        return {"outbound_no": sub["outbound_no"], "sent_now": bool(sent_now)}

    def _build_snapshot(self, conn, draft: dict) -> dict:
        inquiry = self.get_inquiry(draft["inquiry_id"])
        sections = [dict(r) for r in conn.execute(
            "SELECT * FROM sections WHERE draft_id=? ORDER BY section_key",
            (draft["draft_id"],)).fetchall()]
        attachments = [dict(r) for r in conn.execute(
            "SELECT * FROM attachments WHERE draft_id=? ORDER BY attachment_id",
            (draft["draft_id"],)).fetchall()]
        answers = {s["question_id"]: s["section_key"]
                   for s in sections if s["question_id"]}
        return {"inquiry": inquiry,
                "draft": {"draft_id": draft["draft_id"], "seq": draft["seq"],
                          "kind": draft["kind"], "citations": draft["citations"],
                          "reasons": draft["reasons"],
                          "based_on_draft_id": draft["based_on_draft_id"],
                          "corrects_submission_id": draft["corrects_submission_id"]},
                "sections": sections, "attachments": attachments,
                "answers": answers, "finalized_at": self._now()}

    def _job_result(self, job: dict) -> dict:
        if job["type"] == "FINALIZE_AND_SEND":
            with self._conn() as conn:
                sub = conn.execute(
                    "SELECT submission_id,outbound_no FROM submissions "
                    "WHERE draft_id=?", (job["payload"]["draft_id"],)).fetchone()
                if sub:
                    return {"submission_id": sub["submission_id"],
                            "outbound_no": sub["outbound_no"]}
        return {}

    def retry_job(self, job_id: str, await_timeout: float = 30.0) -> dict:
        """重试失败/中断的作业（从检查点续跑）。"""
        job = self.run_job(job_id)
        if job is None:
            job = self._await_job(job_id, await_timeout)
        return job

    def run_pending_jobs(self) -> list[dict]:
        """恢复并执行全部待处理/失败/租约过期的作业。"""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT job_id FROM jobs WHERE status IN ('PENDING','FAILED') OR "
                "(status='RUNNING' AND lease_until < ?) ORDER BY created_at",
                (self._now(),)).fetchall()
        out = []
        for r in rows:
            job = self.run_job(r["job_id"])
            if job is not None:
                out.append(job)
        return out

    def _await_job(self, job_id: str, timeout: float) -> dict:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            job = self.get_job(job_id)
            if job["status"] in ("DONE", "FAILED"):
                return job
            time.sleep(0.02)
        return self.get_job(job_id)

    def get_job(self, job_id: str) -> dict:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM jobs WHERE job_id=?",
                               (job_id,)).fetchone()
            if not row:
                raise DomainError("NOT_FOUND", f"作业不存在: {job_id}", 404)
            return self._job_view(row)

    @staticmethod
    def _job_view(row) -> dict:
        return {"job_id": row["job_id"], "job_key": row["job_key"],
                "type": row["type"], "status": row["status"],
                "checkpoint": row["checkpoint"],
                "payload": json.loads(row["payload_json"]),
                "result": json.loads(row["result_json"]) if row["result_json"] else None,
                "error": json.loads(row["error_json"]) if row["error_json"] else None,
                "attempts": row["attempts"], "claimed_by": row["claimed_by"],
                "created_at": row["created_at"], "updated_at": row["updated_at"]}

    # ------------------------------------------------------------------ 回执登记
    def register_receipt(self, *, submission_id: str, regulator_ref: str,
                         payload: dict, actor: str) -> dict:
        """登记监管回执（按 (提交件, 监管方引用号) 幂等）。"""
        with self._write_tx() as conn:
            sub = conn.execute("SELECT * FROM submissions WHERE submission_id=?",
                               (submission_id,)).fetchone()
            if not sub:
                raise DomainError("NOT_FOUND", f"提交件不存在: {submission_id}", 404)
            hit = conn.execute(
                "SELECT receipt_id FROM receipts WHERE submission_id=? AND "
                "regulator_ref=?", (submission_id, regulator_ref)).fetchone()
            if hit:
                return {"receipt_id": hit["receipt_id"], "deduped": True}
            receipt_id = _uid()
            conn.execute(
                "INSERT INTO receipts(receipt_id,submission_id,regulator_ref,"
                "payload_json,received_at) VALUES(?,?,?,?,?)",
                (receipt_id, submission_id, regulator_ref, _json(payload),
                 self._now()))
            if sub["status"] in ("FINALIZED", "SENT"):
                conn.execute("UPDATE submissions SET status='ACKED'"
                             " WHERE submission_id=?", (submission_id,))
            self._audit(conn, actor, "RECEIPT_REGISTERED", "submission",
                        submission_id, {"receipt_id": receipt_id,
                                        "regulator_ref": regulator_ref})
            return {"receipt_id": receipt_id, "deduped": False}

    # ------------------------------------------------------------------ 更正关系
    def create_correction(self, *, submission_id: str, reason: str,
                          actor: str) -> dict:
        """针对已发送提交件创建更正底稿（后续定稿产生更正件）。

        新底稿按 *当前* 资料版本重新锁定，复制原章节/附件，并把更正原因
        与引用漂移一并写入 reasons；原提交件与回执永久保留。
        """
        with self._write_tx() as conn:
            sub = conn.execute("SELECT * FROM submissions WHERE submission_id=?",
                               (submission_id,)).fetchone()
            if not sub:
                raise DomainError("NOT_FOUND", f"提交件不存在: {submission_id}", 404)
            src = self._draft_view(self._draft_row(conn, sub["draft_id"]))
            reasons = [{"type": "CORRECTION", "reason": reason,
                        "original_submission": submission_id}]
            conflicts = self._collect_conflicts(conn, src)
            reasons += [{"type": "EVIDENCE_UPDATED", **d}
                        for d in conflicts["evidence_drift"]]
            reasons += [{"type": "EVIDENCE_MISSING", **m}
                        for m in conflicts["missing_evidence"]]
            new_draft = self._lock_draft(
                conn, inquiry_id=src["inquiry_id"],
                evidence_ids=[c["evidence_id"] for c in src["citations"]],
                actor=actor, kind="CORRECTION", based_on_draft_id=src["draft_id"],
                reasons=reasons, corrects_submission_id=submission_id)
            self._copy_sections(conn, src["draft_id"], new_draft["draft_id"])
            self._audit(conn, actor, "CORRECTION_CREATED", "submission",
                        submission_id,
                        {"correction_draft": new_draft["draft_id"],
                         "reason": reason})
            return new_draft

    def correction_chain(self, submission_id: str) -> list[dict]:
        """更正关系链：从原始提交件到最新更正件（含各自回执）。"""
        with self._conn() as conn:
            sub = conn.execute("SELECT * FROM submissions WHERE submission_id=?",
                               (submission_id,)).fetchone()
            if not sub:
                raise DomainError("NOT_FOUND", f"提交件不存在: {submission_id}", 404)
            while sub["corrects_submission_id"]:
                sub = conn.execute(
                    "SELECT * FROM submissions WHERE submission_id=?",
                    (sub["corrects_submission_id"],)).fetchone()
            chain, current = [], sub
            while current:
                receipts = [dict(r) for r in conn.execute(
                    "SELECT * FROM receipts WHERE submission_id=? "
                    "ORDER BY received_at", (current["submission_id"],)).fetchall()]
                entry = self._submission_view(current)
                entry["receipts"] = receipts
                chain.append(entry)
                current = conn.execute(
                    "SELECT * FROM submissions WHERE corrects_submission_id=?",
                    (current["submission_id"],)).fetchone()
            return chain

    # ------------------------------------------------------------------ 独立核对
    def independent_check(self, submission_id: str) -> dict:
        """独立核对：不信任服务侧标志位，从存储快照重算并重跑校验。

        逐项核对：payload 哈希完整性、外发编号唯一、发件记录唯一、问题
        唯一可追踪回答、附件与正文一致、锁定引用可在资料库复核、敏感字段
        符合辖区披露策略、定稿作业履历完整、被更正原件保留、审计留痕存在。
        """
        with self._conn() as conn:
            sub = conn.execute("SELECT * FROM submissions WHERE submission_id=?",
                               (submission_id,)).fetchone()
            if not sub:
                raise DomainError("NOT_FOUND", f"提交件不存在: {submission_id}", 404)
            p = json.loads(sub["payload_json"])
            checks = []

            def add(name, fn):
                # 被篡改的 payload 可能结构非法：单项核对异常本身记为不通过
                try:
                    ok, detail = fn()
                except Exception as e:  # noqa: BLE001 - 核对器必须健壮
                    ok, detail = False, {"error": str(e)}
                checks.append({"check": name, "ok": bool(ok),
                               "detail": detail})

            add("payload_integrity",
                lambda: (sha256_text(sub["payload_json"]) == sub["payload_hash"],
                         {}))

            def _outbound_unique():
                dup = conn.execute(
                    "SELECT COUNT(*) AS c FROM submissions WHERE outbound_no=?",
                    (sub["outbound_no"],)).fetchone()["c"]
                return dup == 1, {"count": dup}
            add("outbound_number_unique", _outbound_unique)

            def _single_dispatch():
                if sub["status"] not in ("SENT", "ACKED", "CORRECTED"):
                    return True, {"skipped": "not_sent"}
                oc = conn.execute(
                    "SELECT COUNT(*) AS c FROM outbox WHERE outbound_no=?",
                    (sub["outbound_no"],)).fetchone()["c"]
                return oc == 1, {"count": oc}
            add("single_dispatch_record", _single_dispatch)

            def _questions_ok():
                sec_by_key = {s["section_key"]: s for s in p["sections"]}
                problems = []
                for q in p["inquiry"]["questions"]:
                    qid = q["question_id"]
                    key = p["answers"].get(qid)
                    sec = sec_by_key.get(key) if key else None
                    if sec is None or sec.get("question_id") != qid \
                            or not sec["content"].strip():
                        problems.append({"question_id": qid})
                seen: dict[str, list] = {}
                for s in p["sections"]:
                    if s.get("question_id"):
                        seen.setdefault(s["question_id"], []).append(
                            s["section_key"])
                for qid, keys in seen.items():
                    if len(keys) > 1:
                        problems.append({"question_id": qid, "sections": keys})
                return not problems, {"problems": problems}
            add("questions_uniquely_answered", _questions_ok)

            def _attachments_ok():
                refs: set[str] = set()
                for s in p["sections"]:
                    refs.update(_parse_att_tokens(s["content"]))
                att_ids = {a["attachment_id"] for a in p["attachments"]}
                return refs == att_ids, {"referenced": sorted(refs),
                                         "attached": sorted(att_ids)}
            add("attachments_consistent", _attachments_ok)

            def _citations_ok():
                problems = []
                for c in p["draft"]["citations"]:
                    rec = ev_fetch(conn, c["evidence_id"], c["version"]) \
                        if c["version"] is not None else None
                    if rec is None:
                        problems.append({"evidence_id": c["evidence_id"],
                                         "version": c["version"],
                                         "problem": "source_unavailable"})
                    elif rec["content_hash"] != c["content_hash"]:
                        problems.append({"evidence_id": c["evidence_id"],
                                         "version": c["version"],
                                         "problem": "hash_mismatch"})
                return not problems, {"problems": problems}
            add("citations_verifiable", _citations_ok)

            def _disclosure_ok():
                texts = [(f"section:{s['section_key']}", s["content"])
                         for s in p["sections"]]
                texts += [(f"attachment:{a['attachment_id']}", a["content"])
                          for a in p["attachments"]]
                findings = self._scan_sensitive(
                    p["inquiry"]["jurisdiction"], texts)
                return not findings, {"findings": findings}
            add("disclosure_policy_compliant", _disclosure_ok)

            def _job_done():
                job = conn.execute(
                    "SELECT * FROM jobs WHERE job_key=?",
                    (f"finalize:{p['draft']['draft_id']}",)).fetchone()
                return (job is not None and job["status"] == "DONE",
                        {"job_status": job["status"] if job else None})
            add("finalize_job_completed", _job_done)

            def _original_retained():
                corr = p["draft"].get("corrects_submission_id")
                if not corr:
                    return True, {"skipped": "not_a_correction"}
                orig = conn.execute(
                    "SELECT 1 FROM submissions WHERE submission_id=?",
                    (corr,)).fetchone()
                return orig is not None, {"original": corr}
            add("corrected_original_retained", _original_retained)

            def _audit_present():
                ac = conn.execute(
                    "SELECT COUNT(*) AS c FROM audit_log "
                    "WHERE entity_id IN (?,?)",
                    (sub["submission_id"], p["draft"]["draft_id"])).fetchone()["c"]
                return ac > 0, {"entries": ac}
            add("audit_trail_present", _audit_present)

            return {"submission_id": submission_id,
                    "ok": all(c["ok"] for c in checks), "checks": checks}

    # ------------------------------------------------------------------ 查询与留痕
    @staticmethod
    def _submission_view(row) -> dict:
        return {"submission_id": row["submission_id"],
                "draft_id": row["draft_id"], "inquiry_id": row["inquiry_id"],
                "outbound_no": row["outbound_no"],
                "corrects_submission_id": row["corrects_submission_id"],
                "payload": json.loads(row["payload_json"]),
                "payload_hash": row["payload_hash"], "status": row["status"],
                "created_at": row["created_at"]}

    def get_submission(self, submission_id: str) -> dict:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM submissions WHERE submission_id=?",
                               (submission_id,)).fetchone()
            if not row:
                raise DomainError("NOT_FOUND", f"提交件不存在: {submission_id}", 404)
            return self._submission_view(row)

    def list_submissions(self, inquiry_id: str) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM submissions WHERE inquiry_id=? ORDER BY created_at",
                (inquiry_id,)).fetchall()
            return [self._submission_view(r) for r in rows]

    def audit_trail(self, entity_type: str | None = None,
                    entity_id: str | None = None) -> list[dict]:
        """操作留痕查询（按实体过滤，按发生顺序返回）。"""
        sql, cond, args = "SELECT * FROM audit_log", [], []
        if entity_type:
            cond.append("entity_type=?")
            args.append(entity_type)
        if entity_id:
            cond.append("entity_id=?")
            args.append(entity_id)
        if cond:
            sql += " WHERE " + " AND ".join(cond)
        sql += " ORDER BY id"
        with self._conn() as conn:
            return [dict(r) for r in conn.execute(sql, args).fetchall()]
