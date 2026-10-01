# 监管问询答复编排器（Regulatory Inquiry Response Orchestrator）

接收监管方的**问题清单、截止时间和可引用资料范围**，把病例、已发送报文、
适用法规与统计结论的**精确版本**锁定成答复底稿；组稿期间引用变化不会悄悄
混入当前答复，而是产生**带原因的新底稿候选**。资料缺失、引用越权或版本
互相矛盾时阻止定稿，并准确列出尚未满足的问题。

纯 Python 标准库（3.11+，SQLite），独立子包 `reg_inquiry/`、独立数据库
`inquiry.db`，与 `pv_service/` 物理隔离、无第三方依赖。

```bash
# 端到端剧本演示
python3 run_inquiry_demo.py

# 测试（59 个：锁定/漂移/协作/校验/发送恢复/双节点幂等/篡改核对/审计链）
python3 -m unittest discover -t . -s tests_reg -v
```

## 需求映射

| 需求 | 实现 |
|---|---|
| 冻结问题清单/截止时间/可引用范围 | `inquiries` 只增（触发器拒绝改删），`scope_json` 在登记时固化 |
| 病例/报文/法规/统计**精确版本**锁定 | `materials` 四类只增版本（`(material_id,version)` 唯一、内容指纹去重）；`draft_pins` 冻结版本行 id + content_hash + 引用锚 `CASE:M@3` |
| 资料缺失 / 引用越权阻断并列出未满足项 | `lock_draft` 逐项收集 `MISSING_MATERIALS`（含缺失 id/版本/类别）与 `OUT_OF_SCOPE`（资料 id/类别不在范围），不落任何半成品底稿 |
| 版本互相矛盾阻断 | 资料注册时声明 `fact_key/fact_value`；同 fact_key 不同值在锁定（`ContradictionError`）与定稿（`PIN_CONTRADICTION`）两处拦截并登记 `contradictions` |
| 引用变化不混入当前答复，另立带原因候选 | 注册新版本时 `spawn_drift_candidates` 为引用旧版本的开放 BASE/更正底稿克隆出 `CANDIDATE`（pin 新版本、携带 drift_reason 与 old→new 变化明细、克隆章节/附件并把附件溯源重指新版本）；当前底稿 pins 一个字节不改；同链幂等 |
| 章节独立编辑、多人并行 | `sections (draft_id,qid)` 唯一；不同章节互不干扰，5 撰写人并发同章节仅 1 人成功 |
| 竞争更新以调用方看到的修订为前提 | 乐观修订号：提交必须带 `expected_revision`；过期提交返回 `STALE_REVISION`、不覆盖，冲突独立事务落 `section_conflicts`（不随业务回滚丢失） |
| 定稿：每问唯一可追踪回答 | manifest 为每问生成 `answer_uid=sha256(draft,qid,section,revision)` 与 `body_sha256`；`NO_ANSWER/EMPTY_ANSWER/MULTIPLE_ANSWERS` 阻断 |
| 附件与正文引用一致 | 附件以稳定锚 `@attachment:<id>` 在正文引用；`ATTACHMENT_NOT_CITED/ATTACHMENT_REF_MISSING/ATTACHMENT_REF_NOT_LOCKED` 阻断；附件溯源必须指向已锁定 pin |
| 敏感字段符合收件辖区披露策略 | 辖区只增策略（`REDACT/PROHIBIT/ALLOW`）；`SENSITIVE_NOT_REDACTED/SENSITIVE_PROHIBIT` 按辖区阻断 |
| 不可变提交件 | `submissions` 行触发器只放行发送状态机列；`manifest_hash` 锁定清单；底稿 `OPEN→FINALIZED`，同问询其余开放底稿置 SUPERSEDED |
| 后续改动只能产生更正件，保留原件与回执 | `create_correction_draft` 生成 kind=CORRECTION、带 `corrects_submission_id` 指针的新底稿/新提交件/新编号；原提交件、回执、履历原样保留，`correction_chain` 可遍历 |
| 可恢复作业（组稿/校验/定稿/发送） | `jobs`（LOCK_DRAFT/VALIDATE/FINALIZE/SEND）：幂等键唯一、条件 UPDATE 领取、attempt 租约栅栏、checkpoint 心跳、崩溃恢复孤儿作业与 SENDING 残留 |
| 重复请求/双节点不产生重复件/编号/履历 | 外发编号在 IMMEDIATE 事务内由序列分配 + `outgoing_number UNIQUE`；发送条件领取 + 处理器以提交件当前状态短路重放；`submission_events (sub,type,detail)` 去重；网关以编号为幂等键 |
| 监管回执 | `receipts` 回执号唯一、与提交件外发编号交叉核对；回执已存在时崩溃恢复不再外发 |
| 独立核对 | `verifier.py` 不走组装路径，逐行重算 manifest/正文/附件/pin/编号/履历/回执/更正链/审计哈希链，篡改逐项列出（BODY/MANIFEST/ATTACHMENT/PIN/RECEIPT hash mismatch 等） |
| 操作留痕 | 全局只增哈希链 `audit_events`（seq/prev_hash/entry_hash，帧覆盖全部业务字段），幂等键防重复履历；`verify_audit_chain` 离线重放 |

## 关键设计

**为什么引用变化“混不进来”**：底稿锁定时把引用解析成 `materials` 的**具体行**
（版本行 id + 版本号 + content_hash），之后读到的都是冻结快照；新版本注册
只追加、不改旧行（触发器），漂移逻辑另开 CANDIDATE 而不改任何 pin。
定稿使用的 manifest 再次固定 pins 与正文/附件哈希，提交件封存后连状态机
之外的字段都无法 UPDATE。

**为什么双节点/重复请求安全（三层）**：
1. SQLite `BEGIN IMMEDIATE` 使所有写者全局串行；编号在同事务内由
   `UPDATE ... RETURNING last_no+1` 分配，`outgoing_number UNIQUE` 兜底；
2. 作业领取是带 `WHERE status='PENDING' OR 租约过期` 的条件 UPDATE，
   `attempt` 栅栏让租约过期后旧工作者的完成/失败结果写不回；
3. 发送处理器在事务内先看提交件当前状态（SENT/DELIVERED/REJECTED 直接
   幂等返回），外部传输以外发编号为幂等键——即使“网关已受理、本地崩溃”，
   重放只取回同一受理号。

**乐观并发与留痕并存**：过期提交抛 `STALE_REVISION` 会回滚业务事务，
编排器随即在**独立事务**重放冲突留痕，保证“不覆盖”与“冲突可查询”同时成立。

**永久失败可重开**：校验失败（VALIDATION_FAILED）等永久失败作业保留为
FAILED 留痕；补齐内容后以同一幂等键重新定稿时，旧行改名 `#failed-<id>`
保留，新作业占用原键重跑（触发器仅放行这一种键改名）。

## API 一览（`InquiryOrchestrator`）

| 能力 | 方法 |
|---|---|
| 问询登记 | `register_inquiry` / `get_inquiry` / `list_inquiries` |
| 资料/策略 | `register_material`（新版本自动派生漂移候选）/ `put_disclosure_policy` |
| 底稿锁定 | `lock_draft`（直接，幂等）/ `lock_draft_job`（可恢复作业） |
| 章节协作 | `submit_section(draft,qid,body,expected_revision=...)` / `section_history` |
| 附件 | `add_attachment(derived_from_ref=...)` / `get_attachment` |
| 冲突查询 | `conflicts(draft)`（章节冲突/矛盾/漂移事件与警告） |
| 定稿校验 | `validate(draft)`（只读结构化报告）/ `validate_job` |
| 定稿封存 | `finalize(draft)`（校验通过才生成不可变提交件） |
| 发送与重试 | `send(submission_id)`（作业；自动重试/恢复/幂等）/ `submission_status` |
| 回执登记 | `register_receipt(receipt_number,payload)` / `list_receipts` |
| 更正关系 | `create_correction_draft(submission,refs,reason)` / `correction_chain` |
| 独立核对 | `verify_submission` / `verify_all` |
| 操作留痕 | `audit_trail(inquiry_id,event_type)` / `verify_audit_chain` |
| 作业恢复 | `pump()`（工作节点一步）/ `run_job(id)` / `recover()` / `list_jobs` |

引用锚语法：`[CASE:material_id@version]`、`[SENT_REPORT:..@..]`、
`[REG:..@..]`、`[STAT:..@..]`；附件锚 `@attachment:<id>`。
