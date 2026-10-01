# 药物不良事件病例接收与监管报送服务（Pharmacovigilance Case & Reporting Service）

接收医院、患者热线、合作企业三渠道的不良事件报文，处理重复上报、随访更正、
监管加急报送与报告版本追溯。**纯 Python 标准库实现**（Python 3.11+，SQLite），
无任何第三方依赖。

## 需求映射

| 需求 | 实现 |
|---|---|
| 字段缺失 / 姓名别名 / 多语言导致重复上报 | `normalize.py`：中/英/西语药名、反应、严重性、因果性、姓名别名词典；宽松日期解析 |
| 给出**可复核**疑似重复簇，专员确认合并 | `dedup.py` 加权打分（强标识 100 分短路；姓名+DOB+药+反应+日期组合 ≥70），每条边带分值与命中理由；`PROPOSED → CONFIRMED/REJECTED` 只有人工能驱动 |
| 原始来源记录始终保留 | `source_records`、`case_versions`、`case_events` 全部只增，由 SQLite 触发器拒绝 UPDATE/DELETE |
| 随访修正用药时间/严重性/因果性 | 白名单补丁 + `new_reactions/new_drugs/drug_start_dates`，产生**新版本**而非覆盖 |
| 按规则判断加急 | `rules.py`：严重×非预期=15 日加急；严重已预期 30 日；非严重 90 日；无实质新信息不重复报送。每条决策带 `rule_id`+理由 |
| 新版本更正既有报告 | FOLLOWUP 报告 `followup_of_id` 关联原报告，payload 携带 `supersedes_regulatory_numbers` |
| 来源任何时刻只归属一个当前病例 | `case_membership` 只增（旧行 1→0 停用+新行插入）+ 部分唯一索引 `WHERE active=1` 数据库兜底 |
| 合并/拆分与回执并发 | 进程内共享 RLock + `BEGIN IMMEDIATE`；转移前再校验 active 归属，冲突返回 409 |
| 已发送报告可追溯 | 报告行永久保留；病例血缘闭包 `case_lineage` 使合并/拆分后的完整时间线可从存活病例追溯 |
| 重试/重启不产生重复监管编号 | 编号在业务事务内由序列表分配 + `regulatory_number UNIQUE`；发送只是状态机（QUEUED→SENDING→ACKED/FAILED/VOID）；网关以编号为幂等键；重启把 SENDING 重置回 QUEUED，重放取回同一 ack |
| 病例工作台 / 规则化接口 / 版本时间线 | HTTP JSON API（见下） |
| **药物安全决定双人会签** | `countersign.py` + `revision.py`：不可变评审单（候选归并/严重等级变化/待发送决定冻结，触发器禁改）；两名不同复核员对同一数据修订号签署才放行，同人重复提交不累计，任一人否决即终态；**复核员身份按 `identity.py` NFKC+大小写+首尾空白规范化稳定判定，空白/非法身份明确拒绝，原始提交值原样留痕，旧库启动时迁移到规范化身份键**；新材料/归并/拆解/规则重算在业务事务内推进修订号并原子作废旧单（意见保留但不能发送）；租约原子领取/到期释放/重启恢复；放行按 `report_id` 只解锁一次；未会签强制拦截发送，老调用可用显式兼容开关 |
| 可移交的证据包导出与离线核验 | `export_service.py`：自包含 tar（来源原文/版本快照/合并拆分关系/报送回执/审计时间线），清单根 + 每文件 SHA-256 + Ed25519 签名；`verifier.py` 在另一进程纯离线逐项核验 |
| **药物安全法规规则变更：影响分析与受控启用** | `rule_pkg.py`（不可变规则包内容/HMAC 签名/生效区间/版本化引擎）+ `rule_gov.py`（登记三校验、历史快照试算、受控启用/回滚/定时、检查点续跑、已发送只生成更正候选、治理审计哈希链）；计算/会签/发送路径固定本次规则版本 |
| **药物安全信号：可复演窗口计算** | `signal_metrics.py`（四格表 + PRR/ROR 对数正态 95%CI + 卡方 + 阈值判定）、`signal_engine.py`（截止线快照/冻结术语归一/归并拆解撤回解析/确定性分片）、`signal_service.py`（定义与术语版本只增、计算键幂等、分片检查点、只增信号修订、撤销-重计贡献台账、可追踪差异、乐观修订号处置、崩溃恢复）、独立 `signal.db`（触发器兜底不可变/去重） |

## 双人会签（药物安全决定）

药物安全决定（候选归并关系、严重等级变化、待发送监管报告）在发送前必须
经过**两名不同复核员对同一份不可变评审单、同一个数据修订号**会签。

**评审单（不可变）**：发起时把三类内容冻结进 `review_sheet_items`
（数据库触发器拒绝 UPDATE/DELETE）：待发送决定（`PENDING_REPORT`，含规则
`rule_id`/加急/截止日）、候选归并簇（`MERGE_CANDIDATE`，含分值理由）、
严重等级翻转（`SERIOUSNESS_CHANGE`）。按内容 SHA-256 去重，部分唯一索引
`ux_review_open_content` 保证并发重复发起只产生一张 OPEN 单。

**数据修订号（失效机制）**：`review_revision` 单行全局版本。新材料入库
（接收/随访）、归并确认、拆解、规则重新计算都在各自的 `BEGIN IMMEDIATE`
事务内 `bump_revision()`，并原子地把所有 OPEN 单置为 `INVALIDATED`
（记录 `NEW_MATERIAL/MERGED/SPLIT/RULE_RECALCULATED` 原因）。旧单的签署
意见（`review_events` 流水 + `review_signatures` 槽位）全部保留、可审计，
但永远不能再推动放行；再次发起只能引用最新修订号。

**会签规则**：

- 状态机单向：`OPEN → APPROVED | REJECTED | INVALIDATED`（均终态）；
- `review_signatures` 主键 `(sheet_id, reviewer_key)`：同一身份只能占一个
  意见槽，重复签署返回 409 `ALREADY_SIGNED`，**不累计票数**；身份键由
  `identity.py` 做 NFKC Unicode 等价折叠 + `casefold` 大小写归一 +
  仅去首尾空白，因此 `alice` / `ALICE` / `" alice "` / 全角 `Ａlice` /
  `Å`(U+212B) 与 `Å`(U+00C5) 都稳定判为同一复核员，单人无法靠表面写法
  差异占满两槽；两名不同复核员 APPROVED 才完成；任一人 VETOED 立即
  REJECTED 结束本轮；
- 空白 / 非字符串 / 超长身份在领取、释放、赞成、否决、撤回及待办/审计
  查询上一律明确拒绝（400 `BLANK_REVIEWER` / `INVALID_REVIEWER` /
  `REVIEWER_TOO_LONG`），绝不把缺失身份静默当作某个复核员；内部空白
  有意义（`"a b"` 与 `"ab"` 是两个人）；
- 每次提交的**原始字符串原样留痕**（签名 `reviewer`、事件 `actor`、
  放行 `approved_by_json`），规范化键 `reviewer_key` / `actor_key` /
  `lease_holder_key` 只用于身份判定与比较，全程可追溯；
- 撤回只对 OPEN 单的赞成意见有效（删槽，流水保留），否决为终态不可撤回；
- 全部动作（创建/领取/释放/赞成/否决/撤回/失效/租约到期）追加进只增的
  `review_events`，会签通过/否决同时镜像到病例审计哈希链。

**租约（待办协作）**：领取是带条件的原子 UPDATE（未持有或已到期才能成功），
多人抢领只有一人成功，其余 409；TTL 到期由 `expire_leases` 释放并留痕；
`ReviewService` 构造（服务恢复）时清理到期租约，**不产生任何业务结论**。
租约持有者按规范化身份键判定：同一复核员用任一等价写法领取是本人续租
（幂等），释放/待办可见性/到期留痕也都以同一身份键稳定比较。租约只影响
待办工作台可见性，不限制签署权（真正的并发安全由槽位主键保证）。

**一个生效结论**：抢领、同人重复签、批准与撤回/否决并发、服务恢复等所有
竞争都在共享 RLock + SQLite 写锁上串行，只可能有一个终态。批准完成时
`countersign_approvals` 以 `report_id` 为主键幂等解锁冻结的待发送决定；
已 ACKED/VOID 的冻结项跳过；修订后新排队的报告必须重新会签。

**发送闸门与兼容开关**：默认不改变既有行为；以
`--require-countersign` 启动后，未取得会签放行的报告在派发时返回
403 `COUNTERSIGN_REQUIRED`（后台派发线程同样遵守，只发送已放行项）。
原有无需会签的调用可在 `--allow-countersign-compat` 启动时，于派发请求中
显式带 `"allow_ungated": true` 继续使用（未在服务端开启兼容时该字段无效）。

```bash
# 强制双人会签启动；老调用可经显式兼容开关放行
python3 -m pv_service.serve --data data/pv --port 8080 --seed-demo \
    --require-countersign --allow-countersign-compat
```

典型流程：`POST /reviews` 冻结评审单 → 两名复核员分别
`/claim`（可选）、`/approve` → `POST /reports/dispatch` 发送；
任一人 `/veto` 本轮终止；评审期间随访/合并/拆分/规则重算自动作废旧单，
从 `GET /reviews/revisions` 可查失效原因，从 `GET /reviews/{id}` 可见
保留的全部历史意见。

## 药物安全法规规则变更：影响分析与受控启用

法规报送口径以**不可变、带签名、可追溯版本**的规则包管理。模块独立于
既有计算路径，但把「本次使用的规则版本」一路固定到报告行、会签冻结单
与发送回执，使旧规则与候选规则、旧结果与更正结果都可复核、不可混淆。

**规则包登记与三校验**（`rule_pkg.py` / `rule_gov.py`）：每个包携带
辖区、版本、半开生效区间 `[effective_from, effective_to)`、依赖版本、
内容摘要与报送天数口径（`serious/listed/routine_days`）。登记时：

1. *签名校验*：HMAC-SHA256（密钥按 `signer_id` 在只增密钥注册表解析，
   常量时间比较），签名不符/未知签名人拒绝；
2. *依赖校验*：`depends_on` 的 (package,version) 必须已登记；
3. *时间区间冲突*：同辖区任意两包生效区间不得相交（首尾相接合法）。

内容指纹 = 规范 JSON 的 SHA-256：**相同内容重复登记返回原版本**
（`deduped=true`）；**不同内容占用同一版本号** 由
`UNIQUE(jurisdiction,version)` 兜底拒绝（409 `VERSION_CONFLICT`）。
规则包行只增（触发器拒绝 UPDATE/DELETE）。

**试算（影响分析，纯只读）**：选择一组历史数据快照（显式病例/版本，
或按 `as_of/limit` 选最新版本），创建任务时把快照副本冻结进
`rule_trial_units`，再用基线版本（缺省 `builtin:legacy@0`）与候选版本
分别重算决定，逐分片落字段级差异与受影响病例集合。试算**不改病例、
不会签、不发送**；任务按 `(辖区,候选包,幂等键)` 唯一。

**受控启用绑定当前版本**：启用可带 `expected_package_id/version` 乐观
条件，调用方基于过期视图启用返回 409 `VERSION_MOVED`。并发启用/回滚/
定时在共享 RLock + 辖区状态行上串行；同 `idempotency_key` 重放返回同一
结论——**只有一个生效结果**。

**已发送 vs 未发送（关键合规边界）**：

- *未发送*（QUEUED/SENDING/FAILED）：启用成功后在单事务内按新版本重算
  决定/截止日并改写固定版本，**监管编号不变**；新版本判定无需报送则
  VOID（编号保留）。重算推进数据修订号，**作废旧会签单**，须以新决定
  重新双人会签才能发送；
- *已发送*（ACKED）：报告行、原始规则版本、历史结果**永不改动**，只
  生成一条可复核的 `rule_correction_candidates`（含原始/候选决定与差异），
  同 (报告,候选版本) 去重，绝不覆盖原记录。

**两阶段 + 检查点**：启用作业阶段一逐分片把新决定暂存到
`rule_job_units`（每个单元独立短事务），全部分片成功后阶段二在**单
BEGIN IMMEDIATE 事务**内原子应用并重写辖区版本指针。**任一分片失败 →
作业 FAILED、版本指针不动、无部分启用**；进程中断后 `recover` 把
RUNNING 任务重置为可续跑，从 `next_shard` 只补未完成单元，
**重复请求不重复生成差异、更正候选或审计事件**。定时生效由
`rule_schedules`（每辖区至多一个 PENDING）到点触发，FIRED 关联启用记录。

`rule_audit_events` 是一条全局只增哈希链（seq/prev_hash/entry_hash，
审计帧带幂等键），`GET /rules/audit/verify` 可离线重放核验。

```bash
# 后台自动续跑中断任务 + 触发到点定时生效
python3 -m pv_service.serve --data data/pv --port 8080 --seed-demo \
    --jurisdiction CN --auto-rule-scheduler
```

## 药物安全信号：可复演窗口计算

分析员为一个监测问题登记**术语归一版本**与**监测定义**（药品集合、事件分组、
观察期、最小样本量、PRR/ROR 阈值），随后可对任意**病例版本截止线**反复提交
窗口计算。结果落在与病例库分离的 `data/<dir>/signal.db`，不反向改动病例库。

### 可复演性是如何冻结的

- **计算键** `computation_key = sha256(定义|截止线|术语版本|模式|基线运行)`：
  `sig_runs.computation_key UNIQUE` 兜底，并发或重复提交同一参数只产生
  **一个作业**；相同配置与截止线再次计算返回同一 `run_id`、同一份信号修订。
- **提交即冻结输入**：提交事务按 `created_at <= cutoff` 选出每病例的最新
  版本，写入 `sig_run_case_snapshot`；截止线之后的迟到随访/归并/因果修正
  绝不混入已冻结运行（`GET .../runs/{id}/verify` 只读重算比对校验和）。
- **术语版本固定**：事件原文按运行冻结的 `sig_terms` 映射到规范事件码；
  换术语版本会改变计算键，旧运行仍按旧版本可复算。
- 定义/术语/病例版本表只增（触发器拒绝改删）；浮点使用固定的标准正态
  97.5% 常量，分片按 `sha256(source_id) % shard_count` 确定性切分。

### 四格表与判定

对每个药品组 D × 事件组 E × 观察期输出 `a/b/c/d`（D∩E / D∩¬E / ¬D∩E /
¬D∩¬E）与 `n`，并计算 PRR=(a/(a+b))/(c/(c+d))、ROR=ad/(bc)、两者
95%CI（Woolf 对数正态）与 Pearson 卡方。零格默认不静默加修正（指标为
`null`，判定 `INSUFFICIENT`/未通过），口径需要 Haldane-Anscombe 修正时
在定义里显式 `continuity_correction:true`。判定阈值支持 `prr_min`、
`prr_ci_lower_than`、`ror_min`、`ror_ci_lower_than`、`chi_square_min`、
`min_n/min_a` 与 `require=ALL|ANY`，输出 `SIGNAL/NO_SIGNAL/INSUFFICIENT`
及逐项门槛明细。

### 分片、失败与崩溃恢复

全量回算与增量更新都走分片作业：每个分片在**独立短事务**提交检查点
（计数、花名册、状态与 checksum 落 `sig_run_shards`），崩溃后
`POST /signals/recover` 重置失败片并从已提交片续跑。只有**全部分片
COMMITTED** 才在**单个 IMMEDIATE 事务**内发布；任一片失败 → 作业
FAILED，不发布任何半成品指标/修订。并发推进同一作业只发布一组修订
（发布是幂等的，重复进入直接返回）。

### 只增信号修订、撤销-重计与可追踪差异

- 发布只向 `sig_signal_revisions` **追加**每个 (定义,窗口,药品组,事件组)
  的新 `revision_no`；四格计数/指标/判定列由触发器锁定，后台计算**无法
  覆盖**已发布快照，只有分析师处置列允许状态机更新。
- 后续窗口遇到迟到随访、病例归并/拆解、撤回或因果评价修正时，发布事务先
  `revoked=1` 撤销这些窗口的旧贡献，再按新版本写入新贡献。
  `sig_case_contributions` 的部分唯一索引保证**同一来源在一个窗口只
  贡献一次**（格归属存该来源的完整 `cells` 快照，整体替换不残留）。
- 被吸收来源标 `MERGED_AWAY`、撤回标 `WITHDRAWN`、不可映射事件标
  `EVENT_UNMAPPED`、非监测药品标 `DRUG_NOT_MONITORED`，全部进入可追溯
  排除清单。修订跨药品组/事件组或病例退出样本时，`sig_signal_diffs`
  给出病例级 `NEW_ENTERED/EXITED_SAMPLE/DRUG_GROUP_CHANGE/EVENT_GROUP_CHANGE/
  CAUSALITY_UPDATE/...`（含旧格→新格）与指标 delta。

### 分析师处置与乐观修订号

修订发布后为 `PENDING`，分析师可 `CONFIRMED/REFUTED/WATCH`。处置必须带
`expected_revision_no`（链头修订号）：基于过期号或对非链头旧修订操作一律
`409 STALE_REVISION`；**旧修订上已有的确认/驳回/观察结论永久保留**
（`409 ALREADY_DISPOSED`），新修订是独立 PENDING 行必须重新处置。所有
动作进入只增的全局审计哈希链 `sig_audit_events`（`GET /signals/audit/verify`
离线重放核验）。

```bash
# 端到端：术语版本 + 定义 + 病例版本 + 提交（默认 wait 到发布）
curl -XPOST localhost:8080/api/v1/signals/terms -d '{
  "term_version":"terms@1",
  "mappings":{"肝炎":"HEP","hepatitis":"HEP","头痛":"HEAD"}}'
curl -XPOST localhost:8080/api/v1/signals/definitions -d '{
  "definition_code":"HEP-X-2026Q1",
  "config":{"drug_groups":{"X":["X"]},"event_groups":{"HEP":["HEP"]},
    "windows":[{"id":"2026Q1","start":"2026-01-01","end":"2026-04-01"}],
    "min_n":3,"prr_min":2,"prr_ci_lower_than":1,
    "ror_min":2,"ror_ci_lower_than":1,"require":"ALL"}}'
curl -XPOST localhost:8080/api/v1/signals/runs -d '{
  "definition_id":"...","term_version_id":"...","cutoff":"2026-04-01T00:00:00"}'
```

| 方法 | 路径 | 说明 |
|---|---|---|
| POST/GET | `/api/v1/signals/terms`、`/terms/{id}` | 术语归一版本登记（内容幂等）/列表/详情 |
| POST/GET | `/api/v1/signals/definitions`、`/definitions/{id}` | 监测定义（内容冻结、相同内容幂等） |
| POST | `/api/v1/signals/cases` | 录入只增病例版本（含 merged_from/split_from/撤回/因果） |
| POST/GET | `/api/v1/signals/runs` | 提交计算（计算键幂等；`wait/mode/base_run_id/shard_count`）/列表 |
| GET | `/api/v1/signals/runs/{id}` | 运行与分片提交进度 |
| POST | `/api/v1/signals/runs/{id}/shards` | 推进未提交分片（`max_shards`，检查点） |
| POST | `/api/v1/signals/runs/{id}/publish` | 续跑到可发布并原子发布 |
| GET | `/api/v1/signals/runs/{id}/verify` | 只读重算核验可复演性（分片校验和/计数比对） |
| GET | `/api/v1/signals/runs/{id}/exclusions` | 各窗口排除来源及原因 |
| GET | `/api/v1/signals/definitions/{id}/results` | 窗口四格计数/指标/判定（默认仅链头修订） |
| GET | `/api/v1/signals/revisions/{id}` | 修订详情：指标 + 冻结入选病例清单 + 差异 |
| POST | `/api/v1/signals/revisions/{id}/disposition` | 确认/驳回/观察（`expected_revision_no` 乐观号） |
| GET | `/api/v1/signals/definitions/{id}/diffs` | 修订差异（病例级跨组/退出 + 指标 delta） |
| GET | `/api/v1/signals/definitions/{id}/dispositions` | 分析师处置历史 |
| GET | `/api/v1/signals/contributions` | 病例贡献追踪（`source_id/window_id`，含撤销链） |
| POST | `/api/v1/signals/recover` | 崩溃恢复：重置失败分片，从已提交分片续跑 |
| GET | `/api/v1/signals/audit`、`/audit/verify` | 审计事件查询 / 哈希链核验 |

## 证据包与离线核验

安全专员对**病例的某个版本**发起导出，得到一个自包含、可移交、带签名的 tar 包。

**包内五段证据 + 完整性锚点**

- `sources/*.json` 来源原文（医院/热线/企业 payload 原样）；
- `cases/*.json`、`export/snapshot.json` 发起时绑定的版本快照序列；
- `relations.json` 截止锚点时的合并/拆分关系（含被合并病例与拆分母病例血缘）；
- `reports/*.json` 报送记录、**截止时冻结状态**与监管回执 ack；
- `timeline/*.json` 各病例审计哈希链（`seq/prev_hash/entry_hash`）；
- `manifest.json`（清单：每文件 sha256/size + 链头）、`manifest.sig`（Ed25519）、
  `package.json`（签名公钥与指纹）。

**四条硬约束如何成立**

1. *快照绑定，不拼接*：导出锚定具体版本，整个收集在一个 `BEGIN IMMEDIATE`
   事务内按 rowid 截止线读取；导出期间的随访/合并/拆分只追加新版本/新行，
   绝不混进包里。引用悬空（来源/报告缺失）或审计链断裂 → 任务 `FAILED`
   （`SNAPSHOT_INCOMPLETE` / `AUDIT_CHAIN_BROKEN`），不落任何半成品包。
   报告状态由截止审计帧重放冻结，锚点之后才到达的 ACK 不会被拼进来。
2. *相同请求只留一份包*：`export_tasks(case_id, version_id, idempotency_key)`
   UNIQUE + 内容寻址文件（`packages/<manifest-root>.tar`）+ 确定性构建，
   并发请求、失败重试、进程重启都只产生同一任务、同一文件。
3. *崩溃恢复*：重启把 `BUILDING` 重置为 `PENDING` 重跑；包文件丢失的
   `COMPLETED` 任务重新排队；只清理被重置/无主的临时目录，不影响并发进程。
4. *离线核验逐项失败、绝不误报有效*：纯标准库 Ed25519（RFC 8032，
   与 OpenSSL 双向交叉验证）核验签名；重算清单根、每个文件摘要、审计链
   每一帧、版本状态、报告回执一致性、合并拆分关系。任何一项失败都在结果中
   逐条列出具体失败项，`valid=false`。可用带外公钥指纹 `--fingerprint` 绑定来源。

```bash
# 发起导出（HTTP）后下载；或直接离线核验（不连数据库/网络）
python3 -m pv_service.verify_offline package.tar                 # 退出码 0 有效 / 2 核验失败 / 3 无法打开
python3 -m pv_service.verify_offline package.tar --fingerprint <hex>  # 绑定带外信任公钥
python3 -m pv_service.verify_offline package.tar --json          # 完整结构化报告
```

## 快速开始

```bash
# 运行测试（274 个，含并发、崩溃恢复、幂等、证据包篡改/离线核验、双人会签、
# 法规规则登记/试算/受控启用/回滚/定时/分片失败/检查点续跑/治理审计链/
# 信号窗口计算复演/撤销重计/修订差异/乐观处置）
python3 -m unittest discover -s tests -v

# 端到端剧本演示（无需起服务）
python3 run_demo.py

# 启动 HTTP 服务
python3 -m pv_service.serve --data data/pv --port 8080 --seed-demo
```

## HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/v1/intake` | 来源报文接收（`source_system`+`external_id` 幂等） |
| GET | `/api/v1/cases` | 病例工作台（状态、来源、待复核簇、报告） |
| GET | `/api/v1/cases/{id}` | 病例详情：全部版本 + 完整时间线 |
| POST | `/api/v1/cases/{id}/followup` | 随访材料（产生新版本，自动判定是否报送/加急） |
| POST | `/api/v1/cases/{id}/split` | 将某来源拆分到新病例 |
| GET | `/api/v1/duplicates?status=PROPOSED` | 待复核疑似重复簇（含分值理由） |
| POST | `/api/v1/duplicates/{id}/confirm` | 专员确认合并（可指定 survivor） |
| POST | `/api/v1/duplicates/{id}/reject` | 专员否决（该边永不复活） |
| GET | `/api/v1/reports?status=` | 报告列表 |
| GET | `/api/v1/reports/due` | 待报送/逾期视图（加急优先排序） |
| POST | `/api/v1/reports/dispatch` | 同步派发待发送报告（强制会签时未放行返回 403；兼容模式可带 `allow_ungated`） |
| GET | `/api/v1/reports/{id}` | 报告详情（含监管回执 ack） |
| POST | `/api/v1/reports/{id}/requeue` | 失败报告人工重置重发（编号不变，会签闸门保留） |
| POST | `/api/v1/reviews` | 发起不可变会签评审单（`report_ids` 可选；冻结最新修订号） |
| GET | `/api/v1/reviews/todos` | 待办领取工作台（`?reviewer=`） |
| POST | `/api/v1/reviews/{id}/claim` | 领取待办租约（`{reviewer}`，多人抢领仅一人成功） |
| POST | `/api/v1/reviews/{id}/release` | 主动释放租约 |
| POST | `/api/v1/reviews/{id}/approve` | 赞成签署（`{reviewer,comment}`；两名不同人即放行） |
| POST | `/api/v1/reviews/{id}/veto` | 否决（任一人否决即结束本轮） |
| POST | `/api/v1/reviews/{id}/withdraw` | 撤回本人赞成（仅 OPEN 单） |
| POST | `/api/v1/reviews/{id}/expire` | 释放到期租约（维护/恢复） |
| GET | `/api/v1/reviews/{id}` | 评审单详情（冻结项/活意见/完整事件流水） |
| GET | `/api/v1/reviews/history` | 决定历史（`?report_id=&case_id=&status=`） |
| GET | `/api/v1/reviews/audit` | 会签审计查询（`?sheet_id=&event_type=&actor=`） |
| GET | `/api/v1/reviews/revisions` | 修订号推进与旧单失效原因历史 |
| GET | `/api/v1/reviews/approvals` | 会签放行记录（`?report_id=`） |
| POST | `/api/v1/rules/recalculate` | 规则重新计算（作废旧评审单，逐病例落审计） |
| POST | `/api/v1/rule-packages` | 登记不可变规则包（签名/依赖/区间三校验；相同内容返回原版本） |
| POST | `/api/v1/rule-packages/sign` | 用密钥对规则包内容生成 HMAC 签名（登记辅助） |
| GET | `/api/v1/rule-packages` | 规则包列表（`?jurisdiction=`） |
| GET | `/api/v1/rule-packages/{id}` | 规则包详情（含内容/指纹/签名/依赖） |
| GET | `/api/v1/rules/current` | 当前生效规则版本（`?jurisdiction=`，缺省内置 legacy） |
| POST | `/api/v1/rules/trials` | 创建试算任务（冻结历史快照集合，`candidate`/`baseline`/`selector`） |
| GET | `/api/v1/rules/trials` | 试算任务列表（`?jurisdiction=`） |
| GET | `/api/v1/rules/trials/{id}` | 试算任务状态 |
| POST | `/api/v1/rules/trials/{id}/run` | 从检查点续跑试算（`max_shards` 可选） |
| GET | `/api/v1/rules/trials/{id}/progress` | 进度 + 受影响病例/报告集合 |
| GET | `/api/v1/rules/trials/{id}/diffs` | 旧/候选决定差异（`?changed_only=&limit=`） |
| POST | `/api/v1/rules/activate` | 受控启用（`target`，可带 `expected_*` 绑定当前版本/幂等键） |
| POST | `/api/v1/rules/rollback` | 回滚（`target` 可省=上一版本） |
| GET | `/api/v1/rules/activations` | 启用/回滚/定时生效历史（`?jurisdiction=`） |
| GET | `/api/v1/rules/jobs` | 启用作业列表（`?jurisdiction=`） |
| GET | `/api/v1/rules/jobs/{id}` | 作业状态/分片进度/更正候选数 |
| POST | `/api/v1/rules/jobs/{id}/run` | 从检查点续跑启用作业 |
| GET | `/api/v1/rules/corrections` | 已发送报告的更正候选（`?report_id=&jurisdiction=`） |
| POST | `/api/v1/rules/schedules` | 创建定时生效计划（每辖区至多一个 PENDING） |
| GET | `/api/v1/rules/schedules` | 定时计划列表（`?status=`） |
| POST | `/api/v1/rules/schedules/{id}/cancel` | 取消定时计划 |
| POST | `/api/v1/rules/recover` | 崩溃恢复（重置中断任务）并续跑（`{"resume":true}`） |
| GET | `/api/v1/rules/audit` | 规则治理审计事件（`?event_type=&jurisdiction=`） |
| GET | `/api/v1/rules/audit/verify` | 核验治理审计哈希链 |
| GET | `/api/v1/cases/{id}/timeline` | 病例审计时间线（含 seq/prev_hash/entry_hash 哈希链） |
| POST | `/api/v1/exports` | 发起证据包导出（`case_id`+可选 `version_id`/`idempotency_key`，幂等；默认 `wait`） |
| GET | `/api/v1/exports` | 导出任务列表（可按 `?case_id=` 过滤） |
| GET | `/api/v1/exports/{id}` | 导出任务状态与构建进度 |
| GET | `/api/v1/exports/{id}/download` | 下载证据包（`application/x-tar`） |
| POST | `/api/v1/exports/{id}/verify` | 服务端离线核验（逐项结果并留档） |
| GET | `/api/v1/exports/{id}/verifications` | 该任务的历史核验结果 |

### 报文示例

```jsonc
// POST /api/v1/intake
{
  "source_system": "HOSPITAL",        // HOSPITAL | HOTLINE | ENTERPRISE
  "external_id": "H-2026-1001",
  "received_at": "2026-09-10T09:00:00",
  "payload": {
    "patient_name": "张三", "gender": "男", "dob": "1980-03-03",
    "patient_identifier": "ID-998",
    "drugs": [{"name": "布洛芬", "start_date": "2026-09-05", "causality": "可能"}],
    "reactions": ["皮疹"],
    "seriousness": "不严重", "event_date": "2026-09-07"
  }
}
```

同事件的热线报文可以是英文、别名（老张）、斜杠日期（2026/09/05）、
西语药名（ibuprofeno/sarpullido）——归一化后仍会识别为同事件。

## 关键设计说明

**编号安全为什么成立（三层）**
1. `number_sequence` 的 `INSERT ... ON CONFLICT DO UPDATE SET last=last+1`
   在与病例变更同一 `BEGIN IMMEDIATE` 事务内执行，SQLite 写事务全局串行；
2. `regulatory_reports.regulatory_number` 有 UNIQUE 约束 + 版本唯一索引
   （一个病例版本至多一份报告），重试只 UPDATE 状态行，永不重新分配编号；
3. 监管网关（`gateway.py`）以编号为幂等键，即使在“网关已受理、本地回执未落库”
   窗口崩溃，重启重放也只返回同一个 ack。作废报告（合并去重）编号同样永不复用。

**加急判定不是写死的**：规则是 `rules.py` 中按顺序求值的规则链，
天数（15/30/90）与 listedness 参考表均可配置；随访是否触发报送取决于
`significant_changes()` 对用药时间/严重性/因果性/转归/新增药械反应的差异分析。

**拆分并发**：来源转移 = 停用旧归属行 + 插入新归属行，部分唯一索引保证
数据库层面不可能出现双归属；8 线程并发拆分同一来源只有 1 个成功（有测试）。

## 模块

```
pv_service/
  db.py             表结构、不可变触发器、IMMEDIATE 事务、旧库审计链迁移
  normalize.py      多语言/别名/严重性/因果归一化、宽松日期
  dedup.py          加权打分、强标识短路、并查集聚类
  rules.py          15/30/90 日规则链、随访实质变化分析
  rule_pkg.py       规则包内容规范、HMAC 签名、生效区间、内容指纹、版本化引擎
  rule_audit.py     规则治理全局审计哈希链（登记/启用/回滚/定时/试算/恢复）
  rule_gov.py       登记三校验、历史快照试算、受控启用/回滚/定时、两阶段
                    分片+检查点续跑、已发送更正候选、幂等、乐观并发
  revision.py       数据修订号原语：业务事务内推进并原子作废旧评审单
  identity.py       复核员身份规范化（NFKC+casefold+首尾空白）与空白/非法拒绝
  countersign.py    双人会签：不可变评审单、租约、签署/否决/撤回、放行闸门、审计
  service.py        病例工作台：接收、随访、合并、拆分、血缘、时间线、编号分配、规则重算
  reporting.py      报告状态机、退避重试、崩溃恢复、后台派发
  gateway.py        模拟监管网关（幂等回执、可脚本化故障）
  audit.py          病例审计哈希链（seq/prev_hash/entry_hash，逐帧核验）
  ed25519.py        纯 Python Ed25519（RFC 8032，零第三方依赖）
  package_codec.py  确定性 tar 证据包 + 清单根 + 签名
  export_service.py 导出任务：版本快照绑定、去重、崩溃恢复、内容寻址落盘
  verifier.py       离线核验纯函数（逐项失败，绝不误报有效）
  verify_offline.py 离线核验命令行（另一进程，不连数据库）
  signal_db.py      信号子库 schema（signal.db）：只增触发器/去重部分唯一索引
  signal_metrics.py PRR/ROR(95%CI)/卡方纯函数与阈值判定（零格不静默修正）
  signal_engine.py  截止线快照解析、冻结术语归一、归并/拆解/撤回、确定性分片
  signal_audit.py   信号子系统全局审计哈希链
  signal_service.py 监测定义/术语版本、计算键幂等、分片检查点发布、撤销-重计
                    贡献台账、修订差异、乐观修订号处置、崩溃恢复、复算核验
  app.py            装配（共享锁、参考数据）
  api.py            标准库 http.server 的 JSON 接口
tests/              274 个测试（归一化/规则、去重、不可变、合并拆分、报送、
                     并发、API、重启恢复、审计链、Ed25519、证据包导出/篡改/离线核验、
                     双人会签核心/并发租约/HTTP 接口/兼容开关/
                     复核员身份规范化/旧库身份迁移/
                     法规规则包/治理/崩溃续跑/治理并发/治理 HTTP/
                     信号指标/引擎/复演/撤销重计/差异/处置乐观锁/分片崩溃/
                     计算键并发幂等/信号 HTTP 接口）
```

---

# 监管问询答复编排器（inquiry_reply，独立模块）

接收监管方的问题清单、截止时间与可引用资料范围，把病例、已发送报文、
适用法规与统计结论的**精确版本**锁定成答复底稿，组织多人协作撰写、
定稿校验、可恢复发送、回执登记与更正管理。**纯 Python 标准库实现**
（Python 3.11+，SQLite），独立数据库文件，不依赖 pv_service。

## 需求映射

| 需求 | 实现 |
|---|---|
| 问询登记（问题清单/截止时间/可引用资料范围） | `register_inquiry`：`idempotency_key` 唯一约束去重重复登记；范围支持精确 id 与 `前缀*` |
| 精确版本锁定成答复底稿 | `lock_draft`：冻结每条引用的 `(evidence_id, version, content_hash)`；`evidence_versions` 只增（触发器），旧版本永远可取回 |
| 组稿期间引用变化不悄悄混入，形成带原因的新底稿候选 | 底稿锁定内容不可改（`trg_drafts_guard` 触发器兜底）；`query_conflicts` 报告漂移；`refresh_draft` 生成 `REFRESH` 候选并写入 `EVIDENCE_UPDATED/EVIDENCE_MISSING` 原因，旧底稿置 `SUPERSEDED` 原样保留 |
| 资料缺失/引用越权/版本矛盾阻止定稿并列出未满足项 | `validate_draft`：`MISSING_EVIDENCE / SCOPE_VIOLATION / VERSION_CONFLICT / UNLOCKED_CITATION / STALE_CITATION / EVIDENCE_TAMPERED` 等硬性问题全部落入 `issues`，`finalize_and_send` 以 `VALIDATION_FAILED` 拒绝并附报告 |
| 多人分章节协作，过期提交不得覆盖新内容 | `submit_section`/`put_attachment` 乐观修订号：`base_revision` 必须等于调用方看到的当前修订号，否则 409 `STALE_REVISION`（附当前号）；不同章节可并行 |
| 定稿校验：问题唯一可追踪、附件一致、敏感字段合规 | 每问题恰一个非空回答章节（`QUESTION_UNANSWERED/AMBIGUOUS/EMPTY_ANSWER/UNKNOWN_QUESTION`）；正文 `[[att:id]]` 与附件双向一致（`ATTACHMENT_MISSING/UNREFERENCED`）；`policy.py` 按收件辖区扫描敏感字段（命中值脱敏留痕） |
| 不可变提交件，后续改动只能产生更正件，保留原件与回执 | `submissions` 触发器只允许 status 推进，payload/外发编号永不改写；`create_correction` 产生 `CORRECTION` 底稿 → 新提交件关联 `corrects_submission_id`；`correction_chain` 还原整条更正链（含各件回执） |
| 可恢复作业，中断后续跑，重复请求/双节点不产生重复 | `jobs.job_key` 唯一（同一答复键一个作业）；步骤效果+履历+检查点同事务提交；租约原子领取（单条 UPDATE）；外发编号在 finalize 事务内由序列表分配；`retry_job`/`run_pending_jobs` 断点续跑 |
| 回执登记 | `register_receipt`：`(submission_id, regulator_ref)` 唯一幂等 |
| 独立核对 | `independent_check`：不信服务侧标志位，重算 payload 哈希、重跑问题/附件/引用/披露校验、核对作业履历与审计留痕，篡改可检出 |
| 操作留痕 | `audit_log` 只增（触发器兜底），`audit_trail` 按实体过滤查询 |

## 生命周期

```
register_inquiry ──> lock_draft ──> submit_section/put_attachment（多人并行）
                          │                ▲
        资料变化 ──> query_conflicts ──> refresh_draft（带原因新候选，旧底稿作废保留）
                          ▼
                   validate_draft（未满足项清单）──> finalize_and_send
                          │                        （validate→finalize→send 可恢复作业）
                          ▼
                   submissions（不可变）──> register_receipt（监管回执）
                          │
                   create_correction ──> 更正底稿 ──> 更正件（关联原件）
                          ▼
                   independent_check（独立核对）/ audit_trail（操作留痕）
```

## 运行

```bash
python3 -m inquiry_reply                 # 端到端演示（临时库）
python3 -m unittest tests.test_inquiry_reply -v   # 35 个测试
```

## 模块

```
inquiry_reply/
  db.py         表结构、不可变触发器（底稿锁定/提交件/回执/履历/留痕）、IMMEDIATE 事务
  evidence.py   可引用资料库：病例/报文/法规/统计的只增版本化存储
  policy.py     收件辖区披露策略（敏感字段规则，可注册覆盖）
  transport.py  外发通道契约（outbound_no 幂等）；默认本地发件箱
  service.py    编排器：登记/锁定/协作/冲突/校验/作业/回执/更正/核对/留痕
  __main__.py   端到端演示
tests/test_inquiry_reply.py   35 个测试（锁定/漂移候选/阻止定稿/乐观并发/
                                作业恢复/双节点幂等/回执/更正/独立核对/留痕）
```
