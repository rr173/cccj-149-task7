"""监管报送规则引擎（可插拔）。

规则口径（ICH E2D/E2A 通用模型，天数可配置）：
- R1 严重 + 非预期（unlisted）可疑药品反应 -> 加急 15 个日历日
- R2 严重 + 已预期（listed）              -> 30 日
- R3 非严重自发报告                       -> 90 日常规报送
随访材料产生 *对评价有实质影响的新信息*（用药时间、严重性、因果性、
转归、预期性变化）时需要 follow-up：升级为严重且非预期的按 15 日加急，
否则按当前分级走 30/90 日；无实质新信息则不重复报送。

每条 Decision 带 rule_id 与 rationale，供安全专员复核。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Callable, Protocol

from .normalize import CAUSALITY_RANK, canonical_causality

SUSPECT_CAUSALITY_MIN_RANK = 3   # POSSIBLE 及以上视为可疑


@dataclass
class RuleContext:
    case_id: str
    snapshot: dict
    awareness_date: date          # 获知日期（随访为材料到达日期）
    is_followup: bool
    prior_snapshot: dict | None = None
    is_listed_fn: Callable[[str, str], bool] | None = None
    material_id: str | None = None


@dataclass
class Decision:
    should_report: bool
    expedited: bool
    days_due: int
    due_date: str
    rule_id: str
    rationale: str
    listedness: list[dict] = field(default_factory=list)
    significant_changes: list[str] = field(default_factory=list)
    report_type: str = "INITIAL"

    def as_dict(self) -> dict:
        return {
            "should_report": self.should_report,
            "expedited": self.expedited,
            "days_due": self.days_due,
            "due_date": self.due_date,
            "rule_id": self.rule_id,
            "rationale": self.rationale,
            "listedness": self.listedness,
            "significant_changes": self.significant_changes,
            "report_type": self.report_type,
        }


class ReportRule(Protocol):
    rule_id: str
    def matches(self, ctx: RuleContext) -> bool: ...
    def decide(self, ctx: RuleContext, days: int) -> Decision: ...


def _add_days(d: date, days: int) -> str:
    return (d + timedelta(days=days)).isoformat()


def _suspect_drug_reaction_pairs(snap: dict) -> list[tuple[str, str]]:
    """根据因果评价挑出可疑 (药品, 反应) 对。"""
    drugs = snap.get("drugs") or []
    reactions = snap.get("reactions") or []
    if isinstance(drugs, str):
        drugs = [drugs]
    if isinstance(reactions, str):
        reactions = [reactions]
    drug_names, suspect_drugs = [], set()
    for d in drugs:
        if isinstance(d, dict):
            name = d.get("name") or d.get("drug") or ""
            if CAUSALITY_RANK.get(canonical_causality(d.get("causality")), 0) \
                    >= SUSPECT_CAUSALITY_MIN_RANK or d.get("suspect", False):
                suspect_drugs.add(name)
            drug_names.append(name)
        else:
            drug_names.append(d)
    # 病例级 causality 存在且达可疑阈值时，所有药都视为可疑
    case_rank = CAUSALITY_RANK.get(canonical_causality(snap.get("causality")), 0)
    if case_rank >= SUSPECT_CAUSALITY_MIN_RANK:
        suspect_drugs.update(drug_names)
    rx_terms = []
    for r in reactions:
        rx_terms.append(r.get("term") or r.get("reaction") or r.get("name")
                        if isinstance(r, dict) else r)
    return [(d, r) for d in drug_names for r in rx_terms
            if (not suspect_drugs or d in suspect_drugs)]


def assess_listedness(ctx: RuleContext) -> list[dict]:
    from .normalize import canonical_drug, canonical_reaction
    pairs = _suspect_drug_reaction_pairs(ctx.snapshot)
    out = []
    for d, r in pairs:
        cd, cr = canonical_drug(d), canonical_reaction(r)
        listed = bool(ctx.is_listed_fn and ctx.is_listed_fn(cd, cr))
        out.append({"drug": cd, "reaction": cr, "listed": listed})
    return out


def is_serious(snap: dict) -> bool:
    from .normalize import is_serious
    return is_serious(snap)


def significant_changes(old: dict | None, new: dict) -> list[str]:
    """识别随访中影响获益-风险评价的实质性变化（对应需求所列三类修正 + 转归）。"""
    if not old:
        return []
    changes: list[str] = []
    if is_serious(old) != is_serious(new):
        changes.append(f"seriousness:{is_serious(old)}->{is_serious(new)}")
    c_old = canonical_causality(old.get("causality"))
    c_new = canonical_causality(new.get("causality"))
    if c_old != c_new:
        changes.append(f"causality:{c_old}->{c_new}")
    # 用药时间修正
    from .normalize import drug_dates
    if drug_dates(old) != drug_dates(new):
        changes.append("drug_timing_corrected")
    o_old = str(old.get("outcome") or "").strip().lower()
    o_new = str(new.get("outcome") or "").strip().lower()
    if o_old != o_new and o_new:
        changes.append(f"outcome:{o_old or 'unknown'}->{o_new}")
    # 新增可疑药品 / 新反应
    from .normalize import canonical_drugs, canonical_reactions
    new_drugs = set(canonical_drugs(new)) - set(canonical_drugs(old))
    new_rx = set(canonical_reactions(new)) - set(canonical_reactions(old))
    if new_drugs:
        changes.append("new_suspect_drug:" + ",".join(sorted(new_drugs)))
    if new_rx:
        changes.append("new_reaction:" + ",".join(sorted(new_rx)))
    # 预期性变化
    if not changes:
        return changes
    return changes


class SeriousUnlistedRule:
    rule_id = "R1_SERIOUS_UNLISTED_15D"

    def matches(self, ctx: RuleContext) -> bool:
        if not is_serious(ctx.snapshot):
            return False
        pairs = assess_listedness(ctx)
        return bool(pairs) and not all(p["listed"] for p in pairs)

    def decide(self, ctx: RuleContext, days=15) -> Decision:
        pairs = assess_listedness(ctx)
        unlisted = [f'{p["drug"]}/{p["reaction"]}' for p in pairs if not p["listed"]]
        return Decision(
            should_report=True, expedited=True, days_due=days,
            due_date=_add_days(ctx.awareness_date, days),
            rule_id=self.rule_id,
            rationale=f"严重且非预期可疑反应（{'; '.join(unlisted) or '未在参考表命中'}），"
                      f"按 {days} 日加急报送",
            listedness=pairs)


class SeriousListedRule:
    rule_id = "R2_SERIOUS_LISTED_30D"

    def matches(self, ctx: RuleContext) -> bool:
        return is_serious(ctx.snapshot)

    def decide(self, ctx: RuleContext, days=30) -> Decision:
        pairs = assess_listedness(ctx)
        return Decision(
            should_report=True, expedited=False, days_due=days,
            due_date=_add_days(ctx.awareness_date, days),
            rule_id=self.rule_id,
            rationale=f"严重但已预期（listed），按 {days} 日报送",
            listedness=pairs)


class NonSeriousRule:
    rule_id = "R3_NONSERIOUS_90D"

    def matches(self, ctx: RuleContext) -> bool:
        return True   # 兜底

    def decide(self, ctx: RuleContext, days=90) -> Decision:
        return Decision(
            should_report=True, expedited=False, days_due=days,
            due_date=_add_days(ctx.awareness_date, days),
            rule_id=self.rule_id,
            rationale=f"非严重自发报告，按 {days} 日常规报送",
            listedness=assess_listedness(ctx))


class RuleEngine:
    """按顺序求值的规则链；随访在其外层决定是否需要 follow-up。"""

    def __init__(self, rules: list[ReportRule] | None = None,
                 serious_days: int = 15, listed_days: int = 30,
                 routine_days: int = 90):
        self.rules = rules or [SeriousUnlistedRule(), SeriousListedRule(), NonSeriousRule()]
        self._days = {"R1": serious_days, "R2": listed_days, "R3": routine_days}

    def evaluate(self, ctx: RuleContext) -> Decision:
        # 随访：仅当存在实质性新信息才发 follow-up
        changes = significant_changes(ctx.prior_snapshot, ctx.snapshot) if ctx.is_followup else []
        if ctx.is_followup and not changes:
            last = assess_listedness(ctx)
            return Decision(False, False, 0, "", "R0_FOLLOWUP_NO_NEW_INFO",
                            "随访材料未改变用药时间/严重性/因果性/转归评价，无需重复报送",
                            listedness=last, significant_changes=[],
                            report_type="FOLLOWUP")
        for i, rule in enumerate(self.rules):
            if rule.matches(ctx):
                days = list(self._days.values())[i]
                d = rule.decide(ctx, days)
                if ctx.is_followup:
                    d.report_type = "FOLLOWUP"
                    d.significant_changes = changes
                    d.rationale = "随访含实质新信息（" + ", ".join(changes) + "）；" + d.rationale
                return d
        raise RuntimeError("rule chain exhausted")
