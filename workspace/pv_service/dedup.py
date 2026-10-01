"""疑似重复检测。

原则（对应需求）：
- 系统只给出 *可复核的疑似重复簇*：每条边都带分值与命中理由，安全专员可复核。
- 绝不自动合并；CONFIRMED/REJECTED 由人工动作驱动。
- 字段缺失 / 姓名别名 / 多语言差异在 profile 层归一化后再比对。
- 加权打分：强标识（身份证/病历号）直接判重；姓名+出生日期+性别+
  药品集合+反应集合+事件日期组合达阈值才成簇。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta

from .normalize import (
    canonical_drugs,
    canonical_name,
    canonical_reactions,
    drug_dates,
    parse_date,
)

# ---- 权重与阈值（可调参；实际部署可按渠道/药品类目调优）--------------------
W_STRONG_ID = 100
W_NAME = 30
W_DOB = 25
W_GENDER = 5
W_DRUG = 20
W_REACTION = 15
W_EVENT_DATE = 5
W_DRUG_START = 5

CLUSTER_THRESHOLD = 70          # >= 提议成簇
STRONG_IDENTIFIER_KEYS = ("patient_identifier", "national_id",
                          "medical_record_no", "mrn", "id_card")


@dataclass
class Profile:
    case_id: str
    name: str = ""
    dob: str = ""                 # ISO 字符串
    gender: str = ""
    strong_ids: set[str] = field(default_factory=set)
    drugs: set[str] = field(default_factory=set)
    reactions: set[str] = field(default_factory=set)
    event_date: str = ""
    drug_starts: dict[str, str] = field(default_factory=dict)


def build_profile(case_id: str, snap: dict) -> Profile:
    p = snap.get("patient") or {}
    strong = set()
    for k in STRONG_IDENTIFIER_KEYS:
        v = p.get(k) or snap.get(k)
        if v:
            strong.add(f"{k}={str(v).strip().lower()}")
    event_dt = parse_date(snap.get("event_date") or snap.get("onset_date"))
    return Profile(
        case_id=case_id,
        name=canonical_name(p.get("name") or snap.get("patient_name")),
        dob=(parse_date(p.get("dob") or p.get("birth_date") or snap.get("dob"))
             or _min_date()).isoformat() if (p.get("dob") or p.get("birth_date")
                                             or snap.get("dob")) else "",
        gender=clean_gender(p.get("gender") or snap.get("gender")),
        strong_ids=strong,
        drugs=set(canonical_drugs(snap)),
        reactions=set(canonical_reactions(snap)),
        event_date=event_dt.isoformat() if event_dt else "",
        drug_starts=drug_dates(snap),
    )


def _min_date():
    from datetime import date
    return date.min


def clean_gender(v) -> str:
    s = str(v or "").strip().lower()
    if s in ("m", "male", "男", "masculino"):
        return "m"
    if s in ("f", "female", "女", "femenino"):
        return "f"
    return s


def _jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    return inter / len(a | b) if inter else 0.0


def score_profiles(p1: Profile, p2: Profile) -> tuple[int, list[dict]]:
    """返回 0..100 的疑似重复分与逐项命中理由。"""
    reasons: list[dict] = []
    total = 0

    # 1) 强标识完全一致 —— 直接满分（同身份证/病历号）
    shared_ids = p1.strong_ids & p2.strong_ids
    if shared_ids:
        reasons.append({"factor": "strong_identifier", "weight": W_STRONG_ID,
                        "detail": sorted(shared_ids)})
        return W_STRONG_ID, reasons

    if p1.name and p2.name and p1.name == p2.name:
        total += W_NAME
        reasons.append({"factor": "name_alias", "weight": W_NAME,
                        "detail": p1.name})
    if p1.dob and p2.dob and p1.dob == p2.dob:
        total += W_DOB
        reasons.append({"factor": "date_of_birth", "weight": W_DOB,
                        "detail": p1.dob})
    if p1.gender and p2.gender and p1.gender == p2.gender:
        total += W_GENDER
        reasons.append({"factor": "gender", "weight": W_GENDER})

    drug_j = _jaccard(p1.drugs, p2.drugs)
    if drug_j:
        w = round(W_DRUG * drug_j)
        total += w
        reasons.append({"factor": "drug_overlap", "weight": w,
                        "detail": {"shared": sorted(p1.drugs & p2.drugs),
                                   "jaccard": round(drug_j, 2)}})

    rx_j = _jaccard(p1.reactions, p2.reactions)
    if rx_j:
        w = round(W_REACTION * rx_j)
        total += w
        reasons.append({"factor": "reaction_overlap", "weight": w,
                        "detail": {"shared": sorted(p1.reactions & p2.reactions),
                                   "jaccard": round(rx_j, 2)}})

    if p1.event_date and p2.event_date:
        d1, d2 = parse_date(p1.event_date), parse_date(p2.event_date)
        if d1 and d2 and abs(d1 - d2) <= timedelta(days=7):
            total += W_EVENT_DATE
            reasons.append({"factor": "event_date_close", "weight": W_EVENT_DATE,
                            "detail": [p1.event_date, p2.event_date]})

    # 用药开始日期一致（同一药品）
    shared_drug_starts = sorted(
        (d, dt) for d, dt in p1.drug_starts.items()
        if d in p2.drug_starts and p2.drug_starts[d] == dt)
    if shared_drug_starts:
        total += W_DRUG_START
        reasons.append({"factor": "drug_start_date", "weight": W_DRUG_START,
                        "detail": shared_drug_starts})

    return min(total, 100), reasons


class UnionFind:
    def __init__(self, items):
        self.parent = {x: x for x in items}

    def find(self, x):
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra
