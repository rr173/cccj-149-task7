"""多语言 / 别名归一化。

同一事件经医院、热线、企业三个渠道上报时，可能出现：
- 药名语言不同：布洛芬 / ibuprofen / ibuprofeno
- 反应语言不同：皮疹 / rash / sarpullido
- 严重性措辞不同：严重 / serious / grave，住院 / hospitalised
- 姓名别名：张三 / 老张 / 张三（身份证同音），Bob / Robert
所有归一化只产生 *用于比对* 的 canonical 值，原始 payload 永不改动。
"""
from __future__ import annotations

import re
import unicodedata
from datetime import date, datetime
from typing import Any

# ---------------------------------------------------------------------------
# 词典（实际项目中应来自术语库 MedDRA/WHODrug；此处内置可演示的多语言映射）
# ---------------------------------------------------------------------------
_DRUG_ALIASES: dict[str, str] = {
    # 布洛芬
    "布洛芬": "ibuprofen", "ibuprofen": "ibuprofen", "ibuprofeno": "ibuprofen",
    "brufen": "ibuprofen", "芬必得": "ibuprofen",
    # 阿莫西林
    "阿莫西林": "amoxicillin", "amoxicillin": "amoxicillin",
    "amoxicilline": "amoxicillin", "amoxil": "amoxicillin",
    # 华法林
    "华法林": "warfarin", "warfarin": "warfarin", "warfarina": "warfarin",
    "coumadin": "warfarin",
    # 阿托伐他汀
    "阿托伐他汀": "atorvastatin", "atorvastatin": "atorvastatin",
    "atorvastatina": "atorvastatin", "立普妥": "atorvastatin", "lipitor": "atorvastatin",
}

_REACTION_ALIASES: dict[str, str] = {
    "皮疹": "rash", "rash": "rash", "skin rash": "rash",
    "sarpullido": "rash", "erupcion cutanea": "rash", "exantema": "rash",
    "恶心": "nausea", "nausea": "nausea", "nauseas": "nausea",
    "呕吐": "vomiting", "vomiting": "vomiting", "vomito": "vomiting",
    "呕吐物": "vomiting",
    "肝损伤": "liver injury", "liver injury": "liver injury",
    "hepatic injury": "liver injury", "lesion hepatica": "liver injury",
    "drug-induced liver injury": "liver injury", "drug induced liver injury": "liver injury",
    "药物性肝损伤": "liver injury",
    "肝炎": "hepatitis", "hepatitis": "hepatitis", "hepatitis aguda": "hepatitis",
    "出血": "bleeding", "bleeding": "bleeding", "haemorrhage": "bleeding",
    "hemorrhage": "bleeding", "hemorragia": "bleeding", "胃肠道出血": "gi bleeding",
    "gi bleeding": "gi bleeding", "gastrointestinal bleeding": "gi bleeding",
    "hemorragia gastrointestinal": "gi bleeding",
    "过敏反应": "anaphylaxis", "anaphylaxis": "anaphylaxis",
    "anafilaxia": "anaphylaxis", "严重过敏": "anaphylaxis",
    "呼吸困难": "dyspnoea", "dyspnoea": "dyspnoea", "dyspnea": "dyspnoea",
    "dificultad para respirar": "dyspnoea",
}

_SERIOUSNESS_TERMS: dict[str, str] = {
    # -> SERIOUS / NON_SERIOUS
    "严重": "SERIOUS", "serious": "SERIOUS", "grave": "SERIOUS",
    "severo": "SERIOUS", "severely ill": "SERIOUS",
    "不严重": "NON_SERIOUS", "non-serious": "NON_SERIOUS", "nonserious": "NON_SERIOUS",
    "mild": "NON_SERIOUS", "轻微": "NON_SERIOUS", "leve": "NON_SERIOUS",
    "moderate": "NON_SERIOUS", "中等": "NON_SERIOUS",
}

_CAUSALITY_TERMS: dict[str, str] = {
    "肯定": "CERTAIN", "certain": "CERTAIN", "definite": "CERTAIN", "seguro": "CERTAIN",
    "很可能": "PROBABLE", "probable": "PROBABLE", "probablemente": "PROBABLE",
    "可能": "POSSIBLE", "possible": "POSSIBLE", "posible": "POSSIBLE",
    "不太可能": "UNLIKELY", "unlikely": "UNLIKELY", "improbable": "UNLIKELY",
    "无关": "UNRELATED", "unrelated": "UNRELATED", "no relacionado": "UNRELATED",
}

# 严重性升级标准（CIOMS/ICH E2D 口径）：满足任一即为严重
SERIOUSNESS_CRITERIA_KEYS = {
    "death": ("死亡", "death", "fallecimiento", "muerte"),
    "life_threatening": ("危及生命", "life threatening", "life-threatening",
                         "potentially fatal", "riesgo de muerte"),
    "hospitalisation": ("住院", "导致住院", "hospitalisation", "hospitalization",
                        "hospitalised", "hospitalized", "hospitalizacion",
                        "requiring hospital admission"),
    "disability": ("致残", "永久损伤", "disability", "incapacidad", "persistent disability"),
    "congenital_anomaly": ("先天异常", "congenital anomaly", "anomalia congenita"),
    "medically_important": ("医学重要", "medically important", "medicalmente importante"),
}

# 常见跨语言姓名别名（演示用；真实系统维护在身份主数据中）
_NAME_ALIASES: dict[str, str] = {}
for _group in [
    {"zhang san", "张三", "老张", "san zhang"},
    {"li si", "李四", "小李", "si li"},
    {"wang wu", "王五", "王老五"},
    {"bob", "robert", "bobby"},
    {"maria", "mary"},
    {"jose", "jose luis", "josé"},
]:
    _canon = sorted(_group, key=len)[0]
    for _n in _group:
        _NAME_ALIASES[_n] = _canon
del _group, _n, _canon


def clean_text(value: Any) -> str:
    """去重音/大小写/空白/标点的文本清洗（用于键匹配，不作为展示值）。"""
    if value is None:
        return ""
    s = str(value).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[\s()（）【】\[\],，。.\-/]+", " ", s).strip()
    return s


def canonical_drug(name: Any) -> str:
    key = clean_text(name)
    if not key:
        return ""
    if key in _DRUG_ALIASES:
        return _DRUG_ALIASES[key]
    # 单引号/连字符差异的二次尝试
    if key.replace(" ", "") in {clean_text(k).replace(" ", ""): v
                                for k, v in _DRUG_ALIASES.items()}:
        return {clean_text(k).replace(" ", ""): v for k, v in _DRUG_ALIASES.items()}[
            key.replace(" ", "")]
    return key


def canonical_reaction(name: Any) -> str:
    return _REACTION_ALIASES.get(clean_text(name), clean_text(name))


def canonical_name(name: Any) -> str:
    key = clean_text(name)
    if not key:
        return ""
    return _NAME_ALIASES.get(key, key)


def seriousness_terms(value: Any) -> set[str]:
    """把自由文本的严重性描述（可多值）解析为命中的标准标准集合。"""
    raw = str(value or "").lower()
    hits: set[str] = set()
    level = _SERIOUSNESS_TERMS.get(clean_text(value), "")
    if level == "SERIOUS":
        hits.add("serious_flag")
    for criterion, terms in SERIOUSNESS_CRITERIA_KEYS.items():
        if any(t in raw for t in terms):
            hits.add(criterion)
    return hits


def is_serious(payload: dict[str, Any]) -> bool:
    """根据严重等级词 + 任一严重性标准判定是否严重。"""
    if _SERIOUSNESS_TERMS.get(clean_text(payload.get("seriousness")), "") == "SERIOUS":
        return True
    if payload.get("serious") is True:
        return True
    raw = " ".join(str(payload.get(k, "")) for k in
                   ("seriousness", "seriousness_criteria", "outcome", "narrative")).lower()
    for terms in SERIOUSNESS_CRITERIA_KEYS.values():
        if any(t in raw for t in terms):
            return True
    if isinstance(payload.get("seriousness_criteria"), list):
        return True
    return False


def canonical_causality(value: Any) -> str:
    return _CAUSALITY_TERMS.get(clean_text(value), clean_text(value).upper() or "UNASSESSED")


CAUSALITY_RANK = {
    "CERTAIN": 5, "PROBABLE": 4, "POSSIBLE": 3,
    "UNLIKELY": 1, "UNRELATED": 0, "UNASSESSED": 2,
}


def parse_date(value: Any) -> date | None:
    """宽松解析日期（ISO / 中文日期 / 斜杠格式）。"""
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    s = str(value).strip()
    m = re.match(r"(\d{4})\D+(\d{1,2})\D+(\d{1,2})", s)
    if m:
        y, mo, d = (int(x) for x in m.groups())
        try:
            return date(y, mo, d)
        except ValueError:
            return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def canonical_drugs(payload: dict[str, Any]) -> list[str]:
    drugs = payload.get("drugs") or payload.get("drug") or []
    if isinstance(drugs, str):
        drugs = [drugs]
    out = []
    for d in drugs:
        if isinstance(d, dict):
            c = canonical_drug(d.get("name") or d.get("drug"))
        else:
            c = canonical_drug(d)
        if c:
            out.append(c)
    return sorted(set(out))


def canonical_reactions(payload: dict[str, Any]) -> list[str]:
    rx = payload.get("reactions") or payload.get("reaction") or []
    if isinstance(rx, str):
        rx = [rx]
    out = []
    for r in rx:
        if isinstance(r, dict):
            c = canonical_reaction(r.get("term") or r.get("reaction") or r.get("name"))
        else:
            c = canonical_reaction(r)
        if c:
            out.append(c)
    return sorted(set(out))


def drug_dates(payload: dict[str, Any]) -> dict[str, str]:
    """{canonical_drug: 'YYYY-MM-DD'} 用药开始时间（随访可能修正它）。"""
    out: dict[str, str] = {}
    drugs = payload.get("drugs") or payload.get("drug") or []
    if isinstance(drugs, str):
        drugs = [drugs]
    for d in drugs:
        if not isinstance(d, dict):
            continue
        c = canonical_drug(d.get("name") or d.get("drug"))
        dt = parse_date(d.get("start_date") or d.get("drug_start_date")
                        or d.get("administration_date"))
        if c and dt:
            out[c] = dt.isoformat()
    return out
