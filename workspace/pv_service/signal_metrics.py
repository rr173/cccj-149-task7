"""不相称测定指标纯函数：四格表、PRR/ROR 及 95% 置信区间、卡方、信号判定。

四格表（针对一个药品组 D 与一个事件组 E）：

    a = D 且 E 的病例数
    b = D 但非 E
    c = 非 D 但 E
    d = 非 D 且非 E
    n = a+b+c+d（观察期内全部入选病例）

- PRR（比例报告比）= (a/(a+b)) / (c/(c+d))
- ROR（报告比值比）= (a/b) / (c/d) = a*d/(b*c)
- 两者 95%CI 用经典对数正态近似（Woolf 法）：
    ln(PRR) ± 1.96 * sqrt(1/a - 1/(a+b) + 1/c - 1/(c+d))
    ln(ROR) ± 1.96 * sqrt(1/a + 1/b + 1/c + 1/d)
- 卡方（未做 Yates 校正的标准 Pearson，与常见 PV 口径一致）。

零格处理：分母为 0 或任一必要格为 0 时，该指标不可估计（None），不做
静默的连续性修正，以免不同实现给出不同“信号”；分析口径需要修正时在
监测定义里显式开启 continuity_correction（全体一致加 0.5），结果可复现。
"""
from __future__ import annotations

import math
from typing import Optional

Z95 = 1.959963984540054  # 标准正态 97.5% 分位（固定常量，保证跨平台一致）


def _ratio_ci(estimate: Optional[float], se_log: Optional[float]):
    if estimate is None or se_log is None or not math.isfinite(se_log) or se_log <= 0:
        return estimate, None, None
    log_e = math.log(estimate)
    low = math.exp(log_e - Z95 * se_log)
    high = math.exp(log_e + Z95 * se_log)
    return estimate, low, high


def compute_metrics(a: int, b: int, c: int, d: int,
                    *, continuity_correction: bool = False) -> dict:
    """返回四格计数与全部指标。零格 -> 相应指标为 None（不可估计）。"""
    a, b, c, d = int(a), int(b), int(c), int(d)
    n = a + b + c + d

    if continuity_correction:
        # Haldane-Anscombe 修正：所有格 +0.5（仅在定义显式要求时）。
        a2, b2, c2, d2 = a + 0.5, b + 0.5, c + 0.5, d + 0.5
    else:
        a2, b2, c2, d2 = float(a), float(b), float(c), float(d)

    # ---- PRR ----
    exposed = a2 + b2          # D 总数
    nonexposed = c2 + d2       # 非 D 总数
    prr = prr_se = None
    if exposed > 0 and nonexposed > 0:
        p1 = a2 / exposed
        p0 = c2 / nonexposed
        if p1 > 0 and p0 > 0:
            prr = p1 / p0
            var = 0.0
            # 1/a - 1/(a+b) + 1/c - 1/(c+d)，负值由四格一致性保证 >=0
            if a2 > 0 and c2 > 0:
                var = 1.0 / a2 - 1.0 / exposed + 1.0 / c2 - 1.0 / nonexposed
            prr_se = math.sqrt(var) if var > 0 else None
    prr, prr_lo, prr_hi = _ratio_ci(prr, prr_se)

    # ---- ROR ----
    ror = ror_se = None
    if b2 > 0 and c2 > 0 and a2 > 0 and d2 > 0:
        ror = (a2 * d2) / (b2 * c2)
        ror_se = math.sqrt(1.0 / a2 + 1.0 / b2 + 1.0 / c2 + 1.0 / d2)
    ror, ror_lo, ror_hi = _ratio_ci(ror, ror_se)

    # ---- Pearson chi-square（1 df，无 Yates 校正）----
    chi = None
    row1, row0 = a2 + b2, c2 + d2
    col1, col0 = a2 + c2, b2 + d2
    if n > 0 and row1 and row0 and col1 and col0:
        chi = 0.0
        for obs, rr, cc in ((a2, row1, col1), (b2, row1, col0),
                            (c2, row0, col1), (d2, row0, col0)):
            exp = rr * cc / n
            if exp > 0:
                chi += (obs - exp) ** 2 / exp

    return {
        "a": a, "b": b, "c": c, "d": d, "n": n,
        "prr": _r(prr), "prr_ci_low": _r(prr_lo), "prr_ci_high": _r(prr_hi),
        "ror": _r(ror), "ror_ci_low": _r(ror_lo), "ror_ci_high": _r(ror_hi),
        "chi_square": _r(chi),
    }


def _r(x, ndigits: int = 6):
    if x is None or not math.isfinite(x):
        return None
    return round(float(x), ndigits)


def evaluate(metrics: dict, criteria: dict) -> dict:
    """按分析师阈值判定。criteria 键：

    min_n / min_a            最小样本量（总 n、目标格 a 的下限）
    prr_min                  PRR 点估计下限
    prr_ci_lower_than        要求 PRR 95%CI 下限 > 该值（典型 1.0）
    ror_min                  ROR 点估计下限
    ror_ci_lower_than        要求 ROR 95%CI 下限 > 该值（典型 1.0）
    chi_square_min           可选卡方下限（典型 4.0，1df）
    require                  'ALL'（默认）或 'ANY'：各指标门槛的合取/析取

    样本不足固定判 INSUFFICIENT；指标不可估计（None）的门槛按未通过处理，
    但 require=ANY 下其它已满足的指标仍可通过。
    """
    min_n = int(criteria.get("min_n", 0))
    min_a = int(criteria.get("min_a", 0))
    if metrics["n"] < min_n or metrics["a"] < min_a:
        return {"determination": "INSUFFICIENT",
                "reasons": [f"n={metrics['n']}<min_n={min_n}"]
                            if metrics["n"] < min_n else
                            [f"a={metrics['a']}<min_a={min_a}"]}

    checks: list[tuple[str, bool, str]] = []

    def add(name, ok, detail):
        checks.append((name, bool(ok), detail))

    if criteria.get("prr_min") is not None:
        add("prr_min", metrics["prr"] is not None and metrics["prr"] >= criteria["prr_min"],
            f"prr={metrics['prr']}>={criteria['prr_min']}")
    if criteria.get("prr_ci_lower_than") is not None:
        thr = criteria["prr_ci_lower_than"]
        add("prr_ci_low", metrics["prr_ci_low"] is not None and metrics["prr_ci_low"] > thr,
            f"prr_ci_low={metrics['prr_ci_low']}>{thr}")
    if criteria.get("ror_min") is not None:
        add("ror_min", metrics["ror"] is not None and metrics["ror"] >= criteria["ror_min"],
            f"ror={metrics['ror']}>={criteria['ror_min']}")
    if criteria.get("ror_ci_lower_than") is not None:
        thr = criteria["ror_ci_lower_than"]
        add("ror_ci_low", metrics["ror_ci_low"] is not None and metrics["ror_ci_low"] > thr,
            f"ror_ci_low={metrics['ror_ci_low']}>{thr}")
    if criteria.get("chi_square_min") is not None:
        add("chi_square_min",
            metrics["chi_square"] is not None and metrics["chi_square"] >= criteria["chi_square_min"],
            f"chi_square={metrics['chi_square']}>={criteria['chi_square_min']}")

    if not checks:
        # 无任何指标门槛时，达到样本量即视为 NO_SIGNAL（没有阳性判据）
        return {"determination": "NO_SIGNAL", "reasons": ["未配置任何指标阈值"]}

    require = str(criteria.get("require", "ALL")).upper()
    passed = [name for name, ok, _ in checks if ok]
    reasons = [detail for _, ok, detail in checks if not ok]
    if require == "ANY":
        signal = len(passed) > 0
    else:
        signal = len(passed) == len(checks)
    return {
        "determination": "SIGNAL" if signal else "NO_SIGNAL",
        "reasons": ([] if signal else reasons),
        "passed_checks": passed,
        "all_checks": [{"name": n, "passed": ok, "detail": d} for n, ok, d in checks],
    }
