"""
Research results computation and Plotly chart generation.

Computes violations, dark patterns, cookie/purpose predictions, and generates
interactive charts using the same methodology as the original paper's
crawl_summary.py (same thresholds, same filters, same logic).
"""

import io
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import plotly.graph_objects as go
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger("dashboard.results")

COOKIEBLOCK_THRESHOLD = 2

SUPPORTED_LANGUAGES = [
    "da", "de", "en", "es", "fi", "fr", "it", "nl", "pl", "pt", "sv",
]

VIOLATION_LABELS = {
    "missing_notice": "Missing notice",
    "missing_reject": "No reject button",
    "AA_cookies_detected_after_reject": "Ignored reject",
    "AA_cookies_detected_after_close": "Implicit consent after close",
    "AA_cookies_detected_after_save": "Ignored save / Prefilled purposes",
    "AA_cookies_detected_prior_to_interaction": "Implicit consent prior to interaction",
    "undeclared_purposes": "Undeclared purposes",
}

DARK_PATTERN_LABELS = {
    "forced_action": "Forced action",
    "interface_interference": "Interface interference",
}

VIOLATION_COLOR = "#dc2626"
DARK_PATTERN_COLOR = "#ea580c"

_CHART_FONT = dict(color="#e4e6ed", size=12)
_CHART_BG = "rgba(0,0,0,0)"


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

def fetch_experiment_data(engine: Engine, experiment_id: str) -> Dict[str, Any]:
    """Fetch all data needed for results, matching crawl_summary.py methodology."""

    websites = pd.read_sql(
        text("SELECT * FROM websites WHERE experiment_id = :eid"),
        engine, params={"eid": experiment_id},
    )
    if websites.empty:
        return {"empty": True, "total": 0}

    total = len(websites)
    supported = websites[websites["language"].isin(SUPPORTED_LANGUAGES)]
    unsupported = websites[
        (~websites["language"].isna()) & (~websites["language"].isin(SUPPORTED_LANGUAGES))
    ]

    error_ids = pd.read_sql(
        text("""
            SELECT DISTINCT e.website_id
            FROM errors e JOIN websites w ON e.website_id = w.id
            WHERE w.experiment_id = :eid
        """),
        engine, params={"eid": experiment_id},
    )["website_id"].tolist()

    crawl_results = pd.read_sql(
        text("""
            SELECT w.id, w.name, w.url, w.language, w.crux_rank, w.crux_country,
                   w.success, w.cmp,
                   cr.cookie_notice_detected,
                   cr.accept_button_detected, cr.reject_button_detected,
                   cr.close_button_detected, cr.save_button_detected,
                   cr.accept_button_detected_without_reject_button,
                   cr.cmp_detected,
                   cr.mentions_legitimate_interest_in_initial_text,
                   cr.mentions_legitimate_interest,
                   cr.forced_action_detected, cr.interface_interference_detected,
                   cr.tracking_detected,
                   cr.tracking_detected_after_reject,
                   cr.tracking_detected_after_close,
                   cr.tracking_detected_after_save,
                   cr.tracking_detected_prior_to_interaction,
                   cr.tracking_purposes_detected_in_initial_text,
                   cr.tracking_purposes_detected,
                   cr.website_id
            FROM websites w
            JOIN crawl_results cr ON cr.website_id = w.id
            WHERE w.experiment_id = :eid
        """),
        engine, params={"eid": experiment_id},
    )

    crawl_results = crawl_results[crawl_results["language"].isin(SUPPORTED_LANGUAGES)]
    crawl_results = crawl_results[~crawl_results["website_id"].isin(error_ids)]

    try:
        cookie_counts = pd.read_sql(
            text("""
                SELECT cp.classification, COUNT(*) AS cnt
                FROM cookies_with_predictions cp
                JOIN websites w ON cp.website_id = w.id
                WHERE w.experiment_id = :eid
                GROUP BY cp.classification
            """),
            engine, params={"eid": experiment_id},
        )
        cookie_agg = dict(zip(cookie_counts["classification"], cookie_counts["cnt"]))
    except Exception:
        cookie_agg = {}
        logger.debug("cookies_with_predictions table not available yet")

    try:
        purpose_counts = pd.read_sql(
            text("""
                SELECT pp.purpose_detected, COUNT(*) AS cnt
                FROM purpose_predictions pp
                JOIN cb_text t ON pp.sentence_id = t.id
                JOIN websites w ON t.website_id = w.id
                WHERE w.experiment_id = :eid
                GROUP BY pp.purpose_detected
            """),
            engine, params={"eid": experiment_id},
        )
        purpose_agg = dict(zip(purpose_counts["purpose_detected"], purpose_counts["cnt"]))
    except Exception:
        purpose_agg = {}
        logger.debug("purpose_predictions table not available yet")

    return {
        "empty": False,
        "total": total,
        "supported_count": len(supported),
        "unsupported_count": len(unsupported),
        "no_language_count": int(websites["language"].isna().sum()),
        "analyzed_count": len(crawl_results),
        "error_count": len(error_ids),
        "crawl_results": crawl_results,
        "cookie_agg": cookie_agg,
        "purpose_agg": purpose_agg,
    }


# ---------------------------------------------------------------------------
# Violation / dark-pattern computation  (mirrors crawl_summary.py exactly)
# ---------------------------------------------------------------------------

def _viol(df: pd.DataFrame, base: int) -> Tuple[int, int, float]:
    count = len(df)
    pct = round(count / base * 100, 1) if base > 0 else 0.0
    return count, base, pct


def compute_violations(cr: pd.DataFrame) -> Dict[str, Tuple[int, int, float]]:
    """Same logic as crawl_summary.get_violations(), returns raw numbers."""
    if cr.empty:
        return {}

    T = COOKIEBLOCK_THRESHOLD
    r: Dict[str, Tuple[int, int, float]] = {}

    r["missing_reject"] = _viol(
        cr[(cr["accept_button_detected_without_reject_button"] == True)
           & (cr["tracking_detected"] >= T)],
        len(cr[cr["accept_button_detected"] >= 1]),
    )

    for opt in ["reject", "close", "save"]:
        track = f"tracking_detected_after_{opt}"
        base_col = f"{opt}_button_detected"
        r[track.replace("tracking", "AA_cookies")] = _viol(
            cr[cr[track] >= T],
            len(cr[cr[base_col] >= 1]),
        )

    r["AA_cookies_detected_prior_to_interaction"] = _viol(
        cr[cr["tracking_detected_prior_to_interaction"] >= T],
        len(cr[cr["cookie_notice_detected"] >= 1]),
    )

    # NOTE: original uses  >  (strictly greater) for undeclared_purposes
    r["undeclared_purposes"] = _viol(
        cr[(cr["tracking_detected"] > T)
           & (cr["tracking_purposes_detected_in_initial_text"] == 0)],
        len(cr[cr["cookie_notice_detected"] >= 1]),
    )

    r["missing_notice"] = _viol(
        cr[(cr["cookie_notice_detected"] == False)
           & (cr["tracking_detected"] >= T)],
        len(cr[cr["tracking_detected"] >= T]),
    )

    return r


def compute_dark_patterns(cr: pd.DataFrame) -> Dict[str, Tuple[int, int, float]]:
    """Same logic as crawl_summary.get_dark_patterns(), returns raw numbers."""
    if cr.empty:
        return {}

    r: Dict[str, Tuple[int, int, float]] = {}

    r["forced_action"] = _viol(
        cr[cr["forced_action_detected"] >= 1],
        len(cr[cr["cookie_notice_detected"] >= 1]),
    )

    r["interface_interference"] = _viol(
        cr[cr["interface_interference_detected"] >= 1],
        len(cr[
            (cr["accept_button_detected"] == 1)
            & ((cr["reject_button_detected"] == 1)
               | (cr["close_button_detected"] == 1)
               | (cr["save_button_detected"] == 1))
        ]),
    )

    return r


def _count_websites_with_any_violation(
    cr: pd.DataFrame, *, include_missing_notice: bool = True,
) -> int:
    """Count unique websites that have at least one violation.

    In crawl_summary.py, ``violations_with_cookie_notices`` is computed
    **before** ``missing_notice`` is added to the set, so its numerator
    excludes ``missing_notice``.  Pass ``include_missing_notice=False``
    to reproduce that exact count.
    """
    if cr.empty:
        return 0
    T = COOKIEBLOCK_THRESHOLD
    masks = [
        (cr["accept_button_detected_without_reject_button"] == True) & (cr["tracking_detected"] >= T),
        cr["tracking_detected_after_reject"] >= T,
        cr["tracking_detected_after_close"] >= T,
        cr["tracking_detected_after_save"] >= T,
        cr["tracking_detected_prior_to_interaction"] >= T,
        (cr["tracking_detected"] > T) & (cr["tracking_purposes_detected_in_initial_text"] == 0),
    ]
    if include_missing_notice:
        masks.append(
            (cr["cookie_notice_detected"] == False) & (cr["tracking_detected"] >= T)
        )
    names: set = set()
    for m in masks:
        names.update(cr.loc[m, "name"].tolist())
    return len(names)


# ---------------------------------------------------------------------------
# Chart builders
# ---------------------------------------------------------------------------

def _chart_html(fig: go.Figure) -> str:
    return fig.to_html(
        full_html=False,
        include_plotlyjs=False,
        config={"responsive": True},
    )


def build_violations_chart(
    violations: Dict[str, Tuple[int, int, float]],
    dark_patterns: Dict[str, Tuple[int, int, float]],
) -> Optional[str]:
    if not violations and not dark_patterns:
        return None

    rows = [
        ("interface_interference", dark_patterns, DARK_PATTERN_COLOR),
        ("forced_action", dark_patterns, DARK_PATTERN_COLOR),
        ("undeclared_purposes", violations, VIOLATION_COLOR),
        ("AA_cookies_detected_prior_to_interaction", violations, VIOLATION_COLOR),
        ("AA_cookies_detected_after_close", violations, VIOLATION_COLOR),
        ("AA_cookies_detected_after_save", violations, VIOLATION_COLOR),
        ("AA_cookies_detected_after_reject", violations, VIOLATION_COLOR),
        ("missing_reject", violations, VIOLATION_COLOR),
        ("missing_notice", violations, VIOLATION_COLOR),
    ]

    all_labels = {**VIOLATION_LABELS, **DARK_PATTERN_LABELS}
    y, x, colors, texts = [], [], [], []
    for key, src, color in rows:
        d = src.get(key)
        if d is None:
            continue
        count, base, pct = d
        y.append(all_labels.get(key, key))
        x.append(pct)
        colors.append(color)
        texts.append(f"{count}/{base} ({pct}%)" if base > 0 else "N/A")

    if not y:
        return None

    fig = go.Figure(go.Bar(
        y=y, x=x, orientation="h",
        text=texts, textposition="outside", textfont=dict(size=11),
        marker_color=colors,
        hovertemplate="%{y}: %{text}<extra></extra>",
    ))
    fig.update_layout(
        paper_bgcolor=_CHART_BG, plot_bgcolor=_CHART_BG,
        font=_CHART_FONT,
        margin=dict(l=260, r=80, t=10, b=40),
        height=max(300, len(y) * 42 + 60),
        xaxis=dict(title="Percentage of applicable websites", range=[0, 110],
                    gridcolor="rgba(255,255,255,0.1)"),
        yaxis=dict(autorange="reversed"),
        showlegend=False,
    )
    return _chart_html(fig)


def build_rank_chart(cr: pd.DataFrame) -> Optional[str]:
    if cr.empty or cr["crux_rank"].dropna().nunique() == 0:
        return None

    ranks = sorted(cr["crux_rank"].dropna().unique().astype(int))
    palette = ["#6366f1", "#22c55e", "#f59e0b", "#ef4444", "#ec4899"]

    all_labels = {**VIOLATION_LABELS, **DARK_PATTERN_LABELS}
    keys = list(VIOLATION_LABELS.keys()) + list(DARK_PATTERN_LABELS.keys())
    labels = [all_labels[k] for k in keys]

    fig = go.Figure()
    for i, rank in enumerate(ranks):
        subset = cr[cr["crux_rank"] == rank]
        v = compute_violations(subset)
        dp = compute_dark_patterns(subset)
        merged = {**v, **dp}
        pcts = [merged.get(k, (0, 0, 0))[2] for k in keys]
        fig.add_trace(go.Bar(
            y=labels, x=pcts, name=f"Rank {rank:,}", orientation="h",
            marker_color=palette[i % len(palette)],
        ))

    fig.update_layout(
        paper_bgcolor=_CHART_BG, plot_bgcolor=_CHART_BG,
        font=_CHART_FONT,
        margin=dict(l=260, r=40, t=10, b=40),
        height=max(280, len(keys) * 55 + 80),
        barmode="group",
        xaxis=dict(title="Percentage", range=[0, 110], gridcolor="rgba(255,255,255,0.1)"),
        yaxis=dict(autorange="reversed"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return _chart_html(fig)


def build_cookies_chart(cookie_agg: Dict) -> Optional[str]:
    if not cookie_agg:
        return None

    necessary = int(cookie_agg.get(0, 0))
    analytics = int(cookie_agg.get(1, 0))
    if necessary + analytics == 0:
        return None

    fig = go.Figure(go.Pie(
        labels=["Necessary / Functional", "Analytics / Advertising"],
        values=[necessary, analytics],
        marker=dict(colors=["#22c55e", "#ef4444"]),
        hole=0.4, textinfo="label+percent+value", textfont=dict(size=12),
        hovertemplate="%{label}: %{value} cookies (%{percent})<extra></extra>",
    ))
    fig.update_layout(
        paper_bgcolor=_CHART_BG, plot_bgcolor=_CHART_BG,
        font=_CHART_FONT,
        margin=dict(l=20, r=20, t=10, b=20),
        height=340,
        legend=dict(orientation="h", yanchor="bottom", y=-0.15, xanchor="center", x=0.5),
    )
    return _chart_html(fig)


def build_purposes_chart(purpose_agg: Dict) -> Optional[str]:
    if not purpose_agg:
        return None

    aa = int(purpose_agg.get(1, 0))
    other = int(purpose_agg.get(0, 0))
    if aa + other == 0:
        return None

    fig = go.Figure(go.Bar(
        x=["AA Purpose Declared", "Other / No AA Purpose"],
        y=[aa, other],
        marker_color=["#f59e0b", "#6366f1"],
        text=[aa, other], textposition="outside", textfont=dict(size=13),
    ))
    fig.update_layout(
        paper_bgcolor=_CHART_BG, plot_bgcolor=_CHART_BG,
        font=_CHART_FONT,
        margin=dict(l=40, r=40, t=10, b=40),
        height=300,
        yaxis=dict(title="Number of sentences", gridcolor="rgba(255,255,255,0.1)"),
        showlegend=False,
    )
    return _chart_html(fig)


# ---------------------------------------------------------------------------
# CMP-based analysis  (mirrors crawl_summary.py lines 250-302)
# ---------------------------------------------------------------------------

def _parse_cmp(x):
    if isinstance(x, str):
        try:
            x = json.loads(x)
        except (json.JSONDecodeError, TypeError):
            return [], False
    if not isinstance(x, dict):
        return [], False
    consentomatic = x.get("consentomatic") or []
    tcfapi = x.get("tcfapi")
    uses_tcf = isinstance(tcfapi, dict) and "cmpId" in tcfapi
    return consentomatic, uses_tcf


def _filter_cmps(cr: pd.DataFrame, cmps: List[str]) -> pd.DataFrame:
    return cr[cr["_consentomatic"].apply(lambda x: bool(set(x) & set(cmps)))]


def _fmt(v: Optional[Tuple[int, int, float]]) -> str:
    if v is None:
        return "0/0"
    count, base, pct = v
    return f"{count}/{base} ({pct}%)" if base > 0 else f"{count}/0"


def compute_cmp_analysis(cr: pd.DataFrame) -> List[Dict[str, str]]:
    """CMP-based analysis matching crawl_summary.py lines 250-302."""
    if cr.empty or "cmp" not in cr.columns:
        return []

    cr = cr.copy()
    parsed = cr["cmp"].apply(_parse_cmp)
    cr["_consentomatic"] = parsed.apply(lambda x: x[0])
    cr["_uses_tcf_iab"] = parsed.apply(lambda x: x[1])

    rows: List[Dict[str, str]] = []

    tcf = cr[cr["_uses_tcf_iab"]]
    tcf_v = compute_violations(tcf) if not tcf.empty else {}
    rows.append({
        "group": "Websites using TCF IAB",
        "count": len(tcf),
        "metric": "Ignored reject",
        "value": _fmt(tcf_v.get("AA_cookies_detected_after_reject")),
    })
    rows.append({
        "group": "Websites using TCF IAB",
        "count": len(tcf),
        "metric": "Implicit consent prior to interaction",
        "value": _fmt(tcf_v.get("AA_cookies_detected_prior_to_interaction")),
    })

    bollinger = _filter_cmps(cr, ["onetrust", "cookiebot", "termly"])
    b_v = compute_violations(bollinger) if not bollinger.empty else {}
    rows.append({
        "group": "Bollinger et al. (Cookiebot + OneTrust + Termly)",
        "count": len(bollinger),
        "metric": "Implicit consent prior to interaction",
        "value": _fmt(b_v.get("AA_cookies_detected_prior_to_interaction")),
    })

    cookiebot_only = _filter_cmps(cr, ["cookiebot"])
    cb_v = compute_violations(cookiebot_only) if not cookiebot_only.empty else {}
    rows.append({
        "group": "Bollinger et al. (Cookiebot only)",
        "count": len(cookiebot_only),
        "metric": "Ignored reject",
        "value": _fmt(cb_v.get("AA_cookies_detected_after_reject")),
    })

    nouwens = _filter_cmps(cr, ["onetrust", "cookiebot", "crownpeak", "trustarc", "quantcast"])
    n_v = compute_violations(nouwens) if not nouwens.empty else {}
    n_dp = compute_dark_patterns(nouwens) if not nouwens.empty else {}
    rows.append({
        "group": "Nouwens et al. (Cookiebot + OneTrust + Crownpeak + Quantcast + Trustarc)",
        "count": len(nouwens),
        "metric": "Implicit consent prior to interaction",
        "value": _fmt(n_v.get("AA_cookies_detected_prior_to_interaction")),
    })
    rows.append({
        "group": "Nouwens et al. (Cookiebot + OneTrust + Crownpeak + Quantcast + Trustarc)",
        "count": len(nouwens),
        "metric": "Forced action",
        "value": _fmt(n_dp.get("forced_action")),
    })

    return rows


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def generate_results(
    engine: Engine, experiment_id: str, *, include_charts: bool = True,
) -> Dict[str, Any]:
    """Fetch data, compute violations/dark-patterns, optionally generate charts."""

    data = fetch_experiment_data(engine, experiment_id)
    if data.get("empty"):
        return {"empty": True}

    cr = data.get("crawl_results", pd.DataFrame())

    violations = compute_violations(cr)
    dark_patterns = compute_dark_patterns(cr)

    cookie_notices = int(len(cr[cr["cookie_notice_detected"] >= 1])) if not cr.empty else 0
    total = data["total"]
    analyzed = data["analyzed_count"]
    supported = data["supported_count"]
    unsupported = data["unsupported_count"]
    n_total = _count_websites_with_any_violation(cr, include_missing_notice=True)
    n_excl_mn = _count_websites_with_any_violation(cr, include_missing_notice=False)

    def _ratio(val: int, base: int) -> str:
        if base > 0:
            return f"{val}/{base} ({round(val / base * 100, 1)}%)"
        return f"{val}/0"

    overview = {
        "total": total,
        "supported": supported,
        "supported_ratio": _ratio(supported, total),
        "unsupported": unsupported,
        "unsupported_ratio": _ratio(unsupported, total),
        "no_language": data["no_language_count"],
        "analyzed": analyzed,
        "analyzed_ratio": _ratio(analyzed, supported),
        "errors": data["error_count"],
        "cookie_notices": cookie_notices,
        "cookie_notices_pct": round(100 * cookie_notices / analyzed, 1) if analyzed > 0 else 0,
        "with_violations": n_total,
        "violations_total_ratio": _ratio(n_total, analyzed),
        "violations_pct": round(
            100 * n_total / analyzed, 1
        ) if analyzed > 0 else 0,
        "violations_with_cn": n_excl_mn,
        "violations_cn_ratio": _ratio(n_excl_mn, cookie_notices),
        "violations_with_cn_pct": round(
            100 * n_excl_mn / cookie_notices, 1
        ) if cookie_notices > 0 else 0,
        "total_cookies": sum(data.get("cookie_agg", {}).values()),
        "total_sentences": sum(data.get("purpose_agg", {}).values()),
    }

    table_rows: List[Dict] = []
    all_labels = {**VIOLATION_LABELS, **DARK_PATTERN_LABELS}
    for key, val in violations.items():
        count, base, pct = val
        table_rows.append({"key": key, "label": all_labels.get(key, key),
                           "count": count, "base": base, "pct": pct, "type": "violation"})
    for key, val in dark_patterns.items():
        count, base, pct = val
        table_rows.append({"key": key, "label": all_labels.get(key, key),
                           "count": count, "base": base, "pct": pct, "type": "dark_pattern"})

    cmp_rows = compute_cmp_analysis(cr)

    result: Dict[str, Any] = {
        "empty": False,
        "overview": overview,
        "violations": violations,
        "dark_patterns": dark_patterns,
        "table_rows": table_rows,
        "cmp_rows": cmp_rows,
    }

    if include_charts:
        result.update({
            "violations_chart": build_violations_chart(violations, dark_patterns),
            "rank_chart": build_rank_chart(cr),
            "cookies_chart": build_cookies_chart(data.get("cookie_agg", {})),
            "purposes_chart": build_purposes_chart(data.get("purpose_agg", {})),
        })

    return result


def results_to_csv(engine: Engine, experiment_id: str) -> str:
    """Generate CSV export of violations, dark patterns, and CMP analysis."""
    res = generate_results(engine, experiment_id, include_charts=False)
    if res.get("empty"):
        return ""
    buf = io.StringIO()

    rows = res.get("table_rows", [])
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df[["type", "label", "count", "base", "pct"]]
        df.columns = ["Type", "Violation", "Count", "Base", "Percentage"]
    df.to_csv(buf, index=False)

    cmp = res.get("cmp_rows", [])
    if cmp:
        buf.write("\n")
        cmp_df = pd.DataFrame(cmp)
        cmp_df.columns = ["CMP Group", "Websites", "Metric", "Result"]
        cmp_df.to_csv(buf, index=False)

    return buf.getvalue()
