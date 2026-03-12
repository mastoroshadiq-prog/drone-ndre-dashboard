"""
Dashboard NDRE 2025 vs 2026 — Kebun Kelapa Sawit AME II & AME IV
Pengguna: Direktur, Manager, Asisten Manager, Mandor
"""
import io
from collections import Counter, defaultdict
from typing import Dict, List, Optional

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import folium
from streamlit_folium import st_folium

from supabase_helper import (
    fetch_anomaly_koordinat,
    fetch_blok_summary,
    fetch_comparison_sample,
    fetch_divisi_summary,
    fetch_transition_matrix,
    fetch_koordinat_blok,
    get_supabase_client,
)
from cincin_api import render_cincin_api_tab


# ══════════════════════════════════════════════════════════════════
# KONSTANTA
# ══════════════════════════════════════════════════════════════════
DATASET_OPTIONS = {
    "AME II (NDRE_02_2026)": "NDRE_02_2026",
    "AME IV (AME_IV_2026)": "AME_IV_2026",
    "Semua Divisi": "__ALL__",
}
ANOMALY_TAGS = ["KOORDINAT_AME_II_2026", "KOORDINAT_AME_IV_2026"]

WARNA_KLASS = {
    "Stres Sangat Berat": "#c0392b",
    "Stres Berat": "#e67e22",
    "Stres Sedang": "#f1c40f",
    "Stres Ringan": "#2ecc71",
    "Tidak Ada Data": "#bdc3c7",
    "Lainnya": "#95a5a6",
}

DELTA_THRESHOLD = 0.05

# ══════════════════════════════════════════════════════════════════
# HELPER KALKULASI
# ══════════════════════════════════════════════════════════════════
def safe_div(a, b, default=0.0):
    return round(a / b * 100, 1) if b > 0 else default


def pct_str(a, b):
    return f"{safe_div(a, b):.1f}%"

import re
def format_blok_display(blok):
    match = re.match(r'^([A-Z])(\d+)([A-Z]?)$', str(blok).strip())
    if match:
        charPart = match.group(1)
        numPart = int(match.group(2))
        suffixPart = match.group(3)
        if not suffixPart:
            if charPart == 'F' and numPart == 8:
                suffixPart = 'B'
            else:
                suffixPart = 'A'
        return f'{charPart}{numPart:03d}{suffixPart}'
    return blok


def health_score(divisi_rows: List[Dict]) -> Dict:
    """Hitung skor kesehatan vegetasi 0-100 dari ringkasan divisi."""
    total = sum(r.get("total_pohon", 0) or 0 for r in divisi_rows)
    sangat_berat = sum(r.get("klass26_sangat_berat", 0) or 0 for r in divisi_rows)
    stres_berat  = sum(r.get("klass26_stres_berat", 0) or 0 for r in divisi_rows)
    improved     = sum(r.get("count_improved", 0) or 0 for r in divisi_rows)
    degraded     = sum(r.get("count_degraded", 0) or 0 for r in divisi_rows)
    orphan       = sum(r.get("orphan_no_link", 0) or 0 for r in divisi_rows)

    score = 100.0
    score -= safe_div(sangat_berat, max(1, total)) * 0.5
    score -= safe_div(stres_berat,  max(1, total)) * 0.3
    score -= safe_div(degraded,     max(1, total)) * 0.1
    score -= safe_div(orphan,       max(1, total)) * 0.1
    score = max(0.0, min(100.0, round(score, 1)))

    if score >= 75:
        label, color, icon = "BAIK", "#27ae60", "🟢"
    elif score >= 55:
        label, color, icon = "PERLU PERHATIAN", "#f39c12", "🟡"
    elif score >= 35:
        label, color, icon = "KRITIS", "#e67e22", "🟠"
    else:
        label, color, icon = "DARURAT", "#c0392b", "🔴"

    return {"score": score, "label": label, "color": color, "icon": icon,
            "total": total, "sangat_berat": sangat_berat, "stres_berat": stres_berat,
            "improved": improved, "degraded": degraded}


def aggregate_divisi(divisi_rows: List[Dict]) -> Dict:
    """Agregat beberapa baris per-divisi menjadi 1 total."""
    agg = defaultdict(int)
    for r in divisi_rows:
        for k, v in r.items():
            if isinstance(v, (int, float)) and v is not None:
                agg[k] += v
    return dict(agg)


def normalize_klass(k: Optional[str]) -> str:
    if not k or k in ("-", "(blank)", ""):
        return "Tidak Ada Data"
    k = str(k).strip()
    if "Sangat Berat" in k:
        return "Stres Sangat Berat"
    if "Berat" in k:
        return "Stres Berat"
    if "Sedang" in k:
        return "Stres Sedang"
    if "Ringan" in k:
        return "Stres Ringan"
    return "Tidak Ada Data"


# ══════════════════════════════════════════════════════════════════
# LOAD DATA (cached 5 menit)
# ══════════════════════════════════════════════════════════════════
def _nv(v, default=0):
    """Null-safe numeric value."""
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def _norm_klass(k):
    if not k or str(k).strip() in ("-", "(blank)", "", "nan"):
        return None
    k = str(k).strip()
    if "Sangat Berat" in k:
        return "Stres Sangat Berat"
    if "Berat" in k:
        return "Stres Berat"
    if "Sedang" in k:
        return "Stres Sedang"
    if "Ringan" in k:
        return "Stres Ringan"
    return None


def compute_from_raw(rows: List[Dict]) -> Dict:
    """Kompute agregasi divisi & blok dari raw rows tanpa SQL Views.
    
    Catatan data:
    - AME II : klass_ndre_1_25 & ndre_1_25 terisi langsung di kolom
    - AME IV : klass_ndre_1_25 NULL, data 2025 ada di raw_csv_json->source_2026->klassndre12025
    """
    THRESH = 0.05

    def _resolve_klass25(r: Dict):
        """Ambil klasifikasi 2025 — dari kolom langsung (AME II) atau raw_csv_json (AME IV)."""
        k = _norm_klass(r.get("klass_ndre_1_25"))
        if k:
            return k
        raw = r.get("raw_csv_json")
        if isinstance(raw, dict):
            src26 = raw.get("source_2026") or {}
            k_raw = src26.get("klassndre12025") if isinstance(src26, dict) else None
            return _norm_klass(k_raw)
        return None

    def _resolve_ndre25(r: Dict):
        """Ambil nilai NDRE 2025 — dari kolom langsung atau raw_csv_json."""
        v = r.get("ndre_1_25")
        if v is not None:
            return _nv(v)
        raw = r.get("raw_csv_json")
        if isinstance(raw, dict):
            src26 = raw.get("source_2026") or {}
            if isinstance(src26, dict):
                raw_val = src26.get("ndre125")
                if raw_val and str(raw_val).strip() not in ("-", "", "(blank)"):
                    try:
                        return float(raw_val)
                    except (ValueError, TypeError):
                        pass
        return None

    # blok_agg: {(dataset_tag, divisi, blok): {...}}
    blok_agg: Dict = defaultdict(lambda: defaultdict(float))

    for r in rows:
        key = (r.get("dataset_tag", ""), r.get("divisi", "?"), r.get("blok", "?"))
        a = blok_agg[key]
        a["total_pohon"] += 1

        v25  = _resolve_ndre25(r)   # resolved (AME II direct, AME IV from raw_csv_json)
        v26  = r.get("ndre_2_26")
        k25  = _resolve_klass25(r)  # resolved
        k26  = _norm_klass(r.get("klass_ndre_2_26"))

        has25 = v25 is not None
        has26 = v26 is not None
        if has25: a["sum_25"] += v25; a["cnt_25"] += 1
        if has26: a["sum_26"] += _nv(v26); a["cnt_26"] += 1
        if has25 and has26:
            a["pohon_lengkap"] += 1
            dlt = _nv(v26) - v25
            a["sum_delta"] += dlt; a["cnt_delta"] += 1
            if dlt >= THRESH:     a["count_improved"] += 1
            elif dlt <= -THRESH:  a["count_degraded"] += 1
            else:                 a["count_stable"] += 1
        else:
            # Coba pakai kolom ndre_delta yang sudah ada (untuk AME II)
            dlt_col = r.get("ndre_delta")
            if dlt_col is not None:
                d = _nv(dlt_col)
                a["sum_delta"] += d; a["cnt_delta"] += 1
                if d >= THRESH:     a["count_improved"] += 1
                elif d <= -THRESH:  a["count_degraded"] += 1
                else:               a["count_stable"] += 1
            else:
                a["count_no_delta"] += 1

        # Klass 2026
        if k26 == "Stres Sangat Berat": a["klass26_sangat_berat"] += 1
        elif k26 == "Stres Berat":      a["klass26_stres_berat"] += 1
        elif k26 == "Stres Sedang":     a["klass26_sedang"] += 1
        elif k26 == "Stres Ringan":     a["klass26_ringan"] += 1
        else:                           a["klass26_tidak_ada"] += 1

        # Klass 2025 (resolved)
        if k25 == "Stres Sangat Berat": a["klass25_sangat_berat"] += 1
        elif k25 == "Stres Berat":      a["klass25_stres_berat"] += 1
        elif k25 == "Stres Sedang":     a["klass25_sedang"] += 1
        elif k25 == "Stres Ringan":     a["klass25_ringan"] += 1

        if r.get("id_npokok") is None:  a["orphan_no_link"] += 1

    blok_summary = []
    divisi_dict: Dict = defaultdict(lambda: defaultdict(float))

    for (ds, div, blk), a in blok_agg.items():
        tot = a.get("total_pohon", 0)
        row = {
            "dataset_tag":          ds,
            "divisi":               div,
            "blok":                 blk,
            "total_pohon":          int(tot),
            "pohon_ada_2025":       int(a.get("cnt_25", 0)),
            "pohon_ada_2026":       int(a.get("cnt_26", 0)),
            "pohon_lengkap":        int(a.get("pohon_lengkap", 0)),
            "avg_ndre_2025":        round(a["sum_25"] / a["cnt_25"], 6) if a.get("cnt_25") else None,
            "avg_ndre_2026":        round(a["sum_26"] / a["cnt_26"], 6) if a.get("cnt_26") else None,
            "avg_delta":            round(a["sum_delta"] / a["cnt_delta"], 6) if a.get("cnt_delta") else None,
            "count_improved":       int(a.get("count_improved", 0)),
            "count_degraded":       int(a.get("count_degraded", 0)),
            "count_stable":         int(a.get("count_stable", 0)),
            "count_no_delta":       int(a.get("count_no_delta", 0)),
            "klass26_sangat_berat": int(a.get("klass26_sangat_berat", 0)),
            "klass26_stres_berat":  int(a.get("klass26_stres_berat", 0)),
            "klass26_sedang":       int(a.get("klass26_sedang", 0)),
            "klass26_ringan":       int(a.get("klass26_ringan", 0)),
            "klass26_tidak_ada":    int(a.get("klass26_tidak_ada", 0)),
            "klass25_sangat_berat": int(a.get("klass25_sangat_berat", 0)),
            "klass25_stres_berat":  int(a.get("klass25_stres_berat", 0)),
            "klass25_sedang":       int(a.get("klass25_sedang", 0)),
            "klass25_ringan":       int(a.get("klass25_ringan", 0)),
            "orphan_no_link":       int(a.get("orphan_no_link", 0)),
            "total_blok":           1,
        }
        blok_summary.append(row)

        # Agregasi ke level divisi
        dk = (ds, div)
        for field in ["total_pohon","pohon_lengkap","count_improved","count_degraded",
                      "count_stable","klass26_sangat_berat","klass26_stres_berat",
                      "klass26_sedang","klass26_ringan","klass26_tidak_ada",
                      "klass25_sangat_berat","klass25_stres_berat","klass25_sedang",
                      "klass25_ringan","orphan_no_link","cnt_25","cnt_26"]:
            divisi_dict[dk][field] += a.get(field, 0)
        divisi_dict[dk]["sum_25"]    += a.get("sum_25", 0)
        divisi_dict[dk]["sum_26"]    += a.get("sum_26", 0)
        divisi_dict[dk]["sum_delta"] += a.get("sum_delta", 0)
        divisi_dict[dk]["cnt_delta"] += a.get("cnt_delta", 0)
        divisi_dict[dk]["blok_count"] += 1

    divisi_summary = []
    for (ds, div), a in divisi_dict.items():
        divisi_summary.append({
            "dataset_tag":          ds,
            "divisi":               div,
            "total_pohon":          int(a["total_pohon"]),
            "pohon_lengkap":        int(a["pohon_lengkap"]),
            "avg_ndre_2025":        round(a["sum_25"] / a["cnt_25"], 6) if a.get("cnt_25") else None,
            "avg_ndre_2026":        round(a["sum_26"] / a["cnt_26"], 6) if a.get("cnt_26") else None,
            "avg_delta":            round(a["sum_delta"] / a["cnt_delta"], 6) if a.get("cnt_delta") else None,
            "count_improved":       int(a["count_improved"]),
            "count_degraded":       int(a["count_degraded"]),
            "count_stable":         int(a["count_stable"]),
            "klass26_sangat_berat": int(a["klass26_sangat_berat"]),
            "klass26_stres_berat":  int(a["klass26_stres_berat"]),
            "klass26_sedang":       int(a["klass26_sedang"]),
            "klass26_ringan":       int(a["klass26_ringan"]),
            "klass26_tidak_ada":    int(a["klass26_tidak_ada"]),
            "klass25_sangat_berat": int(a["klass25_sangat_berat"]),
            "klass25_stres_berat":  int(a["klass25_stres_berat"]),
            "klass25_sedang":       int(a["klass25_sedang"]),
            "klass25_ringan":       int(a["klass25_ringan"]),
            "orphan_no_link":       int(a["orphan_no_link"]),
            "total_blok":           int(a["blok_count"]),
        })

    # Transition matrix (AME II & AME IV dengan data 2025 resolved)
    trans_counter: Dict = defaultdict(int)
    for r in rows:
        k25 = _resolve_klass25(r)
        k26 = _norm_klass(r.get("klass_ndre_2_26"))
        if k25 and k26:
            key = (r.get("dataset_tag", ""), r.get("divisi", ""), k25, k26)
            trans_counter[key] += 1
    transition = [
        {"dataset_tag": ds, "divisi": div, "klass_2025": k25,
         "klass_2026": k26, "jumlah_pohon": cnt}
        for (ds, div, k25, k26), cnt in trans_counter.items()
    ]

    return divisi_summary, blok_summary, transition


@st.cache_data(ttl=300, show_spinner=False)
def load_all_data(selected_datasets: tuple, divisi_filter: str):
    client = get_supabase_client()
    tags = list(selected_datasets) if "__ALL__" not in selected_datasets else None
    div_arg = divisi_filter if divisi_filter != "SEMUA" else None

    # Ambil anomali dulu (selalu dari tabel langsung, kecil)
    anomaly = fetch_anomaly_koordinat(client, dataset_tags=ANOMALY_TAGS, divisi=div_arg)

    # Coba SQL Views
    divisi_summary = fetch_divisi_summary(client, dataset_tags=tags, divisi=div_arg)
    blok_summary   = fetch_blok_summary(client, dataset_tags=tags, divisi=div_arg)
    transition     = fetch_transition_matrix(client, dataset_tags=tags, divisi=div_arg)
    view_ok        = len(divisi_summary) > 0

    if not view_ok:
        # Fallback: komputasi dari raw data
        raw_rows = fetch_comparison_sample(
            client, dataset_tags=tags, divisi=div_arg, max_rows=110000,
        )
        divisi_summary, blok_summary, transition = compute_from_raw(raw_rows)
        
    return {
        "divisi_summary": divisi_summary,
        "blok_summary":   blok_summary,
        "transition":     transition,
        "anomaly":        anomaly,
        "view_ok":        view_ok,
    }


# ══════════════════════════════════════════════════════════════════
# SECTION: HEADER & FIlters
# ══════════════════════════════════════════════════════════════════
def render_filters():
    st.markdown("### ⚙️ Filter Data")
    col1, col2, col3 = st.columns([2, 5, 2])
    with col1:
        dataset_label = st.selectbox(
            "Dataset Penerbangan",
            options=list(DATASET_OPTIONS.keys()),
            index=2,
            help="Pilih dataset drone. 'Semua Divisi' menampilkan AME II & AME IV.",
        )
        selected_dataset_tag = DATASET_OPTIONS[dataset_label]
    
    with col2:
        divisi_filter = st.selectbox(
            "Filter Divisi",
            options=["SEMUA", "AME II", "AME IV"],
            index=0,
        )

    with col3:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🔄 Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
            
    st.caption("🕐 Data di-cache 5 menit | 📅 Data terakhir: Penerbangan Drone Feb 2026")

    return selected_dataset_tag, divisi_filter


# ══════════════════════════════════════════════════════════════════
# SECTION 1: RINGKASAN EKSEKUTIF
# ══════════════════════════════════════════════════════════════════
def render_executive_summary(data: Dict):
    st.header("\U0001f4ca Ringkasan Eksekutif")

    div_rows  = data["divisi_summary"]
    blok_rows = data["blok_summary"]
    anomaly   = data["anomaly"]

    if not div_rows:
        st.warning("Data ringkasan tidak tersedia. Pastikan SQL Views sudah dibuat atau data tersedia.")
        return

    health = health_score(div_rows)
    total  = health["total"]

    # ══ BARIS 1: Metric Cards ═══════════════════════════════════════
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric(
            "\U0001f33f Total Pohon Terpantau",
            f"{total:,}",
            help="Total pohon yang terdata dari penerbangan drone",
        )
    with col2:
        pct_sb = safe_div(health["sangat_berat"] + health["stres_berat"], total)
        st.metric(
            "\U0001f534 Stres Berat+SB",
            f"{health['sangat_berat'] + health['stres_berat']:,}",
            f"{pct_sb:.1f}% dari total",
            delta_color="inverse",
        )
    with col3:
        pct_imp = safe_div(health["improved"], total)
        st.metric(
            "\U0001f53c Kondisi Membaik",
            f"{health['improved']:,}",
            f"{pct_imp:.1f}% dari total",
        )
    with col4:
        pct_deg = safe_div(health["degraded"], total)
        st.metric(
            "\U0001f53d Kondisi Menurun",
            f"{health['degraded']:,}",
            f"-{pct_deg:.1f}% dari total",
            delta_color="inverse",
        )
    with col5:
        st.metric(
            "\U0001f3af Skor Kesehatan",
            f"{health['score']}/100",
            health["label"],
        )

    # ══ BARIS 2: Health Banner ══════════════════════════════════════
    banner_bg = health["color"] + "22"
    st.markdown(
        f"""
        <div style="background:{banner_bg}; border-left:6px solid {health['color']};
                    padding:14px 20px; border-radius:8px; margin:10px 0;">
            <span style="font-size:1.4rem;">{health['icon']}</span>
            <strong style="font-size:1.1rem; margin-left:10px;">
                Status Vegetasi Kebun: {health['label']} ({health['score']}/100)
            </strong>
            <p style="margin:5px 0 0 0; color:#444; font-size:0.9rem;">
                {_health_guidance(health['score'])}
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("---")

    # ══ BARIS 3: Donut Per Divisi + ringkasan angka + tren ══════════
    st.markdown("#### \U0001f334 Kondisi & Distribusi per Divisi")

    divisi_groups: Dict[str, list] = defaultdict(list)
    for r in div_rows:
        divisi_groups[r.get("divisi", "UNKNOWN")].append(r)

    div_cols = st.columns(len(divisi_groups))
    for i, (divisi, rows) in enumerate(sorted(divisi_groups.items())):
        agg  = aggregate_divisi(rows)
        h    = health_score(rows)
        tot  = h["total"]
        has_2025 = agg.get("pohon_lengkap", 0) > 0
        note = "" if has_2025 else "Hanya data 2026"

        labels_d = ["Stres Sangat Berat", "Stres Berat", "Stres Sedang", "Stres Ringan", "Tidak Ada Data"]
        vals_d   = [
            int(agg.get("klass26_sangat_berat", 0)),
            int(agg.get("klass26_stres_berat",  0)),
            int(agg.get("klass26_sedang",        0)),
            int(agg.get("klass26_ringan",        0)),
            int(agg.get("klass26_tidak_ada",     0)),
        ]
        colors_d = [WARNA_KLASS[l] for l in labels_d]

        fig_d = go.Figure(go.Pie(
            labels=labels_d, values=vals_d,
            marker_colors=colors_d,
            hole=0.6,
            textinfo="percent",
            hovertemplate="%{label}<br>%{value:,} pohon (%{percent})<extra></extra>",
            showlegend=False,
        ))
        fig_d.add_annotation(
            text=f"{h['icon']} {h['score']}<br>{h['label']}",
            x=0.5, y=0.5, showarrow=False, font_size=11,
        )
        title_txt = f"<b>{divisi}</b> - {tot:,} pohon"
        if note:
            title_txt += f"<br><span style='color:#e67e22;font-size:11px'>{note}</span>"
        fig_d.update_layout(
            height=250, margin=dict(t=30, b=0, l=0, r=0),
            title=dict(text=title_txt, font_size=13, x=0.5),
        )
        with div_cols[i]:
            st.plotly_chart(fig_d, use_container_width=True, key=f"exec_donut_{divisi}")
            # Ringkasan angka di bawah chart
            pct_k = safe_div(h["sangat_berat"] + h["stres_berat"], tot)
            pct_s = safe_div(int(agg.get("klass26_sedang", 0)), tot)
            pct_r = safe_div(int(agg.get("klass26_ringan", 0)), tot)
            st.markdown(
                f"""
                <div style="background:#f8f9fa;border-radius:8px;padding:10px 12px;
                            font-size:0.82rem;line-height:1.9;">
                    \U0001f534 Stres Berat+SB
                    <strong style="float:right;color:#c0392b">{h['sangat_berat']+h['stres_berat']:,} ({pct_k:.1f}%)</strong><br>
                    \U0001f7e1 Stres Sedang
                    <strong style="float:right;color:#e67e22">{int(agg.get('klass26_sedang',0)):,} ({pct_s:.1f}%)</strong><br>
                    \U0001f7e2 Stres Ringan
                    <strong style="float:right;color:#27ae60">{int(agg.get('klass26_ringan',0)):,} ({pct_r:.1f}%)</strong>
                </div>
                """,
                unsafe_allow_html=True,
            )
            if has_2025:
                imp_d  = int(agg.get("count_improved", 0))
                deg_d  = int(agg.get("count_degraded", 0))
                diff_d = imp_d - deg_d
                arrow  = "Tren Membaik" if diff_d > 0 else "Tren Menurun" if diff_d < 0 else "Stabil"
                color_t = "#27ae60" if diff_d > 0 else "#e74c3c" if diff_d < 0 else "#7f8c8d"
                st.markdown(
                    f"<div style='margin-top:6px;text-align:center;font-size:0.82rem;"
                    f"color:{color_t};font-weight:600;'>"
                    f"{arrow} ({imp_d:,} naik vs {deg_d:,} turun)</div>",
                    unsafe_allow_html=True,
                )

    st.markdown("---")

    # ══ BARIS 4: Hotspot Blok | Tren Pie | Anomali Alert ═══════════
    col_hot, col_trend, col_anom = st.columns([2, 2, 1])

    # ── Top 5 Blok ────────────────────────────────────────────────
    with col_hot:
        st.markdown("##### \U0001f534 Top 5 Blok Prioritas")
        if blok_rows:
            df_b = pd.DataFrame(blok_rows)
            for c in ["count_degraded", "total_pohon"]:
                if c in df_b.columns:
                    df_b[c] = pd.to_numeric(df_b[c], errors="coerce").fillna(0)
            if "count_degraded" in df_b.columns:
                top5 = df_b.nlargest(5, "count_degraded").copy()
                top5["pct"] = top5.apply(
                    lambda r: safe_div(r.get("count_degraded", 0), max(r.get("total_pohon", 1), 1)), axis=1
                )
                labels5  = [
                    f"{r.get('divisi','')} - {r.get('blok','')}" for _, r in top5.iterrows()
                ]
                counts5  = list(top5["count_degraded"].astype(int))
                colors5  = [
                    "#c0392b" if p > 30 else "#e67e22" if p > 15 else "#f39c12"
                    for p in top5["pct"]
                ]
                fig_h5 = go.Figure(go.Bar(
                    x=counts5, y=labels5, orientation="h",
                    marker_color=colors5,
                    text=[f"{c:,}" for c in counts5],
                    textposition="outside",
                ))
                fig_h5.update_layout(
                    height=220, margin=dict(t=5, b=10, l=130, r=50),
                    xaxis_title="Pohon Menurun",
                    yaxis=dict(autorange="reversed"),
                    plot_bgcolor="white",
                )
                st.plotly_chart(fig_h5, use_container_width=True, key="exec_top5")
                st.caption("Klik tab Tren & Hotspot untuk 15 blok lengkap")
        else:
            st.info("Data blok belum tersedia.")

    # ── Tren Pie AME II ───────────────────────────────────────────
    with col_trend:
        st.markdown("##### \U0001f4c8 Tren AME II (2025 vs 2026)")
        ame2_rows = [r for r in div_rows if r.get("divisi") == "AME II"]
        if ame2_rows:
            agg2  = aggregate_divisi(ame2_rows)
            imp2  = int(agg2.get("count_improved", 0))
            deg2  = int(agg2.get("count_degraded", 0))
            stab2 = int(agg2.get("count_stable",   0))
            tot2  = imp2 + deg2 + stab2
            fig_t = go.Figure(go.Pie(
                labels=["Membaik", "Menurun", "Stabil"],
                values=[imp2, deg2, stab2],
                marker_colors=["#27ae60", "#e74c3c", "#bdc3c7"],
                hole=0.52,
                textinfo="percent+label",
                textposition="outside",
                hovertemplate="%{label}<br>%{value:,} pohon (%{percent})<extra></extra>",
                showlegend=False,
            ))
            fig_t.add_annotation(
                text=f"{tot2:,}<br>pohon",
                x=0.5, y=0.5, showarrow=False, font_size=11,
            )
            fig_t.update_layout(height=220, margin=dict(t=5, b=10, l=10, r=10))
            st.plotly_chart(fig_t, use_container_width=True, key="exec_tren_pie")
            diff2  = imp2 - deg2
            c2     = "#27ae60" if diff2 > 0 else "#e74c3c"
            lbl2   = f"+{diff2:,} lebih banyak membaik" if diff2 > 0 else f"{abs(diff2):,} lebih banyak menurun"
            st.markdown(
                f"<div style='text-align:center;color:{c2};font-weight:600;font-size:0.85rem'>{lbl2}</div>",
                unsafe_allow_html=True,
            )
        else:
            st.info("Data tren AME II tidak tersedia.")

    # ── Anomali Alert ─────────────────────────────────────────────
    with col_anom:
        st.markdown("##### \u26a0\ufe0f Anomali Data")
        total_anom = len(anomaly)
        by_div_a   = defaultdict(int)
        for r in anomaly:
            by_div_a[r.get("divisi", "?")] += 1
        rows_div_html = "".join(
            f'<div style="font-size:0.78rem;color:#555;margin:2px 0;"><b>{d}:</b> {c}</div>'
            for d, c in sorted(by_div_a.items())
        )
        st.markdown(
            f"""
            <div style="background:#fff3cd;border:1px solid #ffc107;border-radius:10px;
                        padding:14px;text-align:center;">
                <div style="font-size:2.5rem;font-weight:800;color:#c0392b;line-height:1">
                    {total_anom}</div>
                <div style="font-size:0.82rem;font-weight:600;color:#856404;margin:4px 0 8px 0;">
                    Pohon Tanpa Nomor ID</div>
                <hr style="border-color:#ffc10730;margin:6px 0;">
                {rows_div_html}
                <div style="margin-top:8px;font-size:0.72rem;color:#856404;">
                    Semua PENDING</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.caption("Tab Anomali Data untuk detail & unduh CSV")



def _health_guidance(score: float) -> str:
    if score >= 75:
        return "Kondisi vegetasi secara umum baik. Lanjutkan pemantauan rutin dan pemupukan sesuai jadwal."
    elif score >= 55:
        return "Ada blok yang perlu perhatian. Rekomendasikan pengecekan lapangan pada area dengan stres berat."
    elif score >= 35:
        return "Kondisi kritis. Segera lakukan inspeksi lapangan dan tindakan agronomis pada blok-blok prioritas."
    else:
        return "Darurat! Diperlukan tindakan segera. Hubungi tim agronomis untuk intervensi mendesak."


# ══════════════════════════════════════════════════════════════════
# SECTION 2: PERBANDINGAN 2025 vs 2026 per Divisi
# ══════════════════════════════════════════════════════════════════
def render_divisi_comparison(data: Dict):
    st.header("📅 Perbandingan NDRE 2025 vs 2026 per Divisi")
    st.caption("Distribusi Vegetasi — perubahan kondisi dalam divisi yang sama antara penerbangan 2025 dan 2026")

    div_rows = data["divisi_summary"]
    if not div_rows:
        st.info("Data tidak tersedia.")
        return

    divisi_groups: Dict[str, List] = defaultdict(list)
    for r in div_rows:
        divisi_groups[r.get("divisi", "?")].append(r)

    KLASS_NAMES  = ["Stres Sangat Berat", "Stres Berat", "Stres Sedang", "Stres Ringan"]
    KLASS25_KEYS = ["klass25_sangat_berat", "klass25_stres_berat", "klass25_sedang", "klass25_ringan"]
    KLASS26_KEYS = ["klass26_sangat_berat", "klass26_stres_berat", "klass26_sedang", "klass26_ringan"]
    COLORS_KLASS = ["#c0392b", "#e67e22", "#f1c40f", "#2ecc71"]

    def make_donut_veg(vals, tot_v, label, key_id):
        """Donut chart Distribusi Vegetasi untuk satu tahun."""
        fig = go.Figure(go.Pie(
            labels=KLASS_NAMES, values=vals,
            marker_colors=COLORS_KLASS,
            hole=0.55,
            textinfo="percent",
            hovertemplate="%{label}<br>%{value:,} pohon (%{percent})<extra></extra>",
            showlegend=False,
        ))
        fig.add_annotation(
            text=f"<b>{tot_v:,}</b><br>pohon",
            x=0.5, y=0.5, showarrow=False, font_size=13,
        )
        fig.update_layout(
            title=f"<b>{label}</b>",
            height=300,
            margin=dict(t=40, b=10, l=10, r=10),
        )
        return fig

    def legend_bar():
        """Legend warna kategori vegetasi (bersama)."""
        leg_cols = st.columns(len(KLASS_NAMES))
        for li, (name, color) in enumerate(zip(KLASS_NAMES, COLORS_KLASS)):
            with leg_cols[li]:
                st.markdown(
                    f"<div style='text-align:center;'>"
                    f"<span style='background:{color};color:white;padding:3px 10px;"
                    f"border-radius:4px;font-size:0.78rem;font-weight:600;'>"
                    f"{name}</span></div>",
                    unsafe_allow_html=True,
                )
        st.markdown("<br>", unsafe_allow_html=True)

    # ── Loop per divisi (AME II, AME IV — urut alfabet) ───────────────
    for divisi in sorted(divisi_groups.keys()):
        rows = divisi_groups[divisi]
        agg  = aggregate_divisi(rows)

        vals_25  = [int(agg.get(k, 0)) for k in KLASS25_KEYS]
        vals_26  = [int(agg.get(k, 0)) for k in KLASS26_KEYS]
        tot_25   = sum(vals_25)
        tot_26   = sum(vals_26)
        has_2025 = tot_25 > 0

        # ── Header divisi ──────────────────────────────────────────────
        st.markdown("---")
        col_title, col_badge = st.columns([4, 1])
        with col_title:
            st.subheader(f"\U0001f334 {divisi}")
        with col_badge:
            if has_2025:
                st.success("\u2705 Data 2025 & 2026 tersedia")
            else:
                st.warning("\u26a0\ufe0f Hanya data 2026")

        # ── Distribusi Vegetasi ────────────────────────────────────────
        st.markdown("#### \U0001f33f Distribusi Vegetasi")

        if has_2025:
            # Side-by-side: 2025 di kiri, 2026 di kanan
            col_d25, col_arr, col_d26 = st.columns([5, 1, 5])
            with col_d25:
                st.plotly_chart(
                    make_donut_veg(vals_25, tot_25, "Penerbangan 2025", f"veg25_{divisi}"),
                    use_container_width=True, key=f"veg25_{divisi}",
                )
            with col_arr:
                st.markdown(
                    "<div style='text-align:center;font-size:2.5rem;"
                    "margin-top:110px;color:#3498db;'>\u2192</div>",
                    unsafe_allow_html=True,
                )
            with col_d26:
                st.plotly_chart(
                    make_donut_veg(vals_26, tot_26, "Penerbangan 2026", f"veg26_{divisi}"),
                    use_container_width=True, key=f"veg26_{divisi}",
                )
        else:
            # Hanya 2026 — tampilkan terpusat
            col_sp, col_mid, col_sp2 = st.columns([2, 4, 2])
            with col_mid:
                st.plotly_chart(
                    make_donut_veg(vals_26, tot_26, f"Penerbangan 2026", f"veg26only_{divisi}"),
                    use_container_width=True, key=f"veg26only_{divisi}",
                )

        legend_bar()

        if not has_2025:
            # Snapshot table 2026 saja
            st.markdown("#### \U0001f4cb Kondisi Vegetasi 2026")
            snap_rows = []
            for name, v26 in zip(KLASS_NAMES, vals_26):
                snap_rows.append({
                    "Kategori Vegetasi": name,
                    "Jumlah Pohon (2026)": f"{v26:,}",
                    "Persentase": f"{safe_div(v26, max(tot_26, 1)):.1f}%",
                })
            st.dataframe(
                pd.DataFrame(snap_rows),
                use_container_width=True,
                hide_index=True,
            )
            continue

        # ── Bar chart perubahan ────────────────────────────────────────
        st.markdown("#### \U0001f4ca Perubahan Jumlah Pohon per Kategori")
        fig_bar = go.Figure()
        fig_bar.add_trace(go.Bar(
            name="2025", x=KLASS_NAMES, y=vals_25,
            marker_color="#3498db",
            text=[f"{v:,}" for v in vals_25], textposition="outside",
        ))
        fig_bar.add_trace(go.Bar(
            name="2026", x=KLASS_NAMES, y=vals_26,
            marker_color="#e74c3c",
            text=[f"{v:,}" for v in vals_26], textposition="outside",
        ))
        fig_bar.update_layout(
            barmode="group",
            title=f"{divisi} — Jumlah Pohon per Kategori Vegetasi (2025 vs 2026)",
            yaxis_title="Jumlah Pohon",
            height=360,
            margin=dict(t=50, b=30),
            legend=dict(orientation="h", x=0.4, y=1.12),
            plot_bgcolor="white",
        )
        st.plotly_chart(fig_bar, use_container_width=True, key=f"bar_{divisi}")

        # ── Tabel delta ────────────────────────────────────────────────
        st.markdown("#### \U0001f4cb Perubahan Detail per Kategori")
        delta_rows = []
        for name, v25, v26 in zip(KLASS_NAMES, vals_25, vals_26):
            delta      = v26 - v25
            pct25      = safe_div(v25, max(tot_25, 1))
            pct26      = safe_div(v26, max(tot_26, 1))
            delta_poin = round(pct26 - pct25, 1)
            delta_rows.append({
                "Kategori Vegetasi": name,
                "2025": f"{v25:,} ({pct25:.1f}%)",
                "2026": f"{v26:,} ({pct26:.1f}%)",
                "Perubahan": f"{'\u25b2' if delta > 0 else '\u25bc' if delta < 0 else '='} {abs(delta):,}",
                "\u0394 % Poin": f"{delta_poin:+.1f}",
                "_delta": delta,
                "_dpoin": delta_poin,
            })
        df_delta = pd.DataFrame(delta_rows)
        st.dataframe(
            df_delta.drop(columns=["_delta", "_dpoin"]),
            use_container_width=True,
            hide_index=True,
            column_config={
                "Kategori Vegetasi": st.column_config.TextColumn(width="medium"),
                "2025":             st.column_config.TextColumn(width="medium"),
                "2026":             st.column_config.TextColumn(width="medium"),
                "Perubahan":        st.column_config.TextColumn(width="medium"),
                "\u0394 % Poin":    st.column_config.TextColumn(width="small"),
            },
        )

        # ── Interpretasi otomatis ──────────────────────────────────────
        interp = []
        for row in delta_rows:
            name    = row["Kategori Vegetasi"]
            delta_v = row["_delta"]
            ptp     = row["_dpoin"]
            is_bad  = "Berat" in name
            if abs(delta_v) < 50:
                continue
            if is_bad and delta_v > 0:
                interp.append(("error",
                    f"\u26a0\ufe0f **{name}** di {divisi} **naik {delta_v:,} pohon** "
                    f"({ptp:+.1f}% poin) dari 2025 ke 2026. "
                    "Rekomendasikan audit lapangan — cek pemupukan, hama, dan drainase."))
            elif is_bad and delta_v < 0:
                interp.append(("success",
                    f"\u2705 **{name}** di {divisi} **turun {abs(delta_v):,} pohon** "
                    f"({ptp:+.1f}% poin) — kondisi vegetasi membaik dibanding 2025."))
            elif not is_bad and delta_v > 0:
                interp.append(("success",
                    f"\u2705 **{name}** di {divisi} **naik {delta_v:,} pohon** "
                    f"({ptp:+.1f}% poin) — lebih banyak pohon dalam kondisi vegetasi baik."))

        if interp:
            st.markdown("#### \U0001f4a1 Interpretasi Otomatis")
            for level, msg in interp:
                if level == "error":
                    st.error(msg)
                else:
                    st.success(msg)

        # ── Metrik tren individual ─────────────────────────────────────
        imp   = int(agg.get("count_improved", 0))
        deg   = int(agg.get("count_degraded", 0))
        stab  = int(agg.get("count_stable",   0))
        tot_t = imp + deg + stab
        if tot_t > 0:
            st.markdown("#### \U0001f4c8 Tren Perubahan Individual Pohon (2025 \u2192 2026)")
            mc1, mc2, mc3, mc4 = st.columns(4)
            mc1.metric("Total Pohon Dinilai",  f"{tot_t:,}")
            mc2.metric("\u2b06\ufe0f Kondisi Membaik",  f"{imp:,}",  f"{safe_div(imp,  tot_t):.1f}%")
            mc3.metric("\u2b07\ufe0f Kondisi Menurun",  f"{deg:,}",  f"-{safe_div(deg,  tot_t):.1f}%",
                       delta_color="inverse")
            mc4.metric("\u27a1\ufe0f Stabil",           f"{stab:,}", f"{safe_div(stab, tot_t):.1f}%")


# ══════════════════════════════════════════════════════════════════
# SECTION 3: TREN & HOTSPOT
# ══════════════════════════════════════════════════════════════════
def render_trend_hotspot(data: Dict):
    st.header("🎯 Tren & Blok Prioritas Tindakan")
    st.caption("Analisis perubahan kondisi vegetasi per blok — menggunakan data 2025 & 2026 untuk semua divisi")

    blok_rows = data["blok_summary"]
    if not blok_rows:
        st.info("Data ringkasan blok tidak tersedia.")
        return

    df_blok = pd.DataFrame(blok_rows)
    if "blok" in df_blok.columns:
        df_blok["blok"] = df_blok["blok"].apply(format_blok_display)
        
    num_cols = ["count_improved", "count_degraded", "count_stable", "total_pohon",
                "pohon_ada_2025", "pohon_lengkap",
                "klass26_sangat_berat", "klass26_stres_berat",
                "klass25_sangat_berat", "klass25_stres_berat",
                "avg_delta", "avg_ndre_2025", "avg_ndre_2026"]
    for col in num_cols:
        if col in df_blok.columns:
            df_blok[col] = pd.to_numeric(df_blok[col], errors="coerce").fillna(0)

    divisi_list = sorted(df_blok["divisi"].unique()) if "divisi" in df_blok.columns else ["(semua)"]

    # ── Loop per divisi ──────────────────────────────────────────────
    for divisi_name in divisi_list:
        df_div = df_blok[df_blok["divisi"] == divisi_name] if "divisi" in df_blok.columns else df_blok
        if df_div.empty:
            continue

        imp   = int(df_div["count_improved"].sum())
        deg   = int(df_div["count_degraded"].sum())
        stab  = int(df_div["count_stable"].sum())
        has_trend = (imp + deg + stab) > 0

        # ── Header divisi ──────────────────────────────────────────
        st.markdown("---")
        col_h, col_b = st.columns([4, 1])
        with col_h:
            st.subheader(f"🌴 {divisi_name}")
        with col_b:
            if has_trend:
                st.success("✅ Data 2025 & 2026")
            else:
                st.info("ℹ️ Data 2026 saja")

        # ── Arah Perubahan (hanya jika ada data tren 2025↔2026) ───
        if has_trend:
            total_trend = imp + deg + stab
            st.markdown("#### 📈 Arah Perubahan Kondisi Vegetasi (2025 → 2026)")
            fig_trend = go.Figure(go.Bar(
                x=["⬆️ Membaik", "⬇️ Menurun", "➡️ Stabil"],
                y=[imp, deg, stab],
                marker_color=["#27ae60", "#e74c3c", "#95a5a6"],
                text=[
                    f"{imp:,} ({safe_div(imp,  total_trend):.1f}%)",
                    f"{deg:,} ({safe_div(deg,  total_trend):.1f}%)",
                    f"{stab:,} ({safe_div(stab, total_trend):.1f}%)",
                ],
                textposition="outside",
            ))
            fig_trend.update_layout(
                height=340,
                yaxis_title="Jumlah Pohon",
                title=f"{divisi_name}: {imp:,} membaik · {deg:,} menurun · {stab:,} stabil "
                      f"(dari {total_trend:,} pohon dengan data lengkap)",
                margin=dict(t=55, b=10),
                plot_bgcolor="white",
            )
            st.plotly_chart(fig_trend, use_container_width=True, key=f"trend_{divisi_name}")

            # Interpretasi tren
            if imp > deg:
                st.success(
                    f"✅ **{divisi_name}** tren **positif** — "
                    f"**{imp - deg:,} lebih banyak pohon membaik** dibanding menurun. "
                    "Pertahankan program pemupukan dan perawatan saat ini."
                )
            elif deg > imp:
                st.warning(
                    f"⚠️ **{divisi_name}** — **{deg - imp:,} lebih banyak pohon menurun** "
                    "dibanding membaik. Perlu evaluasi program agronomis di lapangan."
                )
            else:
                st.info(f"ℹ️ **{divisi_name}** kondisi relatif seimbang antara membaik dan menurun.")

            # ── 15 Blok Prioritas Penurunan ────────────────────────
            st.markdown("#### 🔴 15 Blok Prioritas Tindakan (Penurunan Terbesar)")
            df_deg = df_div[df_div["count_degraded"] > 0].nlargest(15, "count_degraded").copy()
            if not df_deg.empty:
                df_deg["pct_deg"] = df_deg.apply(
                    lambda r: safe_div(r["count_degraded"], r["total_pohon"]), axis=1
                )
                df_deg["label"] = df_deg.apply(
                    lambda r: f"{r['blok']} ({int(r['count_degraded']):,} pohon)", axis=1
                )
                colors_deg = [
                    "#c0392b" if p > 30 else "#e67e22" if p > 15 else "#f39c12"
                    for p in df_deg["pct_deg"]
                ]
                fig_deg = go.Figure(go.Bar(
                    x=df_deg["count_degraded"],
                    y=df_deg["label"],
                    orientation="h",
                    marker_color=colors_deg,
                    text=df_deg["pct_deg"].apply(lambda p: f"{p:.1f}%"),
                    textposition="outside",
                ))
                fig_deg.update_layout(
                    title=f"{divisi_name} — Blok Prioritas (🔴 merah = >30% pohon menurun)",
                    height=max(300, len(df_deg) * 30 + 80),
                    xaxis_title="Jumlah Pohon Menurun",
                    yaxis=dict(autorange="reversed"),
                    margin=dict(t=50, b=10, l=180),
                    plot_bgcolor="white",
                )
                st.plotly_chart(fig_deg, use_container_width=True, key=f"deg_{divisi_name}")
            else:
                st.success(f"✅ Tidak ada blok di {divisi_name} yang tercatat mengalami penurunan kondisi.")
        else:
            st.info(
                f"ℹ️ **{divisi_name}**: Data tren tahun-ke-tahun belum tersedia "
                "(data 2025 diperlukan sebagai pembanding). "
                "Hotspot kondisi 2026 tetap ditampilkan di bawah."
            )

        # ── Hotspot Stres Berat per Blok (selalu tampil, 2026) ────
        st.markdown("#### 🌡️ Hotspot Konsentrasi Stres Berat per Blok (2026)")
        df_heat = df_div.copy()
        df_heat["pct_kritis"] = df_heat.apply(
            lambda r: safe_div(
                (r.get("klass26_sangat_berat", 0) or 0) + (r.get("klass26_stres_berat", 0) or 0),
                r.get("total_pohon", 1) or 1,
            ), axis=1
        )
        df_heat = df_heat.nlargest(30, "pct_kritis")
        if not df_heat.empty:
            import plotly.express as px
            fig_heat = px.bar(
                df_heat.sort_values("pct_kritis"),
                x="pct_kritis", y="blok", orientation="h",
                color="pct_kritis",
                color_continuous_scale=["#27ae60", "#f1c40f", "#e67e22", "#c0392b"],
                labels={"pct_kritis": "% Stres Berat+SB", "blok": "Blok"},
                title=f"{divisi_name} — % Pohon Stres Berat & Sangat Berat per Blok (Top 30, kondisi 2026)",
                text=df_heat["pct_kritis"].apply(lambda x: f"{x:.1f}%"),
            )
            fig_heat.update_traces(textposition="outside")
            fig_heat.update_layout(
                height=max(350, min(len(df_heat) * 25 + 80, 700)),
                margin=dict(t=50, b=10, l=100),
                coloraxis_showscale=False,
            )
            st.plotly_chart(fig_heat, use_container_width=True, key=f"heat_{divisi_name}")


# ══════════════════════════════════════════════════════════════════
# SECTION 4: MATRIKS TRANSISI
# ══════════════════════════════════════════════════════════════════
def render_transition_matrix(data: Dict):
    st.header("🔀 Matriks Pergerakan Kategori NDRE (AME II)")
    st.caption("Menunjukkan berapa pohon yang berpindah kategori dari 2025 ke 2026")

    trans = data["transition"]
    if not trans:
        st.info("Data transisi tidak tersedia (view belum dibuat atau data AME II tidak ada 2025).")
        return

    df_t = pd.DataFrame(trans)
    if df_t.empty:
        return

    # Filter AME II saja
    if "divisi" in df_t.columns:
        df_t = df_t[df_t["divisi"] == "AME II"]

    if "klass_2025" not in df_t.columns or "klass_2026" not in df_t.columns:
        st.info("Kolom transisi tidak ditemukan.")
        return

    # Pivot untuk heatmap
    df_pivot = df_t.pivot_table(
        index="klass_2025", columns="klass_2026",
        values="jumlah_pohon", aggfunc="sum", fill_value=0,
    )
    ORDER = ["Stres Sangat Berat", "Stres Berat", "Stres Sedang", "Stres Ringan"]
    idx_order = [x for x in ORDER if x in df_pivot.index]
    col_order = [x for x in ORDER if x in df_pivot.columns]
    if idx_order and col_order:
        df_pivot = df_pivot.reindex(index=idx_order, columns=col_order, fill_value=0)

    fig = px.imshow(
        df_pivot,
        text_auto=True,
        color_continuous_scale=["#ffffff", "#ffeaa7", "#e17055", "#c0392b"],
        labels=dict(x="Kondisi 2026", y="Kondisi 2025", color="Jumlah Pohon"),
        title="Matriks Transisi: Kondisi 2025 → 2026 (AME II)",
    )
    fig.update_layout(height=380, margin=dict(t=50, b=50))
    st.plotly_chart(fig, use_container_width=True)

    # Interpretasi diagonal
    st.markdown("**Cara Membaca:**")
    st.markdown(
        "- **Diagonal (kiri atas → kanan bawah):** pohon yang kondisinya **tidak berubah**  \n"
        "- **Di atas diagonal:** kondisi **memburuk** (bergerak ke kategori lebih parah)  \n"
        "- **Di bawah diagonal:** kondisi **membaik** (bergerak ke kategori lebih ringan)"
    )


# ══════════════════════════════════════════════════════════════════
# SECTION 5: ANOMALI KOORDINAT
# ══════════════════════════════════════════════════════════════════
def render_anomaly_section(data: Dict):
    st.header("⚠️ Anomali Data: Pohon Tanpa Nomor Identifikasi")

    anomaly = data["anomaly"]
    if not anomaly:
        st.success("✅ Tidak ada anomali koordinat yang perlu ditangani.")
        return

    df_a = pd.DataFrame(anomaly)
    if "blok" in df_a.columns:
        df_a["blok"] = df_a["blok"].apply(format_blok_display)
        
    total_anom = len(df_a)

    # ── Ringkasan Metrik ──────────────────────────────────────────
    col1, col2, col3, col4 = st.columns(4)
    by_div = df_a.groupby("divisi").size().to_dict() if "divisi" in df_a.columns else {}
    pending = (df_a["review_status"] == "PENDING").sum() if "review_status" in df_a.columns else total_anom
    missing_x = df_a["reason_codes"].apply(
        lambda rc: "MISSING_X" in (rc or [])
    ).sum() if "reason_codes" in df_a.columns else 0

    col1.metric("Total Anomali", f"{total_anom:,}", help="Pohon tanpa nomor identifikasi (n_pokok kosong)")
    col2.metric("AME II", f"{by_div.get('AME II', 0):,}")
    col3.metric("AME IV", f"{by_div.get('AME IV', 0):,}")
    col4.metric("Belum Diselesaikan", f"{pending:,}", delta_color="inverse")

    st.error(
        f"🚨 **{total_anom} titik koordinat pohon** terdeteksi drone tetapi **tidak memiliki nomor pohon (n_pokok)**. "
        f"Data pohon-pohon ini **tidak dapat diintegrasikan** ke analisis NDRE hingga diverifikasi di lapangan. "
        f"Semua {pending} anomali masih berstatus **PENDING** — belum ada yang diselesaikan."
    )

    # ── Breakdown per Blok ────────────────────────────────────────
    st.subheader("Blok dengan Anomali Terbanyak")
    if "blok" in df_a.columns and "divisi" in df_a.columns:
        df_blok_a = df_a.groupby(["divisi", "blok"]).size().reset_index(name="jumlah")
        df_blok_a = df_blok_a.sort_values("jumlah", ascending=False).head(20)
        df_blok_a["label"] = df_blok_a["divisi"] + " — " + df_blok_a["blok"]
        fig_anom = px.bar(
            df_blok_a, x="jumlah", y="label", orientation="h",
            color="divisi",
            color_discrete_map={"AME II": "#3498db", "AME IV": "#e74c3c"},
            labels={"jumlah": "Jumlah Anomali", "label": "Blok"},
            title="Top Blok dengan Pohon Tanpa Nomor Identifikasi",
            text="jumlah",
        )
        fig_anom.update_traces(textposition="outside")
        fig_anom.update_layout(
            height=max(300, len(df_blok_a) * 28 + 80),
            yaxis=dict(autorange="reversed"),
            margin=dict(t=50, b=10, l=160),
        )
        st.plotly_chart(fig_anom, use_container_width=True)

    # ── Tabel Detail & Download ───────────────────────────────────
    st.subheader("Daftar Lengkap Anomali (untuk Tim Lapangan)")
    cols_show = [c for c in ["dataset_tag", "divisi", "blok", "n_baris_raw",
                             "n_pokok_raw", "reason_codes", "review_status",
                             "anomaly_point"] if c in df_a.columns]
    df_display = df_a[cols_show].copy()
    if "reason_codes" in df_display.columns:
        df_display["reason_codes"] = df_display["reason_codes"].apply(
            lambda x: ", ".join(x) if isinstance(x, list) else str(x or "")
        )
    st.dataframe(df_display, use_container_width=True, height=300)

    # Download
    col_dl1, col_dl2 = st.columns(2)
    with col_dl1:
        csv_bytes = df_display.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            "📥 Unduh CSV Anomali (untuk lapangan)",
            data=csv_bytes,
            file_name="anomali_koordinat_npokok_kosong.csv",
            mime="text/csv",
            use_container_width=True,
        )
    with col_dl2:
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df_display.to_excel(writer, index=False, sheet_name="Anomali Koordinat")
        st.download_button(
            "📊 Unduh Excel Anomali",
            data=buf.getvalue(),
            file_name="anomali_koordinat_npokok_kosong.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )


# ══════════════════════════════════════════════════════════════════
# SECTION 6: DRILL-DOWN BLOK
# ══════════════════════════════════════════════════════════════════
def render_blok_drilldown(data: Dict):
    st.header("🔍 Detail per Blok")

    blok_rows = data["blok_summary"]
    if not blok_rows:
        st.info("Data tidak tersedia.")
        return

    df = pd.DataFrame(blok_rows)
    if "blok" in df.columns:
        df["blok"] = df["blok"].apply(format_blok_display)
        
    num_cols = ["total_pohon", "count_improved", "count_degraded", "count_stable",
                "count_no_delta", "klass26_sangat_berat", "klass26_stres_berat",
                "klass26_sedang", "klass26_ringan", "orphan_no_link",
                "avg_ndre_2025", "avg_ndre_2026", "avg_delta"]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)

    if "total_pohon" in df.columns and "klass26_stres_berat" in df.columns:
        df["% Stres Berat+SB"] = df.apply(
            lambda r: round(safe_div(
                r.get("klass26_sangat_berat", 0) + r.get("klass26_stres_berat", 0),
                r.get("total_pohon", 1),
            ), 1), axis=1
        )

    col_rename = {
        "divisi": "Divisi", "blok": "Blok", "total_pohon": "Total Pohon",
        "count_improved": "Membaik", "count_degraded": "Menurun",
        "count_stable": "Stabil", "count_no_delta": "Tanpa Delta",
        "klass26_sangat_berat": "SB 26", "klass26_stres_berat": "Stres Berat 26",
        "klass26_sedang": "Sedang 26", "klass26_ringan": "Ringan 26",
        "avg_ndre_2025": "Avg NDRE 2025", "avg_ndre_2026": "Avg NDRE 2026",
        "avg_delta": "Avg Delta", "orphan_no_link": "Orphan",
        "% Stres Berat+SB": "% Kritis",
    }
    show_cols = [c for c in col_rename if c in df.columns]
    df_show = df[show_cols].rename(columns=col_rename)

    # Sort by % Kritis
    if "% Kritis" in df_show.columns:
        df_show = df_show.sort_values("% Kritis", ascending=False)

    st.dataframe(
        df_show,
        use_container_width=True,
        height=400,
        column_config={
            "Avg NDRE 2025": st.column_config.NumberColumn(format="%.4f"),
            "Avg NDRE 2026": st.column_config.NumberColumn(format="%.4f"),
            "Avg Delta": st.column_config.NumberColumn(format="%.4f"),
            "% Kritis": st.column_config.ProgressColumn(
                min_value=0, max_value=100, format="%.1f%%"
            ),
        },
    )

    # Download
    buf_xls = io.BytesIO()
    with pd.ExcelWriter(buf_xls, engine="openpyxl") as writer:
        df_show.to_excel(writer, index=False, sheet_name="Detail Blok NDRE")
    st.download_button(
        "📊 Unduh Tabel Detail Blok (Excel)",
        data=buf_xls.getvalue(),
        file_name="detail_blok_ndre_2026.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


# ══════════════════════════════════════════════════════════════════
# SECTION 7: REKOMENDASI
# ══════════════════════════════════════════════════════════════════
def render_recommendations(data: Dict):
    st.header("💡 Rekomendasi Tindak Lanjut")

    div_rows = data["divisi_summary"]
    blok_rows = data["blok_summary"]
    anomaly   = data["anomaly"]

    if not div_rows and not blok_rows:
        st.info("Tidak ada data untuk membuat rekomendasi.")
        return

    recs = []

    # Analisis per divisi
    divisi_groups = defaultdict(list)
    for r in div_rows:
        divisi_groups[r.get("divisi", "?")].append(r)

    for divisi, rows in divisi_groups.items():
        agg = aggregate_divisi(rows)
        total = agg.get("total_pohon", 0) or 1
        pct_sb = safe_div(agg.get("klass26_sangat_berat", 0), total)
        pct_b  = safe_div(agg.get("klass26_stres_berat", 0), total)
        pct_kritis = pct_sb + pct_b

        if pct_sb > 5:
            recs.append(("🔴 SEGERA", divisi,
                f"{pct_sb:.1f}% pohon di {divisi} Stres Sangat Berat. "
                "Lakukan inspeksi lapangan segera — cek gejala serangan penyakit (Ganoderma), "
                "keracunan tanah, atau kekeringan ekstrem."))
        elif pct_kritis > 25:
            recs.append(("🟠 PENTING", divisi,
                f"{pct_kritis:.1f}% pohon di {divisi} dalam kondisi stres berat. "
                "Rekomendasikan pengecekan kebutuhan pupuk (N, Mg, K) dan kondisi drainase blok."))

        if divisi == "AME II":
            deg = agg.get("count_degraded", 0)
            imp = agg.get("count_improved", 0)
            if deg > imp:
                recs.append(("🟡 PERHATIAN", divisi,
                    f"AME II: {deg:,} pohon menurun vs {imp:,} membaik. "
                    "Evaluasi efektivitas pemupukan terakhir dan jadwal aplikasi berikutnya."))

    # Analisis blok terpanas
    if blok_rows:
        df_b = pd.DataFrame(blok_rows)
        for c in ["count_degraded", "total_pohon", "klass26_sangat_berat", "klass26_stres_berat"]:
            if c in df_b.columns:
                df_b[c] = pd.to_numeric(df_b[c], errors="coerce").fillna(0)
        if "count_degraded" in df_b.columns:
            top3 = df_b.nlargest(3, "count_degraded")
            for _, row in top3.iterrows():
                if row.get("count_degraded", 0) > 100:
                    recs.append(("🔴 SEGERA", f"{row.get('divisi','')} Blok {row.get('blok','')}",
                        f"Blok {row.get('blok','')} ({row.get('divisi','')}) memiliki "
                        f"{int(row.get('count_degraded',0)):,} pohon yang kondisinya menurun. "
                        "Prioritaskan kunjungan lapangan mandor ke blok ini minggu ini."))

    # Anomali
    if anomaly:
        total_anom = len(anomaly)
        recs.append(("🟡 PERHATIAN", "Data Koordinat",
            f"{total_anom} pohon terdeteksi drone tanpa nomor identifikasi (n_pokok kosong). "
            "Unduh daftar anomali dan tugaskan surveyor untuk verifikasi & pengisian data di lapangan."))

    # AME IV
    if "AME IV" in divisi_groups:
        recs.append(("ℹ️ INFO", "AME IV",
            "AME IV hanya memiliki data 2026, belum ada data 2025. "
            "Simpan data penerbangan 2026 ini sebagai baseline untuk perbandingan tahun depan."))

    # Tampilkan rekomendasi
    priority_order = {"🔴 SEGERA": 0, "🟠 PENTING": 1, "🟡 PERHATIAN": 2, "ℹ️ INFO": 3}
    recs.sort(key=lambda x: priority_order.get(x[0], 9))

    for priority, subject, message in recs:
        if "SEGERA" in priority:
            st.error(f"**{priority} — {subject}**\n\n{message}")
        elif "PENTING" in priority:
            st.warning(f"**{priority} — {subject}**\n\n{message}")
        elif "PERHATIAN" in priority:
            st.info(f"**{priority} — {subject}**\n\n{message}")
        else:
            st.info(f"**{priority} — {subject}**\n\n{message}")


# ══════════════════════════════════════════════════════════════════
# SECTION 8: TOP EKSTRIM (LEADERBOARD)
# ══════════════════════════════════════════════════════════════════
def render_top_ekstrim_tab(data: Dict):
    st.header("🏆 Peringkat Blok Perubahan Ekstrim (Top 10)")
    st.caption("Menampilkan Top 10 blok untuk penurunan dan peningkatan per divisi (AME II & AME IV) jika histori 2025 tersedia.")

    blok_rows = data.get("blok_summary", [])
    if not blok_rows:
        st.info("Data ringkasan blok belum tersedia.")
        return

    # Histori valid = punya pasangan data 2025-2026 di level pohon
    valid_bloks = [b for b in blok_rows if (b.get("pohon_lengkap", 0) or 0) > 0]
    if not valid_bloks:
        st.warning("⚠️ Tidak ada histori NDRE 2025-2026 untuk dihitung Top 10 Ekstrim.")
        return

    df_rank = pd.DataFrame(valid_bloks)

    num_cols = [
        "count_degraded", "count_improved", "avg_delta",
        "klass25_sangat_berat", "klass25_stres_berat", "klass25_sedang", "klass25_ringan",
        "klass26_sangat_berat", "klass26_stres_berat", "klass26_sedang", "klass26_ringan",
    ]
    for c in num_cols:
        if c in df_rank.columns:
            df_rank[c] = pd.to_numeric(df_rank[c], errors="coerce").fillna(0)

    df_rank["label_blok"] = df_rank.apply(
        lambda r: f"Blok {format_blok_display(r.get('blok', ''))} ({r.get('divisi', '')})", axis=1
    )

    # Hitung populasi Kritis (Sangat Berat + Berat) dan Sehat (Sedang + Ringan)
    df_rank["kritis_25"] = df_rank.get("klass25_sangat_berat", 0) + df_rank.get("klass25_stres_berat", 0)
    df_rank["kritis_26"] = df_rank.get("klass26_sangat_berat", 0) + df_rank.get("klass26_stres_berat", 0)
    df_rank["sehat_25"] = df_rank.get("klass25_ringan", 0) + df_rank.get("klass25_sedang", 0)
    df_rank["sehat_26"] = df_rank.get("klass26_ringan", 0) + df_rank.get("klass26_sedang", 0)

    def get_bad_text(r):
        k25 = int(r.get("kritis_25", 0))
        k26 = int(r.get("kritis_26", 0))
        pct = ((k26 - k25) / k25 * 100) if k25 > 0 else 0
        count_deg = int(r.get("count_degraded", 0))
        return f"Naik {pct:.1f}% ({k25}→{k26}) | +{count_deg} pohon"

    def get_good_text(r):
        s25 = int(r.get("sehat_25", 0))
        s26 = int(r.get("sehat_26", 0))
        pct = ((s26 - s25) / s25 * 100) if s25 > 0 else 0
        count_imp = int(r.get("count_improved", 0))
        return f"Naik {pct:.1f}% ({s25}→{s26}) | +{count_imp} pohon"

    df_rank["teks_bad"] = df_rank.apply(get_bad_text, axis=1)
    df_rank["teks_good"] = df_rank.apply(get_good_text, axis=1)

    # Fallback ranking berbasis perubahan komposisi kelas (untuk kasus delta individual belum terisi)
    df_rank["worsen_by_class"] = (df_rank["kritis_26"] - df_rank["kritis_25"]).clip(lower=0)
    df_rank["improve_by_class"] = (df_rank["sehat_26"] - df_rank["sehat_25"]).clip(lower=0)

    divisi_list = sorted(df_rank["divisi"].dropna().unique().tolist()) if "divisi" in df_rank.columns else ["(semua)"]

    for divisi_name in divisi_list:
        df_div = df_rank[df_rank["divisi"] == divisi_name] if "divisi" in df_rank.columns else df_rank
        if df_div.empty:
            continue

        st.markdown("---")
        st.subheader(f"🌴 {divisi_name}")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("<h5 style='color:#c0392b; text-align:center;'>🔻 Top 10 Penurunan Paling Kritis</h5>", unsafe_allow_html=True)
            has_delta_degraded = (df_div["count_degraded"] > 0).any()
            metric_bad = "count_degraded" if has_delta_degraded else "worsen_by_class"
            top_bad = df_div.nlargest(10, metric_bad).sort_values(metric_bad, ascending=True)
            top_bad = top_bad[top_bad[metric_bad] > 0]

            if not top_bad.empty:
                fig_bad = px.bar(
                    top_bad,
                    x=metric_bad,
                    y="label_blok",
                    orientation="h",
                    text="teks_bad",
                    color=metric_bad,
                    color_continuous_scale=[[0, "#f5b7b1"], [1, "#922b21"]],
                    labels={
                        "count_degraded": "Jumlah Pohon Menurun",
                        "worsen_by_class": "Kenaikan Pohon Kritis (Berbasis Kelas)",
                        "label_blok": "Nama Blok",
                    },
                )
                fig_bad.update_layout(
                    height=450,
                    showlegend=False,
                    coloraxis_showscale=False,
                    xaxis_title="Total Pohon Kritis (Merosot Kelas)",
                    yaxis_title="",
                    plot_bgcolor="white",
                    margin=dict(r=20, l=120, b=0, t=10),
                )
                fig_bad.update_traces(
                    textposition="inside",
                    insidetextanchor="middle",
                    textfont=dict(color="white", size=11),
                )
                st.plotly_chart(fig_bad, use_container_width=True, key=f"top_10_bad_chart_{divisi_name}")
                if metric_bad == "worsen_by_class":
                    st.caption("Mode ranking fallback aktif: berdasarkan kenaikan total kelas kritis 2025→2026 per blok.")
            else:
                st.success("Tidak ada penurunan ekstrem pada divisi ini.")

        with col2:
            st.markdown("<h5 style='color:#27ae60; text-align:center;'>🌟 Top 10 Peningkatan (Membaik)</h5>", unsafe_allow_html=True)
            has_delta_improved = (df_div["count_improved"] > 0).any()
            metric_good = "count_improved" if has_delta_improved else "improve_by_class"
            top_good = df_div.nlargest(10, metric_good).sort_values(metric_good, ascending=True)
            top_good = top_good[top_good[metric_good] > 0]

            if not top_good.empty:
                fig_good = px.bar(
                    top_good,
                    x=metric_good,
                    y="label_blok",
                    orientation="h",
                    text="teks_good",
                    color=metric_good,
                    color_continuous_scale=[[0, "#abebc6"], [1, "#1d8348"]],
                    labels={
                        "count_improved": "Jumlah Pohon Membaik",
                        "improve_by_class": "Kenaikan Pohon Sehat (Berbasis Kelas)",
                        "label_blok": "Nama Blok",
                    },
                )
                fig_good.update_layout(
                    height=450,
                    showlegend=False,
                    coloraxis_showscale=False,
                    xaxis_title="Total Pohon Membaik (Naik Kelas)",
                    yaxis_title="",
                    plot_bgcolor="white",
                    margin=dict(r=20, l=120, b=0, t=10),
                )
                fig_good.update_traces(
                    textposition="inside",
                    insidetextanchor="middle",
                    textfont=dict(color="white", size=11),
                )
                st.plotly_chart(fig_good, use_container_width=True, key=f"top_10_good_chart_{divisi_name}")
                if metric_good == "improve_by_class":
                    st.caption("Mode ranking fallback aktif: berdasarkan kenaikan total kelas sehat 2025→2026 per blok.")
            else:
                st.info("Belum ada peningkatan signifikan pada divisi ini.")

    st.caption("Sumber data histori 2025 AME IV di-resolve dari payload sumber (raw CSV JSON) pada pipeline komparasi, sehingga blok AME IV ikut masuk ranking jika data lengkap tersedia.")
                

# ══════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════
def main():
    st.set_page_config(
        page_title="Dashboard NDRE Kebun Sawit | AME II & AME IV",
        page_icon="🌿",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # Custom CSS
    st.markdown("""
    <style>
    .block-container { padding-top: 1.5rem; }
    div[data-testid="metric-container"] {
        background: #f8f9fa;
        border: 1px solid #e9ecef;
        border-radius: 10px;
        padding: 14px 18px;
    }
    /* Stylish Tabs */
    div[data-testid="stTabs"] button[role="tab"] { 
        font-size: 1.05rem; 
        font-weight: 700; 
        padding: 12px 24px;
        background-color: #f1f2f6; 
        border: 1px solid #dfe4ea;
        border-bottom: none;
        border-radius: 8px 8px 0 0;
        margin-right: 6px;
        color: #576574;
        transition: all 0.2s ease;
    }
    div[data-testid="stTabs"] button[role="tab"]:hover {
        background-color: #eafae3;
        color: #27ae60;
    }
    div[data-testid="stTabs"] button[role="tab"][aria-selected="true"] {
        background-color: #2ecc71 !important;
        color: white !important;
        border-color: #2ecc71;
        box-shadow: 0 -4px 10px rgba(46, 204, 113, 0.4);
    }
    </style>
    """, unsafe_allow_html=True)

    # Page Header
    st.markdown("""
    <div style="background:linear-gradient(135deg,#1a472a,#2ecc71);
                padding:20px 28px; border-radius:14px; margin-bottom:20px; color:white;">
        <h1 style="margin:0; font-size:1.8rem;">🌿 Dashboard Pemantauan Vegetasi Drone</h1>
        <p style="margin:6px 0 0 0; opacity:0.9; font-size:1rem;">
            Perbandingan NDRE 2025 vs 2026 · Divisi AME II & AME IV · Data Penerbangan Feb 2026
        </p>
    </div>
    """, unsafe_allow_html=True)

    # Filters (Formerly Sidebar) - Dihilangkan untuk kepraktisan rapat
    # selected_dataset_tag, divisi_filter = render_filters()
    # st.markdown("---")
    
    # Set default ke SEMUA data sesuai preferensi tanpa filter
    selected_dataset_tag = "__ALL__"
    divisi_filter = "SEMUA"

    # Determine dataset tags
    if selected_dataset_tag == "__ALL__":
        tags_tuple = ("NDRE_02_2026", "AME_IV_2026")
    else:
        tags_tuple = (selected_dataset_tag,)

    # Load data dengan spinner
    with st.spinner("⏳ Memuat data dari Supabase…"):
        try:
            data = load_all_data(tags_tuple, divisi_filter)
        except Exception as exc:
            st.error(f"❌ Gagal memuat data: {exc}")
            st.caption("Pastikan SUPABASE_URL dan SUPABASE_KEY sudah benar di file .env")
            return

    # Warning jika view belum dibuat
    if not data["view_ok"]:
        st.warning(
            "⚠️ **SQL Views belum dibuat di Supabase.** Dashboard menggunakan data mentah (lebih lambat). "
            "Jalankan file `sql/create_dashboard_ndre_views.sql` di Supabase SQL Editor untuk performa optimal."
        )

    # Data header
    total_div = sum(r.get("total_pohon", 0) or 0 for r in data["divisi_summary"])
    total_anom = len(data["anomaly"])
    if total_div > 0:
        st.markdown(f"""
        <div style="margin-bottom: 15px; padding: 12px; background: #eafae3; border-left: 5px solid #27ae60; border-radius: 6px;">
            <strong style="color: #1a472a; font-size: 1.05rem;">ℹ️ Informasi Keseluruhan Blok:</strong><br>
            <span style="color: #2c3e50;">
            • Total Tanaman Dewasa Terpantau: <b>{total_div:,} pohon</b><br>
            • Anomali Data Koordinat: <b>{total_anom:,} titik</b>
            </span>
        </div>
        """, unsafe_allow_html=True)

    # Tabs — 5 tab utama
    tab1, tab2, tab5, tab3, tab4 = st.tabs([
        "📅 Tren 2025 vs 2026",
        "🎯 Tren & Hotspot",
        "🏆 Top Ekstrim",
        "⚠️ Anomali Data",
        "🔥 Analisis Cincin Api",
    ])

    with tab1:
        render_divisi_comparison(data)
    with tab2:
        render_trend_hotspot(data)
    with tab5:
        render_top_ekstrim_tab(data)
    with tab3:
        render_anomaly_section(data)
    with tab4:
        render_cincin_api_tab(data, selected_dataset_tag)


if __name__ == "__main__":
    main()

