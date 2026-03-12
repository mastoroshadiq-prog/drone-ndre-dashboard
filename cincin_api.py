import pandas as pd
import numpy as np
import streamlit as st
import plotly.graph_objects as go
import re

from supabase_helper import get_supabase_client, fetch_comparison_sample, fetch_koordinat_blok

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
        return f"{charPart}{numPart:03d}{suffixPart}"
    return blok

def safe_float(v):
    try:
        return float(v)
    except:
        return np.nan

def format_rupiah(v):
    try:
        return f"Rp {float(v):,.0f}".replace(",", ".")
    except:
        return "Rp 0"

def get_ndre25(r):
    v25 = r.get("ndre_1_25")
    if pd.notna(v25): 
        return safe_float(v25)
    
    raw = r.get("raw_csv_json") or {}
    if isinstance(raw, dict):
        src = raw.get("source_2026", {})
        if isinstance(src, dict) and "ndre125" in src:
            val = src.get("ndre125")
            if val and str(val).strip() not in ("-", "", "nan"):
                return safe_float(val)
    return np.nan

def extract_status(r):
    raw = r.get("raw_csv_json") or {}
    if isinstance(raw, dict):
        for year in ["source_2026", "source_2025"]:
            src = raw.get(year, {})
            if isinstance(src, dict):
                ket = str(src.get("ket", "")).strip()
                if ket and ket not in ("-", "nan", ""):
                    ket_lower = ket.lower()
                    is_mati = "mati" in ket_lower or "kosong" in ket_lower
                    is_tbm = "tbm" in ket_lower
                    is_sisip = ("sisip" in ket_lower or is_tbm) and not is_mati
                    return pd.Series([is_sisip, is_mati, ket])
    return pd.Series([False, False, "Pokok Utama"])

@st.cache_data(ttl=300, show_spinner=False)
def load_cincin_data(selected_dataset_tag: str, sel_div: str, sel_blok: str):
    client = get_supabase_client()
    tags = [selected_dataset_tag] if selected_dataset_tag != "__ALL__" else None
    
    raw_rows = fetch_comparison_sample(client, dataset_tags=tags, divisi=sel_div, blok=sel_blok, max_rows=10000)
    coord_rows = fetch_koordinat_blok(client, dataset_tags=tags, divisi=sel_div, blok=sel_blok)
    
    return raw_rows, coord_rows

def get_stats_html(df, suffix, trench_cfg=None):
    kat_col = f"kategori_{suffix}"
    core = (df[kat_col] == "🔴 MERAH (INTI)").sum()
    ring1 = (df[kat_col] == "🟠 ORANYE (CINCIN)").sum()
    ring2 = (df[kat_col] == "🟡 KUNING (SUSPECT)").sum()
    sehat = (df[kat_col] == "🟢 HIJAU (SEHAT)").sum()
    sisip_count = int((df["is_sisip"] == True).sum()) if "is_sisip" in df.columns else 0
    mati_count = int((df["is_mati"] == True).sum()) if "is_mati" in df.columns else 0

    if trench_cfg is None:
        trench_cfg = {
            "jarak_tanam_m": 9.0,
            "lebar_parit_m": 1.0,
            "dalam_parit_m": 1.0,
            "biaya_galian_per_m3": 75000.0,
            "biaya_pancang_per_titik": 15000.0,
            "overhead_pct": 10.0,
        }

    html = f"""
    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-bottom: 8px;">
        <div style="background-color: #1e212b; padding: 12px; border-radius: 8px; border-left: 5px solid #c0392b;">
            <div style="color: #c0392b; font-size: 0.75rem; font-weight: 800; margin-bottom: 4px; letter-spacing: 0.5px;">🔴 MERAH (INTI)</div>
            <div style="color: white; font-size: 1.4rem; font-weight: 700; line-height: 1;">{core:,} <span style="font-size: 0.8rem; font-weight: 400; color: #8e9ba9;">pohon</span></div>
        </div>
        <div style="background-color: #1e212b; padding: 12px; border-radius: 8px; border-left: 5px solid #e67e22;">
            <div style="color: #e67e22; font-size: 0.75rem; font-weight: 800; margin-bottom: 4px; letter-spacing: 0.5px;">🟠 ORANYE (CINCIN)</div>
            <div style="color: white; font-size: 1.4rem; font-weight: 700; line-height: 1;">{ring1:,} <span style="font-size: 0.8rem; font-weight: 400; color: #8e9ba9;">pohon</span></div>
        </div>
        <div style="background-color: #1e212b; padding: 12px; border-radius: 8px; border-left: 5px solid #f1c40f;">
            <div style="color: #f1c40f; font-size: 0.75rem; font-weight: 800; margin-bottom: 4px; letter-spacing: 0.5px;">🟡 KUNING (SUSPECT)</div>
            <div style="color: white; font-size: 1.4rem; font-weight: 700; line-height: 1;">{ring2:,} <span style="font-size: 0.8rem; font-weight: 400; color: #8e9ba9;">pohon</span></div>
        </div>
        <div style="background-color: #1e212b; padding: 12px; border-radius: 8px; border-left: 5px solid #2ecc71;">
            <div style="color: #2ecc71; font-size: 0.75rem; font-weight: 800; margin-bottom: 4px; letter-spacing: 0.5px;">🟢 HIJAU (SEHAT)</div>
            <div style="color: white; font-size: 1.4rem; font-weight: 700; line-height: 1;">{sehat:,} <span style="font-size: 0.8rem; font-weight: 400; color: #8e9ba9;">pohon</span></div>
        </div>
    </div>
    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-bottom: 8px;">
        <div style="background-color: #1e212b; padding: 12px; border-radius: 8px; border-left: 5px solid #bdc3c7;">
            <div style="color: #bdc3c7; font-size: 0.75rem; font-weight: 800; margin-bottom: 4px; letter-spacing: 0.5px;">⚪ SISIP (TOTAL BLOK)</div>
            <div style="color: white; font-size: 1.2rem; font-weight: 700; line-height: 1;">{sisip_count:,} <span style="font-size: 0.8rem; font-weight: 400; color: #8e9ba9;">pohon</span></div>
        </div>
        <div style="background-color: #1e212b; padding: 12px; border-radius: 8px; border-left: 5px solid #34495e;">
            <div style="color: #95a5a6; font-size: 0.75rem; font-weight: 800; margin-bottom: 4px; letter-spacing: 0.5px;">⚫ MATI/KOSONG (TOTAL BLOK)</div>
            <div style="color: white; font-size: 1.2rem; font-weight: 700; line-height: 1;">{mati_count:,} <span style="font-size: 0.8rem; font-weight: 400; color: #8e9ba9;">pohon</span></div>
        </div>
    </div>
    """
    
    sisip_df = df[df["is_sisip"] == True]
    if not sisip_df.empty:
        sisip_counts = sisip_df["ket_raw"].value_counts()
        sisip_text = "<br>".join([f"• {k}: <b>{v}</b>" for k, v in sisip_counts.items()])
        html += f"""
<div style="background-color: #1e212b; padding: 12px; border-radius: 8px; border-left: 5px solid #bdc3c7; margin-bottom: 8px;">
    <div style="color: #bdc3c7; font-size: 0.75rem; font-weight: 800; margin-bottom: 4px; letter-spacing: 0.5px;">⚪ SISIP (IGNORED)</div>
    <div style="color: white; font-size: 1.2rem; font-weight: 700; line-height: 1.2; margin-bottom: 4px;">
        {len(sisip_df):,} <span style="font-size: 0.8rem; font-weight: 400; color: #8e9ba9;">pohon disisihkan (Masih terlalu muda)</span>
    </div>
    <div style="font-size: 0.8rem; color: #95a5a6; line-height: 1.4;">
        {sisip_text}
    </div>
</div>
"""
        
    mati_df = df[df["is_mati"] == True]
    if not mati_df.empty:
        mati_counts = mati_df["ket_raw"].value_counts()
        mati_text = "<br>".join([f"• {k}: <b>{v}</b>" for k, v in mati_counts.items()])
        html += f"""
<div style="background-color: #1e212b; padding: 12px; border-radius: 8px; border-left: 5px solid #34495e; margin-bottom: 8px;">
    <div style="color: #95a5a6; font-size: 0.75rem; font-weight: 800; margin-bottom: 4px; letter-spacing: 0.5px;">⚫ POHON KOSONG / MATI</div>
    <div style="color: white; font-size: 1.2rem; font-weight: 700; line-height: 1.2; margin-bottom: 4px;">
        {len(mati_df):,} <span style="font-size: 0.8rem; font-weight: 400; color: #8e9ba9;">titik Episentrum Kosong</span>
    </div>
    <div style="font-size: 0.8rem; color: #7f8c8d; line-height: 1.4;">
        {mati_text}
    </div>
</div>
"""

    
    # Parit Isolasi Stats
    if f"parit_{suffix}" in df.columns:
        parit_trees = int(df[f"parit_{suffix}"].sum())
        jarak_tanam_m = float(trench_cfg.get("jarak_tanam_m", 9.0))
        lebar_parit_m = float(trench_cfg.get("lebar_parit_m", 1.0))
        dalam_parit_m = float(trench_cfg.get("dalam_parit_m", 1.0))
        biaya_galian_per_m3 = float(trench_cfg.get("biaya_galian_per_m3", 75000.0))
        biaya_pancang_per_titik = float(trench_cfg.get("biaya_pancang_per_titik", 15000.0))
        overhead_pct = float(trench_cfg.get("overhead_pct", 10.0))

        panjang_parit_m = parit_trees * jarak_tanam_m
        volume_galian_m3 = panjang_parit_m * lebar_parit_m * dalam_parit_m
        biaya_galian = volume_galian_m3 * biaya_galian_per_m3
        biaya_pancang = parit_trees * biaya_pancang_per_titik
        subtotal_biaya = biaya_galian + biaya_pancang
        overhead_biaya = subtotal_biaya * (overhead_pct / 100.0)
        total_anggaran = subtotal_biaya + overhead_biaya

        html += f"""
<div style="background-color: #1e212b; padding: 12px; border-radius: 8px; border: 1.5px dashed #7f8c8d; border-left: 5px solid #95a5a6; margin-bottom: 15px;">
    <div style="color: #bdc3c7; font-size: 0.75rem; font-weight: 800; margin-bottom: 4px; letter-spacing: 0.5px;">⛏️ RENCANA PARIT ISOLASI</div>
    <div style="color: white; font-size: 1.2rem; font-weight: 700; line-height: 1;">
        {parit_trees:,} <span style="font-size: 0.8rem; font-weight: 400; color: #8e9ba9;">pohon pancang perbatasan</span> 
        <span style="color:#7f8c8d; margin: 0 8px;">|</span> 
        <span style="font-size: 0.95rem; color:#f1c40f;">Luas Galian ~ {panjang_parit_m:,} Meter</span>
    </div>
    <div style="margin-top:8px; color:#dfe6e9; font-size:0.82rem; line-height:1.5;">
        • Volume Galian: <b>{volume_galian_m3:,.1f} m³</b> ({lebar_parit_m:.2f}m × {dalam_parit_m:.2f}m × {panjang_parit_m:,.0f}m)<br>
        • Estimasi Biaya Galian: <b>{format_rupiah(biaya_galian)}</b><br>
        • Estimasi Biaya Pancang/Batas: <b>{format_rupiah(biaya_pancang)}</b><br>
        • Overhead ({overhead_pct:.1f}%): <b>{format_rupiah(overhead_biaya)}</b><br>
        • <span style="color:#f1c40f;">Total Estimasi Anggaran: <b>{format_rupiah(total_anggaran)}</b></span>
    </div>
</div>
"""
    return html

def get_hex_neighbors(b, p):
    if b % 2 == 0:
        offsets = [(0, -1), (0, 1), (-1, -1), (-1, 0), (1, -1), (1, 0)]
    else:
        offsets = [(0, -1), (0, 1), (-1, 0), (-1, 1), (1, 0), (1, 1)]
    return [(b + db, p + dp) for db, dp in offsets]

def calc_cincin_api(df, val_col, suffix, threshold=0.15, min_sick_neighbors=3, include_suspect_in_quarantine=True):
    # Pre-map nilai NDRE asli untuk Smoothing
    val_map = {}
    for _, row in df.iterrows():
        b, p = int(row["n_baris"]), int(row["n_pokok"])
        val_map[(b, p)] = row[val_col]

    # 1. Spatial Focal Smoothing (Rata-rata Heksagonal)
    # Menghaluskan noise individual pohon (drone noise, shadow, dll)
    # sehingga cluster penyakit yang sebenarnya (kumpulan) terbentuk solid.
    smoothed_vals = []
    for _, row in df.iterrows():
        b, p = int(row["n_baris"]), int(row["n_pokok"])
        nbs = get_hex_neighbors(b, p)
        # Ambil nilai tetangga yang valid + nilai diri sendiri
        valid_vals = [val_map[nb] for nb in nbs if nb in val_map]
        valid_vals.append(val_map[(b, p)])
        smoothed_vals.append(np.mean(valid_vals))
        
    df[f"smoothed_{suffix}"] = smoothed_vals

    # 2. Hitung Ranking Percentile pada Nilai yang Telah Dihaluskan
    if "is_sisip" in df.columns and "is_mati" in df.columns:
        valid_mask = ~(df["is_sisip"] | df["is_mati"])
    else:
        valid_mask = pd.Series(True, index=df.index)
        
    df.loc[valid_mask, f"pct_{suffix}"] = df.loc[valid_mask, f"smoothed_{suffix}"].rank(pct=True, method='dense')
    df.loc[~valid_mask, f"pct_{suffix}"] = np.nan
    
    # Precompute map is_suspect
    is_suspect_map = {}
    for _, row in df.iterrows():
        b, p = int(row["n_baris"]), int(row["n_pokok"])
        is_sisip = row.get("is_sisip", False)
        is_mati = row.get("is_mati", False)
        
        if is_sisip:
            is_suspect_map[(b, p)] = False
        elif is_mati:
            is_suspect_map[(b, p)] = True
        else:
            is_suspect_map[(b, p)] = row[f"pct_{suffix}"] <= threshold

    # Fase 1: Klasifikasi Core & Suspect
    kategori = []
    merah_coords = set()
    
    for _, row in df.iterrows():
        b, p = int(row["n_baris"]), int(row["n_pokok"])
        is_sisip = row.get("is_sisip", False)
        is_mati = row.get("is_mati", False)
        
        if is_sisip:
            kategori.append("⚪ SISIP (IGNORED)")
        elif is_mati:
            kategori.append("⚫ KOSONG/MATI")
            merah_coords.add((b, p))
        elif is_suspect_map[(b, p)]:
            neighbors = get_hex_neighbors(b, p)
            sick_count = sum(1 for nb in neighbors if is_suspect_map.get(nb, False))
            if sick_count >= min_sick_neighbors:
                kategori.append("🔴 MERAH (INTI)")
                merah_coords.add((b, p))
            else:
                kategori.append("🟡 KUNING (SUSPECT)")
        else:
            kategori.append("🟢 HIJAU (SEHAT)")
            
    df[f"kategori_{suffix}"] = kategori
    
    # Fase 2: Expand Ring (Oranye)
    for _, row in df.iterrows():
        b, p = int(row["n_baris"]), int(row["n_pokok"])
        kat = df.at[row.name, f"kategori_{suffix}"]
        if kat not in ("🔴 MERAH (INTI)", "⚫ KOSONG/MATI", "⚪ SISIP (IGNORED)"):
            neighbors = get_hex_neighbors(b, p)
            if any(nb in merah_coords for nb in neighbors):
                df.at[row.name, f"kategori_{suffix}"] = "🟠 ORANYE (CINCIN)"

    # Fase 3: Rute Parit Isolasi (Trench)
    quarantine_status = {"🔴 MERAH (INTI)", "⚫ KOSONG/MATI", "🟠 ORANYE (CINCIN)"}
    if include_suspect_in_quarantine:
        quarantine_status.add("🟡 KUNING (SUSPECT)")

    infected_coords = set()
    for _, row in df.iterrows():
        kat = df.at[row.name, f"kategori_{suffix}"]
        if kat in quarantine_status:
            infected_coords.add((int(row["n_baris"]), int(row["n_pokok"])))
            
    parit = []
    for _, row in df.iterrows():
        b, p = int(row["n_baris"]), int(row["n_pokok"])
        if (b, p) not in infected_coords:
            neighbors = get_hex_neighbors(b, p)
            if any(nb in infected_coords for nb in neighbors):
                parit.append(True)
            else:
                parit.append(False)
        else:
            parit.append(False)
            
    df[f"parit_{suffix}"] = parit

    return df

def create_plotly_hex_map(df, val_col, suffix, year, include_suspect_in_quarantine=True):
    """Plotting spasial baris-pokok dengan layout mata lima (hex grid shift)"""
    fig = go.Figure()
    
    categories = [
        ("⚪ SISIP (IGNORED)", "#bdc3c7", "#95a5a6", 6),
        ("🟢 HIJAU (SEHAT)", "#eafae3", "#82e0aa", 8),
        ("🟡 KUNING (SUSPECT)", "#f1c40f", "#d68910", 10),
        ("🟠 ORANYE (CINCIN)", "#e67e22", "#ba4a00", 12),
        ("🔴 MERAH (INTI)", "#c0392b", "#7b241c", 14),
        ("⚫ KOSONG/MATI", "#2c3e50", "#1a252f", 12)
    ]
    
    # Terapkan offset heksagonal agar visualnya persis Mata Lima
    x_positions = df["n_pokok"] + (df["n_baris"] % 2) * 0.5
    
    # ⛏️ Gambar Garis Batas Parit Isolasi (Trench Boundary)
    if f"parit_{suffix}" in df.columns:
        m_parit = df[f"parit_{suffix}"] == True
        parit_df = df[m_parit]
        
        if not parit_df.empty:
            # 1. Kumpulkan semua koordinat
            quarantine_status = {"🔴 MERAH (INTI)", "⚫ KOSONG/MATI", "🟠 ORANYE (CINCIN)"}
            if include_suspect_in_quarantine:
                quarantine_status.add("🟡 KUNING (SUSPECT)")

            infected_coords = set()
            for _, row in df.iterrows():
                if row[f"kategori_{suffix}"] in quarantine_status:
                    infected_coords.add((int(row["n_baris"]), int(row["n_pokok"])))
            
            parit_coords = set(zip(parit_df["n_baris"], parit_df["n_pokok"]))
            all_nodes = infected_coords.union(parit_coords)
            
            # 2. Cari semua pola segitiga ketetanggaan (faces of hex grid)
            triangles = set()
            for u in all_nodes:
                neighbors = [n for n in get_hex_neighbors(*u) if n in all_nodes]
                for v in neighbors:
                    v_neighbors = set(get_hex_neighbors(*v))
                    common = set(neighbors).intersection(v_neighbors)
                    for w in common:
                        triplet = tuple(sorted([u, v, w]))
                        triangles.add(triplet)
            
            # 3. Fungsi mencari titik potong persis di tengah antara dua pohon
            def get_mid(u, v):
                xu = u[1] + (u[0] % 2) * 0.5
                yu = u[0]
                xv = v[1] + (v[0] % 2) * 0.5
                yv = v[0]
                return ((xu + xv) / 2, (yu + yv) / 2)
            
            # 4. Tarik garis batas pemisah (separating segments)
            trench_x, trench_y = [], []
            for u, v, w in triangles:
                nodes = [u, v, w]
                p_nodes = [n for n in nodes if n in parit_coords]
                i_nodes = [n for n in nodes if n in infected_coords]
                
                if len(p_nodes) == 2 and len(i_nodes) == 1:
                    m1 = get_mid(i_nodes[0], p_nodes[0])
                    m2 = get_mid(i_nodes[0], p_nodes[1])
                    trench_x.extend([m1[0], m2[0], None])
                    trench_y.extend([m1[1], m2[1], None])
                elif len(p_nodes) == 1 and len(i_nodes) == 2:
                    m1 = get_mid(p_nodes[0], i_nodes[0])
                    m2 = get_mid(p_nodes[0], i_nodes[1])
                    trench_x.extend([m1[0], m2[0], None])
                    trench_y.extend([m1[1], m2[1], None])
            
            if trench_x:
                fig.add_trace(go.Scatter(
                    x=trench_x,
                    y=trench_y,
                    mode="lines",
                    line=dict(color="#f39c12", width=4, dash="dash"),
                    name="Garis Isolasi",
                    hoverinfo="skip"
                ))
            
            # Gambar Lingkaran Batas (Halo outline) di sekeliling pohon parit
            x_parit = x_positions[m_parit]
            fig.add_trace(go.Scatter(
                x=x_parit,
                y=parit_df["n_baris"],
                mode="markers",
                marker=dict(
                    size=16,
                    symbol="circle-open",
                    color="#2980b9", # Biru Batas Karantina
                    line=dict(width=3, color="#2980b9")
                ),
                name="Batas Isolasi",
                hoverinfo="skip"
            ))
            
    for cat_name, fill_col, stroke_col, size in categories:
        m_cat = df[f"kategori_{suffix}"] == cat_name
        d_sub = df[m_cat]
        x_sub = x_positions[m_cat]
        
        if d_sub.empty:
            continue
            
        customdata = d_sub[["n_baris", "n_pokok", val_col, f"pct_{suffix}", "ket_raw"]].values
        
        title = cat_name.split(' ')[1] if ' ' in cat_name else cat_name
        hovertemplate = (
            f"<b>{title}</b><br><br>" + 
            "Row: %{customdata[0]:.0f} | Tree: %{customdata[1]:.0f}<br>" +
            "Status: <b>%{customdata[4]}</b><br>" +
            f"NDRE {year}: %{{customdata[2]:.3f}}<br>" +
            "Percentile: %{customdata[3]:.1%}<extra></extra>"
        )

        fig.add_trace(go.Scatter(
            x=x_sub,
            y=d_sub["n_baris"],
            mode="markers",
            marker=dict(
                size=size,
                color=fill_col,
                line=dict(color=stroke_col, width=1.5),
                opacity=0.9
            ),
            name=cat_name,
            customdata=customdata,
            hovertemplate=hovertemplate
        ))

    fig.update_layout(
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis=dict(showgrid=False, zeroline=False, title="", showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, title="", autorange="reversed", showticklabels=False),
        margin=dict(l=0, r=0, t=10, b=0),
        showlegend=False,
        yaxis_scaleanchor="x",  # Keep aspect ratio round naturally
        dragmode="pan",
        hoverlabel=dict(bgcolor="white", font_size=13, font_family="Arial", font_color="#1e212b")
    )
    
    return fig

def render_cincin_api_tab(data: dict, selected_dataset_tag: str):
    st.header("🔥 Cincin Api (Ring of Fire) - Perbandingan 2025 vs 2026")
    st.caption("Klasifikasi berbasis aturan spasial heksagonal (mata lima) dan ranking persentil blok.")
    
    threshold_val = st.slider("Threshold Persentil Sakit", min_value=0.05, max_value=0.30, value=0.15, step=0.01, help="Pohon dengan NDRE pada % terendah dianggap berpotensi MERAH/KUNING.")

    blok_rows = data.get("blok_summary", [])
    if not blok_rows:
        st.info("Data ringkasan blok tidak tersedia. Harap pastikan koneksi database baik.")
        return

    # Cari blok yang memiliki data histori (2025)
    valid_bloks = [b for b in blok_rows if b.get('pohon_lengkap', 0) > 0]
    if not valid_bloks:
        st.warning("⚠️ Tidak ada blok dengan histori NDRE 2025 di dataset ini untuk dianalisis.")
        return

    st.markdown("### 🔍 Analisis Peta Spasial Cincin Api per Blok")

    div_bloks = sorted(list(set([(b['divisi'], b['blok']) for b in valid_bloks])))
    divisi_opts = sorted(list(set([d for d, b in div_bloks])))
    
    col_div, col_blk, _ = st.columns([1, 1, 3])
    with col_div:
        sel_div = st.selectbox("Pilih Divisi", options=divisi_opts, key="cincin_div")
    with col_blk:
        blok_opts = sorted([b for d, b in div_bloks if d == sel_div])
        sel_blok = st.selectbox("Pilih Blok", options=blok_opts, key="cincin_blok", format_func=format_blok_display)

    if not sel_div or not sel_blok:
        return

    # Parameter estimasi ditempatkan tepat di bawah dropdown divisi/blok
    default_params = {
        "jarak_tanam_m": 9.0,
        "lebar_parit_m": 1.0,
        "dalam_parit_m": 1.0,
        "biaya_galian_per_m3": 75000.0,
        "biaya_pancang_per_titik": 15000.0,
        "overhead_pct": 10.0,
        "include_suspect_in_quarantine": True,
    }

    # Nilai yang dipakai kalkulasi (applied) tidak berubah sampai tombol aksi ditekan
    for k, v in default_params.items():
        applied_key = f"applied_{k}"
        draft_key = f"draft_{k}"
        if applied_key not in st.session_state:
            st.session_state[applied_key] = v
        if draft_key not in st.session_state:
            st.session_state[draft_key] = st.session_state[applied_key]

    with st.expander("⚙️ Parameter Dinamis Estimasi Parit Isolasi & Anggaran", expanded=False):
        st.info(
            "ℹ️ **Kenapa parameter ini dibutuhkan?** "
            "Estimasi volume dan anggaran parit sangat dipengaruhi dimensi fisik parit, "
            "jarak antar titik pancang, biaya satuan, dan faktor overhead operasional. "
            "Silakan sesuaikan dengan standar lapangan masing-masing estate/divisi."
        )
        with st.form("form_trench_params"):
            include_suspect_in_quarantine = st.checkbox(
                "🧪 Libatkan pohon KUNING (suspect) ke zona karantina parit",
                key="draft_include_suspect_in_quarantine",
                help="Jika aktif, garis batas parit ditarik di luar area kuning. Jika nonaktif, pohon kuning dikeluarkan dari zona karantina."
            )

            c1, c2, c3 = st.columns(3)
            with c1:
                jarak_tanam_m = st.number_input(
                    "Jarak antar titik (meter)",
                    min_value=1.0,
                    max_value=15.0,
                    step=0.5,
                    key="draft_jarak_tanam_m",
                    help="Dipakai untuk konversi jumlah titik pancang menjadi estimasi panjang parit."
                )
                lebar_parit_m = st.number_input(
                    "Lebar parit (meter)",
                    min_value=0.3,
                    max_value=5.0,
                    step=0.1,
                    key="draft_lebar_parit_m",
                    help="Komponen dimensi volume galian. Semakin lebar, volume dan biaya naik."
                )
            with c2:
                dalam_parit_m = st.number_input(
                    "Kedalaman parit (meter)",
                    min_value=0.3,
                    max_value=5.0,
                    step=0.1,
                    key="draft_dalam_parit_m",
                    help="Komponen dimensi volume galian. Semakin dalam, volume dan biaya naik."
                )
                biaya_galian_per_m3 = st.number_input(
                    "Biaya galian per m³ (Rp)",
                    min_value=0.0,
                    step=5000.0,
                    key="draft_biaya_galian_per_m3",
                    help="Tarif pekerjaan tanah per meter kubik sesuai harga lokal/vendor."
                )
            with c3:
                biaya_pancang_per_titik = st.number_input(
                    "Biaya pancang per titik (Rp)",
                    min_value=0.0,
                    step=1000.0,
                    key="draft_biaya_pancang_per_titik",
                    help="Biaya material + tenaga untuk setiap titik pancang batas parit."
                )
                overhead_pct = st.number_input(
                    "Overhead/contingency (%)",
                    min_value=0.0,
                    max_value=100.0,
                    step=0.5,
                    key="draft_overhead_pct",
                    help="Cadangan biaya operasional tak langsung (transport, supervisi, risiko lapangan)."
                )

            cbtn1, cbtn2 = st.columns([1, 1])
            with cbtn1:
                apply_params = st.form_submit_button("✅ Terapkan Parameter", use_container_width=True)
            with cbtn2:
                reset_params = st.form_submit_button("↺ Reset Draft ke Nilai Aktif", use_container_width=True)

            if apply_params:
                for k in default_params.keys():
                    st.session_state[f"applied_{k}"] = st.session_state[f"draft_{k}"]
                st.success("Parameter berhasil diterapkan. Kalkulasi dan visualisasi diperbarui.")

            if reset_params:
                for k in default_params.keys():
                    st.session_state[f"draft_{k}"] = st.session_state[f"applied_{k}"]
                st.info("Draft dikembalikan ke nilai aktif saat ini.")

        st.caption("Tip: ubah nilai di form lalu klik **Terapkan Parameter** agar perubahan berdampak ke peta dan estimasi.")

    trench_cfg = {
        "jarak_tanam_m": st.session_state["applied_jarak_tanam_m"],
        "lebar_parit_m": st.session_state["applied_lebar_parit_m"],
        "dalam_parit_m": st.session_state["applied_dalam_parit_m"],
        "biaya_galian_per_m3": st.session_state["applied_biaya_galian_per_m3"],
        "biaya_pancang_per_titik": st.session_state["applied_biaya_pancang_per_titik"],
        "overhead_pct": st.session_state["applied_overhead_pct"],
    }

    include_suspect_in_quarantine = st.session_state["applied_include_suspect_in_quarantine"]

    st.markdown("---")

    disp_blok = format_blok_display(sel_blok)

    with st.spinner(f"🔥 Menyusun perbandingan Spasial Heksagonal {sel_div} - {disp_blok} ..."):
        raw_rows, coord_rows = load_cincin_data(selected_dataset_tag, sel_div, sel_blok)
        if not raw_rows or not coord_rows:
            st.error("❌ Data observasi atau koordinat tidak ditemukan untuk blok ini.")
            return
            
        df_ndre = pd.DataFrame(raw_rows)
        df_coord = pd.DataFrame(coord_rows)
        
        df_ndre["val_2025"] = df_ndre.apply(get_ndre25, axis=1)
        df_ndre["val_2026"] = pd.to_numeric(df_ndre["ndre_2_26"], errors='coerce')
        
        # Validasi Pohon
        df_ndre = df_ndre.dropna(subset=["val_2025", "val_2026"])
        if df_ndre.empty:
            st.error("❌ Semua record NDRE di blok ini kehilangan data 2025 atau 2026 yang valid.")
            return
        
        df_coord["n_baris"] = pd.to_numeric(df_coord["n_baris"], errors='coerce')
        df_coord["n_pokok"] = pd.to_numeric(df_coord["n_pokok"], errors='coerce')
        df_ndre["n_baris"] = pd.to_numeric(df_ndre["n_baris"], errors='coerce')
        df_ndre["n_pokok"] = pd.to_numeric(df_ndre["n_pokok"], errors='coerce')
        df_ndre[["is_sisip", "is_mati", "ket_raw"]] = df_ndre.apply(extract_status, axis=1)
        
        # Penyelamatan data Sisip dan Mati: Isi NDRE kosong dengan value dummy 
        # agar tidak terhapus oleh filter dropna, sehingga tetap tampil di peta koordinat
        for col in ["val_2025", "val_2026"]:
            m_isna = df_ndre[col].isna()
            df_ndre.loc[m_isna & df_ndre["is_sisip"], col] = 1.0  # Asumsikan sehat
            df_ndre.loc[m_isna & df_ndre["is_mati"], col] = -1.0  # Asumsikan sakit
            
        df = pd.merge(df_ndre, df_coord, on=["n_baris", "n_pokok"], how="inner")
        # Hanya drop jika masih ada yang Na (Pohon Utamanya kosong NDRE-nya)
        df = df.dropna(subset=["val_2025", "val_2026", "n_baris", "n_pokok"])
        
        if df.empty:
            st.error("❌ Gagal menyatukan nilai NDRE dengan Grid. Periksa anomali ID Pohon.")
            return
            
        # Eksekusi Algoritma Inti Cincin Api Berbasis Mata Lima
        df = calc_cincin_api(df, "val_2025", "25", threshold=threshold_val)
        df = calc_cincin_api(
            df,
            "val_2025",
            "25",
            threshold=threshold_val,
            include_suspect_in_quarantine=include_suspect_in_quarantine,
        )
        df = calc_cincin_api(
            df,
            "val_2026",
            "26",
            threshold=threshold_val,
            include_suspect_in_quarantine=include_suspect_in_quarantine,
        )
        
        col_map1, col_map2 = st.columns(2)
        
        with col_map1:
            st.markdown(f"<h5 style='text-align: center;'>Penerbangan 2025</h5>", unsafe_allow_html=True)
            st.markdown(get_stats_html(df, "25", trench_cfg), unsafe_allow_html=True)
            
            fig_25 = create_plotly_hex_map(
                df,
                "val_2025",
                "25",
                "2025",
                include_suspect_in_quarantine=include_suspect_in_quarantine,
            )
            st.plotly_chart(fig_25, use_container_width=True, key="fig25", config={'scrollZoom': True})
            
        with col_map2:
            st.markdown(f"<h5 style='text-align: center;'>Penerbangan 2026</h5>", unsafe_allow_html=True)
            st.markdown(get_stats_html(df, "26", trench_cfg), unsafe_allow_html=True)
            
            fig_26 = create_plotly_hex_map(
                df,
                "val_2026",
                "26",
                "2026",
                include_suspect_in_quarantine=include_suspect_in_quarantine,
            )
            st.plotly_chart(fig_26, use_container_width=True, key="fig26", config={'scrollZoom': True})
        
        st.markdown("<br>", unsafe_allow_html=True)
        st.caption(f"**Insight:** Menampilkan Cincin Api di blok {sel_div} - {disp_blok} ({len(df):,} pohon terdeteksi). "
                   "Peta di atas adalah **Grid Spasial Heksagonal (Mata Lima)**, mengasumsikan offset +0.5 pada baris ganjil/genap agar susunan pohon saling mengunci secara alami.")
        st.caption("Catatan: angka pancang/perimeter parit dihitung untuk blok yang sedang dipilih (bukan agregat lintas blok).")
