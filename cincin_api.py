import pandas as pd
import numpy as np
import streamlit as st
import plotly.graph_objects as go
import re

from supabase_helper import get_supabase_client, fetch_comparison_sample, fetch_koordinat_blok

def format_blok_display(blok):
    match = re.match(r'^([A-Z])(\d+)([A-Z]?)$', str(blok))
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

@st.cache_data(ttl=300, show_spinner=False)
def load_cincin_data(selected_dataset_tag: str, sel_div: str, sel_blok: str):
    client = get_supabase_client()
    tags = [selected_dataset_tag] if selected_dataset_tag != "__ALL__" else None
    
    raw_rows = fetch_comparison_sample(client, dataset_tags=tags, divisi=sel_div, blok=sel_blok, max_rows=10000)
    coord_rows = fetch_koordinat_blok(client, dataset_tags=tags, divisi=sel_div, blok=sel_blok)
    
    return raw_rows, coord_rows

def get_stats_html(df, suffix):
    kat_col = f"kategori_{suffix}"
    core = (df[kat_col] == "🔴 MERAH (INTI)").sum()
    ring1 = (df[kat_col] == "🟠 ORANYE (CINCIN)").sum()
    ring2 = (df[kat_col] == "🟡 KUNING (SUSPECT)").sum()
    sehat = (df[kat_col] == "🟢 HIJAU (SEHAT)").sum()

    html = f"""
    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px; margin-bottom: 15px;">
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
    """
    return html

def calc_cincin_api(df, val_col, suffix, threshold=0.15, min_sick_neighbors=3):
    # Optimasi pencarian tetangga Heksagonal Mata Lima
    def get_hex_neighbors(b, p):
        if b % 2 == 0:
            offsets = [(0, -1), (0, 1), (-1, -1), (-1, 0), (1, -1), (1, 0)]
        else:
            offsets = [(0, -1), (0, 1), (-1, 0), (-1, 1), (1, 0), (1, 1)]
        return [(b + db, p + dp) for db, dp in offsets]

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
    # NDRE Rendah = Rentan (Sakit). Kita rank persentil berurut ke atas.
    df[f"pct_{suffix}"] = df[f"smoothed_{suffix}"].rank(pct=True, method='dense')
    
    # Precompute map is_suspect
    is_suspect_map = {}
    for _, row in df.iterrows():
        b, p = int(row["n_baris"]), int(row["n_pokok"])
        is_suspect_map[(b, p)] = row[f"pct_{suffix}"] <= threshold

    # Fase 1: Klasifikasi Core & Suspect
    kategori = []
    merah_coords = set()
    
    for _, row in df.iterrows():
        b, p = int(row["n_baris"]), int(row["n_pokok"])
        if is_suspect_map[(b, p)]:
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
    # Ubah yang TETANGGA langsung dari MERAH (tapi bukan MERAH) menjadi ORANYE
    for _, row in df.iterrows():
        b, p = int(row["n_baris"]), int(row["n_pokok"])
        if df.at[row.name, f"kategori_{suffix}"] != "🔴 MERAH (INTI)":
            neighbors = get_hex_neighbors(b, p)
            if any(nb in merah_coords for nb in neighbors):
                df.at[row.name, f"kategori_{suffix}"] = "🟠 ORANYE (CINCIN)"

    return df

def create_plotly_hex_map(df, val_col, suffix, year):
    """Plotting spasial baris-pokok dengan layout mata lima (hex grid shift)"""
    fig = go.Figure()
    
    categories = [
        ("🟢 HIJAU (SEHAT)", "#eafae3", "#82e0aa", 8),
        ("🟡 KUNING (SUSPECT)", "#f1c40f", "#d68910", 11),
        ("🟠 ORANYE (CINCIN)", "#e67e22", "#ba4a00", 14),
        ("🔴 MERAH (INTI)", "#c0392b", "#7b241c", 18)
    ]
    
    # Terapkan offset heksagonal agar visualnya persis Mata Lima
    x_positions = df["n_pokok"] + (df["n_baris"] % 2) * 0.5
    
    for cat_name, fill_col, stroke_col, size in categories:
        m_cat = df[f"kategori_{suffix}"] == cat_name
        d_sub = df[m_cat]
        x_sub = x_positions[m_cat]
        
        if d_sub.empty:
            continue
            
        customdata = d_sub[["n_baris", "n_pokok", val_col, f"pct_{suffix}"]].values
        
        title = cat_name.split(' ')[1]
        hovertemplate = (
            f"<b>{title}</b><br><br>" + 
            "Row: %{customdata[0]:.0f} | Tree: %{customdata[1]:.0f}<br>" +
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
        hoverlabel=dict(bgcolor="white", font_size=13, font_family="Arial")
    )
    
    return fig

def render_cincin_api_tab(data: dict, selected_dataset_tag: str):
    st.header("🔥 Cincin Api (Ring of Fire) - Perbandingan 2025 vs 2026")
    st.caption("Klasifikasi berbasis aturan spasial heksagonal (mata lima) dan ranking persentil blok.")
    
    col_t1, col_t2 = st.columns([1, 4])
    with col_t1:
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
        
        df = pd.merge(df_ndre, df_coord, on=["n_baris", "n_pokok"], how="inner")
        df = df.dropna(subset=["n_baris", "n_pokok"])
        
        if df.empty:
            st.error("❌ Gagal menyatukan nilai NDRE dengan Grid. Periksa anomali ID Pohon.")
            return
            
        # Eksekusi Algoritma Inti Cincin Api Berbasis Mata Lima
        df = calc_cincin_api(df, "val_2025", "25", threshold=threshold_val)
        df = calc_cincin_api(df, "val_2026", "26", threshold=threshold_val)
        
        col_map1, col_map2 = st.columns(2)
        
        with col_map1:
            st.markdown(f"<h5 style='text-align: center;'>Penerbangan 2025</h5>", unsafe_allow_html=True)
            st.markdown(get_stats_html(df, "25"), unsafe_allow_html=True)
            
            fig_25 = create_plotly_hex_map(df, "val_2025", "25", "2025")
            st.plotly_chart(fig_25, use_container_width=True, key="fig25", config={'scrollZoom': True})
            
        with col_map2:
            st.markdown(f"<h5 style='text-align: center;'>Penerbangan 2026</h5>", unsafe_allow_html=True)
            st.markdown(get_stats_html(df, "26"), unsafe_allow_html=True)
            
            fig_26 = create_plotly_hex_map(df, "val_2026", "26", "2026")
            st.plotly_chart(fig_26, use_container_width=True, key="fig26", config={'scrollZoom': True})
        
        st.markdown("<br>", unsafe_allow_html=True)
        st.caption(f"**Insight:** Menampilkan Cincin Api di blok {sel_div} - {disp_blok} ({len(df):,} pohon terdeteksi). "
                   "Peta di atas adalah **Grid Spasial Heksagonal (Mata Lima)**, mengasumsikan offset +0.5 pada baris ganjil/genap agar susunan pohon saling mengunci secara alami.")
