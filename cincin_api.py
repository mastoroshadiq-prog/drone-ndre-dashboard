import pandas as pd
import numpy as np
import streamlit as st
import plotly.graph_objects as go

from supabase_helper import get_supabase_client, fetch_comparison_sample, fetch_koordinat_blok

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

def calc_z_score_and_map(df, val_col, suffix):
    mean_val = df[val_col].mean()
    std_val = df[val_col].std()
    if pd.isna(std_val) or std_val == 0:
        std_val = 1e-6
    
    # Gunakan Pure Z-Score asli (tanpa smoothing) karena plot scatter
    # Grid Lurus (n_baris vs n_pokok) secara alami akan memperlihatkan
    # pola spasial tanpa noise lat/lon.
    raw_z_scores = (df[val_col] - mean_val) / std_val
    df[f"z_score_{suffix}"] = raw_z_scores
    
    # 2. Thresholding pure Z-Score
    m_core = raw_z_scores <= -1.5
    m_ring1 = (raw_z_scores > -1.5) & (raw_z_scores <= -1.0)
    m_ring2 = (raw_z_scores > -1.0) & (raw_z_scores <= -0.5)
    
    df[f"kategori_{suffix}"] = "🟢 HIJAU (SEHAT)"
    df.loc[m_ring2, f"kategori_{suffix}"] = "🟡 KUNING (SUSPECT)"
    df.loc[m_ring1, f"kategori_{suffix}"] = "🟠 ORANYE (CINCIN)"
    df.loc[m_core, f"kategori_{suffix}"] = "🔴 MERAH (INTI)"
    
    return df

def create_plotly_grid_map(df, val_col, suffix, year):
    """Memutar koordinat murni dari nomor baris dan pokok agar lurus horizontal sempurna"""
    fig = go.Figure()
    
    # (Nama Kategori, Fill Color, Line Color, Radius Size)
    categories = [
        ("🟢 HIJAU (SEHAT)", "#eafae3", "#82e0aa", 8),
        ("🟡 KUNING (SUSPECT)", "#f1c40f", "#d68910", 11),
        ("🟠 ORANYE (CINCIN)", "#e67e22", "#ba4a00", 14),
        ("🔴 MERAH (INTI)", "#c0392b", "#7b241c", 18)
    ]
    
    for cat_name, fill_col, stroke_col, size in categories:
        d_sub = df[df[f"kategori_{suffix}"] == cat_name]
        if d_sub.empty:
            continue
            
        # customdata array to feed tooltip natively (much faster than looping HTML)
        customdata = d_sub[["n_baris", "n_pokok", val_col, f"z_score_{suffix}"]].values
        
        # Tooltip template
        title = cat_name.split(' ')[1]  # ex: MERAH, ORANYE
        hovertemplate = (
            f"<b>{title}</b><br><br>" + 
            "Row: %{customdata[0]:.0f} | Tree: %{customdata[1]:.0f}<br>" +
            f"NDRE {year}: %{{customdata[2]:.3f}}<br>" +
            "Z-Score: %{customdata[3]:.2f}<extra></extra>"
        )

        fig.add_trace(go.Scatter(
            x=d_sub["n_pokok"],
            y=d_sub["n_baris"],
            mode="markers",
            marker=dict(
                size=size,
                color=fill_col,
                line=dict(color=stroke_col, width=1.8),
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
        yaxis_scaleanchor="x",  # Mengunci grid agar bulat bundar / proporsional
        dragmode="pan",
        hoverlabel=dict(
            bgcolor="white",
            font_size=13,
            font_family="Arial"
        )
    )
    
    return fig

def render_cincin_api_tab(data: dict, selected_dataset_tag: str):
    st.header("🔥 Cincin Api (Ring of Fire) - Perbandingan 2025 vs 2026")
    st.caption("Deteksi pergeseran pusat stres vegetasi berbasis Z-Score NDRE (Grid Spasial murni).")

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
        sel_blok = st.selectbox("Pilih Blok", options=blok_opts, key="cincin_blok")

    if not sel_div or not sel_blok:
        return
        
    st.markdown("---")

    with st.spinner(f"🔥 Menyusun perbandingan Grid Spasial {sel_div} - {sel_blok} ..."):
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
        
        # Merge via n_baris + n_pokok (Ini menjamin Grid XY)
        df_coord["n_baris"] = pd.to_numeric(df_coord["n_baris"], errors='coerce')
        df_coord["n_pokok"] = pd.to_numeric(df_coord["n_pokok"], errors='coerce')
        df_ndre["n_baris"] = pd.to_numeric(df_ndre["n_baris"], errors='coerce')
        df_ndre["n_pokok"] = pd.to_numeric(df_ndre["n_pokok"], errors='coerce')
        
        df = pd.merge(df_ndre, df_coord, on=["n_baris", "n_pokok"], how="inner")
        df = df.dropna(subset=["n_baris", "n_pokok"])
        
        if df.empty:
            st.error("❌ Gagal menyatukan nilai NDRE dengan Grid. Periksa anomali ID Pohon.")
            return
            
        # Hitung Z-Score murni untuk Plotly
        df = calc_z_score_and_map(df, "val_2025", "25")
        df = calc_z_score_and_map(df, "val_2026", "26")
        
        col_map1, col_map2 = st.columns(2)
        
        with col_map1:
            st.markdown(f"<h5 style='text-align: center;'>Penerbangan 2025</h5>", unsafe_allow_html=True)
            st.markdown(get_stats_html(df, "25"), unsafe_allow_html=True)
            
            # Rentangkan di Plotly Scatter Cartesian
            fig_25 = create_plotly_grid_map(df, "val_2025", "25", "2025")
            st.plotly_chart(fig_25, use_container_width=True, key="fig25", config={'scrollZoom': True})
            
        with col_map2:
            st.markdown(f"<h5 style='text-align: center;'>Penerbangan 2026</h5>", unsafe_allow_html=True)
            st.markdown(get_stats_html(df, "26"), unsafe_allow_html=True)
            
            fig_26 = create_plotly_grid_map(df, "val_2026", "26", "2026")
            st.plotly_chart(fig_26, use_container_width=True, key="fig26", config={'scrollZoom': True})
        
        st.markdown("<br>", unsafe_allow_html=True)
        st.caption(f"**Insight:** Menampilkan pergerakan Cincin Api antar tahun di blok {sel_div} - {sel_blok} ({len(df):,} pohon terdeteksi). "
                   "Peta di atas adalah **Grid Spasial Relatif (Nomor Pokok x Nomor Baris)**, memastikan susunan pohon tampak lurus sempurna secara horizontal tanpa distorsi lekukan GPS. "
                   "Gunakan Scroll/Sentuh untuk Zoom dan Geser formasi.")
