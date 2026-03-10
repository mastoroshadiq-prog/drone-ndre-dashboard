import pandas as pd
import numpy as np
import streamlit as st
import folium
from streamlit_folium import st_folium
import plotly.express as px
from scipy.spatial import KDTree

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
    
    # 1. Fetch raw comparison 
    raw_rows = fetch_comparison_sample(client, dataset_tags=tags, divisi=sel_div, blok=sel_blok, max_rows=10000)
    # 2. Fetch koordinat
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

def calc_z_score_and_map(df, val_col, suffix, ring1_radius=10.0, ring2_radius=20.0):
    mean_val = df[val_col].mean()
    std_val = df[val_col].std()
    if pd.isna(std_val) or std_val == 0:
        std_val = 1e-6
    
    z_scores = (df[val_col] - mean_val) / std_val
    df[f"z_score_{suffix}"] = z_scores
    
    # Kategori default: Hijau (Sehat)
    df[f"kategori_{suffix}"] = "🟢 HIJAU (SEHAT)"
    df[f"warna_{suffix}"] = "#d1f2eb" # Hijau transparan
    df[f"radius_{suffix}"] = 3
    df[f"fill_opacity_{suffix}"] = 0.5
    
    # Aproksimasi derajat Lat/Lon ke Meter (Ekuator)
    if "x_m" not in df.columns:
        LAT_TO_M = 111320.0
        LON_TO_M = 111320.0 * np.cos(np.radians(df["latitude"].mean()))
        df["x_m"] = df["longitude"] * LON_TO_M
        df["y_m"] = df["latitude"] * LAT_TO_M
    
    # 1. Deteksi Core Ekstrem (Z <= -1.5)
    m_core = z_scores <= -1.5
    df.loc[m_core, [f"kategori_{suffix}", f"warna_{suffix}", f"radius_{suffix}", f"fill_opacity_{suffix}"]] = [
        "🔴 MERAH (INTI)", "#c0392b", 6, 0.9]
    
    core_points = df[m_core][["x_m", "y_m"]].values
    
    # 2. Deteksi Ketetanggaan (Ring 1 & Ring 2)
    if len(core_points) > 0:
        core_tree = KDTree(core_points)
        all_points = df[["x_m", "y_m"]].values
        
        # Cari jarak titik ini ke Core terdekat
        distances, _ = core_tree.query(all_points, k=1)
        
        # Syarat "tertular" (spatial): berada dalam radius X meter DARI core, DAN punya kerentanan (Z <= 0).
        # Jika sangat sehat (Z > 0), tidak dihitung tertular walau dekat.
        m_ring1 = (distances <= ring1_radius) & (~m_core) & (z_scores <= -0.2)
        m_ring2 = (distances <= ring2_radius) & (~m_core) & (~m_ring1) & (z_scores <= 0.0)
        
        df.loc[m_ring1, [f"kategori_{suffix}", f"warna_{suffix}", f"radius_{suffix}", f"fill_opacity_{suffix}"]] = [
            "🟠 ORANYE (CINCIN)", "#e67e22", 5, 0.8]
        df.loc[m_ring2, [f"kategori_{suffix}", f"warna_{suffix}", f"radius_{suffix}", f"fill_opacity_{suffix}"]] = [
            "🟡 KUNING (SUSPECT)", "#f1c40f", 5, 0.7]
    
    return df

def create_folium_map(df, val_col, suffix, year):
    center_lat = df["latitude"].mean()
    center_lon = df["longitude"].mean()
    
    m = folium.Map(location=[center_lat, center_lon], zoom_start=17, max_zoom=20)
    folium.TileLayer('CartoDB positron', name="CartoDB Light").add_to(m)

    for _, row in df.iterrows():
        warna = row[f"warna_{suffix}"]
        kategori = row[f"kategori_{suffix}"]
        kat_label = kategori

        tt_html = f'''
            <div style="font-family:sans-serif;min-width:140px;">
                <strong style="color:{warna}">{kat_label}</strong><br><br>
                <b>Row:</b> {int(row['n_baris'])} | <b>Tree:</b> {int(row['n_pokok'])}<br>
                <b>NDRE {year}:</b> {row[val_col]:.3f}<br>
                <b>Z-Score:</b> {row[f"z_score_{suffix}"]:.2f}<br>
            </div>
        '''
        
        folium.CircleMarker(
            location=[row["latitude"], row["longitude"]],
            radius=row[f"radius_{suffix}"],
            color=warna if kategori != "🟢 HIJAU (SEHAT)" else "#27ae60",
            weight=1 if kategori == "🟢 HIJAU (SEHAT)" else 2,
            fill=True,
            fill_color=warna,
            fill_opacity=row[f"fill_opacity_{suffix}"],
            tooltip=folium.Tooltip(tt_html)
        ).add_to(m)
        
    return m

def render_cincin_api_tab(data: dict, selected_dataset_tag: str):
    st.header("🔥 Cincin Api (Ring of Fire) - Perbandingan 2025 vs 2026")
    st.caption("Deteksi pergeseran pusat stres vegetasi spasial berbasis Z-Score NDRE untuk Blok yang sama.")

    blok_rows = data.get("blok_summary", [])
    if not blok_rows:
        st.info("Data ringkasan blok tidak tersedia. Harap pastikan koneksi database baik.")
        return

    # Cari blok yang memiliki data histori (2025) untuk perbandingan
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

    with st.spinner(f"🔥 Menyusun perbandingan Spasial {sel_div} - {sel_blok} ..."):
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
        
        # Merge via n_baris + n_pokok
        df_coord["n_baris"] = pd.to_numeric(df_coord["n_baris"], errors='coerce')
        df_coord["n_pokok"] = pd.to_numeric(df_coord["n_pokok"], errors='coerce')
        df_ndre["n_baris"] = pd.to_numeric(df_ndre["n_baris"], errors='coerce')
        df_ndre["n_pokok"] = pd.to_numeric(df_ndre["n_pokok"], errors='coerce')
        
        df = pd.merge(df_ndre, df_coord, on=["n_baris", "n_pokok"], how="inner")
        df = df.dropna(subset=["latitude", "longitude"])
        
        if df.empty:
            st.error("❌ Gagal menyatukan nilai NDRE dengan Koordinat GIS. Periksa anomali ID Pohon.")
            return
            
        # Hitung Z-Score + warna untuk masing-masing tahun
        df = calc_z_score_and_map(df, "val_2025", "25")
        df = calc_z_score_and_map(df, "val_2026", "26")
        
        # UI Kiri dan Kanan
        col_map1, col_map2 = st.columns(2)
        
        with col_map1:
            st.markdown(f"<h5 style='text-align: center;'>Penerbangan 2025</h5>", unsafe_allow_html=True)
            st.markdown(get_stats_html(df, "25"), unsafe_allow_html=True)
            map_25 = create_folium_map(df, "val_2025", "25", "2025")
            st_folium(map_25, height=550, use_container_width=True, key="map25", returned_objects=[])
            
        with col_map2:
            st.markdown(f"<h5 style='text-align: center;'>Penerbangan 2026</h5>", unsafe_allow_html=True)
            st.markdown(get_stats_html(df, "26"), unsafe_allow_html=True)
            map_26 = create_folium_map(df, "val_2026", "26", "2026")
            st_folium(map_26, height=550, use_container_width=True, key="map26", returned_objects=[])
        
        st.markdown("<br>", unsafe_allow_html=True)
        st.caption(f"**Insight:** Menampilkan pergerakan Cincin Api antar tahun di blok {sel_div} - {sel_blok} ({len(df):,} pohon terdeteksi). "
                   "Kategori dihitung berbasis *Z-Score* dari NDRE.")
