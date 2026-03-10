import pandas as pd
import numpy as np
import streamlit as st
import folium
from streamlit_folium import st_folium
import plotly.express as px

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

def render_cincin_api_tab(data: dict, selected_dataset_tag: str):
    st.header("🔥 Cincin Api (Ring of Fire) - Level Pohon")
    st.caption("Deteksi penyebaran stres vegetasi spasial berbasis Z-Score NDRE 2025 vs 2026.")

    blok_rows = data.get("blok_summary", [])
    if not blok_rows:
        st.info("Data ringkasan blok tidak tersedia. Harap pastikan koneksi database baik.")
        return

    # Cari blok yang memiliki data histori (2025) untuk perbandingan delta
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

    with st.spinner(f"🔥 Menghitung sebaran Cincin Api {sel_div} - {sel_blok} ..."):
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

        df_ndre["delta"] = df_ndre["val_2026"] - df_ndre["val_2025"]
        
        # Mapping z-score
        mean_delta = df_ndre["delta"].mean()
        std_delta = df_ndre["delta"].std()
        if pd.isna(std_delta) or std_delta == 0:
            std_delta = 1e-6
            
        df_ndre["z_score"] = (df_ndre["delta"] - mean_delta) / std_delta
        
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
            
        # Z-Score ke Cincin Api Categories
        # Core (Sangat Menurun Ekstrem) => Merah
        # Ring 1 (Menurun Signifikan) => Oranye
        # Ring 2 (Menurun Sedikit) => Kuning
        # Normal => Hijau Pucat
        df["kategori"] = "NORMAL"
        df["warna"] = "#d1f2eb" # Hijau transparan
        df["radius"] = 3
        df["fill_opacity"] = 0.5
        
        m_core = df["z_score"] <= -1.5
        m_ring1 = (df["z_score"] > -1.5) & (df["z_score"] <= -1.0)
        m_ring2 = (df["z_score"] > -1.0) & (df["z_score"] <= -0.5)
        
        df.loc[m_core, ["kategori", "warna", "radius", "fill_opacity"]] = ["🔥 CORE (MERAH)", "#c0392b", 5, 0.9]
        df.loc[m_ring1, ["kategori", "warna", "radius", "fill_opacity"]] = ["🟠 RING 1 (ORANYE)", "#e67e22", 5, 0.8]
        df.loc[m_ring2, ["kategori", "warna", "radius", "fill_opacity"]] = ["🟡 RING 2 (KUNING)", "#f1c40f", 4, 0.7]

        # Menghitung statistik visual
        count_core = m_core.sum()
        count_r1 = m_ring1.sum()
        count_r2 = m_ring2.sum()
        count_n = len(df) - count_core - count_r1 - count_r2
        
        col_st1, col_st2, col_st3, col_st4 = st.columns(4)
        col_st1.metric("🔴 Core Episentrum", f"{count_core} pohon", "Z ≤ -1.5", delta_color="inverse")
        col_st2.metric("🟠 Ring 1 (Bahaya)", f"{count_r1} pohon", "-1.5 < Z ≤ -1.0", delta_color="inverse")
        col_st3.metric("🟡 Ring 2 (Rentan)", f"{count_r2} pohon", "-1.0 < Z ≤ -0.5", delta_color="inverse")
        col_st4.metric("🟢 Aman (Normal)", f"{count_n} pohon", "Z > -0.5", delta_color="normal")
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Peta Folium (LeafletJS)
        center_lat = df["latitude"].mean()
        center_lon = df["longitude"].mean()
        
        m = folium.Map(location=[center_lat, center_lon], zoom_start=18, max_zoom=20)
        # Tambah tile base yang cocok dengan tutupan lahan. Jika satelit butuh API key khusus, 
        # kita pakai OpenStreetMap / CartoDB positron sebagai canvas bersih untuk koordinat pohon.
        folium.TileLayer('CartoDB positron', name="CartoDB Light").add_to(m)

        # Plot setiap pohon
        for _, row in df.iterrows():
            tt_html = f'''
                <div style="font-family:sans-serif;min-width:140px;">
                    <strong style="color:{row['warna']}">{row['kategori']}</strong><br><br>
                    <b>Row:</b> {int(row['n_baris'])} | <b>Tree:</b> {int(row['n_pokok'])}<br>
                    <b>NDRE 2026:</b> {row['val_2026']:.3f}<br>
                    <b>Z-Score:</b> {row['z_score']:.2f}<br>
                </div>
            '''
            
            folium.CircleMarker(
                location=[row["latitude"], row["longitude"]],
                radius=row["radius"],
                color=row["warna"] if row["kategori"] != "NORMAL" else "#27ae60",
                weight=1 if row["kategori"] == "NORMAL" else 2,
                fill=True,
                fill_color=row["warna"],
                fill_opacity=row["fill_opacity"],
                tooltip=folium.Tooltip(tt_html)
            ).add_to(m)

        # Render Map in Streamlit
        st_folium(m, width="100%", height=600, returned_objects=[])

        st.caption(f"**Insight:** Menampilkan {len(df):,} titik pohon. "
                   "Cincin Api (Cluster Merah-Oranye) mewakili penyebaran stres NDRE antar pohon bertetangga dalam Blok yang sama. "
                   "Gunakan kontrol +/- di peta untuk zoom in level detil individu.")
