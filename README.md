# Progress Dashboard Pemantauan Vegetasi Drone (NDRE)
**Tanggal:** 9 Maret 2026

---

## Latar Belakang

Dashboard ini dibangun untuk mendukung pengambilan keputusan agronomi berbasis data hasil penerbangan drone di kebun kelapa sawit. Data yang dianalisis adalah indeks NDRE (*Normalized Difference Red Edge*) yang mencerminkan kondisi kesehatan vegetasi pada level pohon individu.

---

## Data yang Digunakan

| Aspek | Keterangan |
|-------|------------|
| **Divisi** | AME II dan AME IV |
| **Periode** | Penerbangan 2025 (baseline) vs Penerbangan Februari 2026 |
| **Total Pohon Terpantau** | **179.474 pohon** |
| **Sumber Data** | Supabase (database cloud) |
| **Anomali Terdeteksi** | 379 titik koordinat pohon tanpa nomor identifikasi (n_pokok) |

---

## Fitur Utama Dashboard

Dashboard dibangun menggunakan **Streamlit** dan terdiri dari **3 tab** yang berfokus pada informasi esensial:

### 1. 📅 Tren 2025 vs 2026
Perbandingan distribusi kondisi vegetasi tahun ke tahun, **per divisi secara terpisah**:
- **Donut chart** Distribusi Vegetasi 2025 → 2026 (side-by-side)
- **Bar chart** perubahan jumlah pohon per kategori (Stres Sangat Berat, Stres Berat, Stres Sedang, Stres Ringan)
- **Tabel delta** perubahan jumlah dan persentase per kategori
- **Interpretasi otomatis** — sistem memberi peringatan jika kategori stres meningkat atau memberikan konfirmasi jika kondisi membaik
- **Metrik tren individual**: berapa pohon yang Membaik / Menurun / Stabil

> AME II: **10.929 pohon membaik** vs **4.846 menurun** dari 94.993 yang terdata lengkap
> AME IV: **72.698 pohon stabil** (data 2025 berhasil diintegrasikan dari arsip JSON)

### 2. 🎯 Tren & Hotspot
Analisis prioritas tindakan lapangan per blok:
- **Chart arah perubahan** kondisi vegetasi (Membaik / Menurun / Stabil) — ditampilkan untuk **kedua divisi**
- **15 Blok Prioritas** dengan penurunan terbesar (color-coded: merah >30%, oranye >15%)
- **Hotspot heatmap** konsentrasi stres berat per blok kondisi 2026 (Top 30 blok)

> AME II: Blok E11 (841 pohon, 31.9% menurun) dan E12 menjadi prioritas utama tindakan lapangan

### 3. ⚠️ Anomali Data
Daftar pohon yang terdeteksi drone namun tidak memiliki nomor identifikasi (n_pokok):
- **379 anomali** total (AME IV: 341 · AME II: 38)
- Distribusi per blok — blok C14 dan A21 di AME IV adalah yang terbanyak
- Data ini belum dapat diintegrasikan ke analisis NDRE hingga diverifikasi di lapangan

---

## Status Teknis

| Komponen | Status |
|----------|--------|
| Database (Supabase) | ✅ Aktif — 3 SQL Views teroptimasi |
| Dashboard Streamlit | ✅ Berjalan lokal (`localhost:8501`) |
| Data AME II (2025 & 2026) | ✅ Lengkap |
| Data AME IV (2025 & 2026) | ✅ Terintegrasi (data 2025 digali dari arsip JSON) |
| Performa loading | ✅ Cepat — views agregasi per blok, bukan scan raw 110k baris |

---

## Kesimpulan & Rekomendasi Tindak Lanjut

1. **AME II** menunjukkan tren **positif** — lebih banyak pohon membaik dibanding menurun. Program pemupukan dan perawatan dapat dipertahankan.
2. **Blok E11 dan E12 (AME II)** perlu kunjungan lapangan prioritas — >25% pohon mengalami penurunan kondisi NDRE.
3. **AME IV** kondisi vegetasi relatif stabil antara 2025 dan 2026. Perlu verifikasi lapangan apakah ini mencerminkan kondisi nyata atau keterbatasan data 2025.
4. **379 pohon anomali** (tanpa n_pokok) perlu diverifikasi surveyor di lapangan, khususnya di blok C14 dan A21 AME IV.
5. Dashboard ini siap digunakan sebagai alat monitoring rutin pasca setiap penerbangan drone berikutnya.
