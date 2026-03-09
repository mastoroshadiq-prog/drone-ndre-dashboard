-- ================================================================
-- FIX DASHBOARD NDRE VIEWS — Jalankan script ini di Supabase
-- Tanggal: 2026-03-09
-- Deskripsi:
--   Script ini menghapus semua view lama dan membuat ulang dari awal.
--   vw_ndre_divisi_summary dioptimasi agar tidak timeout (aggregate
--   dari vw_ndre_blok_summary, bukan scan langsung tabel raw 110k baris)
-- ================================================================

-- ── LANGKAH 1: Hapus semua view lama ─────────────────────────────
-- CASCADE memastikan dependensi ikut dihapus
DROP VIEW IF EXISTS vw_ndre_divisi_summary CASCADE;
DROP VIEW IF EXISTS vw_ndre_blok_summary   CASCADE;
DROP VIEW IF EXISTS vw_ndre_transition     CASCADE;


-- ── LANGKAH 2: VIEW 1 — Ringkasan per Blok ───────────────────────
-- Sumber data: kebun_observasi_ndre_comparison
-- AME II : klass_ndre_1_25 terisi langsung di kolom
-- AME IV : klass_ndre_1_25 NULL, data 2025 ada di raw_csv_json->source_2026
CREATE VIEW vw_ndre_blok_summary AS
WITH base AS (
  SELECT
    *,
    -- Resolve klass 2025 (AME II dari kolom, AME IV dari JSON)
    CASE
      WHEN klass_ndre_1_25 IS NOT NULL
        AND klass_ndre_1_25 NOT IN ('-', '(blank)', '')
        THEN klass_ndre_1_25
      WHEN raw_csv_json IS NOT NULL
        AND raw_csv_json->'source_2026'->>'klassndre12025' IS NOT NULL
        AND raw_csv_json->'source_2026'->>'klassndre12025' NOT IN ('-', '(blank)', '')
        THEN raw_csv_json->'source_2026'->>'klassndre12025'
      ELSE NULL
    END AS klass25_resolved,
    -- Resolve ndre 2025 (AME II dari kolom, AME IV dari JSON)
    CASE
      WHEN ndre_1_25 IS NOT NULL THEN ndre_1_25
      WHEN raw_csv_json IS NOT NULL
        AND raw_csv_json->'source_2026'->>'ndre125' IS NOT NULL
        AND raw_csv_json->'source_2026'->>'ndre125' NOT IN ('-', '(blank)', '')
        THEN (raw_csv_json->'source_2026'->>'ndre125')::FLOAT
      ELSE NULL
    END AS ndre25_resolved
  FROM kebun_observasi_ndre_comparison
)
SELECT
  dataset_tag,
  divisi,
  blok,
  COUNT(*)                                                                           AS total_pohon,
  COUNT(*) FILTER (WHERE ndre25_resolved IS NOT NULL)                                AS pohon_ada_2025,
  COUNT(*) FILTER (WHERE ndre_2_26 IS NOT NULL)                                      AS pohon_ada_2026,
  COUNT(*) FILTER (WHERE ndre25_resolved IS NOT NULL AND ndre_2_26 IS NOT NULL)      AS pohon_lengkap,
  ROUND(AVG(ndre25_resolved)::NUMERIC, 6)                                            AS avg_ndre_2025,
  ROUND(AVG(ndre_2_26)::NUMERIC, 6)                                                  AS avg_ndre_2026,
  ROUND(AVG(ndre_2_26 - ndre25_resolved)::NUMERIC, 6)                               AS avg_delta,
  -- Tren individual (threshold 0.05)
  COUNT(*) FILTER (WHERE ndre25_resolved IS NOT NULL AND ndre_2_26 IS NOT NULL
                     AND (ndre_2_26 - ndre25_resolved) >=  0.05)                     AS count_improved,
  COUNT(*) FILTER (WHERE ndre25_resolved IS NOT NULL AND ndre_2_26 IS NOT NULL
                     AND (ndre_2_26 - ndre25_resolved) <= -0.05)                     AS count_degraded,
  COUNT(*) FILTER (WHERE ndre25_resolved IS NOT NULL AND ndre_2_26 IS NOT NULL
                     AND (ndre_2_26 - ndre25_resolved) >  -0.05
                     AND (ndre_2_26 - ndre25_resolved) <   0.05)                     AS count_stable,
  COUNT(*) FILTER (WHERE ndre25_resolved IS NULL OR ndre_2_26 IS NULL)               AS count_no_delta,
  -- Klasifikasi 2026
  COUNT(*) FILTER (WHERE klass_ndre_2_26 ILIKE '%Sangat Berat%')                    AS klass26_sangat_berat,
  COUNT(*) FILTER (WHERE klass_ndre_2_26 ILIKE '%Stres Berat%'
                     AND  klass_ndre_2_26 NOT ILIKE '%Sangat%')                      AS klass26_stres_berat,
  COUNT(*) FILTER (WHERE klass_ndre_2_26 ILIKE '%Sedang%')                          AS klass26_sedang,
  COUNT(*) FILTER (WHERE klass_ndre_2_26 ILIKE '%Ringan%')                          AS klass26_ringan,
  COUNT(*) FILTER (WHERE klass_ndre_2_26 IS NULL
                      OR  klass_ndre_2_26 IN ('-', '(blank)', ''))                   AS klass26_tidak_ada,
  -- Klasifikasi 2025 (resolved)
  COUNT(*) FILTER (WHERE klass25_resolved ILIKE '%Sangat Berat%')                   AS klass25_sangat_berat,
  COUNT(*) FILTER (WHERE klass25_resolved ILIKE '%Stres Berat%'
                     AND  klass25_resolved NOT ILIKE '%Sangat%')                     AS klass25_stres_berat,
  COUNT(*) FILTER (WHERE klass25_resolved ILIKE '%Sedang%')                         AS klass25_sedang,
  COUNT(*) FILTER (WHERE klass25_resolved ILIKE '%Ringan%')                         AS klass25_ringan,
  COUNT(*) FILTER (WHERE id_npokok IS NULL)                                          AS orphan_no_link
FROM base
GROUP BY dataset_tag, divisi, blok;

COMMENT ON VIEW vw_ndre_blok_summary IS
  'Ringkasan NDRE per blok — klass25 resolved dari kolom langsung (AME II) atau raw_csv_json (AME IV)';


-- ── LANGKAH 3: VIEW 2 — Ringkasan per Divisi ─────────────────────
-- DIOPTIMASI: aggregate dari vw_ndre_blok_summary
-- Tidak timeout karena tidak scan 110k baris raw + JSON parsing
CREATE VIEW vw_ndre_divisi_summary AS
SELECT
  dataset_tag,
  divisi,
  SUM(total_pohon)::bigint            AS total_pohon,
  SUM(pohon_lengkap)::bigint          AS pohon_lengkap,
  ROUND(
    (SUM(avg_ndre_2025 * pohon_ada_2025)
     / NULLIF(SUM(pohon_ada_2025), 0))::NUMERIC, 6
  )                                   AS avg_ndre_2025,
  ROUND(
    (SUM(avg_ndre_2026 * pohon_ada_2026)
     / NULLIF(SUM(pohon_ada_2026), 0))::NUMERIC, 6
  )                                   AS avg_ndre_2026,
  ROUND(
    (SUM(avg_delta * pohon_lengkap)
     / NULLIF(SUM(pohon_lengkap), 0))::NUMERIC, 6
  )                                   AS avg_delta,
  -- Tren individual
  SUM(count_improved)::bigint         AS count_improved,
  SUM(count_degraded)::bigint         AS count_degraded,
  SUM(count_stable)::bigint           AS count_stable,
  -- Klasifikasi 2026
  SUM(klass26_sangat_berat)::bigint   AS klass26_sangat_berat,
  SUM(klass26_stres_berat)::bigint    AS klass26_stres_berat,
  SUM(klass26_sedang)::bigint         AS klass26_sedang,
  SUM(klass26_ringan)::bigint         AS klass26_ringan,
  SUM(klass26_tidak_ada)::bigint      AS klass26_tidak_ada,
  -- Klasifikasi 2025 (resolved)
  SUM(klass25_sangat_berat)::bigint   AS klass25_sangat_berat,
  SUM(klass25_stres_berat)::bigint    AS klass25_stres_berat,
  SUM(klass25_sedang)::bigint         AS klass25_sedang,
  SUM(klass25_ringan)::bigint         AS klass25_ringan,
  SUM(orphan_no_link)::bigint         AS orphan_no_link,
  COUNT(DISTINCT blok)::bigint        AS total_blok
FROM vw_ndre_blok_summary
GROUP BY dataset_tag, divisi;

COMMENT ON VIEW vw_ndre_divisi_summary IS
  'Ringkasan NDRE per divisi — aggregate dari vw_ndre_blok_summary (dioptimasi, tidak timeout)';


-- ── LANGKAH 4: VIEW 3 — Matriks Transisi Kelas ───────────────────
-- Menampilkan perpindahan kelas vegetasi pohon dari 2025 ke 2026
CREATE VIEW vw_ndre_transition AS
WITH base AS (
  SELECT
    dataset_tag,
    divisi,
    CASE
      WHEN klass_ndre_1_25 IS NOT NULL
        AND klass_ndre_1_25 NOT IN ('-', '(blank)', '')
        THEN klass_ndre_1_25
      WHEN raw_csv_json IS NOT NULL
        AND raw_csv_json->'source_2026'->>'klassndre12025' IS NOT NULL
        AND raw_csv_json->'source_2026'->>'klassndre12025' NOT IN ('-', '(blank)', '')
        THEN raw_csv_json->'source_2026'->>'klassndre12025'
      ELSE NULL
    END AS klass25_resolved,
    klass_ndre_2_26
  FROM kebun_observasi_ndre_comparison
)
SELECT
  dataset_tag,
  divisi,
  COALESCE(klass25_resolved, 'Tidak Ada Data') AS klass_2025,
  COALESCE(klass_ndre_2_26,  'Tidak Ada Data') AS klass_2026,
  COUNT(*) AS jumlah_pohon
FROM base
WHERE klass25_resolved IS NOT NULL
  AND klass_ndre_2_26  IS NOT NULL
  AND klass_ndre_2_26  NOT IN ('-', '(blank)')
GROUP BY dataset_tag, divisi, klass25_resolved, klass_ndre_2_26
ORDER BY jumlah_pohon DESC;

COMMENT ON VIEW vw_ndre_transition IS
  'Matriks transisi klass NDRE 2025 → 2026 — klass25 resolved untuk AME II & AME IV';


-- ── VERIFIKASI ────────────────────────────────────────────────────
-- Jalankan query ini setelah script di atas untuk memastikan views berhasil:
-- SELECT * FROM vw_ndre_divisi_summary LIMIT 10;
-- SELECT * FROM vw_ndre_blok_summary   LIMIT 5;
-- SELECT * FROM vw_ndre_transition     LIMIT 10;
