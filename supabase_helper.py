import os
from functools import lru_cache
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from postgrest.exceptions import APIError
from supabase import Client, create_client

load_dotenv()

# ─── Singleton client ─────────────────────────────────────────────────────────
_client: Optional[Client] = None


def get_supabase_client() -> Client:
    global _client
    if _client is None:
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")
        if not url or not key:
            raise RuntimeError("SUPABASE_URL / SUPABASE_KEY tidak ditemukan di environment")
        _client = create_client(url, key)
    return _client


# ─── Filter helper ────────────────────────────────────────────────────────────
def _apply_filters(query: Any, filters: Optional[List[Dict[str, Any]]]) -> Any:
    if not filters:
        return query
    for f in filters:
        op  = f.get("op")
        col = f.get("column")
        val = f.get("value")
        if op == "eq":
            query = query.eq(col, val)
        elif op == "neq":
            query = query.neq(col, val)
        elif op == "in":
            query = query.in_(col, val)
        elif op == "gte":
            query = query.gte(col, val)
        elif op == "lte":
            query = query.lte(col, val)
        elif op == "is":
            query = query.is_(col, val)
        elif op == "like":
            query = query.like(col, val)
    return query


# ─── Paginated fetch ──────────────────────────────────────────────────────────
def fetch_paginated(
    client: Client,
    table: str,
    select: str,
    filters: Optional[List[Dict[str, Any]]] = None,
    page_size: int = 2000,
    order_by: Optional[str] = None,
    ascending: bool = True,
    max_rows: Optional[int] = None,
) -> List[Dict[str, Any]]:
    def _is_timeout(exc: Exception) -> bool:
        if isinstance(exc, APIError):
            if getattr(exc, "code", None) == "57014":
                return True
            msg = str(getattr(exc, "message", "") or str(exc)).lower()
            return "statement timeout" in msg or "57014" in msg
        return "statement timeout" in str(exc).lower() or "57014" in str(exc).lower()

    all_rows: List[Dict[str, Any]] = []
    start = 0
    current_page_size = max(200, int(page_size))

    while True:
        if max_rows is not None and len(all_rows) >= max_rows:
            break

        fetch_size = current_page_size
        if max_rows is not None:
            remaining = max_rows - len(all_rows)
            if remaining <= 0:
                break
            fetch_size = min(fetch_size, remaining)

        end = start + fetch_size - 1
        query = client.table(table).select(select)
        query = _apply_filters(query, filters)
        if order_by:
            query = query.order(order_by, desc=not ascending)

        try:
            response = query.range(start, end).execute()
        except Exception as exc:
            if _is_timeout(exc) and current_page_size > 200:
                current_page_size = max(200, current_page_size // 2)
                continue
            raise

        rows = response.data or []
        if not rows:
            break

        all_rows.extend(rows)
        if len(rows) < fetch_size:
            break
        start += len(rows)

    return all_rows


# ─── Aggregated fetchers (menggunakan VIEW di Supabase) ───────────────────────
def fetch_divisi_summary(
    client: Client,
    dataset_tags: Optional[List[str]] = None,
    divisi: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Ambil ringkasan per divisi dari vw_ndre_divisi_summary."""
    filters = []
    if dataset_tags:
        filters.append({"op": "in", "column": "dataset_tag", "value": dataset_tags})
    if divisi and divisi != "SEMUA":
        filters.append({"op": "eq", "column": "divisi", "value": divisi})
        
    try:
        return fetch_paginated(
            client, "vw_ndre_divisi_summary", "*",
            filters=filters, page_size=500, max_rows=500,
        )
    except Exception:
        # Fallback: view belum dibuat, return empty
        return []


def fetch_blok_summary(
    client: Client,
    dataset_tags: Optional[List[str]] = None,
    divisi: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Ambil ringkasan per blok dari vw_ndre_blok_summary."""
    filters = []
    if dataset_tags:
        filters.append({"op": "in", "column": "dataset_tag", "value": dataset_tags})
    if divisi and divisi != "SEMUA":
        filters.append({"op": "eq", "column": "divisi", "value": divisi})
    try:
        return fetch_paginated(
            client, "vw_ndre_blok_summary", "*",
            filters=filters, page_size=2000, max_rows=5000,
        )
    except Exception:
        return []


def fetch_transition_matrix(
    client: Client,
    dataset_tags: Optional[List[str]] = None,
    divisi: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Ambil matriks transisi klasifikasi dari vw_ndre_transition."""
    filters = []
    if dataset_tags:
        filters.append({"op": "in", "column": "dataset_tag", "value": dataset_tags})
    if divisi and divisi != "SEMUA":
        filters.append({"op": "eq", "column": "divisi", "value": divisi})
        
    try:
        return fetch_paginated(
            client, "vw_ndre_transition", "*",
            filters=filters, page_size=1000, max_rows=1000,
        )
    except Exception:
        return []


def fetch_anomaly_koordinat(
    client: Client,
    dataset_tags: Optional[List[str]] = None,
    divisi: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Ambil semua data anomali koordinat."""
    filters = [{"op": "eq", "column": "is_included_dashboard", "value": True}]
    if dataset_tags:
        filters.append({"op": "in", "column": "dataset_tag", "value": dataset_tags})
    if divisi and divisi != "SEMUA":
        filters.append({"op": "eq", "column": "divisi", "value": divisi})
    return fetch_paginated(
        client,
        "kebun_pokok_koordinat_anomali",
        "dataset_tag,divisi,blok,n_baris_raw,n_pokok_raw,reason_codes,review_status,anomaly_point,source_row_number",
        filters=filters,
        page_size=1000,
        max_rows=5000,
    )


def fetch_comparison_sample(
    client: Client,
    dataset_tags: Optional[List[str]] = None,
    divisi: Optional[str] = None,
    blok: Optional[str] = None,
    page_size: int = 2000,
    max_rows: int = 50000,
) -> List[Dict[str, Any]]:
    """Ambil raw comparison rows untuk tabel detail / drill-down.
    
    Note: raw_csv_json diikutkan agar fallback compute_from_raw dapat
    membaca data NDRE 2025 AME IV (tersimpan di source_2026->ndre125).
    """
    filters = []
    if dataset_tags:
        filters.append({"op": "in", "column": "dataset_tag", "value": dataset_tags})
    if divisi and divisi != "SEMUA":
        filters.append({"op": "eq", "column": "divisi", "value": divisi})
    if blok and blok != "SEMUA":
        filters.append({"op": "eq", "column": "blok", "value": blok})
    return fetch_paginated(
        client,
        "kebun_observasi_ndre_comparison",
        "dataset_tag,divisi,blok,n_baris,n_pokok,ndre_1_25,ndre_2_26,ndre_delta,"
        "klass_ndre_1_25,klass_ndre_2_26,id_npokok,raw_csv_json",
        filters=filters,
        page_size=page_size,
        max_rows=max_rows,
    )


def fetch_koordinat_blok(
    client: Client,
    dataset_tags: Optional[List[str]] = None,
    divisi: Optional[str] = None,
    blok: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Ambil data latitude/longitude dari kebun_pokok_koordinat per blok."""
    filters = []
    if dataset_tags:
        filters.append({"op": "in", "column": "dataset_tag", "value": dataset_tags})
    if divisi and divisi != "SEMUA":
        filters.append({"op": "eq", "column": "divisi", "value": divisi})
    if blok and blok != "SEMUA":
        filters.append({"op": "eq", "column": "blok", "value": blok})
        
    try:
        return fetch_paginated(
            client,
            "kebun_pokok_koordinat",
            "divisi,blok,n_baris,n_pokok,latitude,longitude",
            filters=filters,
            page_size=5000,
            max_rows=50000,
        )
    except Exception:
        return []


def fetch_global_sisip_stats(client: Client, dataset_tags: Optional[List[str]] = None, divisi: Optional[str] = None) -> Dict[str, int]:
    """Mengambil jumlah pohon berstatus Sisip menggunakan fast COUNT(*) via query ilike pada kolom raw_csv_json."""
    base_query = client.table("kebun_observasi_ndre_comparison")
    filters = []
    if dataset_tags:
        filters.append({"op": "in", "column": "dataset_tag", "value": dataset_tags})
    if divisi and divisi != "SEMUA":
        filters.append({"op": "eq", "column": "divisi", "value": divisi})
        
    def _apply(q):
        for f in filters:
            q = getattr(q, f["op"] if f["op"] != "in" else "in_")(f["column"], f["value"])
        return q

    try:
        q_26 = _apply(client.table("kebun_observasi_ndre_comparison").select("id_npokok", count="exact"))
        res_26 = q_26.ilike("raw_csv_json->source_2026->>ket", "%Sisip%").limit(1).execute()
        count_26 = res_26.count if hasattr(res_26, "count") and res_26.count is not None else len(res_26.data)
        
        q_25 = _apply(client.table("kebun_observasi_ndre_comparison").select("id_npokok", count="exact"))
        res_25 = q_25.ilike("raw_csv_json->source_2025->>ket", "%Sisip%").limit(1).execute()
        count_25 = res_25.count if hasattr(res_25, "count") and res_25.count is not None else len(res_25.data)
        return {"sisip_2026": count_26, "sisip_2025": count_25}
    except Exception:
        return {"sisip_2026": 0, "sisip_2025": 0}

