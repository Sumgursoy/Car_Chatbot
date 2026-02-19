"""
Arabam MCP Server — FastMCP
============================
Güvenli, parametreli SQL şablonları ile araç ilanı araçları.
Gemini bu tool'ları MCP protokolü üzerinden çağırır.

Çalıştırma:
  python mcp_server.py
"""

import os
import json
import google.generativeai as genai
from dotenv import load_dotenv
from fastmcp import FastMCP

load_dotenv()

from db import execute_query, get_db_stats, get_pool
from vector_db import semantic_search, get_collection_info, ensure_collection
from vision import analyze_listing
from logger import get_logger

log = get_logger("mcp")

# Gemini Embedding (sadece semantik arama için)
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
EMBED_MODEL = "models/gemini-embedding-001"

# ─── FastMCP Server ───
mcp = FastMCP("Arabam MCP Server")

# ─── Ortak JOIN bloğu (normalize tablolar) ───

BASE_JOIN = """
    FROM ilanlar i
    LEFT JOIN markalar m ON i.marka_id = m.id
    LEFT JOIN seriler ser ON i.seri_id = ser.id
    LEFT JOIN modeller modl ON i.model_id = modl.id
    LEFT JOIN yakit_tipleri yt ON i.yakit_tipi_id = yt.id
    LEFT JOIN vites_tipleri vt ON i.vites_tipi_id = vt.id
    LEFT JOIN kasa_tipleri kt ON i.kasa_tipi_id = kt.id
    LEFT JOIN renkler r ON i.renk_id = r.id
    LEFT JOIN iller il ON i.il_id = il.id
    LEFT JOIN ilceler ilc ON i.ilce_id = ilc.id
"""

# Kısa JOIN (sadece sık kullanılanlar)
SHORT_JOIN = """
    FROM ilanlar i
    LEFT JOIN markalar m ON i.marka_id = m.id
    LEFT JOIN seriler ser ON i.seri_id = ser.id
    LEFT JOIN modeller modl ON i.model_id = modl.id
    LEFT JOIN renkler r ON i.renk_id = r.id
    LEFT JOIN iller il ON i.il_id = il.id
"""


def build_conditions(marka="", seri="", model="", yakit_tipi="", vites_tipi="",
                     kasa_tipi="", renk="", il="", min_fiyat=0, max_fiyat=0,
                     min_yil=0, max_yil=0, min_km=0, max_km=0):
    """Filtre parametrelerinden WHERE koşulları oluşturur."""
    conditions = []
    if marka:
        conditions.append(f"m.ad = '{marka}'")
    if seri:
        conditions.append(f"ser.ad = '{seri}'")
    if model:
        conditions.append(f"modl.ad = '{model}'")
    if yakit_tipi:
        conditions.append(f"yt.ad = '{yakit_tipi}'")
    if vites_tipi:
        conditions.append(f"vt.ad = '{vites_tipi}'")
    if kasa_tipi:
        conditions.append(f"kt.ad = '{kasa_tipi}'")
    if renk:
        conditions.append(f"r.ad = '{renk}'")
    if il:
        conditions.append(f"il.ad = '{il}'")
    if min_fiyat > 0:
        conditions.append(f"i.fiyat >= {min_fiyat}")
    if max_fiyat > 0:
        conditions.append(f"i.fiyat <= {max_fiyat}")
    if min_yil > 0:
        conditions.append(f"i.yil >= {min_yil}")
    if max_yil > 0:
        conditions.append(f"i.yil <= {max_yil}")
    if min_km > 0:
        conditions.append(f"i.kilometre >= {min_km}")
    if max_km > 0:
        conditions.append(f"i.kilometre <= {max_km}")
    return conditions


# ─────────────── TOOL 1: araba_ara ───────────────

@mcp.tool
def araba_ara(
    marka: str = "",
    seri: str = "",
    model: str = "",
    min_fiyat: int = 0,
    max_fiyat: int = 0,
    min_yil: int = 0,
    max_yil: int = 0,
    min_km: int = 0,
    max_km: int = 0,
    yakit_tipi: str = "",
    vites_tipi: str = "",
    kasa_tipi: str = "",
    renk: str = "",
    il: str = "",
    siralama: str = "fiyat_artan",
    limit: int = 10,
) -> str:
    """Filtrelere göre araç ilanı arar. Marka, fiyat aralığı, yıl, yakıt tipi gibi kriterlere göre araç listesi döner."""
    log.info(f"araba_ara: marka={marka}, max_fiyat={max_fiyat}, yakit={yakit_tipi}")

    conditions = build_conditions(marka=marka, seri=seri, model=model,
                                  yakit_tipi=yakit_tipi, vites_tipi=vites_tipi,
                                  kasa_tipi=kasa_tipi, renk=renk, il=il,
                                  min_fiyat=min_fiyat, max_fiyat=max_fiyat,
                                  min_yil=min_yil, max_yil=max_yil,
                                  min_km=min_km, max_km=max_km)

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    order_map = {
        "fiyat_artan": "i.fiyat ASC",
        "fiyat_azalan": "i.fiyat DESC",
        "yil_yeni": "i.yil DESC",
        "yil_eski": "i.yil ASC",
        "km_az": "i.kilometre ASC",
        "km_cok": "i.kilometre DESC",
    }
    order = order_map.get(siralama, "i.fiyat ASC")
    safe_limit = min(max(1, limit), 50)

    sql = f"""
        SELECT i.ilan_id, i.baslik, m.ad AS marka, ser.ad AS seri, modl.ad AS model,
               i.fiyat, i.yil, i.kilometre,
               yt.ad AS yakit_tipi, vt.ad AS vites_tipi, kt.ad AS kasa_tipi,
               r.ad AS renk, il.ad AS il
        {BASE_JOIN}
        {where}
        ORDER BY {order}
        LIMIT {safe_limit}
    """
    try:
        columns, rows = execute_query(sql)
        results = [dict(zip(columns, [str(v) if v is not None else None for v in row])) for row in rows]
        return json.dumps({"sonuc_sayisi": len(results), "sonuclar": results}, ensure_ascii=False)
    except Exception as e:
        log.error(f"araba_ara hatası: {e}")
        return json.dumps({"hata": str(e)}, ensure_ascii=False)


# ─────────────── TOOL 2: ilan_detay_getir ───────────────

@mcp.tool
def ilan_detay_getir(ilan_id: str) -> str:
    """Belirli bir ilanın tüm detaylarını (boya durumu, tramer dahil) getirir.
    ilan_id parametresi hem veritabanı ID'si (örn: 2) hem de arabam.com ilan numarası olabilir.
    Küçük sayılar (< 100000) önce veritabanı ID'si olarak aranır."""
    log.info(f"ilan_detay_getir: {ilan_id}")

    # Küçük sayıysa önce veritabanı id'sine bak, bulamazsa ilan_id'ye bak
    search_conditions = []
    try:
        numeric_id = int(str(ilan_id).strip())
        if numeric_id < 100000:
            search_conditions.append(f"i.id = {numeric_id}")
        search_conditions.append(f"i.ilan_id = '{ilan_id}'")
    except ValueError:
        search_conditions.append(f"i.ilan_id = '{ilan_id}'")

    for condition in search_conditions:
        sql = f"""
            SELECT i.id AS db_id, i.ilan_id, i.baslik, i.fiyat, i.yil, i.kilometre,
                   i.motor_hacmi_cc, i.motor_gucu_hp,
                   i.tramer_tl, i.boya_degisen_ozet,
                   m.ad AS marka, ser.ad AS seri, modl.ad AS model,
                   yt.ad AS yakit_tipi, vt.ad AS vites_tipi, kt.ad AS kasa_tipi,
                   r.ad AS renk, il.ad AS il, ilc.ad AS ilce
            {BASE_JOIN}
            WHERE {condition}
            LIMIT 1
        """
        try:
            columns, rows = execute_query(sql)
            if rows:
                result = dict(zip(columns, [str(v) if v is not None else None for v in rows[0]]))
                found_ilan_id = result.get("ilan_id", ilan_id)

                # Boya detayları
                try:
                    bcols, brows = execute_query(f"""
                        SELECT bd.parca_adi, bd.durum
                        FROM boya_detaylari bd
                        JOIN ilanlar i ON bd.ilan_db_id = i.id
                        WHERE i.ilan_id = '{found_ilan_id}'
                    """)
                    result["boya_detaylari"] = [dict(zip(bcols, row)) for row in brows]
                except Exception:
                    result["boya_detaylari"] = []

                return json.dumps(result, ensure_ascii=False)
        except Exception as e:
            log.error(f"ilan_detay_getir hatası ({condition}): {e}")
            continue

    return json.dumps({"hata": "İlan bulunamadı"}, ensure_ascii=False)


# ─────────────── TOOL 3: fiyat_istatistikleri ───────────────

@mcp.tool
def fiyat_istatistikleri(
    marka: str = "",
    seri: str = "",
    min_yil: int = 0,
    max_yil: int = 0,
    yakit_tipi: str = "",
    vites_tipi: str = "",
    kasa_tipi: str = "",
    renk: str = "",
    il: str = "",
) -> str:
    """Filtrelere göre araç fiyat istatistiklerini döner: minimum, maksimum, ortalama fiyat ve ilan sayısı. Marka, seri, yıl, yakıt tipi, vites tipi, kasa tipi, renk ve il bazında filtreleme yapılabilir."""
    log.info(f"fiyat_istatistikleri: marka={marka}, seri={seri}, vites={vites_tipi}")

    conditions = ["i.fiyat > 0"]
    conditions += build_conditions(marka=marka, seri=seri, yakit_tipi=yakit_tipi,
                                   vites_tipi=vites_tipi, kasa_tipi=kasa_tipi,
                                   renk=renk, il=il, min_yil=min_yil, max_yil=max_yil)

    where = "WHERE " + " AND ".join(conditions)

    sql = f"""
        SELECT
            COUNT(*) as ilan_sayisi,
            MIN(i.fiyat) as min_fiyat,
            MAX(i.fiyat) as max_fiyat,
            ROUND(AVG(i.fiyat)) as ortalama_fiyat
        {BASE_JOIN}
        {where}
    """
    try:
        columns, rows = execute_query(sql)
        result = dict(zip(columns, [str(v) if v is not None else None for v in rows[0]]))
        return json.dumps(result, ensure_ascii=False)
    except Exception as e:
        log.error(f"fiyat_istatistikleri hatası: {e}")
        return json.dumps({"hata": str(e)}, ensure_ascii=False)


# ─────────────── TOOL 4: marka_seri_listele ───────────────

@mcp.tool
def marka_seri_listele(marka: str = "", seri: str = "") -> str:
    """Veritabanındaki marka, seri ve model listesini döner. Marka verilirse o markanın serileri, seri verilirse o serinin modelleri listelenir."""
    log.info(f"marka_seri_listele: marka={marka}, seri={seri}")

    try:
        if seri and marka:
            sql = f"""
                SELECT DISTINCT modl.ad AS model, COUNT(*) as ilan_sayisi
                FROM ilanlar i
                JOIN markalar m ON i.marka_id = m.id
                JOIN seriler ser ON i.seri_id = ser.id
                JOIN modeller modl ON i.model_id = modl.id
                WHERE m.ad = '{marka}' AND ser.ad = '{seri}'
                GROUP BY modl.ad
                ORDER BY ilan_sayisi DESC
            """
        elif marka:
            sql = f"""
                SELECT DISTINCT ser.ad AS seri, COUNT(*) as ilan_sayisi
                FROM ilanlar i
                JOIN markalar m ON i.marka_id = m.id
                JOIN seriler ser ON i.seri_id = ser.id
                WHERE m.ad = '{marka}'
                GROUP BY ser.ad
                ORDER BY ilan_sayisi DESC
            """
        else:
            sql = """
                SELECT DISTINCT m.ad AS marka, COUNT(*) as ilan_sayisi
                FROM ilanlar i
                JOIN markalar m ON i.marka_id = m.id
                GROUP BY m.ad
                ORDER BY ilan_sayisi DESC
            """

        columns, rows = execute_query(sql)
        results = [dict(zip(columns, [str(v) if v is not None else None for v in row])) for row in rows]
        return json.dumps({"sonuclar": results}, ensure_ascii=False)
    except Exception as e:
        log.error(f"marka_seri_listele hatası: {e}")
        return json.dumps({"hata": str(e)}, ensure_ascii=False)


# ─────────────── TOOL 5: ilan_sayisi ───────────────

@mcp.tool
def ilan_sayisi(
    marka: str = "",
    seri: str = "",
    yakit_tipi: str = "",
    vites_tipi: str = "",
    kasa_tipi: str = "",
    il: str = "",
    min_yil: int = 0,
    max_yil: int = 0,
    min_fiyat: int = 0,
    max_fiyat: int = 0,
) -> str:
    """Filtrelere göre kaç ilan olduğunu sayar."""
    log.info(f"ilan_sayisi: marka={marka}")

    conditions = build_conditions(marka=marka, seri=seri, yakit_tipi=yakit_tipi,
                                  vites_tipi=vites_tipi, kasa_tipi=kasa_tipi,
                                  il=il, min_yil=min_yil, max_yil=max_yil,
                                  min_fiyat=min_fiyat, max_fiyat=max_fiyat)

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    sql = f"SELECT COUNT(*) as toplam {BASE_JOIN} {where}"
    try:
        columns, rows = execute_query(sql)
        return json.dumps({"toplam": int(rows[0][0])}, ensure_ascii=False)
    except Exception as e:
        log.error(f"ilan_sayisi hatası: {e}")
        return json.dumps({"hata": str(e)}, ensure_ascii=False)


# ─────────────── TOOL 6: renk_dagilimi ───────────────

@mcp.tool
def renk_dagilimi(marka: str = "") -> str:
    """İlanlardaki renk dağılımını gösterir. Opsiyonel olarak marka filtresi uygulanabilir."""
    log.info(f"renk_dagilimi: marka={marka}")

    where = f"WHERE m.ad = '{marka}'" if marka else ""

    sql = f"""
        SELECT r.ad AS renk, COUNT(*) as adet
        FROM ilanlar i
        LEFT JOIN renkler r ON i.renk_id = r.id
        LEFT JOIN markalar m ON i.marka_id = m.id
        {where}
        GROUP BY r.ad
        ORDER BY adet DESC
    """
    try:
        columns, rows = execute_query(sql)
        results = [dict(zip(columns, [str(v) if v is not None else None for v in row])) for row in rows]
        return json.dumps({"sonuclar": results}, ensure_ascii=False)
    except Exception as e:
        log.error(f"renk_dagilimi hatası: {e}")
        return json.dumps({"hata": str(e)}, ensure_ascii=False)


# ─────────────── TOOL 7: il_dagilimi ───────────────

@mcp.tool
def il_dagilimi(marka: str = "", limit: int = 10) -> str:
    """İlanların şehir bazlı dağılımını gösterir. Opsiyonel olarak marka filtresi uygulanabilir."""
    log.info(f"il_dagilimi: marka={marka}")

    where = f"WHERE m.ad = '{marka}'" if marka else ""
    safe_limit = min(max(1, limit), 81)

    sql = f"""
        SELECT il.ad AS il, COUNT(*) as adet
        FROM ilanlar i
        LEFT JOIN iller il ON i.il_id = il.id
        LEFT JOIN markalar m ON i.marka_id = m.id
        {where}
        GROUP BY il.ad
        ORDER BY adet DESC
        LIMIT {safe_limit}
    """
    try:
        columns, rows = execute_query(sql)
        results = [dict(zip(columns, [str(v) if v is not None else None for v in row])) for row in rows]
        return json.dumps({"sonuclar": results}, ensure_ascii=False)
    except Exception as e:
        log.error(f"il_dagilimi hatası: {e}")
        return json.dumps({"hata": str(e)}, ensure_ascii=False)


# ─────────────── RRF Reranker ───────────────

def rrf_merge(sql_results: list[dict], semantic_results: list[dict], k: int = 60) -> list[dict]:
    """Reciprocal Rank Fusion ile iki sonuç listesini birleştirir.
    Tam eşleşmeler (SQL) her zaman semantik benzerliklerden önce gelir."""
    scores = {}

    for rank, item in enumerate(sql_results):
        key = str(item.get("ilan_id", ""))
        if not key:
            continue
        if key not in scores:
            scores[key] = {"score": 0.0, "data": item, "sources": []}
        scores[key]["score"] += 1.0 / (k + rank + 1)
        scores[key]["sources"].append("keyword")

    for rank, item in enumerate(semantic_results):
        key = str(item.get("ilan_id", ""))
        if not key:
            continue
        if key not in scores:
            scores[key] = {"score": 0.0, "data": item, "sources": []}
        scores[key]["score"] += 1.0 / (k + rank + 1)
        scores[key]["sources"].append("semantic")

    merged = sorted(scores.values(), key=lambda x: x["score"], reverse=True)
    return merged


# ─────────────── TOOL 8: hibrit_arac_ara ───────────────

@mcp.tool
def hibrit_arac_ara(
    sorgu: str,
    marka: str = "",
    min_fiyat: int = 0,
    max_fiyat: int = 0,
    min_yil: int = 0,
    max_yil: int = 0,
    yakit_tipi: str = "",
    vites_tipi: str = "",
    limit: int = 10,
) -> str:
    """Hibrit arama: Doğal dil sorgusunu hem anahtar kelime (SQL) hem de semantik (Qdrant) olarak arar ve sonuçları birleştirir.
    Tam eşleşmeler (marka/model adı) her zaman en üstte yer alır.
    Kullanıcı bir araç adı, marka, model veya doğal dil açıklaması yazdığında bu tool kullanılmalıdır.
    Örnek: 'Astra', 'ekonomik SUV', 'beyaz BMW sedan', 'aile aracı'"""
    log.info(f"hibrit_arac_ara: sorgu='{sorgu}', marka={marka}")

    safe_limit = min(max(1, limit), 30)
    fetch_limit = safe_limit * 2  # Her kaynaktan daha fazla çekip RRF ile kırpacağız

    # ── 1. SQL Keyword Search ──
    sql_results = []
    try:
        # Sorguyu kelimelere ayır
        keywords = [w.strip() for w in sorgu.split() if len(w.strip()) >= 2]
        if keywords:
            like_conditions = []
            for kw in keywords:
                safe_kw = kw.replace("'", "''").replace("%", "\\%")
                like_conditions.append(
                    f"(i.baslik LIKE '%{safe_kw}%' OR m.ad LIKE '%{safe_kw}%' "
                    f"OR ser.ad LIKE '%{safe_kw}%' OR modl.ad LIKE '%{safe_kw}%' "
                    f"OR i.ilan_aciklamasi LIKE '%{safe_kw}%')"
                )

            # Ek filtreler
            extra_conditions = build_conditions(
                marka=marka, yakit_tipi=yakit_tipi, vites_tipi=vites_tipi,
                min_fiyat=min_fiyat, max_fiyat=max_fiyat,
                min_yil=min_yil, max_yil=max_yil
            )

            all_conditions = like_conditions + extra_conditions
            where = "WHERE " + " AND ".join(all_conditions)

            sql = f"""
                SELECT i.ilan_id, i.baslik, m.ad AS marka, ser.ad AS seri, modl.ad AS model,
                       i.fiyat, i.yil, i.kilometre,
                       yt.ad AS yakit_tipi, vt.ad AS vites_tipi, kt.ad AS kasa_tipi,
                       r.ad AS renk, il.ad AS il
                {BASE_JOIN}
                {where}
                ORDER BY i.fiyat ASC
                LIMIT {fetch_limit}
            """
            columns, rows = execute_query(sql)
            sql_results = [
                dict(zip(columns, [str(v) if v is not None else None for v in row]))
                for row in rows
            ]
            log.info(f"  SQL keyword araması: {len(sql_results)} sonuç")
    except Exception as e:
        log.error(f"  SQL keyword arama hatası: {e}")

    # ── 2. Qdrant Semantic Search ──
    semantic_results = []
    try:
        result = genai.embed_content(
            model=EMBED_MODEL,
            content=sorgu,
            task_type="retrieval_query"
        )
        query_vector = result['embedding']

        # Qdrant filtreleri
        qdrant_filters = {}
        if marka:
            qdrant_filters["marka"] = marka
        if min_fiyat > 0:
            qdrant_filters["fiyat_min"] = min_fiyat
        if max_fiyat > 0:
            qdrant_filters["fiyat_max"] = max_fiyat
        if min_yil > 0:
            qdrant_filters["yil_min"] = min_yil
        if max_yil > 0:
            qdrant_filters["yil_max"] = max_yil
        if yakit_tipi:
            qdrant_filters["yakit_tipi"] = yakit_tipi

        hits = semantic_search(
            query_vector,
            limit=fetch_limit,
            filters=qdrant_filters if qdrant_filters else None
        )

        for h in hits:
            p = h["payload"]
            semantic_results.append({
                "ilan_id": str(p.get("ilan_id", "")),
                "baslik": p.get("baslik", ""),
                "marka": p.get("marka", ""),
                "seri": p.get("seri", ""),
                "model": p.get("model", ""),
                "yil": str(p.get("yil", "")) if p.get("yil") else None,
                "fiyat": str(p.get("fiyat", "")) if p.get("fiyat") else None,
                "kilometre": str(p.get("kilometre", "")) if p.get("kilometre") else None,
                "yakit_tipi": p.get("yakit_tipi", ""),
                "vites_tipi": p.get("vites_tipi", ""),
                "kasa_tipi": p.get("kasa_tipi", ""),
                "renk": p.get("renk", ""),
                "il": p.get("il", ""),
                "_semantic_score": round(h["score"], 4),
            })
        log.info(f"  Semantik arama: {len(semantic_results)} sonuç")
    except Exception as e:
        log.error(f"  Semantik arama hatası: {e}")

    # ── 3. RRF Merge ──
    if not sql_results and not semantic_results:
        return json.dumps({"sonuc_sayisi": 0, "sonuclar": [], "mesaj": "Sonuç bulunamadı"}, ensure_ascii=False)

    merged = rrf_merge(sql_results, semantic_results)

    # Sonuçları düzenle
    final = []
    for item in merged[:safe_limit]:
        entry = item["data"].copy()
        entry["_kaynaklar"] = "+".join(item["sources"])
        entry["_rrf_skor"] = round(item["score"], 6)
        # _semantic_score iç alanını kaldır
        entry.pop("_semantic_score", None)
        final.append(entry)

    log.info(f"  Hibrit sonuç: {len(final)} ilan (SQL: {len(sql_results)}, Semantic: {len(semantic_results)})")

    return json.dumps({
        "sonuc_sayisi": len(final),
        "arama_bilgisi": {
            "sql_sonuc": len(sql_results),
            "semantik_sonuc": len(semantic_results),
            "birlesik_sonuc": len(final)
        },
        "sonuclar": final
    }, ensure_ascii=False)


# ─────────────── TOOL 9: benzer_arac_bul ───────────────

@mcp.tool
def benzer_arac_bul(aciklama: str, limit: int = 10) -> str:
    """Doğal dil açıklamasına göre benzer araçları semantik olarak bulur. Örnek: 'aileler için geniş SUV', 'ekonomik şehir aracı'."""
    log.info(f"benzer_arac_bul: {aciklama}")

    try:
        # Gemini Embedding ile soru vektörü oluştur
        result = genai.embed_content(
            model=EMBED_MODEL,
            content=aciklama,
            task_type="retrieval_query"
        )
        query_vector = result['embedding']

        safe_limit = min(max(1, limit), 20)
        results = semantic_search(query_vector, limit=safe_limit)

        cars = []
        for r in results:
            p = r["payload"]
            cars.append({
                "benzerlik_skoru": round(r["score"], 4),
                "ilan_id": p.get("ilan_id", ""),
                "baslik": p.get("baslik", ""),
                "marka": p.get("marka", ""),
                "seri": p.get("seri", ""),
                "model": p.get("model", ""),
                "yil": p.get("yil"),
                "fiyat": p.get("fiyat"),
                "kilometre": p.get("kilometre"),
                "yakit_tipi": p.get("yakit_tipi", ""),
                "vites_tipi": p.get("vites_tipi", ""),
                "kasa_tipi": p.get("kasa_tipi", ""),
                "renk": p.get("renk", ""),
                "il": p.get("il", ""),
            })

        return json.dumps({"sonuc_sayisi": len(cars), "sonuclar": cars}, ensure_ascii=False)
    except Exception as e:
        log.error(f"benzer_arac_bul hatası: {e}")
        return json.dumps({"hata": str(e)}, ensure_ascii=False)


# ─────────────── TOOL 9: veritabani_ozeti ───────────────

@mcp.tool
def veritabani_ozeti() -> str:
    """Veritabanının genel istatistiklerini döner: toplam ilan, marka sayısı, fiyat aralığı, yıl aralığı."""
    log.info("veritabani_ozeti çağrıldı")

    try:
        db_stats = get_db_stats()

        # Qdrant bilgisi
        try:
            vector_stats = get_collection_info()
        except Exception:
            vector_stats = {"durum": "bağlantı yok"}

        return json.dumps({
            "mysql": db_stats,
            "qdrant": vector_stats
        }, ensure_ascii=False)
    except Exception as e:
        log.error(f"veritabani_ozeti hatası: {e}")
        return json.dumps({"hata": str(e)}, ensure_ascii=False)


# ─────────────── TOOL 11: ilan_gorselleri_analiz_et ───────────────

@mcp.tool
def ilan_gorselleri_analiz_et(url: str) -> str:
    """Verilen ilan URL'sindeki fotoğrafları Crawl4AI ile çeker ve Gemini Vision ile analiz eder.
    Aracın gerçek durumunu fotoğraflardan tespit eder: boya, aşınma, sigara yanığı, panel aralıkları.
    Kullanıcı bir ilan linki verdiğinde bu tool kullanılır.
    Örnek: 'şu ilanın fotoğraflarını analiz et: https://www.arabam.com/ilan/123456'"""
    log.info(f"ilan_gorselleri_analiz_et: {url}")

    try:
        import asyncio
        result = asyncio.run(analyze_listing(url))

        if "hata" in result and result.get("gorsel_sayisi", 0) == 0:
            return json.dumps({
                "hata": result["hata"],
                "url": url,
            }, ensure_ascii=False)

        # Screenshot base64'ü çok büyük olduğu için MCP response'dan çıkar
        response = {
            "url": url,
            "sayfa_basligi": result.get("page_title", ""),
            "analiz_edilen_gorsel_sayisi": result.get("gorsel_sayisi", 0),
            "gorsel_analiz": result.get("analiz", ""),
            "gorsel_urlleri": result.get("image_urls", []),
        }
        return json.dumps(response, ensure_ascii=False)

    except Exception as e:
        log.error(f"ilan_gorselleri_analiz_et hatası: {e}")
        return json.dumps({"hata": str(e)}, ensure_ascii=False)


# ─────────────── MAIN ───────────────

if __name__ == "__main__":
    PORT = int(os.getenv("MCP_PORT", "8000"))
    log.info("🚀 FastMCP Server başlatılıyor…")

    # Veritabanı hazırlığı
    ensure_collection()

    log.info(f"✅ FastMCP Server çalışıyor: http://0.0.0.0:{PORT}/mcp")
    log.info(f"   Tools: araba_ara, ilan_detay_getir, fiyat_istatistikleri, "
             f"marka_seri_listele, ilan_sayisi, renk_dagilimi, "
             f"il_dagilimi, hibrit_arac_ara, benzer_arac_bul, veritabani_ozeti, "
             f"ilan_gorselleri_analiz_et")

    mcp.run(transport="sse", host="0.0.0.0", port=PORT)
