"""
MySQL → Qdrant Vektör İndeksleme
==================================
Tüm ilanları MySQL'den çekip Gemini Embedding ile vektörleştirip Qdrant'a yükler.

Kullanım:
    python index_vectors.py              # Tüm ilanları indeksle
    python index_vectors.py --force      # Collection'ı sıfırlayıp yeniden indeksle
"""

import os
import sys
import time
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()

from db import get_pool
from vector_db import get_client, ensure_collection, upsert_batch, COLLECTION_NAME
from qdrant_client.models import PointStruct
from logger import get_logger

log = get_logger("indexer")

# Gemini Embedding ayarları
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
EMBED_MODEL = "models/gemini-embedding-001"
BATCH_SIZE = 50  # Qdrant batch boyutu
EMBED_BATCH = 20  # Gemini embed batch boyutu (rate limit)


def build_text(row: dict) -> str:
    """İlan verisinden aranabilir metin oluşturur."""
    parts = []

    if row.get("marka"):
        parts.append(row["marka"])
    if row.get("seri"):
        parts.append(row["seri"])
    if row.get("model"):
        parts.append(row["model"])
    if row.get("yil"):
        parts.append(f"{row['yil']} model")
    if row.get("kilometre"):
        parts.append(f"{row['kilometre']:,} km".replace(",", "."))
    if row.get("fiyat"):
        parts.append(f"{row['fiyat']:,} TL".replace(",", "."))
    if row.get("yakit_tipi"):
        parts.append(row["yakit_tipi"])
    if row.get("vites_tipi"):
        parts.append(row["vites_tipi"])
    if row.get("kasa_tipi"):
        parts.append(row["kasa_tipi"])
    if row.get("renk"):
        parts.append(row["renk"])
    if row.get("il"):
        parts.append(row["il"])
    if row.get("boya_degisen_ozet"):
        parts.append(f"Boya: {row['boya_degisen_ozet']}")
    if row.get("tramer_tl"):
        parts.append(f"Tramer: {int(row['tramer_tl']):,} TL".replace(",", "."))
    if row.get("aciklama"):
        # Açıklamayı kısalt — embedding için ilk 500 karakter yeterli
        parts.append(row["aciklama"][:500])

    return " | ".join(parts)


def embed_texts(texts: list[str]) -> list[list[float]]:
    """Gemini ile metinleri vektörleştirir."""
    result = genai.embed_content(
        model=EMBED_MODEL,
        content=texts,
        task_type="retrieval_document"
    )
    return result['embedding']


def fetch_all_listings() -> list[dict]:
    """MySQL'den tüm ilanları JOIN'lerle çeker."""
    conn = get_pool().get_connection()
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT
                i.id,
                i.ilan_id,
                i.baslik,
                i.fiyat,
                i.yil,
                i.kilometre,
                i.motor_hacmi_cc,
                i.motor_gucu_hp,
                i.tramer_tl,
                i.boya_degisen_ozet,
                i.ilan_aciklamasi AS aciklama,
                m.ad AS marka,
                s.ad AS seri,
                md.ad AS model,
                yt.ad AS yakit_tipi,
                vt.ad AS vites_tipi,
                kt.ad AS kasa_tipi,
                r.ad AS renk,
                il.ad AS il
            FROM ilanlar i
            LEFT JOIN markalar m ON i.marka_id = m.id
            LEFT JOIN seriler s ON i.seri_id = s.id
            LEFT JOIN modeller md ON i.model_id = md.id
            LEFT JOIN yakit_tipleri yt ON i.yakit_tipi_id = yt.id
            LEFT JOIN vites_tipleri vt ON i.vites_tipi_id = vt.id
            LEFT JOIN kasa_tipleri kt ON i.kasa_tipi_id = kt.id
            LEFT JOIN renkler r ON i.renk_id = r.id
            LEFT JOIN iller il ON i.il_id = il.id
        """)
        rows = cursor.fetchall()
        cursor.close()
        return rows
    finally:
        conn.close()


def main():
    force = "--force" in sys.argv

    log.info("=" * 50)
    log.info("🚀 Vektör indeksleme başlıyor…")
    log.info("=" * 50)

    # Force modunda collection'ı sil ve yeniden oluştur
    if force:
        client = get_client()
        try:
            client.delete_collection(COLLECTION_NAME)
            log.info(f"🗑️ Collection silindi: {COLLECTION_NAME}")
        except Exception:
            pass

    ensure_collection()

    # Mevcut vektör sayısı
    client = get_client()
    info = client.get_collection(COLLECTION_NAME)
    existing_count = info.points_count or 0
    log.info(f"📊 Mevcut vektör sayısı: {existing_count}")

    # MySQL'den ilanları çek
    log.info("📂 MySQL'den ilanlar çekiliyor…")
    listings = fetch_all_listings()
    log.info(f"   {len(listings)} ilan yüklendi")

    if existing_count >= len(listings) and not force:
        log.info("✅ Tüm ilanlar zaten indekslenmiş. --force ile yeniden indeksleyebilirsiniz.")
        return

    # Batch olarak işle
    total = len(listings)
    indexed = 0
    batch_points = []

    for i, listing in enumerate(listings):
        text = build_text(listing)

        # Payload (Qdrant'ta saklanacak metadata)
        payload = {
            "ilan_id": listing["ilan_id"],
            "baslik": listing.get("baslik", ""),
            "marka": listing.get("marka", ""),
            "seri": listing.get("seri", ""),
            "model": listing.get("model", ""),
            "yil": listing.get("yil", 0),
            "kilometre": listing.get("kilometre", 0),
            "fiyat": listing.get("fiyat", 0),
            "yakit_tipi": listing.get("yakit_tipi", ""),
            "vites_tipi": listing.get("vites_tipi", ""),
            "kasa_tipi": listing.get("kasa_tipi", ""),
            "renk": listing.get("renk", ""),
            "il": listing.get("il", ""),
            "tramer_tl": float(listing["tramer_tl"]) if listing.get("tramer_tl") else None,
            "boya_degisen_ozet": listing.get("boya_degisen_ozet", ""),
            "text": text  # aranacak metin
        }

        batch_points.append({
            "id": listing["id"],
            "text": text,
            "payload": payload
        })

        # Batch dolduğunda embed + upsert
        if len(batch_points) >= BATCH_SIZE or i == total - 1:
            texts = [p["text"] for p in batch_points]

            # Gemini rate limit'e takılmamak için parçala
            all_vectors = []
            for j in range(0, len(texts), EMBED_BATCH):
                chunk = texts[j:j + EMBED_BATCH]
                try:
                    vectors = embed_texts(chunk)
                    all_vectors.extend(vectors)
                except Exception as e:
                    log.error(f"Embedding hatası: {e}")
                    time.sleep(5)
                    try:
                        vectors = embed_texts(chunk)
                        all_vectors.extend(vectors)
                    except Exception as e2:
                        log.error(f"Embedding tekrar hatası: {e2}")
                        # Boş vektörlerle doldur (skip edilecek)
                        all_vectors.extend([[0.0] * 768] * len(chunk))

                time.sleep(0.5)  # Rate limit koruması

            # Qdrant'a yükle
            points = [
                PointStruct(
                    id=batch_points[k]["id"],
                    vector=all_vectors[k],
                    payload=batch_points[k]["payload"]
                )
                for k in range(len(batch_points))
                if all_vectors[k][0] != 0.0  # Boş vektörleri atla
            ]

            if points:
                upsert_batch(points)

            indexed += len(points)
            log.info(f"  ✅ {indexed}/{total} ilan indekslendi")
            batch_points = []

    log.info(f"\n{'=' * 50}")
    log.info(f"🏁 İndeksleme tamamlandı!")
    log.info(f"   Toplam: {indexed} vektör")
    log.info(f"{'=' * 50}")


if __name__ == "__main__":
    main()
