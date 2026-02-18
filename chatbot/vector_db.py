"""
Qdrant Vektör Veritabanı Yöneticisi
=====================================
- Qdrant bağlantısı
- Collection oluşturma
- Semantik arama
"""

import os
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance, VectorParams, PointStruct,
    Filter, FieldCondition, MatchValue, Range
)
from logger import get_logger

log = get_logger("vector_db")

QDRANT_HOST = os.getenv("QDRANT_HOST", "localhost")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))
COLLECTION_NAME = "car_listings"
VECTOR_SIZE = 3072  # Gemini gemini-embedding-001 boyutu

_client = None


def get_client() -> QdrantClient:
    """Qdrant client singleton."""
    global _client
    if _client is None:
        _client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
        log.info(f"Qdrant bağlantısı: {QDRANT_HOST}:{QDRANT_PORT}")
    return _client


def ensure_collection():
    """Collection yoksa oluştur."""
    client = get_client()
    collections = [c.name for c in client.get_collections().collections]

    if COLLECTION_NAME not in collections:
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(
                size=VECTOR_SIZE,
                distance=Distance.COSINE
            )
        )
        log.info(f"Collection oluşturuldu: {COLLECTION_NAME} ({VECTOR_SIZE}D, cosine)")
    else:
        log.info(f"Collection zaten mevcut: {COLLECTION_NAME}")


def upsert_batch(points: list[PointStruct]):
    """Batch olarak vektörleri yükle."""
    client = get_client()
    client.upsert(
        collection_name=COLLECTION_NAME,
        points=points,
        wait=True
    )
    log.info(f"{len(points)} nokta Qdrant'a yüklendi")


def semantic_search(
    query_vector: list[float],
    limit: int = 10,
    filters: dict = None
) -> list[dict]:
    """
    Semantik arama — sorgu vektörüne en yakın ilanları döner.

    Args:
        query_vector: Gemini embedding vektörü
        limit: Maksimum sonuç sayısı
        filters: Opsiyonel filtreler (marka, yil_min, fiyat_max vs.)
    Returns:
        [{id, score, payload}, ...]
    """
    client = get_client()

    # Filtre oluştur
    qdrant_filter = None
    if filters:
        conditions = []
        if "marka" in filters:
            conditions.append(
                FieldCondition(key="marka", match=MatchValue(value=filters["marka"]))
            )
        if "yil_min" in filters:
            conditions.append(
                FieldCondition(key="yil", range=Range(gte=filters["yil_min"]))
            )
        if "yil_max" in filters:
            conditions.append(
                FieldCondition(key="yil", range=Range(lte=filters["yil_max"]))
            )
        if "fiyat_max" in filters:
            conditions.append(
                FieldCondition(key="fiyat", range=Range(lte=filters["fiyat_max"]))
            )
        if "fiyat_min" in filters:
            conditions.append(
                FieldCondition(key="fiyat", range=Range(gte=filters["fiyat_min"]))
            )
        if "yakit_tipi" in filters:
            conditions.append(
                FieldCondition(key="yakit_tipi", match=MatchValue(value=filters["yakit_tipi"]))
            )
        if conditions:
            qdrant_filter = Filter(must=conditions)

    results = client.query_points(
        collection_name=COLLECTION_NAME,
        query=query_vector,
        query_filter=qdrant_filter,
        limit=limit,
        with_payload=True
    )

    log.info(f"Semantik arama: {len(results.points)} sonuç (limit={limit})")

    return [
        {
            "id": point.id,
            "score": point.score,
            "payload": point.payload
        }
        for point in results.points
    ]


def get_collection_info() -> dict:
    """Collection istatistikleri."""
    client = get_client()
    try:
        info = client.get_collection(COLLECTION_NAME)
        return {
            "collection": COLLECTION_NAME,
            "vectors_count": info.points_count,
            "points_count": info.points_count,
            "status": info.status.value
        }
    except Exception as e:
        log.error(f"Collection bilgisi alınamadı: {e}")
        return {"collection": COLLECTION_NAME, "status": "not_found"}
