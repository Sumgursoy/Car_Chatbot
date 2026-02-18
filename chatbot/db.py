"""
MySQL bağlantı yöneticisi
==========================
- Connection pool ile verimli bağlantı
- Güvenli SQL çalıştırma (sadece SELECT)
- Şema bilgisi çekme
"""

import os
import mysql.connector
from mysql.connector import pooling
from logger import get_logger

log = get_logger("db")

DB_CONFIG = {
    "host":     os.getenv("MYSQL_HOST", "localhost"),
    "user":     os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", ""),
    "database": os.getenv("MYSQL_DATABASE", "arabam_chatbot"),
}

# Connection pool
_pool = None

def get_pool():
    global _pool
    if _pool is None:
        _pool = pooling.MySQLConnectionPool(
            pool_name="chatbot_pool",
            pool_size=5,
            **DB_CONFIG
        )
    return _pool


def execute_query(sql: str) -> tuple:
    """
    Güvenli SQL çalıştır — sadece SELECT izinli.
    Returns: (columns, rows)
    """
    log.info(f"SQL çalıştırılıyor: {sql[:200]}")

    # Güvenlik: sadece SELECT sorgularına izin ver
    cleaned = sql.strip().upper()
    if not cleaned.startswith("SELECT"):
        log.warning(f"Engellenen sorgu (SELECT değil): {sql[:100]}")
        raise ValueError("Sadece SELECT sorguları çalıştırılabilir!")

    blocked = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE", "EXEC"]
    for kw in blocked:
        if kw in cleaned and kw != cleaned.split()[0]:
            log.warning(f"Engellenen anahtar kelime ({kw}): {sql[:100]}")
            raise ValueError(f"Engellenen anahtar kelime: {kw}")

    conn = get_pool().get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
        cursor.close()
        log.info(f"Sorgu tamamlandı: {len(rows)} satır döndü")
        return columns, rows
    except Exception as e:
        log.error(f"SQL hatası: {e} — Sorgu: {sql[:200]}")
        raise
    finally:
        conn.close()


def execute_admin(sql: str):
    """DDL komutları çalıştır (CREATE VIEW vb.) — sadece sunucu başlangıcında kullanılır."""
    conn = get_pool().get_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        log.info(f"Admin SQL çalıştırıldı: {sql[:100]}")
    except Exception as e:
        log.error(f"Admin SQL hatası: {e}")
        raise
    finally:
        conn.close()


def create_view():
    """v_ilanlar VIEW'ını oluşturur — tüm JOIN'ler burada tek seferde yapılır."""
    log.info("v_ilanlar VIEW oluşturuluyor...")
    execute_admin("DROP VIEW IF EXISTS v_ilanlar")
    execute_admin("""
        CREATE VIEW v_ilanlar AS
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
            m.ad AS marka,
            ser.ad AS seri,
            modl.ad AS model,
            yt.ad AS yakit_tipi,
            vt.ad AS vites_tipi,
            kt.ad AS kasa_tipi,
            r.ad AS renk,
            il.ad AS il,
            ilc.ad AS ilce
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
    """)
    log.info("✅ v_ilanlar VIEW oluşturuldu")


def get_schema_info() -> str:
    """v_ilanlar VIEW'ının sütun bilgisini döner (Gemini'ye gönderilecek)."""
    conn = get_pool().get_connection()
    try:
        cursor = conn.cursor()

        # v_ilanlar VIEW sütunları
        cursor.execute("DESCRIBE v_ilanlar")
        cols = cursor.fetchall()
        col_lines = []
        for col in cols:
            name, dtype, null, key, default, extra = col
            col_lines.append(f"  {name} {dtype}")

        schema = "VIEW v_ilanlar (ana sorgu tablosu):\n" + "\n".join(col_lines)

        # boya_detaylari tablosu (detay sorguları için)
        cursor.execute("DESCRIBE boya_detaylari")
        cols = cursor.fetchall()
        col_lines = []
        for col in cols:
            name, dtype, null, key, default, extra = col
            col_lines.append(f"  {name} {dtype}")

        schema += "\n\nTABLE boya_detaylari:\n" + "\n".join(col_lines)
        schema += "\n  İlişki: boya_detaylari.ilan_db_id → v_ilanlar.id"

        cursor.close()
        return schema
    finally:
        conn.close()


def get_db_stats() -> dict:
    """Veritabanı istatistiklerini döner."""
    conn = get_pool().get_connection()
    try:
        cursor = conn.cursor()
        stats = {}

        cursor.execute("SELECT COUNT(*) FROM ilanlar")
        stats["toplam_ilan"] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM markalar")
        stats["marka_sayisi"] = cursor.fetchone()[0]

        cursor.execute("SELECT MIN(fiyat), MAX(fiyat), AVG(fiyat) FROM ilanlar WHERE fiyat > 0")
        row = cursor.fetchone()
        stats["min_fiyat"] = int(row[0]) if row[0] else 0
        stats["max_fiyat"] = int(row[1]) if row[1] else 0
        stats["ort_fiyat"] = int(row[2]) if row[2] else 0

        cursor.execute("SELECT MIN(yil), MAX(yil) FROM ilanlar WHERE yil > 0")
        row = cursor.fetchone()
        stats["min_yil"] = row[0] if row[0] else 0
        stats["max_yil"] = row[1] if row[1] else 0

        cursor.close()
        return stats
    finally:
        conn.close()


def get_sample_values() -> str:
    """Lookup tablolarındaki örnek değerleri döner (Gemini'nin doğru değer kullanması için)."""
    conn = get_pool().get_connection()
    try:
        cursor = conn.cursor()
        samples = []

        # Markalar
        cursor.execute("SELECT ad FROM markalar ORDER BY ad")
        markalar = [r[0] for r in cursor.fetchall()]
        samples.append(f"Markalar: {', '.join(markalar)}")

        # Yakıt tipleri
        cursor.execute("SELECT ad FROM yakit_tipleri ORDER BY ad")
        yakit = [r[0] for r in cursor.fetchall()]
        samples.append(f"Yakıt Tipleri: {', '.join(yakit)}")

        # Vites tipleri
        cursor.execute("SELECT ad FROM vites_tipleri ORDER BY ad")
        vites = [r[0] for r in cursor.fetchall()]
        samples.append(f"Vites Tipleri: {', '.join(vites)}")

        # Kasa tipleri
        cursor.execute("SELECT ad FROM kasa_tipleri ORDER BY ad")
        kasa = [r[0] for r in cursor.fetchall()]
        samples.append(f"Kasa Tipleri: {', '.join(kasa)}")

        # Renkler
        cursor.execute("SELECT ad FROM renkler ORDER BY ad")
        renkler = [r[0] for r in cursor.fetchall()]
        samples.append(f"Renkler: {', '.join(renkler)}")

        # Popüler iller
        cursor.execute("""
            SELECT il.ad, COUNT(*) as c FROM ilanlar i
            JOIN iller il ON i.il_id = il.id
            GROUP BY il.ad ORDER BY c DESC LIMIT 10
        """)
        iller = [f"{r[0]}({r[1]})" for r in cursor.fetchall()]
        samples.append(f"En çok ilanlı iller: {', '.join(iller)}")

        cursor.close()
        return "\n".join(samples)
    finally:
        conn.close()
