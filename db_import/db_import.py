"""
MySQL'e Normalize Veri Aktarımı
================================
arabam_dataset.json → arabam_chatbot veritabanı

Kullanım:
  pip install mysql-connector-python python-dotenv
  python db_import.py
"""

import json, os, sys, logging
from dotenv import load_dotenv
import mysql.connector

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-5s │ %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("db_import")

# ─────────────────────────── CONFIG ───────────────────────────

DB_CONFIG = {
    "host":     os.getenv("MYSQL_HOST", "localhost"),
    "user":     os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", ""),
}
DB_NAME   = os.getenv("MYSQL_DATABASE", "arabam_chatbot")
DATA_FILE = r"C:\Users\sumgu\OneDrive\Masaüstü\car chatbot\data\arabam_dataset_clean.json"

# ─────────────────────────── SCHEMA ───────────────────────────

TABLES = [
    # 1. Bağımsız lookup tablolar (FK yok)
    """CREATE TABLE IF NOT EXISTS markalar (
        id INT AUTO_INCREMENT PRIMARY KEY,
        ad VARCHAR(50) NOT NULL UNIQUE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",

    """CREATE TABLE IF NOT EXISTS yakit_tipleri (
        id INT AUTO_INCREMENT PRIMARY KEY,
        ad VARCHAR(30) NOT NULL UNIQUE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",

    """CREATE TABLE IF NOT EXISTS vites_tipleri (
        id INT AUTO_INCREMENT PRIMARY KEY,
        ad VARCHAR(30) NOT NULL UNIQUE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",

    """CREATE TABLE IF NOT EXISTS kasa_tipleri (
        id INT AUTO_INCREMENT PRIMARY KEY,
        ad VARCHAR(60) NOT NULL UNIQUE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",

    """CREATE TABLE IF NOT EXISTS renkler (
        id INT AUTO_INCREMENT PRIMARY KEY,
        ad VARCHAR(40) NOT NULL UNIQUE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",

    """CREATE TABLE IF NOT EXISTS iller (
        id INT AUTO_INCREMENT PRIMARY KEY,
        ad VARCHAR(50) NOT NULL UNIQUE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",

    # 2. FK bağımlı tablolar
    """CREATE TABLE IF NOT EXISTS seriler (
        id INT AUTO_INCREMENT PRIMARY KEY,
        marka_id INT NOT NULL,
        ad VARCHAR(100) NOT NULL,
        FOREIGN KEY (marka_id) REFERENCES markalar(id),
        UNIQUE KEY uq_seri (marka_id, ad)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",

    """CREATE TABLE IF NOT EXISTS modeller (
        id INT AUTO_INCREMENT PRIMARY KEY,
        seri_id INT NOT NULL,
        ad VARCHAR(150) NOT NULL,
        FOREIGN KEY (seri_id) REFERENCES seriler(id),
        UNIQUE KEY uq_model (seri_id, ad)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",

    """CREATE TABLE IF NOT EXISTS ilceler (
        id INT AUTO_INCREMENT PRIMARY KEY,
        il_id INT NOT NULL,
        ad VARCHAR(100) NOT NULL,
        FOREIGN KEY (il_id) REFERENCES iller(id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",

    # 3. Ana tablo (tüm FK'lara bağımlı)
    """CREATE TABLE IF NOT EXISTS ilanlar (
        id INT AUTO_INCREMENT PRIMARY KEY,
        ilan_id VARCHAR(20) UNIQUE,
        ilan_url TEXT,
        baslik TEXT,
        marka_id INT,
        seri_id INT,
        model_id INT,
        versiyon VARCHAR(150),
        yakit_tipi_id INT,
        vites_tipi_id INT,
        kasa_tipi_id INT,
        renk_id INT,
        motor_hacmi_cc INT,
        motor_gucu_hp INT,
        silindir INT,
        tork_nm INT,
        yakit_ort_lt DECIMAL(4,1),
        cekis VARCHAR(30),
        yil INT,
        kilometre INT,
        fiyat BIGINT,
        arac_durumu VARCHAR(50),
        sinif VARCHAR(20),
        garanti VARCHAR(30),
        kimden VARCHAR(30),
        takasa_uygun VARCHAR(30),
        il_id INT,
        ilce_id INT,
        boya_degisen_ozet VARCHAR(200),
        tramer_tl BIGINT,
        ilan_aciklamasi TEXT,
        FOREIGN KEY (marka_id) REFERENCES markalar(id),
        FOREIGN KEY (seri_id) REFERENCES seriler(id),
        FOREIGN KEY (model_id) REFERENCES modeller(id),
        FOREIGN KEY (yakit_tipi_id) REFERENCES yakit_tipleri(id),
        FOREIGN KEY (vites_tipi_id) REFERENCES vites_tipleri(id),
        FOREIGN KEY (kasa_tipi_id) REFERENCES kasa_tipleri(id),
        FOREIGN KEY (renk_id) REFERENCES renkler(id),
        FOREIGN KEY (il_id) REFERENCES iller(id),
        FOREIGN KEY (ilce_id) REFERENCES ilceler(id),
        INDEX idx_fiyat (fiyat),
        INDEX idx_yil (yil),
        INDEX idx_km (kilometre),
        INDEX idx_marka_yil (marka_id, yil),
        INDEX idx_il_fiyat (il_id, fiyat)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",

    # 4. Boya detay (ilanlar'a bağımlı)
    """CREATE TABLE IF NOT EXISTS boya_detaylari (
        id INT AUTO_INCREMENT PRIMARY KEY,
        ilan_db_id INT NOT NULL,
        parca_adi VARCHAR(60) NOT NULL,
        durum VARCHAR(40) NOT NULL,
        FOREIGN KEY (ilan_db_id) REFERENCES ilanlar(id) ON DELETE CASCADE,
        INDEX idx_ilan (ilan_db_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
]

# ─────────────────────────── HELPERS ──────────────────────────

def clean_val(val):
    """'-' veya boş stringleri None'a çevir."""
    if val is None:
        return None
    if isinstance(val, str):
        val = val.strip()
        if val in ("", "-"):
            return None
    return val


def get_or_create(cursor, table, ad, extra_col=None, extra_val=None):
    """Lookup tablosuna kayıt ekle veya var olanın ID'sini döndür."""
    if ad is None:
        return None

    if extra_col:
        cursor.execute(
            f"SELECT id FROM {table} WHERE {extra_col} = %s AND ad = %s",
            (extra_val, ad)
        )
    else:
        cursor.execute(f"SELECT id FROM {table} WHERE ad = %s", (ad,))

    row = cursor.fetchone()
    if row:
        return row[0]

    if extra_col:
        cursor.execute(
            f"INSERT INTO {table} ({extra_col}, ad) VALUES (%s, %s)",
            (extra_val, ad)
        )
    else:
        cursor.execute(f"INSERT INTO {table} (ad) VALUES (%s)", (ad,))

    return cursor.lastrowid


# ─────────────────────────── MAIN ─────────────────────────────

def main():
    # 1. Veriyi yükle
    log.info(f"📂 {DATA_FILE} okunuyor…")
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    log.info(f"   {len(data)} ilan yüklendi")

    # 2. MySQL bağlantısı
    log.info(f"🔌 MySQL bağlanıyor ({DB_CONFIG['host']})…")
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # 3. Veritabanını sıfırdan oluştur (varsa drop)
    cursor.execute(f"DROP DATABASE IF EXISTS `{DB_NAME}`")
    cursor.execute(f"CREATE DATABASE `{DB_NAME}` "
                   f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
    cursor.execute(f"USE `{DB_NAME}`")
    log.info(f"   Veritabanı: {DB_NAME}")

    # 4. Tabloları sırayla oluştur
    log.info("📋 Tablolar oluşturuluyor…")
    for sql in TABLES:
        cursor.execute(sql)
    conn.commit()
    log.info("   ✅ Tablolar hazır")

    # 5. İlanları aktar
    log.info("🚀 Veri aktarımı başlıyor…")

    # Cache: lookup → id  (performans için)
    cache = {
        "markalar": {},
        "yakit_tipleri": {},
        "vites_tipleri": {},
        "kasa_tipleri": {},
        "renkler": {},
        "iller": {},
    }

    inserted = 0
    skipped = 0
    paint_count = 0

    for i, rec in enumerate(data):
        # Temizle
        rec = {k: clean_val(v) for k, v in rec.items() if not isinstance(v, dict)}

        # boya_degisen_detay dict'i ayrıca al
        paint_detail = data[i].get("boya_degisen_detay", {})
        if not isinstance(paint_detail, dict):
            paint_detail = {}

        ilan_id = rec.get("ilan_id")
        if not ilan_id:
            skipped += 1
            continue

        # Duplicate check
        cursor.execute("SELECT id FROM ilanlar WHERE ilan_id = %s", (ilan_id,))
        if cursor.fetchone():
            skipped += 1
            continue

        # --- Lookup ID'lerini al/oluştur ---

        # Marka
        marka = rec.get("marka")
        if marka and marka not in cache["markalar"]:
            cache["markalar"][marka] = get_or_create(cursor, "markalar", marka)
        marka_id = cache["markalar"].get(marka)

        # Seri (marka'ya bağlı)
        seri = rec.get("seri")
        seri_id = None
        if seri and marka_id:
            seri_id = get_or_create(cursor, "seriler", seri, "marka_id", marka_id)

        # Model (seri'ye bağlı)
        model = rec.get("model")
        model_id = None
        if model and seri_id:
            model_id = get_or_create(cursor, "modeller", model, "seri_id", seri_id)

        # Yakıt tipi
        yakit = rec.get("yakit_tipi")
        if yakit and yakit not in cache["yakit_tipleri"]:
            cache["yakit_tipleri"][yakit] = get_or_create(cursor, "yakit_tipleri", yakit)
        yakit_id = cache["yakit_tipleri"].get(yakit)

        # Vites tipi
        vites = rec.get("vites_tipi")
        if vites and vites not in cache["vites_tipleri"]:
            cache["vites_tipleri"][vites] = get_or_create(cursor, "vites_tipleri", vites)
        vites_id = cache["vites_tipleri"].get(vites)

        # Kasa tipi
        kasa = rec.get("kasa_tipi")
        if kasa and kasa not in cache["kasa_tipleri"]:
            cache["kasa_tipleri"][kasa] = get_or_create(cursor, "kasa_tipleri", kasa)
        kasa_id = cache["kasa_tipleri"].get(kasa)

        # Renk
        renk = rec.get("renk")
        if renk and renk not in cache["renkler"]:
            cache["renkler"][renk] = get_or_create(cursor, "renkler", renk)
        renk_id = cache["renkler"].get(renk)

        # İl
        il = rec.get("il")
        if il and il not in cache["iller"]:
            cache["iller"][il] = get_or_create(cursor, "iller", il)
        il_id = cache["iller"].get(il)

        # İlçe (il'e bağlı)
        ilce = rec.get("ilce")
        ilce_id = None
        if ilce and il_id:
            ilce_id = get_or_create(cursor, "ilceler", ilce, "il_id", il_id)

        # --- Ana kayıt ---
        cursor.execute("""
            INSERT INTO ilanlar (
                ilan_id, ilan_url, baslik,
                marka_id, seri_id, model_id, versiyon,
                yakit_tipi_id, vites_tipi_id, kasa_tipi_id, renk_id,
                motor_hacmi_cc, motor_gucu_hp, silindir, tork_nm,
                yakit_ort_lt, cekis,
                yil, kilometre, fiyat,
                arac_durumu, sinif, garanti,
                kimden, takasa_uygun,
                il_id, ilce_id,
                boya_degisen_ozet, tramer_tl,
                ilan_aciklamasi
            ) VALUES (
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s
            )
        """, (
            ilan_id, rec.get("ilan_url"), rec.get("baslik"),
            marka_id, seri_id, model_id, rec.get("versiyon"),
            yakit_id, vites_id, kasa_id, renk_id,
            rec.get("motor_hacmi_cc"), rec.get("motor_gucu_hp"),
            rec.get("silindir"), rec.get("tork_nm"),
            rec.get("yakit_ort_lt"), rec.get("cekis"),
            rec.get("yil"), rec.get("kilometre"), rec.get("fiyat"),
            rec.get("arac_durumu"), rec.get("sinif"), rec.get("garanti"),
            rec.get("kimden"), rec.get("takasa_uygun"),
            il_id, ilce_id,
            rec.get("boya_degisen_ozet"), rec.get("tramer_tl"),
            rec.get("ilan_aciklamasi"),
        ))

        ilan_db_id = cursor.lastrowid
        inserted += 1

        # --- Boya detayları ---
        for parca, durum in paint_detail.items():
            cursor.execute(
                "INSERT INTO boya_detaylari (ilan_db_id, parca_adi, durum) VALUES (%s, %s, %s)",
                (ilan_db_id, parca, durum)
            )
            paint_count += 1

        # Her 500 kayıtta commit + log
        if inserted % 500 == 0:
            conn.commit()
            log.info(f"  ✅ {inserted}/{len(data)} ilan aktarıldı")

    # Final commit
    conn.commit()

    # 6. Sonuç özeti
    log.info(f"\n{'='*50}")
    log.info(f"🏁 TAMAMLANDI!")
    log.info(f"   İlan aktarılan: {inserted}")
    log.info(f"   Atlanan (duplicate/eksik): {skipped}")
    log.info(f"   Boya detayı: {paint_count}")
    log.info(f"{'='*50}")

    # Tablo istatistikleri
    log.info("\n📊 Tablo İstatistikleri:")
    tables = ["markalar", "seriler", "modeller", "yakit_tipleri", "vites_tipleri",
              "kasa_tipleri", "renkler", "iller", "ilceler", "ilanlar", "boya_detaylari"]
    for t in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {t}")
        count = cursor.fetchone()[0]
        log.info(f"   {t}: {count} kayıt")

    cursor.close()
    conn.close()
    log.info("\n✅ Bağlantı kapatıldı. Chatbot veritabanı hazır!")


if __name__ == "__main__":
    main()
