"""
Veritabanı Veri Kalitesi Düzeltmeleri
======================================
Tek script ile:
  1. Geçersiz il kaydını sil ("100);")
  2. İlçe tablosunu kontrol et
  3. Tramer bilgisini açıklamalardan çıkar → ilanlar.tramer_tl
  4. Boya/değişen özetini açıklamalardan çıkar → ilanlar.boya_degisen_ozet
  5. Boya detaylarını parse et → boya_detaylari tablosu

Kullanım: python fix_data.py
"""

import re, os, logging
from dotenv import load_dotenv
import mysql.connector

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-5s │ %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("fix_data")

DB_CONFIG = {
    "host":     os.getenv("MYSQL_HOST", "localhost"),
    "user":     os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", ""),
    "database": os.getenv("MYSQL_DATABASE", "arabam_chatbot"),
}

# ─────────────── TRAMER EXTRACTION ───────────────

TRAMER_PATTERNS = [
    r'tramer\s*[:;=]?\s*([\d.,\s]+)\s*(?:tl|₺)',
    r'([\d.,\s]+)\s*(?:tl|₺)\s*tramer',
    r'tramer\s+(?:kayd[ıi]|tutar[ıi]|bedeli?)\s*[:;=]?\s*([\d.,\s]+)\s*(?:tl|₺)?',
    r'tramer\s*[:;=]\s*([\d.,]+)',
]

def parse_tramer(text):
    if not text:
        return None
    text_lower = text.lower()
    if 'tramer' not in text_lower:
        return None
    for pattern in TRAMER_PATTERNS:
        m = re.search(pattern, text_lower)
        if m:
            val = m.group(1).strip().replace('.', '').replace(',', '').replace(' ', '')
            try:
                num = int(val)
                if 100 <= num <= 5_000_000:  # makul aralık: 100 TL - 5M TL
                    return num
            except ValueError:
                continue
    return None

# ─────────────── BOYA/DEĞİŞEN EXTRACTION ───────────────

# Araç parçaları
PARCALAR = [
    'kaput', 'tavan', 'bagaj', 'çamurluk', 'kapı', 'tampon',
    'sol ön çamurluk', 'sağ ön çamurluk', 'sol arka çamurluk', 'sağ arka çamurluk',
    'sol ön kapı', 'sağ ön kapı', 'sol arka kapı', 'sağ arka kapı',
    'ön tampon', 'arka tampon',
    'sol taraf', 'sağ taraf', 'sol yan', 'sağ yan',
    'ön panel', 'arka panel',
]

BOYA_PATTERNS = [
    # "boyasız" veya "boyasızdır" 
    r'boyasız',
    # "hatasız boyasız değişensiz"
    r'hatasız\s*boyasız\s*değişensiz',
    # "boya yoktur" / "boya yok"
    r'boya\s+yok(?:tur)?',
    # "X boya vardır" / "X boyalı"
    r'(\w[\w\s]*?)\s+boya(?:lı|sı var|vardır|\s+var)',
    # "X değişen" / "X değişendir"
    r'(\w[\w\s]*?)\s+değişen(?:dir)?',
    # "değişen yoktur" / "değişen yok" 
    r'değişen\s+yok(?:tur)?',
]

def parse_boya_ozet(text):
    """Açıklamadan boya/değişen özeti çıkar."""
    if not text:
        return None
    text_lower = text.lower()
    
    parts = []
    
    # Tam boyasız
    if re.search(r'boyasız\s*değişensiz', text_lower):
        return "Boyasız, Değişensiz"
    
    if re.search(r'hatasız\s*boyasız', text_lower):
        return "Hatasız, Boyasız"
    
    # Boya var mı?
    boya_var = re.search(r'boya\s+(?:var|mevcut)', text_lower)
    degisen_var = re.search(r'değişen\s+(?:var|mevcut)', text_lower)
    boya_yok = re.search(r'boya(?:sız|\s+yok)', text_lower)
    degisen_yok = re.search(r'değişen(?:siz|\s+yok)', text_lower)
    
    if boya_yok:
        parts.append("Boyasız")
    elif boya_var:
        parts.append("Boyalı")
    
    if degisen_yok:
        parts.append("Değişensiz")
    elif degisen_var:
        parts.append("Değişen Var")
    
    return ", ".join(parts) if parts else None


def parse_boya_detay(text):
    """Açıklamadan parça bazlı boya detayı çıkar."""
    if not text:
        return []
    text_lower = text.lower()
    details = []
    
    # "kaput boyalı", "tavan boyasız", "bagaj değişen" vb.
    for parca in PARCALAR:
        # Boyalı
        if re.search(rf'{parca}\s*(?:boyalı|boya(?:lıdır|\s+var))', text_lower):
            details.append((parca.title(), "Boyalı"))
        elif re.search(rf'{parca}\s*(?:boyasız|orijinal|orjinal)', text_lower):
            details.append((parca.title(), "Orijinal"))
        
        # Değişen
        if re.search(rf'{parca}\s*(?:değişen|değişmiş)', text_lower):
            details.append((parca.title(), "Değişen"))
    
    # Genel kalıplar: "sağ ön kapı boyalı, sol arka çamurluk değişen"
    # "X boyalıdır" formatı
    for m in re.finditer(r'((?:sol|sağ)?\s*(?:ön|arka)?\s*(?:kapı|çamurluk|tampon|panel))\s+(boyalı|değişen|orijinal|orjinal)', text_lower):
        parca_adi = m.group(1).strip().title()
        durum = m.group(2).strip().title()
        if durum == "Orjinal":
            durum = "Orijinal"
        if (parca_adi, durum) not in details:
            details.append((parca_adi, durum))
    
    return details


# ─────────────── MAIN ───────────────

def main():
    log.info("🔌 MySQL bağlanıyor…")
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # ═══════════════════════════════════════
    # 1. GEÇERSİZ İL KAYDI SİL
    # ═══════════════════════════════════════
    log.info("\n📍 [1/5] Geçersiz il kayıtları temizleniyor…")
    
    # Geçerli olmayan il adlarını bul
    cursor.execute("SELECT id, ad FROM iller")
    all_iller = cursor.fetchall()
    invalid_iller = [(id, ad) for id, ad in all_iller if not re.match(r'^[A-Za-zÇçĞğİıÖöŞşÜü\s]+$', ad)]
    
    for il_id, il_ad in invalid_iller:
        log.info(f"  ❌ Geçersiz il bulundu: '{il_ad}' (id={il_id})")
        
        # Bu il'e bağlı ilçeleri bul
        cursor.execute("SELECT COUNT(*) FROM ilceler WHERE il_id = %s", (il_id,))
        ilce_count = cursor.fetchone()[0]
        
        # Bu il'e bağlı ilanları NULL'a çek
        cursor.execute("UPDATE ilanlar SET il_id = NULL, ilce_id = NULL WHERE il_id = %s", (il_id,))
        updated = cursor.rowcount
        
        # İlçeleri sil
        cursor.execute("DELETE FROM ilceler WHERE il_id = %s", (il_id,))
        
        # İl'i sil
        cursor.execute("DELETE FROM iller WHERE id = %s", (il_id,))
        
        log.info(f"    → {updated} ilan NULL'a çekildi, {ilce_count} ilçe silindi")
    
    if not invalid_iller:
        log.info("  ✅ Geçersiz il yok")
    
    conn.commit()
    
    # İl sayısını doğrula
    cursor.execute("SELECT COUNT(*) FROM iller")
    il_count = cursor.fetchone()[0]
    log.info(f"  📊 Kalan il sayısı: {il_count}")

    # ═══════════════════════════════════════
    # 2. İLÇE/MAHALLE AYIRMA
    # ═══════════════════════════════════════
    log.info("\n📍 [2/6] İlçe tablosu: mahalle/ilçe ayrıştırılıyor…")
    
    # Mahalle sütunu ekle (yoksa)
    try:
        cursor.execute("ALTER TABLE ilceler ADD COLUMN mahalle VARCHAR(100) DEFAULT NULL AFTER ad")
        log.info("  ➕ 'mahalle' sütunu eklendi")
    except mysql.connector.errors.ProgrammingError:
        log.info("  ℹ️  'mahalle' sütunu zaten mevcut")
    
    # Unique key'i güncelle: (il_id, ad) → (il_id, ad, mahalle)
    try:
        cursor.execute("ALTER TABLE ilceler DROP INDEX uq_ilce")
        log.info("  🔄 Eski unique key (il_id, ad) kaldırıldı")
    except mysql.connector.errors.DatabaseError:
        pass  # zaten yok
    
    # Yeni unique key: il_id + mahalle + ad (mahalle NULL olabilir)
    # NULL mahalleler aynı ilçe adıyla çakışmaz (MySQL'de NULL != NULL)
    try:
        cursor.execute("ALTER TABLE ilceler ADD UNIQUE KEY uq_ilce_mah (il_id, mahalle, ad)")
        log.info("  ✅ Yeni unique key (il_id, mahalle, ad) oluşturuldu")
    except mysql.connector.errors.DatabaseError:
        log.info("  ℹ️  Unique key zaten mevcut")
    conn.commit()
    
    # İlçe adını düz ilçe'ye çevir, mahalle'yi ayır
    # Format: "Doğrugöz Mh. Akşehir" → mahalle="Doğrugöz Mh.", ilce="Akşehir"
    # Format: "Merkez Şehitkamil"      → mahalle=NULL, ilce="Şehitkamil"
    # Format: "Mersinli Mh. Konak"     → mahalle="Mersinli Mh.", ilce="Konak"
    
    cursor.execute("SELECT id, ad FROM ilceler")
    all_ilceler = cursor.fetchall()
    log.info(f"  📊 {len(all_ilceler)} ilçe kaydı bulundu")
    
    split_count = 0
    merkez_count = 0
    
    for ilce_id, full_ad in all_ilceler:
        mahalle = None
        ilce_ad = full_ad
        
        # "Mh." veya "Mah." ile ayır
        mh_match = re.match(r'^(.+?\s+(?:Mh\.|Mah\.|Mahallesi))\s+(.+)$', full_ad)
        if mh_match:
            mahalle = mh_match.group(1).strip()
            ilce_ad = mh_match.group(2).strip()
            split_count += 1
        # "Merkez İlçe" formatı
        elif re.match(r'^Merkez\s+(.+)$', full_ad):
            ilce_ad = re.match(r'^Merkez\s+(.+)$', full_ad).group(1).strip()
            merkez_count += 1
        
        # Güncelle
        cursor.execute(
            "UPDATE ilceler SET ad = %s, mahalle = %s WHERE id = %s",
            (ilce_ad, mahalle, ilce_id)
        )
    
    conn.commit()
    log.info(f"  ✅ {split_count} kayıtta mahalle ayrıldı, {merkez_count} 'Merkez' temizlendi")
    
    # Şimdi duplicate ilçeler olabilir (aynı il_id + aynı ilce adı ama farklı mahalle)
    # İlanların ilce_id referanslarını koruyarak duplicate ilçeleri birleştirmemize GEREK YOK
    # çünkü her mahalle+ilçe kombinasyonu benzersiz bir konum bilgisidir
    
    # Örnek göster
    cursor.execute("""
        SELECT il.ad, ilc.mahalle, ilc.ad 
        FROM ilceler ilc 
        JOIN iller il ON ilc.il_id = il.id 
        ORDER BY il.ad 
        LIMIT 10
    """)
    log.info("  📋 Örnek kayıtlar (il → mahalle → ilçe):")
    for il, mah, ilce in cursor.fetchall():
        log.info(f"    {il} → {mah or '-'} → {ilce}")
    
    cursor.execute("SELECT COUNT(*) FROM ilceler")
    log.info(f"  📊 Toplam ilçe: {cursor.fetchone()[0]}")
    
    # İl'siz ilçe var mı?
    cursor.execute("SELECT COUNT(*) FROM ilceler WHERE il_id NOT IN (SELECT id FROM iller)")
    orphan = cursor.fetchone()[0]
    if orphan > 0:
        log.warning(f"  ⚠️  {orphan} yetim ilçe siliniyor")
        cursor.execute("DELETE FROM ilceler WHERE il_id NOT IN (SELECT id FROM iller)")
        conn.commit()

    # ═══════════════════════════════════════
    # 3. TRAMER BİLGİSİ ÇIKAR
    # ═══════════════════════════════════════
    log.info("\n💰 [3/5] Tramer bilgisi açıklamalardan çıkarılıyor…")
    
    # Önce sıfırla
    cursor.execute("UPDATE ilanlar SET tramer_tl = NULL")
    
    cursor.execute("SELECT id, ilan_aciklamasi FROM ilanlar WHERE ilan_aciklamasi IS NOT NULL")
    rows = cursor.fetchall()
    
    tramer_updated = 0
    tramer_vals = []
    
    for db_id, aciklama in rows:
        tramer = parse_tramer(aciklama)
        if tramer:
            cursor.execute("UPDATE ilanlar SET tramer_tl = %s WHERE id = %s", (tramer, db_id))
            tramer_updated += 1
            tramer_vals.append(tramer)
    
    conn.commit()
    log.info(f"  ✅ {tramer_updated}/{len(rows)} ilanda tramer bulundu")
    if tramer_vals:
        log.info(f"     Ortalama: {sum(tramer_vals)//len(tramer_vals):,} TL")
        log.info(f"     Min: {min(tramer_vals):,} TL — Max: {max(tramer_vals):,} TL")

    # ═══════════════════════════════════════
    # 4. BOYA/DEĞİŞEN ÖZETİ ÇIKAR
    # ═══════════════════════════════════════
    log.info("\n🎨 [4/5] Boya/değişen özeti çıkarılıyor…")
    
    # Önce sıfırla
    cursor.execute("UPDATE ilanlar SET boya_degisen_ozet = NULL")
    
    boya_updated = 0
    
    for db_id, aciklama in rows:
        ozet = parse_boya_ozet(aciklama)
        if ozet:
            cursor.execute("UPDATE ilanlar SET boya_degisen_ozet = %s WHERE id = %s", (ozet, db_id))
            boya_updated += 1
    
    conn.commit()
    log.info(f"  ✅ {boya_updated}/{len(rows)} ilanda boya özeti bulundu")
    
    # Dağılım göster
    cursor.execute("""
        SELECT boya_degisen_ozet, COUNT(*) 
        FROM ilanlar 
        WHERE boya_degisen_ozet IS NOT NULL 
        GROUP BY boya_degisen_ozet 
        ORDER BY COUNT(*) DESC 
        LIMIT 10
    """)
    log.info("  📊 Boya özeti dağılımı:")
    for ozet, cnt in cursor.fetchall():
        log.info(f"     {ozet}: {cnt}")

    # ═══════════════════════════════════════
    # 5. BOYA DETAYLARI TABLOSU
    # ═══════════════════════════════════════
    log.info("\n🔧 [5/5] Boya detayları tablosu dolduruluyor…")
    
    # Tabloyı temizle
    cursor.execute("DELETE FROM boya_detaylari")
    
    detay_count = 0
    ilan_with_detay = 0
    
    for db_id, aciklama in rows:
        detaylar = parse_boya_detay(aciklama)
        if detaylar:
            ilan_with_detay += 1
            for parca, durum in detaylar:
                cursor.execute(
                    "INSERT INTO boya_detaylari (ilan_db_id, parca_adi, durum) VALUES (%s, %s, %s)",
                    (db_id, parca, durum)
                )
                detay_count += 1
    
    conn.commit()
    log.info(f"  ✅ {ilan_with_detay} ilandan {detay_count} boya detayı çıkarıldı")
    
    # Detay dağılımı
    cursor.execute("""
        SELECT durum, COUNT(*) 
        FROM boya_detaylari 
        GROUP BY durum 
        ORDER BY COUNT(*) DESC
    """)
    log.info("  📊 Boya detay dağılımı:")
    for durum, cnt in cursor.fetchall():
        log.info(f"     {durum}: {cnt}")

    # ═══════════════════════════════════════
    # SONUÇ ÖZETİ
    # ═══════════════════════════════════════
    log.info(f"\n{'='*50}")
    log.info(f"🏁 TÜM DÜZELTMELER TAMAMLANDI!")
    log.info(f"{'='*50}")
    
    tables = ["iller", "ilceler", "ilanlar", "boya_detaylari"]
    for t in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {t}")
        log.info(f"   {t}: {cursor.fetchone()[0]} kayıt")
    
    cursor.execute("SELECT COUNT(*) FROM ilanlar WHERE tramer_tl IS NOT NULL")
    log.info(f"   tramer_tl dolu: {cursor.fetchone()[0]}")
    
    cursor.execute("SELECT COUNT(*) FROM ilanlar WHERE boya_degisen_ozet IS NOT NULL")
    log.info(f"   boya_degisen_ozet dolu: {cursor.fetchone()[0]}")

    cursor.close()
    conn.close()
    log.info("\n✅ Bağlantı kapatıldı.")


if __name__ == "__main__":
    main()
