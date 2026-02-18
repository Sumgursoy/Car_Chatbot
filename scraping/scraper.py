"""
arabam.com Scraper — 10.000-15.000 Güncel İlan
================================================
Kullanım:
  python scraper.py                  # Tam çalıştır (~12.500 ilan)
  python scraper.py --test           # Test: 1 marka, 1 sayfa
  python scraper.py --devam          # Kaldığı yerden devam et
"""

import requests
from bs4 import BeautifulSoup
import json, time, random, re, os, sys, logging
from datetime import datetime

# ─────────────────────────── CONFIG ───────────────────────────

BASE = "https://www.arabam.com"

BRANDS = {
    "Volkswagen": "volkswagen", "Fiat": "fiat", "Renault": "renault",
    "Toyota": "toyota", "Honda": "honda", "BMW": "bmw",
    "Mercedes-Benz": "mercedes-benz", "Audi": "audi",
    "Hyundai": "hyundai", "Kia": "kia", "Opel": "opel",
    "Peugeot": "peugeot", "Ford": "ford", "Citroen": "citroen",
    "Skoda": "skoda", "Dacia": "dacia", "Nissan": "nissan",
    "Volvo": "volvo", "Seat": "seat", "Mazda": "mazda",
}

TARGET = 12500                     # toplam hedef ilan
PER_BRAND = TARGET // len(BRANDS)  # marka başı ~625
OUTPUT     = "data/arabam_dataset.json"
PROGRESS   = "data/progress.json"
LOGFILE    = "data/scrape.jsonl"

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(levelname)-5s │ %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("scraper")

# ─────────────────────────── HTTP ─────────────────────────────

def fetch(url, session, retries=3):
    """Rate-limited GET with retry."""
    for i in range(retries):
        try:
            time.sleep(random.uniform(1.5, 3.0))
            r = session.get(url, timeout=20)
            if r.status_code == 200:
                return r
            if r.status_code == 429:
                w = 30 * (i + 1)
                log.warning(f"⏳ 429 rate-limit, {w}s bekleniyor…")
                time.sleep(w)
            elif r.status_code == 403:
                log.warning(f"🚫 403 Engel: {url}")
                time.sleep(15)
            else:
                log.warning(f"HTTP {r.status_code}: {url}")
        except requests.RequestException as e:
            log.warning(f"Hata ({i+1}/{retries}): {e}")
            time.sleep(5 * (i + 1))
    return None

def new_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Accept-Language": "tr-TR,tr;q=0.9",
        "Accept": "text/html,application/xhtml+xml",
    })
    return s

# ─────────────────────────── PARSERS ──────────────────────────

def parse_price(text):
    """'845.000 TL' → 845000"""
    if not text: return None
    cleaned = re.sub(r"[^\d.,]", "", str(text))
    if not cleaned: return None
    cleaned = cleaned.replace(".", "").replace(",", ".")
    try:
        n = float(cleaned)
        return int(n) if n == int(n) else n
    except ValueError:
        return None

def parse_int(text):
    """'90.000 km' → 90000,  '1248 cc' → 1248"""
    if not text: return None
    if isinstance(text, (int, float)): return int(text)
    cleaned = re.sub(r"[^\d.,]", "", str(text))
    if not cleaned: return None
    cleaned = cleaned.replace(".", "").replace(",", ".")
    try:
        return int(float(cleaned))
    except ValueError:
        return None

def parse_float(text):
    """'4,1 lt' → 4.1,  '12,8 sn' → 12.8"""
    if not text: return None
    cleaned = re.sub(r"[^\d,.]", "", str(text))
    if not cleaned: return None
    cleaned = cleaned.replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None

# ─────────────────────────── LISTING PAGE ─────────────────────

def get_listing_urls(brand_slug, page, session):
    """Bir listing sayfasından ilan URL'leri + toplam sayfa sayısı döner."""
    url = f"{BASE}/ikinci-el/otomobil/{brand_slug}?page={page}"
    r = fetch(url, session)
    if not r:
        return [], 0

    soup = BeautifulSoup(r.text, "lxml")

    # Toplam sayfa
    total = 1
    # Yöntem 1: span#js-hook-for-total-page-count (en güvenilir)
    page_span = soup.find("span", id="js-hook-for-total-page-count")
    if page_span:
        try:
            total = int(page_span.get_text(strip=True))
        except ValueError:
            pass

    # Yöntem 2: Sayfa linklerinden en büyük sayıyı bul
    if total <= 1:
        for a in soup.find_all("a", href=True):
            m = re.search(r"page=(\d+)", a["href"])
            if m:
                total = max(total, int(m.group(1)))

    # İlan URL'leri  — /ilan/.../{id} formatında
    seen = set()
    urls = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/ilan/" not in href:
            continue
        # ID check — son kısım sayı olmalı
        m = re.search(r"/(\d{5,})$", href)
        if not m:
            continue
        full = BASE + href if href.startswith("/") else href
        if full not in seen:
            seen.add(full)
            urls.append(full)

    return urls, total

# ─────────────────────────── DETAIL PAGE ──────────────────────

# Sayfadaki key→value eşleme tablosu
FIELD_MAP = {
    "Yıl":           ("yil",           parse_int),
    "Kilometre":     ("kilometre",     parse_int),
    "Yakıt Tipi":    ("yakit_tipi",    str),
    "Vites Tipi":    ("vites_tipi",    str),
    "Kasa Tipi":     ("kasa_tipi",     str),
    "Renk":          ("renk",          str),
    "Motor Hacmi":   ("motor_hacmi_cc", parse_int),
    "Motor Gücü":    ("motor_gucu_hp", parse_int),
    "Çekiş":         ("cekis",         str),
    "Araç Durumu":   ("arac_durumu",   str),
    "Kimden":        ("kimden",        str),
    "Takasa Uygun":  ("takasa_uygun",  str),
    "Ağır Hasarlı":  ("agir_hasarli",  str),
    "Boya-değişen":  ("boya_degisen_ozet", str),
    "Sınıfı":        ("sinif",         str),
    "Silindir Sayısı": ("silindir",    parse_int),
    "Tork":          ("tork_nm",       parse_int),
    "Ort. Yakıt Tüketimi":     ("yakit_ort_lt", parse_float),
    "Ortalama Yakıt Tüketimi": ("yakit_ort_lt", parse_float),
    "Garanti Durumu":          ("garanti",      str),
}


def scrape_detail(url, session):
    """Tek bir ilan detay sayfasını parse eder → dict."""
    r = fetch(url, session)
    if not r:
        return None

    soup = BeautifulSoup(r.text, "lxml")
    data = {"ilan_url": url}

    # — İlan ID
    m = re.search(r"/(\d+)$", url)
    if m:
        data["ilan_id"] = m.group(1)

    # — Başlık (h1)
    h1 = soup.find("h1")
    if h1:
        data["baslik"] = h1.get_text(strip=True)

    # — Breadcrumb → Marka / Seri / Model / Versiyon
    #   DOM: Otomobil > Fiat > Egea > 1.3 Multijet > Easy
    breadcrumbs = []
    for a in soup.select("a[href*='/ikinci-el/otomobil']"):
        txt = a.get_text(strip=True)
        if txt and txt != "Otomobil":
            breadcrumbs.append(txt)
    if breadcrumbs:
        data["marka"] = breadcrumbs[0] if len(breadcrumbs) > 0 else None
        data["seri"]  = breadcrumbs[1] if len(breadcrumbs) > 1 else None
        data["model"] = breadcrumbs[2] if len(breadcrumbs) > 2 else None
        data["versiyon"] = breadcrumbs[3] if len(breadcrumbs) > 3 else None

    # — Fiyat   (DOM: StaticText "Fiyat" → StaticText "845.000 TL")
    #   HTML'de genelde: <span>845.000 TL</span> veya benzer bir element
    price_found = False
    # Yöntem 1: meta tag
    meta = soup.find("meta", {"itemprop": "price"})
    if meta and meta.get("content"):
        data["fiyat"] = parse_price(meta["content"])
        price_found = True

    if not price_found:
        # Yöntem 2: Fiyat metnini ara
        for el in soup.find_all(string=re.compile(r"\d{2,3}\.\d{3}\s*TL")):
            txt = el.strip()
            # "Fiyat" label'ından sonraki bir elment olmalı
            if "TL" in txt and parse_price(txt):
                data["fiyat"] = parse_price(txt)
                price_found = True
                break

    # — Key-Value alanları (Genel Bakış, Motor ve Performans, vs.)
    #   DOM yapısı: Art arda gelen StaticText çiftleri (key, value)
    #   Bunlar genelde <li> veya <div> içinde <span> çiftleri olarak gelir
    all_kv = _extract_key_values(soup)
    for key, value in all_kv:
        if key in FIELD_MAP:
            field_name, converter = FIELD_MAP[key]
            try:
                data[field_name] = converter(value)
            except (ValueError, TypeError):
                data[field_name] = value

    # — İlan Tarihi
    for key, value in all_kv:
        if key == "İlan Tarihi":
            data["ilan_tarihi"] = value
        elif key == "İlan No":
            data["ilan_id"] = value

    # — Konum (İl / İlçe)
    #   DOM: "Kayabaşı Mh. Başakşehir, İstanbul"
    _parse_location(soup, data)

    # — Boya-Değişen detay  (graphics-symbol elements + status text)
    _parse_paint_detail(soup, data)

    # — Tramer
    #   DOM: StaticText "Tramer" → StaticText "155.519 TL"
    for key, value in all_kv:
        if key == "Tramer":
            data["tramer_tl"] = parse_price(value)

    # — İlan Açıklaması (Açıklama heading'i altındaki tüm metin)
    _parse_description(soup, data)

    return data


def _extract_key_values(soup):
    """Sayfadaki tüm key-value çiftlerini çıkarır.

    arabam.com'da spec alanları genelde şu HTML yapısında:
      <li>
        <span>Yıl</span>
        <span>2023</span>
      </li>
    veya bazen:
      <div class="...">
        <span>Motor Hacmi</span>
        <span>1248 cc</span>
      </div>
    """
    pairs = []

    # Yöntem 1: İçinde tam 2 span olan li/div elementleri
    for container in soup.find_all(["li", "div"]):
        spans = container.find_all("span", recursive=False)
        if len(spans) == 2:
            key = spans[0].get_text(strip=True)
            val = spans[1].get_text(strip=True)
            if key and val and len(key) < 60:
                pairs.append((key, val))

    # Yöntem 2: dt/dd çiftleri
    for dt in soup.find_all("dt"):
        dd = dt.find_next_sibling("dd")
        if dd:
            pairs.append((dt.get_text(strip=True), dd.get_text(strip=True)))

    # Yöntem 3: Tablo satırları
    for tr in soup.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) == 2:
            pairs.append((tds[0].get_text(strip=True), tds[1].get_text(strip=True)))

    return pairs


def _parse_location(soup, data):
    """Konum bilgisini çıkarır.

    Detay sayfasında konum genelde ilan başlığının altında görünür:
      'Kayabaşı Mh. Başakşehir, İstanbul'
    veya başlıkta: '... 2023 Model İstanbul 90.000 km Beyaz'
    """
    # Yöntem 1: Başlıktan parse et
    baslik = data.get("baslik", "")
    if baslik:
        # "... Model İstanbul 90.000 km ..." formatı
        m = re.search(
            r"Model\s+([A-ZÇĞİÖŞÜa-zçğıöşü]+(?:\s[A-ZÇĞİÖŞÜa-zçğıöşü]+)?)\s+\d",
            baslik
        )
        if m:
            data["il"] = m.group(1).strip()

    # Yöntem 2: Sayfa içindeki konum metni — "İl, İlçe" formatında
    # Genelde ilan başlığı yanındaki küçük konum text'i
    for el in soup.find_all(string=re.compile(r",\s*(İstanbul|Ankara|İzmir|Bursa|Antalya|Adana|Konya|Kocaeli|Mersin|Denizli|Aydın|Muğla|Trabzon|Gaziantep|Samsun|Eskişehir|Sakarya|Kayseri|Diyarbakır|Manisa|Hatay|Malatya|Batman|Afyonkarahisar|Zonguldak|Balıkesir|Tekirdağ|Edirne|Çanakkale|Elazığ|Erzurum|Düzce|Bolu|Yalova|Karabük|Kastamonu|Giresun|Ordu|Rize|Artvin|Sinop|Tokat|Amasya|Çorum|Kırıkkale|Aksaray|Nevşehir|Niğde|Karaman|Isparta|Burdur|Uşak|Kütahya|Bilecik|Mardin|Şanlıurfa|Adıyaman|Kahramanmaraş|Osmaniye|Tunceli|Bingöl|Bitlis|Van|Hakkari|Muş|Ağrı|Iğdır|Kars|Ardahan|Şırnak|Siirt|Bartın|Kırşehir|Kırklareli|Bayburt|Gümüşhane|Yozgat|Sivas|Çankırı)")):
        txt = el.strip()
        parts = [p.strip() for p in txt.split(",")]
        if len(parts) >= 2:
            data["ilce"] = parts[-2] if len(parts) > 2 else parts[0]
            data["il"] = parts[-1]
            break

    # Yöntem 3: Lokasyon metnini bul (adresimsi yapı)
    if "il" not in data:
        for el in soup.find_all(string=re.compile(r"(Başakşehir|Beylikdüzü|Esenyurt|Kadıköy|Osmangazi|Muratpaşa|Nazilli|Merkezefendi|Adapazarı|Yenimahalle|Sultanbeyli|Avcılar|Yıldırım|Sinanpaşa|Yatağan|Ereğli)")):
            txt = el.strip()
            if "," in txt:
                parts = [p.strip() for p in txt.split(",")]
                if len(parts) >= 2:
                    data["ilce"] = parts[0].split()[-1] if parts[0] else None
                    data["il"] = parts[-1]
                    break


def _parse_paint_detail(soup, data):
    """Boya-değişen detaylarını çıkarır.

    DOM'da graphics-symbol elements var:
      'Ön Tampon: Belirtilmemiş', 'Sol Arka Çamurluk: Değişmiş', vs.
    """
    paint = {}

    # SVG/graphics-symbol elementlerinden
    for sym in soup.find_all(attrs={"role": "graphics-symbol"}):
        label = sym.get("aria-label", "") or ""
        if not label:
            # Bazen alt attribute'ta olabilir
            label = sym.get_text(strip=True)
        if ":" in label:
            part, status = label.rsplit(":", 1)
            paint[part.strip()] = status.strip()

    # Alternatif: title attribute içeren SVG elementleri
    if not paint:
        for el in soup.find_all("title"):
            txt = el.get_text(strip=True)
            if ":" in txt and any(w in txt for w in ["Tampon", "Kaput", "Kapı", "Çamurluk", "Tavan"]):
                part, status = txt.rsplit(":", 1)
                paint[part.strip()] = status.strip()

    if paint:
        data["boya_degisen_detay"] = paint
        # Özet istatistikler
        boyali = sum(1 for v in paint.values() if "Boyalı" in v or "boyalı" in v)
        degisen = sum(1 for v in paint.values() if "Değişmiş" in v or "değişmiş" in v)
        orijinal = sum(1 for v in paint.values() if "Orijinal" in v or "Orjinal" in v)
        if boyali or degisen or orijinal:
            parts = []
            if degisen: parts.append(f"{degisen} değişen")
            if boyali:  parts.append(f"{boyali} boyalı")
            if orijinal: parts.append(f"{orijinal} orijinal")
            data["boya_degisen_ozet"] = ", ".join(parts)


def _parse_description(soup, data):
    """İlan açıklamasını çıkarır.

    'Açıklama' heading'i altındaki içerik bloğu.
    """
    desc_heading = None
    for h in soup.find_all(["h5", "h4", "h3", "h2"]):
        if h.get_text(strip=True) == "Açıklama":
            desc_heading = h
            break

    if desc_heading:
        # Heading'den sonraki kardeş elementlerden metni topla
        texts = []
        for sib in desc_heading.find_next_siblings():
            # Bir sonraki heading'e gelince dur
            if sib.name in ["h2", "h3", "h4", "h5"]:
                break
            txt = sib.get_text(separator="\n", strip=True)
            if txt:
                texts.append(txt)
        if texts:
            data["ilan_aciklamasi"] = "\n".join(texts)
            return

    # Fallback: div#TextContent veya class içinde description
    for sel in ["#TextContent", ".detail-description", "[class*='description']"]:
        el = soup.select_one(sel)
        if el:
            data["ilan_aciklamasi"] = el.get_text(separator="\n", strip=True)
            return

# ─────────────────────────── PROGRESS ─────────────────────────

def load_progress():
    if os.path.exists(PROGRESS):
        with open(PROGRESS, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"done_brands": [], "done_urls": [], "count": 0}

def save_progress(prog):
    os.makedirs(os.path.dirname(PROGRESS), exist_ok=True)
    with open(PROGRESS, "w", encoding="utf-8") as f:
        json.dump(prog, f, ensure_ascii=False)

def append_jsonl(record):
    os.makedirs(os.path.dirname(LOGFILE), exist_ok=True)
    with open(LOGFILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

def save_json(data):
    """Ana JSON çıktısını kaydet."""
    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    log.info(f"  💾 JSON kaydedildi: {len(data)} ilan → {OUTPUT}")

# ─────────────────────────── MAIN ─────────────────────────────

def main():
    test_mode = "--test" in sys.argv
    resume    = "--devam" in sys.argv

    if test_mode:
        log.info("🧪 TEST MODU — 1 marka, 1 sayfa")
        brands = {"Fiat": "fiat"}
        max_per_brand = 20
    else:
        brands = BRANDS
        max_per_brand = PER_BRAND

    # Progress
    prog = load_progress() if resume else {"done_brands": [], "done_urls": set(), "count": 0}
    if isinstance(prog.get("done_urls"), list):
        prog["done_urls"] = set(prog["done_urls"])

    all_data = []
    session = new_session()

    # Eğer devam ediyorsak, önceki verileri yükle
    if resume and os.path.exists(LOGFILE):
        with open(LOGFILE, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    all_data.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
        log.info(f"📂 {len(all_data)} önceki kayıt yüklendi")

    total_collected = len(all_data)

    try:
        for brand_name, brand_slug in brands.items():
            if brand_name in prog.get("done_brands", []):
                log.info(f"⏭️  {brand_name} zaten tamamlanmış, atlıyorum")
                continue

            log.info(f"\n{'='*50}")
            log.info(f"🚗 {brand_name} ilanları toplanıyor…")
            log.info(f"{'='*50}")

            brand_count = 0

            # İlk sayfayı çek, toplam sayfa sayısını bul
            first_urls, total_pages = get_listing_urls(brand_slug, 1, session)
            if not first_urls:
                log.warning(f"❌ {brand_name} — listing sayfası alınamadı")
                continue

            # Kaç sayfa tarayalım? (marka başına max ilan / sayfa başına ~20)
            pages_needed = min(total_pages, (max_per_brand // 18) + 2)
            log.info(f"  📄 {total_pages} sayfa mevcut, {pages_needed} sayfa taranacak")

            # Tüm sayfalardan URL topla
            all_urls = list(first_urls)
            for p in range(2, pages_needed + 1):
                urls, _ = get_listing_urls(brand_slug, p, session)
                all_urls.extend(urls)
                log.info(f"  📄 Sayfa {p}/{pages_needed} — {len(urls)} URL")

            # Tekrarları kaldır
            all_urls = list(dict.fromkeys(all_urls))
            log.info(f"  🔗 Toplam {len(all_urls)} benzersiz URL")

            # Detayları çek
            for i, url in enumerate(all_urls):
                if brand_count >= max_per_brand:
                    break
                if total_collected >= TARGET and not test_mode:
                    break
                if url in prog.get("done_urls", set()):
                    continue

                record = scrape_detail(url, session)
                if record and record.get("fiyat"):
                    all_data.append(record)
                    append_jsonl(record)
                    brand_count += 1
                    total_collected += 1
                    prog["done_urls"].add(url) if isinstance(prog["done_urls"], set) else prog["done_urls"].append(url)

                    if total_collected % 10 == 0:
                        log.info(
                            f"  ✅ [{total_collected}/{TARGET}] "
                            f"{record.get('marka','?')} {record.get('seri','')} — "
                            f"{record.get('yil','?')} — {record.get('fiyat','?')} TL — "
                            f"{record.get('il','?')}"
                        )

                    # Her 50 ilandan sonra progress + JSON kaydet
                    if total_collected % 50 == 0:
                        save_progress({
                            "done_brands": prog.get("done_brands", []),
                            "done_urls": list(prog["done_urls"]) if isinstance(prog["done_urls"], set) else prog["done_urls"],
                            "count": total_collected,
                        })
                        save_json(all_data)
                else:
                    log.debug(f"  ⚠️ Atlandı (veri eksik): {url}")

                # Session yenile (her 200 istekte)
                if total_collected % 200 == 0:
                    session = new_session()
                    log.info("  🔄 Session yenilendi")

            # Marka tamamlandı
            prog.setdefault("done_brands", []).append(brand_name)
            save_progress({
                "done_brands": prog["done_brands"],
                "done_urls": list(prog["done_urls"]) if isinstance(prog["done_urls"], set) else prog["done_urls"],
                "count": total_collected,
            })
            save_json(all_data)
            log.info(f"  ✅ {brand_name}: {brand_count} ilan toplandı (toplam: {total_collected})")

            if total_collected >= TARGET and not test_mode:
                log.info(f"🎯 Hedefe ulaşıldı: {total_collected} ilan")
                break

    except KeyboardInterrupt:
        log.info(f"\n⛔ Ctrl+C — Güvenli kapanış…")

    # Her durumda (normal bitiş veya Ctrl+C) JSON'u kaydet
    save_json(all_data)
    save_progress({
        "done_brands": prog.get("done_brands", []),
        "done_urls": list(prog["done_urls"]) if isinstance(prog["done_urls"], set) else prog["done_urls"],
        "count": total_collected,
    })

    log.info(f"\n{'='*50}")
    log.info(f"🏁 {'DURDURULDU' if total_collected < TARGET else 'TAMAMLANDI'}!")
    log.info(f"   Toplam: {len(all_data)} ilan")
    log.info(f"   Çıktı:  {OUTPUT}")
    log.info(f"   Devam etmek için: python scraper.py --devam")
    log.info(f"{'='*50}")

    # Test modunda örnek veri göster
    if test_mode and all_data:
        log.info("\n📋 Örnek kayıt:")
        sample = all_data[0]
        for k, v in sample.items():
            if k == "ilan_aciklamasi":
                v = v[:100] + "…" if len(str(v)) > 100 else v
            if k == "boya_degisen_detay":
                v = json.dumps(v, ensure_ascii=False)
            log.info(f"   {k}: {v}")


if __name__ == "__main__":
    main()
