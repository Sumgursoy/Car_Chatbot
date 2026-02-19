"""
Vision Module — Crawl4AI + Gemini Vision
==========================================
Kullanıcının verdiği ilan URL'sindeki fotoğrafları Crawl4AI ile çeker,
Gemini 2.0 Flash Vision ile analiz eder.

Akış:
  1. Crawl4AI → URL'ye git, screenshot + görselleri topla
  2. Görselleri base64'e çevir
  3. Gemini Vision'a gönder → detaylı Türkçe analiz al
"""

import os
import asyncio
import base64
import json
import httpx
from logger import get_logger

log = get_logger("vision")


# ─────────────── CRAWL4AI İLE GÖRSEL TOPLAMA ───────────────

async def crawl_listing_images(url: str) -> dict:
    """
    Verilen ilan URL'sine Crawl4AI ile gidip görselleri ve screenshot'u toplar.

    Returns:
        {
            "screenshot_b64": str | None,
            "images_b64": list[str],
            "image_urls": list[str],
            "page_title": str,
            "page_text": str       # ilan açıklama metni (markdown)
        }
    """
    from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode

    log.info(f"Crawl başlatılıyor: {url}")

    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        screenshot=True,
        screenshot_wait_for=2.0,
        wait_for_images=True,
        scan_full_page=True,
        page_timeout=30000,
        verbose=False,
    )

    try:
        async with AsyncWebCrawler() as crawler:
            result = await crawler.arun(url=url, config=run_config)

            if not result.success:
                log.error(f"Crawl başarısız: {result.error_message}")
                return {
                    "hata": f"Sayfa yüklenemedi: {result.error_message}",
                    "screenshot_b64": None,
                    "images_b64": [],
                    "image_urls": [],
                    "page_title": "",
                    "page_text": "",
                }

            # Screenshot (base64)
            screenshot_b64 = result.screenshot if result.screenshot else None
            log.info(f"Screenshot: {'✅' if screenshot_b64 else '❌'}")

            # Sayfa başlığı ve metni
            page_title = ""
            if hasattr(result, 'metadata') and result.metadata:
                page_title = result.metadata.get("title", "")
            page_text = result.markdown_v2.raw_markdown if hasattr(result, 'markdown_v2') and result.markdown_v2 else (result.markdown or "")

            # Görselleri topla
            all_images = result.media.get("images", []) if result.media else []
            log.info(f"Toplam {len(all_images)} görsel bulundu")

            # Kaliteli görselleri filtrele (score > 3, küçük ikonları atla)
            quality_images = []
            for img in all_images:
                src = img.get("src", "")
                score = img.get("score", 0)
                # Küçük ikonları, logo'ları ve placeholder'ları atla
                if not src or "logo" in src.lower() or "icon" in src.lower():
                    continue
                if "placeholder" in src.lower() or "avatar" in src.lower():
                    continue
                if score is not None and score >= 2:
                    quality_images.append(img)
                elif score is None:
                    # Score yoksa da ekle
                    quality_images.append(img)

            # En iyi 5 görseli seç (skora göre)
            quality_images.sort(key=lambda x: x.get("score", 0), reverse=True)
            selected = quality_images[:5]
            log.info(f"Seçilen görsel sayısı: {len(selected)}")

            # Görselleri indir ve base64'e çevir
            image_urls = [img["src"] for img in selected]
            images_b64 = await _download_images_as_base64(image_urls)

            return {
                "screenshot_b64": screenshot_b64,
                "images_b64": images_b64,
                "image_urls": image_urls,
                "page_title": page_title,
                "page_text": page_text[:2000],  # İlk 2000 karakter yeterli
            }

    except Exception as e:
        log.error(f"Crawl hatası: {e}")
        return {
            "hata": str(e),
            "screenshot_b64": None,
            "images_b64": [],
            "image_urls": [],
            "page_title": "",
            "page_text": "",
        }


async def _download_images_as_base64(urls: list[str]) -> list[str]:
    """Görsel URL'lerini indirip base64 string olarak döner."""
    results = []
    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        for url in urls:
            try:
                resp = await client.get(url)
                if resp.status_code == 200 and resp.headers.get("content-type", "").startswith("image"):
                    b64 = base64.b64encode(resp.content).decode("utf-8")
                    results.append(b64)
                    log.info(f"  ✅ İndirildi: {url[:80]}...")
                else:
                    log.warning(f"  ⚠️ Atlandı ({resp.status_code}): {url[:80]}...")
            except Exception as e:
                log.warning(f"  ❌ İndirme hatası: {e} — {url[:80]}")
    return results


# ─────────────── GEMİNİ VİSİON ANALİZİ ───────────────

async def analyze_images_with_gemini(
    images_b64: list[str],
    screenshot_b64: str | None,
    page_text: str = "",
) -> str:
    """
    Görselleri Gemini 2.0 Flash Vision'a gönderip Türkçe analiz alır.
    """
    import google.generativeai as genai

    genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
    model = genai.GenerativeModel("gemini-2.0-flash")

    # Vision prompt
    prompt = f"""Sen bir araç uzmanısın. Aşağıdaki görseller bir araç ilanından alınmıştır.

Görselleri dikkatle incele ve şu başlıklarda detaylı analiz yap:

## 📋 Analiz Başlıkları

1. **Genel Durum**: Aracın genel görünümü, temizliği, bakımı
2. **Boya ve Kaporta**: Boya durumu, çizik, ezik, pas, boyalı/değişen parça belirtileri
3. **İç Mekan**: Koltuk durumu (aşınma, yırtık, sigara yanığı), gösterge paneli, tavan döşemesi
4. **Panel Aralıkları**: Panel boşlukları simetrik mi? Kaza geçmişi belirtisi var mı?
5. **Jant ve Lastikler**: Jant çizikleri, lastik durumu, aşınma
6. **Tutarsızlık Tespiti**: İlan metninde yazanlar ile fotoğraflar arasında çelişki var mı?

{f'## İlan Metni (Referans):' if page_text else ''}
{page_text[:1000] if page_text else ''}

## Önemli
- Türkçe analiz yap
- Net ve dürüst ol, abartma ama gizleme de
- Emoji kullanarak okunabilirliği artır
- Sonuçta 1-10 arası bir "Görsel Güvenilirlik Skoru" ver
"""

    # Görselleri hazırla
    parts = [prompt]

    # Screenshot'u ekle
    if screenshot_b64:
        try:
            img_bytes = base64.b64decode(screenshot_b64)
            parts.append({
                "mime_type": "image/png",
                "data": img_bytes,
            })
        except Exception as e:
            log.warning(f"Screenshot decode hatası: {e}")

    # Galeri görsellerini ekle
    for i, img_b64 in enumerate(images_b64[:5]):
        try:
            img_bytes = base64.b64decode(img_b64)
            # MIME type tahmini (çoğu JPEG olacak)
            mime = "image/jpeg"
            if img_bytes[:4] == b'\x89PNG':
                mime = "image/png"
            elif img_bytes[:4] == b'RIFF':
                mime = "image/webp"
            parts.append({
                "mime_type": mime,
                "data": img_bytes,
            })
        except Exception as e:
            log.warning(f"Görsel {i} decode hatası: {e}")

    if len(parts) < 2:
        return "❌ Analiz edilecek görsel bulunamadı."

    log.info(f"Gemini Vision'a {len(parts) - 1} görsel gönderiliyor...")

    try:
        response = model.generate_content(parts)
        analysis = response.text
        log.info(f"Gemini analiz tamamlandı ({len(analysis)} karakter)")
        return analysis
    except Exception as e:
        log.error(f"Gemini Vision hatası: {e}")
        return f"❌ Gemini Vision analiz hatası: {str(e)}"


# ─────────────── ÜST SEVİYE FONKSİYON ───────────────

async def analyze_listing(url: str) -> dict:
    """
    Üst seviye fonksiyon: URL'den görselleri çek + Gemini Vision ile analiz et.

    Returns:
        {
            "url": str,
            "page_title": str,
            "gorsel_sayisi": int,
            "screenshot_b64": str | None,
            "analiz": str,
            "image_urls": list[str]
        }
    """
    log.info(f"=== İlan Görsel Analizi Başlıyor: {url} ===")

    # 1. Görselleri crawl et
    crawl_data = await crawl_listing_images(url)

    if "hata" in crawl_data and not crawl_data["images_b64"] and not crawl_data["screenshot_b64"]:
        return {
            "url": url,
            "hata": crawl_data["hata"],
            "gorsel_sayisi": 0,
            "analiz": f"❌ Sayfa crawl edilemedi: {crawl_data['hata']}",
        }

    # 2. Gemini Vision ile analiz et
    analysis = await analyze_images_with_gemini(
        images_b64=crawl_data["images_b64"],
        screenshot_b64=crawl_data["screenshot_b64"],
        page_text=crawl_data["page_text"],
    )

    result = {
        "url": url,
        "page_title": crawl_data["page_title"],
        "gorsel_sayisi": len(crawl_data["images_b64"]),
        "screenshot_b64": crawl_data["screenshot_b64"],
        "analiz": analysis,
        "image_urls": crawl_data["image_urls"],
    }

    log.info(f"=== Analiz Tamamlandı: {len(crawl_data['images_b64'])} görsel ===")
    return result
