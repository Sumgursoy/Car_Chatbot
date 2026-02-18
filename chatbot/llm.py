"""
Gemini LLM Entegrasyonu
========================
- Doğal dil → SQL dönüşümü
- SQL sonuçlarını özetleme
"""

import os
import re
import google.generativeai as genai
from logger import get_logger

log = get_logger("llm")

# API yapılandırması
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

MODEL_NAME = "gemini-2.5-flash"

SYSTEM_PROMPT_TEMPLATE = """Sen bir araç ilanı veritabanı asistanısın. Kullanıcılar sana Türkçe sorular soracak ve sen bu soruları MySQL sorgularına çevireceksin.

## Veritabanı Şeması
{schema}

## Tablolardaki Değerler
{sample_values}

## Tablo İlişkileri
- ilanlar.marka_id → markalar.id
- ilanlar.seri_id → seriler.id (seriler.marka_id → markalar.id)
- ilanlar.model_id → modeller.id (modeller.seri_id → seriler.id)
- ilanlar.yakit_tipi_id → yakit_tipleri.id
- ilanlar.vites_tipi_id → vites_tipleri.id
- ilanlar.kasa_tipi_id → kasa_tipleri.id
- ilanlar.renk_id → renkler.id
- ilanlar.il_id → iller.id
- ilanlar.ilce_id → ilceler.id (ilceler.il_id → iller.id)
- boya_detaylari.ilan_db_id → ilanlar.id

## Kurallar
1. SADECE SELECT sorguları üret. INSERT, UPDATE, DELETE, DROP kesinlikle YASAK.
2. Sonuçları LIMIT ile sınırla (varsayılan LIMIT 20). Kullanıcı "hepsini" istemediği sürece.
3. Fiyat, kilometre gibi sayısal alanlarda FORMAT kullanma — ham sayı döndür.
4. JOIN kullanırken alias kullan (i, m, s, vb.).
5. Türkçe karakter duyarlılığına dikkat et.
6. Sonuçlarda her zaman anlamlı sütunlar göster (marka adı, seri adı vs. — ID değil).
7. Eğer soru veritabanıyla ilgili değilse, SQL üretme ve kibarca açıkla.

## Yanıt Formatı
Eğer SQL sorgusu gerekiyorsa, yanıtını şu formatta ver:
```sql
SELECT sorgusu buraya
```

Eğer SQL gerekmiyorsa (genel sohbet, bilgi sorusu vs.), düz metin olarak yanıtla.
"""


def get_model(schema: str, sample_values: str):
    """System prompt ile yapılandırılmış Gemini modeli döner."""
    system_prompt = SYSTEM_PROMPT_TEMPLATE.format(
        schema=schema,
        sample_values=sample_values
    )
    model = genai.GenerativeModel(
        model_name=MODEL_NAME,
        system_instruction=system_prompt
    )
    return model


def extract_sql(response_text: str) -> str | None:
    """Gemini yanıtından SQL sorgusunu çıkarır."""
    # ```sql ... ``` bloğunu ara
    match = re.search(r"```sql\s*(.*?)\s*```", response_text, re.DOTALL)
    if match:
        sql = match.group(1).strip()
        # Güvenlik kontrolü
        if sql.upper().startswith("SELECT"):
            log.info(f"SQL çıkarıldı: {sql[:150]}")
            return sql
        else:
            log.warning(f"SELECT olmayan SQL engellendi: {sql[:100]}")
    else:
        log.debug("Yanıtta SQL bloğu bulunamadı — düz metin yanıt")
    return None


def summarize_results(model, chat, question: str, columns: list, rows: list, sql: str) -> str:
    """SQL sonuçlarını doğal dile çevirir."""
    log.info(f"Sonuç özetleniyor: {len(rows)} satır, soru: {question[:80]}")
    if not rows:
        return "Bu kriterlere uygun sonuç bulunamadı. 🔍"

    # Sonuçları metin formatına çevir
    if len(rows) <= 20:
        result_text = f"Sütunlar: {', '.join(columns)}\n"
        for row in rows:
            result_text += " | ".join(str(v) for v in row) + "\n"
    else:
        result_text = f"{len(rows)} satır bulundu. İlk 10:\n"
        result_text += f"Sütunlar: {', '.join(columns)}\n"
        for row in rows[:10]:
            result_text += " | ".join(str(v) for v in row) + "\n"

    summary_prompt = f"""Kullanıcı şunu sordu: "{question}"

Çalıştırılan SQL: {sql}

Sonuçlar:
{result_text}

Bu sonuçları kullanıcıya Türkçe olarak doğal ve anlaşılır bir şekilde açıkla. 
- Sayısal değerleri okunabilir formatta yaz (örn: 845.000 TL, 120.000 km).
- Kısa ve öz ol ama bilgilendirici.
- Eğer veriden ilginç bir çıkarım yapılabiliyorsa ekle.
- Emoji kullanabilirsin."""

    response = chat.send_message(summary_prompt)
    return response.text
