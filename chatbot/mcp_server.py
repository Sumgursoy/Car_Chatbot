"""
Arabam MCP Server
==================
FastMCP ile araç ilanı veritabanı tools sunucusu.

Tools:
  - sql_query: Doğal dil → SQL → sonuç
  - semantic_search: Doğal dil → vektör arama
  - get_car_details: ilan_id ile detay
  - get_stats: Veritabanı istatistikleri

Çalıştırma:
  python mcp_server.py
"""

import os
import json
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()

from db import execute_query, get_schema_info, get_sample_values, get_db_stats, create_view
from vector_db import semantic_search, get_collection_info, ensure_collection
from logger import get_logger

log = get_logger("mcp")

# Gemini
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
EMBED_MODEL = "models/gemini-embedding-001"

# ─── Şema cache ───
_schema_cache = None
_samples_cache = None


def _get_schema():
    global _schema_cache
    if _schema_cache is None:
        _schema_cache = get_schema_info()
    return _schema_cache


def _get_samples():
    global _samples_cache
    if _samples_cache is None:
        _samples_cache = get_sample_values()
    return _samples_cache


def _nl_to_sql(question: str) -> str:
    """Doğal dil sorusunu SQL'e çevirir."""
    model = genai.GenerativeModel("gemini-2.0-flash")

    prompt = f"""Sen bir SQL uzmanısın. Aşağıdaki VIEW şemasına göre soruyu SQL sorgusuna çevir.

{_get_schema()}

Mevcut Değerler:
{_get_samples()}

ÖNEMLİ KURALLAR:
1. SADECE SELECT sorgusu üret
2. v_ilanlar VIEW'ını kullan — JOIN YAPMA! Tüm sütunlar zaten bu VIEW'da mevcut.
3. LIMIT 20 varsayılan
4. Türkçe karakter duyarlılığına dikkat et
5. Eğer boya detayı gerekiyorsa: JOIN boya_detaylari bd ON bd.ilan_db_id = v_ilanlar.id

Örnek sorgular:
- "En ucuz 5 BMW": SELECT baslik, marka, seri, fiyat, yil FROM v_ilanlar WHERE marka = 'BMW' ORDER BY fiyat ASC LIMIT 5
- "İstanbul'daki otomatik araçlar": SELECT baslik, marka, fiyat, vites_tipi FROM v_ilanlar WHERE il = 'İstanbul' AND vites_tipi = 'Otomatik' LIMIT 20
- "Ortalama fiyat": SELECT AVG(fiyat) as ortalama_fiyat FROM v_ilanlar

Soru: {question}

SADECE SQL sorgusunu döndür, başka bir şey yazma."""

    response = model.generate_content(prompt)
    sql = response.text.strip()

    # ```sql ... ``` bloğunu temizle
    if sql.startswith("```sql"):
        sql = sql[6:]
    if sql.startswith("```"):
        sql = sql[3:]
    if sql.endswith("```"):
        sql = sql[:-3]

    return sql.strip()


def _embed_query(text: str) -> list[float]:
    """Sorgu metnini Gemini ile vektörleştirir."""
    result = genai.embed_content(
        model=EMBED_MODEL,
        content=text,
        task_type="retrieval_query"
    )
    return result['embedding']


# ─────────────── TOOL HANDLERS ───────────────


def handle_sql_query(args: dict) -> dict:
    """Doğal dil sorusunu SQL'e çevirip çalıştırır."""
    question = args.get("question", "")
    log.info(f"sql_query: {question}")

    try:
        sql = _nl_to_sql(question)
        log.info(f"SQL: {sql[:200]}")
        columns, rows = execute_query(sql)

        results = []
        for row in rows:
            results.append(dict(zip(columns, [
                str(v) if v is not None else None for v in row
            ])))

        return {
            "sql": sql,
            "columns": columns,
            "row_count": len(rows),
            "results": results
        }
    except ValueError as e:
        return {"error": f"Güvenlik: {e}"}
    except Exception as e:
        log.error(f"sql_query hatası: {e}")
        return {"error": str(e)}


def handle_search_similar_cars(args: dict) -> dict:
    """Semantik araç arama."""
    query = args.get("query", "")
    limit = args.get("limit", 10)
    log.info(f"semantic_search: {query}")

    try:
        query_vector = _embed_query(query)
        results = semantic_search(query_vector, limit=limit)

        cars = []
        for r in results:
            p = r["payload"]
            cars.append({
                "score": round(r["score"], 4),
                "baslik": p.get("baslik", ""),
                "marka": p.get("marka", ""),
                "seri": p.get("seri", ""),
                "model": p.get("model", ""),
                "yil": p.get("yil"),
                "kilometre": p.get("kilometre"),
                "fiyat": p.get("fiyat"),
                "yakit_tipi": p.get("yakit_tipi", ""),
                "vites_tipi": p.get("vites_tipi", ""),
                "kasa_tipi": p.get("kasa_tipi", ""),
                "renk": p.get("renk", ""),
                "il": p.get("il", ""),
                "ilan_id": p.get("ilan_id", "")
            })

        return {"query": query, "result_count": len(cars), "results": cars}

    except Exception as e:
        log.error(f"semantic_search hatası: {e}")
        return {"error": str(e)}


def handle_get_car_details(args: dict) -> dict:
    """İlan detayı."""
    ilan_id = args.get("ilan_id", "")
    log.info(f"get_car_details: {ilan_id}")

    try:
        columns, rows = execute_query(f"""
            SELECT ilan_id, baslik, fiyat, yil, kilometre,
                   motor_hacmi_cc, motor_gucu_hp,
                   tramer_tl, boya_degisen_ozet,
                   marka, seri, model,
                   yakit_tipi, vites_tipi,
                   kasa_tipi, renk, il
            FROM v_ilanlar
            WHERE ilan_id = '{ilan_id}'
            LIMIT 1
        """)

        if not rows:
            return {"error": "İlan bulunamadı"}

        result = dict(zip(columns, [str(v) if v is not None else None for v in rows[0]]))

        try:
            bcols, brows = execute_query(f"""
                SELECT bd.parca_adi, bd.durum
                FROM boya_detaylari bd
                JOIN v_ilanlar v ON bd.ilan_db_id = v.id
                WHERE v.ilan_id = '{ilan_id}'
            """)
            result["boya_detaylari"] = [dict(zip(bcols, row)) for row in brows]
        except Exception:
            result["boya_detaylari"] = []

        return result

    except Exception as e:
        log.error(f"get_car_details hatası: {e}")
        return {"error": str(e)}


def handle_get_database_stats(args: dict) -> dict:
    """Veritabanı istatistikleri."""
    log.info("get_database_stats çağrıldı")
    try:
        db_stats = get_db_stats()
        vector_stats = get_collection_info()
        return {"mysql": db_stats, "qdrant": vector_stats}
    except Exception as e:
        log.error(f"get_stats hatası: {e}")
        return {"error": str(e)}


# ─── Tool registry ───
TOOLS = {
    "sql_query": handle_sql_query,
    "search_similar_cars": handle_search_similar_cars,
    "get_car_details": handle_get_car_details,
    "get_database_stats": handle_get_database_stats,
}


# ─────────────── HTTP SERVER ───────────────

from http.server import HTTPServer, BaseHTTPRequestHandler


class MCPHandler(BaseHTTPRequestHandler):
    """MCP tool çağrıları için HTTP handler."""

    def do_POST(self):
        if self.path == "/call-tool":
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length)
            data = json.loads(body)

            tool_name = data.get("name", "")
            arguments = data.get("arguments", {})

            handler = TOOLS.get(tool_name)
            if handler is None:
                self._respond(404, {"error": f"Tool bulunamadı: {tool_name}"})
                return

            result = handler(arguments)
            self._respond(200, result)

        elif self.path == "/list-tools":
            tool_list = [
                {
                    "name": "sql_query",
                    "description": "Doğal dil sorusunu SQL'e çevirip çalıştırır",
                    "parameters": {"question": "string"}
                },
                {
                    "name": "search_similar_cars",
                    "description": "Semantik araç arama",
                    "parameters": {"query": "string", "limit": "int"}
                },
                {
                    "name": "get_car_details",
                    "description": "İlan detayı getirir",
                    "parameters": {"ilan_id": "string"}
                },
                {
                    "name": "get_database_stats",
                    "description": "Veritabanı istatistikleri",
                    "parameters": {}
                }
            ]
            self._respond(200, {"tools": tool_list})
        else:
            self._respond(404, {"error": "Endpoint bulunamadı"})

    def do_GET(self):
        if self.path == "/health":
            self._respond(200, {"status": "ok"})
        elif self.path == "/list-tools":
            self.do_POST()
        else:
            self._respond(200, {"status": "ok", "server": "Arabam MCP Server"})

    def _respond(self, code: int, data: dict):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode())

    def log_message(self, format, *args):
        # Standart HTTP log'u bastır, kendi logger'ımız var
        pass


# ─────────────── MAIN ───────────────

if __name__ == "__main__":
    PORT = int(os.getenv("MCP_PORT", "8000"))
    log.info("🚀 MCP Server başlatılıyor…")

    create_view()
    ensure_collection()

    server = HTTPServer(("0.0.0.0", PORT), MCPHandler)
    log.info(f"✅ MCP Server çalışıyor: http://0.0.0.0:{PORT}")
    log.info(f"   Tools: {list(TOOLS.keys())}")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("MCP Server durduruluyor…")
        server.server_close()
