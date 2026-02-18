"""
Arabam Chatbot — Streamlit Arayüzü (MCP Client)
==================================================
MCP Server üzerinden araç ilanı veritabanını sorgula.
"""

import os
import json
import streamlit as st
import pandas as pd
import httpx
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()

from logger import get_logger

log = get_logger("app")

# Gemini
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

MCP_SERVER_URL = os.getenv("MCP_SERVER_URL", "http://localhost:8000")

# ─────────────── PAGE CONFIG ───────────────

st.set_page_config(
    page_title=" Arabam Chatbot",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─────────────── CUSTOM CSS ───────────────

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

    * { font-family: 'Inter', sans-serif; }

    /* ── Ana arka plan ── */
    .stApp {
        background: linear-gradient(160deg, #0a0a1a 0%, #1a1040 40%, #0d1f3c 70%, #0a0a1a 100%);
    }

    /* ── Sidebar ── */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, rgba(15, 12, 41, 0.98), rgba(10, 10, 26, 0.98));
        border-right: 1px solid rgba(102, 126, 234, 0.15);
    }

    /* ── Başlık ── */
    .main-header {
        text-align: center;
        padding: 2rem 0 0.5rem 0;
    }
    .main-header h1 {
        background: linear-gradient(135deg, #667eea 0%, #a78bfa 50%, #f093fb 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 2.8rem;
        font-weight: 800;
        letter-spacing: -1px;
        margin: 0;
    }
    .main-header p {
        color: rgba(255,255,255,0.45);
        font-size: 0.95rem;
        font-weight: 400;
        margin-top: 0.3rem;
    }

    /* ── Powered by bandı ── */
    .tech-bar {
        display: flex;
        justify-content: center;
        gap: 0.8rem;
        margin: 0.5rem 0 1.5rem 0;
        flex-wrap: wrap;
    }
    .tech-chip {
        background: rgba(255,255,255,0.04);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 20px;
        padding: 0.3rem 0.8rem;
        font-size: 0.7rem;
        color: rgba(255,255,255,0.5);
        font-weight: 500;
        letter-spacing: 0.5px;
    }

    /* ── İstatistik kartları (sidebar) ── */
    .stat-grid {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 0.5rem;
        margin: 0.5rem 0;
    }
    .stat-card {
        background: rgba(255,255,255,0.04);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 12px;
        padding: 0.8rem;
        text-align: center;
        transition: all 0.3s ease;
    }
    .stat-card:hover {
        background: rgba(102, 126, 234, 0.08);
        border-color: rgba(102, 126, 234, 0.25);
        transform: translateY(-1px);
    }
    .stat-card .stat-icon { font-size: 1.2rem; }
    .stat-card .stat-value {
        color: white;
        font-size: 1.1rem;
        font-weight: 700;
        margin: 0.2rem 0 0 0;
        display: block;
    }
    .stat-card .stat-label {
        color: rgba(255,255,255,0.4);
        font-size: 0.65rem;
        text-transform: uppercase;
        letter-spacing: 1px;
        display: block;
    }

    /* ── Stat kartları (tam genişlik) ── */
    .stat-card-wide {
        background: rgba(255,255,255,0.04);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 12px;
        padding: 0.8rem;
        text-align: center;
        margin: 0.5rem 0;
        transition: all 0.3s ease;
    }
    .stat-card-wide:hover {
        background: rgba(102, 126, 234, 0.08);
        border-color: rgba(102, 126, 234, 0.25);
    }
    .stat-card-wide .stat-value {
        color: white; font-size: 0.95rem; font-weight: 600;
    }
    .stat-card-wide .stat-label {
        color: rgba(255,255,255,0.4); font-size: 0.65rem;
        text-transform: uppercase; letter-spacing: 1px;
    }

    /* ── Chat mesajları ── */
    [data-testid="stChatMessage"] {
        background: rgba(255,255,255,0.02);
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 16px;
        padding: 1rem 1.2rem;
        margin-bottom: 0.5rem;
        backdrop-filter: blur(10px);
    }

    /* ── Arama modu etiketi ── */
    .search-badge {
        display: inline-flex;
        align-items: center;
        gap: 0.4rem;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.72rem;
        font-weight: 600;
        letter-spacing: 0.5px;
        margin-bottom: 0.8rem;
    }
    .badge-sql {
        background: linear-gradient(135deg, rgba(59,130,246,0.15), rgba(99,102,241,0.15));
        border: 1px solid rgba(59,130,246,0.25);
        color: #93c5fd;
    }
    .badge-semantic {
        background: linear-gradient(135deg, rgba(168,85,247,0.15), rgba(236,72,153,0.15));
        border: 1px solid rgba(168,85,247,0.25);
        color: #c4b5fd;
    }
    .badge-detail {
        background: linear-gradient(135deg, rgba(52,211,153,0.15), rgba(16,185,129,0.15));
        border: 1px solid rgba(52,211,153,0.25);
        color: #6ee7b7;
    }
    .badge-stats {
        background: linear-gradient(135deg, rgba(251,191,36,0.15), rgba(245,158,11,0.15));
        border: 1px solid rgba(251,191,36,0.25);
        color: #fcd34d;
    }
    .badge-chat {
        background: linear-gradient(135deg, rgba(255,255,255,0.05), rgba(255,255,255,0.08));
        border: 1px solid rgba(255,255,255,0.12);
        color: rgba(255,255,255,0.6);
    }

    /* ── Sonuç sayısı etiketi ── */
    .result-count {
        display: inline-block;
        background: rgba(52,211,153,0.1);
        border: 1px solid rgba(52,211,153,0.2);
        color: #6ee7b7;
        padding: 0.2rem 0.6rem;
        border-radius: 8px;
        font-size: 0.75rem;
        font-weight: 600;
        margin-bottom: 0.5rem;
    }

    /* ── Örnek soru butonları ── */
    [data-testid="stSidebar"] .stButton > button {
        background: rgba(255,255,255,0.04) !important;
        border: 1px solid rgba(255,255,255,0.08) !important;
        color: rgba(255,255,255,0.7) !important;
        border-radius: 10px !important;
        font-size: 0.8rem !important;
        font-weight: 400 !important;
        text-align: left !important;
        padding: 0.5rem 0.8rem !important;
        transition: all 0.3s ease !important;
    }
    [data-testid="stSidebar"] .stButton > button:hover {
        background: rgba(102, 126, 234, 0.12) !important;
        border-color: rgba(102, 126, 234, 0.3) !important;
        color: white !important;
        transform: translateX(4px) !important;
    }

    /* ── Sidebar başlık ── */
    .sidebar-brand {
        text-align: center;
        padding: 0.5rem 0;
    }
    .sidebar-brand h2 {
        background: linear-gradient(135deg, #667eea, #a78bfa);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 1.4rem;
        font-weight: 800;
        margin: 0;
    }
    .sidebar-brand p {
        color: rgba(255,255,255,0.35);
        font-size: 0.75rem;
        margin: 0.2rem 0 0 0;
    }

    /* ── Divider ── */
    .subtle-divider {
        height: 1px;
        background: linear-gradient(90deg, transparent, rgba(102,126,234,0.2), transparent);
        margin: 1rem 0;
    }

    /* ── Chat input ── */
    [data-testid="stChatInput"] textarea {
        background: rgba(255,255,255,0.05) !important;
        border: 1px solid rgba(255,255,255,0.1) !important;
        border-radius: 14px !important;
        color: white !important;
    }
    [data-testid="stChatInput"] textarea:focus {
        border-color: rgba(102,126,234,0.4) !important;
        box-shadow: 0 0 20px rgba(102,126,234,0.1) !important;
    }

    /* ── Hoş geldin kartı ── */
    .welcome-card {
        background: rgba(255,255,255,0.03);
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 20px;
        padding: 2rem;
        text-align: center;
        margin: 1rem auto;
        max-width: 600px;
    }
    .welcome-card h3 {
        color: rgba(255,255,255,0.8);
        font-weight: 600;
        font-size: 1.1rem;
        margin: 0.5rem 0;
    }
    .welcome-card p {
        color: rgba(255,255,255,0.4);
        font-size: 0.85rem;
    }
    .welcome-features {
        display: flex;
        justify-content: center;
        gap: 1.5rem;
        margin-top: 1.2rem;
        flex-wrap: wrap;
    }
    .welcome-feature {
        text-align: center;
    }
    .welcome-feature .wf-icon {
        font-size: 1.8rem;
        display: block;
        margin-bottom: 0.3rem;
    }
    .welcome-feature .wf-label {
        color: rgba(255,255,255,0.5);
        font-size: 0.72rem;
        font-weight: 500;
    }

    /* ── Temizle butonu ── */
    .clear-btn button {
        background: rgba(239,68,68,0.08) !important;
        border: 1px solid rgba(239,68,68,0.2) !important;
        color: #fca5a5 !important;
    }
    .clear-btn button:hover {
        background: rgba(239,68,68,0.15) !important;
        border-color: rgba(239,68,68,0.4) !important;
    }

    /* ── Scrollbar ── */
    ::-webkit-scrollbar { width: 6px; }
    ::-webkit-scrollbar-track { background: transparent; }
    ::-webkit-scrollbar-thumb {
        background: rgba(255,255,255,0.1);
        border-radius: 3px;
    }
    ::-webkit-scrollbar-thumb:hover { background: rgba(255,255,255,0.2); }
</style>
""", unsafe_allow_html=True)

# ─────────────── MCP CLIENT ───────────────


def _build_context(max_turns: int = 5) -> str:
    """Son N tur mesajdan konuşma bağlamı oluşturur."""
    messages = st.session_state.get("messages", [])
    if not messages:
        return ""

    recent = messages[-(max_turns * 2):]  # user+assistant çiftleri
    lines = []
    for msg in recent:
        role = "Kullanıcı" if msg["role"] == "user" else "Asistan"
        # Çok uzun cevapları kısalt
        content = msg["content"][:300] if msg["role"] == "assistant" else msg["content"]
        lines.append(f"{role}: {content}")

    return "\n".join(lines)


def call_mcp_tool(tool_name: str, arguments: dict) -> dict:
    """MCP Server'daki bir tool'u çağırır."""
    log.info(f"MCP tool çağrısı: {tool_name}({arguments})")

    try:
        with httpx.Client(timeout=60.0) as client:
            response = client.post(
                f"{MCP_SERVER_URL}/call-tool",
                json={"name": tool_name, "arguments": arguments}
            )
            response.raise_for_status()
            return response.json()

    except httpx.ConnectError:
        log.error("MCP Server'a bağlanılamadı")
        return {"error": "MCP Server'a bağlanılamadı. docker-compose up çalıştırıldığından emin olun."}
    except Exception as e:
        log.error(f"MCP tool hatası: {e}")
        return {"error": str(e)}


def decide_tool(question: str) -> tuple[str, dict]:
    """Kullanıcı sorusuna göre hangi MCP tool'un çağrılacağına karar verir."""
    model = genai.GenerativeModel("gemini-2.0-flash")
    context = _build_context()

    context_block = ""
    if context:
        context_block = f"""\n\nÖnceki konuşma bağlamı:
---
{context}
---
Yukarıdaki bağlamı dikkate al. Kullanıcı önceki konuşmaya atıf yapıyor olabilir."""

    prompt = f"""Kullanıcı şu soruyu sordu: "{question}"{context_block}

Bu soruyu yanıtlamak için aşağıdaki araçlardan hangisi kullanılmalı?

1. sql_query - Sayısal, istatistiksel, filtreleme soruları için (fiyat, adet, ortalama, liste, sıralama)
   Örnek: "En ucuz 5 BMW", "İstanbul'da kaç ilan var", "Ortalama fiyat nedir"

2. search_similar_cars - Açıklayıcı, subjektif aramalar için (benzerlik, öneri, tip bazlı)
   Örnek: "Aile için geniş SUV", "Ekonomik şehir aracı", "Spor araba önerisi"

3. get_car_details - Spesifik ilan detayı (ilan numarası verildiğinde)
   Örnek: "12345 nolu ilan", "Bu ilanın detayları"

4. get_database_stats - Genel istatistik soruları
   Örnek: "Kaç ilan var", "Veritabanı durumu"

5. none - Araçla ilgisi olmayan genel sohbet

SADECE araç adını döndür (sql_query, search_similar_cars, get_car_details, get_database_stats veya none).
Başka bir şey yazma."""

    response = model.generate_content(prompt)
    tool = response.text.strip().lower().replace("`", "")

    # Konuşma bağlamını sql_query'ye ekle
    if tool == "sql_query":
        return "sql_query", {"question": question, "context": context}
    elif tool == "search_similar_cars":
        return "search_similar_cars", {"query": question, "limit": 10}
    elif tool == "get_car_details":
        import re
        m = re.search(r"(\d{5,})", question)
        ilan_id = m.group(1) if m else question
        return "get_car_details", {"ilan_id": ilan_id}
    elif tool == "get_database_stats":
        return "get_database_stats", {}
    else:
        return "none", {}


def get_badge_html(tool_name: str) -> str:
    """Tool tipine göre badge HTML döner."""
    badges = {
        "sql_query": '<div class="search-badge badge-sql">📊 Veritabanı Sorgusu</div>',
        "search_similar_cars": '<div class="search-badge badge-semantic">🧠 Akıllı Arama</div>',
        "get_car_details": '<div class="search-badge badge-detail">🔎 İlan Detayı</div>',
        "get_database_stats": '<div class="search-badge badge-stats">📈 İstatistikler</div>',
        "none": '<div class="search-badge badge-chat">💬 Sohbet</div>',
    }
    return badges.get(tool_name, "")


def format_semantic_results(data: dict) -> str:
    """Semantik arama sonuçlarını kart formatında döner."""
    if "error" in data:
        return f"❌ Hata: {data['error']}"

    results = data.get("results", [])
    if not results:
        return "Bu kriterlere uygun araç bulunamadı. 🔍"

    cards = []
    for i, car in enumerate(results, 1):
        fiyat = f"{int(car.get('fiyat', 0)):,}".replace(",", ".") if car.get("fiyat") else "?"
        km = f"{int(car.get('kilometre', 0)):,}".replace(",", ".") if car.get("kilometre") else "?"
        score = f"{car.get('score', 0) * 100:.0f}%"

        card = f"""**{i}. {car.get('marka', '')} {car.get('seri', '')} {car.get('model', '')}**
🗓️ {car.get('yil', '?')} · 🛣️ {km} km · ⛽ {car.get('yakit_tipi', '?')} · 🔧 {car.get('vites_tipi', '?')}
💰 **{fiyat} TL** · 📍 {car.get('il', '?')} · 🎯 Benzerlik: {score}"""
        cards.append(card)

    return "\n\n---\n\n".join(cards)


# ─────────────── SESSION ───────────────

if "messages" not in st.session_state:
    st.session_state.messages = []

if "gemini_chat" not in st.session_state:
    try:
        model = genai.GenerativeModel("gemini-2.0-flash")
        st.session_state.gemini_chat = model.start_chat(history=[])
    except Exception:
        st.session_state.gemini_chat = None


# ─────────────── SIDEBAR ───────────────

with st.sidebar:
    st.markdown("""
    <div class="sidebar-brand">
        <h2>🚗 Arabam</h2>
        <p>Akıllı Araç Asistanı</p>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="subtle-divider"></div>', unsafe_allow_html=True)

    # İstatistikler
    try:
        stats = call_mcp_tool("get_database_stats", {})
        if "error" not in stats:
            mysql_stats = stats.get("mysql", {})
            qdrant_stats = stats.get("qdrant", {})

            toplam = mysql_stats.get('toplam_ilan', 0)
            marka = mysql_stats.get('marka_sayisi', 0)
            vektor = qdrant_stats.get('points_count', 0)
            min_f = mysql_stats.get('min_fiyat', 0)
            max_f = mysql_stats.get('max_fiyat', 0)

            st.markdown(f"""
            <div class="stat-grid">
                <div class="stat-card">
                    <span class="stat-icon">🚗</span>
                    <span class="stat-value">{toplam:,}</span>
                    <span class="stat-label">İlan</span>
                </div>
                <div class="stat-card">
                    <span class="stat-icon">🏷️</span>
                    <span class="stat-value">{marka}</span>
                    <span class="stat-label">Marka</span>
                </div>
                <div class="stat-card">
                    <span class="stat-icon">🧠</span>
                    <span class="stat-value">{vektor:,}</span>
                    <span class="stat-label">Vektör</span>
                </div>
                <div class="stat-card">
                    <span class="stat-icon">📅</span>
                    <span class="stat-value">{mysql_stats.get('min_yil', 0)}—{mysql_stats.get('max_yil', 0)}</span>
                    <span class="stat-label">Yıl Aralığı</span>
                </div>
            </div>
            <div class="stat-card-wide">
                <span class="stat-label">💰 Fiyat Aralığı</span><br>
                <span class="stat-value">{min_f:,} — {max_f:,} TL</span>
            </div>
            """, unsafe_allow_html=True)
    except Exception as e:
        st.error(f"İstatistik hatası: {e}")

    st.markdown('<div class="subtle-divider"></div>', unsafe_allow_html=True)
    st.markdown("##### 💡 Örnek Sorular")

    examples = [
        ("🔍", "En ucuz 5 BMW'yi listele"),
        ("🧠", "Aile için geniş SUV önerir misin?"),
        ("📊", "İstanbul'daki otomatik araçların ortalama fiyatı?"),
        ("🧠", "Ekonomik şehir içi araç arıyorum"),
        ("🔍", "2020 ve üzeri dizel araçların marka dağılımı"),
        ("🧠", "Spor tarzı hızlı bir araba istiyorum"),
        ("📊", "Hangi renk en popüler?"),
    ]

    for icon, ex in examples:
        if st.button(f"{icon} {ex}", key=f"ex_{ex}", use_container_width=True):
            st.session_state.example_input = ex

    st.markdown('<div class="subtle-divider"></div>', unsafe_allow_html=True)

    st.markdown("""
    <div class="tech-bar" style="justify-content: center;">
        <span class="tech-chip">Gemini</span>
        <span class="tech-chip">Qdrant</span>
        <span class="tech-chip">MySQL</span>
        <span class="tech-chip">MCP</span>
    </div>
    """, unsafe_allow_html=True)

    with st.container():
        st.markdown('<div class="clear-btn">', unsafe_allow_html=True)
        if st.button("🗑️ Sohbeti Temizle", use_container_width=True):
            st.session_state.messages = []
            st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)


# ─────────────── MAIN ───────────────

st.markdown("""
<div class="main-header">
    <h1>🚗 Arabam Chatbot</h1>
    <p>Binlerce araç ilanını doğal dilde sorgula</p>
</div>
<div class="tech-bar">
    <span class="tech-chip">📊 SQL Sorguları</span>
    <span class="tech-chip">🧠 Semantik Arama</span>
    <span class="tech-chip">🤖 AI Destekli</span>
</div>
""", unsafe_allow_html=True)

# Hoş geldin mesajı (boş chat)
if not st.session_state.messages:
    st.markdown("""
    <div class="welcome-card">
        <h3>Merhaba! 👋 Size nasıl yardımcı olabilirim?</h3>
        <p>Araç ilanları hakkında her türlü sorunuzu yanıtlayabilirim.</p>
        <div class="welcome-features">
            <div class="welcome-feature">
                <span class="wf-icon">🔍</span>
                <span class="wf-label">Fiyat & Filtre</span>
            </div>
            <div class="welcome-feature">
                <span class="wf-icon">🧠</span>
                <span class="wf-label">Akıllı Öneri</span>
            </div>
            <div class="welcome-feature">
                <span class="wf-icon">📊</span>
                <span class="wf-label">İstatistik</span>
            </div>
            <div class="welcome-feature">
                <span class="wf-icon">🔎</span>
                <span class="wf-label">İlan Detayı</span>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

# Chat geçmişi
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        # Arama modu badge'i
        if msg["role"] == "assistant" and "badge" in msg:
            st.markdown(msg["badge"], unsafe_allow_html=True)
        st.markdown(msg["content"])

# Örnek soru
default_input = ""
if "example_input" in st.session_state:
    default_input = st.session_state.pop("example_input")

prompt = st.chat_input("Araçlar hakkında bir şey sor...")

if default_input:
    prompt = default_input

if prompt:
    log.info(f"Kullanıcı sorusu: {prompt}")
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("🤔 Düşünüyorum..."):
            try:
                # Hangi tool kullanılacak?
                tool_name, tool_args = decide_tool(prompt)
                log.info(f"Seçilen tool: {tool_name}")

                badge_html = get_badge_html(tool_name)
                if badge_html:
                    st.markdown(badge_html, unsafe_allow_html=True)

                if tool_name == "none":
                    # Genel sohbet
                    if st.session_state.gemini_chat:
                        resp = st.session_state.gemini_chat.send_message(prompt)
                        answer = resp.text
                    else:
                        answer = "Merhaba! Size araç ilanları hakkında yardımcı olabilirim. 🚗"

                    st.markdown(answer)
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": answer,
                        "badge": badge_html
                    })

                elif tool_name == "sql_query":
                    result = call_mcp_tool("sql_query", tool_args)

                    if "error" in result:
                        st.error(f"❌ {result['error']}")
                        st.session_state.messages.append({
                            "role": "assistant",
                            "content": f"❌ {result['error']}",
                            "badge": badge_html
                        })
                    else:
                        # Sonuçları Gemini ile özetle
                        row_count = result.get('row_count', 0)
                        summary_prompt = f"""Kullanıcı: "{prompt}"
Sonuçlar ({row_count} satır):
{json.dumps(result.get('results', [])[:15], ensure_ascii=False, indent=2)}

Bu sonuçları Türkçe olarak doğal ve anlaşılır şekilde açıkla. 
Sayıları okunabilir yaz (845.000 TL, 120.000 km). Kısa ve öz ol. Emoji kullan.
Eğer birden fazla araç varsa, okunabilir bir liste formatında sun."""

                        if st.session_state.gemini_chat:
                            resp = st.session_state.gemini_chat.send_message(summary_prompt)
                            summary = resp.text
                        else:
                            summary = f"**{row_count} sonuç bulundu.**"

                        # Sonuç sayısı
                        if row_count > 0:
                            st.markdown(
                                f'<div class="result-count">✅ {row_count} sonuç bulundu</div>',
                                unsafe_allow_html=True
                            )

                        st.markdown(summary)

                        st.session_state.messages.append({
                            "role": "assistant",
                            "content": summary,
                            "badge": badge_html
                        })

                elif tool_name == "search_similar_cars":
                    result = call_mcp_tool("search_similar_cars", tool_args)

                    if "error" in result:
                        st.error(f"❌ {result['error']}")
                        st.session_state.messages.append({
                            "role": "assistant",
                            "content": f"❌ {result['error']}",
                            "badge": badge_html
                        })
                    else:
                        result_count = result.get("result_count", 0)
                        summary_prompt = f"""Kullanıcı "{prompt}" diye araç arıyor.
Semantik arama sonuçları (en benzer araçlar):
{json.dumps(result.get('results', [])[:8], ensure_ascii=False, indent=2)}

Bu araçları kullanıcıya Türkçe olarak öner. Her araç için kısa bir açıklama yaz.
Neden bu araçların uygun olduğunu açıkla. Fiyatları okunabilir yaz. Emoji kullan."""

                        if st.session_state.gemini_chat:
                            resp = st.session_state.gemini_chat.send_message(summary_prompt)
                            summary = resp.text
                        else:
                            summary = format_semantic_results(result)

                        if result_count > 0:
                            st.markdown(
                                f'<div class="result-count">🎯 {result_count} benzer araç bulundu</div>',
                                unsafe_allow_html=True
                            )

                        st.markdown(summary)

                        st.session_state.messages.append({
                            "role": "assistant",
                            "content": summary,
                            "badge": badge_html
                        })

                elif tool_name == "get_car_details":
                    result = call_mcp_tool("get_car_details", tool_args)

                    if "error" in result:
                        st.error(f"❌ {result['error']}")
                        st.session_state.messages.append({
                            "role": "assistant",
                            "content": f"❌ {result['error']}",
                            "badge": badge_html
                        })
                    else:
                        detail_text = json.dumps(result, ensure_ascii=False, indent=2)

                        if st.session_state.gemini_chat:
                            resp = st.session_state.gemini_chat.send_message(
                                f"Bu araç ilanının detaylarını Türkçe olarak güzel, okunabilir bir şekilde özetle. Emoji kullan:\n{detail_text}"
                            )
                            summary = resp.text
                        else:
                            summary = f"```json\n{detail_text}\n```"

                        st.markdown(summary)
                        st.session_state.messages.append({
                            "role": "assistant",
                            "content": summary,
                            "badge": badge_html
                        })

                elif tool_name == "get_database_stats":
                    result = call_mcp_tool("get_database_stats", {})
                    detail_text = json.dumps(result, ensure_ascii=False, indent=2)

                    if st.session_state.gemini_chat:
                        resp = st.session_state.gemini_chat.send_message(
                            f"Bu veritabanı istatistiklerini Türkçe olarak güzel bir şekilde özetle. Emoji kullan:\n{detail_text}"
                        )
                        summary = resp.text
                    else:
                        summary = f"```json\n{detail_text}\n```"

                    st.markdown(summary)
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": summary,
                        "badge": badge_html
                    })

            except Exception as e:
                log.error(f"Hata: {e}")
                st.error(f"❌ Bir hata oluştu. Lütfen tekrar deneyin.")
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": f"❌ Bir hata oluştu. Lütfen tekrar deneyin."
                })

    st.rerun()
