"""
Arabam Chatbot — Streamlit Arayüzü (MCP Client + Gemini Function Calling)
===========================================================================
FastMCP Server'a MCP protokolü ile bağlanır.
Gemini, tool'ları otomatik çağırarak kullanıcı sorularını yanıtlar.
"""

import os
import json
import asyncio
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

from google import genai
from google.genai import types
from mcp import ClientSession
from mcp.client.sse import sse_client

from logger import get_logger

log = get_logger("app")

# GenAI Client
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
genai_client = genai.Client(api_key=GEMINI_API_KEY)

MCP_SERVER_URL = os.getenv("MCP_SERVER_URL", "http://localhost:8000")
MCP_SSE_URL = f"{MCP_SERVER_URL}/sse"

MODEL_NAME = "gemini-2.5-flash"

SYSTEM_PROMPT = """Sen "Arabam Chatbot" adlı bir araç ilanı asistanısın. Türkçe konuş.
Bir oto galeri danışmanı gibi davran — samimi, bilgili ve yardımsever ol.

## Görevin
Kullanıcıların araç ilanları hakkındaki sorularını yanıtlamak için sana verilen MCP araçlarını kullan.
Her soruya uygun aracı seçip çağır, sonuçları doğal ve anlaşılır Türkçe ile sun.

## KRİTİK Kurallar — MUTLAKA UYULMASI GEREKEN
1. **ASLA tool çağırmadan cevap verme!** Her soruda MUTLAKA en az bir tool çağır. "Yapamıyorum", "filtreleme yapamıyorum", "bu bilgi mevcut değil" gibi cevaplar YASAK! Önce dene, sonra sonuca göre cevap ver.
2. **Asla bilgi uydurma!** Sadece araçlardan dönen verileri kullanarak cevap ver.
3. Sayısal değerleri okunabilir yaz: 845.000 TL, 120.000 km
4. Sonuçları liste veya tablo formatında sun
5. Kısa ve öz ol ama bilgilendirici
6. Veriden ilginç çıkarımlar yap (örn: "Bu fiyata göre oldukça düşük kilometreli!")
7. Emoji kullan ama abartma
8. Kullanıcı önceki konuşmaya atıf yaparsa, chat geçmişinden bağlamı anla
9. Birden fazla tool çağırabilirsin

## Araç Seçim Kuralları (ÖNEMLİ)
- **`hibrit_arac_ara`** → VARSAYILAN ARAMA ARACI. Kullanıcı herhangi bir araç aradığında, bir özellik belirttiğinde veya doğal dil kullandığında BU TOOL'U KULLAN.
  Bu tool hem SQL keyword araması hem semantik arama yapar.
  İlan başlığı, marka, seri, model VE İLAN AÇIKLAMASI (ilan_aciklamasi) içinde arama yapar.
  Örnekler: "bakımı yapılmış araç", "Astra", "ekonomik SUV", "aile aracı", "temiz araç", "boyasız", "tramersiz", "garaj arabası"
- **`araba_ara`** → Kesin filtrelerle arama (fiyat aralığı, yıl, km, marka, renk gibi yapılandırılmış filtreler).
- **`ilan_detay_getir`** → Belirli bir ilanın tüm detaylarını görmek için.
- **`fiyat_istatistikleri`** → Fiyat istatistikleri.
- **`ilan_sayisi`, `renk_dagilimi`, `il_dagilimi`** → İstatistik sorguları.
- **`marka_seri_listele`** → Marka/seri/model listesi.
- **`veritabani_ozeti`** → Genel veritabanı bilgisi.

## TEKRAR: "Yapamıyorum" deme, her zaman önce `hibrit_arac_ara` ile dene!
"""

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


# ─────────────── MCP + GEMINI ───────────────


async def ask_gemini_with_mcp(user_message: str, chat_history: list) -> str:
    """
    MCP Server'a bağlanıp Gemini'ye tool'ları vererek cevap alır.
    Gemini otomatik olarak gerekli tool'ları çağırır.
    """
    try:
        async with sse_client(MCP_SSE_URL) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                log.info("MCP session başlatıldı")

                # Chat geçmişini Content formatına çevir
                contents = []
                for msg in chat_history:
                    role = "user" if msg["role"] == "user" else "model"
                    contents.append(
                        types.Content(
                            role=role,
                            parts=[types.Part.from_text(text=msg["content"])]
                        )
                    )

                # Mevcut kullanıcı mesajını ekle
                contents.append(
                    types.Content(
                        role="user",
                        parts=[types.Part.from_text(text=user_message)]
                    )
                )

                # Gemini'ye MCP session'ı tool olarak ver
                response = await genai_client.aio.models.generate_content(
                    model=MODEL_NAME,
                    contents=contents,
                    config=types.GenerateContentConfig(
                        system_instruction=SYSTEM_PROMPT,
                        temperature=0.7,
                        tools=[session],
                    ),
                )

                log.info(f"Gemini cevap verdi: {response.text[:100] if response.text else 'boş'}...")
                return response.text or "Üzgünüm, bir cevap oluşturamadım. Lütfen tekrar deneyin."

    except Exception as e:
        log.error(f"MCP/Gemini hatası: {e}")
        return f"❌ Bir hata oluştu: {str(e)}\n\nMCP Server'ın çalıştığından emin olun."


def get_sidebar_stats() -> dict:
    """Sidebar için veritabanı istatistiklerini çeker (httpx ile, MCP session dışında)."""
    import httpx
    try:
        with httpx.Client(timeout=10.0) as client:
            # MCP SSE üzerinden doğrudan tool çağrısı yapamayız sidebar'da,
            # bu yüzden async MCP session kullanıyoruz
            pass
    except Exception:
        pass
    return {}


async def get_stats_via_mcp() -> dict:
    """MCP üzerinden veritabanı istatistiklerini çeker."""
    try:
        async with sse_client(MCP_SSE_URL) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool("veritabani_ozeti", {})
                # MCP tool sonucu TextContent listesi olarak döner
                if result.content and len(result.content) > 0:
                    text = result.content[0].text
                    return json.loads(text)
    except Exception as e:
        log.error(f"Stats hatası: {e}")
    return {}


# ─────────────── SESSION ───────────────

if "messages" not in st.session_state:
    st.session_state.messages = []

if "stats" not in st.session_state:
    try:
        st.session_state.stats = asyncio.run(get_stats_via_mcp())
    except Exception:
        st.session_state.stats = {}


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
    stats = st.session_state.get("stats", {})
    mysql_stats = stats.get("mysql", {})
    qdrant_stats = stats.get("qdrant", {})

    if mysql_stats:
        toplam = mysql_stats.get('toplam_ilan', 0)
        marka = mysql_stats.get('marka_sayisi', 0)
        vektor = qdrant_stats.get('points_count', 0) if isinstance(qdrant_stats, dict) else 0
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
        <span class="tech-chip">Gemini 2.5</span>
        <span class="tech-chip">Qdrant</span>
        <span class="tech-chip">MySQL</span>
        <span class="tech-chip">FastMCP</span>
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
    <span class="tech-chip">🛠️ MCP Tools</span>
    <span class="tech-chip">🧠 Gemini Function Calling</span>
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
                # Gemini + MCP ile cevap al
                answer = asyncio.run(
                    ask_gemini_with_mcp(prompt, st.session_state.messages[:-1])
                )

                st.markdown(answer)
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": answer
                })

            except Exception as e:
                log.error(f"Hata: {e}")
                error_msg = "❌ Bir hata oluştu. Lütfen tekrar deneyin."
                st.error(error_msg)
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": error_msg
                })

    st.rerun()
