"""
app.py — Dashboard de Importações Gocase
Foco: Análise de Frete por Container
Streamlit + Plotly
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pathlib import Path

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────
st.set_page_config(
    page_title="Gocase · Frete & Containers",
    page_icon="🚢",
    layout="wide",
    initial_sidebar_state="expanded",
)

ORANGE      = "#E8571A"
NAVY        = "#1B3A6B"
NAVY_DEEP   = "#0f2247"
GREEN       = "#22C97B"
RED         = "#F04E4E"
YELLOW      = "#F5B731"
PURPLE      = "#7B6CF6"
TEAL        = "#1FC8C0"
BLUE        = "#4A9EFF"
MUTED       = "#8A9BBE"

TEMPLATE = dict(
    layout=go.Layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="sans-serif", color="#C8D6F0"),
        colorway=[ORANGE, PURPLE, GREEN, YELLOW, BLUE, TEAL, RED],
        xaxis=dict(gridcolor="rgba(255,255,255,0.06)", linecolor="rgba(255,255,255,0.1)"),
        yaxis=dict(gridcolor="rgba(255,255,255,0.06)", linecolor="rgba(255,255,255,0.1)"),
        legend=dict(bgcolor="rgba(0,0,0,0)"),
        margin=dict(l=10, r=10, t=36, b=10),
    )
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Nunito:wght@700;800;900&family=Nunito+Sans:wght@400;600&display=swap');
html, body, [class*="css"] {
    font-family: 'Nunito Sans', sans-serif;
    background-color: #0f2247;
    color: #C8D6F0;
}
section[data-testid="stSidebar"] { background: #0c1b3a; border-right: 1px solid rgba(255,255,255,0.07); }
section[data-testid="stSidebar"] label, section[data-testid="stSidebar"] p {
    color: #8A9BBE !important; font-size: 11px !important; font-weight: 700 !important; text-transform: uppercase;
}
div[data-testid="metric-container"] {
    background: rgba(27,58,107,0.45); border: 1px solid rgba(255,255,255,0.08);
    border-radius: 12px; padding: 16px 20px;
}
div[data-testid="metric-container"] label { color: #8A9BBE !important; font-size: 11px !important; text-transform: uppercase; font-weight: 700 !important; }
div[data-testid="metric-container"] [data-testid="stMetricValue"] { color: #FFFFFF !important; font-size: 1.6rem !important; font-weight: 800 !important; }
div[data-testid="metric-container"] [data-testid="stMetricDelta"] { font-size: 12px !important; }
.stTabs [data-baseweb="tab-list"] { gap: 4px; background: rgba(27,58,107,0.3); border-radius: 10px; padding: 4px; }
.stTabs [data-baseweb="tab"] {
    border-radius: 8px; color: #8A9BBE; font-weight: 700; font-size: 13px;
    padding: 6px 18px; background: transparent;
}
.stTabs [aria-selected="true"] { background: #E8571A !important; color: white !important; }
h1,h2,h3,h4 { color: #FFFFFF !important; font-family: 'Nunito', sans-serif !important; font-weight: 800 !important; }
.stDataFrame { border-radius: 10px; overflow: hidden; }
hr { border-color: rgba(255,255,255,0.08) !important; }
</style>
""", unsafe_allow_html=True)

# ──────────────────────────────────────────────
# DADOS
# ──────────────────────────────────────────────
OUTPUT = Path("output") if Path("output").exists() else Path(".")

@st.cache_data(ttl=300)
def load_data():
    def read(name, **kw):
        p = OUTPUT / f"{name}.csv"
        if p.exists():
            return pd.read_csv(p, sep=";", decimal=",", **kw)
        return pd.DataFrame()

    fato = read("fato_importacao")
    for _c in ["etd","eta","data_pi","data_embarque","data_chegada"]:
        if _c in fato.columns:
            fato[_c] = pd.to_datetime(fato[_c], dayfirst=True, errors="coerce")
    cont = read("dim_containers")

    if cont.empty:
        try:
            from pipeline_containers import calcular_frete_container, detalhe_container_bl
            planilhas = list(Path(".").glob("Controle*.xlsx"))
            if planilhas:
                cont     = calcular_frete_container(str(planilhas[0]))
                cont_det = detalhe_container_bl(str(planilhas[0]))
                cont.to_csv(OUTPUT / "dim_containers.csv",        sep=";", decimal=",", index=False)
                cont_det.to_csv(OUTPUT / "dim_containers_det.csv", sep=";", decimal=",", index=False)
        except Exception as e:
            st.warning(f"pipeline_containers não disponível: {e}")

    cont_det = read("dim_containers_det")
    return fato, cont, cont_det

try:
    fato_raw, cont_raw, cont_det_raw = load_data()
except Exception as e:
    st.error(f"Erro ao carregar dados: {e}")
    fato_raw    = pd.DataFrame()
    cont_raw    = pd.DataFrame()
    cont_det_raw= pd.DataFrame()

if fato_raw.empty:
    st.warning("⚠️ Arquivo `output/fato_importacao.csv` não encontrado. Execute o `pipeline_importacao.py` primeiro.")
    st.stop()

# ──────────────────────────────────────────────
# SIDEBAR
# ──────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 🚢 Gocase · Frete")
    st.divider()

    if not fato_raw.empty and "etd" in fato_raw.columns:
        fato_raw["ano"] = pd.to_datetime(fato_raw["etd"], errors="coerce").dt.year
        anos = sorted(fato_raw["ano"].dropna().unique().astype(int).tolist())
        anos_sel = st.multiselect("Ano ETD", anos, default=anos)
    else:
        anos_sel = []

    if not fato_raw.empty and "supplier" in fato_raw.columns:
        sups = sorted(fato_raw["supplier"].dropna().unique().tolist())
        sups_sel = st.multiselect("Fornecedor", sups, default=sups)
    else:
        sups_sel = []

    if not fato_raw.empty and "modal" in fato_raw.columns:
        modais = sorted(fato_raw["modal"].dropna().unique().tolist())
        modais_sel = st.multiselect("Modal", modais, default=modais)
    else:
        modais_sel = []

    if not cont_raw.empty and "tipo" in cont_raw.columns:
        tipos = sorted(cont_raw["tipo"].dropna().unique().tolist())
        tipos_sel = st.multiselect("Tipo Container", tipos, default=tipos)
    else:
        tipos_sel = []

    if not cont_raw.empty and "num_container" in cont_raw.columns:
        conts_disp = sorted(cont_raw["num_container"].dropna().unique().tolist())
        conts_sel = st.multiselect("Nº Container", conts_disp, default=[], placeholder="Todos")
    else:
        conts_sel = []

    col_proc = next((c for c in ["ref_bl","processo","ref_completa","no_pi"] if c in fato_raw.columns), None)
    if col_proc:
        procs_disp = sorted(fato_raw[col_proc].dropna().unique().tolist())
        procs_sel = st.multiselect("Processo / BL", procs_disp, default=[], placeholder="Todos")
    else:
        procs_sel = []

    st.divider()
    st.caption(f"📦 {len(fato_raw):,} processos · {len(cont_raw):,} containers")

# Aplicar filtros
fato = fato_raw.copy()
if anos_sel and "ano" in fato.columns:
    fato = fato[fato["ano"].isin(anos_sel)]
if sups_sel and "supplier" in fato.columns:
    fato = fato[fato["supplier"].isin(sups_sel)]
if modais_sel and "modal" in fato.columns:
    fato = fato[fato["modal"].isin(modais_sel)]
if procs_sel and col_proc and col_proc in fato.columns:
    fato = fato[fato[col_proc].isin(procs_sel)]

cont = cont_raw.copy()
if tipos_sel and "tipo" in cont.columns:
    cont = cont[cont["tipo"].isin(tipos_sel)]
if conts_sel and "num_container" in cont.columns:
    cont = cont[cont["num_container"].isin(conts_sel)]

cont_det = cont_det_raw.copy()
if conts_sel and "num_container" in cont_det.columns:
    cont_det = cont_det[cont_det["num_container"].isin(conts_sel)]
if procs_sel and col_proc and "ref_bl" in cont_det.columns:
    cont_det = cont_det[cont_det["ref_bl"].isin(procs_sel)]

_col_data_fato = next((c for c in ["etd","data_etd"] if c in fato.columns), None)
if _col_data_fato:
    fato[_col_data_fato] = pd.to_datetime(fato[_col_data_fato], dayfirst=True, errors="coerce")
    fato = fato[fato[_col_data_fato].dt.year >= 2025].copy()

_col_data_cont = next((c for c in ["etd","etd_embarque","data_etd"] if c in cont_det.columns), None)
if _col_data_cont:
    cont_det[_col_data_cont] = pd.to_datetime(cont_det[_col_data_cont], dayfirst=True, errors="coerce")
    cont_det = cont_det[cont_det[_col_data_cont].dt.year >= 2025].copy()

# ──────────────────────────────────────────────
# NORMALIZAR COLUNAS
# ──────────────────────────────────────────────
def _find_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def _rename_if_needed(df, candidates, target):
    src = _find_col(df, candidates)
    if src and src != target:
        df = df.rename(columns={src: target})
    return df

fato = _rename_if_needed(fato, ["valor_pi","valor_produto","valor_total_pi","total_pi","valor_pi_usd","fob","valor_fob"], "valor_pi")
fato = _rename_if_needed(fato, ["frete","frete_usd","valor_frete","freight","$ frete"], "frete")
fato = _rename_if_needed(fato, ["supplier","fornecedor","vendor","fabricante"], "supplier")
fato = _rename_if_needed(fato, ["modal","modality","tipo_modal"], "modal")
fato = _rename_if_needed(fato, ["etd","data_etd","etd_china","data_embarque","embarque"], "etd")
fato = _rename_if_needed(fato, ["eta","data_entrega","eta_santos","data_chegada","chegada"], "eta")
fato = _rename_if_needed(fato, ["ref_bl","processo","no_pi","ref_completa","ref_trading"], "ref_bl")

cont = _rename_if_needed(cont, ["num_container","container","nº container","numero_container"], "num_container")
cont = _rename_if_needed(cont, ["frete_total_cont","frete_container","frete_total"], "frete_total_cont")
cont = _rename_if_needed(cont, ["valor_pi_total","valor_pi","total_pi"], "valor_pi_total")
cont = _rename_if_needed(cont, ["frete_por_teu","frete_teu"], "frete_por_teu")

# ──────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────
def usd(v):
    if pd.isna(v): return "—"
    return f"${v:,.0f}" if abs(v) >= 1000 else f"${v:,.2f}"

def pct(v):
    if pd.isna(v): return "—"
    return f"{v*100:.1f}%"

def safe(df, col, fn=sum):
    if col in df.columns and not df.empty:
        return fn(df[col].dropna())
    return np.nan

# ──────────────────────────────────────────────
# HEADER
# ──────────────────────────────────────────────
st.markdown("# 🚢 Análise de Frete · Gocase Importações")

_col_title, _col_btn = st.columns([6, 1])
with _col_btn:
    if st.button("🔄 Atualizar", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

st.divider()

# ──────────────────────────────────────────────
# TICKER DE NOTÍCIAS
# ──────────────────────────────────────────────
import xml.etree.ElementTree as ET
import urllib.request

FEEDS_NEWS = [
    "https://www.comexdobrasil.com/feed/",
    "https://agenciabrasil.ebc.com.br/rss/economia/feed.xml",
    "https://valor.globo.com/rss/economia/",
]

@st.cache_data(ttl=1800)
def buscar_noticias_ticker():
    noticias = []
    for url in FEEDS_NEWS:
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=8) as r:
                xml_raw = r.read()
            root = ET.fromstring(xml_raw)
            for item in root.findall(".//item")[:5]:
                titulo = (item.findtext("title") or "").strip()
                link   = (item.findtext("link")  or "").strip()
                if titulo and link:
                    noticias.append({"titulo": titulo, "link": link})
        except:
            pass
    return noticias

_noticias_ticker = buscar_noticias_ticker()

if _noticias_ticker:
    _items_html = "   ●   ".join(
        f'<a href="{n["link"]}" target="_blank" style="color:#FFFFFF;text-decoration:none;font-weight:600;">{n["titulo"]}</a>'
        for n in _noticias_ticker
    )
    _items_html_dup = _items_html + "   ●   " + _items_html

    st.markdown(f"""
<div style="
    background:rgba(27,58,107,0.6);
    border:1px solid rgba(232,87,26,0.4);
    border-radius:8px;
    padding:10px 16px;
    overflow:hidden;
    white-space:nowrap;
    margin-bottom:16px;
    display:flex;
    align-items:center;
">
    <span style="color:#E8571A;font-weight:800;font-size:13px;text-transform:uppercase;letter-spacing:1px;margin-right:16px;flex-shrink:0;">
        📰 COMEX
    </span>
    <div style="overflow:hidden;flex:1;">
        <span style="display:inline-block;animation:ticker 55s linear infinite;font-size:14px;color:#FFFFFF;">
            {_items_html_dup}
        </span>
    </div>
</div>
<style>
@keyframes ticker {{
    0%   {{ transform: translateX(0%); }}
    100% {{ transform: translateX(-50%); }}
}}
</style>
""", unsafe_allow_html=True)

# ──────────────────────────────────────────────
# TABS
# ──────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "📊 Visão Geral",
    "📦 Por Container",
    "🏭 Por Fornecedor",
    "📈 Tendências",
    "🔍 Drill-through",
    "🚢 Visão Operacional",
    "🎯 Inteligência de Custo",
])

# ══════════════════════════════════════════════
# TAB 1 — VISÃO GERAL
# ══════════════════════════════════════════════
with tab1:
    frete_total = safe(fato, "frete")
    pi_total    = safe(fato, "valor_pi")
    pct_frete   = frete_total / pi_total if pi_total else np.nan
    n_processos = len(fato)
    n_cont      = len(cont)
    frete_teu   = safe(cont, "frete_por_teu", np.mean)

    if "etd" in fato.columns and "frete" in fato.columns:
        _fato_dt = fato.copy()
        _fato_dt["_mes"] = pd.to_datetime(_fato_dt["etd"], errors="coerce").dt.to_period("M")
        _meses_ord = sorted(_fato_dt["_mes"].dropna().unique())
        if len(_meses_ord) >= 2:
            _mes_atual = _meses_ord[-1]
            _mes_ant   = _meses_ord[-2]
            _fat_atual = _fato_dt[_fato_dt["_mes"] == _mes_atual]
            _fat_ant   = _fato_dt[_fato_dt["_mes"] == _mes_ant]
            _frete_atual = _fat_atual["frete"].sum()
            _frete_ant   = _fat_ant["frete"].sum()
            _delta_frete = _frete_atual - _frete_ant
            _delta_pct   = (_fat_atual["frete"].sum() / _fat_atual["valor_pi"].sum()
                            if "valor_pi" in _fat_atual.columns and _fat_atual["valor_pi"].sum() > 0 else np.nan)
            _delta_pct_ant = (_fat_ant["frete"].sum() / _fat_ant["valor_pi"].sum()
                              if "valor_pi" in _fat_ant.columns and _fat_ant["valor_pi"].sum() > 0 else np.nan)
        else:
            _delta_frete = None; _delta_pct = None; _delta_pct_ant = None
    else:
        _delta_frete = None; _delta_pct = None; _delta_pct_ant = None

    if not cont.empty and "frete_por_teu" in cont.columns:
        _max_teu = cont["frete_por_teu"].max()
        _min_teu = cont["frete_por_teu"].min()
        _teu_total = cont["teu"].sum() if "teu" in cont.columns else len(cont)
        _economia_pot = (_max_teu - _min_teu) * _teu_total
    else:
        _economia_pot = None

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("Frete Total", usd(frete_total),
              delta=usd(_delta_frete) + " vs mês ant." if _delta_frete is not None else None,
              delta_color="inverse")
    k2.metric("% Frete / Valor PI", pct(pct_frete),
              delta=f"{(_delta_pct - _delta_pct_ant)*100:+.1f}pp vs mês ant."
                    if _delta_pct and _delta_pct_ant else None,
              delta_color="inverse")
    k3.metric("Frete Médio / TEU", usd(frete_teu))
    k4.metric("Containers", f"{n_cont:,}")
    k5.metric("Processos", f"{n_processos:,}")
    k6.metric("💡 Economia Potencial", usd(_economia_pot) if _economia_pot else "—",
              help="Diferença de custo entre o container mais caro e mais barato por TEU × volume total.")

    st.divider()

    c1, c2 = st.columns([3, 2])

    with c1:
        st.markdown("#### Frete Total por Mês (ETD)")
        st.caption("Período filtrado por ETD · valores em USD")
        if "etd" in fato.columns and "frete" in fato.columns:
            df_mes = fato.copy()
            df_mes["mes"] = pd.to_datetime(df_mes["etd"], errors="coerce").dt.to_period("M").astype(str)
            df_mes = df_mes.groupby("mes")["frete"].sum().reset_index().sort_values("mes")
            media_mes = df_mes["frete"].mean()
            escala_log = st.toggle("Escala logarítmica", value=False, key="log_mes")
            fig = go.Figure()
            fig.add_trace(go.Bar(
                x=df_mes["mes"], y=df_mes["frete"],
                marker_color=ORANGE,
                text=df_mes["frete"].apply(usd),
                textposition="outside",
                textfont=dict(size=9),
            ))
            fig.add_hline(y=media_mes, line_dash="dot", line_color=MUTED,
                          annotation_text=f"Média {usd(media_mes)}",
                          annotation_font_color=MUTED)
            fig.update_layout(
                template=TEMPLATE, height=320,
                xaxis_title="", yaxis_title="USD",
                yaxis=dict(
                    type="log" if escala_log else "linear",
                    tickformat="$,.0f",
                    range=[0, df_mes["frete"].max() * 1.30] if not escala_log else None,
                ),
                bargap=0.3,
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Coluna 'etd' ou 'frete' não encontrada.")

    with c2:
        # MELHORIA: donut → barras horizontais (mais legível)
        st.markdown("#### Frete por Modal")
        st.caption("Participação % no frete total")
        if "modal" in fato.columns and "frete" in fato.columns:
            df_modal = fato.groupby("modal")["frete"].sum().reset_index()
            df_modal["pct"] = df_modal["frete"] / df_modal["frete"].sum()
            df_modal = df_modal.sort_values("frete", ascending=True)
            fig = go.Figure(go.Bar(
                x=df_modal["frete"],
                y=df_modal["modal"],
                orientation="h",
                marker_color=[ORANGE, TEAL, GREEN, PURPLE, BLUE][:len(df_modal)],
                text=df_modal.apply(lambda r: f"{usd(r['frete'])}  ({r['pct']*100:.1f}%)", axis=1),
                textposition="outside",
                textfont=dict(size=11),
            ))
            fig.update_layout(
                template=TEMPLATE, height=320,
                xaxis=dict(tickformat="$,.0f", range=[0, df_modal["frete"].max() * 1.55]),
                xaxis_title="", yaxis_title="",
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Dados de modal não disponíveis.")

    st.divider()

    c3, c4, c5 = st.columns(3)

    with c3:
        st.markdown("#### % Frete / PI por Fornecedor")
        st.caption("Top 10 · mediana como referência")
        if "supplier" in fato.columns and "frete" in fato.columns and "valor_pi" in fato.columns:
            df_sup = fato.groupby("supplier").agg(
                frete=("frete","sum"), pi=("valor_pi","sum")
            ).reset_index()
            df_sup["pct"] = df_sup["frete"] / df_sup["pi"]
            _mediana_pct = df_sup["pct"].median()
            df_sup = df_sup.sort_values("pct", ascending=True).tail(10)
            fig = go.Figure(go.Bar(
                x=df_sup["pct"], y=df_sup["supplier"],
                orientation="h",
                marker_color=[RED if x > _mediana_pct else GREEN for x in df_sup["pct"]],
                text=df_sup["pct"].apply(pct), textposition="outside",
            ))
            fig.add_vline(x=_mediana_pct, line_dash="dot", line_color=MUTED,
                          annotation_text=f"Mediana {pct(_mediana_pct)}",
                          annotation_font_color=MUTED)
            fig.update_layout(template=TEMPLATE, height=300,
                              xaxis=dict(tickformat=".0%"), xaxis_title="")
            st.plotly_chart(fig, use_container_width=True)

    with c4:
        st.markdown("#### Frete Médio / TEU por Tipo")
        st.caption("Média · container 20' vs 40'")
        if not cont.empty and "tipo" in cont.columns and "frete_por_teu" in cont.columns:
            df_teu = cont.groupby("tipo")["frete_por_teu"].mean().reset_index()
            fig = go.Figure(go.Bar(
                x=df_teu["tipo"], y=df_teu["frete_por_teu"],
                marker_color=TEAL, text=df_teu["frete_por_teu"].apply(usd),
                textposition="outside",
            ))
            fig.update_layout(template=TEMPLATE, height=300,
                              yaxis=dict(tickformat="$,.0f",
                                         range=[0, df_teu["frete_por_teu"].max() * 1.30]),
                              xaxis_title="")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Dados de container não disponíveis.")

    with c5:
        st.markdown("#### Top 5 Containers Mais Caros")
        st.caption("Rankeado por frete total")
        if not cont.empty and "frete_total_cont" in cont.columns:
            df_top = cont.nlargest(5, "frete_total_cont")[
                ["num_container","tipo","frete_total_cont","frete_por_teu"]
            ]
            df_top.columns = ["Container","Tipo","Frete Total","Frete/TEU"]
            df_top["Frete Total"] = df_top["Frete Total"].apply(usd)
            df_top["Frete/TEU"]   = df_top["Frete/TEU"].apply(usd)
            st.dataframe(df_top, use_container_width=True, hide_index=True)
        else:
            st.info("Dados de container não disponíveis.")


# ══════════════════════════════════════════════
# TAB 2 — POR CONTAINER
# ══════════════════════════════════════════════
with tab2:
    if cont.empty:
        st.warning("Execute o pipeline_containers.py para gerar dim_containers.csv")
    else:
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Containers Únicos",       f"{len(cont):,}")
        m2.metric("Frete Total",             usd(safe(cont, "frete_total_cont")))
        m3.metric("Frete Médio / Container", usd(safe(cont, "frete_total_cont", np.mean)))
        m4.metric("Frete Médio / TEU",       usd(safe(cont, "frete_por_teu", np.mean)))

        st.divider()

        c1, c2 = st.columns(2)

        with c1:
            # MELHORIA: histograma + linha KDE + mediana
            st.markdown("#### Distribuição Frete Total por Container")
            st.caption("Linha KDE sobreposta · mediana destacada")
            _df_hist = cont.dropna(subset=["frete_total_cont"])
            if not _df_hist.empty:
                _vals = _df_hist["frete_total_cont"].values
                _mediana_hist = np.median(_vals)

                # KDE manual com numpy (sem scipy)
                try:
                    _x_range = np.linspace(_vals.min(), _vals.max(), 200)
                    _h = 1.06 * _vals.std() * len(_vals) ** (-1/5)  # bandwidth Silverman
                    _kde_y = np.array([
                        np.mean(np.exp(-0.5 * ((xi - _vals) / _h) ** 2) / (_h * np.sqrt(2 * np.pi)))
                        for xi in _x_range
                    ])
                    _nbins = 20
                    _bin_w = (_vals.max() - _vals.min()) / _nbins
                    _kde_y_scaled = _kde_y * len(_vals) * _bin_w
                    has_kde = True
                except Exception:
                    has_kde = False

                fig = go.Figure()
                fig.add_trace(go.Histogram(
                    x=_vals, nbinsx=20,
                    marker_color=ORANGE, opacity=0.85,
                    name="Containers",
                ))
                if has_kde:
                    fig.add_trace(go.Scatter(
                        x=_x_range, y=_kde_y_scaled,
                        mode="lines", name="Densidade (KDE)",
                        line=dict(color=BLUE, width=2.5),
                    ))
                fig.add_vline(x=_mediana_hist, line_dash="dash", line_color=YELLOW,
                              annotation_text=f"Mediana {usd(_mediana_hist)}",
                              annotation_font_color=YELLOW,
                              annotation_position="top right")
                fig.update_layout(
                    template=TEMPLATE, height=300,
                    xaxis=dict(tickformat="$,.0f"),
                    xaxis_title="Frete Total (USD)", yaxis_title="Qtd Containers",
                    legend=dict(orientation="h", y=1.1),
                    barmode="overlay",
                )
                st.plotly_chart(fig, use_container_width=True)

        with c2:
            st.markdown("#### Frete / TEU por Tipo de Container")
            st.caption("Distribuição via box plot · outliers visíveis")
            if "tipo" in cont.columns and "frete_por_teu" in cont.columns:
                fig = px.box(
                    cont.dropna(subset=["frete_por_teu","tipo"]),
                    x="tipo", y="frete_por_teu",
                    color="tipo",
                    color_discrete_sequence=[ORANGE, PURPLE, GREEN, TEAL],
                )
                fig.update_layout(template=TEMPLATE, height=300,
                                  yaxis=dict(tickformat="$,.0f"),
                                  xaxis_title="", yaxis_title="Frete/TEU (USD)",
                                  showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # MELHORIA: scatter + linha de regressão + R²
        st.markdown("#### Valor PI vs Frete do Container")
        st.caption("Tamanho do ponto = TEUs · linha de tendência com R²")
        if "valor_pi_total" in cont.columns and "frete_total_cont" in cont.columns:
            df_sc = cont.dropna(subset=["valor_pi_total","frete_total_cont"])
            if len(df_sc) >= 3:
                _x_sc = df_sc["valor_pi_total"].values
                _y_sc = df_sc["frete_total_cont"].values
                _coef_sc = np.polyfit(_x_sc, _y_sc, 1)
                _trend_sc = np.poly1d(_coef_sc)(_x_sc)
                _ss_res = np.sum((_y_sc - _trend_sc)**2)
                _ss_tot = np.sum((_y_sc - _y_sc.mean())**2)
                _r2 = 1 - _ss_res/_ss_tot if _ss_tot > 0 else 0
                _sort_idx = np.argsort(_x_sc)

                fig = px.scatter(
                    df_sc,
                    x="valor_pi_total", y="frete_total_cont",
                    size="teu" if "teu" in df_sc.columns else None,
                    color="tipo" if "tipo" in df_sc.columns else None,
                    hover_data=["num_container"] if "num_container" in df_sc.columns else None,
                    color_discrete_sequence=[ORANGE, PURPLE, GREEN, TEAL],
                )
                fig.add_trace(go.Scatter(
                    x=_x_sc[_sort_idx], y=_trend_sc[_sort_idx],
                    mode="lines",
                    name=f"Tendência (R²={_r2:.2f})",
                    line=dict(color=BLUE, width=2.5, dash="dash"),
                ))
                fig.update_layout(
                    template=TEMPLATE, height=370,
                    xaxis=dict(tickformat="$,.0f"),
                    yaxis=dict(tickformat="$,.0f"),
                    xaxis_title="Valor PI Total (USD)",
                    yaxis_title="Frete Total Container (USD)",
                    legend=dict(orientation="h", y=-0.2),
                )
                st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # MELHORIA: coluna "bls" truncada com contagem
        st.markdown("#### Todos os Containers")
        cols_show = [c for c in [
            "num_container","tipo","teu","qtd_processos","bls",
            "frete_total_cont","frete_por_teu","pct_frete_pi",
            "etd_embarque","eta_chegada","lead_time_medio"
        ] if c in cont.columns]

        df_tab = cont[cols_show].copy()

        # Truncar coluna BLs
        if "bls" in df_tab.columns:
            df_tab["bls"] = df_tab["bls"].apply(
                lambda x: f"{len(str(x).split('|'))} BLs" if pd.notna(x) and str(x).strip() else "—"
            )

        for c in ["frete_total_cont","frete_por_teu"]:
            if c in df_tab.columns:
                df_tab[c] = df_tab[c].apply(usd)
        if "pct_frete_pi" in df_tab.columns:
            df_tab["pct_frete_pi"] = df_tab["pct_frete_pi"].apply(pct)
        if "lead_time_medio" in df_tab.columns:
            df_tab["lead_time_medio"] = df_tab["lead_time_medio"].apply(
                lambda x: f"{x:.0f}d" if pd.notna(x) else "—"
            )
        df_tab.columns = [c.replace("_"," ").title() for c in df_tab.columns]
        st.dataframe(df_tab, use_container_width=True, hide_index=True, height=400)


# ══════════════════════════════════════════════
# TAB 3 — POR FORNECEDOR
# ══════════════════════════════════════════════
with tab3:
    if "supplier" not in fato.columns:
        st.warning("Coluna 'supplier' não encontrada.")
    else:
        df_sup = fato.groupby("supplier").agg(
            frete_total = ("frete",    "sum"),
            valor_pi    = ("valor_pi", "sum"),
            n_processos = ("frete",    "count"),
        ).reset_index()
        df_sup["pct_frete_pi"] = df_sup["frete_total"] / df_sup["valor_pi"]
        df_sup["frete_medio"]  = df_sup["frete_total"]  / df_sup["n_processos"]
        df_sup = df_sup.sort_values("frete_total", ascending=False)

        m1, m2, m3 = st.columns(3)
        m1.metric("Fornecedores Ativos", f"{len(df_sup):,}")
        m2.metric("Maior Frete Total",   usd(df_sup["frete_total"].max()))
        m3.metric("Maior % Frete/PI",    pct(df_sup["pct_frete_pi"].max()))

        st.divider()

        c1, c2 = st.columns(2)

        with c1:
            st.markdown("#### Frete Total por Fornecedor")
            st.caption("Ordenado por valor total · USD")
            # MELHORIA: margem extra para labels não cortarem
            _df_ft = df_sup.sort_values("frete_total", ascending=True)
            _max_ft = _df_ft["frete_total"].max()
            fig = go.Figure(go.Bar(
                x=_df_ft["frete_total"],
                y=_df_ft["supplier"],
                orientation="h",
                marker_color=ORANGE,
                text=_df_ft["frete_total"].apply(usd),
                textposition="outside",
                cliponaxis=False,
            ))
            fig.update_layout(
                template=TEMPLATE, height=max(300, len(_df_ft) * 28),
                xaxis=dict(tickformat="$,.0f", range=[0, _max_ft * 1.40]),
                xaxis_title="",
                margin=dict(l=10, r=90, t=36, b=10),
            )
            st.plotly_chart(fig, use_container_width=True)

        with c2:
            st.markdown("#### % Frete / Valor PI por Fornecedor")
            st.caption("🟢 abaixo da mediana · 🔴 acima")
            _mediana_sup = df_sup["pct_frete_pi"].median()
            df_pct = df_sup.sort_values("pct_frete_pi", ascending=True)
            _max_pct = df_pct["pct_frete_pi"].max()
            fig = go.Figure(go.Bar(
                x=df_pct["pct_frete_pi"],
                y=df_pct["supplier"],
                orientation="h",
                marker_color=[RED if x > _mediana_sup else GREEN for x in df_pct["pct_frete_pi"]],
                text=df_pct["pct_frete_pi"].apply(pct),
                textposition="outside",
                cliponaxis=False,
            ))
            fig.add_vline(x=_mediana_sup, line_dash="dot", line_color=MUTED,
                          annotation_text=f"Mediana {pct(_mediana_sup)}",
                          annotation_font_color=MUTED)
            fig.update_layout(
                template=TEMPLATE, height=max(300, len(df_pct) * 28),
                xaxis=dict(tickformat=".0%", range=[0, _max_pct * 1.35]),
                xaxis_title="",
                margin=dict(l=10, r=60, t=36, b=10),
            )
            st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # MELHORIA: Top N slider + highlight on hover
        st.markdown("#### Evolução Mensal do Frete por Fornecedor")
        st.caption("Selecione o número de fornecedores para visualizar")
        if "etd" in fato.columns:
            _top_n = st.slider("Top N fornecedores", min_value=3, max_value=15, value=6, key="top_n_evol")
            _top_sups = (
                fato.groupby("supplier")["frete"].sum()
                .nlargest(_top_n).index.tolist()
            )

            df_ev = fato.copy()
            df_ev["mes"] = pd.to_datetime(df_ev["etd"], errors="coerce").dt.to_period("M").astype(str)
            df_ev = df_ev.groupby(["mes","supplier"])["frete"].sum().reset_index()
            df_ev_top = df_ev[df_ev["supplier"].isin(_top_sups)]

            fig = px.line(
                df_ev_top, x="mes", y="frete", color="supplier",
                color_discrete_sequence=[ORANGE, PURPLE, GREEN, YELLOW, BLUE, TEAL, RED],
                markers=True,
            )
            fig.update_traces(line=dict(width=2.5))
            fig.update_layout(
                template=TEMPLATE, height=370,
                yaxis=dict(tickformat="$,.0f"),
                xaxis_title="", yaxis_title="Frete (USD)",
                legend=dict(orientation="h", y=-0.25, title_text=""),
            )
            st.plotly_chart(fig, use_container_width=True)

        st.divider()

        st.markdown("#### Resumo por Fornecedor")
        df_tab_sup = df_sup.copy()
        df_tab_sup["frete_total"]  = df_tab_sup["frete_total"].apply(usd)
        df_tab_sup["frete_medio"]  = df_tab_sup["frete_medio"].apply(usd)
        df_tab_sup["valor_pi"]     = df_tab_sup["valor_pi"].apply(usd)
        df_tab_sup["pct_frete_pi"] = df_tab_sup["pct_frete_pi"].apply(pct)
        df_tab_sup.columns = ["Fornecedor","Frete Total","Valor PI","Processos","% Frete/PI","Frete Médio/Processo"]
        st.dataframe(df_tab_sup, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════
# TAB 4 — TENDÊNCIAS
# ══════════════════════════════════════════════
with tab4:
    if "etd" not in fato.columns:
        st.warning("Coluna de data não encontrada.")
    else:
        fato["mes"] = pd.to_datetime(fato["etd"], errors="coerce").dt.to_period("M").astype(str)

        c1, c2 = st.columns(2)

        with c1:
            st.markdown("#### Frete Total Mensal")
            df_t = fato.groupby("mes")["frete"].sum().reset_index()
            media = df_t["frete"].mean()
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df_t["mes"], y=df_t["frete"],
                mode="lines+markers", line=dict(color=ORANGE, width=2.5),
                name="Frete Total",
            ))
            fig.add_hline(y=media, line_dash="dot", line_color=MUTED,
                          annotation_text=f"Média {usd(media)}", annotation_font_color=MUTED)
            fig.update_layout(template=TEMPLATE, height=300,
                              yaxis=dict(tickformat="$.2s", range=[0, df_t["frete"].max() * 1.30]),
                              xaxis_title="")
            st.plotly_chart(fig, use_container_width=True)

        with c2:
            st.markdown("#### % Frete / PI Mensal")
            if "valor_pi" in fato.columns:
                df_pct_t = fato.groupby("mes").agg(
                    frete=("frete","sum"), pi=("valor_pi","sum")
                ).reset_index()
                df_pct_t["pct"] = df_pct_t["frete"] / df_pct_t["pi"]
                _pct_max = min(df_pct_t["pct"].quantile(0.95) * 1.5, 0.50)
                fig = go.Figure(go.Scatter(
                    x=df_pct_t["mes"], y=df_pct_t["pct"],
                    mode="lines+markers", fill="tozeroy",
                    line=dict(color=PURPLE, width=2.5),
                    fillcolor="rgba(123,108,246,0.15)",
                ))
                fig.update_layout(template=TEMPLATE, height=300,
                                  yaxis=dict(tickformat=".1%", range=[0, _pct_max]),
                                  xaxis_title="")
                st.plotly_chart(fig, use_container_width=True)

        c3, c4 = st.columns(2)

        with c3:
            st.markdown("#### Frete Médio / TEU por Mês")
            if not cont_det.empty and "etd" in cont_det.columns and "frete_cont_por_teu" in cont_det.columns:
                df_teu_t = cont_det.copy()
                df_teu_t["mes"] = pd.to_datetime(df_teu_t["etd"], errors="coerce").dt.to_period("M").astype(str)
                df_teu_t = df_teu_t.groupby("mes")["frete_cont_por_teu"].mean().reset_index()
                fig = go.Figure(go.Bar(
                    x=df_teu_t["mes"], y=df_teu_t["frete_cont_por_teu"],
                    marker_color=TEAL,
                    text=df_teu_t["frete_cont_por_teu"].apply(usd),
                    textposition="outside", textfont=dict(size=9),
                ))
                fig.update_layout(template=TEMPLATE, height=300,
                                  yaxis=dict(tickformat="$,.0f",
                                             range=[0, df_teu_t["frete_cont_por_teu"].max() * 1.30]),
                                  xaxis_title="", bargap=0.4)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Execute pipeline_containers para dados de TEU por mês.")

        with c4:
            st.markdown("#### Qtd Containers por Mês")
            if not cont_det.empty and "etd" in cont_det.columns:
                df_cnt = cont_det.copy()
                df_cnt["mes"] = pd.to_datetime(df_cnt["etd"], errors="coerce").dt.to_period("M").astype(str)
                df_cnt = df_cnt.groupby("mes")["num_container"].nunique().reset_index()
                fig = go.Figure(go.Bar(
                    x=df_cnt["mes"], y=df_cnt["num_container"],
                    marker_color=GREEN,
                    text=df_cnt["num_container"], textposition="outside",
                ))
                fig.update_layout(template=TEMPLATE, height=300,
                                  yaxis=dict(title="Containers",
                                             range=[0, max(df_cnt["num_container"].max() * 1.35, 10)]),
                                  xaxis_title="", bargap=0.35)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Execute pipeline_containers para dados de containers por mês.")


# ══════════════════════════════════════════════
# TAB 5 — DRILL-THROUGH
# ══════════════════════════════════════════════
with tab5:
    st.markdown("#### 🔍 Detalhe por Container ou Processo")
    st.caption("ℹ️ A busca considera **todos os anos** disponíveis na base, sem filtro de data.")

    col_busca, col_tipo = st.columns(2)
    with col_busca:
        busca = st.text_input("Buscar container ou BL", placeholder="Ex: FFAU4679971 ou GOC23031-2")
    with col_tipo:
        modo = st.radio("Buscar por", ["Container", "BL / Processo"], horizontal=True)

    if busca:
        _drill_source = cont_det_raw.copy() if not cont_det_raw.empty else cont_det.copy()

        if not _drill_source.empty:
            if modo == "Container" and "num_container" in _drill_source.columns:
                df_res = _drill_source[
                    _drill_source["num_container"].str.upper().str.contains(busca.strip().upper(), na=False)
                ]
            elif "ref_bl" in _drill_source.columns:
                df_res = _drill_source[
                    _drill_source["ref_bl"].str.upper().str.contains(busca.strip().upper(), na=False)
                ]
            else:
                df_res = pd.DataFrame()

            if not df_res.empty:
                r1, r2, r3, r4 = st.columns(4)
                r1.metric("Processos (BLs)",       f"{len(df_res):,}")
                r2.metric("Frete Total Container", usd(safe(df_res, "frete_total_cont", np.mean)))
                r3.metric("Frete / TEU",           usd(safe(df_res, "frete_cont_por_teu", np.mean)))
                r4.metric("% Frete / PI",          pct(safe(df_res, "pct_frete_pi", np.mean)))

                st.divider()

                cols_show = [c for c in [
                    "num_container","tipo","teu","ref_bl","supplier",
                    "etd","eta","modal","status",
                    "frete","frete_total_cont","frete_cont_por_teu",
                    "valor_pi","pct_frete_pi","pct_processo_no_cont"
                ] if c in df_res.columns]

                df_show = df_res[cols_show].copy()

                # MELHORIA: truncar BLs concatenados
                if "bls" in df_show.columns:
                    df_show["bls"] = df_show["bls"].apply(
                        lambda x: f"{len(str(x).split('|'))} BLs" if pd.notna(x) and str(x).strip() else "—"
                    )

                for c in ["frete","frete_total_cont","frete_cont_por_teu","valor_pi"]:
                    if c in df_show.columns:
                        df_show[c] = df_show[c].apply(usd)
                for c in ["pct_frete_pi","pct_processo_no_cont"]:
                    if c in df_show.columns:
                        df_show[c] = df_show[c].apply(pct)

                df_show.columns = [c.replace("_"," ").title() for c in df_show.columns]
                st.dataframe(df_show, use_container_width=True, hide_index=True)

                # MELHORIA: expander com BLs completos
                with st.expander("📋 Ver BLs completos"):
                    if "ref_bl" in df_res.columns:
                        st.write(df_res["ref_bl"].dropna().unique().tolist())
                    elif "bls" in cont_det_raw.columns:
                        _cont_match = df_res["num_container"].iloc[0] if "num_container" in df_res.columns else None
                        if _cont_match:
                            _bls_raw = cont_det_raw[
                                cont_det_raw["num_container"] == _cont_match
                            ]["bls"].dropna().tolist()
                            st.write(_bls_raw)
            else:
                st.info(f"Nenhum resultado para '{busca}'")
        else:
            _fato_busca = fato_raw if not fato_raw.empty else fato
            if "frete" in _fato_busca.columns:
                df_res = _fato_busca[
                    _fato_busca.apply(lambda r: busca.strip().upper() in str(r).upper(), axis=1)
                ]
                if not df_res.empty:
                    st.dataframe(df_res.head(50), use_container_width=True, hide_index=True)
                else:
                    st.info("Nenhum resultado. Execute pipeline_containers para detalhe de containers.")

    else:
        if not cont.empty:
            st.markdown("#### Todos os Containers — Resumo de Frete")
            df_all = cont.copy()

            # MELHORIA: truncar BLs na tabela geral também
            if "bls" in df_all.columns:
                df_all["bls"] = df_all["bls"].apply(
                    lambda x: f"{len(str(x).split('|'))} BLs" if pd.notna(x) and str(x).strip() else "—"
                )

            for c in ["frete_total_cont","frete_por_teu"]:
                if c in df_all.columns:
                    df_all[c] = df_all[c].apply(usd)
            if "pct_frete_pi" in df_all.columns:
                df_all["pct_frete_pi"] = df_all["pct_frete_pi"].apply(pct)
            if "lead_time_medio" in df_all.columns:
                df_all["lead_time_medio"] = df_all["lead_time_medio"].apply(
                    lambda x: f"{x:.0f}d" if pd.notna(x) else "—"
                )
            df_all.columns = [c.replace("_"," ").title() for c in df_all.columns]
            st.dataframe(df_all, use_container_width=True, hide_index=True, height=500)


# ══════════════════════════════════════════════
# TAB 6 — VISÃO OPERACIONAL
# ══════════════════════════════════════════════
with tab6:
    st.markdown("#### 🚢 Visão Operacional de Embarques")
    st.caption("Calculado dinamicamente a partir do Controle de Importações")
    st.divider()

    if fato.empty:
        st.warning("Nenhum dado carregado. Verifique o arquivo fato_importacao.csv.")
    else:
        _f = fato.copy()

        _n_processos  = len(_f)
        _n_fornec     = _f["supplier"].nunique() if "supplier" in _f.columns else 0
        _teu_20       = len(cont[cont["tipo"] == "20'"]) if not cont.empty and "tipo" in cont.columns else 0
        _teu_40       = len(cont[cont["tipo"].isin(["40'","40'HC"])]) if not cont.empty and "tipo" in cont.columns else 0
        _teu_total_op = int(cont["teu"].sum()) if not cont.empty and "teu" in cont.columns else 0
        _frete_medio  = _f["frete"].mean() if "frete" in _f.columns else 0

        k1,k2,k3,k4,k5,k6 = st.columns(6)
        k1.metric("Processos",         f"{_n_processos:,}")
        k2.metric("Fornecedores",      f"{_n_fornec:,}")
        k3.metric("Total TEUs",        f"{_teu_total_op:,}" if _teu_total_op else "—")
        k4.metric("Containers 20'",    f"{_teu_20:,}")
        k5.metric("Containers 40'/HC", f"{_teu_40:,}")
        k6.metric("Frete Médio",       usd(_frete_medio))

        st.divider()

        row1a, row1b = st.columns(2)

        with row1a:
            st.markdown("##### Modal por Nº de Processos")
            if "modal" in _f.columns:
                _df_modal_cnt = _f["modal"].value_counts().reset_index()
                _df_modal_cnt.columns = ["modal","count"]
                _df_modal_cnt["pct"] = _df_modal_cnt["count"] / _df_modal_cnt["count"].sum() * 100
                _df_modal_cnt = _df_modal_cnt.sort_values("count", ascending=True)
                fig = go.Figure(go.Bar(
                    x=_df_modal_cnt["count"], y=_df_modal_cnt["modal"],
                    orientation="h",
                    marker_color=[ORANGE,BLUE,GREEN,PURPLE,TEAL][:len(_df_modal_cnt)],
                    text=_df_modal_cnt.apply(lambda r: f"{int(r['count'])}  ({r['pct']:.1f}%)", axis=1),
                    textposition="outside",
                ))
                _max_x = _df_modal_cnt["count"].max()
                fig.update_layout(
                    template=TEMPLATE, height=max(180, len(_df_modal_cnt)*50),
                    xaxis=dict(range=[0, _max_x * 1.35], title="Processos"),
                    yaxis_title="", margin=dict(l=10,r=10,t=10,b=10),
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Coluna 'modal' não encontrada.")

        with row1b:
            st.markdown("##### Tipo de Container (TEUs)")
            if not cont.empty and "tipo" in cont.columns and "teu" in cont.columns:
                _df_tipo = cont.groupby("tipo")["teu"].sum().reset_index().sort_values("teu", ascending=True)
                fig = go.Figure(go.Bar(
                    x=_df_tipo["teu"], y=_df_tipo["tipo"],
                    orientation="h",
                    marker_color=[ORANGE,BLUE,GREEN,TEAL][:len(_df_tipo)],
                    text=_df_tipo["teu"].apply(lambda x: f"{int(x)} TEUs"),
                    textposition="outside",
                ))
                _max_x = _df_tipo["teu"].max()
                fig.update_layout(
                    template=TEMPLATE, height=max(180, len(_df_tipo)*60),
                    xaxis=dict(range=[0, _max_x * 1.35], title="TEUs"),
                    yaxis_title="", margin=dict(l=10,r=10,t=10,b=10),
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Execute pipeline_containers.py para dados de TEU por tipo.")

        st.divider()

        row2a, row2b = st.columns(2)

        with row2a:
            st.markdown("##### Top 10 Fornecedores — Nº de Processos")
            if "supplier" in _f.columns:
                _df_sup_cnt = _f["supplier"].value_counts().head(10).reset_index()
                _df_sup_cnt.columns = ["supplier","count"]
                _df_sup_cnt = _df_sup_cnt.sort_values("count", ascending=True)
                fig = go.Figure(go.Bar(
                    x=_df_sup_cnt["count"], y=_df_sup_cnt["supplier"],
                    orientation="h", marker_color=TEAL,
                    text=_df_sup_cnt["count"], textposition="outside",
                ))
                _max_x = _df_sup_cnt["count"].max()
                fig.update_layout(
                    template=TEMPLATE, height=380,
                    xaxis=dict(range=[0, _max_x * 1.3], title="Processos"),
                    yaxis_title="", margin=dict(l=10,r=40,t=10,b=10),
                )
                st.plotly_chart(fig, use_container_width=True)

        with row2b:
            st.markdown("##### Top 10 Fornecedores — Valor PI")
            if "supplier" in _f.columns and "valor_pi" in _f.columns:
                _df_sup_pi = _f.groupby("supplier")["valor_pi"].sum().nlargest(10).reset_index()
                _df_sup_pi = _df_sup_pi.sort_values("valor_pi", ascending=True)
                fig = go.Figure(go.Bar(
                    x=_df_sup_pi["valor_pi"], y=_df_sup_pi["supplier"],
                    orientation="h", marker_color=PURPLE,
                    text=_df_sup_pi["valor_pi"].apply(usd),
                    textposition="outside",
                ))
                _max_x = _df_sup_pi["valor_pi"].max()
                fig.update_layout(
                    template=TEMPLATE, height=380,
                    xaxis=dict(range=[0, _max_x * 1.35], tickformat="$,.0f", title="Valor PI (USD)"),
                    yaxis_title="", margin=dict(l=10,r=80,t=10,b=10),
                )
                st.plotly_chart(fig, use_container_width=True)

        st.divider()

        row3a, row3b = st.columns(2)

        with row3a:
            st.markdown("##### Lead Time Real por Modal (ETD → ETA)")
            _col_etd = "etd" if "etd" in _f.columns else "data_etd" if "data_etd" in _f.columns else None
            _col_eta = "eta" if "eta" in _f.columns else "data_entrega" if "data_entrega" in _f.columns else None
            if _col_etd and _col_eta and "modal" in _f.columns:
                _df_lt = _f.copy()
                _df_lt["lead_days"] = (
                    pd.to_datetime(_df_lt[_col_eta], dayfirst=True, errors="coerce") -
                    pd.to_datetime(_df_lt[_col_etd], dayfirst=True, errors="coerce")
                ).dt.days
                _df_lt = _df_lt[(_df_lt["lead_days"] > 0) & (_df_lt["lead_days"] < 180)]
                _df_lt_modal = _df_lt.groupby("modal")["lead_days"].mean().reset_index().sort_values("lead_days")
                if not _df_lt_modal.empty:
                    _cores_lt = [GREEN if d<=25 else YELLOW if d<=40 else RED for d in _df_lt_modal["lead_days"]]
                    fig = go.Figure(go.Bar(
                        x=_df_lt_modal["lead_days"], y=_df_lt_modal["modal"],
                        orientation="h", marker_color=_cores_lt,
                        text=_df_lt_modal["lead_days"].apply(lambda x: f"{x:.0f}d"),
                        textposition="outside",
                    ))
                    _media_lt = _df_lt_modal["lead_days"].mean()
                    _max_lt   = _df_lt_modal["lead_days"].max()
                    fig.add_vline(x=_media_lt, line_dash="dot", line_color=MUTED,
                                  annotation_text=f"Média {_media_lt:.0f}d",
                                  annotation_font_color=MUTED)
                    fig.update_layout(
                        template=TEMPLATE, height=max(180, len(_df_lt_modal)*55),
                        xaxis=dict(range=[0, _max_lt * 1.35], title="Dias"),
                        yaxis_title="", margin=dict(l=10,r=50,t=10,b=10),
                    )
                    st.plotly_chart(fig, use_container_width=True)
                    st.caption("🟢 ≤25d  🟡 26–40d  🔴 >40d")
                else:
                    st.info("Sem dados de lead time válidos.")
            else:
                st.info("Colunas de data (ETD/ETA) não encontradas.")

        with row3b:
            st.markdown("##### Processos por Embarque")
            # MELHORIA: gradiente de intensidade por volume (uma só cor)
            _col_emb = next((c for c in ["embarque","status","situacao","etapa"] if c in _f.columns), None)
            if _col_emb:
                _df_emb = _f[_col_emb].value_counts().reset_index()
                _df_emb.columns = ["valor","count"]
                _total_emb = _df_emb["count"].sum()
                _df_emb["pct"] = _df_emb["count"] / _total_emb

                _mask_outros = _df_emb["pct"] < 0.02
                _df_principais = _df_emb[~_mask_outros].copy()
                _df_outros = _df_emb[_mask_outros]
                if not _df_outros.empty:
                    _outros_row = pd.DataFrame([{
                        "valor": "Outros",
                        "count": _df_outros["count"].sum(),
                        "pct":   _df_outros["pct"].sum(),
                    }])
                    _df_principais = pd.concat([_df_principais, _outros_row], ignore_index=True)

                _df_principais = _df_principais.sort_values("count", ascending=True).tail(11)
                _df_principais["label"] = _df_principais["valor"].apply(
                    lambda x: f"Embarque {x}" if str(x).isdigit() else str(x)
                )
                _df_principais["texto"] = _df_principais.apply(
                    lambda r: f"{int(r['count'])} proc. | {r['pct']*100:.1f}%", axis=1
                )

                # Gradiente azul por volume (quanto maior, mais intenso)
                _norm = _df_principais["count"] / _df_principais["count"].max()
                _colors_grad = [
                    f"rgba(74, 158, 255, {0.35 + 0.65 * v:.2f})" for v in _norm
                ]

                fig = go.Figure(go.Bar(
                    x=_df_principais["count"],
                    y=_df_principais["label"],
                    orientation="h",
                    marker_color=_colors_grad,
                    text=_df_principais["texto"],
                    textposition="outside",
                    textfont=dict(size=10, color="#C8D6F0"),
                    cliponaxis=False,
                ))
                _xmax = _df_principais["count"].max() * 1.55
                _n = len(_df_principais)
                fig.update_layout(
                    template=TEMPLATE, height=max(260, _n * 36),
                    xaxis=dict(range=[0, _xmax], title="Nº de Processos"),
                    yaxis_title="", showlegend=False,
                    margin=dict(l=10, r=10, t=10, b=10),
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Coluna 'embarque' não encontrada.")

        st.divider()

        st.markdown("##### Evolução Mensal de Processos")
        _col_etd2 = "etd" if "etd" in _f.columns else "data_etd" if "data_etd" in _f.columns else None
        if _col_etd2:
            _df_evol = _f.copy()
            _df_evol["mes"] = pd.to_datetime(_df_evol[_col_etd2], dayfirst=True, errors="coerce").dt.to_period("M").astype(str)
            _df_evol_grp = _df_evol.groupby("mes").agg(
                processos=(list(_df_evol.columns)[0], "count"),
                valor_pi=("valor_pi","sum") if "valor_pi" in _df_evol.columns else (list(_df_evol.columns)[0],"count"),
            ).reset_index().sort_values("mes")

            ev1, ev2 = st.columns(2)
            with ev1:
                st.markdown("###### Nº de Processos por Mês")
                fig1 = go.Figure(go.Bar(
                    x=_df_evol_grp["mes"], y=_df_evol_grp["processos"],
                    marker_color=ORANGE,
                    text=_df_evol_grp["processos"], textposition="outside",
                ))
                _max_p = _df_evol_grp["processos"].max()
                fig1.update_layout(
                    template=TEMPLATE, height=280,
                    yaxis=dict(range=[0, _max_p * 1.25], title="Processos"),
                    xaxis_title="", bargap=0.3, margin=dict(l=10,r=10,t=10,b=10),
                )
                st.plotly_chart(fig1, use_container_width=True)

            with ev2:
                st.markdown("###### Valor PI por Mês")
                if "valor_pi" in _df_evol_grp.columns:
                    fig2 = go.Figure(go.Bar(
                        x=_df_evol_grp["mes"], y=_df_evol_grp["valor_pi"],
                        marker_color=BLUE,
                        text=_df_evol_grp["valor_pi"].apply(usd), textposition="outside",
                        textfont=dict(size=9),
                    ))
                    _max_v = _df_evol_grp["valor_pi"].max()
                    fig2.update_layout(
                        template=TEMPLATE, height=280,
                        yaxis=dict(range=[0, _max_v * 1.25], tickformat="$,.0f", title="Valor PI (USD)"),
                        xaxis_title="", bargap=0.3, margin=dict(l=10,r=10,t=10,b=10),
                    )
                    st.plotly_chart(fig2, use_container_width=True)


# ══════════════════════════════════════════════
# TAB 7 — INTELIGÊNCIA DE CUSTO
# ══════════════════════════════════════════════
with tab7:
    st.markdown("#### 🎯 Inteligência de Custo de Frete")
    st.caption("Análises preditivas, alertas e oportunidades de economia")

    st.markdown("##### 🚨 Alertas Automáticos")

    alertas = []

    if "frete" in fato.columns and "valor_pi" in fato.columns:
        _df_alerta = fato.copy()
        _df_alerta["pct_ind"] = _df_alerta["frete"] / _df_alerta["valor_pi"].replace(0, np.nan)

        _acima = _df_alerta[_df_alerta["pct_ind"] > 0.15]
        if len(_acima) > 0:
            alertas.append(("🔴", f"{len(_acima)} processo(s) com frete acima de 15% do valor da PI", "danger"))

        _media_pct = _df_alerta["pct_ind"].mean()
        if _media_pct > 0.08:
            alertas.append(("🟠", f"% Frete médio em {_media_pct*100:.1f}% — acima do benchmark de 8%", "warning"))

        if "etd" in fato.columns:
            _fd = fato.copy()
            _fd["_mes"] = pd.to_datetime(_fd["etd"], errors="coerce").dt.to_period("M")
            _mes_grp = _fd.groupby("_mes")["frete"].sum()
            if len(_mes_grp) >= 3:
                _media_hist = _mes_grp.iloc[:-1].mean()
                _ultimo = _mes_grp.iloc[-1]
                _variacao = (_ultimo - _media_hist) / _media_hist
                if _variacao > 0.20:
                    alertas.append(("🔴", f"Frete do último mês {_variacao*100:.0f}% acima da média histórica ({usd(_media_hist)})", "danger"))
                elif _variacao < -0.10:
                    alertas.append(("🟢", f"Frete do último mês {abs(_variacao)*100:.0f}% abaixo da média — ótimo desempenho!", "success"))

    if not cont.empty and "frete_por_teu" in cont.columns:
        _cont_filtrado = cont.copy()
        _col_etd_cont = next((c for c in ["etd_embarque","etd","data_etd"] if c in _cont_filtrado.columns), None)
        if _col_etd_cont:
            _cont_filtrado[_col_etd_cont] = pd.to_datetime(_cont_filtrado[_col_etd_cont], errors="coerce")
            _cont_filtrado = _cont_filtrado[_cont_filtrado[_col_etd_cont].dt.year >= 2025]

        _cont_valido = _cont_filtrado[_cont_filtrado["frete_por_teu"].notna() & (_cont_filtrado["frete_por_teu"] > 0)]

        if len(_cont_valido) >= 3:
            _media_teu_al = _cont_valido["frete_por_teu"].mean()
            _std_teu      = _cont_valido["frete_por_teu"].std()
            _outliers     = _cont_valido[_cont_valido["frete_por_teu"] > _media_teu_al + 2 * _std_teu]
            if len(_outliers) > 0:
                _nomes = ", ".join(_outliers["num_container"].astype(str).head(3).tolist()) if "num_container" in _outliers.columns else str(len(_outliers))
                alertas.append(("🔴", (
                    f"{len(_outliers)} container(s) com frete/TEU anormal (>2σ) no período filtrado: {_nomes} "
                    f"— Média: {usd(_media_teu_al)} | Limiar: {usd(_media_teu_al + 2*_std_teu)}"
                ), "danger"))

        if "supplier" in fato.columns and "frete" in fato.columns and "valor_pi" in fato.columns:
            _sup_pct = fato.groupby("supplier").apply(
                lambda x: x["frete"].sum() / x["valor_pi"].sum() if x["valor_pi"].sum() > 0 else np.nan
            ).dropna()
            if len(_sup_pct) >= 2:
                _sup_max = _sup_pct.idxmax()
                _sup_min = _sup_pct.idxmin()
                _gap = (_sup_pct.max() - _sup_pct.min()) * 100
                alertas.append(("🟡", f"Gap de frete entre fornecedores: {_sup_max} ({_sup_pct.max()*100:.1f}%) vs {_sup_min} ({_sup_pct.min()*100:.1f}%) — diferença de {_gap:.1f}pp", "warning"))

    if not alertas:
        alertas.append(("🟢", "Nenhum alerta identificado — frete dentro dos parâmetros normais.", "success"))

    _cores_al  = {"danger": "#F04E4E22", "warning": "#F5B73122", "success": "#22C97B22"}
    _bordas_al = {"danger": "#F04E4E",   "warning": "#F5B731",   "success": "#22C97B"}
    for emoji, msg, nivel in alertas:
        st.markdown(
            f'<div style="background:{_cores_al[nivel]};border-left:4px solid {_bordas_al[nivel]};'
            f'border-radius:6px;padding:10px 14px;margin-bottom:8px;font-size:13px;color:#C8D6F0;">'
            f'{emoji} {msg}</div>',
            unsafe_allow_html=True,
        )

    st.divider()

    st.markdown("##### 📈 Previsão de Frete — Próximos 3 Meses")

    if "etd" in fato.columns and "frete" in fato.columns:
        _fp = fato.copy()
        _fp["_mes"] = pd.to_datetime(_fp["etd"], errors="coerce").dt.to_period("M")
        _serie = _fp.groupby("_mes")["frete"].sum().reset_index()
        _serie["_mes_str"] = _serie["_mes"].astype(str)
        _serie = _serie.sort_values("_mes").tail(18)

        if len(_serie) >= 4:
            _x = np.arange(len(_serie))
            _y = _serie["frete"].values
            _coef = np.polyfit(_x, _y, 1)
            _poly = np.poly1d(_coef)

            _last_period  = _serie["_mes"].iloc[-1]
            _future_periods = [(_last_period + i).strftime("%Y-%m") for i in range(1, 4)]
            _future_x = np.arange(len(_serie), len(_serie) + 3)
            _future_y = np.maximum(_poly(_future_x), 0)
            _trend_y  = _poly(_x)

            fig_pred = go.Figure()
            fig_pred.add_trace(go.Bar(
                x=_serie["_mes_str"], y=_serie["frete"],
                name="Histórico", marker_color=ORANGE, opacity=0.85,
            ))
            fig_pred.add_trace(go.Scatter(
                x=_serie["_mes_str"], y=_trend_y,
                name="Tendência", mode="lines",
                line=dict(color=BLUE, width=2, dash="dot"),
            ))
            fig_pred.add_trace(go.Bar(
                x=_future_periods, y=_future_y,
                name="Projeção", marker_color=PURPLE, opacity=0.7,
            ))
            for _fp_date, _fp_val in zip(_future_periods, _future_y):
                fig_pred.add_annotation(
                    x=_fp_date, y=_fp_val, text=usd(_fp_val),
                    showarrow=False, yshift=14, font=dict(size=10, color=PURPLE),
                )
            fig_pred.update_layout(
                template=TEMPLATE, height=350,
                yaxis=dict(tickformat="$,.0f"), xaxis_title="",
                legend=dict(orientation="h", y=1.1),
                barmode="overlay",
            )
            st.plotly_chart(fig_pred, use_container_width=True)
            st.caption("⚠️ Projeção baseada em regressão linear sobre histórico. Não considera sazonalidade ou variações cambiais.")
        else:
            st.info("Histórico insuficiente para projeção (mínimo 4 meses).")
    else:
        st.info("Coluna 'etd' necessária para análise preditiva.")

    st.divider()

    st.markdown("##### 🗓️ Heatmap — Frete por Fornecedor × Mês")

    if "etd" in fato.columns and "supplier" in fato.columns and "frete" in fato.columns:
        _hf = fato.copy()
        _hf["_mes"] = pd.to_datetime(_hf["etd"], errors="coerce").dt.to_period("M").astype(str)
        _pivot = _hf.groupby(["supplier","_mes"])["frete"].sum().unstack(fill_value=0)
        _pivot = _pivot.loc[_pivot.sum(axis=1).nlargest(12).index]
        _pivot = _pivot[sorted(_pivot.columns)[-18:]]

        fig_heat = go.Figure(go.Heatmap(
            z=_pivot.values,
            x=_pivot.columns.tolist(),
            y=_pivot.index.tolist(),
            colorscale=[[0, "#0c1b3a"], [0.5, ORANGE], [1, "#FF2200"]],
            text=[[usd(v) if v > 0 else "" for v in row] for row in _pivot.values],
            texttemplate="%{text}",
            textfont=dict(size=9),
            hoverongaps=False,
        ))
        fig_heat.update_layout(
            template=TEMPLATE, height=420,
            xaxis_title="", yaxis_title="",
            margin=dict(l=140, r=10, t=20, b=40),
        )
        st.plotly_chart(fig_heat, use_container_width=True)
    else:
        st.info("Colunas 'etd', 'supplier' e 'frete' necessárias para o heatmap.")

    st.divider()

    st.markdown("##### 💰 Ranking de Oportunidades de Economia")
    st.caption("Fornecedores onde o % frete está acima da mediana — potencial de renegociação")

    if "supplier" in fato.columns and "frete" in fato.columns and "valor_pi" in fato.columns:
        _df_eco = fato.groupby("supplier").agg(
            frete_total=("frete",    "sum"),
            pi_total   =("valor_pi", "sum"),
            n_processos=("frete",    "count"),
        ).reset_index()
        _df_eco["pct_frete"]   = _df_eco["frete_total"] / _df_eco["pi_total"].replace(0, np.nan)
        _mediana_eco           = _df_eco["pct_frete"].median()
        _df_eco["excesso_pct"] = (_df_eco["pct_frete"] - _mediana_eco).clip(lower=0)
        _df_eco["economia_pot"]= _df_eco["excesso_pct"] * _df_eco["pi_total"]
        _df_eco = _df_eco[_df_eco["economia_pot"] > 0].sort_values("economia_pot", ascending=False).head(10)

        if not _df_eco.empty:
            fig_eco = go.Figure(go.Bar(
                x=_df_eco["economia_pot"],
                y=_df_eco["supplier"],
                orientation="h",
                marker_color=[RED if v > _df_eco["economia_pot"].median() else YELLOW for v in _df_eco["economia_pot"]],
                text=_df_eco["economia_pot"].apply(usd),
                textposition="outside",
            ))
            fig_eco.add_vline(x=_df_eco["economia_pot"].median(), line_dash="dot",
                              line_color=MUTED, annotation_text="Mediana",
                              annotation_font_color=MUTED)
            fig_eco.update_layout(
                template=TEMPLATE, height=380,
                xaxis=dict(tickformat="$,.0f"), xaxis_title="Economia Potencial (USD)",
                margin=dict(l=160, r=80, t=20, b=40),
            )
            st.plotly_chart(fig_eco, use_container_width=True)

            _df_show_eco = _df_eco[["supplier","n_processos","pct_frete","economia_pot"]].copy()
            _df_show_eco["pct_frete"]    = _df_show_eco["pct_frete"].apply(pct)
            _df_show_eco["economia_pot"] = _df_show_eco["economia_pot"].apply(usd)
            _df_show_eco.columns = ["Fornecedor","Processos","% Frete Atual","Economia Potencial"]
            st.dataframe(_df_show_eco, use_container_width=True, hide_index=True)
        else:
            st.success("Todos os fornecedores estão abaixo da mediana de % frete. 🎉")
    else:
        st.info("Dados insuficientes para calcular oportunidades de economia.")