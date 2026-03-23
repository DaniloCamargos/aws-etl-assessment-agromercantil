#importando as bibliotecas
import os
import warnings
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv

warnings.filterwarnings("ignore")
load_dotenv()

#configuração da página
st.set_page_config(
     page_title= "Commodities · Agromercantil"
    ,page_icon= "🌾"
    ,layout= "wide"
    ,initial_sidebar_state= "expanded"
)

#paleta de cores padrão do projeto
PALETA = [
     "#378ADD", "#1D9E75", "#D85A30", "#BA7517"
    ,"#7F77DD", "#D4537E", "#639922", "#E24B4A"
]

#mapeamento de colunas para nomes padronizados
RENOMEAR = {
     "data_referencia": "dt_ref"
    ,"valor_brl": "val_brl"
    ,"variacao_diaria_pct" : "pct_var_dia"
    ,"valor_usd": "val_usd"
}

CSV_DIR = os.getenv(
     "CSV_DIR"
    ,r"C:\Users\dcamargos\Desktop\aws-etl-assessment-agromercantil\inputs\csv"
)

#layout base reutilizável para todos os gráficos plotly
LAYOUT_BASE = dict(
     paper_bgcolor = "#FAFAFA"
    ,plot_bgcolor  = "#F5F5F2"
    ,font= dict(family="Inter, sans-serif", size=12, color="#333")
    ,margin= dict(l=50, r=20, t=50, b=50)
    ,legend= dict(bgcolor="rgba(255,255,255,0.7)", borderwidth=0)
)


#retorna uma lista de cores para cada cultivo, ciclando a paleta se necessário
def cores_para(cultivos):
    n = len(cultivos)
    return (PALETA * ((n // len(PALETA)) + 1))[:n]


#retorna um dicionário cultivo → cor para uso no plotly
def mapa_cor(cultivos):
    return dict(zip(sorted(cultivos), cores_para(sorted(cultivos))))


#renderiza um card de kpi com html
def kpi(label, valor, delta=None, accent="#378ADD"):
    delta_html = ""
    if delta is not None:
        cls= "delta-pos" if delta >= 0 else "delta-neg"
        sinal = "▲" if delta >= 0 else "▼"
        delta_html = f'<div class="kpi-delta {cls}">{sinal} {abs(delta):.2f}%</div>'
    st.markdown(f"""
    <div class="kpi-card" style="--accent:{accent}">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{valor}</div>
        {delta_html}
    </div>""", unsafe_allow_html=True)


#css customizado da aplicação
st.markdown("""
<style>
    .stApp { background-color: #F5F5F2; }

    section[data-testid="stSidebar"]                     { background-color: #1E2A38; }
    section[data-testid="stSidebar"] *                   { color: #E8EDF3 !important; }
    section[data-testid="stSidebar"] .stMultiSelect span { background: #378ADD33; }

    .kpi-card {
        background: white; border-radius: 12px;
        padding: 18px 22px; box-shadow: 0 2px 8px rgba(0,0,0,0.07);
        border-left: 4px solid var(--accent);
    }
    .kpi-label { font-size: 12px; color: #888; text-transform: uppercase; letter-spacing: .05em; margin-bottom: 4px; }
    .kpi-value { font-size: 26px; font-weight: 700; color: #1E2A38; line-height: 1.1; }
    .kpi-delta { font-size: 12px; margin-top: 4px; }
    .delta-pos { color: #1D9E75; } .delta-neg { color: #D85A30; }

    .sec-title {
        font-size: 15px; font-weight: 600; color: #1E2A38;
        border-bottom: 2px solid #378ADD33; padding-bottom: 6px; margin-bottom: 16px;
    }

    .stTabs [data-baseweb="tab-list"] { gap: 6px; }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px 8px 0 0; padding: 8px 18px;
        font-weight: 500; background: #E8EDF3; color: #555;
    }
    .stTabs [aria-selected="true"] { background: #378ADD !important; color: white !important; }
</style>
""", unsafe_allow_html=True)


#carregamento

@st.cache_data(show_spinner="Carregando dados...")
def carregar_dados(csv_dir):
    arquivos = [f for f in os.listdir(csv_dir) if f.endswith(".csv")]
    if not arquivos:
        raise FileNotFoundError(f"nenhum .csv encontrado em: {csv_dir}")

    partes = []
    for arquivo in arquivos:
        caminho = os.path.join(csv_dir, arquivo)
        cultivo = os.path.splitext(arquivo)[0].replace("raw_", "").split("_")[0]
        df_tmp  = pd.read_csv(
             caminho
            ,sep= ";"
            ,encoding= "utf-8"
            ,on_bad_lines= "skip"
            ,engine= "python"
        )
        df_tmp["cultivo"] = cultivo
        partes.append(df_tmp)

    df = pd.concat(partes, ignore_index=True).rename(columns=RENOMEAR)

    if "dt_ref" in df.columns:
        df["dt_ref"] = pd.to_datetime(df["dt_ref"], dayfirst=True, errors="coerce")

    for col in ("val_brl", "pct_var_dia", "val_usd"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    #tenta mapear coluna de região por aliases comuns, ou cria como N/A
    if "regiao" not in df.columns:
        for alias in ("region", "estado", "uf", "local", "origem"):
            if alias in df.columns:
                df.rename(columns={alias: "regiao"}, inplace=True)
                break
        else:
            df["regiao"] = "N/A"

    return df.dropna(subset=["val_brl"])


#estatistica

@st.cache_data(show_spinner=False)
def calcular_estatisticas(df):
    stats = (
        df.groupby("cultivo")["val_brl"]
        .agg(
             contagem= "count"
            ,media= "mean"
            ,mediana= "median"
            ,desvio_pad= "std"
            ,minimo= "min"
            ,maximo= "max"
            ,q1= lambda x: x.quantile(0.25)
            ,q3= lambda x: x.quantile(0.75)
        )
        .round(2).reset_index()
    )
    stats["amplitude_iqr"] = (stats["q3"] - stats["q1"]).round(2)
    stats["cv_pct"]        = (stats["desvio_pad"] / stats["media"] * 100).round(2)
    return stats


#outliers

@st.cache_data(show_spinner=False)
def detectar_outliers(df):
    resultados = []
    for cultivo, grupo in df.groupby("cultivo"):
        serie= grupo["val_brl"].dropna()
        q1, q3= serie.quantile(0.25), serie.quantile(0.75)
        iqr= q3 - q1
        media, std= serie.mean(), serie.std()

        for metodo, mask, lim_inf, lim_sup in [
            ("IQR",     (serie < q1 - 1.5*iqr) | (serie > q3 + 1.5*iqr), round(q1 - 1.5*iqr, 2), round(q3 + 1.5*iqr, 2)),
            ("Z-score", (serie - media).abs() / (std + 1e-9) > 3,          round(media - 3*std, 2), round(media + 3*std, 2)),
        ]:
            out = grupo.loc[mask.index[mask]].copy()
            out["metodo"], out["lim_inf"], out["lim_sup"] = metodo, lim_inf, lim_sup
            resultados.append(out)

    return pd.concat(resultados, ignore_index=True) if resultados else pd.DataFrame()


#graficos

def fig_serie_temporal(df):
    serie = (
        df.groupby(["dt_ref", "cultivo"])["val_brl"]
        .mean().reset_index().sort_values("dt_ref")
    )
    fig = px.line(
         serie
        ,x= "dt_ref"
        ,y= "val_brl"
        ,color= "cultivo"
        ,color_discrete_map = mapa_cor(df["cultivo"].unique())
        ,labels= {"dt_ref": "Data", "val_brl": "Preço médio (R$)", "cultivo": "Commodity"}
        ,title= "Evolução de preços — série temporal"
    )
    fig.update_traces(line_width=2)
    fig.update_layout(**LAYOUT_BASE)
    return fig


def fig_barras_media(df):
    media = df.groupby("cultivo")["val_brl"].mean().reset_index().sort_values("val_brl", ascending=False)
    fig = px.bar(
         media
        ,x= "cultivo"
        ,y= "val_brl"
        ,color= "cultivo"
        ,color_discrete_map = mapa_cor(df["cultivo"].unique())
        ,text_auto= ".2f"
        ,labels= {"cultivo": "Commodity", "val_brl": "Preço médio (R$)"}
        ,title= "Preço médio por commodity"
    )
    fig.update_traces(textposition="outside", marker_line_width=0)
    fig.update_layout(**LAYOUT_BASE, showlegend=False)
    return fig


def fig_variacao_barras(df):
    if "pct_var_dia" not in df.columns:
        return None
    media_var = (
        df.groupby("cultivo")["pct_var_dia"]
        .mean().reset_index()
        .sort_values("pct_var_dia", ascending=False)
    )
    cores_bar = ["#1D9E75" if v >= 0 else "#D85A30" for v in media_var["pct_var_dia"]]
    fig = go.Figure(go.Bar(
         x= media_var["cultivo"]
        ,y= media_var["pct_var_dia"]
        ,marker_color= cores_bar
        ,text= media_var["pct_var_dia"].round(2)
        ,textposition= "outside"
    ))
    fig.add_hline(y=0, line_color="#999", line_width=1)
    fig.update_layout(**LAYOUT_BASE
        ,title= "Variação diária média por commodity (%)"
        ,xaxis_title= "Commodity"
        ,yaxis_title= "Variação média (%)"
        ,showlegend= False
    )
    return fig


def fig_boxplot(df):
    cultivos = sorted(df["cultivo"].unique())
    fig      = go.Figure()
    for cultivo, cor in zip(cultivos, cores_para(cultivos)):
        sub = df.loc[df["cultivo"] == cultivo, "val_brl"].dropna()
        fig.add_trace(go.Box(
             y= sub
            ,name= cultivo
            ,marker_color= cor
            ,boxmean= "sd"
            ,line_width= 1.5
        ))
    fig.update_layout(**LAYOUT_BASE
        ,title= "Distribuição de preços por commodity — boxplot"
        ,yaxis_title= "Valor BRL (R$)"
        ,showlegend= False
    )
    return fig


def fig_scatter(df):
    if "pct_var_dia" not in df.columns:
        return None
    fig = px.scatter(
         df.dropna(subset=["pct_var_dia"])
        ,x= "val_brl"
        ,y= "pct_var_dia"
        ,color= "cultivo"
        ,color_discrete_map= mapa_cor(df["cultivo"].unique())
        ,opacity= 0.55
        ,labels= {"val_brl": "Valor BRL (R$)", "pct_var_dia": "Variação diária (%)", "cultivo": "Commodity"}
        ,title= "Preço BRL × variação diária"
    )
    fig.add_hline(y=0, line_dash="dash", line_color="#aaa", line_width=1)
    fig.update_layout(**LAYOUT_BASE)
    return fig


def fig_histograma(df, cultivo, cor):
    data = df.loc[df["cultivo"] == cultivo, "val_brl"].dropna()
    fig  = go.Figure()
    fig.add_trace(go.Histogram(
         x= data
        ,nbinsx= 20
        ,name= cultivo
        ,marker_color= cor
        ,opacity= 0.82
        ,marker_line= dict(width=0.5, color="white")
    ))
    fig.add_vline(x=data.mean(),   line_dash="dash", line_color="#333",
                  annotation_text=f"média {data.mean():.1f}",     annotation_position="top right")
    fig.add_vline(x=data.median(), line_dash="dot",  line_color="#888",
                  annotation_text=f"mediana {data.median():.1f}", annotation_position="top left")
    fig.update_layout(**LAYOUT_BASE
        ,title= cultivo
        ,xaxis_title= "R$"
        ,yaxis_title= "frequência"
        ,showlegend= False
        ,margin= dict(l=40, r=10, t=40, b=40)
    )
    return fig


#sidebar

with st.sidebar:
    st.markdown("## 🌾 Agromercantil")
    st.markdown("---")

    csv_dir_input = st.text_input("Diretório dos CSVs", value=CSV_DIR)

    try:
        df_raw = carregar_dados(csv_dir_input)
    except Exception as e:
        st.error(f"erro ao carregar dados: {e}")
        st.stop()

    st.markdown("### Filtros")

    #filtro por commodity
    cultivos_disp= sorted(df_raw["cultivo"].unique())
    cultivos_sel= st.multiselect("Commodity", cultivos_disp, default=cultivos_disp, placeholder="Selecione...")

    #filtro por região
    regioes_disp= sorted(df_raw["regiao"].dropna().unique())
    regioes_sel= st.multiselect("Região", regioes_disp
        ,default= regioes_disp if "N/A" not in regioes_disp else []
        ,placeholder= "Todas"
    )

    #filtro de período
    tem_data = "dt_ref" in df_raw.columns and df_raw["dt_ref"].notna().any()
    if tem_data:
        dt_min= df_raw["dt_ref"].min().date()
        dt_max= df_raw["dt_ref"].max().date()
        periodo= st.date_input("Período", value=(dt_min, dt_max), min_value=dt_min, max_value=dt_max)
    else:
        periodo= None

    #seleção de moeda
    moeda= st.radio("Moeda", ["BRL (R$)", "USD (US$)"], horizontal=True)
    col_val= "val_brl" if moeda.startswith("BRL") else ("val_usd" if "val_usd" in df_raw.columns else "val_brl")
    simbolo= "R$" if moeda.startswith("BRL") else "US$"

    st.markdown("---")
    st.caption("Agromercantil · Dashboard v1.0")


#aplicando filtros aos dados

df= df_raw.copy()

if cultivos_sel:
    df = df[df["cultivo"].isin(cultivos_sel)]
if regioes_sel:
    df = df[df["regiao"].isin(regioes_sel)]
if periodo and len(periodo) == 2 and tem_data:
    df = df[(df["dt_ref"].dt.date >= periodo[0]) & (df["dt_ref"].dt.date <= periodo[1])]

#se moeda for USD, substitui val_brl pela coluna correspondente
if col_val != "val_brl" and col_val in df.columns:
    df            = df.copy()
    df["val_brl"] = df[col_val]

if df.empty:
    st.warning("⚠️ nenhum dado para os filtros selecionados.")
    st.stop()


#header

st.markdown("# 🌾 Dashboard de Commodities Agrícolas")
st.markdown(
     f"**{len(df):,}** registros · "
     f"**{df['cultivo'].nunique()}** commodities · "
     f"**{df['regiao'].nunique()}** regiões"
    + (f" · {df['dt_ref'].min().date()} → {df['dt_ref'].max().date()}" if tem_data else "")
)
st.markdown("---")

#kpis

stats = calcular_estatisticas(df)

c1, c2, c3, c4, c5 = st.columns(5)
with c1: kpi("Preço médio geral",  f"{simbolo} {df['val_brl'].mean():,.2f}",   accent="#378ADD")
with c2: kpi("Preço mediano",      f"{simbolo} {df['val_brl'].median():,.2f}", accent="#1D9E75")
with c3: kpi("Máximo registrado",  f"{simbolo} {df['val_brl'].max():,.2f}",    accent="#BA7517")
with c4: kpi("Mínimo registrado",  f"{simbolo} {df['val_brl'].min():,.2f}",    accent="#7F77DD")
with c5:
    if "pct_var_dia" in df.columns:
        med_var = df["pct_var_dia"].mean()
        kpi("Variação média diária", f"{med_var:+.2f}%", delta=med_var, accent="#D4537E")
    else:
        kpi("Commodities", str(df["cultivo"].nunique()), accent="#D4537E")

st.markdown("<br>", unsafe_allow_html=True)


tab1, tab2, tab3, tab4, tab5 = st.tabs([
     "📈 Tendências"
    ,"📊 Distribuições"
    ,"🔍 Análise por Commodity"
    ,"⚠️ Outliers"
    ,"📋 Estatísticas"
])


#tendências: série temporal + barras de média + variação diária
with tab1:
    st.markdown('<div class="sec-title">Série Temporal de Preços</div>', unsafe_allow_html=True)
    if tem_data:
        st.plotly_chart(fig_serie_temporal(df), use_container_width=True)
    else:
        st.info("coluna dt_ref não disponível nos dados filtrados.")

    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown('<div class="sec-title">Preço Médio por Commodity</div>', unsafe_allow_html=True)
        st.plotly_chart(fig_barras_media(df), use_container_width=True)
    with col_b:
        st.markdown('<div class="sec-title">Variação Diária Média</div>', unsafe_allow_html=True)
        fig_var = fig_variacao_barras(df)
        if fig_var:
            st.plotly_chart(fig_var, use_container_width=True)
        else:
            st.info("coluna pct_var_dia não disponível.")


#distribuições: boxplot + scatter
with tab2:
    col_a, col_b = st.columns([3, 2])
    with col_a:
        st.markdown('<div class="sec-title">Boxplot por Commodity</div>', unsafe_allow_html=True)
        st.plotly_chart(fig_boxplot(df), use_container_width=True)
    with col_b:
        st.markdown('<div class="sec-title">Preço × Variação Diária</div>', unsafe_allow_html=True)
        fig_sc = fig_scatter(df)
        if fig_sc:
            st.plotly_chart(fig_sc, use_container_width=True)
        else:
            st.info("coluna pct_var_dia não disponível.")


#análise individual por commodity: kpis + histograma + série + dados brutos
with tab3:
    cultivo_sel = st.selectbox("Selecione a commodity", sorted(df["cultivo"].unique()))
    df_c        = df[df["cultivo"] == cultivo_sel]
    cor_c       = cores_para(sorted(df["cultivo"].unique()))[sorted(df["cultivo"].unique()).index(cultivo_sel)]

    k1, k2, k3, k4 = st.columns(4)
    with k1: kpi("Média", f"{simbolo} {df_c['val_brl'].mean():,.2f}",   accent=cor_c)
    with k2: kpi("Mediana", f"{simbolo} {df_c['val_brl'].median():,.2f}", accent=cor_c)
    with k3: kpi("Desvio", f"{simbolo} {df_c['val_brl'].std():,.2f}",    accent=cor_c)
    with k4: kpi("Registros", f"{len(df_c):,}",                             accent=cor_c)

    st.markdown("<br>", unsafe_allow_html=True)
    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown('<div class="sec-title">Histograma de preços</div>', unsafe_allow_html=True)
        st.plotly_chart(fig_histograma(df_c, cultivo_sel, cor_c), use_container_width=True)

    with col_b:
        if tem_data:
            st.markdown('<div class="sec-title">Série temporal</div>', unsafe_allow_html=True)
            serie_c = df_c.groupby("dt_ref")["val_brl"].mean().reset_index().sort_values("dt_ref")
            fig_st  = px.line(
                 serie_c
                ,x= "dt_ref"
                ,y= "val_brl"
                ,labels = {"dt_ref": "Data", "val_brl": f"Preço ({simbolo})"}
            )
            fig_st.update_traces(line_color=cor_c, line_width=2.2)
            fig_st.update_layout(**LAYOUT_BASE, showlegend=False)
            st.plotly_chart(fig_st, use_container_width=True)

    with st.expander("🗂️ dados brutos desta commodity"):
        cols_exibir = [c for c in ["dt_ref", "regiao", "val_brl", "val_usd", "pct_var_dia"] if c in df_c.columns]
        st.dataframe(
             df_c[cols_exibir].sort_values(cols_exibir[0])
            ,use_container_width = True
            ,height              = 300
        )


#outliers: barras agrupadas iqr vs z-score + tabela resumo + detalhes
with tab4:
    df_out = detectar_outliers(df)

    if df_out.empty:
        st.success("✅ nenhum outlier detectado nos dados filtrados.")
    else:
        resumo = df_out.groupby(["cultivo", "metodo"]).size().reset_index(name="qtd_outliers")

        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown('<div class="sec-title">Quantidade de outliers por método</div>', unsafe_allow_html=True)
            fig_out = px.bar(
                 resumo
                ,x= "cultivo"
                ,y= "qtd_outliers"
                ,color= "metodo"
                ,barmode= "group"
                ,color_discrete_sequence= ["#378ADD", "#D85A30"]
                ,labels= {"cultivo": "Commodity", "qtd_outliers": "Qtd. outliers", "metodo": "Método"}
            )
            fig_out.update_layout(**LAYOUT_BASE)
            st.plotly_chart(fig_out, use_container_width=True)

        with col_b:
            st.markdown('<div class="sec-title">Resumo</div>', unsafe_allow_html=True)
            st.dataframe(resumo, use_container_width=True, hide_index=True)
            st.markdown(f"**total de registros flagados:** {len(df_out):,}")

        with st.expander("🗂️ registros outliers detalhados"):
            cols_out = [c for c in ["cultivo", "dt_ref", "regiao", "val_brl", "metodo", "lim_inf", "lim_sup"] if c in df_out.columns]
            st.dataframe(df_out[cols_out], use_container_width=True, height=350)


#estatísticas descritivas: tabela + coeficiente de variação + download
with tab5:
    st.markdown('<div class="sec-title">Estatísticas Descritivas por Commodity (val_brl em R$)</div>', unsafe_allow_html=True)

    col_a, col_b = st.columns([2, 3])
    with col_a:
        st.dataframe(
             stats.style.format({c: "{:,.2f}" for c in stats.select_dtypes("number").columns})
            ,use_container_width= True
            ,hide_index= True
            ,height= 400
        )
    with col_b:
        fig_cv = px.bar(
             stats.sort_values("cv_pct", ascending=False)
            ,x= "cultivo"
            ,y= "cv_pct"
            ,color= "cultivo"
            ,color_discrete_sequence = cores_para(sorted(stats["cultivo"].unique()))
            ,text_auto= ".1f"
            ,labels= {"cultivo": "Commodity", "cv_pct": "Coef. de variação (%)"}
            ,title= "Coeficiente de variação por commodity — quanto maior, mais volátil"
        )
        fig_cv.update_layout(**LAYOUT_BASE, showlegend=False)
        st.plotly_chart(fig_cv, use_container_width=True)

    #botão de download das estatísticas em csv
    csv_bytes = stats.to_csv(sep=";", index=False, encoding="utf-8").encode("utf-8")
    st.download_button(
         "⬇️ Baixar estatísticas (.csv)"
        ,data= csv_bytes
        ,file_name= "estatisticas_descritivas.csv"
        ,mime= "text/csv"
    )