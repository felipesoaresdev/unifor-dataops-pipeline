import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px

st.set_page_config(page_title="DW TabNews Profissional", layout="wide")

st.title("📊 Dashboard Profissional - Data Warehouse TabNews")

# Conexão com o DW
@st.cache_data(ttl=300)
def carregar_dados():
    conn = psycopg2.connect(
        host="datalake",
        port=5432,
        database="datalake",
        user="datalake_user",
        password="datalake_pass"
    )

    fato = pd.read_sql("SELECT * FROM dw_tabnews.fato_news", conn)
    tempo = pd.read_sql("SELECT * FROM dw_tabnews.dim_tempo", conn)
    autor = pd.read_sql("SELECT * FROM dw_tabnews.dim_autor", conn)
    status = pd.read_sql("SELECT * FROM dw_tabnews.dim_status", conn)
    tipo = pd.read_sql("SELECT * FROM dw_tabnews.dim_tipo", conn)

    conn.close()
    return fato, tempo, autor, status, tipo

fato, tempo, autor, status, tipo = carregar_dados()

# JOINs entre dimensões e fato
df = fato \
    .merge(tempo.add_prefix("criacao_"), left_on="id_tempo_criacao", right_on="criacao_id_tempo", how="left") \
    .merge(tempo.add_prefix("pub_"), left_on="id_tempo_publicacao", right_on="pub_id_tempo", how="left") \
    .merge(tempo.add_prefix("att_"), left_on="id_tempo_atualizacao", right_on="att_id_tempo", how="left") \
    .merge(autor, on="id_autor", how="left") \
    .merge(status, on="id_status", how="left") \
    .merge(tipo, on="id_tipo", how="left")

# Filtros interativos
st.sidebar.header("Filtros")
status_opcao = st.sidebar.multiselect("Status:", options=df["status"].dropna().unique(), default=df["status"].dropna().unique())
tipo_opcao = st.sidebar.multiselect("Tipo:", options=df["tipo"].dropna().unique(), default=df["tipo"].dropna().unique())
autor_opcao = st.sidebar.multiselect("Autor:", options=df["owner_username"].dropna().unique(), default=df["owner_username"].dropna().unique())

# Aplicar filtros
df = df[(df["status"].isin(status_opcao)) & (df["tipo"].isin(tipo_opcao)) & (df["owner_username"].isin(autor_opcao))]

# Layout
col1, col2 = st.columns(2)

with col1:
    st.subheader("📅 Evolução das publicações (Criação)")
    df_criacao = df.groupby("criacao_data").size().reset_index(name="quantidade")
    fig = px.line(df_criacao, x="criacao_data", y="quantidade", title="Posts por Dia (Criação)")
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("📅 Evolução das publicações (Publicação)")
    df_pub = df.groupby("pub_data").size().reset_index(name="quantidade")
    fig = px.line(df_pub, x="pub_data", y="quantidade", title="Posts por Dia (Publicação)")
    st.plotly_chart(fig, use_container_width=True)

st.subheader("👤 Top 10 Autores")
df_autores = df.groupby("owner_username").size().reset_index(name="quantidade") \
               .sort_values(by="quantidade", ascending=False).head(10)
fig = px.bar(df_autores, x="owner_username", y="quantidade", title="Top Autores com Mais Posts")
st.plotly_chart(fig, use_container_width=True)

col3, col4 = st.columns(2)

with col3:
    st.subheader("📌 Distribuição por Status")
    df_status_count = df.groupby("status").size().reset_index(name="quantidade")
    fig = px.pie(df_status_count, names="status", values="quantidade", title="Distribuição por Status")
    st.plotly_chart(fig, use_container_width=True)

with col4:
    st.subheader("🏷️ Distribuição por Tipo")
    df_tipo_count = df.groupby("tipo").size().reset_index(name="quantidade")
    fig = px.bar(df_tipo_count, x="tipo", y="quantidade", title="Distribuição por Tipo")
    st.plotly_chart(fig, use_container_width=True)

# Tabela final com todas dimensões
with st.expander("🔍 Ver tabela de fatos enriquecida com dimensões"):
    st.dataframe(df)
