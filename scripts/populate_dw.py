import pandas as pd
from sqlalchemy import create_engine
import calendar
import os

def populate_dw():
    try:
        print(">> Conectando ao PostgreSQL...")
        pg_user = os.getenv("PG_USER", "datalake_user")
        pg_pass = os.getenv("PG_PASS", "datalake_pass")
        pg_host = os.getenv("PG_HOST", "datalake")
        pg_port = os.getenv("PG_PORT", "5432")
        pg_db   = os.getenv("PG_DB", "datalake")

        engine = create_engine(f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}")
        conn = engine.connect()

        print(">> Lendo staging_tabnews.staging_news...")
        df = pd.read_sql("SELECT * FROM staging_tabnews.staging_news", conn)

        print(">> Extraindo dimensões de tempo...")
        df["created_at"] = pd.to_datetime(df["created_at"])
        df["updated_at"] = pd.to_datetime(df["updated_at"])
        df["published_at"] = pd.to_datetime(df["published_at"])

        def extract_temporal_info(timestamp):
            return {
                "data": timestamp.date(),
                "ano": timestamp.year,
                "mes": timestamp.month,
                "nome_mes": calendar.month_name[timestamp.month],
                "dia": timestamp.day,
                "dia_semana": timestamp.strftime("%A")
            }

        datas = pd.concat([
            pd.DataFrame([extract_temporal_info(d) for d in df["created_at"]]),
            pd.DataFrame([extract_temporal_info(d) for d in df["updated_at"]]),
            pd.DataFrame([extract_temporal_info(d) for d in df["published_at"]])
        ]).drop_duplicates(subset=["data"])

        print(">> Removendo datas já existentes em dim_tempo...")
        datas_existentes = pd.read_sql("SELECT data FROM dw_tabnews.dim_tempo", conn)
        datas = datas[~datas["data"].isin(datas_existentes["data"])]

        if not datas.empty:
            print(f">> Inserindo {len(datas)} novas datas em dim_tempo...")
            datas.to_sql("dim_tempo", engine, schema="dw_tabnews", if_exists="append", index=False, method="multi")
        else:
            print("⚠️ Nenhuma nova data a ser inserida em dim_tempo.")

        tempo_ids = pd.read_sql("SELECT id_tempo, data FROM dw_tabnews.dim_tempo", conn)
        df["id_tempo_criacao"] = df["created_at"].dt.date.map(tempo_ids.set_index("data")["id_tempo"])
        df["id_tempo_publicacao"] = df["published_at"].dt.date.map(tempo_ids.set_index("data")["id_tempo"])
        df["id_tempo_atualizacao"] = df["updated_at"].dt.date.map(tempo_ids.set_index("data")["id_tempo"])

        # Dimensão Autor
        dim_autor = df[["owner_id", "owner_username"]].dropna().drop_duplicates()
        print(">> Removendo autores já existentes em dim_autor...")
        autores_existentes = pd.read_sql("SELECT owner_id FROM dw_tabnews.dim_autor", conn)
        dim_autor["owner_id"] = dim_autor["owner_id"].astype(str)
        autores_existentes["owner_id"] = autores_existentes["owner_id"].astype(str)
        dim_autor = dim_autor[~dim_autor["owner_id"].isin(autores_existentes["owner_id"])]

        if not dim_autor.empty:
            print(f">> Inserindo {len(dim_autor)} autores em dim_autor...")
            dim_autor.to_sql("dim_autor", engine, schema="dw_tabnews", if_exists="append", index=False, method="multi")
        else:
            print("⚠️ Nenhum novo autor a ser inserido.")

        autor_ids = pd.read_sql("SELECT id_autor, owner_id FROM dw_tabnews.dim_autor", conn)
        df["owner_id"] = df["owner_id"].astype(str)
        autor_ids["owner_id"] = autor_ids["owner_id"].astype(str)
        autor_ids = autor_ids.drop_duplicates(subset=["owner_id"]).set_index("owner_id")
        df["id_autor"] = df["owner_id"].map(autor_ids["id_autor"])

        # Dimensão Status
        dim_status = df[["status"]].dropna().drop_duplicates()
        status_existentes = pd.read_sql("SELECT status FROM dw_tabnews.dim_status", conn)
        dim_status = dim_status[~dim_status["status"].isin(status_existentes["status"])]

        if not dim_status.empty:
            dim_status.to_sql("dim_status", engine, schema="dw_tabnews", if_exists="append", index=False)

        status_ids = pd.read_sql("SELECT id_status, status FROM dw_tabnews.dim_status", conn)
        df["id_status"] = df["status"].map(status_ids.set_index("status")["id_status"])

        # Dimensão Tipo
        dim_tipo = df[["type"]].dropna().drop_duplicates().rename(columns={"type": "tipo"})
        tipos_existentes = pd.read_sql("SELECT tipo FROM dw_tabnews.dim_tipo", conn)
        dim_tipo = dim_tipo[~dim_tipo["tipo"].isin(tipos_existentes["tipo"])]

        if not dim_tipo.empty:
            dim_tipo.to_sql("dim_tipo", engine, schema="dw_tabnews", if_exists="append", index=False)

        tipo_ids = pd.read_sql("SELECT id_tipo, tipo FROM dw_tabnews.dim_tipo", conn)
        df["id_tipo"] = df["type"].map(tipo_ids.set_index("tipo")["id_tipo"])

        # FATOS
        fato = df[[
            "id", "id_autor", "id_tempo_criacao", "id_tempo_publicacao",
            "id_tempo_atualizacao", "id_status", "id_tipo", "slug", "title",
            "source_url", "tabcoins", "tabcoins_credit", "tabcoins_debit",
            "children_deep_count", "_ingested_at"
        ]].copy()

        fato = fato.rename(columns={
            "title": "titulo",
            "children_deep_count": "children_count"
        })

        print(">> Verificando duplicatas na fato_news...")
        fato["id"] = fato["id"].astype(str)
        ids_existentes = pd.read_sql("SELECT id FROM dw_tabnews.fato_news", conn)
        ids_existentes["id"] = ids_existentes["id"].astype(str)
        fato = fato.drop_duplicates(subset=["id"])
        fato = fato[~fato["id"].isin(ids_existentes["id"])]

        if not fato.empty:
            print(f">> Inserindo {len(fato)} registros na fato_news...")
            fato.to_sql("fato_news", engine, schema="dw_tabnews", if_exists="append", index=False, method="multi")
        else:
            print("⚠️ Nenhum novo registro a ser inserido na fato_news.")

        print("✅ DW populado com sucesso!")

    except Exception as e:
        print("❌ Erro ao popular DW:")
        import traceback
        traceback.print_exc()
