from sqlalchemy import create_engine, text

def create_dw():
    try:
        print(">> Conectando ao PostgreSQL...")
        pg_user = "datalake_user"
        pg_pass = "datalake_pass"
        pg_host = "datalake"
        pg_port = "5432"
        pg_db   = "datalake"

        engine = create_engine(f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}")

        with engine.connect() as conn:
            print(">> Criando schema 'dw_tabnews'...")
            conn.execute(text("""
                CREATE SCHEMA IF NOT EXISTS dw_tabnews;
            """))

            print(">> Criando tabelas dimensionais e fato...")

            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dw_tabnews.dim_tempo (
                    id_tempo SERIAL PRIMARY KEY,
                    data DATE UNIQUE,
                    ano INT,
                    mes INT,
                    nome_mes VARCHAR(20),
                    dia INT,
                    dia_semana VARCHAR(20)
                );
            """))

            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dw_tabnews.dim_autor (
                    id_autor SERIAL PRIMARY KEY,
                    owner_id UUID UNIQUE,
                    owner_username TEXT
                );
            """))

            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dw_tabnews.dim_status (
                    id_status SERIAL PRIMARY KEY,
                    status TEXT UNIQUE
                );
            """))

            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dw_tabnews.dim_tipo (
                    id_tipo SERIAL PRIMARY KEY,
                    tipo TEXT UNIQUE
                );
            """))

            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dw_tabnews.fato_news (
                    id UUID PRIMARY KEY,
                    id_autor INT,
                    id_tempo_criacao INT,
                    id_tempo_publicacao INT,
                    id_status INT,
                    id_tipo INT,
                    slug TEXT,
                    titulo TEXT,
                    tabcoins INT,
                    tabcoins_credit INT,
                    tabcoins_debit INT,
                    children_count INT,
                    source_url TEXT,
                    _ingested_at TIMESTAMP,

                    FOREIGN KEY (id_autor) REFERENCES dw_tabnews.dim_autor(id_autor),
                    FOREIGN KEY (id_tempo_criacao) REFERENCES dw_tabnews.dim_tempo(id_tempo),
                    FOREIGN KEY (id_tempo_publicacao) REFERENCES dw_tabnews.dim_tempo(id_tempo),
                    FOREIGN KEY (id_status) REFERENCES dw_tabnews.dim_status(id_status),
                    FOREIGN KEY (id_tipo) REFERENCES dw_tabnews.dim_tipo(id_tipo)
                );
            """))

        print("✅ Todas as tabelas foram criadas com sucesso no schema 'dw_tabnews'.")

    except Exception as e:
        print("❌ Erro ao criar as tabelas do DW:")
        print(e)
