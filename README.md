# Unifor DataOps Pipeline

Este projeto implementa um pipeline de dados completo utilizando **Apache Airflow**, **MongoDB**, **PostgreSQL** e **Streamlit**, com o objetivo de realizar a ingestão, transformação, armazenamento e visualização de dados da API pública do [TabNews](https://www.tabnews.com.br).

![image](https://github.com/user-attachments/assets/91e7bad0-55ed-4de5-8106-48714d0c2f5d)

---

## 🛠️ Tecnologias Utilizadas

- [Apache Airflow](https://airflow.apache.org/) — Orquestração de pipelines
- [MongoDB Atlas](https://www.mongodb.com/atlas/database) — Armazenamento de dados brutos
- [PostgreSQL](https://www.postgresql.org/) — Armazenamento analítico (Data Warehouse)
- [Docker & Docker Compose](https://docs.docker.com/) — Infraestrutura do projeto
- [Streamlit](https://streamlit.io/) — Dashboard para visualização dos dados

---



## 📦 Estrutura do Projeto

```bash
unifor-dataops-pipeline/
├── dags/                   # DAGs do Airflow
├── docker-compose.yml      # Orquestração dos serviços
├── init/                   # Scripts de inicialização do banco
├── logs/                   # Logs do Airflow
├── plugins/                # Plugins personalizados do Airflow
├── scripts/                # Scripts de ingestão e transformação de dados
├── streamlit/              # Aplicação Streamlit para visualização dos dados
└── config/                 # Arquivos de configuração e .env
```

---

## 🚀 Como Executar o Projeto

> Pré-requisitos: Docker e Docker Compose instalados

1. Clone o repositório:

```bash
git clone https://github.com/felipesoaresdev/unifor-dataops-pipeline.git
cd unifor-dataops-pipeline
```

2. Crie um arquivo `.env` com as variáveis necessárias (exemplo em `config/.env.example`).

3. Suba os containers com Docker Compose:

```bash
docker compose up -d
```

4. Acesse os serviços:

* Airflow: [http://localhost:8081](http://localhost:8081)
* Streamlit Dashboard: [http://localhost:8501](http://localhost:8501)

---

## 📌 Funcionalidades

* Ingestão de dados da API pública do TabNews via Python
* Armazenamento dos dados brutos no MongoDB
* Processamento e transformação com Apache Spark
* Gravação dos dados transformados em modelo estrela no PostgreSQL
* Dashboard interativo com métricas e gráficos via Streamlit

---

## 🔐 Segurança

> ⚠️ Este projeto é apenas para fins educacionais e de demonstração. **Não utilize as credenciais padrão ou deixe-as no código-fonte em ambientes de produção.**

Recomendações:

* Use variáveis de ambiente para credenciais
* Nunca exponha senhas ou chaves diretamente no código
* Altere o usuário/senha padrão do Airflow (`admin/admin`) antes de expor publicamente

---

## 👨‍💻 Autores

Desenvolvido por:

[Felipe Soares](https://github.com/felipesoaresdev) — Analista de Sistemas | Engenheiro de Dados.

[Winiston Freitas](https://github.com/winistonvf) — Analista de Sistemas | Engenheiro de Dados.

---

## 📄 Licença

Este projeto está licenciado sob a [MIT License](LICENSE).

