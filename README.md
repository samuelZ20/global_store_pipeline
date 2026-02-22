# 🛒 Global Store Data Pipeline (ETL)

Este projeto consiste em um pipeline de dados automatizado para a **Global Store**, integrando extração de APIs, transformação de dados com Pandas e carga em um Data Warehouse na nuvem. Desenvolvido como parte dos meus estudos em Ciência da Computação na **UFLA**.

## 🏗️ Arquitetura (Medallion Architecture)

O fluxo de dados segue os princípios de camadas de dados:
1. **Bronze (Raw)**: Extração direta da FakeStoreAPI com metadados de auditoria (`api_client.py`).
2. **Silver (Clean)**: Processamento e normalização (flattening) do campo `rating` utilizando Pandas (`transform.py`).
3. **Load**: Persistência dos dados estruturados em um banco PostgreSQL hospedado no **Render**.


## 🛠️ Tecnologias Utilizadas

- **Linguagem**: Python 3.12
- **Gestão de Dependências**: Poetry
- **Processamento**: Pandas
- **Conectividade**: SQLAlchemy / Psycopg2
- **Orquestração**: Apache Airflow (DAGs Modulares)
- **Banco de Dados**: PostgreSQL (Render)
- **Visualização**: DBeaver

## 🕸️ Orquestração (Airflow)

O pipeline é orquestrado por uma **DAG** modularizada em três tarefas principais:
- `extract_from_api`
- `transform_with_pandas`
- `load_to_render`

Agendamento definido para execução diária (`@daily`) com sistema de retentativas automáticas.

## 🚀 Como Executar

1. Clone o repositório: `git clone https://github.com/samuelZ20/global_store_pipeline.git`
2. Instale as dependências: `poetry install`
3. Configure o arquivo `.env` com suas credenciais do banco.
4. Execute o pipeline: `poetry run python main.py`

---
**Samuel Frizzone Cardoso** Estudante de Ciência da Computação - UFLA
