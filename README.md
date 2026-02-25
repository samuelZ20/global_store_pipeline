# Global Store ETL Pipeline

Pipeline de dados automatizado que extrai produtos da **FakeStoreAPI**, transforma com **Pandas** e armazena em um **Data Warehouse PostgreSQL (Render)**. Orquestrado via **Apache Airflow** com uma DAG por camada.

---

## 👤 Autor

**Samuel Frizzone Cardoso**

---

## 🚀 Tecnologias

| Tech | Versão |
|---|---|
| Python | 3.12 |
| Apache Airflow | 2.9+ |
| Pandas | 2.0+ |
| SQLAlchemy | 1.4 |
| PostgreSQL | Render Cloud |
| Poetry | Gerenciador de deps |

---

## 🏗️ Arquitetura

```mermaid
flowchart TD
    A([FakeStoreAPI]) -->|GET /products| B

    subgraph Airflow ["Apache Airflow — Orquestração"]
        direction TB

        DAG0[dag_setup_db\n@once]:::infra
        DAG1[dag_check_connection\n@hourly]:::infra
        DAG2[dag_bronze_extract\n@daily]:::bronze
        DAG3[dag_silver_transform\n@daily]:::silver
        DAG4[dag_load_validate\n@daily]:::load

        DAG0 -.->|pré-requisito| DAG2
        DAG1 -.->|monitoramento| DAG2
    end

    B[dag_bronze_extract] --> C[(bronze_products\nJSON bruto)]
    C --> D[dag_silver_transform]
    D --> E[(silver_products\nDados limpos)]
    E --> F[dag_load_validate]
    F --> G([BI / APIs Externas])

    classDef infra fill:#6c757d,color:#fff,stroke:none
    classDef bronze fill:#cd7f32,color:#fff,stroke:none
    classDef silver fill:#aaa,color:#fff,stroke:none
    classDef load fill:#198754,color:#fff,stroke:none
```

---

## 📂 Estrutura do Projeto

```plaintext
global_store_pipeline/
├── dags/
│   ├── dag_setup_db.py          # [INFRA] Cria as tabelas (rodar primeiro, @once)
│   ├── dag_check_connection.py  # [INFRA] Verifica conectividade (@hourly)
│   ├── dag_bronze_extract.py    # [ETL]   API → bronze_products (@daily)
│   ├── dag_silver_transform.py  # [ETL]   bronze → silver_products (@daily)
│   └── dag_load.py              # [ETL]   Validação e exposição final (@daily)
├── src/
│   ├── api_client.py            # Extração da FakeStoreAPI
│   ├── transform.py             # Transformações Pandas (flattening, tipagem)
│   ├── db_manager.py            # Engine SQLAlchemy + conectividade
│   ├── init_db.py               # DDL das tabelas Bronze e Silver
│   └── utils.py                 # Helper genérico de persistência
├── bootstrap.sh                 # Setup do ambiente local
├── pyproject.toml
└── .env                         # Credenciais (não versionado)
```

---

## 🛠️ Instalação

### 1. Clonar o repositório

```bash
git clone https://github.com/samuelZ20/global_store_pipeline.git
cd global_store_pipeline
```

### 2. Instalar dependências

```bash
poetry install
```

### 3. Configurar o `.env`

```env
DB_USER=seu_usuario
DB_PASSWORD=sua_senha
DB_HOST=seu_host.oregon-postgres.render.com
DB_NAME=nome_do_banco
DB_PORT=5432
```

---

## 🏃 Como Executar

### 1. Bootstrap do ambiente

```bash
source bootstrap.sh
```

O script configura o `PYTHONPATH`, cria os symlinks das DAGs em `~/airflow/dags` e carrega o `.env` automaticamente.

### 2. Iniciar o Airflow

```bash
poetry run airflow standalone
```

Acesse `http://localhost:8080` com as credenciais exibidas no terminal (também salvas em `~/airflow/standalone_admin_password.txt`).

### 3. Ordem de execução das DAGs

| Ordem | DAG ID | Descrição |
|:---:|---|---|
| 1️⃣ | `setup_database` | Cria as tabelas — rodar **uma única vez** |
| 2️⃣ | `check_connection` | Valida conectividade com o banco |
| 3️⃣ | `bronze_extract` | Extrai produtos da FakeStoreAPI |
| 4️⃣ | `silver_transform` | Transforma e limpa os dados |
| 5️⃣ | `load_validate` | Valida e expõe os dados finais |

---

## 🧠 Decisões Técnicas

### Uma DAG por Responsabilidade

Cada camada do pipeline é uma DAG independente, permitindo monitoramento, reexecução e extensão isolados. Os dados são **persistidos no banco entre as camadas** (sem XCom cross-DAG):

| Tabela | Camada | Conteúdo |
|---|---|---|
| `bronze_products` | Bronze | Dados brutos da API (`rating` como JSONB) |
| `silver_products` | Silver | Dados transformados e tipados |

### Carga Robusta

Usa **SQLAlchemy Core (Bulk Insert)** via `save_dataframe_to_table()` em `utils.py`, evitando incompatibilidades entre Pandas `to_sql` e drivers PostgreSQL.

### Idempotência

`TRUNCATE` em transação atômica (`engine.begin()`) garante que reexecuções não geram dados duplicados. O setup usa `CREATE TABLE IF NOT EXISTS`.
