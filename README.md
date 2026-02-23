# 🛒 Global Store Data Pipeline (ETL)

Pipeline de dados automatizado (End-to-End) para extração, transformação e carga de produtos de e-commerce. Desenvolvido para processar dados da FakeStoreAPI e popular um Data Warehouse na nuvem utilizando a **Arquitetura de Medalhão**.

---

## 🚀 Tecnologias e Ferramentas

* **Linguagem:** Python 3.12
* **Manipulação e Tratamento:** Pandas
* **Banco de Dados Cloud:** PostgreSQL (Render)
* **Conexão / ORM:** SQLAlchemy
* **Orquestração de Dados:** Apache Airflow
* **Gestão de Dependências:** Poetry

---

## 🏗️ Arquitetura do Projeto

O pipeline foi modularizado em etapas claras para seguir as melhores práticas de Engenharia de Dados:

1. **Setup de Infraestrutura (`init_db.py`)**
   Garante a criação explícita da tabela `silver_products` no PostgreSQL com as tipagens corretas (DDL) antes de qualquer carga de dados.

2. **Camada Bronze (Extração - `api_client.py`)**
   Consumo de dados via API REST com adição automática de `extraction_timestamp` (auditoria) e tratamento de falhas de rede (timeouts).

3. **Camada Silver (Transformação - `transform.py`)**
   Limpeza de dados e *flattening* (achatamento) dinâmico de estruturas JSON aninhadas (`rating`) utilizando a alta performance do Pandas.

4. **Carga (`db_manager.py`)**
   Persistência dos dados estruturados no Data Warehouse utilizando práticas seguras de conexão via variáveis de ambiente.

5. **Orquestração (`dags/global_store_dag.py`)**
   Fluxo estruturado em uma DAG do Airflow, com isolamento de tarefas (**Setup → Extract → Transform → Load**) e comunicação de metadados via XCom.

---

## ⚙️ Como Executar Localmente (Standalone)

Para testar o fluxo de extração e carga no banco de dados localmente (sem a necessidade de subir os containers do Airflow), você pode utilizar o orquestrador embutido `main.py`.

### 📋 Pré-requisitos

* Python 3.12+
* Poetry instalado:

```bash
pip install poetry
```

---

### ▶️ Passo a Passo

#### **1. Clone o repositório**

```bash
git clone https://github.com/samuelZ20/global_store_pipeline.git
cd global_store_pipeline
```

#### **2. Instale as dependências com o Poetry**

```bash
poetry install
```

#### **3. Configure as Variáveis de Ambiente**

Crie um arquivo chamado `.env` na raiz do projeto e adicione as credenciais do seu banco PostgreSQL no Render:

```env
DB_USER=seu_usuario
DB_PASSWORD=sua_senha
DB_HOST=seu_host.render.com
DB_NAME=seu_banco
```

#### **4. Execute o Pipeline Completo**

O comando abaixo validará o banco de dados (criando a tabela se necessário) e fará o ciclo completo de ETL:

```bash
poetry run python main.py
```

---

## 🌬️ Execução via Apache Airflow

A lógica de orquestração distribuída encontra-se no diretório:

```
dags/global_store_dag.py
```

A DAG foi construída utilizando `PythonOperator` e está pronta para ser:

* Acoplada a qualquer ambiente Airflow
* Agendada (`@daily`)
* Executada em ambientes containerizados (Docker, Astro CLI, etc.)

---

## 👨‍💻 Autor

**Samuel Frizzone Cardoso**
Engenharia de Dados — UFLA
