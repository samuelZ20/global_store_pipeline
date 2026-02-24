# 🛒 Global Store ETL Pipeline

Este projeto consiste em um **pipeline de dados automatizado** que extrai informações de produtos da **FakeStoreAPI**, realiza transformações de limpeza e padronização utilizando **Pandas**, e persiste os dados em um **Data Warehouse PostgreSQL hospedado no Render**.

O pipeline é orquestrado pelo **Apache Airflow**, garantindo **idempotência** e **observabilidade** do processo.

---

## 🚀 Tecnologias Utilizadas

* **Linguagem:** Python 3.12
* **Orquestração:** Apache Airflow 2.11+
* **Transformação:** Pandas
* **Banco de Dados:** PostgreSQL (Render)
* **Conexão e Carga:** SQLAlchemy Core 1.4 (Bulk Insert)
* **Gerenciador de Dependências:** Poetry

---

## 📂 Estrutura do Projeto

```
global_store_pipeline/
├── dags/
│   └── global_store_dag.py     # Definição do fluxo de tarefas no Airflow
├── src/                        # Módulos de lógica do pipeline
│   ├── api_client.py           # Extração (Camada Bronze)
│   ├── transform.py            # Transformação (Camada Silver)
│   ├── db_manager.py           # Gerenciamento de conexão com banco
│   └── init_db.py              # DDL e inicialização de tabelas
├── main.py                     # Execução manual (Local)
├── pyproject.toml              # Dependências Poetry
└── .env                        # Variáveis de ambiente (não versionado)
```

---

## 🛠️ Configuração do Ambiente

### 1️⃣ Pré-requisitos

Certifique-se de ter:

* **Python 3.12**
* **Poetry**
* Ambiente Linux/WSL (recomendado para compatibilidade com o Airflow)

---

### 2️⃣ Instalação de Dependências

```bash
poetry install
```

---

### 3️⃣ Variáveis de Ambiente

Crie um arquivo `.env` na raiz do projeto com as credenciais do banco de dados no Render:

```env
DB_USER=seu_usuario
DB_PASSWORD=sua_senha
DB_HOST=seu_host_no_render.com
DB_NAME=global_store_dw
```

---

## 🏃 Como Rodar

### 🔹 Modo Local (Script Rápido)

Para validar a conexão e a lógica ETL sem a interface do Airflow:

```bash
poetry run python main.py
```

---

### 🔹 Modo Orquestrado (Airflow Standalone)

Para rodar com **agendamento e monitoramento visual**:

#### 1. Configuração de Caminhos

No terminal, informe ao Python a localização dos módulos:

```bash
export PYTHONPATH=$PYTHONPATH:$(pwd)
```

#### 2. Inicie o Airflow

```bash
poetry run airflow standalone
```

#### 3. Acesso

Abra o navegador em:

```
http://localhost:8080
```

Localize a DAG **`global_store_multi_task_pipeline`** e ative-a.

---

## 🧠 Decisões Técnicas de Engenharia

### ✅ Idempotência

O processo de carga utiliza `TRUNCATE` dentro de uma transação `engine.begin()`, garantindo que o pipeline possa ser reexecutado sem:

* duplicar dados
* deixar o banco em estado inconsistente

---

### ✅ Carga Robusta

Devido a incompatibilidades entre **Pandas** e **SQLAlchemy** em ambientes virtuais específicos, a carga final é realizada via **SQLAlchemy Core (Bulk Insert)**, contornando o erro:

```
AttributeError: Engine object has no attribute cursor
```

---

### ✅ Modularidade

A lógica é separada em camadas:

* **Bronze:** Extração da API
* **Silver:** Limpeza e padronização dos dados

Essa arquitetura facilita manutenção, testes e expansão futura para novas fontes de dados.

---

## 🎓 Autor

**Samuel Frizzone Cardoso**
