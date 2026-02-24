# 🛒 Global Store ETL Pipeline

Este projeto é um **pipeline de dados automatizado** que extrai informações de produtos da **FakeStoreAPI**, processa os dados com **Pandas** e os armazena em um **Data Warehouse PostgreSQL (Render)**.
A orquestração é realizada via **Apache Airflow**, garantindo monitoramento e reexecução segura (**idempotência**).

---

## 👤 Autor

**Samuel Frizzone Cardoso**

---

## 🚀 Tecnologias

* **Linguagem:** Python 3.12
* **Orquestração:** Apache Airflow 2.11+
* **Transformação:** Pandas
* **Banco de Dados:** PostgreSQL (Render)
* **Gerenciador de Dependências:** Poetry

---

## 📂 Estrutura do Projeto

```plaintext
global_store_pipeline/
├── dags/
│   └── global_store_dag.py     # Orquestração do fluxo de tarefas
├── src/                        # Núcleo da lógica (Modules)
│   ├── api_client.py           # Extração (Bronze)
│   ├── transform.py            # Transformação (Silver)
│   ├── db_manager.py           # Conexão com o Banco
│   └── init_db.py              # DDL e Inicialização
├── main.py                     # Execução Manual/Debug
├── pyproject.toml              # Configurações do Poetry
└── .env                        # Variáveis Sensíveis (Não versionado)
```

---

## 🛠️ Configuração e Instalação

### 1️⃣ Clonar o Repositório

Abra o seu terminal (preferencialmente WSL/Ubuntu) e baixe o projeto:

```bash
git clone https://github.com/samuelZ20/global_store_pipeline.git
cd global_store_pipeline
```

---

### 2️⃣ Instalar Dependências

Utilize o Poetry para criar o ambiente virtual e instalar as bibliotecas:

```bash
poetry install
```

---

### 3️⃣ Configurar Variáveis de Ambiente

Crie um arquivo `.env` na raiz do projeto com as credenciais do seu banco no Render:

```env
DB_USER=seu_usuario
DB_PASSWORD=sua_senha
DB_HOST=seu_host_no_render.com
DB_NAME=global_store_dw
```

---

## 🏃 Como Executar

### 🔹 Modo Local (Teste Rápido)

Valida a lógica ETL e a persistência no banco **sem a interface do Airflow**:

```bash
poetry run python main.py
```

---

### 🔹 Modo Orquestrado (Airflow Standalone)

Para rodar com **agendamento e monitoramento visual**:

#### 1. Vincular DAGs e Módulos

Configure o Airflow para reconhecer a pasta do projeto:

```bash
mkdir -p ~/airflow/dags
ln -s $(pwd)/dags/* ~/airflow/dags/
export PYTHONPATH=$PYTHONPATH:$(pwd)
```

#### 2. Iniciar Airflow

```bash
poetry run airflow standalone
```

#### 3. Acesso

Abra no navegador:

```
http://localhost:8080
```

Faça login com as credenciais geradas no terminal e ative a DAG **global_store_multi_task_pipeline**.

---

## 🧠 Decisões Técnicas

### ✅ Carga Robusta

A persistência utiliza **SQLAlchemy Core (Bulk Insert)** para evitar incompatibilidades de drivers entre o Pandas e o ambiente local.

### ✅ Idempotência

O uso de `TRUNCATE` em transações atômicas (`engine.begin()`) garante que falhas no meio do processo não deixem dados duplicados ou inconsistentes no Data Warehouse.

---

## ✅ Objetivo

Demonstrar a construção de um pipeline ETL moderno com:

* Orquestração profissional
* Separação em camadas (Bronze → Silver)
* Integração com Data Warehouse na nuvem
* Boas práticas de engenharia de dados
