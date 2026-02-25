"""
dag_bronze_extract.py - DAG de Extracao (Camada Bronze).

Responsabilidade: Conectar na FakeStoreAPI, extrair os dados brutos de produtos
e persisti-los na tabela `bronze_products` no PostgreSQL.

Schedule: @daily
"""
from __future__ import annotations

import sys
import os
import json

PROJECT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_PATH not in sys.path:
    sys.path.insert(0, PROJECT_PATH)

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "Samuel Frizzone",
    "depends_on_past": False,
    "start_date": datetime(2026, 2, 25),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def extract_to_bronze(**kwargs) -> None:
    """
    Extrai dados brutos da FakeStoreAPI e salva na tabela bronze_products.
    A coluna 'rating' e armazenada como JSONB para preservar o dado original.
    """
    import pandas as pd
    from src.api_client import extrair_dados
    from src.db_manager import get_engine
    from src.utils import save_dataframe_to_table

    # 1. Extracao
    dados_brutos = extrair_dados("products")
    if not dados_brutos:
        raise ValueError("Extracao falhou: API nao retornou dados.")

    print(f"[OK] {len(dados_brutos)} produtos extraidos da FakeStoreAPI.")

    # 2. Serializa o campo 'rating' (dict) como string JSON para o JSONB do Postgres
    for item in dados_brutos:
        if isinstance(item.get("rating"), dict):
            item["rating"] = json.dumps(item["rating"])

    df_bronze = pd.DataFrame(dados_brutos)

    # 3. Persiste na camada Bronze
    engine = get_engine()
    if not engine:
        raise ConnectionError("Nao foi possivel conectar ao banco para a carga Bronze.")

    save_dataframe_to_table(engine, df_bronze, "bronze_products", truncate=True)
    print("[OK] Camada Bronze carregada com sucesso!")


with DAG(
    dag_id="bronze_extract",
    default_args=default_args,
    description="Extracao de produtos da FakeStoreAPI -> bronze_products",
    schedule_interval="@daily",
    catchup=False,
    tags=["bronze", "extract", "etl"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_to_bronze",
        python_callable=extract_to_bronze,
    )
