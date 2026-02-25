"""
dag_silver_transform.py - DAG de Transformacao (Camada Silver).

Responsabilidade: Ler os dados brutos de `bronze_products`, aplicar as
transformacoes com Pandas (flattening, tipagem, limpeza) e persistir
o resultado limpo em `silver_products`.

Schedule: @daily (executar apos dag_bronze_extract)
"""
from __future__ import annotations

import sys
import os

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


def transform_to_silver(**kwargs) -> None:
    """
    Le bronze_products, aplica transform_products() e salva em silver_products.
    """
    import json
    import pandas as pd
    from sqlalchemy import text
    from src.db_manager import get_engine
    from src.transform import transform_products
    from src.utils import save_dataframe_to_table

    engine = get_engine()
    if not engine:
        raise ConnectionError("Nao foi possivel conectar ao banco para leitura da Bronze.")

    # 1. Le os dados brutos da tabela Bronze
    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM bronze_products;"))
        rows = [dict(row) for row in result]

    if not rows:
        raise ValueError("Tabela bronze_products esta vazia. Execute a DAG bronze_extract primeiro.")

    print(f"[OK] {len(rows)} registros lidos de bronze_products.")

    # 2. Desserializa o campo 'rating' de volta para dict (veio como string JSONB)
    for row in rows:
        if isinstance(row.get("rating"), str):
            row["rating"] = json.loads(row["rating"])

    # 3. Aplica as transformacoes (Bronze -> Silver)
    df_silver = transform_products(rows)

    if df_silver is None or df_silver.empty:
        raise ValueError("Transformacao resultou em DataFrame vazio.")

    print(f"[OK] {len(df_silver)} produtos transformados para a Camada Silver.")

    # 4. Persiste em silver_products
    save_dataframe_to_table(engine, df_silver, "silver_products", truncate=True)
    print("[OK] Camada Silver carregada com sucesso!")


with DAG(
    dag_id="silver_transform",
    default_args=default_args,
    description="Transformacao bronze_products -> silver_products",
    schedule_interval="@daily",
    catchup=False,
    tags=["silver", "transform", "etl"],
) as dag:

    transform_task = PythonOperator(
        task_id="transform_to_silver",
        python_callable=transform_to_silver,
    )
