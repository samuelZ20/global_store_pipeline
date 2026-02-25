"""
dag_load.py - DAG de Carga/Exposicao (Load).

Responsabilidade: Validar e expor os dados da camada Silver.
Extensivel para: exportar para S3, enviar para um BI, notificar via e-mail/Slack, etc.

Schedule: @daily (executar apos dag_silver_transform)
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


def validate_and_expose(**kwargs) -> None:
    """
    Valida os dados finais em silver_products e registra o resultado.
    Falha se a tabela estiver vazia (sinaliza problema no pipeline upstream).
    """
    from sqlalchemy import text
    from src.db_manager import get_engine

    engine = get_engine()
    if not engine:
        raise ConnectionError("Nao foi possivel conectar ao banco para validacao.")

    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) AS total FROM silver_products;"))
        total = result.scalar()

    if not total or total == 0:
        raise ValueError("silver_products esta vazia. Verifique as DAGs upstream (bronze/silver).")

    print(f"[OK] Load concluido com sucesso! {total} produtos disponiveis em silver_products.")
    print("[INFO] Dados prontos para consumo por ferramentas de BI ou APIs externas.")


with DAG(
    dag_id="load_validate",
    default_args=default_args,
    description="Validacao e exposicao dos dados finais de silver_products",
    schedule_interval="@daily",
    catchup=False,
    tags=["load", "validate", "etl"],
) as dag:

    load_task = PythonOperator(
        task_id="validate_and_expose",
        python_callable=validate_and_expose,
    )
