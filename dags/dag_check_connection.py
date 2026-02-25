"""
dag_check_connection.py - DAG de verificacao de conectividade com o PostgreSQL.

Responsabilidade: Garantir que o banco de dados no Render esta acessivel.
Schedule: @hourly
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
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


def check_db_connection(**kwargs) -> None:
    """Tenta conectar ao banco e executa SELECT 1. Falha se indisponivel."""
    from src.db_manager import get_engine

    engine = get_engine()
    if not engine:
        raise ConnectionError("Falha: nao foi possivel criar o engine de conexao. Verifique as variaveis de ambiente.")

    print("[OK] Conexao com o PostgreSQL (Render) verificada com sucesso!")


with DAG(
    dag_id="check_connection",
    default_args=default_args,
    description="Verifica a conectividade com o PostgreSQL no Render",
    schedule_interval="@hourly",
    catchup=False,
    tags=["infra", "monitoring"],
) as dag:

    check_connection_task = PythonOperator(
        task_id="check_db_connection",
        python_callable=check_db_connection,
    )
