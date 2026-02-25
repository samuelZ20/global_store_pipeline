"""
dag_setup_db.py - DAG de Inicializacao do Banco de Dados.

Responsabilidade: Garantir que todas as tabelas do pipeline existam no banco
ANTES de qualquer DAG de carga. Usa CREATE TABLE IF NOT EXISTS, entao e seguro
rodar multiplas vezes - nunca recria nem apaga tabelas ja existentes.

Schedule: @once (ou triggered manualmente antes do primeiro run do pipeline)
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
    "retry_delay": timedelta(minutes=2),
}


def setup_database(**kwargs) -> None:
    """
    Cria as tabelas bronze_products e silver_products caso nao existam.
    Idempotente: CREATE TABLE IF NOT EXISTS garante que execucoes repetidas
    nao causam erros nem perda de dados.
    """
    from sqlalchemy import inspect
    from src.db_manager import get_engine
    from src.init_db import create_tables

    engine = get_engine()
    if not engine:
        raise ConnectionError("Nao foi possivel conectar ao banco para o setup.")

    # Verifica quais tabelas ja existem antes de criar
    inspector = inspect(engine)
    tabelas_existentes = inspector.get_table_names()

    tabelas_esperadas = ["bronze_products", "silver_products"]
    for tabela in tabelas_esperadas:
        if tabela in tabelas_existentes:
            print(f"[INFO] Tabela '{tabela}' ja existe - nenhuma acao necessaria.")
        else:
            print(f"[INFO] Tabela '{tabela}' nao encontrada - sera criada.")

    # Executa o DDL (CREATE TABLE IF NOT EXISTS e idempotente)
    create_tables()

    # Confirmacao final
    inspector = inspect(engine)
    tabelas_apos = inspector.get_table_names()
    for tabela in tabelas_esperadas:
        status = "OK" if tabela in tabelas_apos else "FAIL"
        print(f"[{status}] Tabela '{tabela}' - {'disponivel' if tabela in tabelas_apos else 'FALHOU'}")


with DAG(
    dag_id="setup_database",
    default_args=default_args,
    description="Inicializa as tabelas do pipeline (idempotente - CREATE IF NOT EXISTS)",
    schedule_interval="@once",
    catchup=False,
    tags=["infra", "setup"],
) as dag:

    setup_task = PythonOperator(
        task_id="create_tables_if_not_exist",
        python_callable=setup_database,
    )
