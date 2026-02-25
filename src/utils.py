"""
utils.py - Helpers reutilizaveis para as DAGs do Global Store Pipeline.
"""
from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


def save_dataframe_to_table(engine: Engine, df: pd.DataFrame, table_name: str, truncate: bool = True) -> None:
    """
    Persiste um DataFrame em uma tabela PostgreSQL via SQLAlchemy Core (bulk insert).

    Args:
        engine: Engine do SQLAlchemy ja conectado.
        df: DataFrame a ser persistido.
        table_name: Nome da tabela de destino.
        truncate: Se True, trunca a tabela antes da insercao (idempotencia).

    Raises:
        Exception: propaga qualquer erro SQL para o Airflow capturar a falha.
    """
    if df is None or df.empty:
        raise ValueError(f"DataFrame vazio - nada para persistir na tabela '{table_name}'.")

    registros = df.to_dict(orient="records")
    colunas = ", ".join(registros[0].keys())
    placeholders = ", ".join(f":{col}" for col in registros[0].keys())
    insert_query = text(f"INSERT INTO {table_name} ({colunas}) VALUES ({placeholders})")

    with engine.begin() as conn:
        if truncate:
            conn.execute(text(f"TRUNCATE TABLE {table_name};"))
        conn.execute(insert_query, registros)

    print(f"[OK] {len(df)} registros persistidos em '{table_name}'.")
