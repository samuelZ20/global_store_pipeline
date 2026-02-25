from sqlalchemy import text
from src.db_manager import get_engine


# --- DDL: Camada Bronze ---
DDL_BRONZE_PRODUCTS = """
CREATE TABLE IF NOT EXISTS bronze_products (
    id INTEGER,
    title TEXT,
    price FLOAT,
    category TEXT,
    image TEXT,
    description TEXT,
    rating JSONB,
    extraction_timestamp TIMESTAMP
);
"""

# --- DDL: Camada Silver ---
DDL_SILVER_PRODUCTS = """
CREATE TABLE IF NOT EXISTS silver_products (
    id INTEGER PRIMARY KEY,
    title VARCHAR(255),
    price FLOAT,
    category VARCHAR(100),
    image TEXT,
    rating_rate FLOAT,
    rating_count INTEGER,
    extraction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


def create_tables() -> None:
    """
    Garante que todas as tabelas do pipeline existam no banco de dados.
    Executar antes de qualquer DAG de carga.
    """
    engine = get_engine()

    if not engine:
        print("[ERROR] Erro de Conexao: O motor (engine) nao foi inicializado.")
        return

    try:
        with engine.begin() as conn:
            conn.execute(text(DDL_BRONZE_PRODUCTS))
            print("[OK] Tabela 'bronze_products' verificada/criada.")

            conn.execute(text(DDL_SILVER_PRODUCTS))
            print("[OK] Tabela 'silver_products' verificada/criada.")

    except Exception as e:
        print(f"[ERROR] Falha ao configurar DDL: {e}")
        raise e


if __name__ == "__main__":
    print("[INFO] Iniciando configuracao do banco de dados...")
    create_tables()
