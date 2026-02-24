from sqlalchemy import text
from src.db_manager import get_engine

def create_tables():
    """Garante a infraestrutura necessária no banco de dados antes da carga."""
    engine = get_engine()
    
    if not engine:
        print("❌ Erro de Conexão: O motor (engine) não foi inicializado.")
        return

    # Usamos SERIAL para o ID se quisermos que o Postgres gerencie a contagem automática, 
    # ou INTEGER se o ID vier fixo da FakeStoreAPI.
    ddl_query = """
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
    
    try:
        # 'begin()' abre uma transação. Se a query falhar, ele faz o rollback. 
        # Se funcionar, faz o commit automático ao final do bloco 'with'.
        # Perfeito para SQLAlchemy 1.4 usado no Airflow.
        with engine.begin() as conn:
            conn.execute(text(ddl_query))
        
        print("✅ Infraestrutura: Tabela 'silver_products' verificada/criada com sucesso!")
        
    except Exception as e:
        # Em ambiente sênior, logamos o erro específico para facilitar o debug
        print(f"❌ Falha ao configurar DDL: {e}")
        raise e # Re-lançamos o erro para que o Airflow saiba que a tarefa falhou

if __name__ == "__main__":
    print("⏳ Iniciando configuração de ambiente de banco de dados...")
    create_tables()