from src.db_manager import get_engine
from sqlalchemy import text

def create_tables():
    engine = get_engine()
    
    if not engine:
        print("❌ Erro: Não foi possível conectar ao banco de dados no Render.")
        return

    # Comando DDL (SQL) para criar a tabela se ela não existir
    query = """
    CREATE TABLE IF NOT EXISTS silver_products (
        id INTEGER PRIMARY KEY,
        title VARCHAR(255),
        price FLOAT,
        category VARCHAR(100),
        image TEXT,
        rating_rate FLOAT,
        rating_count INTEGER,
        extraction_timestamp TIMESTAMP
    );
    """
    
    try:
        with engine.connect() as conn:
            conn.execute(text(query))
            conn.commit()
            print("✅ Sucesso: Tabela 'silver_products' verificada/criada no banco de dados!")
    except Exception as e:
        print(f"❌ Erro ao criar a tabela: {e}")

if __name__ == "__main__":
    print("⏳ Iniciando a configuração do banco de dados...")
    create_tables()