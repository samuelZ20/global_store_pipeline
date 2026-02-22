import os
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from sqlalchemy import text

# Carrega as credenciais do .env
load_dotenv()

def get_engine():
    """Cria a conexão com o banco de dados no Render usando as variáveis do .env"""
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")
    
    # URL de conexão para PostgreSQL
    connection_url = f"postgresql://{user}:{password}@{host}/{db_name}"
    
    try:
        engine = create_engine(connection_url)
        # Tenta uma conexão simples para validar
        with engine.connect() as conn:
            print("✅ Conexão com o Render estabelecida com sucesso!")
        return engine
    except SQLAlchemyError as e:
        print(f"❌ Erro ao conectar ao banco de dados: {e}")
        return None


def create_tables(engine):
    """Cria a tabela explicitamente se ela não existir."""
    query = """
    CREATE TABLE IF NOT EXISTS silver_products (
        id INT PRIMARY KEY,
        title TEXT,
        price FLOAT,
        category TEXT,
        image TEXT,
        rating_rate FLOAT,
        rating_count INT,
        extraction_timestamp TIMESTAMP
    );
    """
    with engine.connect() as conn:
        conn.execute(text(query))
        conn.commit()
if __name__ == "__main__":
    get_engine()