import os
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

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

if __name__ == "__main__":
    get_engine()