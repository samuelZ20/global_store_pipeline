import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional

# Carrega as credenciais do .env
load_dotenv()

def get_engine() -> Optional[Engine]:
    """
    Cria a conexão com o banco de dados no Render usando URL segura.
    Retorna a engine do SQLAlchemy ou None em caso de falha.
    """
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")
    
    # Validação Fail-Fast: Verifica se esqueceu alguma variável no .env
    if not all([user, password, host, db_name]):
        print("❌ Erro: Faltam variáveis de ambiente no .env para o banco de dados.")
        return None
    
    # Construção segura da URL (Trata senhas com caracteres especiais como @, /, #)
    connection_url = URL.create(
        drivername="postgresql",
        username=user,
        password=password,
        host=host,
        database=db_name
    )
    
    try:
        engine = create_engine(connection_url)
        # Tenta uma conexão simples para validar
        with engine.connect() as conn:
            print("✅ Conexão com o Render estabelecida com sucesso!")
        return engine
    except SQLAlchemyError as e:
        print(f"❌ Erro ao conectar ao banco de dados: {e}")
        return None

def create_tables(engine: Engine) -> None:
    """Cria a tabela silver_products explicitamente se ela não existir."""
    if not engine:
        print("❌ Erro: Engine inválida fornecida para criação de tabelas.")
        return

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
    try:
        with engine.connect() as conn:
            conn.execute(text(query))
            conn.commit()
            print("✅ Sucesso: Estrutura da tabela validada no banco de dados!")
    except SQLAlchemyError as e:
        print(f"❌ Erro ao criar/validar tabela: {e}")

if __name__ == "__main__":
    # Teste de execução direta
    test_engine = get_engine()
    if test_engine:
        create_tables(test_engine)