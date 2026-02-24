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
    Gerencia a conexão com o banco no Render com foco em resiliência.
    Utiliza pooling para evitar quedas de conexão em ambientes Cloud.
    """
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")
    port = os.getenv("DB_PORT", "5432") # Padrão do Render é 5432
    
    if not all([user, password, host, db_name]):
        print("❌ Erro: Variáveis de ambiente incompletas no .env.")
        return None
    
    connection_url = URL.create(
        drivername="postgresql",
        username=user,
        password=password,
        host=host,
        port=port,
        database=db_name
    )
    
    try:
        engine = create_engine(
            connection_url, 
            pool_pre_ping=True, 
            pool_recycle=3600
        )
        
        # Teste de conectividade rápido usando transação automática (begin)
        # Compatível com SQLAlchemy 1.4 (Sem erro de .commit())
        with engine.begin() as conn:
            conn.execute(text("SELECT 1"))
            print("✅ Conexão com o Render estabelecida e validada!")
            
        return engine
    except SQLAlchemyError as e:
        print(f"❌ Falha crítica de conectividade: {e}")
        return None

def execute_ddl(engine: Engine, query: str) -> None:
    """Executa comandos de estrutura (DDL) com gestão de transação automática."""
    if not engine:
        return

    try:
        # O uso de engine.begin() garante o commit automático no SQLAlchemy 1.4
        with engine.begin() as conn:
            conn.execute(text(query))
        print("✅ Comando SQL executado com sucesso!")
    except SQLAlchemyError as e:
        print(f"❌ Erro na execução SQL: {e}")
        raise e # Re-lança para o Airflow capturar a falha