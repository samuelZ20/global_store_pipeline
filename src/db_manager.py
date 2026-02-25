import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import SQLAlchemyError
from typing import Optional

load_dotenv()

def get_engine() -> Optional[Engine]:
    """
    Gerencia a conexao com o banco no Render com foco em resiliencia.
    Utiliza pooling para evitar quedas de conexao em ambientes Cloud.
    """
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    db_name = os.getenv("DB_NAME")
    port = os.getenv("DB_PORT", "5432")
    
    if not all([user, password, host, db_name]):
        print("[ERROR] Variaveis de ambiente incompletas no .env.")
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
        
        with engine.begin() as conn:
            conn.execute(text("SELECT 1"))
            print("[OK] Conexao com o Render estabelecida e validada!")
            
        return engine
    except SQLAlchemyError as e:
        print(f"[ERROR] Falha critica de conectividade: {e}")
        return None

def execute_ddl(engine: Engine, query: str) -> None:
    """Executa comandos de estrutura (DDL) com gestao de transacao automatica."""
    if not engine:
        return

    try:
        with engine.begin() as conn:
            conn.execute(text(query))
        print("[OK] Comando SQL executado com sucesso!")
    except SQLAlchemyError as e:
        print(f"[ERROR] Erro na execucao SQL: {e}")
        raise e