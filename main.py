from src.api_client import extrair_dados
from src.transform import transform_products
from src.db_manager import get_engine
from src.init_db import create_tables
from sqlalchemy import text
import pandas as pd

def run_pipeline():
    print("🚀 Iniciando o pipeline ETL da Global Store...")

    # --- ETAPA 0: Setup do Banco ---
    print("\n[0/3] Configurando o banco de dados...")
    create_tables()

    # --- ETAPA 1: Extração ---
    print("\n[1/3] Iniciando Extração (Camada Bronze)...")
    dados_brutos = extrair_dados("products")
    
    if not dados_brutos:
        print("❌ Falha: Nenhum dado retornado da API.")
        return

    # --- ETAPA 2: Transformação ---
    print("\n[2/3] Iniciando Transformação (Camada Silver)...")
    df_silver = transform_products(dados_brutos)
    print(f"✅ Transformação concluída. {len(df_silver)} produtos processados.")

    # --- ETAPA 3: Carga ---
    print("\n[3/3] Iniciando Carga no PostgreSQL (Render)...")
    engine = get_engine()
    if engine:
        try:
            # Limpa os dados antigos antes de inserir os novos (evita duplicidade do id)
            with engine.connect() as conn:
                conn.execute(text("TRUNCATE TABLE silver_products;"))
                conn.commit()
            
            # Insere os dados usando 'append' para respeitar a tabela que o init_db criou
            df_silver.to_sql('silver_products', con=engine, if_exists='append', index=False)
            print("✅ Carga finalizada com sucesso! Dados inseridos no Render.")
            
        except Exception as e:
            print(f"❌ Erro durante a carga no banco: {e}")
    else:
        print("❌ Erro: Engine do banco não configurada.")

    print("\n🎉 Pipeline End-to-End executado com sucesso!")

if __name__ == "__main__":
    run_pipeline()