import pandas as pd
from sqlalchemy import text
from src.db_manager import get_engine
from src.init_db import create_tables
from src.api_client import extrair_dados
from src.transform import transform_products

def run_pipeline():
    print("🚀 Iniciando o pipeline ETL da Global Store...")

    # --- ETAPA 0: Infraestrutura ---
    print("\n[0/3] Configurando o banco de dados...")
    create_tables()

    # --- ETAPA 1: Extração (Bronze) ---
    print("\n[1/3] Iniciando Extração (Camada Bronze)...")
    dados_brutos = extrair_dados("products")
    
    if not dados_brutos:
        print("❌ Falha crítica: API não retornou dados.")
        return

    # --- ETAPA 2: Transformação (Silver) ---
    print("\n[2/3] Iniciando Transformação (Camada Silver)...")
    df_silver = transform_products(dados_brutos)
    
    if df_silver is None or df_silver.empty:
        print("⚠️ Aviso: DataFrame vazio. Nada para carregar.")
        return
        
    print(f"✅ Transformação concluída. {len(df_silver)} produtos processados.")

    # --- ETAPA 3: Carga (SQLAlchemy Core) ---
    print("\n[3/3] Iniciando Carga no PostgreSQL (Render)...")
    engine = get_engine()
    
    if not engine:
        print("❌ Falha: Não foi possível conectar ao banco.")
        return

    try:
        # Transformamos o DataFrame em uma lista de dicionários para o SQLAlchemy
        registros = df_silver.to_dict(orient='records')

        # Usamos engine.begin() para que carregue tudo ou nao carregue nada
        with engine.begin() as conn:
            # 1. Limpeza da tabela
            conn.execute(text("TRUNCATE TABLE silver_products;"))
            print("🗑️ Tabela silver_products limpa com sucesso.")
            
            # 2. Carga Direta (Bypassing Pandas to_sql bugs)
            # Criamos o comando de insert explicitamente
            insert_query = text("""
                INSERT INTO silver_products (id, title, price, category, image, rating_rate, rating_count, extraction_timestamp)
                VALUES (:id, :title, :price, :category, :image, :rating_rate, :rating_count, :extraction_timestamp)
            """)
            
            # O SQLAlchemy executa o insert em lote de forma extremamente rápida
            conn.execute(insert_query, registros)
            
        print(f"✅ Sucesso: {len(df_silver)} produtos carregados com integridade no Render!")
        
    except Exception as e:
        print(f"❌ Erro crítico durante a carga: {e}")
        return

    print("\n🎉 Pipeline End-to-End finalizado com sucesso!")

if __name__ == "__main__":
    run_pipeline()