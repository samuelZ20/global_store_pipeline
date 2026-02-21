import sys
import os

# Garante que o Python encontre a pasta src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

from api_client import extrair_dados
from transform import transform_products
from db_manager import get_engine

def run_pipeline():
    print("🚀 Iniciando Pipeline: Global Store Unified...")

    #EXTRAÇÃO (Camada Bronze)
    print("\n--- Etapa 1: Extração ---")
    raw_products = extrair_dados("products")
    
    if not raw_products:
        print("❌ Falha na extração. Abortando...")
        return

    #TRANSFORMAÇÃO (Camada Silver)
    print("\n--- Etapa 2: Transformação ---")
    df_products = transform_products(raw_products)

    #CARGA (Database)
    print("\n--- Etapa 3: Carga no Render ---")
    engine = get_engine()
    
    if engine and df_products is not None:
        try:
            # O Pandas envia o DataFrame direto para o SQL
            # 'if_exists=replace' cria a tabela automaticamente na primeira vez
            df_products.to_sql('silver_products', con=engine, if_exists='replace', index=False)
            print("✅ Dados carregados com sucesso na tabela 'silver_products'!")
        except Exception as e:
            print(f"❌ Erro ao carregar dados: {e}")
    else:
        print("❌ Falha na conexão com o banco de dados.")

if __name__ == "__main__":
    run_pipeline()