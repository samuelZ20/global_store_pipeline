from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import pandas as pd
from sqlalchemy import text

# Definimos o caminho absoluto para que o Airflow encontre a pasta 'src'
PROJECT_PATH = "/mnt/c/Users/samue/OneDrive/Documentos/global_store_pipeline"
SRC_PATH = os.path.join(PROJECT_PATH, 'src')

if SRC_PATH not in sys.path:
    sys.path.append(SRC_PATH)

from api_client import extrair_dados
from transform import transform_products
from db_manager import get_engine
from init_db import create_tables

default_args = {
    'owner': 'Samuel Frizzone',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'global_store_multi_task_pipeline',
    default_args=default_args,
    description='Pipeline ETL Modular para a Global Store',
    schedule_interval='@daily',
    catchup=False
) as dag:

    # --- TAREFA 0: SETUP DO BANCO ---
    def task_setup_db(**kwargs):
        """Garante que a tabela exista antes de começar."""
        create_tables()

    setup_db_task = PythonOperator(
        task_id='setup_database',
        python_callable=task_setup_db
    )

    # --- TAREFA 1: EXTRAÇÃO ---
    def task_extract(**kwargs):
        dados_brutos = extrair_dados("products")
        if not dados_brutos:
            raise Exception("Falha crítica: Extração retornou vazio.")
        return dados_brutos

    extract_task = PythonOperator(
        task_id='extract_from_api',
        python_callable=task_extract
    )

    # --- TAREFA 2: TRANSFORMAÇÃO ---
    def task_transform(**kwargs):
        ti = kwargs['ti']
        dados_brutos = ti.xcom_pull(task_ids='extract_from_api')
        
        df_silver = transform_products(dados_brutos)
        if df_silver is None or df_silver.empty:
            raise Exception("Falha crítica: Transformação resultou em vazio.")
            
        return df_silver.to_dict(orient='records')

    transform_task = PythonOperator(
        task_id='transform_with_pandas',
        python_callable=task_transform
    )

    # --- TAREFA 3: LOAD
    def task_load(**kwargs):
        ti = kwargs['ti']
        # Puxa os dados serializados do XCom
        dados_limpos = ti.xcom_pull(task_ids='transform_with_pandas')
        
        if not dados_limpos:
            raise Exception("❌ Falha: Não há dados no XCom para carregar.")

        df_final = pd.DataFrame(dados_limpos)
        engine = get_engine()
        
        if not engine:
            raise Exception("❌ Falha crítica: Não foi possível obter o motor de conexão.")

        try:
            # Preparamos os dados para o SQLAlchemy Core
            registros = df_final.to_dict(orient='records')

            # 'engine.begin()' garante que o TRUNCATE e o INSERT sejam uma única transação
            with engine.begin() as conn:
                # 1. Limpeza para garantir idempotência
                conn.execute(text("TRUNCATE TABLE silver_products;"))
                
                # 2. Inserção em lote (Bulk Insert)
                insert_query = text("""
                    INSERT INTO silver_products (
                        id, title, price, category, image, 
                        rating_rate, rating_count, extraction_timestamp
                    )
                    VALUES (
                        :id, :title, :price, :category, :image, 
                        :rating_rate, :rating_count, :extraction_timestamp
                    )
                """)
                conn.execute(insert_query, registros)
                
            print(f"✅ Sucesso Sênior: {len(df_final)} produtos persistidos no Render!")
            
        except Exception as e:
            raise Exception(f"Erro crítico durante a carga via SQLAlchemy Core: {e}")

    load_task = PythonOperator(
        task_id='load_to_render',
        python_callable=task_load
    )

    # --- DEPENDÊNCIAS ---
    setup_db_task >> extract_task >> transform_task >> load_task