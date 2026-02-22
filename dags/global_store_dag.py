from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import pandas as pd
from sqlalchemy import text

# Garante que o Airflow encontre a pasta 'src' para importar seus módulos locais
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from api_client import extrair_dados
from transform import transform_products
from db_manager import get_engine
from init_db import create_tables # <-- Novo import!

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
    schedule_interval='@daily', # Atualizado para a sintaxe clássica mais estável
    catchup=False
) as dag:

    # --- TAREFA 0: SETUP DO BANCO (Novo) ---
    def task_setup_db(**kwargs):
        """Garante que a tabela exista com os tipos corretos antes de começar."""
        create_tables()

    setup_db_task = PythonOperator(
        task_id='setup_database',
        python_callable=task_setup_db
    )

    # --- TAREFA 1: EXTRAÇÃO ---
    def task_extract(**kwargs):
        dados_brutos = extrair_dados("products")
        if not dados_brutos:
            raise Exception("Falha crítica: Não foi possível extrair dados da API.")
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
            raise Exception("Falha crítica: A transformação resultou em um conjunto vazio.")
            
        return df_silver.to_dict(orient='records')

    transform_task = PythonOperator(
        task_id='transform_with_pandas',
        python_callable=task_transform
    )

    # --- TAREFA 3: CARGA ---
    def task_load(**kwargs):
        ti = kwargs['ti']
        dados_limpos = ti.xcom_pull(task_ids='transform_with_pandas')
        
        df_final = pd.DataFrame(dados_limpos)
        engine = get_engine()
        
        if not engine:
            raise Exception("Falha crítica: Não foi possível conectar ao banco de dados.")

        try:
            with engine.connect() as conn:
                # Limpa dados antigos da tabela mantendo a estrutura que o Paco queria
                conn.execute(text("TRUNCATE TABLE silver_products;"))
                conn.commit()
            
            # Insere os novos dados (append) respeitando o DDL original
            df_final.to_sql('silver_products', con=engine, if_exists='append', index=False)
            print("✅ Sucesso: Dados persistidos na tabela 'silver_products' no Render.")
        except Exception as e:
            raise Exception(f"Erro crítico durante a carga no banco: {e}")

    load_task = PythonOperator(
        task_id='load_to_render',
        python_callable=task_load
    )

    # --- DEFINIÇÃO DO FLUXO (DEPENDÊNCIAS) ---
    setup_db_task >> extract_task >> transform_task >> load_task