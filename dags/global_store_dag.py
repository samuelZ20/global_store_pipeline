from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import pandas as pd

# Garante que o Airflow encontre a pasta 'src' para importar seus módulos locais
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from api_client import extrair_dados
from transform import transform_products
from db_manager import get_engine

# 1. Configurações básicas da DAG (Default Args)
default_args = {
    'owner': 'Samuel Frizzone',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 2. Definição da DAG
with DAG(
    'global_store_multi_task_pipeline',
    default_args=default_args,
    description='Pipeline ETL Modular para a Global Store - Samuel Frizzone (UFLA)',
    schedule='@daily', 
    catchup=False
) as dag:

    # --- TAREFA 1: EXTRAÇÃO ---
    def task_extract(**kwargs):
        """Busca dados brutos da API e os disponibiliza via XCom."""
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
        """Recupera dados da extração e aplica a lógica da Camada Silver."""
        ti = kwargs['ti']
        dados_brutos = ti.xcom_pull(task_ids='extract_from_api')
        
        df_silver = transform_products(dados_brutos)
        if df_silver is None:
            raise Exception("Falha crítica: A transformação resultou em um conjunto vazio.")
            
        # Converte para dicionário para ser serializável pelo XCom
        return df_silver.to_dict(orient='records')

    transform_task = PythonOperator(
        task_id='transform_with_pandas',
        python_callable=task_transform
    )

    # --- TAREFA 3: CARGA ---
    def task_load(**kwargs):
        """Recupera dados transformados e carrega no PostgreSQL do Render."""
        ti = kwargs['ti']
        dados_limpos = ti.xcom_pull(task_ids='transform_with_pandas')
        
        df_final = pd.DataFrame(dados_limpos)
        engine = get_engine()
        
        if engine:
            df_final.to_sql('silver_products', con=engine, if_exists='replace', index=False)
            print("✅ Sucesso: Dados persistidos na tabela 'silver_products' no Render.")
        else:
            raise Exception("Falha crítica: Não foi possível conectar ao banco de dados.")

    load_task = PythonOperator(
        task_id='load_to_render',
        python_callable=task_load
    )

    # --- 3. DEFINIÇÃO DO FLUXO (DEPENDÊNCIAS) ---
    extract_task >> transform_task >> load_task