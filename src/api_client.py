import os
import requests
from datetime import datetime
from typing import List, Dict, Optional
from dotenv import load_dotenv


load_dotenv()
API_BASE_URL = os.getenv("API_BASE_URL", "https://fakestoreapi.com")

def extrair_dados(endpoint: str) -> Optional[List[Dict]]:
    """
    Conecta na API e busca os dados, adicionando o timestamp de extração (Camada Bronze).
    """
    url = f"{API_BASE_URL}/{endpoint}"
    
    try:
        print(f"Buscando dados de: {url}...")
        
        # Adicionado timeout de 10 segundos (Boas práticas para Airflow/Produção)
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        dados = response.json()
        
        # Otimização: Pegamos o timestamp uma única vez fora do loop
        timestamp_atual = datetime.now().isoformat()
        
        for item in dados:
            item['extraction_timestamp'] = timestamp_atual
            
        return dados

    except requests.exceptions.RequestException as req_err:
        # Captura especificamente erros de rede (timeout, conexão, 404, 500)
        print(f"❌ Erro de rede na extração da API: {req_err}")
        return None
    except ValueError as val_err:
        # Captura erro caso a API retorne algo que não seja um JSON válido
        print(f"❌ Erro ao converter resposta para JSON: {val_err}")
        return None

if __name__ == "__main__":
    produtos = extrair_dados("products")
    if produtos:
        print(f"✅ Sucesso! Extraímos {len(produtos)} produtos.")
        print(f"Exemplo de dado: {produtos[0]}")