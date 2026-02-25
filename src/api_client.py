import os
import requests
from datetime import datetime
from typing import List, Dict, Optional
from dotenv import load_dotenv


load_dotenv()
API_BASE_URL = os.getenv("API_BASE_URL", "https://fakestoreapi.com")

def extrair_dados(endpoint: str) -> Optional[List[Dict]]:
    """
    Conecta na API e busca os dados, adicionando o timestamp de extracao (Camada Bronze).
    """
    url = f"{API_BASE_URL}/{endpoint}"
    
    try:
        print(f"[INFO] Buscando dados de: {url}...")
        
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        dados = response.json()
        
        timestamp_atual = datetime.now().isoformat()
        
        for item in dados:
            item['extraction_timestamp'] = timestamp_atual
            
        return dados

    except requests.exceptions.RequestException as req_err:
        print(f"[ERROR] Erro de rede na extracao da API: {req_err}")
        return None
    except ValueError as val_err:
        print(f"[ERROR] Erro ao converter resposta para JSON: {val_err}")
        return None

if __name__ == "__main__":
    produtos = extrair_dados("products")
    if produtos:
        print(f"[OK] Sucesso! Extraimos {len(produtos)} produtos.")
        print(f"Exemplo de dado: {produtos[0]}")