import requests
import os
from datetime import datetime
from dotenv import load_dotenv

# carregamos as configurações de segurança do arquivo .env
load_dotenv()

# pegamos a URL da API que você salvou no .env
API_BASE_URL = os.getenv("API_BASE_URL")

def extrair_dados(endpoint):
    """
    Função que conecta na API e busca os dados.
    Adiciona um timestamp para sabermos quando a extração foi feita (Boas Práticas).
    """
    url = f"{API_BASE_URL}/{endpoint}"
    
    try:
        print(f"Buscando dados de: {url}...")
        response = requests.get(url)
        
        #se a API responder algo diferente de 'sucesso' (200), ele gera um erro aqui
        response.raise_for_status()
        
        dados = response.json()
        
        #adicionamos a data da extração em cada item (Conceito de Camada Bronze)
        timestamp_atual = datetime.now().isoformat()
        for item in dados:
            item['extraction_timestamp'] = timestamp_atual
            
        return dados

    except Exception as e:
        print(f"Ocorreu um erro na extração: {e}")
        return None

#este bloco abaixo só roda se você executar o arquivo diretamente
if __name__ == "__main__":
    produtos = extrair_dados("products")
    if produtos:
        print(f"Sucesso! Conseguimos extrair {len(produtos)} produtos.")
        # Mostra o primeiro produto só para conferirmos
        print(f"Exemplo de dado: {produtos[0]}")