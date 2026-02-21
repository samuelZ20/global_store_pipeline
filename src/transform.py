import pandas as pd

def transform_products(raw_data):
    """
    Transforma os dados brutos de produtos (Camada Bronze -> Silver).
    Realiza o flattening do campo 'rating'.
    """
    if not raw_data:
        return None
    
    #Converte a lista de dicionários em um DataFrame do Pandas
    df = pd.DataFrame(raw_data)
    
    #Flattening: Transforma o dicionário 'rating' em colunas separadas
    # Isso cria as colunas 'rate' e 'count'
    rating_df = pd.json_normalize(df['rating'])
    
    #Junta as novas colunas ao DataFrame original e remove a coluna 'rating' antiga
    df = pd.concat([df.drop(columns=['rating']), rating_df], axis=1)
    
    #Renomeia para ficar mais claro no banco de dados
    df = df.rename(columns={'rate': 'rating_rate', 'count': 'rating_count'})
    
    #Limpeza simples: Garante que os tipos estão corretos
    df['price'] = df['price'].astype(float)
    
    print("✅ Transformação (Silver) concluída com sucesso!")
    return df

if __name__ == "__main__":
    # Teste rápido simulando um dado da API
    test_data = [
        {
            "id": 1, 
            "title": "Test Product", 
            "price": 29.9, 
            "rating": {"rate": 4.5, "count": 10},
            "extraction_timestamp": "2026-02-21T15:00:00"
        }
    ]
    df_result = transform_products(test_data)
    print(df_result.head())