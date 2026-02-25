import pandas as pd

def transform_products(raw_data):
    """
    Transforma os dados brutos de produtos (Camada Bronze -> Silver).
    Realiza o flattening do campo 'rating'.
    """
    if not raw_data:
        return None
    
    df = pd.DataFrame(raw_data)
    
    rating_df = pd.json_normalize(df['rating'])
    
    df = pd.concat([df.drop(columns=['rating']), rating_df], axis=1)
    
    df = df.rename(columns={'rate': 'rating_rate', 'count': 'rating_count'})
    
    df['price'] = df['price'].astype(float)
    
    print("[OK] Transformacao (Silver) concluida com sucesso!")
    return df

if __name__ == "__main__":
    test_data = [
        {
            "id": 1, 
            "title": "Test Product", 
            "price": 29.9, 
            "rating": {"rate": 4.5, "count": 10},
            "extraction_timestamp": "2026-02-25T13:00:00"
        }
    ]
    df_result = transform_products(test_data)
    print(df_result.head())