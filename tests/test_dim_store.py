import pytest
import pandas as pd

def transform_dim_store(fact_rental_df):
    # Extraer solo las columnas relevantes para Dim_Store y eliminar duplicados
    return fact_rental_df[['store_id']].drop_duplicates()

def test_dim_store_transformation():
    # Datos de prueba simulados
    data = {
        'store_id': [101, 102, 101, 103],
        'other_column': ['X', 'Y', 'Z', 'W']  # Columna adicional para simular datos reales
    }
    fact_rental_df = pd.DataFrame(data)

    # Ejecuta la transformación
    result = transform_dim_store(fact_rental_df)

    # Validaciones
    assert 'store_id' in result.columns
    assert len(result) == 3  # Solo 3 tiendas únicas
    assert result['store_id'].tolist() == [101, 102, 103]  # Verifica que los IDs sean correctos
