import pytest
import pandas as pd

def transform_dim_film(fact_rental_df):
    # Extraer solo las columnas relevantes para Dim_Film y eliminar duplicados
    return fact_rental_df[['film_id']].drop_duplicates()

def test_dim_film_transformation():
    # Datos de prueba simulados
    data = {
        'film_id': [1, 2, 1, 3],
        'other_column': ['A', 'B', 'C', 'D']  # Columna adicional para simular datos reales
    }
    fact_rental_df = pd.DataFrame(data)

    # Ejecuta la transformación
    result = transform_dim_film(fact_rental_df)

    # Validaciones
    assert 'film_id' in result.columns
    assert len(result) == 3  # Solo 3 películas únicas
    assert result['film_id'].tolist() == [1, 2, 3]  # Verifica que los IDs sean correctos
