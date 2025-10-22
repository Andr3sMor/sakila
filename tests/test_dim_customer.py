import pytest
import pandas as pd

def transform_dim_customer(fact_rental_df):
    return fact_rental_df[['customer_id', 'name']].drop_duplicates()

def test_dim_customer_transformation():
    data = {
        'customer_id': [1, 2, 1],
        'name': ['Alice', 'Bob', 'Alice']
    }
    fact_rental_df = pd.DataFrame(data)
    result = transform_dim_customer(fact_rental_df)

    assert 'customer_id' in result.columns
    assert 'name' in result.columns
    assert len(result) == 2  # Solo 2 clientes únicos
