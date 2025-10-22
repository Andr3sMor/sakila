import pytest
import pandas as pd
from io import BytesIO
import pyarrow.parquet as pq
from datetime import datetime

# Simula la función de transformación de Dim_Date
def transform_dim_date(fact_rental_df):
    fact_rental_df['rental_date'] = pd.to_datetime(fact_rental_df['rental_date']).dt.date
    unique_dates = sorted(fact_rental_df['rental_date'].unique())
    dates_df = pd.DataFrame({'rental_date': unique_dates})
    dates_df['date_id'] = pd.to_datetime(dates_df['rental_date']).dt.strftime('%Y%m%d').astype(int)
    dates_df['day'] = pd.to_datetime(dates_df['rental_date']).dt.day
    dates_df['month'] = pd.to_datetime(dates_df['rental_date']).dt.month
    dates_df['year'] = pd.to_datetime(dates_df['rental_date']).dt.year
    dates_df['day_of_week'] = pd.to_datetime(dates_df['rental_date']).dt.day_name()
    dates_df['week_of_year'] = pd.to_datetime(dates_df['rental_date']).dt.isocalendar().week.astype(int)
    dates_df['quarter'] = pd.to_datetime(dates_df['rental_date']).dt.quarter
    dates_df['is_weekend'] = pd.to_datetime(dates_df['rental_date']).dt.dayofweek.isin([5, 6])
    return dates_df

# Prueba unitaria
def test_dim_date_transformation():
    # Datos de prueba simulados
    data = {'rental_date': ['2025-10-20', '2025-10-21', '2025-10-20']}
    fact_rental_df = pd.DataFrame(data)

    # Ejecuta la transformación
    result = transform_dim_date(fact_rental_df)

    # Validaciones
    assert 'date_id' in result.columns
    assert 'day' in result.columns
    assert 'month' in result.columns
    assert 'year' in result.columns
    assert 'day_of_week' in result.columns
    assert 'week_of_year' in result.columns
    assert 'quarter' in result.columns
    assert 'is_weekend' in result.columns
    assert len(result) == 2  # Solo 2 fechas únicas
    assert result['date_id'].iloc[0] == 20251020  # Formato YYYYMMDD
