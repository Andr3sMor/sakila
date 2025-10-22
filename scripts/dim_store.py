import boto3
import pandas as pd
from io import BytesIO
import pyarrow.parquet as pq
import pyarrow as pa

# --- CONFIGURACIÓN ---
s3 = boto3.client('s3')
bucket_input = 'sakila-rds-customers'
prefix_input = 'fact_rental/' 
bucket_output = 'cmjm_datalake'
prefix_output = 'dim_store/'

def main():
    # --- LISTAR ARCHIVOS PARQUET EN S3 ---
    response = s3.list_objects_v2(Bucket=bucket_input, Prefix=prefix_input)
    files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.parquet')]

    if not files:
        raise Exception(f"No se encontraron archivos .parquet en s3://{bucket_input}/{prefix_input}")

    # --- LEER PARQUETS Y COMBINAR ---
    dfs = []
    for file_key in files:
        print(f"Leyendo {file_key} ...")
        obj = s3.get_object(Bucket=bucket_input, Key=file_key)
        buffer = BytesIO(obj['Body'].read())
        table = pq.read_table(buffer)
        df = table.to_pandas()
        dfs.append(df)

    fact_rental_df = pd.concat(dfs, ignore_index=True)

    # --- EXTRAER Y TRANSFORMAR DATOS PARA Dim_Store ---
    dim_store_df = fact_rental_df[['store_id']].drop_duplicates()

    # --- GUARDAR RESULTADO COMO PARQUET SNAPPY ---
    table = pa.Table.from_pandas(dim_store_df, preserve_index=False)
    buffer_out = BytesIO()
    pq.write_table(table, buffer_out, compression='snappy')

    output_key = f"{prefix_output}dim_store.snappy.parquet"
    s3.put_object(Bucket=bucket_output, Key=output_key, Body=buffer_out.getvalue())

    print("ETL completado correctamente.")
    print(f"Archivo generado: s3://{bucket_output}/{output_key}")

if __name__ == "__main__":
    main()
