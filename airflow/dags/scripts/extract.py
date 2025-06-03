import pandas as pd
from minio import Minio
import psycopg2
from io import BytesIO


def extract_table(table, src_db, src_db_user, src_db_password, src_db_host, src_db_port, minio_access_key, minio_secret_key, minio_endpoint):
    conn = psycopg2.connect(
        dbname=src_db,
        user=src_db_user,
        password=src_db_password,
        host=src_db_host,
        port=src_db_port
    )
    df = pd.read_sql(f"SELECT * FROM bookings.{table}", conn)
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
    csv_buffer.seek(0)

    client = Minio(
        endpoint=minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=False
    )

    bucket_name = "extracted-data"
    object_name = f"temp/{table}.csv"
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    client.put_object(
        bucket_name,
        object_name,
        csv_buffer,
        length=csv_buffer.getbuffer().nbytes,
        content_type='application/csv'
    )
    conn.close()
    
    print(f"Table {table} extracted and uploaded to MinIO bucket {bucket_name} as {object_name}, total rows: {len(df)}")
