from minio import Minio
import pandas as pd
from sqlalchemy import create_engine
from io import BytesIO
from pangres import upsert
import json

def load_table(table, dwh_db, dwh_db_user, dwh_db_password, dwh_db_host, dwh_db_port, minio_access_key, minio_secret_key, minio_endpoint):

    # Initialize MinIO client
    client = Minio(
        endpoint=minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=False
    )

    bucket_name = "extracted-data"
    object_name = f"temp/{table}.csv"

    if not client.bucket_exists(bucket_name):
        raise Exception(f"Bucket '{bucket_name}' does not exist")

    # Get the object (CSV)
    response = client.get_object(bucket_name, object_name)
    csv_data = BytesIO(response.read())  # Read into memory
    df = pd.read_csv(csv_data)
 
    print(f"df head data: {df.head()}")
    # # Connect to PostgreSQL (warehouse)
    conn = create_engine(
        f"postgresql://{dwh_db_user}:{dwh_db_password}@{dwh_db_host}:{dwh_db_port}/{dwh_db}"
    )

    schema = "stg"

    # Define primary key columns for each table
    PRIMARY_KEYS = {
        "aircrafts_data": ["aircraft_code"],
        "airports_data": ["airport_code"],
        "bookings": ["book_ref"],
        "tickets": ["ticket_no"],
        "seats": ["aircraft_code", "seat_no"],
        "flights": ["flight_id"],
        "ticket_flights": ["ticket_no", "flight_id"],
        "boarding_passes": ["ticket_no", "flight_id"]
    }

    if table not in PRIMARY_KEYS:
        raise ValueError(f"No primary key defined for table '{table}'")
    df = df.set_index(PRIMARY_KEYS[table])

    if table == "aircrafts_data":
        df['model'] = df['model'].apply(json.dumps)
    elif table == "airports_data":
        df['airport_name'] = df['airport_name'].apply(json.dumps)
        df['city'] = df['city'].apply(json.dumps)
    elif table == "tickets":
        df['contact_data'] = df['contact_data'].apply(json.dumps)
        
    # Perform UPSERT using pangres
    upsert(
        con=conn,
        df=df,
        table_name=table,
        schema=schema,
        if_row_exists="update"
    )

    print(f"insert total data: {len(df)} rows into {table}")
