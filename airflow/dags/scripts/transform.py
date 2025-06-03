import os
from sqlalchemy import create_engine, text

def transform_table(filename: str, dwh_db, dwh_user, dwh_pass, dwh_host, dwh_port):
    
    engine = create_engine(
        f"postgresql+psycopg2://{dwh_user}:{dwh_pass}@{dwh_host}:{dwh_port}/{dwh_db}"
    )

    with open(filename, 'r') as file:
        sql = file.read()

    with engine.begin() as connection:
        result = connection.execute(text(sql))
        print(f"Transformation completed. Rows affected: {result.rowcount}")