from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from scripts.extract import extract_table
from scripts.load import load_table
from scripts.transform import transform_table
import os
from dotenv import load_dotenv

# Load variables from .env file
load_dotenv()

default_args = {
    'start_date': datetime(2025, 5, 25),
    'catchup': False
}

# SRC DATABASE CONFIGURATION
SRC_DB = os.getenv("SRC_DB")
SRC_DB_USER = os.getenv("SRC_DB_USER")
SRC_DB_PASSWORD = os.getenv("SRC_DB_PASSWORD")
SRC_DB_HOST = os.getenv("SRC_DB_HOST")
SRC_DB_PORT = os.getenv("SRC_DB_PORT")

# DWH DATABASE CONFIGURATION
DWH_DB = os.getenv("DWH_DB")
DWH_DB_USER = os.getenv("DWH_DB_USER")
DWH_DB_PASSWORD = os.getenv("DWH_DB_PASSWORD")
DWH_DB_HOST = os.getenv("DWH_DB_HOST")
DWH_DB_PORT = os.getenv("DWH_DB_PORT")

# MINIO CONFIGURATION
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

TABLES = ["aircrafts_data", "airports_data", "boarding_passes", "bookings", "flights", "seats", "ticket_flights", "tickets"]

TRANSFORM_SQL_DIR = "/opt/airflow/dags/scripts/sql/transform"

with DAG(
    dag_id='flights_data_pipeline',
    schedule_interval='@daily',
    default_args=default_args,
    description='ETL pipeline for flight data',
    catchup=False,
    tags=['development']
) as dag:

    with TaskGroup("extract") as extract_group:
        extract_tasks = [
            PythonOperator(
                task_id=f"{table}",
                python_callable=extract_table,
                op_kwargs={
                    "table": table, 
                    "src_db": SRC_DB,
                    "src_db_user": SRC_DB_USER,
                    "src_db_password": SRC_DB_PASSWORD,
                    "src_db_host": SRC_DB_HOST,
                    "src_db_port": SRC_DB_PORT,
                    "minio_access_key": MINIO_ACCESS_KEY,
                    "minio_secret_key": MINIO_SECRET_KEY,
                    "minio_endpoint": MINIO_ENDPOINT}
            )
            for table in TABLES
        ]


    with TaskGroup("load") as load_group:
        load_aircrafts = PythonOperator(
            task_id="aircrafts_data",
            python_callable=load_table,
            op_kwargs={
                "table": "aircrafts_data",
                "dwh_db": DWH_DB,
                "dwh_db_user": DWH_DB_USER,
                "dwh_db_password": DWH_DB_PASSWORD,
                "dwh_db_host": DWH_DB_HOST,
                "dwh_db_port": DWH_DB_PORT,
                "minio_access_key": MINIO_ACCESS_KEY,
                "minio_secret_key": MINIO_SECRET_KEY,
                "minio_endpoint": MINIO_ENDPOINT}
        )

        load_airports = PythonOperator(
            task_id="airports_data",
            python_callable=load_table,
            op_kwargs={
                "table": "airports_data",
                "dwh_db": DWH_DB,
                "dwh_db_user": DWH_DB_USER,
                "dwh_db_password": DWH_DB_PASSWORD,
                "dwh_db_host": DWH_DB_HOST,
                "dwh_db_port": DWH_DB_PORT,
                "minio_access_key": MINIO_ACCESS_KEY,
                "minio_secret_key": MINIO_SECRET_KEY,
                "minio_endpoint": MINIO_ENDPOINT}
        )
        
        load_bookings = PythonOperator(
            task_id="bookings",
            python_callable=load_table,
            op_kwargs={
                "table": "bookings",
                "dwh_db": DWH_DB,
                "dwh_db_user": DWH_DB_USER,
                "dwh_db_password": DWH_DB_PASSWORD,
                "dwh_db_host": DWH_DB_HOST,
                "dwh_db_port": DWH_DB_PORT,
                "minio_access_key": MINIO_ACCESS_KEY,
                "minio_secret_key": MINIO_SECRET_KEY,
                "minio_endpoint": MINIO_ENDPOINT}
        )
        
        load_tickets = PythonOperator(
            task_id="tickets",
            python_callable=load_table,
            op_kwargs={
                "table": "tickets",
                "dwh_db": DWH_DB,
                "dwh_db_user": DWH_DB_USER,
                "dwh_db_password": DWH_DB_PASSWORD,
                "dwh_db_host": DWH_DB_HOST,
                "dwh_db_port": DWH_DB_PORT,
                "minio_access_key": MINIO_ACCESS_KEY,
                "minio_secret_key": MINIO_SECRET_KEY,
                "minio_endpoint": MINIO_ENDPOINT}
        )
        
        load_seats = PythonOperator(
            task_id="seats",
            python_callable=load_table,
            op_kwargs={
                "table": "seats",
                "dwh_db": DWH_DB,
                "dwh_db_user": DWH_DB_USER,
                "dwh_db_password": DWH_DB_PASSWORD,
                "dwh_db_host": DWH_DB_HOST,
                "dwh_db_port": DWH_DB_PORT,
                "minio_access_key": MINIO_ACCESS_KEY,
                "minio_secret_key": MINIO_SECRET_KEY,
                "minio_endpoint": MINIO_ENDPOINT}
        )
        
        load_flights = PythonOperator(
            task_id="flights",
            python_callable=load_table,
            op_kwargs={
                "table": "flights",
                "dwh_db": DWH_DB,
                "dwh_db_user": DWH_DB_USER,
                "dwh_db_password": DWH_DB_PASSWORD,
                "dwh_db_host": DWH_DB_HOST,
                "dwh_db_port": DWH_DB_PORT,
                "minio_access_key": MINIO_ACCESS_KEY,
                "minio_secret_key": MINIO_SECRET_KEY,
                "minio_endpoint": MINIO_ENDPOINT}
        )
        
        load_ticket_flights = PythonOperator(
            task_id="ticket_flights",
            python_callable=load_table,
            op_kwargs={
                "table": "ticket_flights",
                "dwh_db": DWH_DB,
                "dwh_db_user": DWH_DB_USER,
                "dwh_db_password": DWH_DB_PASSWORD,
                "dwh_db_host": DWH_DB_HOST,
                "dwh_db_port": DWH_DB_PORT,
                "minio_access_key": MINIO_ACCESS_KEY,
                "minio_secret_key": MINIO_SECRET_KEY,
                "minio_endpoint": MINIO_ENDPOINT}
        )
        
        load_boarding_passes = PythonOperator(
            task_id="boarding_passes",
            python_callable=load_table,
            op_kwargs={
                "table": "boarding_passes",
                "dwh_db": DWH_DB,
                "dwh_db_user": DWH_DB_USER,
                "dwh_db_password": DWH_DB_PASSWORD,
                "dwh_db_host": DWH_DB_HOST,
                "dwh_db_port": DWH_DB_PORT,
                "minio_access_key": MINIO_ACCESS_KEY,
                "minio_secret_key": MINIO_SECRET_KEY,
                "minio_endpoint": MINIO_ENDPOINT}
        )
        
        load_aircrafts >> load_airports >> load_bookings >> load_tickets >> load_seats >> load_flights >> load_ticket_flights >> load_boarding_passes
        

    with TaskGroup("transform") as transform_group:
        transform_dim_aircrafts = PythonOperator(
            task_id="dim_aircrafts",
            python_callable=transform_table,
            op_kwargs={
                "filename": os.path.join(TRANSFORM_SQL_DIR, "dim_aircrafts.sql"),
                "dwh_db": DWH_DB,
                "dwh_user": DWH_DB_USER,
                "dwh_pass": DWH_DB_PASSWORD,
                "dwh_host": DWH_DB_HOST,
                "dwh_port": DWH_DB_PORT
            }
        )
        
        transform_dim_airport = PythonOperator(
            task_id="dim_airport",
            python_callable=transform_table,
            op_kwargs={
                "filename": os.path.join(TRANSFORM_SQL_DIR, "dim_airport.sql"),
                "dwh_db": DWH_DB,
                "dwh_user": DWH_DB_USER,
                "dwh_pass": DWH_DB_PASSWORD,
                "dwh_host": DWH_DB_HOST,
                "dwh_port": DWH_DB_PORT
            }
        )
        
        transform_dim_passenger = PythonOperator(
            task_id="dim_passenger",
            python_callable=transform_table,
            op_kwargs={
                "filename": os.path.join(TRANSFORM_SQL_DIR, "dim_passenger.sql"),
                "dwh_db": DWH_DB,
                "dwh_user": DWH_DB_USER,
                "dwh_pass": DWH_DB_PASSWORD,
                "dwh_host": DWH_DB_HOST,
                "dwh_port": DWH_DB_PORT
            }
        )
        
        transform_dim_seat = PythonOperator(
            task_id="dim_seat",
            python_callable=transform_table,
            op_kwargs={
                "filename": os.path.join(TRANSFORM_SQL_DIR, "dim_seat.sql"),
                "dwh_db": DWH_DB,
                "dwh_user": DWH_DB_USER,
                "dwh_pass": DWH_DB_PASSWORD,
                "dwh_host": DWH_DB_HOST,
                "dwh_port": DWH_DB_PORT
            }
        )
        
        transform_fct_boarding_pass = PythonOperator(
            task_id="fct_boarding_pass",
            python_callable=transform_table,
            op_kwargs={
                "filename": os.path.join(TRANSFORM_SQL_DIR, "fct_boarding_pass.sql"),
                "dwh_db": DWH_DB,
                "dwh_user": DWH_DB_USER,
                "dwh_pass": DWH_DB_PASSWORD,
                "dwh_host": DWH_DB_HOST,
                "dwh_port": DWH_DB_PORT
            }
        )
        
        transform_fct_booking_ticket = PythonOperator(
            task_id="fct_booking_ticket",
            python_callable=transform_table,
            op_kwargs={
                "filename": os.path.join(TRANSFORM_SQL_DIR, "fct_booking_ticket.sql"),
                "dwh_db": DWH_DB,
                "dwh_user": DWH_DB_USER,
                "dwh_pass": DWH_DB_PASSWORD,
                "dwh_host": DWH_DB_HOST,
                "dwh_port": DWH_DB_PORT
            }
        )
        
        transform_fct_flight_activity = PythonOperator(
            task_id="fct_flight_activity",
            python_callable=transform_table,
            op_kwargs={
                "filename": os.path.join(TRANSFORM_SQL_DIR, "fct_flight_activity.sql"),
                "dwh_db": DWH_DB,
                "dwh_user": DWH_DB_USER,
                "dwh_pass": DWH_DB_PASSWORD,
                "dwh_host": DWH_DB_HOST,
                "dwh_port": DWH_DB_PORT
            }
        )
        
        transform_fct_seat_occupied_daily = PythonOperator(
            task_id="fct_seat_occupied_daily",
            python_callable=transform_table,
            op_kwargs={
                "filename": os.path.join(TRANSFORM_SQL_DIR, "fct_seat_occupied_daily.sql"),
                "dwh_db": DWH_DB,
                "dwh_user": DWH_DB_USER,
                "dwh_pass": DWH_DB_PASSWORD,
                "dwh_host": DWH_DB_HOST,
                "dwh_port": DWH_DB_PORT
            }
        )

        transform_dim_aircrafts >> transform_dim_airport >> transform_dim_passenger >> transform_dim_seat >> transform_fct_boarding_pass >> transform_fct_booking_ticket >> transform_fct_flight_activity >> transform_fct_seat_occupied_daily


    extract_group >> load_group >> transform_group