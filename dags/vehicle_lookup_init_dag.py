import json
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

default_args = {
    'owner': 'JavokhirAbdullaev',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'vehicle_lookup_setup',
    default_args=default_args,
    description='Fetch and store vehicle lookup data in Airflow Variables',
    schedule_interval='@daily',  # Run daily to refresh lookups
    catchup=False,
    tags=['vehicle', 'lookup', 'setup']
)

def initialize_lookups():
    logging.info("Fetching lookup data from PostGIS")
    oltp_hook = PostgresHook(postgres_conn_id='postgres_oltp')

    # Color lookup
    color_lookup = oltp_hook.get_records("SELECT id, name FROM sc_vehicle_color_lk")
    colors = {row[1]: row[0] for row in color_lookup}

    # MMCT lookup
    mmct_lookup = oltp_hook.get_records(
        """SELECT mmct.category_id, mmct.type_id, t.name as type_name, m.name AS model_name
           FROM sc_vehicle_mmct_lk mmct
           JOIN sc_vehicle_model_lk m ON m.id = mmct.model_id
           JOIN sc_vehicle_type_lk as t on t.id = mmct.type_id"""
    )
    mmct = {row[3]: {'category_id': row[0], 'type_id': row[1], 'type_name': row[2]} for row in mmct_lookup}

    # Camera lookup
    camera_lookup = oltp_hook.get_records(
        """SELECT c.id as id, c.reference_id, z.id as zone_id, z.organization_id as tenant_id
           FROM sc_camera AS c JOIN sc_zone as z on z.id = c.zone_id"""
    )
    cameras = {row[1]: {'id': row[0], 'zone_id': row[2], 'tenant_id': row[3]} for row in camera_lookup}

    # Store in Airflow Variables
    lookups = {'colors': colors, 'mmct': mmct, 'cameras': cameras}
    Variable.set("vehicle_lookups", json.dumps(lookups), serialize_json=True)
    logging.info(f"Stored {len(colors)} colors, {len(mmct)} mmct, {len(cameras)} cameras in Variables")

initialize_task = PythonOperator(
    task_id='initialize_lookups',
    python_callable=initialize_lookups,
    dag=dag,
)

initialize_task