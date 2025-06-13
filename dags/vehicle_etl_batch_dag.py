from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
import json
import uuid
import logging
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import RealDictCursor
import re

# Default arguments for the DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'vehicle_data_enrichment',
    default_args=default_args,
    description='Extract vehicle data from Kafka, enrich with PostgreSQL lookups, and load to TimescaleDB',
    schedule_interval=timedelta(minutes=10),
    catchup=False,
    tags=['vehicle', 'kafka', 'enrichment', 'timescaledb']
)


def extract_kafka_and_db_data(**context):
    """
    Extract data from Kafka topic and PostgreSQL lookup tables
    """
    logging.info("Starting data extraction from Kafka and PostgreSQL")

    # Kafka configuration
    kafka_config = {
        'bootstrap_servers': ['kafka:29092'],
        'group_id': 'airflow_enrichment',
        'auto_offset_reset': 'earliest',
        'value_deserializer': lambda x: json.loads(x.decode('utf-8')),
        'consumer_timeout_ms': 30000  # 30 seconds timeout
    }

    # Extract from Kafka
    consumer = KafkaConsumer('vehicle-topic-v1', **kafka_config)
    kafka_messages = []

    try:
        for message in consumer:
            kafka_messages.append(message.value)
            if len(kafka_messages) >= 100:  # Batch size limit
                break
    except Exception as e:
        logging.warning(f"Kafka consumer timeout or error: {e}")
    finally:
        consumer.close()

    logging.info(f"Extracted {len(kafka_messages)} messages from Kafka")

    # Extract lookup data from PostgreSQL OLTP
    oltp_hook = PostgresHook(postgres_conn_id='postgres_oltp')

    # Color lookup
    color_lookup = oltp_hook.get_records(
        "SELECT id, name FROM sc_vehicle_color_lk"
    )

    # MMCT lookup (Make, Model, Category, Type)
    mmct_lookup = oltp_hook.get_records(
        """SELECT mmct.category_id, mmct.type_id, t.name as type_name, m.name AS model_name
           FROM sc_vehicle_mmct_lk mmct
                    JOIN sc_vehicle_model_lk m ON m.id = mmct.model_id
                    JOIN sc_vehicle_type_lk as t on t.id = mmct.type_id"""
    )

    # Camera lookup
    camera_lookup = oltp_hook.get_records(
        """SELECT c.id as id, c.reference_id, z.id as zone_id, z.organization_id as tenant_id
           FROM sc_camera AS c
                    JOIN sc_zone as z on z.id = c.zone_id"""
    )

    # Store data in XCom for next task
    extracted_data = {
        'kafka_messages': kafka_messages,
        'lookups': {
            'colors': {row[1]: row[0] for row in color_lookup},  # name -> id
            'mmct': {row[3]: {'category_id': row[0], 'type_id': row[1], 'type_name': row[2]}
                     for row in mmct_lookup},  # model_name -> details
            'cameras': {row[1]: {'id': row[0], 'zone_id': row[2], 'tenant_id': row[3]}
                        for row in camera_lookup}  # reference_id -> details
        }
    }

    logging.info(
        f"Extracted lookup data: {len(color_lookup)} colors, {len(mmct_lookup)} mmct, {len(camera_lookup)} cameras")
    return extracted_data


def transform_normalize_events(**context):
    """
    Transform and normalize Kafka events with lookup enrichment
    """
    logging.info("Starting data transformation and normalization")

    # Get extracted data from previous task
    extracted_data = context['task_instance'].xcom_pull(task_ids='extract_task')
    kafka_messages = extracted_data['kafka_messages']
    lookups = extracted_data['lookups']

    normalized_events = []
    dlq_events = []

    for event in kafka_messages:
        try:
            normalized_event = normalize_event(event, lookups)

            # Data quality checks
            if is_valid_event(normalized_event):
                normalized_events.append(normalized_event)
            else:
                dlq_events.append({**event, 'error_reason': 'Failed data quality checks'})

        except Exception as e:
            logging.error(f"Error normalizing event: {e}")
            dlq_events.append({**event, 'error_reason': str(e)})

    logging.info(f"Normalized {len(normalized_events)} events, {len(dlq_events)} sent to DLQ")

    return {
        'normalized_events': normalized_events,
        'dlq_events': dlq_events
    }


def normalize_event(event, lookups):
    """
    Normalize a single event with enrichment data
    """
    normalized = {
        'record_id': str(uuid.uuid4()),
        'frame_time': datetime.fromtimestamp(event.get('timestamp', datetime.now().timestamp())),
    }

    # Handle vehicle bbox
    bbox = event.get('bbox', [])
    if len(bbox) == 4:
        normalized['vehicle_bbox'] = bbox
    else:
        normalized['vehicle_bbox'] = []

    # Handle license plate
    license_plate = event.get('license_plate', {})
    normalized['plate_text_latin'] = clean_text(license_plate.get('text', ''))
    normalized['plate_text_arabic'] = clean_text(license_plate.get('arabic_text', ''))
    normalized['plate_confidence'] = float(license_plate.get('confidence', 0.0))
    normalized['text_confidence'] = float(license_plate.get('text_confidence', 0.0))

    # Handle plate bbox
    plate_bbox = license_plate.get('bbox', [])
    if len(plate_bbox) == 4:
        normalized['plate_bbox'] = plate_bbox
    else:
        normalized['plate_bbox'] = []

    # Handle car make/model
    car_make = event.get('car_make', {})
    normalized['make'] = clean_text(car_make.get('make', ''))
    normalized['model'] = clean_text(car_make.get('model', ''))
    normalized['mmc_confidence'] = float(car_make.get('confidence', 0.0))

    # Color enrichment
    color_name = event.get('color', {}).get('name', '')
    normalized['color_name'] = color_name
    normalized['color_id'] = lookups['colors'].get(color_name, 0)
    if normalized['color_id'] == 0:
        normalized['color_name'] = 'undefined'

    # MMCT enrichment
    model_name = normalized['model']
    mmct_data = lookups['mmct'].get(model_name, {})
    if mmct_data and mmct_data.get('type_id') not in [6, 7, 8]:  # Exclude certain types
        normalized['type_id'] = mmct_data.get('type_id', 0)
        normalized['type_name'] = mmct_data.get('type_name', 'undefined')
        normalized['category_id'] = mmct_data.get('category_id', 0)
    else:
        normalized['type_id'] = 0
        normalized['type_name'] = 'undefined'
        normalized['category_id'] = 0

    # Camera enrichment
    camera_id = event.get('camera_id', '')
    camera_data = lookups['cameras'].get(camera_id.lower(), {})
    normalized['camera_id'] = camera_data.get('id', '')
    normalized['zone_id'] = camera_data.get('zone_id', '')
    normalized['tenant_id'] = camera_data.get('tenant_id', '')

    # Other fields
    normalized['dwell_time_seconds'] = event.get('dwell_time', 0)

    return normalized


def clean_text(text):
    """
    Clean and validate text fields
    """
    if not isinstance(text, str):
        return ''

    # Remove null bytes and invalid UTF-8
    cleaned = text.encode('utf-8', errors='replace').decode('utf-8', errors='replace')
    cleaned = cleaned.replace('\x00', '')
    cleaned = cleaned.strip()

    # Limit length to avoid PostgreSQL errors
    return cleaned[:255]


def is_valid_event(event):
    """
    Validate event data quality
    """
    # Check for required fields
    required_checks = [
        event.get('record_id'),
        event.get('make') or event.get('model') or event.get('plate_text_latin') or
        event.get('plate_text_arabic') or event.get('color_id', 0) > 0
    ]

    return all(required_checks)


def load_to_timescaledb(**context):
    """
    Load normalized events to TimescaleDB
    """
    logging.info("Starting data load to TimescaleDB")

    # Get transformed data from previous task
    transformed_data = context['task_instance'].xcom_pull(task_ids='transform_task')
    normalized_events = transformed_data['normalized_events']
    dlq_events = transformed_data['dlq_events']

    # Load to TimescaleDB
    olap_hook = PostgresHook(postgres_conn_id='postgres_olap')

    insert_query = """
                   INSERT INTO sc_vehicle (id, frame_time, type_name, type_id, color_id, color_name, \
                                           make, model, plate_text_latin, plate_text_arabic, \
                                           mmc_confidence, plate_confidence, text_confidence, \
                                           dwell_time_seconds, plate_bbox, vehicle_bbox, \
                                           category_id, zone_id, tenant_id, camera_id) \
                   VALUES (%(record_id)s::uuid, %(frame_time)s::timestamp with time zone, \
                           %(type_name)s, %(type_id)s::int, %(color_id)s::int, %(color_name)s, \
                           %(make)s, %(model)s, %(plate_text_latin)s, %(plate_text_arabic)s, \
                           %(mmc_confidence)s::float, %(plate_confidence)s::float, %(text_confidence)s::float, \
                           %(dwell_time_seconds)s::numeric, %(plate_bbox)s::int[], %(vehicle_bbox)s::int[], \
                           %(category_id)s::int, %(zone_id)s::uuid, %(tenant_id)s::uuid, %(camera_id)s::uuid) \
                   """

    # Batch insert normalized events
    if normalized_events:
        olap_hook.insert_rows(
            table='sc_vehicle',
            rows=[list(event.values()) for event in normalized_events],
            target_fields=list(normalized_events[0].keys()) if normalized_events else []
        )

    # Send DLQ events to Kafka DLQ topic (simplified - in real implementation use KafkaProducer)
    if dlq_events:
        logging.warning(f"Sending {len(dlq_events)} events to DLQ")
        # In a real implementation, you would send these to a Kafka DLQ topic

    logging.info(f"Successfully loaded {len(normalized_events)} events to TimescaleDB")

    return {
        'loaded_events': len(normalized_events),
        'dlq_events': len(dlq_events)
    }


# Define tasks
extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_kafka_and_db_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_normalize_events,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_to_timescaledb,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_task