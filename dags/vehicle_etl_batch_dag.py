import io
import json
import logging
import uuid
from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from kafka import KafkaConsumer
from airflow.models import Variable
from kafka.producer.kafka import KafkaProducer
import csv

# Default arguments for the DAG
default_args = {
    'owner': 'JavokhirAbdullaev',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'vehicle_data_enrichment',
    default_args=default_args,
    description='Extract vehicle data from Kafka, enrich with PostgreSQL lookups, and load to TimescaleDB',
    schedule_interval=timedelta(minutes=1),
    catchup=False,
    tags=['vehicle', 'kafka', 'enrichment', 'timescaledb']
)


def extract_kafka_data(**context):
    """
    Extract data from Kafka topic and PostgreSQL lookup tables
    """
    logging.info("Starting data extraction from Kafka and PostgreSQL")

    # Fetch Kafka connection from Airflow
    kafka_conn = BaseHook.get_connection('kafka_conn')
    kafka_config = {
        'bootstrap_servers': [f"{kafka_conn.host}:{kafka_conn.port}"],
        'group_id': 'airflow_enrichment',
        'auto_offset_reset': 'earliest',
        'value_deserializer': lambda x: json.loads(x.decode('utf-8')),
        'consumer_timeout_ms': 30000  # 30 seconds timeout
    }
    # Add extra config if present
    if kafka_conn.extra_dejson.get('security_protocol'):
        kafka_config['security_protocol'] = kafka_conn.extra_dejson['security_protocol']

    # Extract from Kafka
    consumer = KafkaConsumer('vehicle-topic-v1', **kafka_config)
    kafka_messages = []

    try:
        for message in consumer:
            print("retrieved: " + message.value['camera_id'])
            kafka_messages.append(message.value)
            if len(kafka_messages) >= 1000:  # Batch size limit
                break
    except Exception as e:
        logging.warning(f"Kafka consumer timeout or error: {e}")
    finally:
        consumer.close()

    logging.info(f"Extracted {len(kafka_messages)} messages from Kafka")

    return {'kafka_messages': kafka_messages}


def transform_normalize_events(**context):
    """
    Transform and normalize Kafka events with lookup enrichment
    """
    logging.info("Starting data transformation and normalization")

    # Get extracted data from previous task
    extracted_data = context['task_instance'].xcom_pull(task_ids='extract_task')
    kafka_messages = extracted_data['kafka_messages']
    lookups = json.loads(Variable.get("vehicle_lookups", deserialize_json=True))

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
        'record_id': str(uuid.uuid4())
    }

    # Handle timestamp robustly
    timestamp = event.get('timestamp')
    try:
        if isinstance(timestamp, (int, float)):
            normalized['frame_time'] = datetime.fromtimestamp(timestamp)
        elif isinstance(timestamp, str):
            normalized['frame_time'] = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        else:
            raise ValueError(f"Invalid timestamp format: {timestamp}")
    except (ValueError, TypeError) as e:
        logging.warning(f"Invalid timestamp in event: {timestamp}, using current time")
        normalized['frame_time'] = datetime.now()
        raise ValueError(f"Invalid timestamp: {str(e)}")  # Send to DLQ

    # Handle vehicle bbox
    bbox = event.get('bbox') or []
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
    plate_bbox = license_plate.get('bbox') or []
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
    cleaned = cleaned.replace('\x00', '').strip()

    # Limit length to avoid PostgreSQL errors
    return cleaned[:255]


def is_valid_event(event):
    """
    Validate event data quality
    """
    # Check for required fields
    required_checks = [
        event.get('record_id'),
        isinstance(event['frame_time'], datetime),  # Ensure valid timestamp
        event.get('make') or event.get('model') or event.get('plate_text_latin') or
        event.get('plate_text_arabic') or event.get('color_id', 0) > 0
    ]

    return all(required_checks)

def to_pg_array(arr):
    """Convert a list to PostgreSQL array format"""
    return "{" + ",".join(str(x) for x in arr) + "}" if arr else "{}"

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

    # Batch insert normalized events
    if normalized_events:
        csv_buffer = io.StringIO()
        writer = csv.writer(csv_buffer, quoting=csv.QUOTE_MINIMAL)
        for event in normalized_events:
            row = [
                event['record_id'],
                event['frame_time'].isoformat(), # Always a datetime, ensured by normalize_event
                event['type_name'],
                event['type_id'],
                event['color_id'],
                event['color_name'],
                event['make'],
                event['model'],
                event['plate_text_latin'],
                event['plate_text_arabic'],
                event['mmc_confidence'],
                event['plate_confidence'],
                event['text_confidence'],
                event['dwell_time_seconds'],
                to_pg_array(event['plate_bbox']),
                to_pg_array(event['vehicle_bbox']),
                event['category_id'],
                event['zone_id'],
                event['tenant_id'],
                event['camera_id'],
            ]
            writer.writerow(row)
        csv_buffer.seek(0)
        conn = olap_hook.get_conn()
        cursor = conn.cursor()
        try:
            cursor.copy_expert("COPY sc_vehicle (id, frame_time, type_name, type_id, color_id, color_name, make, model, plate_text_latin, plate_text_arabic, mmc_confidence, plate_confidence, text_confidence, dwell_time_seconds, plate_bbox, vehicle_bbox, category_id, zone_id, tenant_id, camera_id) FROM STDIN WITH CSV", csv_buffer)
            conn.commit()
        except Exception as e:
            logging.error(f"Error during COPY: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    # Send DLQ events to Kafka DLQ topic (simplified - in real implementation use KafkaProducer)
    if dlq_events:
        logging.warning(f"Sending {len(dlq_events)} events to DLQ")
        kafka_conn = BaseHook.get_connection('kafka_conn')
        producer = KafkaProducer(bootstrap_servers=[f"{kafka_conn.host}:{kafka_conn.port}"])
        for event in dlq_events:
            producer.send('vehicle-dlq-v1', json.dumps(event).encode('utf-8'))
        producer.flush()

    logging.info(f"Successfully loaded {len(normalized_events)} events to TimescaleDB")

    return {
        'loaded_events': len(normalized_events),
        'dlq_events': len(dlq_events)
    }


# Define tasks

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_kafka_data,
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