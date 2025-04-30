import requests
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.pool import SimpleConnectionPool
import time
import os
import logging
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from sqlalchemy import create_engine
import uvicorn
import asyncio
from fastapi import FastAPI
from logging_config import setup_logging
from log_cleanup import cleanup_old_logs
from partition_handler import handle_missing_partition_error, create_future_partitions

# Configure logging
logger = setup_logging()

# Environment variables
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'db')
POSTGRES_USER = os.getenv('POSTGRES_USER')  # Remove default value
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')  # Remove default value
POSTGRES_DB = os.getenv('POSTGRES_DB')  # Remove default value
API_BASE_URL = os.getenv('API_BASE_URL', 'https://api.winfleet.lu')
API_USERNAME = os.getenv('API_USERNAME', 'your_username')
API_PASSWORD = os.getenv('API_PASSWORD', 'your_password')
FETCH_INTERVAL = int(os.getenv('FETCH_INTERVAL', 60))  # Default to 60 seconds
API_PORT = int(os.getenv('API_PORT', 8000))

# Rate limit configuration
MAX_REQUESTS_PER_MINUTE = 4  # API limit: 4 requests per minute
TARGET_REQUESTS_PER_MINUTE = 1  # Target: 1 request per minute
MIN_INTERVAL_SECONDS = 60 // TARGET_REQUESTS_PER_MINUTE  # 60 seconds

# Global state for health check, rate limiting, and backups
last_job_success = False
last_job_time = None
rate_limit_wait = 0
request_count = 0
window_start = time.time()

# Connection pool
db_pool = None

def create_session():
    session = requests.Session()
    session.headers.update({'User-Agent': 'DataCollector/1.0'})
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=['GET', 'POST']
    )
    session.mount('http://', HTTPAdapter(max_retries=retries))
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

def init_db():
    global db_pool
    max_attempts = 30
    attempt = 0
    while attempt < max_attempts:
        try:
            if not all([POSTGRES_HOST, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB]):
                raise ValueError("Missing required database environment variables")
                
            logger.info(f"Initializing database connection pool: host={POSTGRES_HOST}, user={POSTGRES_USER}, database={POSTGRES_DB}")
            db_pool = SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                host=POSTGRES_HOST,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                database=POSTGRES_DB
            )
            logger.info("Database connection pool initialized successfully")
            return
        except psycopg2.OperationalError as e:
            attempt += 1
            if attempt < max_attempts:
                wait_time = min(2 ** attempt, 30)  # Cap wait time at 30 seconds
                logger.warning(f"Database connection attempt {attempt}/{max_attempts} failed. Waiting {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(f"Failed to initialize database connection pool after {max_attempts} attempts: {e}")
                raise
        except Exception as e:
            logger.error(f"Failed to initialize database connection pool: {e}")
            raise

def check_rate_limits(response):
    global rate_limit_wait, request_count, window_start
    current_time = time.time()
    
    # Reset counter if minute window has passed
    if current_time - window_start >= 60:
        request_count = 0
        window_start = current_time
    
    request_count += 1
    
    # Add delay between requests even within limits
    if request_count > 1:
        time.sleep(15)  # Minimum 15 second gap between requests
    
    # If we're approaching limit, add additional wait time
    if request_count >= MAX_REQUESTS_PER_MINUTE:
        wait_time = 60 - (current_time - window_start)
        if wait_time > 0:
            logger.warning(f"Approaching rate limit. Waiting {wait_time:.2f} seconds")
            time.sleep(wait_time)
            window_start = time.time()
            request_count = 0

    return rate_limit_wait

def get_access_token(session):
    """Authenticate with Winfleet API and retrieve an access token."""
    login_url = f"{API_BASE_URL}/login"
    payload = {
        "username": API_USERNAME,
        "password": API_PASSWORD
    }
    headers = {
        "Content-Type": "application/json"
    }
    
    try:
        response = session.post(login_url, json=payload, headers=headers)
        response.raise_for_status()
        check_rate_limits(response)
        token_data = response.json()
        return token_data.get("token") or token_data.get("access_token")
    except requests.exceptions.RequestException as e:
        logger.error(f"Authentication failed: {e}")
        return None

def get_assets(session, token):
    assets_url = f"{API_BASE_URL}/v1/assets/"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    try:
        response = session.get(assets_url, headers=headers)
        response.raise_for_status()
        check_rate_limits(response)
        assets_data = response.json()
        logger.debug(f"Raw assets data: {assets_data}")  # Log raw API response
        return assets_data
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to retrieve assets data: {e}")
        return None

def prepare_vehicle_status_data(json_data):
    """
    Prepares vehicle status data for database insertion.
    Only includes status records with id:0 and id:1 from each asset's statusList.
    """
    prepared_data = []
    
    for vehicle in json_data:
        try:
            # Check for missing or invalid fields
            required_fields = ['id', 'name', 'plate_number', 'vin', 'statusList']
            missing_fields = [field for field in required_fields if field not in vehicle or vehicle[field] is None]
            if missing_fields:
                logger.error(f"Vehicle missing required fields {missing_fields}: {vehicle}")
                continue
                
            base_data = {
                'asset_id': vehicle['id'],
                'name': vehicle['name'],
                'plate_number': vehicle['plate_number'],
                'vin': vehicle['vin']
            }
            
            if not isinstance(vehicle['statusList'], list):
                logger.error(f"Invalid statusList for vehicle {vehicle['id']}: {vehicle['statusList']}")
                continue
        
            for status in vehicle['statusList']:
                if status['id'] in [0, 1]:
                    try:
                        # Validate status fields
                        if not all(key in status for key in ['id', 'position', 'status_text']):
                            logger.error(f"Status missing required fields for vehicle {vehicle['id']}: {status}")
                            continue
                            
                        if not all(key in status['position'] for key in ['txDateTime', 'description', 'coordinates']):
                            logger.error(f"Position missing required fields for vehicle {vehicle['id']}: {status['position']}")
                            continue
                            
                        if not all(key in status['position']['coordinates'] for key in ['latitude', 'longitude']):
                            logger.error(f"Coordinates missing required fields for vehicle {vehicle['id']}: {status['position']['coordinates']}")
                            continue

                        
                        # Parse the incoming date format (e.g., 2025-04-28T05:59:04) for timestamptz
                        event_time = datetime.strptime(status['position']['txDateTime'], '%Y-%m-%dT%H:%M:%S')
                        prepared_data.append({
                            **base_data,
                            'position_description': status['position']['description'],
                            'event_time': event_time,  # timestamptz-compatible datetime object
                            'latitude': status['position']['coordinates']['latitude'],
                            'longitude': status['position']['coordinates']['longitude'],
                            'status_text': status['status_text']
                        })
                    except (KeyError, ValueError) as e:
                        logger.error(f"Unexpected error processing vehicle {vehicle.get('id', 'unknown')}: {e}")
                        logger.error(f"Problematic vehicle data: {vehicle}")
                        continue

    logger.info(f"Prepared {len(prepared_data)} records from {len(json_data)} vehicles")
    return prepared_data

def store_vehicle_status_data(prepared_data):
    """Store prepared vehicle status data in the database."""
    if not prepared_data:
        logger.info("No data to store")
        return True

    conn = db_pool.getconn()
    try:
        with conn.cursor() as cursor:
            values = [
                (
                    item['asset_id'],
                    item['name'],
                    item['plate_number'],
                    item['vin'],
                    item['position_description'],
                    item['event_time'],
                    item['latitude'],
                    item['longitude'],
                    item['status_text']
                )
                for item in prepared_data
            ]

            # Attempt batch insert
            try:
                execute_values(
                    cursor,
                    """
                    INSERT INTO posts (
                        asset_id, name, plate_number, vin, position_description,
                        event_time, latitude, longitude, status_text
                    )
                    VALUES %s
                    ON CONFLICT ON CONSTRAINT posts_pkey DO UPDATE
                    SET
                        name = EXCLUDED.name,
                        plate_number = EXCLUDED.plate_number,
                        vin = EXCLUDED.vin,
                        position_description = EXCLUDED.position_description,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        status_text = EXCLUDED.status_text
                    """,
                    values
                )
                conn.commit()
                logger.info(f"Inserted/Updated {len(values)} vehicle status records in batch")
                return True
            except psycopg2.Error as e:
                conn.rollback()
                logger.warning(f"Batch insert failed: {e}. Falling back to row-by-row processing")
                
                # Row-by-row processing
                failed_rows = []
                for i, item in enumerate(prepared_data):
                    try:
                        cursor.execute(
                            """
                            INSERT INTO posts (
                                asset_id, name, plate_number, vin, position_description,
                                event_time, latitude, longitude, status_text
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT ON CONSTRAINT posts_pkey DO UPDATE
                            SET
                                name = EXCLUDED.name,
                                plate_number = EXCLUDED.plate_number,
                                vin = EXCLUDED.vin,
                                position_description = EXCLUDED.position_description,
                                latitude = EXCLUDED.latitude,
                                longitude = EXCLUDED.longitude,
                                status_text = EXCLUDED.status_text
                            """,
                            (
                                item['asset_id'],
                                item['name'],
                                item['plate_number'],
                                item['vin'],
                                item['position_description'],
                                item['event_time'],
                                item['latitude'],
                                item['longitude'],
                                item['status_text']
                            )
                        )
                        conn.commit()
                    except psycopg2.Error as row_e:
                        conn.rollback()
                        logger.error(f"Error storing row {i+1} with asset_id {item['asset_id']}: {row_e}")
                        logger.error(f"Problematic row data: {item}")
                        failed_rows.append(item)
                    except Exception as row_e:
                        conn.rollback()
                        logger.error(f"Unexpected error storing row {i+1} with asset_id {item['asset_id']}: {row_e}")
                        logger.error(f"Problematic row data: {item}")
                        failed_rows.append(item)

                if failed_rows:
                    logger.warning(f"Failed to store {len(failed_rows)} rows out of {len(prepared_data)}")
                    return False
                logger.info(f"Successfully stored {len(prepared_data)} rows individually")
                return True

            except psycopg2.Error as e:
                conn.rollback()
                if "no partition of relation" in str(e):
                    if handle_missing_partition_error(conn, str(e)):
                        # Retry the batch insert after creating the partition
                        return store_vehicle_status_data(prepared_data)
                logger.error(f"Database error while storing data: {e}")
                return False
    except Exception as e:
        logger.error(f"Unexpected error while storing data: {e}")
        conn.rollback()
        return False
    finally:
        db_pool.putconn(conn)

def fetch_and_store(session):
    global last_job_success, last_job_time, rate_limit_wait
    if rate_limit_wait > 0:
        logger.info(f"Rate limit wait active: {rate_limit_wait} seconds remaining")
        time.sleep(rate_limit_wait)
        rate_limit_wait = 0

    attempts = 0
    max_attempts = 3
    success = False
    assets_data = None  # Store the fetched data
    
    while attempts < max_attempts and not success:
        attempts += 1
        logger.info(f"Attempt {attempts} of {max_attempts}")
        try:
            # Only fetch data if we don't have it yet
            if assets_data is None:
                token = get_access_token(session)
                if not token:
                    logger.error("Failed to obtain access token")
                    if attempts < max_attempts:
                        wait_time = 2 ** attempts
                        logger.info(f"Waiting {wait_time} seconds before retry")
                        time.sleep(wait_time)
                    continue

                assets_data = get_assets(session, token)
                if not assets_data:
                    logger.error("Failed to fetch assets data")
                    if attempts < max_attempts:
                        wait_time = 2 ** attempts
                        logger.info(f"Waiting {wait_time} seconds before retry")
                        time.sleep(wait_time)
                    continue

            # Only prepare and store if we have data
            if assets_data:
                prepared_data = prepare_vehicle_status_data(assets_data)
                if not prepared_data:
                    logger.info("No valid data to store after preparation")
                    success = True
                    break

                if store_vehicle_status_data(prepared_data):
                    success = True
                    last_job_success = True
                    last_job_time = datetime.now()
                else:
                    logger.error("Failed to store data")
                    if attempts < max_attempts:
                        wait_time = 2 ** attempts
                        logger.info(f"Waiting {wait_time} seconds before retry")
                        time.sleep(wait_time)

        except Exception as e:
            logger.error(f"Unexpected error in fetch_and_store: {e}")
            logger.error(f"Assets data at time of error: {assets_data}")
            if attempts < max_attempts:
                wait_time = 2 ** attempts
                logger.info(f"Waiting {wait_time} seconds before retry")
                time.sleep(wait_time)
    
    if not success:
        logger.warning(f"All {max_attempts} attempts failed. Will try again at next scheduled interval")
        last_job_success = False
        last_job_time = datetime.now()

def maintenance_task():
    conn = db_pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute("VACUUM ANALYZE posts")
        cursor.execute("REINDEX TABLE posts CONCURRENTLY")
        conn.commit()
        logger.info("Maintenance tasks completed")
    except Exception as e:
        logger.error(f"Maintenance failed: {e}")
    finally:
        db_pool.putconn(conn)

fastapi_app = FastAPI()

@fastapi_app.get("/health")
async def health_check():
    status = "healthy" if last_job_success else "unhealthy"
    last_run = last_job_time.isoformat() if last_job_time else "never"
    
    backup_status = "unhealthy"
    last_backup = "never"
    
    # Check Docker backup volume
    backup_path = "/app/backups"
    try:
        if os.path.exists(backup_path):
            backup_files = [f for f in os.listdir(backup_path) if f.endswith('.sql')]
            if backup_files:
                latest_backup = max(backup_files, key=lambda f: os.path.getmtime(os.path.join(backup_path, f)))
                backup_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(os.path.join(backup_path, latest_backup)))
                backup_status = "healthy" if backup_age.days < 2 else "unhealthy"
                last_backup = datetime.fromtimestamp(os.path.getmtime(os.path.join(backup_path, latest_backup))).isoformat()
    except Exception as e:
        logger.error(f"Error checking backup status: {e}")
        last_backup = "error"

    return {
        "status": status,
        "last_job_time": last_run,
        "rate_limit_wait": rate_limit_wait,
        "requests_in_current_minute": request_count,
        "backup_status": backup_status,
        "last_backup_time": last_backup
    }

async def run_fastapi():
    config = uvicorn.Config(fastapi_app, host="0.0.0.0", port=API_PORT, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()

def main():
    init_db()
    session = create_session()
    
    db_url = f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DB}'
    jobstores = {
        'default': SQLAlchemyJobStore(url=db_url)
    }
    scheduler = BackgroundScheduler(jobstores=jobstores)
    scheduler.add_job(
        fetch_and_store,
        trigger=IntervalTrigger(seconds=max(FETCH_INTERVAL, MIN_INTERVAL_SECONDS)),
        args=[session],
        id='fetch_job',
        name='Fetch and store API data',
        replace_existing=True
    )
    scheduler.add_job(
        maintenance_task,
        trigger=IntervalTrigger(days=7),
        id='maintenance_job',
        name='Weekly database maintenance',
        replace_existing=True
    )
    scheduler.add_job(
        cleanup_old_logs,
        trigger=IntervalTrigger(days=1),
        id='log_cleanup_job',
        name='Daily log cleanup',
        replace_existing=True
    )
    scheduler.add_job(
        create_future_partitions,
        trigger=IntervalTrigger(days=7),
        id='partition_creation_job',
        name='Create future partitions',
        replace_existing=True,
        executor='default',
        misfire_grace_time=3600
    )
    
    try:
        scheduler.start()
        logger.info(f"Scheduler started. Fetching every {max(FETCH_INTERVAL, MIN_INTERVAL_SECONDS)} seconds")
        asyncio.run(run_fastapi())
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logger.info("Scheduler shut down gracefully")
    finally:
        if db_pool:
            db_pool.closeall()

if __name__ == "__main__":
    main()
