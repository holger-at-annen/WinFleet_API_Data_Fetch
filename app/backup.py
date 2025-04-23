import os
import logging
import time
from datetime import datetime, timedelta
import shutil

# Global state for backup status
last_backup_success = False
last_backup_time = None

logger = logging.getLogger(__name__)

def create_backup(backup_file):
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        try:
            # Check if backup directory is writable
            backup_dir = os.path.dirname(backup_file)
            if not os.access(backup_dir, os.W_OK):
                raise OSError(f"Backup directory {backup_dir} is not writable")

            # Environment variables for pg_dump
            POSTGRES_USER = os.getenv('POSTGRES_USER', 'dbuser')
            POSTGRES_DB = os.getenv('POSTGRES_DB', 'apidata')
            POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'db')

            # Run pg_dump
            cmd = f"pg_dump -U {POSTGRES_USER} -h {POSTGRES_HOST} {POSTGRES_DB} > {backup_file}"
            result = os.system(cmd)
            if result != 0:
                raise RuntimeError(f"pg_dump failed with exit code {result}")

            # Verify file was created and is not empty
            if not os.path.exists(backup_file):
                raise FileNotFoundError(f"Backup file {backup_file} was not created")
            if os.path.getsize(backup_file) == 0:
                raise ValueError(f"Backup file {backup_file} is empty")

            logger.info(f"Backup created: {backup_file}")
            return True
        except (OSError, RuntimeError, FileNotFoundError, ValueError) as e:
            logger.error(f"Backup attempt {attempt}/{max_attempts} failed: {e}")
            if attempt < max_attempts:
                wait_time = 2 ** attempt
                logger.info(f"Retrying after {wait_time} seconds")
                time.sleep(wait_time)
            else:
                logger.error(f"All {max_attempts} backup attempts failed for {backup_file}")
                return False
        except Exception as e:
            logger.error(f"Unexpected error during backup attempt {attempt}/{max_attempts}: {e}")
            return False

def manage_daily_backups():
    global last_backup_success, last_backup_time
    try:
        # Ensure backup directories exist
        for dir in ['daily', 'weekly']:
            dir_path = f"/backups/{dir}"
            os.makedirs(dir_path, exist_ok=True)
            if not os.access(dir_path, os.W_OK):
                raise OSError(f"Directory {dir_path} is not writable")

        today = datetime.now()
        is_sunday = today.weekday() == 6
        daily_backup = f"/backups/daily/apidata_{today.strftime('%Y-%m-%d')}.sql"

        if not create_backup(daily_backup):
            last_backup_success = False
            last_backup_time = today
            logger.error("Skipping weekly backup due to daily backup failure")
            return

        last_backup_success = True
        last_backup_time = today

        daily_files = sorted(
            [f for f in os.listdir("/backups/daily") if f.startswith("apidata_") and f.endswith(".sql")],
            key=lambda x: datetime.strptime(x.split("_")[1].split(".")[0], "%Y-%m-%d")
        )
        if len(daily_files) > 7:
            oldest = daily_files[0]
            try:
                os.remove(f"/backups/daily/{oldest}")
                logger.info(f"Removed oldest daily backup: {oldest}")
            except OSError as e:
                logger.error(f"Failed to remove daily backup {oldest}: {e}")

        if is_sunday:
            weekly_backup = f"/backups/weekly/apidata_weekly_{today.strftime('%Y-%m-%d')}.sql"
            try:
                shutil.copy(daily_backup, weekly_backup)
                logger.info(f"Created weekly backup: {weekly_backup}")
            except (OSError, shutil.Error) as e:
                logger.error(f"Failed to create weekly backup {weekly_backup}: {e}")
                return

            weekly_files = sorted(
                [f for f in os.listdir("/backups/weekly") if f.startswith("apidata_weekly_") and f.endswith(".sql")],
                key=lambda x: datetime.strptime(x.split("_")[2].split(".")[0], "%Y-%m-%d")
            )
            if len(weekly_files) > 4:
                oldest = weekly_files[0]
                try:
                    os.remove(f"/backups/weekly/{oldest}")
                    logger.info(f"Removed oldest weekly backup: {oldest}")
                except OSError as e:
                    logger.error(f"Failed to remove weekly backup {oldest}: {e}")

    except Exception as e:
        logger.error(f"Daily backup management failed: {e}")
        last_backup_success = False
        last_backup_time = datetime.now()

def manage_monthly_backups():
    global last_backup_success, last_backup_time
    try:
        dir_path = f"/backups/monthly"
        os.makedirs(dir_path, exist_ok=True)
        if not os.access(dir_path, os.W_OK):
            raise OSError(f"Directory {dir_path} is not writable")

        today = datetime.now()
        monthly_backup = f"/backups/monthly/apidata_monthly_{today.strftime('%Y-%m-%d')}.sql"

        if not create_backup(monthly_backup):
            last_backup_success = False
            last_backup_time = today
            logger.error("Monthly backup failed")
            return

        last_backup_success = True
        last_backup_time = today

        monthly_files = sorted(
            [f for f in os.listdir("/backups/monthly") if f.startswith("apidata_monthly_") and f.endswith(".sql")],
            key=lambda x: datetime.strptime(x.split("_")[2].split(".")[0], "%Y-%m-%d")
        )
        if len(monthly_files) > 12:
            oldest = monthly_files[0]
            try:
                os.remove(f"/backups/monthly/{oldest}")
                logger.info(f"Removed oldest monthly backup: {oldest}")
            except OSError as e:
                logger.error(f"Failed to remove monthly backup {oldest}: {e}")

    except Exception as e:
        logger.error(f"Monthly backup management failed: {e}")
        last_backup_success = False
        last_backup_time = datetime.now()

def manage_annual_backups():
    global last_backup_success, last_backup_time
    try:
        dir_path = f"/backups/annual"
        os.makedirs(dir_path, exist_ok=True)
        if not os.access(dir_path, os.W_OK):
            raise OSError(f"Directory {dir_path} is not writable")

        today = datetime.now()
        annual_backup = f"/backups/annual/apidata_annual_{today.strftime('%Y-%m-%d')}.sql"

        if not create_backup(annual_backup):
            last_backup_success = False
            last_backup_time = today
            logger.error("Annual backup failed")
            return

        last_backup_success = True
        last_backup_time = today

    except Exception as e:
        logger.error(f"Annual backup management failed: {e}")
        last_backup_success = False
        last_backup_time = datetime.now()