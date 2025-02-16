import psycopg2
import boto3
import os
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from dotenv import load_dotenv
from datetime import datetime
import logging
import botocore.exceptions

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Database Connection
conn = psycopg2.connect(
    host=os.getenv("ONPREM_DB_HOST"),
    port=os.getenv("ONPREM_DB_PORT"),
    user=os.getenv("ONPREM_DB_USER"),
    password=os.getenv("ONPREM_DB_PASSWORD"),
    dbname=os.getenv("ONPREM_DB_NAME"),
    options="-c search_path=vital_health_db"
)
cursor = conn.cursor()

# AWS S3 Configuration
S3_BUCKET = os.getenv("S3_BUCKET")
AWS_REGION = os.getenv("AWS_REGION")

# Initialize S3 client
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
    region_name=AWS_REGION
)

# Partitioned S3 Prefix (Organized by Date)
current_date = datetime.now().strftime("%Y/%m/%d")  # Example: 2025/02/14
S3_PREFIX = f"health-data/{current_date}/"

# Define tables to migrate
tables = ["patients", "medical_records", "billing"]

######################################################################
# DELETE EXISTIGN FILES IN S3 BEFORE UPLOADING NEW FILES
######################################################################
def clean_s3_directory(bucket, prefix):
    """Deletes all objects in an S3 directory before re-uploading"""
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" in response:
        for obj in response["Contents"]:
            s3.delete_object(Bucket=bucket, Key=obj["Key"])
        print(f"Deleted all files in {bucket}/{prefix}")

# Run this before uploading new files
clean_s3_directory(S3_BUCKET, S3_PREFIX)
######################################################################

# Function to Upload Files to S3 with Error Handling
def upload_to_s3(file_path, bucket, s3_key):
    try:
        s3.upload_file(file_path, bucket, s3_key)
        logger.info(f"Uploaded {file_path} to s3://{bucket}/{s3_key}")
    except botocore.exceptions.BotoCoreError as e:
        logger.error(f"Upload failed: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

# Process Each Table
for table in tables:
    logger.info(f"Extracting {table}...")

    # Read table in chunks (Prevents memory overload)
    query = f"SELECT * FROM vital_health_db.{table};"  # Explicitly reference the schema
    chunksize = 50  # Fetch 50000 rows at a time

    try:
        data_chunks = pd.read_sql_query(query, conn, chunksize=chunksize)

        for idx, chunk in enumerate(data_chunks):
            file_name = f"{table}_part{idx}.parquet"
            file_path = f"/tmp/{file_name}"  # Store temporarily before upload

            # Convert DataFrame to Parquet with optimized compression
            table_schema = pa.Table.from_pandas(chunk)
            #pq.write_table(table_schema, file_path, compression="ZSTD")  # ZSTD saves space
            pq.write_table(table_schema, file_path, compression="SNAPPY")  # Change ZSTD to SNAPPY


            # Upload to S3
            s3_key = f"{S3_PREFIX}{table}/{file_name}"
            upload_to_s3(file_path, S3_BUCKET, s3_key)

    except Exception as e:
        logger.error(f"Failed to extract data from {table}: {e}")

# Cleanup
cursor.close()
conn.close()
logger.info("Migration completed successfully!")
