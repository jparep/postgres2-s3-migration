import os
import pandas as pd
import psycopg2
import boto3
from dotenv import load_dotenv
from datetime import datetime

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
current_date = datetime.now().strftime("%Y/%m/%d")  
S3_PREFIX = f"health-data/{current_date}/"

# Define tables to migrate
tables = ["patients", "medical_records", "billing"]

# Function to Upload Files to S3
def upload_to_s3(file_path, bucket, s3_key):
    s3.upload_file(file_path, bucket, s3_key)
    print(f"Uploaded {file_path} to s3://{bucket}/{s3_key}")

# Extract and Save as CSV
for table in tables:
    print(f"Extracting {table}...")

    query = f"SELECT * FROM {table};"
    df = pd.read_sql_query(query, conn)

    # Save as CSV
    file_name = f"{table}.csv"
    file_path = f"/tmp/{file_name}"
    df.to_csv(file_path, index=False)

    # Upload to S3
    s3_key = f"{S3_PREFIX}{table}/{file_name}"
    upload_to_s3(file_path, S3_BUCKET, s3_key)

# Cleanup
cursor.close()
conn.close()
print("CSV Migration completed successfully!")
