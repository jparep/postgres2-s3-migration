import pandas as pd
import pyarrow.parquet as pq

# Load and check the first few rows of the Parquet file from S3 (if downloaded locally)
file_path = "/tmp/patients_part0.parquet"  # Update this path if needed

# Load the Parquet file
table = pq.read_table(file_path)
df = table.to_pandas()

# Print the first few rows
print(df.head())

# Print column names
print(table.column_names)
print(table.schema)

