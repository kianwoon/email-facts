# rag.py
import os
import duckdb
from fastapi import FastAPI

# Import PyIceberg components
# from pyiceberg.catalog import load_catalog # No longer needed
# from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError # Less relevant now
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.io import load_file_io
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import (
    ListType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
    BooleanType
)

app = FastAPI()

# --- PyIceberg Table Creation/Verification --- 
def ensure_iceberg_table():
    s3_endpoint = os.environ['AWS_S3_ENDPOINT']
    s3_bucket = os.environ["AWS_S3_BUCKET"]
    aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']

    # Define S3 File IO properties directly
    s3_io_props = {
        "s3.endpoint": s3_endpoint,
        "s3.access-key-id": aws_access_key_id,
        "s3.secret-access-key": aws_secret_access_key,
        # Add region if needed
        # "s3.region": "auto",
    }
    # Load the appropriate FileIO using properties
    print("Loading S3 FileIO...")
    s3_io = load_file_io(properties=s3_io_props)

    # Define the Iceberg schema using PyIceberg types
    # (Keeping the full schema definition)
    iceberg_schema = Schema(
        NestedField(1, "email_id", StringType(), required=False),
        NestedField(2, "message_id", StringType(), required=False),
        NestedField(3, "subject", StringType(), required=False),
        NestedField(4, "sender_name", StringType(), required=False),
        NestedField(5, "sender_address", StringType(), required=False),
        NestedField(6, "recipient_addresses", ListType(element_id=7, element_type=StringType(), element_required=False), required=False),
        NestedField(8, "cc_addresses", ListType(element_id=9, element_type=StringType(), element_required=False), required=False),
        NestedField(10, "received_at", TimestampType(), required=False),
        NestedField(11, "importance", StringType(), required=False),
        NestedField(12, "sensitivity_level", StringType(), required=False),
        NestedField(13, "assigned_department", StringType(), required=False),
        NestedField(14, "content_tags", ListType(element_id=15, element_type=StringType(), element_required=False), required=False),
        NestedField(16, "is_private_content", BooleanType(), required=False),
        NestedField(17, "detected_pii_types", ListType(element_id=18, element_type=StringType(), element_required=False), required=False),
        NestedField(19, "generated_summary", StringType(), required=False),
        NestedField(20, "extracted_key_points", ListType(element_id=21, element_type=StringType(), element_required=False), required=False),
        NestedField(22, "extracted_entities", StructType(
            NestedField(23, "persons", ListType(element_id=24, element_type=StringType(), element_required=False), required=False),
            NestedField(25, "organizations", ListType(element_id=26, element_type=StringType(), element_required=False), required=False),
            NestedField(27, "locations", ListType(element_id=28, element_type=StringType(), element_required=False), required=False),
        ), required=False),
        NestedField(29, "intent", StringType(), required=False),
        NestedField(30, "extraction_method", StringType(), required=False),
        NestedField(31, "extraction_timestamp", TimestampType(), required=False),
        schema_id=1
    )

    table_name = "email_facts"
    table_location = f"s3://{s3_bucket}/{table_name}"
    # Check for existence using a key metadata file path
    metadata_file_path = f"{table_location}/metadata/version-hint.text"

    print(f"Checking for Iceberg table at location: {table_location}")
    print(f"Using metadata check file: {metadata_file_path}")

    try:
        if not s3_io.exists(metadata_file_path):
            print(f"Table metadata not found. Attempting to create table...")
            # Note: Identifier is less critical without a catalog, location is key
            table = Table.create(
                identifier=(table_name,), # Simple identifier
                schema=iceberg_schema,
                location=table_location,
                io=s3_io
            )
            print(f"Table created successfully at {table_location}")
        else:
            print(f"Table metadata found. Loading existing table...")
            table = Table.load(location=table_location, io=s3_io)
            print(f"Table loaded successfully from {table_location}")
            # Optional schema check (remains the same)
            if str(table.schema().as_struct()) != str(iceberg_schema.as_struct()):
                 print("WARNING: Existing table schema does not match desired schema!")
            else:
                 print("Existing table schema matches.")

    except CommitFailedException as e:
        print(f"Error committing Iceberg transaction: {e}")
        # Handle potential concurrent modification issues if needed
        raise
    except Exception as e:
        print(f"Error creating/loading table '{table_name}': {e}")
        raise # Re-raise other critical errors

# Call the function to ensure the table exists before DuckDB setup
ensure_iceberg_table()

# --- DuckDB Connection and Querying --- 

# 1) Connect + install/load extensions (Keep this)
print("Connecting to DuckDB...")
con = duckdb.connect()
con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")
con.execute("INSTALL iceberg;")
con.execute("LOAD iceberg;")

# 2) Configure your R2 bucket over S3 API (Keep this)
print("Configuring DuckDB S3 access...")
con.execute(f"SET s3_endpoint='{os.environ['AWS_S3_ENDPOINT']}';")
con.execute("SET s3_region='auto';") # Keep 'auto' here for DuckDB if it works
con.execute(f"SET s3_access_key_id='{os.environ['AWS_ACCESS_KEY_ID']}';")
con.execute(f"SET s3_secret_access_key='{os.environ['AWS_SECRET_ACCESS_KEY']}';")
con.execute("SET s3_use_ssl=true;")

# 3) Create the table (REMOVE THIS SECTION - PyIceberg handles it now)
# # Build the SQL string without f-string for main part
# s3_bucket = os.environ["AWS_S3_BUCKET"]
# sql_base = """ ... """
# sql_location = f"LOCATION 's3://{s3_bucket}/email_facts'"
# create_table_sql = sql_base + sql_location
# 
# # Print the SQL for debugging
# print("--- Executing SQL ---")
# print(create_table_sql)
# print("---------------------")
# # Execute the constructed SQL string
# con.execute(create_table_sql) # <<< REMOVED


@app.get("/")
def health():
    # Now query the table managed by Iceberg
    # DuckDB might need the full S3 path or catalog identifier depending on version/setup
    # Try querying via S3 path first, assuming DuckDB's iceberg ext can find it
    table_identifier_for_query = f"'s3://{os.environ['AWS_S3_BUCKET']}/email_facts'"
    print(f"Querying count from {table_identifier_for_query}...")
    try:
        cnt = con.execute(f"SELECT count(*) FROM iceberg_scan({table_identifier_for_query})").fetchone()[0]
        print(f"Row count: {cnt}")
        return {"status": "OK", "rows": cnt}
    except Exception as e:
        print(f"Error querying table: {e}")
        return {"status": "ERROR", "error": str(e)}