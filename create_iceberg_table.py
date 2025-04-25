# create_iceberg_table.py
import os
import uuid
from dotenv import load_dotenv
from pyiceberg.io import load_file_io
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType # Import other types as needed
from pyiceberg.table import PartitionSpec, SortOrder, UNPARTITIONED_PARTITION_SPEC, UNSORTED_SORT_ORDER
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.serializers import ToOutputFile
from pyiceberg.exceptions import CommitFailedException

# Load environment variables from .env file (same as rag.py)
load_dotenv()

# Configuration (Copied/adapted from rag.py)
R2_ENDPOINT_URL = os.getenv("AWS_S3_ENDPOINT")
R2_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = os.getenv("AWS_S3_BUCKET")
ICEBERG_CATALOG_NAME = "my_iceberg_data" # Consistent logical name
ICEBERG_WAREHOUSE_PATH = f"s3://{BUCKET_NAME}/{ICEBERG_CATALOG_NAME}"

# Define the target table name
TABLE_NAME = "email_facts"

# Define the Iceberg table schema (Copied from rag.py)
# Ensure this EXACTLY matches the schema in rag.py
iceberg_schema = Schema(
    NestedField(field_id=1, name="id", field_type=StringType(), required=True),
    NestedField(field_id=2, name="fact", field_type=StringType(), required=True),
    NestedField(field_id=3, name="source", field_type=StringType(), required=False),
    schema_id=1, # Ensure schema ID matches rag.py if you check it there
    identifier_field_ids=[1]
)

def create_table():
    """Creates the initial Iceberg table structure and metadata files on S3/R2."""
    print(f"Attempting to create Iceberg table '{TABLE_NAME}' structure in warehouse '{ICEBERG_WAREHOUSE_PATH}'...")

    s3_properties = {
        "s3.endpoint": R2_ENDPOINT_URL,
        "s3.access-key-id": R2_ACCESS_KEY_ID,
        "s3.secret-access-key": R2_SECRET_ACCESS_KEY,
        "warehouse": ICEBERG_WAREHOUSE_PATH, # Provide warehouse context
    }

    s3_io = None
    try:
        s3_io = load_file_io(properties=s3_properties)
        print("S3 FileIO loaded successfully.")
    except Exception as e:
        print(f"FATAL: Error loading S3 FileIO: {e}")
        return # Exit if IO cannot be loaded

    table_location = f"{ICEBERG_WAREHOUSE_PATH}/{TABLE_NAME}"
    metadata_dir = f"{table_location}/metadata"
    version_hint_path = f"{metadata_dir}/version-hint.text"

    # --- Check if table already exists (simple check via version hint) ---
    # This prevents accidentally overwriting an existing table structure.
    # You could add a --force flag to bypass this check if needed.
    try:
        print(f"Checking if version hint file already exists: {version_hint_path}")
        input_file = s3_io.new_input(version_hint_path)
        input_stream = input_file.open()
        _ = input_stream.readall() # Try reading
        input_stream.close()
        print(f"WARNING: Table '{TABLE_NAME}' appears to already exist (version hint found). Aborting creation.")
        print(f"If you intend to overwrite, delete the '{TABLE_NAME}' directory in your R2 bucket first.")
        return # Exit if table seems to exist
    except FileNotFoundError:
        print(f"Version hint not found. Proceeding with table creation.")
    except Exception as read_err:
        print(f"Error checking for existing table, but proceeding cautiously: {read_err}")
        # Decide if you want to proceed or abort on other errors

    # --- Create Table Structure ---
    try:
        # 1. Create initial metadata object
        metadata = new_table_metadata(
            location=table_location,
            schema=iceberg_schema, # Use the schema defined above
            partition_spec=UNPARTITIONED_PARTITION_SPEC, # Or your specific spec
            sort_order=UNSORTED_SORT_ORDER, # Or your specific sort order
            properties={} # Add any specific table properties if needed
        )
        print("Initial TableMetadata object created.")

        # 2. Define path for the first metadata file (version 0)
        metadata_file_name = f"00000-{uuid.uuid4()}.metadata.json"
        metadata_file_path = f"{metadata_dir}/{metadata_file_name}"
        print(f"Target path for initial metadata file: {metadata_file_path}")

        # 3. Write the first metadata file using ToOutputFile
        # Pass the OutputFile object directly; ToOutputFile handles create/close
        metadata_output_file = s3_io.new_output(metadata_file_path)
        ToOutputFile.table_metadata(metadata, metadata_output_file)
        # No need to manually call create() or close() here for metadata
        print("Initial metadata file written successfully via ToOutputFile.")

        # 4. Write the version hint file pointing to version 0 (Manual handling still needed)
        print(f"Writing version hint file: {version_hint_path}")
        hint_output_file = s3_io.new_output(version_hint_path)
        hint_output_stream = hint_output_file.create()
        hint_output_stream.write(b"0\n") # Content is '0' followed by newline
        hint_output_stream.close() # Crucial: close the stream for the hint file
        print("Version hint file written successfully.")

        print(f"\nSUCCESS: Iceberg table '{TABLE_NAME}' structure created at {table_location}")
        print("You should now be able to start the main application (`rag.py`).")

    except Exception as create_e:
        print(f"\nFATAL ERROR during table creation process: {create_e}")
        # You might want to add cleanup logic here to delete partially created files
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Basic check for credentials
    if not all([R2_ENDPOINT_URL, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, BUCKET_NAME]):
        print("FATAL: Missing required environment variables (AWS_S3_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_S3_BUCKET)")
    else:
        create_table() 