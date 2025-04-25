# rag.py
import os
import duckdb
import pyarrow as pa
from fastapi import FastAPI, HTTPException, Body
from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.table import Table
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional # Removed Set
import json
# import uuid # Removed
# import time # Already removed
import pandas as pd

# --- Debugging Environment Variables ---
print("--- Environment Variable Debug --- ")
print(f"AWS_S3_ENDPOINT: {os.getenv('AWS_S3_ENDPOINT')}")
print(f"AWS_ACCESS_KEY_ID: {'<present>' if os.getenv('AWS_ACCESS_KEY_ID') else '<None>'}") # Don't log the actual key
print(f"AWS_SECRET_ACCESS_KEY: {'<present>' if os.getenv('AWS_SECRET_ACCESS_KEY') else '<None>'}") # Don't log the actual secret
print(f"AWS_S3_BUCKET: {os.getenv('AWS_S3_BUCKET')}")
print(f"DUCKDB_S3_REGION: {os.getenv('DUCKDB_S3_REGION')}")
print("--------------------------------")

# Load environment variables
load_dotenv()

# Configuration
# Read R2 REST Catalog specific variables
R2_CATALOG_URI = os.getenv("R2_CATALOG_URI")
# R2 Warehouse Name (acts as namespace)
R2_WAREHOUSE_NAME = os.getenv("R2_WAREHOUSE_NAME")
# Read R2 credentials & endpoint (needed for BOTH catalog auth and FileIO)
R2_ENDPOINT_URL = os.getenv("R2_ENDPOINT_URL")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")

# --- Add validation checks --- 
def validate_env_vars(**kwargs):
    missing_vars = [name for name, value in kwargs.items() if value is None]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

validate_env_vars(
    R2_CATALOG_URI=R2_CATALOG_URI,
    R2_WAREHOUSE_NAME=R2_WAREHOUSE_NAME,
    R2_ENDPOINT_URL=R2_ENDPOINT_URL,
    R2_ACCESS_KEY_ID=R2_ACCESS_KEY_ID,
    R2_SECRET_ACCESS_KEY=R2_SECRET_ACCESS_KEY
)
print("R2 Configuration variables loaded.")
# --- End validation checks --- 

# Define DuckDB region separately or reuse if appropriate
S3_REGION = os.getenv("S3_REGION", "auto")

# Define the Iceberg table schema (Still useful for endpoint definitions)
iceberg_schema = pa.schema([
    pa.field("id", pa.string(), nullable=False),
    pa.field("fact", pa.string(), nullable=False),
    pa.field("source", pa.string(), nullable=True)
])
# Or use the NestedField version if preferred, but pa.schema is simpler if just for reference
# iceberg_schema = Schema(
#     NestedField(field_id=1, name="id", field_type=StringType(), required=True),
#     NestedField(field_id=2, name="fact", field_type=StringType(), required=True),
#     NestedField(field_id=3, name="source", field_type=StringType(), required=False),
#     schema_id=1,
#     identifier_field_ids=[1]
# )

# Initialize FastAPI app
app = FastAPI()

# --- Globals ---
# Removed db_conn global
# Rename global variable to reflect it's loaded via catalog
email_facts_table: Optional[Table] = None
r2_catalog: Optional[Any] = None # Using Any for Catalog type hint simplicity

try:
    print("--- Initializing R2 REST Catalog Connection ---")

    # Define properties for PyIceberg load_catalog
    # Using explicit SigV4 configuration and client.* credentials
    catalog_name = "r2_rest_catalog" # Logical name for this catalog instance
    catalog_props = {
        "type": "rest",
        "uri": R2_CATALOG_URI,

        # Explicitly enable and configure SigV4 for the REST Catalog API calls
        "rest.sigv4-enabled": "true",
        "rest.signing-name": "s3",        # R2 Catalog API uses AWS SigV4 with 's3' service name
        "rest.signing-region": S3_REGION, # Use region from env var (e.g., 'auto')

        # S3 endpoint for underlying file IO (PyArrow/fsspec) - might also be used by signer
        "s3.endpoint": R2_ENDPOINT_URL,

        # Explicit AWS credentials for boto3/botocore used by PyIceberg's SigV4 signer
        # This avoids reliance on environment variable discovery issues.
        "client.access-key-id": R2_ACCESS_KEY_ID,
        "client.secret-access-key": R2_SECRET_ACCESS_KEY,
        # "client.session-token": AWS_SESSION_TOKEN, # Add if using temporary STS credentials
    }

    # Log properties, redacting secrets for security
    log_props_safe = {k: ('***' if 'secret' in k or 'key' in k else v) for k, v in catalog_props.items()}
    print(f"Attempting to load catalog '{catalog_name}' with properties:")
    for key, value in log_props_safe.items():
        print(f"  - {key}: {value}")

    # Load the catalog
    r2_catalog = load_catalog(catalog_name, **catalog_props)
    print(f"Successfully loaded PyIceberg REST catalog: {r2_catalog.name}")

    # Load the specific table using the R2 Warehouse Name as the namespace
    table_name = "email_facts"
    # R2 Warehouse Name acts as the top-level namespace in the catalog URI structure
    full_table_identifier = (R2_WAREHOUSE_NAME, table_name) # Tuple format: (namespace, table)
    print(f"Attempting to load table: {full_table_identifier}")

    email_facts_table = r2_catalog.load_table(full_table_identifier)
    print(f"Successfully loaded table: {email_facts_table.identifier}")
    print("--- Initialization Complete ---")

except Exception as startup_e:
    print("--- FATAL STARTUP ERROR ---")
    print(f"Error during R2 Catalog/Table initialization: {startup_e}")
    import traceback
    traceback.print_exc() # Print full traceback for startup errors
    email_facts_table = None # Ensure table is None if startup fails
    r2_catalog = None
    print("---------------------------")
    # Re-raise to ensure FastAPI knows startup failed critically
    # This prevents the server from starting in a broken state.
    raise

# --- API Endpoints ---

@app.post("/add_fact")
async def add_fact(fact_data: Dict[str, Any] = Body(...)):
    """Adds a new fact to the Iceberg table using PyIceberg append via REST Catalog."""
    if not email_facts_table: # Check if the table object loaded
        raise HTTPException(status_code=500, detail="PyIceberg table not initialized")

    required_keys = ["id", "fact"]
    if not all(key in fact_data for key in required_keys):
        raise HTTPException(status_code=400, detail="Missing required keys: id, fact")

    # Prepare data for PyIceberg append
    try:
        # Create a PyArrow Table from the single row dictionary
        # Ensure data types match the schema
        data_dict = {
            "id": [fact_data['id']],
            "fact": [fact_data['fact']],
            "source": [fact_data.get('source')] # Handles optional field
        }
        # Use schema defined earlier for consistency
        pa_table = pa.Table.from_pydict(data_dict, schema=iceberg_schema)
        print(f"Prepared pyarrow table for append: {pa_table}")

        # Use a 'with' statement for the transaction - commit goes via REST Catalog
        print(f"Appending data to PyIceberg table '{email_facts_table.identifier}'...")
        try:
            # Refresh table state before transaction
            print("Refreshing table state from catalog...")
            email_facts_table.refresh()

            with email_facts_table.transaction() as tx:
                tx.append(pa_table)
            # Commit is handled automatically by the context manager on successful exit
            print("Data appended successfully via PyIceberg transaction.")
        except Exception as e:
            print(f"Error appending data via PyIceberg transaction: {e}")
            import traceback
            traceback.print_exc()
            raise HTTPException(status_code=500, detail=f"Error appending data via PyIceberg transaction: {e}")

        return {"message": "Fact added successfully"}
    except HTTPException as http_exc:
        # Catch potential validation/HTTP errors and re-raise
        raise http_exc
    except Exception as e:
        # Catch unexpected errors during data prep
        print(f"Unexpected error preparing data for add_fact: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Unexpected error adding fact: {e}")

@app.get("/get_fact/{fact_id}")
async def get_fact(fact_id: str):
    """Retrieves a fact by its ID using PyIceberg table scan."""
    if not email_facts_table:
        raise HTTPException(status_code=500, detail="PyIceberg table not initialized")

    try:
        # Scan the table using PyIceberg, filtering by id using a string filter
        print(f"Scanning table '{email_facts_table.identifier}' for id = {fact_id}")
        # Construct the filter string carefully, quoting the string value
        filter_str = f"id = '{fact_id}'"
        print(f"Using row_filter: {filter_str}")

        # Refresh table state before scanning
        print("Refreshing table state from catalog...")
        email_facts_table.refresh()

        # Use to_pandas() for easier row conversion
        results_df = email_facts_table.scan(
            row_filter=filter_str
        ).to_pandas()

        if not results_df.empty:
            # Convert the first row to a dictionary
            # Handle potential NaNs or Nones for the optional 'source' field
            fact_dict = results_df.iloc[0].where(pd.notna(results_df.iloc[0]), None).to_dict()
            print(f"Found fact: {fact_dict}")
            return fact_dict
        else:
            print(f"Fact with id {fact_id} not found.")
            raise HTTPException(status_code=404, detail="Fact not found")

    except HTTPException as http_exc:
        # Re-raise HTTP exceptions (like 404)
        raise http_exc
    except Exception as e:
        print(f"Error scanning fact: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to retrieve fact: {e}")

@app.get("/list_facts")
async def list_facts(limit: int = 10):
    """Lists facts from the Iceberg table using PyIceberg table scan."""
    if not email_facts_table:
        raise HTTPException(status_code=500, detail="PyIceberg table not initialized")

    try:
        # Scan the table using PyIceberg with a limit
        print(f"Scanning table '{email_facts_table.identifier}' with limit {limit}")

        # Refresh table state before scanning
        print("Refreshing table state from catalog...")
        email_facts_table.refresh()

        # Use to_pandas() for easier row conversion
        results_df = email_facts_table.scan(limit=limit).to_pandas()

        if not results_df.empty:
            # Convert DataFrame to list of dictionaries
            # Handle potential NaNs/Nones for optional fields
            facts_list = results_df.where(pd.notna(results_df), None).to_dict('records')
            print(f"Found {len(facts_list)} facts.")
            return facts_list
        else:
            print("No facts found.")
            return []
    except Exception as e:
        print(f"Error listing facts: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to list facts: {e}")

# Health check endpoint
@app.get("/health")
async def health_check():
    """Checks if the PyIceberg table object is loaded."""
    if email_facts_table and email_facts_table.identifier:
         # Optionally add a lightweight check, like accessing a property
         try:
             _ = email_facts_table.schema() # Simple check
             return {"status": "ok", "message": f"Service is healthy, table '{email_facts_table.identifier}' loaded."}
         except Exception as hc_err:
             print(f"Health check failed accessing table schema: {hc_err}")
             return {"status": "degraded", "message": f"Table object loaded but check failed: {hc_err}"}
    else:
        return {"status": "unhealthy", "message": "PyIceberg table failed to load on startup."}

# --- Main Execution ---
if __name__ == "__main__":
    import uvicorn
    # Check if running in Docker or locally for host binding
    host = os.getenv("DOCKER_HOST", "127.0.0.1") # Default to localhost if not in Docker
    uvicorn.run(app, host=host, port=8080)