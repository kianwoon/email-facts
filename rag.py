# rag.py
import os
import duckdb
import pyarrow as pa
from fastapi import FastAPI, HTTPException, Body
from pyiceberg.io import load_file_io
from pyiceberg.table import Table
from pyiceberg.table.metadata import TableMetadata, TableMetadataUtil
from pyiceberg.catalog.noop import NoopCatalog
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional, Set
import pyarrow.fs as pa_fs # Import pyarrow.fs
from pyarrow.fs import FileSelector # Added FileSelector
import json # Keep json
import uuid # Import uuid for generating metadata filenames

# --- Custom Catalog for S3 Commits ---

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.io import FileIO
from pyiceberg.table import Table
from pyiceberg.table.metadata import TableMetadata, TableMetadataUtil


class SimpleS3Catalog(Catalog):
    """
    A minimal PyIceberg Catalog implementation that handles commits directly
    to S3/R2 by writing metadata files and updating the version hint.

    Assumes a flat structure within the warehouse path (no database grouping).
    """
    def __init__(self, name: str, **properties: Any):
        # Store properties which should include S3 config needed for FileIO
        super().__init__(name, **properties)
        # We expect FileIO to be configured externally and passed or created here
        # For simplicity, let's assume the Table object using this catalog
        # will have its IO configured, and we can access it during commit.

    def load_table(self, identifier: str) -> Table:
        # This simple catalog doesn't maintain its own state or load tables.
        # Table loading is handled externally before associating with this catalog.
        raise NotImplementedError("SimpleS3Catalog does not load tables directly.")

    # Removed the original commit_table definition that caused NameError
    # def commit_table(self, request: CommitTableRequest) -> CommitTableResponse:
    #     ...
    #     pass

    # Keep the redefined version based on traceback observation
    def commit_table(self, table: Table, requirements: List, updates: List) -> TableMetadata:
        """
        Handles the commit by writing the new metadata file and updating version-hint.text.
        Uses the signature observed in the traceback: (self, table, requirements, updates).
        Returns the newly committed TableMetadata.
        """
        if not table.io:
            raise ValueError("Table FileIO is not configured, cannot commit.")
        if not table.metadata:
            raise ValueError("Table metadata is not loaded, cannot commit.")
        if not table.metadata_location:
             raise ValueError("Table metadata location is not known, cannot commit.")

        current_metadata = table.metadata
        # The Table object should have already prepared the *next* metadata state
        # based on the transaction's updates before calling this method.
        # We just need to write it out.
        new_metadata = current_metadata # Assume table.metadata is the *new* state

        # Determine the new version and filename
        current_version = int(current_metadata.current_version_id) # Or from metadata_location? Let's use current_version_id
        next_version = current_version + 1
        new_metadata_filename = f"{next_version:05d}-{uuid.uuid4()}.metadata.json"
        metadata_folder_path = "/".join(table.metadata_location.split('/')[:-1]) # e.g., s3://.../table/metadata
        new_metadata_full_path = f"{metadata_folder_path}/{new_metadata_filename}"
        version_hint_path = f"{metadata_folder_path}/version-hint.text"

        print(f"Committing new metadata version {next_version} to {new_metadata_full_path}")
        print(f"Updating version hint at {version_hint_path}")

        try:
            # 1. Serialize the new metadata
            metadata_json_bytes = TableMetadataUtil.to_json(new_metadata).encode('utf-8')

            # 2. Write the new metadata file
            metadata_output_file = table.io.new_output(new_metadata_full_path)
            with metadata_output_file.create() as file_stream:
                file_stream.write(metadata_json_bytes)
            print(f"Successfully wrote new metadata file: {new_metadata_full_path}")

            # 3. Write the new version hint
            hint_output_file = table.io.new_output(version_hint_path)
            with hint_output_file.create(overwrite=True) as file_stream:
                file_stream.write(str(next_version).encode('utf-8'))
            print(f"Successfully updated version hint to: {next_version}")

            # Return the metadata that was committed
            # The Table object seems to expect the *new* metadata back.
            return new_metadata

        except Exception as e:
            print(f"Commit failed: Error during S3 write operations: {e}")
            # Attempt cleanup? Difficult without transactions.
            # Try deleting the potentially orphaned metadata file
            try:
                if table.io.exists(new_metadata_full_path):
                    print(f"Attempting to clean up orphaned metadata file: {new_metadata_full_path}")
                    table.io.delete(new_metadata_full_path)
            except Exception as cleanup_e:
                print(f"Cleanup failed: Could not delete orphaned metadata file: {cleanup_e}")

            raise CommitFailedException(f"Failed to commit metadata updates to S3: {e}") from e

    # Implement other required abstract methods (even if as NotImplementedError)
    def create_table(self, identifier: str, schema, spec, properties, location) -> Table:
        raise NotImplementedError("SimpleS3Catalog does not support create_table.")

    def drop_table(self, identifier: str):
        raise NotImplementedError("SimpleS3Catalog does not support drop_table.")

    def rename_table(self, from_identifier: str, to_identifier: str):
         raise NotImplementedError("SimpleS3Catalog does not support rename_table.")

    def list_tables(self, namespace: str) -> List[str]:
        raise NotImplementedError("SimpleS3Catalog does not support list_tables.")

    # Need list_namespaces? Let's add a basic version.
    def list_namespaces(self, namespace: str = None) -> List[str]:
         # Assuming no nested namespaces for simplicity
         return []

    # --- Add stubs for other required abstract methods --- 

    def create_namespace(self, namespace: str, properties: Dict[str, str] = None):
        raise NotImplementedError("SimpleS3Catalog does not support namespace creation.")

    # Note: create_table_transaction might not exist in all PyIceberg versions? 
    # If it causes an error, we might need to adjust based on the actual base class.
    # Let's add it based on the TypeError for now.
    def create_table_transaction(self, identifier: str, schema, spec, properties, location):
        raise NotImplementedError("SimpleS3Catalog does not support create_table_transaction.")
        
    def drop_namespace(self, namespace: str) -> None:
        raise NotImplementedError("SimpleS3Catalog does not support dropping namespaces.")

    def load_namespace_properties(self, namespace: str) -> Dict[str, str]:
        raise NotImplementedError("SimpleS3Catalog does not support loading namespace properties.")

    def update_namespace_properties(
        self, namespace: str, removals: Optional[Set[str]] = None, updates: Optional[Dict[str, str]] = None
    ):
        raise NotImplementedError("SimpleS3Catalog does not support updating namespace properties.")

    def register_table(self, identifier: str, metadata_location: str) -> Table:
        raise NotImplementedError("SimpleS3Catalog does not support registering tables.")

    def table_exists(self, identifier: str) -> bool:
        raise NotImplementedError("SimpleS3Catalog does not support table_exists check.")

    def purge_table(self, identifier: str) -> None:
        raise NotImplementedError("SimpleS3Catalog does not support purging tables.")

    def list_views(self, namespace: str) -> List[str]:
        raise NotImplementedError("SimpleS3Catalog does not support listing views.")

    def view_exists(self, identifier: str) -> bool:
        raise NotImplementedError("SimpleS3Catalog does not support view_exists check.")

    def drop_view(self, identifier: str) -> None:
        raise NotImplementedError("SimpleS3Catalog does not support dropping views.")

# --- End Custom Catalog ---

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
# Read variables using the names set in Koyeb
R2_ENDPOINT_URL = os.getenv("AWS_S3_ENDPOINT")
R2_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
DUCKDB_S3_REGION = os.getenv("DUCKDB_S3_REGION", "auto") # Default to auto if not set
ICEBERG_CATALOG_NAME = "my_iceberg_data" # Or any logical name
# Use the AWS_S3_BUCKET variable from Koyeb
ICEBERG_WAREHOUSE_PATH = f"s3://{os.getenv('AWS_S3_BUCKET')}/{ICEBERG_CATALOG_NAME}" # Base path for tables

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

# Function to initialize DuckDB connection with Iceberg and S3 extensions
def get_duckdb_connection():
    con = duckdb.connect(database=':memory:', read_only=False)
    try:
        con.sql("INSTALL iceberg;")
        con.sql("LOAD iceberg;")
        con.sql("INSTALL httpfs;") # Required for S3 access
        con.sql("LOAD httpfs;")

        # Prepare endpoint URL: remove schema for DuckDB setting
        endpoint_url = R2_ENDPOINT_URL
        if endpoint_url and endpoint_url.startswith("https://"):
            endpoint_url = endpoint_url[len("https://"):]
            print(f"Stripped https:// from endpoint for DuckDB: {endpoint_url}")

        # Configure S3 credentials for DuckDB
        con.sql(f"SET s3_endpoint='{endpoint_url}';") # Use stripped URL
        con.sql(f"SET s3_access_key_id='{R2_ACCESS_KEY_ID}';")
        con.sql(f"SET s3_secret_access_key='{R2_SECRET_ACCESS_KEY}';")
        con.sql("SET s3_use_ssl=true;") # Let DuckDB handle SSL based on this flag
        # con.sql("SET s3_use_virtual_host=false;") # Removed: Unrecognized by local DuckDB version
        # con.sql("SET s3_use_path_style=true;") # Re-add: Force path-style addressing (for R2/MinIO)
        # con.sql("SET s3_use_path_style=true;") # Removed: Still unrecognized in 1.2.2 locally
        # con.sql("SET s3_use_path_style=true;") # Removed: Unrecognized by local DuckDB version (again)
        con.sql(f"SET s3_region='{DUCKDB_S3_REGION}';") # Use region from env or 'auto'
        # Allow reading latest version without explicit version hint
        # con.sql("SET unsafe_enable_version_guessing = true;") # Re-enabled for 1.2.2 - Removing again
        # print("Enabled unsafe_enable_version_guessing for DuckDB session.") # Re-enabled for 1.2.2 - Removing again
    except Exception as e:
        print(f"Error initializing DuckDB extensions or S3 config: {e}")
        raise
    return con

# --- Helper Function to Get Latest Metadata File Path ---
def get_latest_metadata_path(base_table_path: str) -> str:
    """
    Finds the latest Iceberg metadata file path using version-hint.text.

    Args:
        base_table_path: The S3 base path of the Iceberg table (e.g., s3://bucket/prefix/table_name).

    Returns:
        The full S3 path to the latest .metadata.json file.

    Raises:
        HTTPException: If S3 filesystem cannot be configured or files cannot be found.
    """
    print(f"Finding latest metadata for table: {base_table_path}")

    # Configure pyarrow.fs.S3FileSystem (for reading metadata files)
    print("Configuring pyarrow.fs.S3FileSystem for reading metadata...")
    try:
        # Re-use environment variables directly for simplicity here
        s3_fs = pa_fs.S3FileSystem(
            access_key=os.getenv("AWS_ACCESS_KEY_ID"),
            secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region=os.getenv("DUCKDB_S3_REGION", "auto"),
            endpoint_override=os.getenv("AWS_S3_ENDPOINT")
        )
        print("pyarrow.fs.S3FileSystem configured.")
    except Exception as fs_err:
        print(f"FATAL: Failed to configure pyarrow.fs.S3FileSystem: {fs_err}")
        # Raise HTTPException here as this is likely called during a request
        raise HTTPException(status_code=500, detail=f"Failed to configure pyarrow S3 FS: {fs_err}")

    # Construct paths relative to the S3 bucket root (for pyarrow fs)
    metadata_path_relative = f"{base_table_path.replace('s3://', '')}/metadata"
    version_hint_path_relative = f"{metadata_path_relative}/version-hint.text"

    try:
        # 1. Read version hint
        print(f"Reading version hint from: {version_hint_path_relative}")
        with s3_fs.open_input_stream(version_hint_path_relative) as input_stream:
            version_hint_bytes = input_stream.readall()
        latest_version_str = version_hint_bytes.decode('utf-8').strip()
        print(f"Read version hint: '{latest_version_str}'")

        # 2. Find the specific metadata file (expecting format 0000{V}-uuid.metadata.json)
        target_metadata_filename = None
        # Pad version number if needed (assuming hint is just '0', '1', etc.)
        version_prefix = f"{int(latest_version_str):05d}-" # e.g., "00000-"
        print(f"Looking for metadata file with prefix '{version_prefix}' in '{metadata_path_relative}'")

        selector = FileSelector(metadata_path_relative, allow_not_found=False)
        file_infos = s3_fs.get_file_info(selector)

        found_files = []
        for file_info in file_infos:
            if not file_info.is_file:
                continue
            filename = file_info.path.split('/')[-1]
            found_files.append(filename) # Keep track for debugging
            if filename.startswith(version_prefix) and filename.endswith(".metadata.json"):
                target_metadata_filename = filename
                print(f"Found target metadata file: {target_metadata_filename}")
                break

        if not target_metadata_filename:
             print(f"Metadata files found in directory: {found_files}") # Debugging output
             raise FileNotFoundError(f"Could not find metadata file for version {latest_version_str} (prefix {version_prefix}) in {metadata_path_relative}")

        # 3. Construct full S3 path
        full_metadata_path = f"{base_table_path}/metadata/{target_metadata_filename}"
        print(f"Returning full metadata path: {full_metadata_path}")
        return full_metadata_path

    except FileNotFoundError as fnf_err:
        print(f"FATAL: Metadata file or version hint not found. Error: {fnf_err}")
        raise HTTPException(status_code=500, detail=f"Metadata file/hint not found: {fnf_err}")
    except pa.lib.ArrowIOError as arrow_io_err:
         print(f"FATAL: PyArrow IO Error accessing S3. Check credentials/path. Error: {arrow_io_err}")
         raise HTTPException(status_code=500, detail=f"S3 Access Error: {arrow_io_err}")
    except Exception as load_err:
        print(f"FATAL: Unexpected error finding metadata path. Error: {load_err}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error finding metadata path: {load_err}")

# --- Function to Load PyIceberg Table for Writes ---
def load_pyiceberg_table_for_writes(table_path: str, catalog: Catalog) -> Table:
    """Loads a PyIceberg Table object configured for writes, using the provided catalog."""
    print(f"Attempting to load PyIceberg table for writes from path: {table_path} using catalog: {catalog.name}")

    # Configure IO Properties (used by both PyIceberg IO and PyArrow FS)
    io_properties = {
        "s3.endpoint": os.getenv("AWS_S3_ENDPOINT"),
        "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID"),
        "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "s3.region": os.getenv("DUCKDB_S3_REGION", "auto") # Use region from env or 'auto'
    }

    # Configure PyIceberg's FileIO using load_file_io
    print("Configuring PyIceberg FileIO using load_file_io...")
    try:
        # Use the central load_file_io function
        # pyiceberg_file_io = PyArrowFileIO(properties=io_properties)
        iceberg_io = load_file_io(properties=io_properties)
        print("PyIceberg FileIO configured.")
    except Exception as io_err:
        print(f"FATAL: Failed to configure PyIceberg FileIO: {io_err}")
        raise RuntimeError(f"Failed to configure S3 IO for PyIceberg: {io_err}") # Use RuntimeError during startup

    # Configure pyarrow.fs.S3FileSystem (ONLY for listing metadata files)
    # We still need this because PyIceberg FileIO doesn't have a standard list function
    print("Configuring pyarrow.fs.S3FileSystem for listing metadata...")
    try:
        # Use the same keys/endpoint as io_properties for consistency
        s3_fs = pa_fs.S3FileSystem(
            access_key=io_properties["s3.access-key-id"],
            secret_key=io_properties["s3.secret-access-key"],
            region=io_properties["s3.region"],
            endpoint_override=io_properties["s3.endpoint"]
        )
        print("pyarrow.fs.S3FileSystem configured for listing.")
    except Exception as fs_err:
        print(f"FATAL: Failed to configure pyarrow.fs.S3FileSystem: {fs_err}")
        raise RuntimeError(f"Failed to configure pyarrow S3 FS: {fs_err}")

    # Paths
    metadata_path_relative = f"{table_path.replace('s3://', '')}/metadata" # Path relative to bucket root for pa_fs
    version_hint_path_s3 = f"{table_path}/metadata/version-hint.text" # Full S3 path for iceberg_io

    try:
        # 1. Read version hint using PyIceberg IO
        print(f"Reading version hint using PyIceberg IO from: {version_hint_path_s3}")
        # with s3_fs.open_input_stream(version_hint_path_relative) as input_stream:
        with iceberg_io.new_input(version_hint_path_s3).open() as input_stream:
            version_hint_bytes = input_stream.read()
        latest_version_str = version_hint_bytes.decode('utf-8').strip()
        print(f"Read version hint: '{latest_version_str}'")

        # 2. Find the specific metadata file using pyarrow fs (listing)
        target_metadata_filename = None
        version_prefix = f"{int(latest_version_str):05d}-"
        print(f"Listing metadata files with pyarrow fs for prefix '{version_prefix}' in '{metadata_path_relative}'")
        selector = FileSelector(metadata_path_relative, allow_not_found=False)
        file_infos = s3_fs.get_file_info(selector)
        for file_info in file_infos:
            if not file_info.is_file: continue
            filename = file_info.path.split('/')[-1]
            if filename.startswith(version_prefix) and filename.endswith(".metadata.json"):
                target_metadata_filename = filename
                print(f"Found target metadata file: {target_metadata_filename}")
                break
        if not target_metadata_filename:
             raise FileNotFoundError(f"Could not find metadata file for version {latest_version_str} (prefix {version_prefix}) in {metadata_path_relative}")

        # 3. Read the target metadata file content using PyIceberg IO
        full_metadata_path_s3 = f"{table_path}/metadata/{target_metadata_filename}" # Full S3 path
        print(f"Reading metadata content using PyIceberg IO from: {full_metadata_path_s3}")
        # with s3_fs.open_input_stream(target_metadata_path_relative) as meta_stream:
        with iceberg_io.new_input(full_metadata_path_s3).open() as meta_stream:
            metadata_bytes = meta_stream.read()
        metadata_json_str = metadata_bytes.decode('utf-8')

        # 4. Parse the metadata
        print("Parsing metadata JSON...")
        parsed_metadata = TableMetadataUtil.parse_raw(metadata_json_str)
        print(f"Metadata parsed successfully. Format version: {parsed_metadata.format_version}")

        # 5. Instantiate the PyIceberg Table object using the constructor
        print("Instantiating pyiceberg.table.Table using constructor...")
        table_identifier = tuple(table_path.replace("s3://", "").split("/")[-2:]) # e.g., ('my_iceberg_data', 'email_facts')
        
        # Pass identifier, parsed metadata, configured IO, metadata_location, and the *provided catalog*
        table = Table(
            identifier=table_identifier,
            metadata=parsed_metadata,
            io=iceberg_io,
            metadata_location=full_metadata_path_s3,
            catalog=catalog  # Use the provided catalog instance
        )
        
        # --- DEBUG: Inspect the created table object ---
        print(f"DEBUG: Attributes of created table object: {dir(table)}")
        # --- END DEBUG ---

        # Removed manual io assignment as it's passed to constructor
        # table.io = iceberg_io
        # Access the internal _identifier attribute instead
        print(f"Successfully instantiated pyiceberg table '{table._identifier}' for writes with catalog '{table.catalog.name}'.")
        return table

    except FileNotFoundError as fnf_err:
        print(f"FATAL: Metadata file or version hint not found during PyIceberg load. Error: {fnf_err}")
        raise RuntimeError(f"Metadata file/hint not found: {fnf_err}")
    except pa.lib.ArrowIOError as arrow_io_err:
         print(f"FATAL: PyArrow IO Error accessing S3 during PyIceberg load. Error: {arrow_io_err}")
         raise RuntimeError(f"S3 Access Error: {arrow_io_err}")
    except Exception as load_err:
        print(f"FATAL: Unexpected error loading PyIceberg table. Error: {load_err}")
        import traceback
        traceback.print_exc()
        raise RuntimeError(f"Error loading PyIceberg table: {load_err}")

# Initialize FastAPI app
app = FastAPI()

# --- Globals ---
# Initialize DuckDB connection AND PyIceberg Table at startup
db_conn = None  # Initialize globals to None
email_facts_table_for_writes: Optional[Table] = None # Ensure type hint matches base Table
simple_catalog: Optional[SimpleS3Catalog] = None # Global for the catalog instance

try:
    print("Initializing DuckDB connection...")
    db_conn = get_duckdb_connection()
    print("DuckDB connection initialized.")

    print("Initializing SimpleS3Catalog...")
    # Pass necessary properties (like S3 config) if the catalog needs them directly,
    # but our current implementation relies on the Table's IO.
    simple_catalog = SimpleS3Catalog(name="simple_s3_catalog")
    print(f"SimpleS3Catalog '{simple_catalog.name}' initialized.")

    print("Loading Iceberg table via PyIceberg for Writes...")
    table_name = "email_facts"
    full_table_path = f"{ICEBERG_WAREHOUSE_PATH}/{table_name}"
    # Load the table object for write operations, passing the catalog
    email_facts_table_for_writes = load_pyiceberg_table_for_writes(full_table_path, simple_catalog)
    print(f"PyIceberg table '{email_facts_table_for_writes._identifier}' loaded for writes with catalog '{email_facts_table_for_writes.catalog.name}'.")

# This except block now correctly corresponds to the outer try block
except Exception as startup_e:
    print(f"FATAL: Startup failed during initialization: {startup_e}")
    # db_conn might already be None if get_duckdb_connection failed, but set again for clarity
    db_conn = None
    email_facts_table_for_writes = None # Ensure table is None if startup fails
    simple_catalog = None # Ensure catalog is None if startup fails
    raise # Re-raise to ensure FastAPI knows startup failed critically

# --- API Endpoints ---

@app.post("/add_fact")
async def add_fact(fact_data: Dict[str, Any] = Body(...)):
    """Adds a new fact to the Iceberg table using PyIceberg append."""
    if not email_facts_table_for_writes: # Check if the table object loaded
        raise HTTPException(status_code=500, detail="PyIceberg table not initialized for writes")

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
        pa_table = pa.Table.from_pydict(data_dict, schema=iceberg_schema)
        print(f"Prepared pyarrow table for append: {pa_table}")

        # Use a 'with' statement for the transaction
        print(f"Appending data to PyIceberg table '{email_facts_table_for_writes._identifier}'...")
        try:
            with email_facts_table_for_writes.transaction() as tx:
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
        raise http_exc

@app.get("/get_fact/{fact_id}")
async def get_fact(fact_id: str):
    """Retrieves a fact by its ID using DuckDB iceberg_scan with explicit metadata path."""
    if not db_conn:
        raise HTTPException(status_code=500, detail="Database not initialized")

    table_name = "email_facts"
    base_table_path = f"{ICEBERG_WAREHOUSE_PATH}/{table_name}"

    try:
        # Get the latest metadata file path *first*
        metadata_file_path = get_latest_metadata_path(base_table_path)
        print(f"Using metadata file for query: {metadata_file_path}")

        # Use the specific metadata file in iceberg_scan
        query = f"SELECT * FROM iceberg_scan('{metadata_file_path}') WHERE id = ?"

        print(f"Executing query: {query} with param: {fact_id}")
        result = db_conn.execute(query, [fact_id]).fetchone()
        print(f"Query result: {result}")
        if result:
            # Convert result tuple to dict based on schema (using simple list now)
            columns = [f.name for f in iceberg_schema]
            return dict(zip(columns, result))
        else:
            raise HTTPException(status_code=404, detail="Fact not found")
    except HTTPException as http_exc: # Re-raise HTTP exceptions from helper
        raise http_exc
    except Exception as e:
        print(f"Error querying fact: {e}")
        # Attempt to get more specific DuckDB error if possible
        duckdb_error_msg = str(e)
        raise HTTPException(status_code=500, detail=f"Failed to retrieve fact: {duckdb_error_msg}")

@app.get("/list_facts")
async def list_facts(limit: int = 10):
    """Lists facts from the Iceberg table using DuckDB iceberg_scan with explicit metadata path."""
    if not db_conn:
        raise HTTPException(status_code=500, detail="Database not initialized")

    table_name = "email_facts"
    base_table_path = f"{ICEBERG_WAREHOUSE_PATH}/{table_name}"

    try:
        # Get the latest metadata file path *first*
        metadata_file_path = get_latest_metadata_path(base_table_path)
        print(f"Using metadata file for query: {metadata_file_path}")

        # Use the specific metadata file in iceberg_scan
        query = f"SELECT * FROM iceberg_scan('{metadata_file_path}') LIMIT ?"

        print(f"Executing query: {query} with limit: {limit}")
        results = db_conn.execute(query, [limit]).fetchall()
        print(f"Query results count: {len(results)}")
        if results:
            columns = [f.name for f in iceberg_schema]
            return [dict(zip(columns, row)) for row in results]
        else:
            return []
    except HTTPException as http_exc: # Re-raise HTTP exceptions from helper
        raise http_exc
    except Exception as e:
        print(f"Error listing facts: {e}")
         # Attempt to get more specific DuckDB error if possible
        duckdb_error_msg = str(e)
        raise HTTPException(status_code=500, detail=f"Failed to list facts: {duckdb_error_msg}")

# Health check endpoint
@app.get("/health")
async def health_check():
    """Checks DuckDB connection responsiveness."""
    if not db_conn:
         return {"status": "unhealthy", "message": "DB connection failed on startup."}
    try:
        # Try a minimal, non-table query to check connection responsiveness
        _ = db_conn.execute("SELECT 1").fetchone()
        return {"status": "ok", "message": "Service is healthy, DB connection active."}
    except Exception as e:
        print(f"Health check failed during simple DuckDB query: {e}")
        # Attempt to get specific error
        duckdb_error_msg = str(e)
        return {"status": "degraded", "message": f"Service might be unhealthy. DB query failed: {duckdb_error_msg}"}

# --- Main Execution ---
if __name__ == "__main__":
    import uvicorn
    # Check if running in Docker or locally for host binding
    host = os.getenv("DOCKER_HOST", "127.0.0.1") # Default to localhost if not in Docker
    uvicorn.run(app, host=host, port=8080)