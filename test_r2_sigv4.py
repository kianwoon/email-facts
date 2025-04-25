import os
import requests
from botocore.session import Session
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from dotenv import load_dotenv
import datetime

# --- Configuration ---
# Load environment variables from .env file
load_dotenv()

# Get R2 details from environment variables
R2_CATALOG_URI = os.getenv("R2_CATALOG_URI")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
S3_REGION = os.getenv("S3_REGION", "auto") # Default to 'auto' if not set

# --- Validation ---
if not all([R2_CATALOG_URI, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, S3_REGION]):
    print("Error: Missing one or more required environment variables:")
    print(f"  R2_CATALOG_URI: {'Set' if R2_CATALOG_URI else 'Missing'}")
    print(f"  R2_ACCESS_KEY_ID: {'Set' if R2_ACCESS_KEY_ID else 'Missing'}")
    print(f"  R2_SECRET_ACCESS_KEY: {'Set' if R2_SECRET_ACCESS_KEY else 'Missing'}")
    print(f"  S3_REGION: {'Set' if S3_REGION else 'Missing (defaulted to auto?)'}")
    exit(1)

# Construct the target URL (config endpoint)
# Ensure the URI doesn't have a trailing slash before appending path
base_uri = R2_CATALOG_URI.rstrip('/')
target_url = f"{base_uri}/v1/config" # Endpoint PyIceberg is failing on

print(f"--- Testing SigV4 Request ---")
print(f"Target URL: {target_url}")
print(f"Region: {S3_REGION}")
print(f"Access Key ID: {R2_ACCESS_KEY_ID[:5]}...{R2_ACCESS_KEY_ID[-4:]}") # Log partial key

# --- SigV4 Signing ---
# Prepare the request object for botocore
request = AWSRequest(method='GET', url=target_url, data=None)

# Create botocore session and credentials
session = Session()
credentials = session.get_credentials()
# OVERRIDE credentials with our R2 values if standard AWS chain didn't pick them up
# (Boto3 might pick up R2 keys if set as AWS_ACCESS_KEY_ID etc. in env)
# Forcing specific keys is safer for testing:
from botocore.credentials import Credentials
r2_creds = Credentials(access_key=R2_ACCESS_KEY_ID, secret_key=R2_SECRET_ACCESS_KEY)

# Create the SigV4 signer
# Service name is 's3' for R2 as it's S3-compatible
sigv4_signer = SigV4Auth(r2_creds, 's3', S3_REGION)

# Add the signature headers to the request object
sigv4_signer.add_auth(request)

# --- Make Request ---
# Prepare headers for the actual HTTP request using 'requests' library
# Botocore's request.headers are prepared headers for signing, not final request headers
prepared_headers = dict(request.headers.items())

print(f"Sending GET request to {target_url}...")
# Use requests library to send the prepared request
try:
    response = requests.get(target_url, headers=prepared_headers)

    print(f"--- Response ---")
    print(f"Status Code: {response.status_code}")
    print(f"Headers: {response.headers}")
    try:
        # Try parsing as JSON if possible
        response_json = response.json()
        print(f"Body (JSON): {response_json}")
    except requests.exceptions.JSONDecodeError:
        # Print as text if not JSON
        print(f"Body (Text): {response.text}")

    if response.status_code == 200:
        print("\\nSUCCESS: Received a 200 OK response. Credentials and signing seem correct.")
    elif response.status_code == 403:
        print("\\nFAILURE: Received a 403 Forbidden. Check credentials, permissions, and region.")
        print("           Ensure the R2 API Token has BOTH 'R2 Data Catalog' and 'R2 Storage' permissions.")
    else:
        print(f"\\nWARNING: Received unexpected status code {response.status_code}.")

except requests.exceptions.RequestException as e:
    print(f"\\n--- Request Exception ---")
    print(f"Error during request: {e}")

except Exception as e:
    print(f"\\n--- Unexpected Error ---")
    print(f"An unexpected error occurred: {e}")
    import traceback
    traceback.print_exc() 