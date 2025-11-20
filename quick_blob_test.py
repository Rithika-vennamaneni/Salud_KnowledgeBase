# quick_blob_test.py
import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, ContainerClient
from datetime import datetime
load_dotenv()

conn = os.getenv("AZURE_STORAGE_CONNSTRING")
container = os.getenv("AZURE_STORAGE_CONTAINER", "pdfs")
if not conn:
    raise SystemExit("AZURE_STORAGE_CONNSTRING missing in .env")

bs = BlobServiceClient.from_connection_string(conn)
container_client = bs.get_container_client(container)

# create if not exists
try:
    container_client.create_container()
except Exception:
    pass

local = "sample.pdf"
if not os.path.exists(local):
    with open(local, "wb") as f:
        f.write(b"%PDF-1.4\n% sample pdf for test\n")  # tiny fake pdf

blob_name = f"quicktest/{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}-sample.pdf"
with open(local, "rb") as data:
    container_client.upload_blob(name=blob_name, data=data, overwrite=True)
print("Uploaded blob:", blob_name)

print("\nListing blobs in container:", container)
for b in container_client.list_blobs(name_starts_with="quicktest/"):
    print(b.name, b.size, b.last_modified)
