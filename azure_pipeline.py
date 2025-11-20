# azure_pipeline.py
import os
import uuid
from datetime import datetime, timedelta
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from azure.ai.documentintelligence import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
import psycopg2
from psycopg2.extras import Json

load_dotenv()

# ---------------- ENV ----------------
AZ_BLOB_CONN = os.getenv("AZURE_STORAGE_CONNSTRING")
AZ_CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "pdfs")
DOCINT_ENDPOINT = os.getenv("AZURE_DOCINT_ENDPOINT")
DOCINT_KEY = os.getenv("AZURE_DOCINT_KEY")
MODEL_ID = os.getenv("PROCESSOR_MODEL", "prebuilt-document")

PGHOST = os.getenv("PGHOST")
PGPORT = os.getenv("PGPORT", "5432")
PGDATABASE = os.getenv("PGDATABASE")
PGUSER = os.getenv("PGUSER")
PGPASSWORD = os.getenv("PGPASSWORD")

# ---------------- Clients ----------------
blob_service = BlobServiceClient.from_connection_string(AZ_BLOB_CONN)
container_client = blob_service.get_container_client(AZ_CONTAINER)
doc_client = DocumentAnalysisClient(endpoint=DOCINT_ENDPOINT, credential=AzureKeyCredential(DOCINT_KEY))

# ---------------- Blob upload ----------------
def upload_to_blob(local_path) -> str:
    filename = os.path.basename(local_path)
    blob_name = f"{datetime.utcnow().strftime('%Y%m%d')}/{uuid.uuid4()}-{filename}"
    with open(local_path, "rb") as data:
        container_client.upload_blob(name=blob_name, data=data, overwrite=True)
    return blob_name

def generate_sas_url(blob_name, minutes=30):
    acct_name = blob_service.account_name
    # account_key is available in connection string; BlobServiceClient doesn't expose it directly.
    # For production, generate SAS via azure.identity Managed Identity or via storage account key from env.
    account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    if not account_key:
        raise RuntimeError("AZURE_STORAGE_ACCOUNT_KEY missing; required for SAS generation.")
    sas = generate_blob_sas(
        account_name=acct_name,
        container_name=AZ_CONTAINER,
        blob_name=blob_name,
        account_key=account_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(minutes=minutes)
    )
    return f"https://{acct_name}.blob.core.windows.net/{AZ_CONTAINER}/{blob_name}?{sas}"

# ---------------- Document Intelligence (bytes) ----------------
def analyze_bytes(local_path):
    with open(local_path, "rb") as f:
        poller = doc_client.begin_analyze_document(MODEL_ID, document=f)
        result = poller.result()
    try:
        return result.to_dict()
    except Exception:
        # minimal fallback
        pages = [p.to_dict() for p in getattr(result, "pages", [])]
        return {"pages": pages}

# ---------------- Save to Postgres ----------------
def save_metadata_and_json(meta, analysis_json):
    conn = psycopg2.connect(
        host=PGHOST, port=PGPORT, dbname=PGDATABASE, user=PGUSER, password=PGPASSWORD, sslmode="require"
    )
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO documents
                    (payer_id, filename, file_path, file_size_bytes, document_type, original_url, downloaded_at, processed_at, azure_document_id, created_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    RETURNING document_id
                """, (
                    meta.get("payer_id"),
                    meta["filename"],
                    meta["blob_path"],
                    meta.get("file_size_bytes"),
                    meta.get("document_type", "pdf"),
                    meta.get("original_url"),
                    meta.get("downloaded_at"),
                    datetime.utcnow(),
                    meta.get("azure_document_id"),
                    datetime.utcnow()
                ))
                doc_id = cur.fetchone()[0]

                cur.execute("""
                    INSERT INTO document_intelligence_results
                    (document_id, azure_model_used, confidence_score, page_count, processing_time_seconds, raw_response, structured_data, extraction_timestamp)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    doc_id,
                    MODEL_ID,
                    None,
                    len(analysis_json.get("pages", [])),
                    None,
                    Json(analysis_json),
                    Json(analysis_json),
                    datetime.utcnow()
                ))
        return doc_id
    finally:
        conn.close()

# ---------------- Orchestration ----------------
def process_local_pdf(local_path, payer_id=None, original_url=None, upload_blob=True, use_sas_for_docint=False):
    # Upload to blob (optional)
    blob_path = None
    if upload_blob:
        blob_path = upload_to_blob(local_path)
    meta = {
        "payer_id": payer_id,
        "filename": os.path.basename(local_path),
        "blob_path": blob_path,
        "file_size_bytes": os.path.getsize(local_path) if os.path.exists(local_path) else None,
        "document_type": "pdf",
        "original_url": original_url,
        "downloaded_at": datetime.utcnow().isoformat(),
        "azure_document_id": None
    }

    # Analyze
    if use_sas_for_docint and blob_path:
        sas_url = generate_sas_url(blob_path, minutes=60)
        # Some SDK versions provide begin_analyze_document_from_url; otherwise you can call REST API.
        poller = doc_client.begin_analyze_document_from_url(MODEL_ID, sas_url)
        result = poller.result()
        try:
            analysis_json = result.to_dict()
        except Exception:
            analysis_json = {"pages": [p.to_dict() for p in getattr(result, "pages", [])]}
    else:
        analysis_json = analyze_bytes(local_path)

    # Save to DB
    doc_id = save_metadata_and_json(meta, analysis_json)
    return doc_id, blob_path, analysis_json
