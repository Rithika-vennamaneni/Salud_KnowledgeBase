from azure_pdf_uploader import AzurePDFUploader
import os
from dotenv import load_dotenv

# Load your settings from .env file
load_dotenv()

# Get connection string
connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

print("Testing Azure connection...")
print("-" * 50)

try:
    # Try to connect
    uploader = AzurePDFUploader(connection_string, "insurance-pdfs")
    print("✅ SUCCESS! Connected to Azure Storage!")
    print("✅ Container 'insurance-pdfs' is ready!")
    
    # List any existing files
    pdfs = uploader.list_pdfs()
    print(f"✅ Found {len(pdfs)} existing PDFs in storage")
    
except Exception as e:
    print(f"❌ ERROR: {e}")
    print("\nPlease check:")
    print("1. Your .env file exists")
    print("2. Your connection string is correct")
    print("3. You have internet connection")