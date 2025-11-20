"""
Test extraction on your actual PDF
"""

import json
import os
from llm_pdf_extractor import LLMPDFExtractor

# Your actual PDF path (from find_pdfs.py output)
test_pdf = "./payer_pdfs/anthem/OH_CAID_ProviderManual.pdf"

print("="*60)
print("Testing Groq LLM PDF Extraction")
print("="*60)

# Check if file exists
if not os.path.exists(test_pdf):
    print(f"\n❌ PDF not found: {test_pdf}")
    print("\nLet me search for PDFs...")
    
    # Auto-find
    for root, dirs, files in os.walk("."):
        for file in files:
            if file.endswith('.pdf'):
                test_pdf = os.path.join(root, file)
                print(f"✓ Found: {test_pdf}")
                break
        if test_pdf:
            break

if not os.path.exists(test_pdf):
    print("\n❌ No PDFs found in project!")
    print("Run your crawler first: python3 payer_portal_crawler.py")
    exit(1)

print(f"\n✓ Using PDF: {test_pdf}")
print(f"   Size: {round(os.path.getsize(test_pdf) / (1024*1024), 2)} MB")

# Check for Groq API key
if not os.getenv("GROQ_API_KEY"):
    print("\n⚠️  Loading .env file...")
    from dotenv import load_dotenv
    load_dotenv()
    
    if not os.getenv("GROQ_API_KEY"):
        print("\n❌ GROQ_API_KEY not found!")
        print("\nCreate a .env file with:")
        print("GROQ_API_KEY=your_key_here")
        print("\nGet key from: https://console.groq.com")
        exit(1)

print("✓ Groq API key found\n")

# Initialize extractor
print("Initializing Groq extractor...")
extractor = LLMPDFExtractor(model="llama-3.3-70b-versatile")

print("Starting extraction (this may take 30-60 seconds)...\n")

# Extract
result = extractor.extract_to_json(test_pdf)

# Display results
print("\n" + "="*60)
print("EXTRACTION RESULTS:")
print("="*60)
print(json.dumps(result, indent=2))

# Save to file
output_file = "test_output.json"
with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(result, f, indent=2, ensure_ascii=False)

print(f"\n✓ Results saved to: {output_file}")
print("="*60)