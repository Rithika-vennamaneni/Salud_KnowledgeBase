"""
Test script to verify everything works
"""
from azure_integration import PDFToStructuredPipeline
from config import AZURE_CONNECTION_STRING
import os

def test_basic_integration():
    """Test basic pipeline functionality"""
    
    print("Testing Azure connection...")
    
    # Initialize pipeline
    pipeline = PDFToStructuredPipeline(AZURE_CONNECTION_STRING)
    
    print("✓ Azure connection successful!")
    print("✓ Containers created")
    
    # Test with existing PDFs
    pdf_dir = "./pdfs"  # Your existing PDF directory
    
    if os.path.exists(pdf_dir):
        pdf_files = [f for f in os.listdir(pdf_dir) if f.endswith('.pdf')]
        
        if pdf_files:
            print(f"\nFound {len(pdf_files)} PDFs to process")
            
            # Process first PDF as test
            test_pdf = os.path.join(pdf_dir, pdf_files[0])
            print(f"Testing with: {test_pdf}")
            
            policies = pipeline.process_single_pdf(
                pdf_path=test_pdf,
                payer_name="anthem",  # Change to actual payer
                source_url="test"
            )
            
            print(f"✓ Extracted {len(policies)} policies")
            
            if policies:
                print(f"\nSample policy:")
                print(f"  ID: {policies[0].policy_id}")
                print(f"  Type: {policies[0].policy_type}")
                print(f"  Confidence: {policies[0].confidence_score}")
                print(f"  Effective Date: {policies[0].effective_date}")
        else:
            print("No PDFs found in ./pdfs directory")
    else:
        print("./pdfs directory not found")
        print("Run your crawler first to download PDFs")

if __name__ == "__main__":
    test_basic_integration()