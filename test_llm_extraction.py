"""
Quick test script for LLM PDF extraction
"""

import json
from llm_pdf_extractor import LLMPDFExtractor

def test_single_pdf():
    """Test extraction on a single PDF"""
    
    print("="*60)
    print("Testing Groq LLM PDF Extraction")
    print("="*60)
    
    # Initialize extractor
    extractor = LLMPDFExtractor(model="llama-3.3-70b-versatile")
    
    # Test with one of your downloaded PDFs
    # UPDATE THIS PATH to one of your actual PDF files
    test_pdf = "downloads/anthem/sample_document.pdf"
    
    print(f"\nExtracting from: {test_pdf}\n")
    
    # Extract
    result = extractor.extract_to_json(test_pdf)
    
    # Display results
    print("="*60)
    print("EXTRACTION RESULTS:")
    print("="*60)
    print(json.dumps(result, indent=2))
    
    # Save to file
    output_file = "test_output.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    print(f"\n✓ Results saved to: {output_file}")


def test_batch_processing():
    """Test batch processing on multiple PDFs"""
    
    print("="*60)
    print("Testing Batch Processing")
    print("="*60)
    
    # Initialize extractor
    extractor = LLMPDFExtractor(model="llama-3.3-70b-versatile")
    
    # List your PDF files
    # UPDATE THESE PATHS to your actual PDF files
    pdf_files = [
        "downloads/anthem/doc1.pdf",
        "downloads/anthem/doc2.pdf",
        "downloads/anthem/doc3.pdf"
    ]
    
    # Batch process
    results = extractor.batch_process(pdf_files, output_dir="test_json_output")
    
    print("\n" + "="*60)
    print("BATCH RESULTS:")
    print("="*60)
    print(f"Total PDFs: {results['total']}")
    print(f"Successful: {results['successful']}")
    print(f"Failed: {results['failed']}")


if __name__ == "__main__":
    # Test single PDF first
    test_single_pdf()
    
    # Then test batch (uncomment when ready)
    # test_batch_processing()