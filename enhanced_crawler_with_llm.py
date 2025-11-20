"""
Enhanced Crawler with LLM Integration
Combines your existing crawler with Groq-based JSON extraction
"""

import os
import json
from llm_pdf_extractor import LLMPDFExtractor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EnhancedPayerCrawler:
    """
    Integrates your existing crawler with LLM-based extraction
    """
    
    def __init__(self):
        self.llm_extractor = LLMPDFExtractor(model="llama-3.3-70b-versatile")
    
    def process_downloaded_pdfs(self, pdf_directory: str, output_dir: str = "knowledge_base_json"):
        """
        Process all PDFs from your crawler output
        
        Args:
            pdf_directory: Directory containing downloaded PDFs
            output_dir: Directory to save extracted JSON
        """
        # Get all PDF files
        pdf_files = []
        for root, dirs, files in os.walk(pdf_directory):
            for file in files:
                if file.endswith('.pdf'):
                    pdf_files.append(os.path.join(root, file))
        
        logger.info(f"Found {len(pdf_files)} PDFs to process")
        
        # Batch process with LLM
        results = self.llm_extractor.batch_process(pdf_files, output_dir)
        
        return results
    
    def crawl_and_extract(self, payer_name: str, use_existing_crawler: str = "basic"):
        """
        Complete workflow: Crawl + Extract
        
        Args:
            payer_name: Name of the payer (e.g., "anthem", "uhc")
            use_existing_crawler: Which crawler to use ("basic", "csv", "bfs")
        """
        logger.info(f"Starting crawl and extraction for: {payer_name}")
        
        # STEP 1: Use your existing crawler
        if use_existing_crawler == "basic":
            from payer_portal_crawler import PayerPortalCrawler
            crawler = PayerPortalCrawler()
            crawl_results = crawler.crawl_payer(payer_name)
            pdf_dir = f"downloads/{payer_name}"
        
        elif use_existing_crawler == "bfs":
            # Use your BFS crawler
            logger.info("Using BFS crawler - implement your BFS crawl here")
            pdf_dir = f"downloads/{payer_name}"
        
        else:
            logger.error("Invalid crawler type")
            return None
        
        # STEP 2: Extract with LLM
        logger.info("Starting LLM extraction...")
        extraction_results = self.process_downloaded_pdfs(
            pdf_directory=pdf_dir,
            output_dir=f"knowledge_base/{payer_name}"
        )
        
        return {
            "crawl_results": crawl_results,
            "extraction_results": extraction_results
        }


def integrate_with_existing_system():
    """
    Example: Integrate with your existing downloaded PDFs
    """
    
    # Initialize
    enhanced_crawler = EnhancedPayerCrawler()
    
    # Option 1: Process already downloaded PDFs
    print("\n" + "="*60)
    print("OPTION 1: Process existing downloaded PDFs")
    print("="*60)
    
    # Update this path to where your PDFs are downloaded
    results = enhanced_crawler.process_downloaded_pdfs(
        pdf_directory="downloads",  # Your existing download folder
        output_dir="knowledge_base_json"
    )
    
    print(f"\n✓ Processed {results['successful']} PDFs successfully")
    print(f"✗ Failed: {results['failed']} PDFs")
    
    # Option 2: Full workflow (crawl + extract)
    # Uncomment to use:
    # print("\n" + "="*60)
    # print("OPTION 2: Full Crawl + Extract Workflow")
    # print("="*60)
    # 
    # full_results = enhanced_crawler.crawl_and_extract(
    #     payer_name="anthem",
    #     use_existing_crawler="basic"
    # )


if __name__ == "__main__":
    integrate_with_existing_system()