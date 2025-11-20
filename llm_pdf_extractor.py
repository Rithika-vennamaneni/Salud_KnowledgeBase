"""
Groq LLM-Based PDF to JSON Extractor with Chunking for Large PDFs
Handles healthcare insurance documents of any size
"""

import os
import json
import pdfplumber
from groq import Groq
from typing import Dict, List, Optional
from tenacity import retry, stop_after_attempt, wait_exponential
from dotenv import load_dotenv
import logging
import time

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LLMPDFExtractor:
    """Extract structured JSON from healthcare insurance PDFs using Groq LLM"""
    
    def __init__(self, model: str = "llama-3.3-70b-versatile"):
        """
        Initialize the Groq LLM PDF extractor
        
        Args:
            model: Groq model to use
        """
        api_key = os.getenv("GROQ_API_KEY")
        if not api_key:
            raise ValueError("GROQ_API_KEY not found in environment variables")
        
        self.client = Groq(api_key=api_key)
        self.model = model
        self.max_chars_per_chunk = 20000  # Safe limit for Groq free tier
    
    def extract_text_from_pdf(self, pdf_path: str) -> str:
        """Extract text from PDF using pdfplumber"""
        try:
            text_content = []
            with pdfplumber.open(pdf_path) as pdf:
                for page_num, page in enumerate(pdf.pages, 1):
                    text = page.extract_text()
                    if text:
                        text_content.append(f"--- Page {page_num} ---\n{text}")
            
            full_text = "\n\n".join(text_content)
            logger.info(f"Extracted {len(full_text)} characters from {pdf_path}")
            return full_text
        
        except Exception as e:
            logger.error(f"Error extracting text from {pdf_path}: {str(e)}")
            return ""
    
    def chunk_text(self, text: str, max_chars: int) -> List[str]:
        """
        Split text into chunks that fit within token limits
        
        Args:
            text: Full text to chunk
            max_chars: Maximum characters per chunk
            
        Returns:
            List of text chunks
        """
        if len(text) <= max_chars:
            return [text]
        
        chunks = []
        pages = text.split("--- Page")
        current_chunk = ""
        
        for page in pages:
            if not page.strip():
                continue
                
            page_text = "--- Page" + page if page != pages[0] else page
            
            # If single page is too large, split it
            if len(page_text) > max_chars:
                if current_chunk:
                    chunks.append(current_chunk)
                    current_chunk = ""
                
                # Split large page into smaller parts
                words = page_text.split()
                temp_chunk = ""
                for word in words:
                    if len(temp_chunk) + len(word) + 1 <= max_chars:
                        temp_chunk += word + " "
                    else:
                        if temp_chunk:
                            chunks.append(temp_chunk)
                        temp_chunk = word + " "
                if temp_chunk:
                    chunks.append(temp_chunk)
            
            # Add page to current chunk if it fits
            elif len(current_chunk) + len(page_text) <= max_chars:
                current_chunk += page_text
            else:
                chunks.append(current_chunk)
                current_chunk = page_text
        
        if current_chunk:
            chunks.append(current_chunk)
        
        logger.info(f"Split document into {len(chunks)} chunks")
        return chunks
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def extract_chunk_to_json(self, text_chunk: str, chunk_num: int, total_chunks: int) -> Dict:
        """Extract structured JSON from a text chunk using Groq LLM"""
        
        schema = self._get_default_schema()
        prompt = self._create_chunk_extraction_prompt(text_chunk, chunk_num, total_chunks, schema)
        
        try:
            logger.info(f"Processing chunk {chunk_num}/{total_chunks} with Groq...")
            
            chat_completion = self.client.chat.completions.create(
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert healthcare insurance document parser. Extract all relevant information accurately and return ONLY valid JSON."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                model=self.model,
                temperature=0.1,
                max_tokens=4000,
                response_format={"type": "json_object"}
            )
            
            response_text = chat_completion.choices[0].message.content
            result = json.loads(response_text)
            
            # Add rate limiting delay
            time.sleep(1)  # Respect Groq rate limits
            
            return result
        
        except Exception as e:
            logger.error(f"Error processing chunk {chunk_num}: {str(e)}")
            return {"error": str(e), "chunk": chunk_num}
    
    def merge_chunk_results(self, chunk_results: List[Dict]) -> Dict:
        """Merge results from multiple chunks into single JSON"""
        
        merged = {
            "insurance_provider": None,
            "plan_name": None,
            "plan_type": None,
            "document_type": None,
            "effective_date": None,
            "state_specific": None,
            "coverage_details": {},
            "prior_authorization": {},
            "timely_filing": {},
            "appeals_process": {},
            "key_requirements": [],
            "contact_information": {},
            "additional_info": []
        }
        
        for chunk_result in chunk_results:
            if "error" in chunk_result:
                continue
            
            # Merge top-level fields (first non-null value wins)
            for key in ["insurance_provider", "plan_name", "plan_type", "document_type", 
                       "effective_date", "state_specific"]:
                if key in chunk_result and chunk_result[key] and not merged[key]:
                    merged[key] = chunk_result[key]
            
            # Merge nested objects
            for key in ["coverage_details", "prior_authorization", "timely_filing", 
                       "appeals_process", "contact_information"]:
                if key in chunk_result and isinstance(chunk_result[key], dict):
                    merged[key].update(chunk_result[key])
            
            # Merge lists
            if "key_requirements" in chunk_result and isinstance(chunk_result["key_requirements"], list):
                merged["key_requirements"].extend(chunk_result["key_requirements"])
            
            # Collect any additional info
            for key, value in chunk_result.items():
                if key not in merged and value:
                    if isinstance(value, list):
                        merged["additional_info"].extend(value)
                    elif isinstance(value, str):
                        merged["additional_info"].append(f"{key}: {value}")
        
        # Deduplicate lists
        if merged["key_requirements"]:
            merged["key_requirements"] = list(set(merged["key_requirements"]))
        
        return merged
    
    def extract_to_json(self, pdf_path: str, custom_schema: Optional[Dict] = None) -> Dict:
        """
        Extract structured JSON from PDF (handles large PDFs with chunking)
        
        Args:
            pdf_path: Path to the PDF file
            custom_schema: Optional custom JSON schema
            
        Returns:
            Structured JSON dictionary
        """
        # Extract text
        pdf_text = self.extract_text_from_pdf(pdf_path)
        
        if not pdf_text:
            return {"error": "Failed to extract text from PDF", "file": pdf_path}
        
        # Check if we need chunking
        if len(pdf_text) <= self.max_chars_per_chunk:
            logger.info("Document fits in single chunk, processing...")
            result = self.extract_chunk_to_json(pdf_text, 1, 1)
        else:
            # Chunk and process
            chunks = self.chunk_text(pdf_text, self.max_chars_per_chunk)
            logger.info(f"Processing {len(chunks)} chunks...")
            
            chunk_results = []
            for i, chunk in enumerate(chunks, 1):
                logger.info(f"Processing chunk {i}/{len(chunks)}...")
                chunk_result = self.extract_chunk_to_json(chunk, i, len(chunks))
                chunk_results.append(chunk_result)
            
            # Merge results
            logger.info("Merging chunk results...")
            result = self.merge_chunk_results(chunk_results)
        
        # Add metadata
        result["_metadata"] = {
            "source_file": os.path.basename(pdf_path),
            "file_size_mb": round(os.path.getsize(pdf_path) / (1024*1024), 2),
            "model_used": self.model,
            "extraction_method": "groq_llm_chunked" if len(pdf_text) > self.max_chars_per_chunk else "groq_llm"
        }
        
        return result
    
    def _get_default_schema(self) -> Dict:
        """Get default JSON schema for healthcare insurance documents"""
        return {
            "insurance_provider": "string",
            "plan_name": "string",
            "plan_type": "string",
            "document_type": "string",
            "effective_date": "string",
            "state_specific": "string",
            "coverage_details": {
                "deductible": "string",
                "copay": "string",
                "coinsurance": "string",
                "out_of_pocket_max": "string",
                "covered_services": ["list"]
            },
            "prior_authorization": {
                "required_for": ["list"],
                "submission_method": "string",
                "turnaround_time": "string"
            },
            "timely_filing": {
                "deadline": "string",
                "calculation_method": "string"
            },
            "appeals_process": {
                "timeline": "string",
                "levels": ["list"],
                "submission_method": "string"
            },
            "key_requirements": ["list of important requirements"],
            "contact_information": {
                "phone": "string",
                "fax": "string",
                "email": "string",
                "portal": "string"
            }
        }
    
    def _create_chunk_extraction_prompt(self, text_chunk: str, chunk_num: int, 
                                       total_chunks: int, schema: Dict) -> str:
        """Create extraction prompt for a chunk"""
        
        schema_str = json.dumps(schema, indent=2)
        
        chunk_context = f"This is chunk {chunk_num} of {total_chunks} from the document." if total_chunks > 1 else ""
        
        prompt = f"""Extract structured information from this healthcare insurance document chunk and return it as JSON.

{chunk_context}

**IMPORTANT:**
1. Return ONLY valid JSON, no markdown, no explanations
2. If information is not found, use null
3. Extract ALL relevant information from this chunk
4. Be precise with numbers, dates, and requirements

**TARGET SCHEMA:**
{schema_str}

**DOCUMENT TEXT:**
{text_chunk}

Return extracted information as valid JSON:"""
        
        return prompt
    
    def batch_process(self, pdf_paths: List[str], output_dir: str = "extracted_json") -> Dict:
        """Process multiple PDFs and save results"""
        os.makedirs(output_dir, exist_ok=True)
        
        results = {
            "total": len(pdf_paths),
            "successful": 0,
            "failed": 0,
            "files": []
        }
        
        for i, pdf_path in enumerate(pdf_paths, 1):
            logger.info(f"\nProcessing {i}/{len(pdf_paths)}: {pdf_path}")
            
            try:
                json_data = self.extract_to_json(pdf_path)
                
                filename = os.path.splitext(os.path.basename(pdf_path))[0]
                output_path = os.path.join(output_dir, f"{filename}.json")
                
                with open(output_path, 'w', encoding='utf-8') as f:
                    json.dump(json_data, f, indent=2, ensure_ascii=False)
                
                logger.info(f"✓ Saved to {output_path}")
                
                results["successful"] += 1
                results["files"].append({
                    "input": pdf_path,
                    "output": output_path,
                    "status": "success"
                })
                
            except Exception as e:
                logger.error(f"✗ Failed: {str(e)}")
                results["failed"] += 1
                results["files"].append({
                    "input": pdf_path,
                    "output": None,
                    "status": "failed",
                    "error": str(e)
                })
        
        summary_path = os.path.join(output_dir, "_batch_summary.json")
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"\n{'='*50}")
        logger.info(f"Batch Processing Complete!")
        logger.info(f"Successful: {results['successful']}/{results['total']}")
        logger.info(f"Failed: {results['failed']}/{results['total']}")
        logger.info(f"Summary: {summary_path}")
        logger.info(f"{'='*50}")
        
        return results