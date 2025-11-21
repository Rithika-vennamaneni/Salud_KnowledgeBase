"""
Quick test script to verify RAG setup is working correctly
Run this after installing dependencies
"""

from rag_implementation import RAGPipeline, HealthcarePolicyBot
import os
from pathlib import Path

def test_basic_setup():
    """Test 1: Basic setup and initialization"""
    print("="*70)
    print("TEST 1: Basic Setup")
    print("="*70)
    
    try:
        rag = RAGPipeline(persist_directory="./test_vector_db")
        print("✅ RAG Pipeline initialized successfully")
        return rag
    except Exception as e:
        print(f"❌ Error initializing RAG: {e}")
        return None

def test_json_processing(rag, json_dir="./final_json_output"):
    """Test 2: Process JSON files"""
    print("\n" + "="*70)
    print("TEST 2: Processing JSON Files")
    print("="*70)
    
    # Check if directory exists
    if not Path(json_dir).exists():
        print(f"❌ Directory not found: {json_dir}")
        print("Please update the path to your JSON files")
        return False
    
    json_files = list(Path(json_dir).glob("*.json"))
    print(f"Found {len(json_files)} JSON files")
    
    if len(json_files) == 0:
        print("❌ No JSON files found")
        return False
    
    try:
        # Process files
        chunks = rag.process_json_files(json_dir)
        print(f"✅ Successfully processed {len(chunks)} chunks")
        return True
    except Exception as e:
        print(f"❌ Error processing files: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_semantic_search(rag):
    """Test 3: Semantic search"""
    print("\n" + "="*70)
    print("TEST 3: Semantic Search")
    print("="*70)
    
    test_queries = [
        "prior authorization",
        "medical records submission",
        "claim deadlines"
    ]
    
    for query in test_queries:
        print(f"\n🔍 Query: '{query}'")
        try:
            results = rag.query(query, n_results=2)
            print(f"✅ Found {len(results['relevant_chunks'])} relevant chunks")
            
            # Show first result snippet
            if results['relevant_chunks']:
                first_result = results['relevant_chunks'][0]
                print(f"   Top result from: {first_result['metadata']['source_file']}")
                print(f"   Preview: {first_result['text'][:150]}...")
        except Exception as e:
            print(f"❌ Search failed: {e}")
    
    return True

def test_with_claude(rag):
    """Test 4: Claude integration (optional)"""
    print("\n" + "="*70)
    print("TEST 4: Claude Integration (Optional)")
    print("="*70)
    
    api_key = os.getenv("ANTHROPIC_API_KEY")
    
    if not api_key:
        print("⚠️  No ANTHROPIC_API_KEY found in environment")
        print("   Skipping Claude test (this is optional)")
        print("   To enable: export ANTHROPIC_API_KEY='your-key-here'")
        return True
    
    try:
        bot = HealthcarePolicyBot(api_key=api_key, rag_pipeline=rag)
        print("✅ Bot initialized successfully")
        
        # Test question
        print("\n🤖 Testing bot with question...")
        answer = bot.ask("What requires prior authorization?")
        print("\nBot answer:")
        print(answer)
        print("\n✅ Claude integration working!")
        
    except Exception as e:
        print(f"❌ Claude test failed: {e}")
        return False
    
    return True

def show_database_stats(rag):
    """Show final database statistics"""
    print("\n" + "="*70)
    print("DATABASE STATISTICS")
    print("="*70)
    
    stats = rag.vector_store.get_collection_stats()
    print(f"Total chunks in database: {stats['total_chunks']}")
    print(f"Collection name: {stats['collection_name']}")
    
    # Show example queries
    print("\n📝 Example queries you can try:")
    example_queries = [
        "What requires prior authorization?",
        "How do I submit claims?",
        "What are the deadlines?",
        "What documentation is needed?",
        "How do I verify eligibility?"
    ]
    for i, query in enumerate(example_queries, 1):
        print(f"   {i}. {query}")

def main():
    """Run all tests"""
    print("\n" + "🏥 HEALTHCARE POLICY BOT - RAG SETUP TEST " + "\n")
    
    # Test 1: Basic setup
    rag = test_basic_setup()
    if not rag:
        print("\n❌ Setup failed. Please check dependencies.")
        return
    
    # Test 2: Process JSON
    success = test_json_processing(rag, json_dir="./final_json_output")
    if not success:
        print("\n⚠️  Could not process JSON files.")
        print("Please ensure:")
        print("1. Your JSON files are in './final_json_output' directory")
        print("2. Or update the path in this script")
        return
    
    # Test 3: Search
    test_semantic_search(rag)
    
    # Test 4: Claude (optional)
    test_with_claude(rag)
    
    # Show stats
    show_database_stats(rag)
    
    print("\n" + "="*70)
    print("✅ ALL TESTS COMPLETE!")
    print("="*70)
    print("\nNext steps:")
    print("1. Try your own queries using rag.query('your question')")
    print("2. Build a UI with Streamlit or Gradio")
    print("3. Deploy as a web service")
    print("\n")

if __name__ == "__main__":
    # Optional: Load environment variables from .env file
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    
    main()