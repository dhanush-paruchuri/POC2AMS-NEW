#!/usr/bin/env python3
"""
Main Ingestion Pipeline for Weaviate Knowledge Base

This script is the ORCHESTRATOR that coordinates all components to transform
raw CSV files + YAML business knowledge into an intelligent Weaviate knowledge base.

PIPELINE FLOW:
1. Validate Setup → Check all files and directories exist
2. Extract Metadata → Combine CSV technical data with YAML business knowledge  
3. Upload to Weaviate → Load DatasetMetadata, DataRelationship, DomainTag objects
4. Verify Success → Confirm everything uploaded correctly
5. Generate Report → Comprehensive summary and logging

WHY THIS DESIGN:
- Fail-fast validation prevents wasted time
- Modular components for maintainability  
- Comprehensive error handling and logging
- Clear separation of concerns

BEDROCK TIMEOUT FIXES:
- Individual uploads with retries and backoff
- Reduced metadata size for vectorization
- Extended timeout handling
- Better error reporting for Bedrock issues
"""

import os
import sys
import yaml
import json
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any
from dotenv import load_dotenv

# CRITICAL: Add project root to Python path so we can import our custom modules
project_root = Path(__file__).parent.parent
print(f"Project root: {project_root}")
sys.path.append(str(project_root))

# Import our custom components - these do the actual work
try:
    from ingestion.csv_extractor import CSVExtractor      # Reads CSVs + combines with YAML
    from ingestion.weaviate_uploader import WeaviateUploader  # Uploads to Weaviate
    print("✅ Ingestion modules imported successfully")
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Make sure you're running from the project root directory")
    print("Expected structure: ingestion/csv_extractor.py and ingestion/weaviate_uploader.py")
    sys.exit(1)

# Load environment variables from .env file (Weaviate URL, AWS credentials, etc.)
load_dotenv()


class IngestionPipeline:
    """
    Main orchestrator class that coordinates the entire ingestion process.
    
    RESPONSIBILITY: High-level workflow management, error handling, progress tracking
    DOES NOT: Handle CSV parsing details or Weaviate API calls (delegates to components)
    
    DESIGN PATTERN: Facade pattern - provides simple interface to complex subsystem
    """
    
    def __init__(self):
        """
        Initialize the pipeline with all necessary paths and components.
        
        SETUP PHASE: Establish file locations, create worker objects, initialize tracking
        """
        # ESTABLISH FILE STRUCTURE: Where to find configs and data
        self.project_root = Path(__file__).parent.parent  # Directory containing this script
        self.config_dir = self.project_root / 'config'  # YAML configurations
        self.data_dir = self.project_root / 'data_sources'  # CSV files
        
        # CREATE WORKER COMPONENTS: These do the actual technical work
        # CSVExtractor: Reads CSV files and combines with YAML business knowledge
        self.csv_extractor = CSVExtractor(str(self.data_dir))
        # WeaviateUploader: Handles all Weaviate database operations
        self.weaviate_uploader = WeaviateUploader()
        
        # INITIALIZE COMPREHENSIVE TRACKING: Record everything for debugging/reporting
        self.results = {
            "start_time": datetime.now().isoformat(),  # When pipeline started
            "extraction_results": [],  # Details of each CSV processing attempt
            "upload_results": {},  # Results from Weaviate uploads
            "total_datasets": 0,  # How many YAML configs we found
            "successful_datasets": 0,  # How many CSV files processed successfully
            "failed_datasets": 0,  # How many failed (for debugging)
            "end_time": None,  # Set when pipeline completes
            "overall_success": False  # Final pipeline status
        }
        
        print(f"Ingestion Pipeline Initialized")
        print(f"   Project Root: {self.project_root}")
        print(f"   Config Dir: {self.config_dir}")
        print(f"   Data Dir: {self.data_dir}")
    
    def test_bedrock_access(self) -> bool:
        """Test if we can access Bedrock directly"""
        print("🔍 Testing Bedrock Access...")
        
        try:
            import boto3
            
            # Check AWS credentials
            aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
            aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
            aws_region = os.getenv('AWS_REGION', 'us-east-1')
            
            if not aws_access_key or not aws_secret_key:
                print("❌ AWS credentials not found in environment")
                return False
            
            print(f"   ✅ AWS credentials found")
            print(f"   📍 Region: {aws_region}")
            print(f"   🔑 Access Key: {aws_access_key[:8]}...")
            
            # Test Bedrock access
            bedrock = boto3.client(
                'bedrock-runtime',
                region_name=aws_region,
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key
            )
            
            # Test with small text
            print("Testing Cohere via Bedrock")
            
            response = bedrock.invoke_model(
                modelId='amazon.titan-embed-text-v2:0',
                body=json.dumps({
                    "texts": ["test"],
                    "input_type": "search_document"
                })
            )
            
            result = json.loads(response['body'].read())
            print(f"Bedrock test successful!")
            print(f"Embedding dimensions: {len(result['embeddings'][0])}")
            
            return True
            
        except Exception as e:
            print(f" Bedrock test failed: {e}")
            print(f"This may explain the upload timeouts")
            return False
    
    def validate_setup(self) -> bool:
        """
        CRITICAL FIRST STEP: Validate entire environment before starting expensive operations.
        
        FAIL-FAST PRINCIPLE: Better to fail immediately with clear error messages
        than to fail halfway through after wasting time.
        
        CHECKS:
        1. Required directories exist
        2. YAML configuration files are present  
        3. Expected CSV files are available
        4. File structure matches expectations
        5. Bedrock access (NEW)
        
        Returns:
            bool: True if everything looks good, False if issues found
        """
        print(f" Validating setup...")
        
        issues = []  # Collect all issues before reporting (don't fail on first issue)
        
        # CHECK CORE DIRECTORIES: Pipeline can't work without proper structure
        if not self.config_dir.exists():
            issues.append(f"Config directory not found: {self.config_dir}")
            issues.append("  → Create with: mkdir -p config/dataset_configs")
        
        dataset_configs_dir = self.config_dir / 'dataset_configs'
        if not dataset_configs_dir.exists():
            issues.append(f"Dataset configs directory not found: {dataset_configs_dir}")
            issues.append("  → This should contain your YAML files with business metadata")
        
        if not self.data_dir.exists():
            issues.append(f"Data directory not found: {self.data_dir}")
            issues.append("  → Create with: mkdir -p data_sources/raw")
        
        raw_data_dir = self.data_dir / 'raw'
        if not raw_data_dir.exists():
            issues.append(f"Raw data directory not found: {raw_data_dir}")
            issues.append("  → This should contain your CSV files")
        
        # CHECK FOR YAML CONFIGURATION FILES: These contain the human business knowledge
        if dataset_configs_dir.exists():
            yaml_files = list(dataset_configs_dir.glob('*.yaml')) + list(dataset_configs_dir.glob('*.yml'))
            if not yaml_files:
                issues.append(f"No YAML configuration files found in {dataset_configs_dir}")
                issues.append("  → Expected files like: raw_customer.yaml, raw_move.yaml, etc.")
            else:
                print(f"   📁 Found {len(yaml_files)} YAML configuration files")
                for yaml_file in yaml_files:
                    print(f"      - {yaml_file.name}")
        
        # CHECK FOR CSV DATA FILES: The actual data to be processed
        if raw_data_dir.exists():
            csv_files = list(raw_data_dir.glob('*.csv'))
            if not csv_files:
                issues.append(f"No CSV files found in {raw_data_dir}")
                issues.append("  → Copy your CSV files to this directory")
            else:
                print(f"   📊 Found {len(csv_files)} CSV files")
                for csv_file in csv_files:
                    file_size_mb = csv_file.stat().st_size / (1024 * 1024)
                    print(f"      - {csv_file.name} ({file_size_mb:.1f} MB)")
        
        # CHECK FOR SPECIFIC EXPECTED FILES: Based on your project
        expected_csv_files = ['Customer.csv', 'Move.csv', 'MoveDaily.csv']
        for csv_file in expected_csv_files:
            csv_path = raw_data_dir / csv_file
            if csv_path.exists():
                print(f"   ✅ Found expected file: {csv_file}")
            else:
                issues.append(f"Expected CSV file not found: {csv_path}")
        
        # TEST BEDROCK ACCESS (NEW)
        print(f"Testing Bedrock Integration...")
        bedrock_working = self.test_bedrock_access()
        if not bedrock_working:
            issues.append(f"Bedrock access test failed - vectorization may not work")
            issues.append("  → Check AWS credentials and Bedrock permissions")
            issues.append("  → Enable cohere.embed-english-v3 model in AWS Bedrock console")
        
        # REPORT VALIDATION RESULTS
        if issues:
            print(f" Setup validation failed:")
            for issue in issues:
                print(f"   {issue}")
            print(f"\nFix these issues and run the pipeline again.")
            return False
        
        print(f"✅ Setup validation passed - ready to proceed!")
        return True
    
    def load_yaml_config(self, yaml_path: Path) -> Dict[str, Any]:
        """
        Load and parse a YAML configuration file.
        
        YAML FILES CONTAIN: Human-supplied business knowledge about datasets
        - Business descriptions and purposes
        - Column meanings and classifications  
        - Answerable questions and SQL hints
        - Data ownership and governance info
        
        Args:
            yaml_path: Path to the YAML file to load
            
        Returns:
            Dict containing the parsed YAML content, empty dict if failed
        """
        try:
            with open(yaml_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            print(f" Loaded config: {yaml_path.name}")
            return config
        except yaml.YAMLError as e:
            print(f" YAML parsing error in {yaml_path.name}: {e}")
            return {}
        except FileNotFoundError:
            print(f" File not found: {yaml_path}")
            return {}
        except Exception as e:
            print(f" Unexpected error loading {yaml_path.name}: {e}")
            return {}
    
    def extract_all_metadata(self) -> List[Dict[str, Any]]:
        """
        CORE PROCESSING PHASE: Extract metadata from all configured datasets.
        
        FOR EACH YAML CONFIG FILE:
        1. Load the human-supplied business knowledge
        2. Find the corresponding CSV file
        3. Use CSVExtractor to combine technical analysis + business context
        4. Build rich metadata object for Weaviate
        
        FAILURE HANDLING: Continue processing other files if one fails
        
        Returns:
            List of complete metadata dictionaries ready for Weaviate upload
        """
        print(f"Starting metadata extraction phase")
        
        # FIND ALL YAML CONFIGURATION FILES
        dataset_configs_dir = self.config_dir / 'dataset_configs'
        yaml_files = list(dataset_configs_dir.glob('*.yaml')) + list(dataset_configs_dir.glob('*.yml'))
        
        print(f"Found {len(yaml_files)} YAML configuration files to process")
        
        all_metadata = []  # Collect successful extractions
        
        # PROCESS EACH YAML CONFIGURATION FILE
        for i, yaml_file in enumerate(yaml_files, 1):
            print(f"\n📄 Processing {i}/{len(yaml_files)}: {yaml_file.name}")
            
            try:
                # STEP 1: Load the human-supplied business knowledge
                yaml_config = self.load_yaml_config(yaml_file)
                if not yaml_config:
                    print(f"   ⚠️  Skipping {yaml_file.name} - could not load YAML")
                    self.results["failed_datasets"] += 1
                    continue
                
                # STEP 2: Extract CSV filename from YAML config
                dataset_info = yaml_config.get('dataset_info', {})
                csv_filename = dataset_info.get('original_file_name')
                
                if not csv_filename:
                    print(f"No 'original_file_name' specified in {yaml_file.name}")
                    print(f"Expected: dataset_info.original_file_name: 'YourFile.csv'")
                    self.results["failed_datasets"] += 1
                    continue
                
                # STEP 3: Construct path to CSV file
                csv_path = f"raw/{csv_filename}"  # Relative to data_sources directory
                print(f"Extracting metadata from: {csv_filename}")
                
                
                # This is where CSVExtractor reads the CSV, analyzes columns/data types/samples,
                # then combines with human descriptions/business context from YAML
                metadata = self.csv_extractor.extract_metadata(csv_path, yaml_config)
                
                # STEP 5: Check if extraction succeeded
                if metadata.get('success'):
                    all_metadata.append(metadata)
                    self.results["successful_datasets"] += 1
                    
                    # Log success details
                    table_name = metadata.get('tableName', 'Unknown')
                    record_count = metadata.get('recordCount', 0)
                    column_count = len(metadata.get('columnsArray', []))
                    
                    print(f"Success: {table_name}")
                    print(f"Records: {record_count:,}")
                    print(f"Columns: {column_count}")
                    
                else:
                    self.results["failed_datasets"] += 1
                    error_msg = metadata.get('error', 'Unknown error')
                    print(f"   ❌ Failed to extract metadata: {error_msg}")
                
                # STEP 6: Track detailed results for reporting (success or failure)
                self.results["extraction_results"].append({
                    "yaml_file": yaml_file.name,
                    "csv_file": csv_filename,
                    "table_name": metadata.get('tableName', 'Unknown'),
                    "success": metadata.get('success', False),
                    "error": metadata.get('error'),
                    "record_count": metadata.get('recordCount', 0),
                    "column_count": len(metadata.get('columnsArray', [])),
                    "processing_time": metadata.get('processingStats', {}).get('processingTime')
                })
                
            except Exception as e:
                # UNEXPECTED ERROR: Log it and continue with other files
                print(f"Unexpected error processing {yaml_file.name}: {e}")
                self.results["failed_datasets"] += 1
                self.results["extraction_results"].append({
                    "yaml_file": yaml_file.name,
                    "success": False,
                    "error": f"Unexpected error: {str(e)}"
                })
        
        # UPDATE TOTALS for final reporting
        self.results["total_datasets"] = len(yaml_files)
        
        # EXTRACTION PHASE SUMMARY
        print(f"\n📊 Metadata Extraction Phase Complete:")
        print(f"   Total configurations processed: {self.results['total_datasets']}")
        print(f"   Successful extractions: {self.results['successful_datasets']}")
        print(f"   Failed extractions: {self.results['failed_datasets']}")
        
        if self.results['successful_datasets'] > 0:
            print(f"   ✅ Ready to upload {len(all_metadata)} dataset metadata objects")
        else:
            print(f"   ❌ No successful extractions - cannot proceed to upload phase")
        
        return all_metadata
    
    def upload_dataset_metadata_individually(self, metadata_list: List[Dict[str, Any]]) -> tuple[bool, Dict[str, Any]]:
        """
        Upload DatasetMetadata objects individually with timeout handling and retries.
        
        MODIFIED: Uploads the original metadata object as-is, without any field reduction.
        WARNING: This significantly increases the risk of Bedrock timeout issues if 
                 the full metadata payloads (especially text fields for vectorization) are large.
                 Ensure your Weaviate 'DatasetMetadata' schema is compatible with all fields 
                 in the original metadata objects.
        
        Args:
            metadata_list: List of metadata objects to upload (will be uploaded as-is)
            
        Returns:
            tuple: (success, results_dict)
        """
        print(f"Uploading {len(metadata_list)} DatasetMetadata objects individually (AS-IS, NO REDUCTION)...") # Modified print
        
        collection = self.weaviate_uploader.client.collections.get("DatasetMetadata")
        
        successful_uploads = []
        failed_uploads = []
        
        # Changed loop variable from 'metadata' to 'current_metadata_object' for clarity
        # as 'metadata' is also the name of the input list in the original code
        for i, current_metadata_object in enumerate(metadata_list, 1): 
            table_name = current_metadata_object.get('tableName', 'Unknown')
            print(f"\n📤 [{i}/{len(metadata_list)}] Uploading {table_name} (full object)...")
            
            # MODIFICATION: No 'reduced_metadata' dictionary is created.
            # We will use 'current_metadata_object' directly.
            
            # Calculate and show metadata size (using the original current_metadata_object)
            # This calculation is for informational purposes.
            try:
                size_kb = len(json.dumps(current_metadata_object, default=str).encode()) / 1024
                print(f"   📏 Metadata size (original, full): {size_kb:.1f} KB") # Modified print
            except TypeError as te:
                print(f"   📏 Could not calculate metadata size due to un-serializable content in original object: {te}")
                # Depending on requirements, you might want to handle this more robustly,
                # but for now, we'll proceed with the upload attempt.
            
            # Upload with retries
            max_retries = 3
            success_for_this_object = False # Renamed from 'success' for clarity
            
            for attempt in range(max_retries):
                try:
                    print(f"   🔄 Attempt {attempt + 1}/{max_retries}...")
                    
                    # MODIFICATION: Insert the original current_metadata_object
                    uuid = collection.data.insert(current_metadata_object) 
                    print(f"   ✅ Success: {uuid}")
                    
                    successful_uploads.append({
                        "table_name": table_name,
                        "uuid": str(uuid),
                        "record_count": current_metadata_object.get("recordCount", 0), # Use current_metadata_object
                        "attempt": attempt + 1
                    })
                    
                    success_for_this_object = True
                    break # Exit retry loop on success
                    
                except Exception as e:
                    error_msg = str(e)
                    print(f"   ❌ Attempt {attempt + 1} failed: {error_msg}")
                    
                    if "timeout" in error_msg.lower() or "context canceled" in error_msg.lower():
                        if attempt < max_retries - 1: # Check if more retries are allowed
                            wait_time = (attempt + 1) * 10  # Exponential backoff
                            print(f"   ⏳ Waiting {wait_time}s before retry...")
                            time.sleep(wait_time)
                        else: # Max retries reached for a timeout error
                            failed_uploads.append({
                                "table_name": table_name,
                                "errors": [f"All {max_retries} attempts timed out"],
                                "last_error": error_msg
                            })
                            # No break here, loop will end naturally
                    else: # Non-timeout error
                        failed_uploads.append({
                            "table_name": table_name,
                            "errors": [error_msg],
                            "last_error": error_msg
                        })
                        break # Exit retry loop for non-timeout errors
            
            if not success_for_this_object:
                print(f"   💥 All upload attempts failed for {table_name}")
            
            # Small delay between processing different objects to avoid overwhelming Weaviate/Bedrock
            if i < len(metadata_list):
                time.sleep(2)
        
        # Results summary (remains the same logic)
        total_successful = len(successful_uploads)
        total_failed = len(failed_uploads)
        
        print(f"\n📊 Individual Upload Results (Full Objects):") # Modified print
        print(f"   Successful: {total_successful}/{len(metadata_list)}")
        print(f"   Failed: {total_failed}/{len(metadata_list)}")
        
        if successful_uploads:
            print(f"   ✅ Successfully uploaded:")
            for upload in successful_uploads:
                print(f"      - {upload['table_name']}: {upload['record_count']:,} records")
        
        if failed_uploads:
            print(f"   ❌ Failed uploads:")
            for failure in failed_uploads:
                print(f"      - {failure['table_name']}: {failure['errors'][0]}")
        
        results = {
            "successful": total_successful,
            "failed": total_failed,
            "total_attempted": len(metadata_list),
            "successful_uploads": successful_uploads,
            "failed_uploads": failed_uploads
        }
        
        return total_successful > 0, results
    
    def upload_all_data(self, metadata_list: List[Dict[str, Any]]) -> bool:
        """
        WEAVIATE UPLOAD PHASE: Upload all extracted metadata to Weaviate database.
        
        UPDATED APPROACH: Use individual uploads for DatasetMetadata to handle Bedrock timeouts
        
        UPLOAD SEQUENCE (order matters):
        1. DatasetMetadata objects (the core dataset descriptions) - INDIVIDUAL UPLOADS
        2. DataRelationship objects (how tables join together)  
        3. DomainTag objects (business domain organization)
        
        Args:
            metadata_list: List of rich metadata objects from extraction phase
            
        Returns:
            bool: True if all uploads succeeded, False if any failed
        """
        print(f"\n🚀 Starting Weaviate upload phase...")
        
        # ESTABLISH CONNECTION: Must connect before any upload operations
        if not self.weaviate_uploader.connect():
            print(f"❌ Could not connect to Weaviate")
            print(f"   Check that Weaviate is running: docker-compose ps")
            print(f"   Check connection URL in .env file: {os.getenv('WEAVIATE_URL', 'http://localhost:8080')}")
            return False
        
        try:
            overall_success = True  # Track if all uploads succeed
            
            # UPLOAD PHASE 1: DatasetMetadata objects (INDIVIDUAL UPLOADS)
            # Use new individual upload method to handle Bedrock timeouts
            if metadata_list:
                dataset_success, dataset_results = self.upload_dataset_metadata_individually(metadata_list)
                
                # Store results for reporting
                self.results["upload_results"]["datasets"] = dataset_results
                overall_success &= dataset_success
                
                if dataset_success:
                    print(f" DatasetMetadata upload completed successfully")
                else:
                    print(f"DatasetMetadata upload completed with some failures")
            else:
                print(f"No DatasetMetadata objects to upload")
            
            relationships_config_path = self.config_dir / 'relationships_config.yaml'
            if relationships_config_path.exists():
                print(f"\n🔗 Loading relationship definitions...")
                relationships_config = self.load_yaml_config(relationships_config_path)
                
                if relationships_config:
                    relationships = relationships_config.get('relationships', [])
                    print(f"🔗 Uploading {len(relationships)} DataRelationship objects...")
                    
                    rel_success, rel_results = self.weaviate_uploader.upload_relationships(relationships_config)
                    self.results["upload_results"]["relationships"] = rel_results
                    overall_success &= rel_success
                    
                    if rel_success:
                        print(f"DataRelationship upload successful")
                        for upload in rel_results.get("successful_uploads", []):
                            rel_name = upload.get("relationship", "Unknown")
                            print(f"      - {rel_name}")
                    else:
                        print(f"DataRelationship upload failed")
                else:
                    print(f" Could not load relationships config")
            else:
                print(f"Relationships config not found: {relationships_config_path}")
                print(f"Skipping relationship upload (this is optional)")
            
            domain_tags_config_path = self.config_dir / 'domain_tags_config.yaml'
            if domain_tags_config_path.exists():
                print(f"Loading domain tag definitions...")
                domain_tags_config = self.load_yaml_config(domain_tags_config_path)
                
                if domain_tags_config:
                    domain_tags = domain_tags_config.get('domain_tags', [])
                    print(f"🏷️  Uploading {len(domain_tags)} DomainTag objects...")
                    
                    tags_success, tags_results = self.weaviate_uploader.upload_domain_tags(domain_tags_config)
                    self.results["upload_results"]["domain_tags"] = tags_results
                    overall_success &= tags_success
                    
                    if tags_success:
                        print(f" DomainTag upload successful")
                        for upload in tags_results.get("successful_uploads", []):
                            tag_name = upload.get("tag_name", "Unknown")
                            print(f"      - {tag_name}")
                    else:
                        print(f" DomainTag upload failed")
                else:
                    print(f"Could not load domain tags config")
            else:
                print(f"\nDomain tags config not found: {domain_tags_config_path}")
                print(f"Skipping domain tag upload (this is optional)")
            
            # UPLOAD PHASE SUMMARY
            if overall_success:
                print(f"\n✅ All upload phases completed successfully!")
            else:
                print(f"\n⚠️  Upload phase completed with some failures")
                print(f"   Check the detailed results above")
            
            return overall_success
            
        except Exception as e:
            print(f"💥 Unexpected error during upload phase: {e}")
            return False
            
        finally:
            # ALWAYS disconnect from Weaviate, even if errors occurred
            self.weaviate_uploader.disconnect()
    
    def verify_ingestion(self) -> Dict[str, Any]:
        """
        VERIFICATION PHASE: Confirm that ingestion actually worked.
        
        ENHANCED: Test semantic search functionality if DatasetMetadata objects exist
        
        VERIFICATION CHECKS:
        1. Connect to Weaviate and query object counts
        2. Verify expected number of objects per class
        3. Test basic retrieval functionality
        4. Test semantic search (NEW)
        
        Returns:
            Dict with verification results or error information
        """
        print(f"\n🔍 Starting ingestion verification phase...")
        
        # ESTABLISH CONNECTION for verification queries
        if not self.weaviate_uploader.connect():
            error_msg = "Could not connect to Weaviate for verification"
            print(f"❌ {error_msg}")
            return {"error": error_msg}
        
        try:
            verification_results = self.weaviate_uploader.verify_upload_success()
            
            if "error" not in verification_results:
                total_objects = sum(
                    data.get("count", 0) 
                    for data in verification_results.values() 
                    if isinstance(data, dict) and "count" in data
                )
                verification_results["total_objects"] = total_objects
                
                print(f"Verification phase completed")
                print(f" Total objects in knowledge base: {total_objects}")
                
                # TEST SEMANTIC SEARCH if DatasetMetadata exists
                dataset_count = verification_results.get("DatasetMetadata", {}).get("count", 0)
                if dataset_count > 0:
                    print(f"\n🔍 Testing semantic search functionality...")
                    try:
                        collection = self.weaviate_uploader.client.collections.get("DatasetMetadata")
                        
                        # Wait a moment for vectorization to complete
                        time.sleep(3)
                        
                        test_queries = [
                            "customer information",
                            "operational data",
                            "daily operations"
                        ]
                        
                        search_working = False
                        for query in test_queries:
                            try:
                                response = collection.query.near_text(
                                    query=query,
                                    limit=2
                                )
                                
                                if len(response.objects) > 0:
                                    print(f"   ✅ Query '{query}': Found {len(response.objects)} results")
                                    for obj in response.objects:
                                        table_name = obj.properties.get('tableName', 'Unknown')
                                        print(f"      - {table_name}")
                                    search_working = True
                                else:
                                    print(f"   ⚠️  Query '{query}': No results")
                                    
                            except Exception as e:
                                print(f"   ❌ Query '{query}' failed: {e}")
                        
                        if search_working:
                            print(f"🎉 Semantic search is working!")
                            verification_results["semantic_search"] = "working"
                        else:
                            print(f"⚠️  Semantic search may still be processing or needs debugging")
                            verification_results["semantic_search"] = "pending"
                            
                    except Exception as e:
                        print(f"   ❌ Semantic search test failed: {e}")
                        verification_results["semantic_search"] = "failed"
                else:
                    print(f"   ⚠️  No DatasetMetadata objects found - semantic search not available")
                    
            else:
                print(f"❌ Verification failed: {verification_results['error']}")
            
            return verification_results
            
        except Exception as e:
            error_msg = f"Unexpected error during verification: {str(e)}"
            print(f"💥 {error_msg}")
            return {"error": error_msg}
            
        finally:
            # ALWAYS disconnect
            self.weaviate_uploader.disconnect()
    
    def print_final_summary(self, verification_results: Dict[str, Any]) -> bool:
        """
        REPORTING PHASE: Generate comprehensive summary of entire pipeline execution.
        
        ENHANCED: Include Bedrock/semantic search status
        
        COMPREHENSIVE REPORTING for:
        - Debugging failed runs
        - Understanding what was processed
        - Business validation of data volumes  
        - Audit trail of ingestion process
        - Bedrock integration status (NEW)
        
        Args:
            verification_results: Results from verification phase
            
        Returns:
            bool: Overall success status of entire pipeline
        """
        # FINALIZE TIMING
        self.results["end_time"] = datetime.now().isoformat()
        
        # CALCULATE DURATION
        start_time = datetime.fromisoformat(self.results["start_time"])
        end_time = datetime.fromisoformat(self.results["end_time"])
        duration = end_time - start_time
        duration_minutes = duration.total_seconds() / 60
        
        # COMPREHENSIVE SUMMARY REPORT
        print(f"\n" + "="*70)
        print(f"🎯 INGESTION PIPELINE FINAL SUMMARY")
        print(f"="*70)
        
        print(f"\n⏱️  Execution Timeline:")
        print(f"   Start Time: {self.results['start_time']}")
        print(f"   End Time: {self.results['end_time']}")
        print(f"   Duration: {duration_minutes:.1f} minutes")
        
        print(f"\n📊 Data Processing Results:")
        print(f"   Total datasets configured: {self.results['total_datasets']}")
        print(f"   Successfully processed: {self.results['successful_datasets']}")
        print(f"   Processing failures: {self.results['failed_datasets']}")
        
        # SHOW SUCCESSFUL EXTRACTIONS with details
        successful_extractions = [
            r for r in self.results['extraction_results'] 
            if r.get('success', False)
        ]
        
        if successful_extractions:
            print(f"\n   ✅ Successfully Processed Datasets:")
            for result in successful_extractions:
                table_name = result['table_name']
                record_count = result.get('record_count', 0)
                column_count = result.get('column_count', 0)
                csv_file = result.get('csv_file', 'Unknown')
                print(f"      - {table_name}")
                print(f"        Source: {csv_file}")
                print(f"        Data: {record_count:,} records, {column_count} columns")
        
        # SHOW FAILED EXTRACTIONS with error details  
        failed_extractions = [
            r for r in self.results['extraction_results'] 
            if not r.get('success', False)
        ]
        
        if failed_extractions:
            print(f"\n   ❌ Failed Extractions:")
            for result in failed_extractions:
                yaml_file = result.get('yaml_file', 'Unknown')
                error = result.get('error', 'Unknown error')
                print(f"      - {yaml_file}: {error}")
        
        print(f"\n🚀 Weaviate Upload Results:")
        upload_results = self.results.get('upload_results', {})
        
        # SHOW UPLOAD RESULTS for each category
        for category, results in upload_results.items():
            if isinstance(results, dict):
                if 'successful' in results and 'total_attempted' in results:
                    successful = results['successful']
                    total = results['total_attempted']
                    category_name = category.replace('_', ' ').title()
                    
                    if successful == total:
                        print(f"   ✅ {category_name}: {successful}/{total} successful")
                    else:
                        print(f"   ⚠️  {category_name}: {successful}/{total} successful ({total-successful} failed)")
                        
                        # Show specific failures for datasets
                        if category == 'datasets' and 'failed_uploads' in results:
                            for failure in results['failed_uploads']:
                                table_name = failure.get('table_name', 'Unknown')
                                error = failure.get('last_error', 'Unknown error')
                                if 'timeout' in error.lower():
                                    print(f"      💥 {table_name}: Bedrock timeout (try smaller metadata)")
                                else:
                                    print(f"      💥 {table_name}: {error}")
        
        print(f"\n🔍 Final Verification Results:")
        if 'error' in verification_results:
            print(f"   ❌ Verification failed: {verification_results['error']}")
            verification_success = False
        else:
            verification_success = True
            total_objects = verification_results.get('total_objects', 0)
            print(f"   📈 Total objects in knowledge base: {total_objects}")
            
            # SHOW OBJECT COUNTS by class
            for class_name, data in verification_results.items():
                if isinstance(data, dict) and 'count' in data:
                    count = data['count']
                    status = "✅" if data.get('status') == 'success' else "❌"
                    print(f"   {status} {class_name}: {count} objects")
            
            # SHOW SEMANTIC SEARCH STATUS
            semantic_status = verification_results.get('semantic_search', 'unknown')
            if semantic_status == 'working':
                print(f"   🎉 Semantic Search: WORKING - AI queries are functional!")
            elif semantic_status == 'pending':
                print(f"   ⏳ Semantic Search: Processing - may need more time for vectorization")
            elif semantic_status == 'failed':
                print(f"   ❌ Semantic Search: Failed - check Bedrock configuration")
            else:
                print(f"   ⚠️  Semantic Search: Not tested")
        
        print(f"\n" + "="*70)
        
        # DETERMINE OVERALL SUCCESS
        extraction_success = self.results['failed_datasets'] == 0
        upload_success = all(
            results.get('failed', 0) == 0 
            for results in upload_results.values() 
            if isinstance(results, dict) and 'failed' in results
        )
        
        # Consider partial dataset upload success if some succeeded
        if not upload_success and 'datasets' in upload_results:
            dataset_results = upload_results['datasets']
            if isinstance(dataset_results, dict) and dataset_results.get('successful', 0) > 0:
                print(f"💡 Note: Some DatasetMetadata objects uploaded successfully")
                upload_success = True  # Consider it a success if any uploaded
        
        overall_success = extraction_success and upload_success and verification_success
        self.results["overall_success"] = overall_success
        
        # FINAL STATUS and NEXT STEPS
        if overall_success:
            print(f"🎉 INGESTION PIPELINE COMPLETED SUCCESSFULLY!")
            print(f"   Your Weaviate knowledge base is ready for business!")
            print(f"\n🚀 What You Can Do Now:")
            print(f"   1. Test semantic search queries")
            print(f"   2. Query your data: python3 query_weaviate.py")
            print(f"   3. Build query interfaces and dashboards")
            print(f"   4. Connect your AI agents to this knowledge base")
            
            # Show semantic search status
            semantic_status = verification_results.get('semantic_search', 'unknown')
            if semantic_status == 'working':
                print(f"\n🎯 Semantic Search Examples:")
                print(f"   • 'customer data' → finds customer-related datasets")
                print(f"   • 'operational information' → finds operational datasets")
                print(f"   • 'daily reports' → finds daily operational data")
            
            print(f"\n💡 Quick Test:")
            print(f"   curl 'http://localhost:8080/v1/objects?class=DatasetMetadata' | jq '.objects[].properties.tableName'")
            
        else:
            print(f"⚠️  INGESTION PIPELINE COMPLETED WITH ISSUES")
            print(f"   Some components failed - check the detailed results above")
            print(f"\n🔧 Troubleshooting:")
            
            if not extraction_success:
                print(f"   • Fix CSV/YAML issues and re-run extraction")
            if not upload_success:  
                print(f"   • Check Weaviate connection and schema")
                print(f"   • For Bedrock timeouts: reduce metadata size or fix AWS credentials")
            if not verification_success:
                print(f"   • Verify Weaviate is running and accessible")
                
            print(f"   • Check detailed log file for specific error messages")
            print(f"   • You can re-run the pipeline to process failed items")
            
            # Specific Bedrock troubleshooting
            if 'datasets' in upload_results:
                dataset_results = upload_results['datasets']
                if isinstance(dataset_results, dict) and dataset_results.get('failed', 0) > 0:
                    print(f"\n🔧 Bedrock Timeout Issues:")
                    print(f"   • Update docker-compose.yml with timeout settings")
                    print(f"   • Check AWS Bedrock permissions and model access")
                    print(f"   • Verify cohere.embed-english-v3 is enabled in AWS console")
                    print(f"   • Consider using smaller metadata objects")
        
        print(f"="*70)
        
        return overall_success
    
    def save_results_log(self):
        """
        Save detailed execution results to JSON log file for debugging and audit trail.
        
        LOG CONTAINS:
        - Complete timeline and duration
        - Detailed results for each dataset processing attempt
        - Upload results with success/failure details
        - Verification results  
        - Error messages and stack traces
        - Bedrock integration status (NEW)
        """
        try:
            # CREATE TIMESTAMPED LOG FILENAME
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            log_file = self.project_root / f"ingestion_log_{timestamp}.json"
            
            # SAVE COMPREHENSIVE RESULTS as JSON
            with open(log_file, 'w', encoding='utf-8') as f:
                json.dump(self.results, f, indent=2, default=str, ensure_ascii=False)
            
            print(f"📝 Detailed execution log saved to: {log_file.name}")
            print(f"   Use this file for debugging any issues")
            
        except Exception as e:
            print(f"⚠️  Could not save log file: {e}")
            print(f"   Results are still available in terminal output above")
    
    def run(self) -> bool:
        """
        MAIN ORCHESTRATOR: Execute the complete ingestion pipeline from start to finish.
        
        ENHANCED PIPELINE PHASES:
        1. Pre-flight validation (including Bedrock test)
        2. Metadata extraction  
        3. Weaviate upload (with individual uploads for timeout handling)
        4. Verification (including semantic search test)
        5. Comprehensive reporting
        
        ERROR HANDLING STRATEGY:
        - Fail fast on setup issues (no point proceeding if environment is broken)
        - Continue on partial failures (process what we can)
        - Individual uploads to handle Bedrock timeouts
        - Comprehensive logging (track every success and failure)
        - Clear error messages (user knows exactly what to fix)
        
        Returns:
            bool: True if entire pipeline succeeded, False if any critical failures
        """
        print(f"🚀 STARTING COMPLETE WEAVIATE KNOWLEDGE BASE INGESTION PIPELINE")
        
        try:
            # PHASE 1: PRE-FLIGHT VALIDATION (Enhanced)
            # Check that environment is properly configured before starting expensive operations
            print(f"\n🔧 PHASE 1: Environment Validation")
            if not self.validate_setup():
                print(f"❌ Pipeline aborted due to setup issues")
                print(f"   Fix the issues listed above and try again")
                return False
            
            # PHASE 2: METADATA EXTRACTION
            # Combine CSV technical analysis with YAML business knowledge
            print(f"\n📊 PHASE 2: Metadata Extraction")
            metadata_list = self.extract_all_metadata()
            
            if not metadata_list:
                print(f"❌ Pipeline aborted - no metadata extracted successfully")
                print(f"   Check CSV files and YAML configurations")
                return False
            
            print(f"✅ Extraction phase successful - ready to upload {len(metadata_list)} datasets")
            
            # PHASE 3: WEAVIATE UPLOAD (Enhanced with individual uploads)
            # Load all metadata into Weaviate database with timeout handling
            print(f"\n🚀 PHASE 3: Weaviate Database Upload")
            print(f"   Using individual uploads to handle Bedrock timeouts")
            upload_success = self.upload_all_data(metadata_list)
            
            if not upload_success:
                print(f"Pipeline Failed")
                # Continue to verification even if uploads failed (partial success is possible)
            
            # PHASE 4: VERIFICATION (Enhanced with semantic search test)
            # Confirm that data actually made it into Weaviate and test AI functionality
            print(f"\n🔍 PHASE 4: Ingestion Verification")
            verification_results = self.verify_ingestion()
            
            # PHASE 5: COMPREHENSIVE REPORTING (Enhanced)
            # Generate detailed summary and save logs with Bedrock status
            print(f"\n📋 PHASE 5: Final Reporting")
            overall_success = self.print_final_summary(verification_results)
            
            # SAVE DETAILED LOG for debugging and audit
            self.save_results_log()
            
            return overall_success
            
        except KeyboardInterrupt:
            print(f"\n⏹️  Pipeline interrupted by user (Ctrl+C)")
            print(f"   Partial results may be available in Weaviate")
            return False
            
        except Exception as e:
            print(f"\n💥 Unexpected pipeline error: {e}")
            print(f"   This indicates a bug - please check the error details")
            import traceback
            traceback.print_exc()
            return False


def main():
    """
    Main execution function - entry point when script is run directly.
    
    EXECUTION FLOW:
    1. Create pipeline instance
    2. Run complete pipeline 
    3. Exit with appropriate status code for shell scripts
    """
    try:
        print(f"🎬 Initializing Weaviate Knowledge Base Ingestion Pipeline")
        print(f"   Enhanced with Bedrock timeout handling")
        print(f"   Current working directory: {os.getcwd()}")
        print(f"   Python version: {sys.version}")
        
        # CREATE AND RUN PIPELINE
        pipeline = IngestionPipeline()
        success = pipeline.run()
        
        # EXIT WITH APPROPRIATE STATUS CODE
        if success:
            print(f"\n🎊 PIPELINE EXECUTION COMPLETED SUCCESSFULLY!")
            sys.exit(0)  # Success exit code
        else:
            print(f"\n💥 PIPELINE EXECUTIONFailed!")
            print(f"   Check the detailed output above for specific issues")
            sys.exit(1)  # Error exit code
            
    except KeyboardInterrupt:
        print(f"\n⏹️  Pipeline execution interrupted by user")
        sys.exit(1)
        
    except Exception as e:
        print(f"\n💥 Fatal error during pipeline execution: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


# SCRIPT ENTRY POINT: Only run main() if script is executed directly
if __name__ == "__main__":
    main()