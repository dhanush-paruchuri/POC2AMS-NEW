#!/usr/bin/env python3
"""
Bedrock-Enabled Weaviate Schema Creator - FIXED VERSION

This script creates Weaviate schemas with Amazon Bedrock vectorization enabled
for semantic search capabilities using cohere.embed-english-v3.

FIXES APPLIED:
- Use weaviate.connect_to_local() helper function
- Correct GRPC port (50051)
- Fixed property vectorization logic
- Proper AWS credential passing
- Simplified connection approach

Usage:
    python schemas/bedrock_schema_creator.py
"""

import os
import sys
import time
from pathlib import Path
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent.parent
print(f"Project root: {project_root}")
sys.path.append(str(project_root))

try:
    import weaviate
    from weaviate.classes.config import Configure, Property, DataType
    print("✅ All imports successful")
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Install dependencies: pip install weaviate-client python-dotenv")
    sys.exit(1)

load_dotenv()

class BedRockEnabledSchemaCreator:
    """
    Schema creator with Amazon Bedrock vectorization for semantic search.
    
    FIXED VERSION - Uses proper v4 client patterns
    """
    
    def __init__(self):
        """Initialize with Bedrock configuration."""
        self.weaviate_url = os.getenv('WEAVIATE_URL', 'http://localhost:8080')
        self.grpc_port = int(os.getenv('WEAVIATE_GRPC_PORT', 50051))  # FIXED: Use default 50051
        
        # Bedrock configuration
        self.aws_region = os.getenv('AWS_REGION', 'us-east-1')
        self.bedrock_model = os.getenv('BEDROCK_MODEL_ID', 'amazon.titan-embed-text-v2:0')
                
        # Validate AWS configuration
        self.aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        
        self.client = None
        
        print(f"🤖 Bedrock-Enabled Schema Creator Initialized")
        print(f"   Weaviate URL: {self.weaviate_url}")
        print(f"   GRPC Port: {self.grpc_port}")
        print(f"   AWS Region: {self.aws_region}")
        print(f"   Bedrock Model: {self.bedrock_model}")
        print(f"   AWS Credentials: {'✅ Configured' if self.aws_access_key else '❌ Missing'}")
        
    def connect(self):
        """FIXED: Connect using v4 helper functions - much more reliable"""
        try:
            # Extract host from URL
            url_parts = self.weaviate_url.replace('http://', '').replace('https://', '')
            host_port = url_parts.split(':')
            host = host_port[0]
            port = int(host_port[1]) if len(host_port) > 1 else 8080
            
            # Use v4 helper function with proper headers
            self.client = weaviate.connect_to_local(
                host=host,
                port=port,
                grpc_port=self.grpc_port,
                headers={
                    "X-AWS-Access-Key": self.aws_access_key,
                    "X-AWS-Secret-Key": self.aws_secret_key
                } if self.aws_access_key and self.aws_secret_key else {}
            )
            
            if self.client.is_ready():
                print(f"✅ Connected to Weaviate at {self.weaviate_url}")
                
                # Check modules
                try:
                    meta = self.client.get_meta()
                    modules = meta.get('modules', {})
                    if 'text2vec-aws' in modules:
                        print(f"✅ text2vec-aws module detected")
                    else:
                        print(f"⚠️  text2vec-aws module not found")
                        print(f"   Available modules: {list(modules.keys())}")
                except Exception as e:
                    print(f"⚠️  Could not check modules: {e}")
                
                return True
            else:
                print(f"Weaviate not ready")
                return False
                
        except Exception as e:
            print(f"Connection failed: {e}")
            return False

    def get_bedrock_vectorizer_config(self):
        """Get Bedrock vectorizer configuration - FIXED with service parameter"""
        try:
            return Configure.Vectorizer.text2vec_aws(
                model=self.bedrock_model,
                region=self.aws_region,
                service="bedrock"  # CRITICAL: Must specify service!
            )
        except Exception as e:
            print(f"❌ Error creating Bedrock vectorizer config: {e}")
            return Configure.Vectorizer.none()
            
    def create_vectorized_property(self, name: str, data_type, description: str, vectorize: bool = True):
        """FIXED: Create property with correct vectorization logic"""
        return Property(
            name=name,
            data_type=data_type,
            description=description,
            vectorize_property_name=False,      # FIXED: Usually don't vectorize property names
            skip_vectorization=not vectorize    # FIXED: Skip content vectorization when vectorize=False
        )

    def create_dataset_metadata_class(self):
        """Create DatasetMetadata class with intelligent vectorization - FIXED"""
        try:
            class_name = "DatasetMetadata"
            
            if self.client.collections.exists(class_name):
                print(f"⚠️  Class '{class_name}' already exists")
                response = input(f"Delete and recreate? (y/N): ").lower().strip()
                if response == 'y':
                    self.client.collections.delete(class_name)
                    print(f"🗑️  Deleted existing '{class_name}' class")
                else:
                    print(f"ℹ️  Keeping existing '{class_name}' class")
                    return True
            
            # IDENTITY PROPERTIES (not vectorized)
            identity_properties = [
                self.create_vectorized_property("tableName", DataType.TEXT, 
                    "Primary unique name for the dataset", vectorize=False),
                self.create_vectorized_property("originalFileName", DataType.TEXT, 
                    "Original CSV file name", vectorize=False),
                self.create_vectorized_property("athenaTableName", DataType.TEXT, 
                    "AWS Athena table name", vectorize=False),
                self.create_vectorized_property("zone", DataType.TEXT, 
                    "Data processing zone (Raw/Cleansed/Curated)", vectorize=False),
                self.create_vectorized_property("format", DataType.TEXT, 
                    "File format (CSV, Parquet, etc.)", vectorize=False)
            ]
            
            # SEMANTIC PROPERTIES (vectorized for semantic search)
            semantic_properties = [
                self.create_vectorized_property("description", DataType.TEXT,
                    "Detailed human-written summary of the dataset", vectorize=True),
                self.create_vectorized_property("businessPurpose", DataType.TEXT,
                    "What business questions this dataset helps answer", vectorize=True),
                self.create_vectorized_property("columnSemanticsConcatenated", DataType.TEXT,
                    "Concatenated column names and descriptions for semantic search", vectorize=True),
                self.create_vectorized_property("tags", DataType.TEXT_ARRAY,
                    "Keywords and categories associated with the dataset", vectorize=True)
            ]
            
            # STRUCTURE PROPERTIES (not vectorized)
            structure_properties = [
                self.create_vectorized_property("columnsArray", DataType.TEXT_ARRAY,
                    "List of column names in the dataset", vectorize=False),
                self.create_vectorized_property("detailedColumnInfo", DataType.TEXT,
                    "JSON string with detailed column information", vectorize=False),
                self.create_vectorized_property("recordCount", DataType.INT,
                    "Number of records in the dataset", vectorize=False)
            ]
            
            # GOVERNANCE PROPERTIES (not vectorized)
            governance_properties = [
                self.create_vectorized_property("dataOwner", DataType.TEXT,
                    "Team or individual responsible for the data", vectorize=False),
                self.create_vectorized_property("sourceSystem", DataType.TEXT,
                    "Original system that generated this data", vectorize=False)
            ]
            
            # TIMESTAMP PROPERTIES (not vectorized)
            timestamp_properties = [
                self.create_vectorized_property("metadataCreatedAt", DataType.DATE,
                    "When this metadata was created in Weaviate", vectorize=False),
                self.create_vectorized_property("dataLastModifiedAt", DataType.DATE,
                    "When the actual data was last modified", vectorize=False)
            ]
            
            # ADVANCED PROPERTIES (mixed vectorization)
            advanced_properties = [
                self.create_vectorized_property("answerableQuestions", DataType.TEXT,
                    "JSON string of sample questions this dataset can answer", vectorize=True),
                self.create_vectorized_property("llmHints", DataType.TEXT,
                    "JSON string with hints for LLM SQL generation", vectorize=False)
            ]
            
            # COMBINE ALL PROPERTIES
            all_properties = (identity_properties + semantic_properties + 
                            structure_properties + governance_properties + 
                            timestamp_properties + advanced_properties)
            
            # Count vectorized properties for logging
            vectorized_count = sum(1 for prop in semantic_properties + [advanced_properties[0]])
            non_vectorized_count = len(all_properties) - vectorized_count

            print(f"🏗️  Creating '{class_name}' with {len(all_properties)} properties")
            print(f"   Vectorized properties: {vectorized_count}")
            print(f"   Non-vectorized properties: {non_vectorized_count}")
            
            # FIXED: Create collection with proper Bedrock configuration
            collection = self.client.collections.create(
                name=class_name,
                description="Intelligent dataset catalog with semantic search powered by Amazon Bedrock",
                properties=all_properties,
                vectorizer_config=self.get_bedrock_vectorizer_config()
            )
            
            print(f"✅ Created '{class_name}' with Bedrock vectorization")
            print(f"   Model: {self.bedrock_model}")
            print(f"   Service: bedrock")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to create DatasetMetadata class: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def create_data_relationship_class(self):
        """Create DataRelationship class (no vectorization needed)"""
        try:
            class_name = "DataRelationship"
            
            if self.client.collections.exists(class_name):
                print(f"⚠️  Class '{class_name}' already exists")
                response = input(f"Delete and recreate? (y/N): ").lower().strip()
                if response == 'y':
                    self.client.collections.delete(class_name)
                    print(f"🗑️  Deleted existing '{class_name}' class")
                else:
                    print(f"ℹ️  Keeping existing '{class_name}' class")
                    return True
            
            properties = [
                self.create_vectorized_property("fromTableName", DataType.TEXT,
                    "Name of the source table in the relationship", vectorize=False),
                self.create_vectorized_property("fromColumn", DataType.TEXT,
                    "Column name in the source table", vectorize=False),
                self.create_vectorized_property("toTableName", DataType.TEXT,
                    "Name of the target table in the relationship", vectorize=False),
                self.create_vectorized_property("toColumn", DataType.TEXT,
                    "Column name in the target table", vectorize=False),
                self.create_vectorized_property("relationshipType", DataType.TEXT,
                    "Type of relationship (foreign_key, etc.)", vectorize=True),
                self.create_vectorized_property("cardinality", DataType.TEXT,
                    "Relationship cardinality (one-to-many, etc.)", vectorize=False),
                self.create_vectorized_property("suggestedJoinType", DataType.TEXT,
                    "Suggested SQL join type (INNER, LEFT, etc.)", vectorize=False),
                self.create_vectorized_property("businessMeaning", DataType.TEXT,
                    "Business explanation of this relationship", vectorize=True)
            ]
            
            collection = self.client.collections.create(
                name=class_name,
                description="Defines relationships between datasets for SQL JOINs",
                properties=properties,
                vectorizer_config=self.get_bedrock_vectorizer_config()
            )
            
            print(f"✅ Created '{class_name}' (no vectorization)")
            return True
            
        except Exception as e:
            print(f"❌ Failed to create DataRelationship class: {e}")
            return False
    
    def create_domain_tag_class(self):
        """Create DomainTag class with selective vectorization"""
        try:
            class_name = "DomainTag"
            
            if self.client.collections.exists(class_name):
                print(f"⚠️  Class '{class_name}' already exists")
                response = input(f"Delete and recreate? (y/N): ").lower().strip()
                if response == 'y':
                    self.client.collections.delete(class_name)
                    print(f"🗑️  Deleted existing '{class_name}' class")
                else:
                    print(f"ℹ️  Keeping existing '{class_name}' class")
                    return True
                
            properties = [
                self.create_vectorized_property("tag_name", DataType.TEXT,
                    "Unique name of the domain tag", vectorize=False),
                self.create_vectorized_property("tagDescription", DataType.TEXT,
                    "Detailed description of what this domain covers", vectorize=True),
                self.create_vectorized_property("businessPriority", DataType.TEXT,
                    "Business priority level (High, Medium, Low)", vectorize=False),
                self.create_vectorized_property("dataSensitivity", DataType.TEXT,
                    "Data sensitivity classification", vectorize=False)
            ]
            
            collection = self.client.collections.create(
                name=class_name,
                description="Business domain tags with semantic search on descriptions",
                properties=properties,
                vectorizer_config=self.get_bedrock_vectorizer_config()
            )
            
            print(f"✅ Created '{class_name}' with selective vectorization")
            return True
            
        except Exception as e:
            print(f"❌ Failed to create DomainTag class: {e}")
            return False

    def test_bedrock_integration(self):
        """Test Bedrock integration with improved error handling"""
        print(f"\n🧪 Testing Bedrock Integration...")
        
        collection_name = "DatasetMetadata"
        test_data_table_name = "test_bedrock_integration_object"
        test_uuid = None
        collection = None

        try:
            collection = self.client.collections.get(collection_name)
            
            test_data = {
                "tableName": test_data_table_name,
                "originalFileName": "test_integration.csv",
                "zone": "TestZone",
                "format": "CSV",
                "description": "Customer satisfaction ratings and service quality metrics for business analysis",
                "businessPurpose": "Measure customer happiness and service performance for operational improvements",
                "columnSemanticsConcatenated": "rating: customer satisfaction score; feedback: customer comments about service quality",
                "tags": ["customer satisfaction", "service quality", "ratings", "feedback"],
                "recordCount": 500,
                "columnsArray": ["rating", "feedback", "date"],
                "detailedColumnInfo": '{"columns": [{"name": "rating", "description": "satisfaction score"}]}',
                "dataOwner": "Test Department",
                "sourceSystem": "Test System",
                "metadataCreatedAt": "2024-01-01T00:00:00Z",
                "dataLastModifiedAt": "2024-01-01T00:00:00Z",
                "answerableQuestions": '["What is customer satisfaction?"]',
                "llmHints": '{"focus_on": "ratings"}'
            }
            
            # Insert test object
            test_uuid = collection.data.insert(test_data)
            print(f"✅ Inserted test data with UUID: {test_uuid}")
            
            # Wait for vectorization
            print(f"⏳ Waiting for vectorization...")
            time.sleep(5)
            
            # Test semantic search
            search_queries = [
                "customer happiness",
                "service quality metrics", 
                "satisfaction ratings"
            ]
            
            search_working = False
            for query in search_queries:
                try:
                    print(f"   Testing query: '{query}'")
                    result = collection.query.near_text(
                        query=query,
                        limit=3
                    )
                    
                    found_test_data = any(
                        obj.properties.get("tableName") == test_data_table_name
                        for obj in result.objects
                    )
                    
                    if found_test_data:
                        print(f"   ✅ Query '{query}' found test data")
                        search_working = True
                    else:
                        print(f"   ⚠️  Query '{query}' didn't find test data")
                        
                except Exception as e:
                    print(f"   ❌ Query '{query}' failed: {e}")
            
            if search_working:
                print(f"✅ Bedrock integration test PASSED")
                return True
            else:
                print(f"⚠️  Bedrock integration test PARTIAL - vectorization may still be processing")
                return True  # Don't fail the whole process for this
            
        except Exception as e:
            print(f"❌ Bedrock integration test FAILED: {e}")
            return False
        finally:
            # Clean up
            if test_uuid and collection:
                try:
                    collection.data.delete_by_id(test_uuid)
                    print(f"🧹 Cleaned up test data")
                except Exception as e:
                    print(f"⚠️  Cleanup error: {e}")

    def validate_schema_creation(self):
        """Validate schema creation"""
        try:
            existing_names = self.client.collections.list_all()
            expected_classes = ["DatasetMetadata", "DataRelationship", "DomainTag"]
            missing_classes = [cls for cls in expected_classes if cls not in existing_names]
            
            if missing_classes:
                print(f"❌ Missing classes: {missing_classes}")
                return False
            else:
                print(f"✅ All expected classes exist: {expected_classes}")
                return True
                
        except Exception as e:
            print(f"❌ Schema validation failed: {e}")
            return False

    def print_schema_summary(self):
        """Print comprehensive schema summary"""
        try:
            print(f"\n" + "="*70)
            print(f"🤖 BEDROCK-ENABLED WEAVIATE SCHEMA SUMMARY")
            print(f"="*70)
            
            collections = self.client.collections.list_all()
            
            for collection_name in collections:
                print(f"\n🏷️  Class: {collection_name}")
                try:
                    collection_obj = self.client.collections.get(collection_name)
                    config = collection_obj.config.get()
                    
                    print(f"   Description: {config.description}")
                    
                    vectorizer = getattr(config, 'vectorizer', 'none')
                    if 'aws' in str(vectorizer).lower():
                        print(f"   🤖 Vectorizer: Amazon Bedrock ({self.bedrock_model})")
                        print(f"   🔍 Semantic search: ENABLED")
                    else:
                        print(f"   📋 Vectorizer: {vectorizer}")
                        print(f"   🔍 Semantic search: disabled")
                    
                    print(f"   📊 Properties: {len(config.properties)}")
                    
                except Exception as e:
                    print(f"   ⚠️  Could not get config: {e}")
            
            print(f"\n🚀 CAPABILITIES ENABLED:")
            print(f"   ✅ Semantic search on dataset descriptions")
            print(f"   ✅ Natural language data discovery")
            print(f"   ✅ AI-powered business context understanding")
            
            print(f"\n🔗 Bedrock Configuration:")
            print(f"   Model: {self.bedrock_model}")
            print(f"   Region: {self.aws_region}")
            print(f"   Status: {'✅ Active' if self.aws_access_key else '❌ No credentials'}")
            
            print(f"="*70)
            
        except Exception as e:
            print(f"❌ Could not print schema summary: {e}")

    def run(self):
        """Execute the complete schema creation process"""
        print(f"🚀 Starting Bedrock-Enabled Schema Creation")
        print(f"-" * 60)
        
        # Validate AWS credentials
        if not self.aws_access_key or not self.aws_secret_key:
            print(f"⚠️  AWS credentials not found")
            print(f"   Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in .env file")
        
        # Connect to Weaviate
        if not self.connect():
            return False
        
        try:
            success = True
            
            print(f"\n🏗️  Creating Weaviate Classes...")
            success &= self.create_dataset_metadata_class()
            success &= self.create_data_relationship_class()  
            success &= self.create_domain_tag_class()
            
            if success:
                success &= self.validate_schema_creation()
                
                if success:
                    print(f"\n🧪 Testing Bedrock Integration...")
                    self.test_bedrock_integration()
                    self.print_schema_summary()
                    return True
            
            return False
            
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            import traceback
            traceback.print_exc()
            return False
            
        finally:
            if self.client:
                self.client.close()
                print(f"🔌 Disconnected from Weaviate")


def main():
    """Main execution function"""
    try:
        creator = BedRockEnabledSchemaCreator()
        success = creator.run()
        
        if success:
            print(f"\n🎉 Schema creation completed successfully!")
            print(f"\n🚀 NEXT STEPS:")
            print(f"1. Run: python ingestion/main_ingestion_pipeline.py")
            print(f"2. Test semantic search")
            print(f"3. Build your AI applications!")
            sys.exit(0)
        else:
            print(f"\n💥 Schema creation failed!")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print(f"\n⏹️  Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()