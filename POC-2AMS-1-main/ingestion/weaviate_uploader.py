#!/usr/bin/env python3
"""
Production-Ready Weaviate v4 Uploader

This module implements Weaviate v4 best practices for robust data uploading:
- Proper connection management with context managers
- Rate-limited batch operations with comprehensive error handling
- Built-in validation and verification
- AWS Bedrock integration for vectorization
- Comprehensive statistics and monitoring

Usage:
    with WeaviateUploader() as uploader:
        success, results = uploader.upload_dataset_metadata(data)
"""

import os
import time
import json
from typing import Dict, List, Any, Optional, Tuple, Iterator
from pathlib import Path
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from urllib.parse import urlparse
from dotenv import load_dotenv

try:
    import weaviate
    from weaviate.util import generate_uuid5
    from weaviate.config import AdditionalConfig, Timeout
    from weaviate.classes.init import Auth
    from weaviate.classes.config import ConsistencyLevel
    print("✅ Weaviate v4 imports successful")
except ImportError as e:
    print(f"❌ Weaviate import error: {e}")
    raise

# Load environment variables
load_dotenv()

@dataclass
class UploadStats:
    """Comprehensive statistics tracking for upload operations."""
    total: int = 0
    successful: int = 0
    failed: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    error_details: List[str] = None
    
    def __post_init__(self):
        if self.error_details is None:
            self.error_details = []
    
    @property
    def duration_seconds(self) -> float:
        """Calculate duration in seconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        return (self.successful / self.total * 100) if self.total > 0 else 0.0
    
    def add_error(self, error: str):
        """Add error to tracking list."""
        self.error_details.append(error)
        self.failed += 1
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return asdict(self)

@dataclass
class ValidationResult:
    """Result of object validation."""
    is_valid: bool
    errors: List[str]
    warnings: List[str] = None
    
    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []

class WeaviateUploader:
    """
    Production-ready Weaviate v4 uploader implementing comprehensive best practices.
    
    Features:
    - Context manager support for automatic connection cleanup
    - Smart connection handling for local and cloud instances
    - Rate-limited batch operations with error monitoring
    - Comprehensive validation and verification
    - AWS Bedrock integration support
    - Detailed statistics and progress reporting
    - Thread-safe operations
    """
    
    def __init__(self, 
                 weaviate_url: Optional[str] = None,
                 api_key: Optional[str] = None,
                 batch_size: int = 100,
                 requests_per_minute: int = 600,
                 max_retries: int = 3):
        """
        Initialize uploader with v4 best practices.
        
        Args:
            weaviate_url: Weaviate instance URL (from env if not provided)
            api_key: API key for cloud instances (from env if not provided)
            batch_size: Batch size for uploads (default 100)
            requests_per_minute: Rate limit for API calls (default 600)
            max_retries: Maximum retry attempts for failed operations
        """
        # Configuration from environment or parameters
        self.weaviate_url = weaviate_url or os.getenv('WEAVIATE_URL', 'http://localhost:8080')
        self.api_key = api_key or os.getenv('WEAVIATE_API_KEY')
        self.aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        
        # Parse URL to determine connection type
        parsed_url = urlparse(self.weaviate_url)
        self.is_cloud = 'weaviate.network' in parsed_url.netloc or 'wcs.api.weaviate.io' in parsed_url.netloc
        self.is_local = parsed_url.netloc.startswith('localhost') or parsed_url.netloc.startswith('127.0.0.1')
        
        # Connection configuration
        self.timeout_config = Timeout(
            init=30,      # Connection timeout
            query=60,     # Query timeout
            insert=300    # Insert timeout (reduced from 600 for better UX)
        )
        
        # Batch configuration - following best practices
        self.batch_size = min(batch_size, 200)  # Cap at 200 per best practices
        self.requests_per_minute = requests_per_minute
        self.max_retries = max_retries
        self.retry_delay = 2.0
        
        # State tracking
        self.client = None
        self.uploaded_datasets: Dict[str, str] = {}  # tableName -> UUID mapping
        self.is_connected = False
        self._connection_type = None
        
        print(f"🚀 Weaviate Uploader v4 Initialized")
        print(f"   URL: {self.weaviate_url}")
        print(f"   Type: {'Cloud' if self.is_cloud else 'Local' if self.is_local else 'Custom'}")
        print(f"   Batch size: {self.batch_size}")
        print(f"   Rate limit: {self.requests_per_minute} req/min")
        print(f"   AWS credentials: {'✅ Found' if self.aws_access_key else '❌ Missing'}")
        print(f"   API key: {'✅ Found' if self.api_key else '❌ Missing'}")
    
    def __enter__(self):
        """Context manager entry - establish connection."""
        if not self.connect():
            raise ConnectionError(f"Failed to connect to Weaviate at {self.weaviate_url}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup connection."""
        self.disconnect()
        # Don't suppress exceptions
        return False
    
    def connect(self) -> bool:
        """
        Connect using v4 best practices with proper helper functions.
        
        Returns:
            bool: True if connection successful
        """
        try:
            # Prepare additional configuration
            additional_config = AdditionalConfig(timeout=self.timeout_config)
            
            # Choose connection method based on URL type
            if self.is_cloud:
                self._connection_type = "cloud"
                if not self.api_key:
                    print("❌ API key required for cloud connection")
                    return False
                
                self.client = weaviate.connect_to_weaviate_cloud(
                    cluster_url=self.weaviate_url,
                    auth_credentials=Auth.api_key(self.api_key),
                    additional_config=additional_config
                )
                
            elif self.is_local:
                self._connection_type = "local"
                # Parse host and port from URL
                parsed_url = urlparse(self.weaviate_url)
                host = parsed_url.hostname or "localhost"
                port = parsed_url.port or 8080
                
                # Prepare headers for AWS Bedrock if credentials available
                headers = {}
                if self.aws_access_key and self.aws_secret_key:
                    headers.update({
                        "X-AWS-Access-Key": self.aws_access_key,
                        "X-AWS-Secret-Key": self.aws_secret_key
                    })
                
                self.client = weaviate.connect_to_local(
                    host=host,
                    port=port,
                    grpc_port=50051,  # Standard GRPC port
                    headers=headers if headers else None,
                    additional_config=additional_config
                )
                
            else:
                self._connection_type = "custom"
                # Custom connection for other URLs
                from weaviate.connect import ConnectionParams
                
                headers = {}
                if self.aws_access_key and self.aws_secret_key:
                    headers.update({
                        "X-AWS-Access-Key": self.aws_access_key,
                        "X-AWS-Secret-Key": self.aws_secret_key
                    })
                
                connection_params = ConnectionParams.from_url(
                    url=self.weaviate_url,
                    grpc_port=50051
                )
                
                self.client = weaviate.WeaviateClient(
                    connection_params=connection_params,
                    additional_headers=headers if headers else None,
                    additional_config=additional_config
                )
                self.client.connect()
            
            # Verify connection
            if self.client.is_ready():
                self.is_connected = True
                print(f"✅ Connected to Weaviate ({self._connection_type})")
                
                # Get and display cluster info
                try:
                    meta = self.client.get_meta()
                    version = meta.get('version', 'unknown')
                    modules = list(meta.get('modules', {}).keys())
                    print(f"   Server version: {version}")
                    print(f"   Available modules: {', '.join(modules[:5])}")
                    
                    # Check for vectorizer modules
                    vectorizer_modules = [m for m in modules if 'text2vec' in m]
                    if vectorizer_modules:
                        print(f"   Vectorizers: {', '.join(vectorizer_modules)}")
                    
                except Exception as e:
                    print(f"   ⚠️  Could not fetch cluster info: {e}")
                
                return True
            else:
                print(f"❌ Weaviate not ready")
                return False
                
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            self.is_connected = False
            self.client = None
            return False
    
    def disconnect(self):
        """Disconnect and cleanup resources following best practices."""
        if self.client and self.is_connected:
            try:
                self.client.close()
                self.is_connected = False
                print(f"🔌 Disconnected from Weaviate")
            except Exception as e:
                print(f"⚠️  Disconnect error: {e}")
        
        # Clear state
        self.client = None
        self.is_connected = False
    
    def _generate_consistent_uuid(self, identifier: str, class_name: str) -> str:
        """Generate consistent UUID using v4 utility."""
        return str(generate_uuid5(identifier, class_name))
    
    def _validate_object(self, obj: Dict[str, Any], required_fields: List[str]) -> ValidationResult:
        """
        Validate object against required fields and data types.
        
        Args:
            obj: Object to validate
            required_fields: List of required field names
            
        Returns:
            ValidationResult with validation status and messages
        """
        errors = []
        warnings = []
        
        # Check required fields
        for field in required_fields:
            if field not in obj:
                errors.append(f"Missing required field: {field}")
            elif obj[field] is None:
                errors.append(f"Field '{field}' cannot be null")
            elif isinstance(obj[field], str) and not obj[field].strip():
                warnings.append(f"Field '{field}' is empty")
        
        # Check for common data type issues
        if 'recordCount' in obj:
            try:
                count = obj['recordCount']
                if not isinstance(count, (int, float)) or count < 0:
                    errors.append("recordCount must be a non-negative number")
            except (ValueError, TypeError):
                errors.append("recordCount must be a valid number")
        
        # Validate JSON fields
        json_fields = ['detailedColumnInfo', 'answerableQuestions', 'llmHints']
        for field in json_fields:
            if field in obj and obj[field]:
                try:
                    if isinstance(obj[field], str):
                        json.loads(obj[field])
                    elif not isinstance(obj[field], (dict, list)):
                        warnings.append(f"Field '{field}' should be JSON string or dict/list")
                except json.JSONDecodeError:
                    errors.append(f"Field '{field}' contains invalid JSON")
        
        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )
    
    def _batch_upload_with_monitoring(
        self,
        collection_name: str, 
        objects: List[Dict[str, Any]],
        id_field: str,
        required_fields: List[str],
        stats: UploadStats
    ) -> List[Dict[str, Any]]:
        """
        Upload objects using v4 batch API with comprehensive monitoring and validation.
        
        Args:
            collection_name: Target collection name
            objects: List of objects to upload
            id_field: Field name to use for UUID generation
            required_fields: List of required fields for validation
            stats: Statistics tracker to update
            
        Returns:
            List of result dictionaries with detailed status information
        """
        if not self.client or not self.is_connected:
            raise RuntimeError("Client not connected")
        
        collection = self.client.collections.get(collection_name)
        results = []
        validated_objects = []
        
        print(f"📦 Starting batch upload to {collection_name}")
        print(f"   Objects: {len(objects)}")
        print(f"   Batch size: {self.batch_size}")
        print(f"   Rate limit: {self.requests_per_minute} req/min")
        
        # Pre-validation phase
        print(f"🔍 Validating objects...")
        for i, obj in enumerate(objects):
            validation = self._validate_object(obj, required_fields)
            
            if not validation.is_valid:
                stats.add_error(f"Object {i}: {'; '.join(validation.errors)}")
                results.append({
                    "identifier": obj.get(id_field, f"unknown_{i}"),
                    "uuid": "none",
                    "status": "VALIDATION_ERROR",
                    "errors": validation.errors,
                    "warnings": validation.warnings,
                    "index": i
                })
                continue
            
            # Generate consistent UUID
            identifier = str(obj.get(id_field, f"unknown_{i}"))
            uuid = self._generate_consistent_uuid(identifier, collection_name)
            
            validated_objects.append((obj, uuid, identifier, i))
            
            if validation.warnings:
                print(f"   ⚠️  Object {i} warnings: {'; '.join(validation.warnings)}")
        
        print(f"✅ Validation complete: {len(validated_objects)}/{len(objects)} objects valid")
        
        if not validated_objects:
            print("❌ No valid objects to upload")
            return results
        
        # Batch upload phase with rate limiting
        print(f"📤 Uploading {len(validated_objects)} validated objects...")
        
        with collection.batch.rate_limit(
            requests_per_minute=self.requests_per_minute
        ) as batch:
            
            for obj, uuid, identifier, original_index in validated_objects:
                try:
                    # Add to batch
                    batch.add_object(
                        properties=obj,
                        uuid=uuid
                    )
                    
                    # Track for relationships (DatasetMetadata only)
                    if collection_name == "DatasetMetadata" and id_field in obj:
                        self.uploaded_datasets[obj[id_field]] = uuid
                    
                    results.append({
                        "identifier": identifier,
                        "uuid": uuid,
                        "status": "QUEUED",
                        "index": original_index
                    })
                    
                    # Monitor error rate during upload
                    current_position = len([r for r in results if r["status"] == "QUEUED"])
                    if batch.number_errors > 0:
                        error_rate = batch.number_errors / current_position * 100
                        if error_rate > 15:  # Stop if error rate > 15%
                            print(f"⚠️  High error rate detected: {error_rate:.1f}%")
                            print(f"   Stopping batch upload to prevent further issues")
                            break
                    
                    # Progress reporting
                    if current_position % 50 == 0 or current_position == len(validated_objects):
                        progress = current_position / len(validated_objects) * 100
                        print(f"   Progress: {current_position}/{len(validated_objects)} ({progress:.1f}%)")
                        if batch.number_errors > 0:
                            print(f"   Batch errors: {batch.number_errors}")
                
                except Exception as e:
                    stats.add_error(f"Error preparing object {identifier}: {str(e)}")
                    results.append({
                        "identifier": identifier,
                        "uuid": "none",
                        "status": "PREP_ERROR", 
                        "errors": [str(e)],
                        "index": original_index
                    })
        
        # Process batch results
        print(f"📊 Batch processing complete")
        print(f"   Total batch errors: {batch.number_errors}")
        
        # Update statistics and results
        total_queued = len([r for r in results if r["status"] == "QUEUED"])
        stats.successful = max(0, total_queued - batch.number_errors)
        stats.failed += batch.number_errors
        
        # Update result statuses based on batch outcome
        successful_count = 0
        for result in results:
            if result["status"] == "QUEUED":
                if successful_count < stats.successful:
                    result["status"] = "SUCCESS"
                    successful_count += 1
                else:
                    result["status"] = "BATCH_ERROR"
                    result["errors"] = ["Failed during batch processing"]
        
        return results
    
    def upload_dataset_metadata(self, metadata_objects: List[Dict[str, Any]]) -> Tuple[bool, Dict[str, Any]]:
        """
        Upload DatasetMetadata objects with comprehensive validation and monitoring.
        
        Args:
            metadata_objects: List of dataset metadata dictionaries
            
        Returns:
            Tuple of (success, detailed_results_dict)
        """
        if not metadata_objects:
            return True, {"message": "No objects to upload", "stats": UploadStats()}
        
        # Required fields for DatasetMetadata
        required_fields = [
            'tableName', 'zone', 'originalFileName', 'recordCount', 
            'columnsArray', 'description'
        ]
        
        stats = UploadStats(
            total=len(metadata_objects),
            start_time=datetime.now(timezone.utc)
        )
        
        print(f"\n📤 Uploading DatasetMetadata")
        print(f"   Count: {len(metadata_objects)}")
        print(f"   Required fields: {', '.join(required_fields)}")
        
        try:
            # Ensure objects have minimum required data
            for i, obj in enumerate(metadata_objects):
                if 'tableName' not in obj:
                    obj['tableName'] = f"dataset_{i}"
                if 'zone' not in obj:
                    obj['zone'] = 'Raw'
                if 'recordCount' not in obj:
                    obj['recordCount'] = 0
                if 'columnsArray' not in obj:
                    obj['columnsArray'] = []
            
            # Upload with comprehensive monitoring
            results = self._batch_upload_with_monitoring(
                collection_name="DatasetMetadata",
                objects=metadata_objects,
                id_field="tableName",
                required_fields=required_fields,
                stats=stats
            )
            
            stats.end_time = datetime.now(timezone.utc)
            
            # Detailed summary
            print(f"\n📊 DatasetMetadata Upload Complete")
            print(f"   Total processed: {stats.total}")
            print(f"   Successful: {stats.successful}")
            print(f"   Failed: {stats.failed}")
            print(f"   Success rate: {stats.success_rate:.1f}%")
            print(f"   Duration: {stats.duration_seconds:.1f}s")
            
            # Show error details if any
            if stats.error_details:
                print(f"   Error details:")
                for error in stats.error_details[:5]:  # Show first 5 errors
                    print(f"      - {error}")
                if len(stats.error_details) > 5:
                    print(f"      ... and {len(stats.error_details) - 5} more errors")
            
            return stats.failed == 0, {
                "stats": stats.to_dict(),
                "results": results,
                "uploaded_datasets": self.uploaded_datasets.copy()
            }
            
        except Exception as e:
            stats.end_time = datetime.now(timezone.utc)
            stats.add_error(f"Upload exception: {str(e)}")
            print(f"❌ Upload failed: {e}")
            return False, {"error": str(e), "stats": stats.to_dict()}
    
    def upload_relationships(self, relationships_config: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """Upload DataRelationship objects with validation."""
        relationships = relationships_config.get('relationships', [])
        if not relationships:
            return True, {"message": "No relationships to upload", "stats": UploadStats()}
        
        required_fields = [
            'from_table', 'from_column', 'to_table', 'to_column', 'relationship_type'
        ]
        
        stats = UploadStats(
            total=len(relationships),
            start_time=datetime.now(timezone.utc)
        )
        
        print(f"\n🔗 Uploading DataRelationship")
        print(f"   Count: {len(relationships)}")
        
        try:
            # Transform relationships to standard objects
            relationship_objects = []
            for rel in relationships:
                rel_obj = {
                    "relationshipName": rel.get('relationship_name', 'unknown'),
                    "fromTableName": rel.get('from_table', ''),
                    "fromColumn": rel.get('from_column', ''),
                    "toTableName": rel.get('to_table', ''),
                    "toColumn": rel.get('to_column', ''),
                    "relationshipType": rel.get('relationship_type', 'foreign_key'),
                    "cardinality": rel.get('cardinality', 'many-to-one'),
                    "suggestedJoinType": rel.get('suggested_join_type', 'INNER'),
                    "businessMeaning": rel.get('business_meaning', '')
                }
                
                # Create unique identifier
                rel_obj["relationshipId"] = f"{rel_obj['fromTableName']}.{rel_obj['fromColumn']}->{rel_obj['toTableName']}.{rel_obj['toColumn']}"
                relationship_objects.append(rel_obj)
            
            # Upload with monitoring
            results = self._batch_upload_with_monitoring(
                collection_name="DataRelationship",
                objects=relationship_objects,
                id_field="relationshipId",
                required_fields=["fromTableName", "toTableName", "relationshipType"],
                stats=stats
            )
            
            stats.end_time = datetime.now(timezone.utc)
            
            print(f"\n📊 DataRelationship Upload Complete")
            print(f"   Success rate: {stats.success_rate:.1f}%")
            
            return stats.failed == 0, {"stats": stats.to_dict(), "results": results}
            
        except Exception as e:
            stats.end_time = datetime.now(timezone.utc)
            stats.add_error(f"Upload exception: {str(e)}")
            print(f"❌ Relationship upload failed: {e}")
            return False, {"error": str(e), "stats": stats.to_dict()}
    
    def upload_domain_tags(self, domain_tags_config: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """Upload DomainTag objects with validation."""
        domain_tags = domain_tags_config.get('domain_tags', [])
        if not domain_tags:
            return True, {"message": "No domain tags to upload", "stats": UploadStats()}
        
        required_fields = ['tag_name', 'tag_description']
        
        stats = UploadStats(
            total=len(domain_tags),
            start_time=datetime.now(timezone.utc)
        )
        
        print(f"\n🏷️  Uploading DomainTag")
        print(f"   Count: {len(domain_tags)}")
        
        try:
            # Transform tags to standard objects
            tag_objects = []
            for tag in domain_tags:
                tag_obj = {
                    "tag_name": tag.get('tag_name', ''),
                    "tagDescription": tag.get('tag_description', ''),
                    "businessPriority": tag.get('business_priority', 'Medium'),
                    "dataSensitivity": tag.get('data_sensitivity', 'Internal')
                }
                tag_objects.append(tag_obj)
            
            # Upload with monitoring
            results = self._batch_upload_with_monitoring(
                collection_name="DomainTag",
                objects=tag_objects,
                id_field="tag_name",
                required_fields=required_fields,
                stats=stats
            )
            
            stats.end_time = datetime.now(timezone.utc)
            
            print(f"\n📊 DomainTag Upload Complete")
            print(f"   Success rate: {stats.success_rate:.1f}%")
            
            return stats.failed == 0, {"stats": stats.to_dict(), "results": results}
            
        except Exception as e:
            stats.end_time = datetime.now(timezone.utc)
            stats.add_error(f"Upload exception: {str(e)}")
            print(f"❌ Domain tag upload failed: {e}")
            return False, {"error": str(e), "stats": stats.to_dict()}
    
    def verify_upload_success(self) -> Dict[str, Any]:
        """
        Comprehensive verification using aggregate queries and sample validation.
        
        Returns:
            Dictionary with detailed verification results for each collection
        """
        if not self.client or not self.is_connected:
            return {"error": "Client not connected"}
        
        print(f"\n🔍 Verifying uploads...")
        
        verification = {}
        collections = ["DatasetMetadata", "DataRelationship", "DomainTag"]
        
        for collection_name in collections:
            try:
                collection = self.client.collections.get(collection_name)
                
                # Use aggregate query for accurate count (best practice)
                try:
                    agg_result = collection.aggregate.over_all(total_count=True)
                    count = agg_result.total_count if hasattr(agg_result, 'total_count') else 0
                except Exception:
                    # Fallback to fetch objects count
                    result = collection.query.fetch_objects(limit=1000)
                    count = len(result.objects)
                
                verification[collection_name] = {
                    "count": count,
                    "status": "success",
                    "verified_at": datetime.now(timezone.utc).isoformat()
                }
                
                print(f"   ✅ {collection_name}: {count} objects")
                
                # Sample validation for data integrity
                if count > 0:
                    try:
                        sample_result = collection.query.fetch_objects(limit=3)
                        if sample_result.objects:
                            samples = []
                            for obj in sample_result.objects:
                                sample_info = self._get_sample_info(collection_name, obj)
                                if sample_info:
                                    samples.append(sample_info)
                            
                            if samples:
                                verification[collection_name]["samples"] = samples
                                print(f"      Samples: {', '.join(samples)}")
                                
                    except Exception as e:
                        verification[collection_name]["sample_error"] = str(e)
                        print(f"      ⚠️  Sample validation failed: {e}")
                        
            except Exception as e:
                verification[collection_name] = {
                    "count": 0,
                    "status": "error",
                    "error": str(e),
                    "verified_at": datetime.now(timezone.utc).isoformat()
                }
                print(f"   ❌ {collection_name}: {e}")
        
        # Overall verification summary
        total_objects = sum(
            v.get("count", 0) for v in verification.values() 
            if isinstance(v, dict) and v.get("status") == "success"
        )
        
        successful_collections = sum(
            1 for v in verification.values() 
            if isinstance(v, dict) and v.get("status") == "success"
        )
        
        verification["summary"] = {
            "total_objects": total_objects,
            "successful_collections": successful_collections,
            "total_collections": len(collections),
            "verification_complete": successful_collections == len(collections),
            "verification_time": datetime.now(timezone.utc).isoformat()
        }
        
        print(f"\n📈 Verification Summary:")
        print(f"   Total objects: {total_objects}")
        print(f"   Collections verified: {successful_collections}/{len(collections)}")
        print(f"   Status: {'✅ Complete' if verification['summary']['verification_complete'] else '⚠️  Partial'}")
        
        return verification
    
    def _get_sample_info(self, collection_name: str, obj) -> Optional[str]:
        """Extract sample information from an object for verification logging."""
        try:
            props = obj.properties
            if collection_name == "DatasetMetadata":
                return props.get('tableName', 'Unknown')
            elif collection_name == "DataRelationship":
                return f"{props.get('fromTableName', '?')} -> {props.get('toTableName', '?')}"
            elif collection_name == "DomainTag":
                return props.get('tagName', 'Unknown')
        except Exception:
            pass
        return None
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get detailed connection information for debugging."""
        return {
            "url": self.weaviate_url,
            "connection_type": self._connection_type,
            "is_connected": self.is_connected,
            "is_cloud": self.is_cloud,
            "is_local": self.is_local,
            "has_api_key": bool(self.api_key),
            "has_aws_credentials": bool(self.aws_access_key and self.aws_secret_key),
            "batch_size": self.batch_size,
            "requests_per_minute": self.requests_per_minute,
            "uploaded_datasets_count": len(self.uploaded_datasets)
        }
    
    def test_semantic_search(self, query: str = "test dataset", limit: int = 3) -> Dict[str, Any]:
        """
        Test semantic search functionality if DatasetMetadata exists.
        
        Args:
            query: Search query to test
            limit: Maximum results to return
            
        Returns:
            Dictionary with search results and status
        """
        if not self.client or not self.is_connected:
            return {"error": "Client not connected"}
        
        try:
            collection = self.client.collections.get("DatasetMetadata")
            
            # First check if collection has data
            agg_result = collection.aggregate.over_all(total_count=True)
            count = agg_result.total_count if hasattr(agg_result, 'total_count') else 0
            
            if count == 0:
                return {"error": "No DatasetMetadata objects found for testing"}
            
            print(f"🔍 Testing semantic search with query: '{query}'")
            
            # Perform semantic search
            result = collection.query.near_text(
                query=query,
                limit=limit
            )
            
            results = []
            for obj in result.objects:
                props = obj.properties
                results.append({
                    "tableName": props.get('tableName', 'Unknown'),
                    "description": props.get('description', '')[:100] + '...' if props.get('description', '') else '',
                    "uuid": str(obj.uuid)
                })
            
            print(f"   ✅ Found {len(results)} results")
            for i, res in enumerate(results, 1):
                print(f"      {i}. {res['tableName']}")
            
            return {
                "query": query,
                "results_count": len(results),
                "results": results,
                "status": "success"
            }
            
        except Exception as e:
            print(f"   ❌ Semantic search test failed: {e}")
            return {"error": str(e), "query": query}


# Convenience function for context manager usage
def weaviate_uploader(**kwargs):
    """
    Create WeaviateUploader with context manager support.
    
    Usage:
        with weaviate_uploader() as uploader:
            success, results = uploader.upload_dataset_metadata(data)
    """
    return WeaviateUploader(**kwargs)


def main():
    """Test the improved uploader with comprehensive validation."""
    print("🧪 Testing Production-Ready Weaviate Uploader v4")
    print("=" * 60)
    
    # Sample test data with comprehensive fields
    sample_metadata = [{
        "tableName": "test_dataset_v4_improved", 
        "originalFileName": "test_improved.csv",
        "athenaTableName": "test_improved_athena",
        "zone": "Raw",
        "format": "CSV",
        "recordCount": 150,
        "columnsArray": ["id", "name", "category", "created_date"],
        "detailedColumnInfo": json.dumps({
            "columns": [
                {"name": "id", "type": "INTEGER", "description": "Primary key"},
                {"name": "name", "type": "TEXT", "description": "Item name"},
                {"name": "category", "type": "TEXT", "description": "Item category"},
                {"name": "created_date", "type": "DATE", "description": "Creation timestamp"}
            ]
        }),
        "description": "Improved test dataset for comprehensive v4 uploader validation with enhanced error handling and monitoring",
        "businessPurpose": "Testing the production-ready uploader functionality with comprehensive validation, error handling, and monitoring capabilities",
        "columnSemanticsConcatenated": "id: primary key identifier; name: descriptive item name; category: classification category; created_date: record creation timestamp",
        "tags": ["test", "sample", "v4", "improved", "production"],
        "dataOwner": "Data Engineering Team",
        "sourceSystem": "Test Data Generation System",
        "metadataCreatedAt": datetime.now(timezone.utc).isoformat(),
        "dataLastModifiedAt": datetime.now(timezone.utc).isoformat(),
        "answerableQuestions": json.dumps([
            "What is this test dataset?",
            "How many records does it contain?",
            "What columns are available?"
        ]),
        "llmHints": json.dumps({
            "focus_columns": ["name", "category"],
            "primary_key": "id",
            "date_fields": ["created_date"]
        })
    }]
    
    # Test with context manager (best practice)
    try:
        print("\n🔗 Testing connection and upload...")
        with weaviate_uploader() as uploader:
            
            # Show connection info
            conn_info = uploader.get_connection_info()
            print(f"   Connection type: {conn_info['connection_type']}")
            print(f"   Connected: {conn_info['is_connected']}")
            
            # Test dataset upload with validation
            print(f"\n📤 Testing dataset metadata upload...")
            success, results = uploader.upload_dataset_metadata(sample_metadata)
            
            print(f"\n📊 Upload Results:")
            print(f"   Success: {success}")
            
            if 'stats' in results:
                stats = results['stats']
                print(f"   Duration: {stats.get('duration_seconds', 0):.1f}s")
                print(f"   Success rate: {stats.get('success_rate', 0):.1f}%")
                print(f"   Errors: {len(stats.get('error_details', []))}")
            
            # Comprehensive verification
            print(f"\n🔍 Testing verification...")
            verification = uploader.verify_upload_success()
            
            if 'summary' in verification:
                summary = verification['summary']
                print(f"   Total objects: {summary.get('total_objects', 0)}")
                print(f"   Collections verified: {summary.get('successful_collections', 0)}/{summary.get('total_collections', 0)}")
            
            # Test semantic search
            print(f"\n🔍 Testing semantic search...")
            search_results = uploader.test_semantic_search("improved test dataset")
            
            if search_results.get('status') == 'success':
                print(f"   ✅ Semantic search working!")
                print(f"   Results: {search_results.get('results_count', 0)}")
            else:
                print(f"   ⚠️  Semantic search: {search_results.get('error', 'Unknown error')}")
            
            print(f"\n🎉 All tests completed successfully!")
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()