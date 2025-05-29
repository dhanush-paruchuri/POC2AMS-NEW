#!/usr/bin/env python3
"""
CSV Extractor for Weaviate Knowledge Base

This module extracts technical metadata from CSV files and combines it with
human-supplied YAML configurations to create rich dataset metadata.

Usage:
    from ingestion.csv_extractor import CSVExtractor
    extractor = CSVExtractor()
    metadata = extractor.extract_metadata('path/to/file.csv', yaml_config)
"""

import os
import pandas as pd
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import yaml

class CSVExtractor:
    """Extracts technical and business metadata from CSV files."""
    
    def __init__(self, data_dir: str = "data_sources"):
        """
        Initialize CSV extractor.
        
        Args:
            data_dir: Directory containing CSV files
        """
        self.data_dir = Path(data_dir)
        print(f"🔧 CSV Extractor initialized")
        print(f"   Data directory: {self.data_dir}")
    
    def read_csv_safely(self, file_path: Path) -> Tuple[Optional[pd.DataFrame], Dict[str, Any]]:
        """
        Safely read CSV file and extract basic information.
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            Tuple of (DataFrame or None, metadata dict)
        """
        metadata = {
            "file_exists": False,
            "file_size_bytes": 0,
            "last_modified": None,
            "read_success": False,
            "error_message": None,
            "encoding_used": "utf-8"
        }
        
        try:
            # Check if file exists
            if not file_path.exists():
                metadata["error_message"] = f"File not found: {file_path}"
                return None, metadata
            
            metadata["file_exists"] = True
            metadata["file_size_bytes"] = file_path.stat().st_size
            metadata["last_modified"] = datetime.fromtimestamp(
                file_path.stat().st_mtime, tz=timezone.utc
            ).isoformat()
            
            # Try to read CSV with different encodings if needed
            encodings_to_try = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
            
            for encoding in encodings_to_try:
                try:
                    df = pd.read_csv(file_path, encoding=encoding, low_memory=False)
                    metadata["encoding_used"] = encoding
                    metadata["read_success"] = True
                    print(f"✅ Successfully read {file_path.name} with {encoding} encoding")
                    return df, metadata
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    metadata["error_message"] = f"Error reading with {encoding}: {str(e)}"
                    continue
            
            # If all encodings failed
            metadata["error_message"] = "Could not read file with any encoding"
            return None, metadata
            
        except Exception as e:
            metadata["error_message"] = f"Unexpected error: {str(e)}"
            return None, metadata
    
    def analyze_dataframe(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Analyze DataFrame to extract technical metadata.
        
        Args:
            df: Pandas DataFrame
            
        Returns:
            Dictionary with analysis results
        """
        analysis = {
            "record_count": len(df),
            "column_count": len(df.columns),
            "columns_array": df.columns.tolist(),
            "data_types": {},
            "null_counts": {},
            "sample_values": {},
            "memory_usage_mb": 0
        }
        
        try:
            # Basic statistics
            analysis["memory_usage_mb"] = round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2)
            
            # Column analysis
            for column in df.columns:
                # Data type
                analysis["data_types"][column] = str(df[column].dtype)
                
                # Null count
                analysis["null_counts"][column] = int(df[column].isnull().sum())
                
                # Sample values (non-null, unique, first few)
                non_null_values = df[column].dropna()
                if len(non_null_values) > 0:
                    unique_values = non_null_values.unique()
                    # Get first 3 unique values as samples
                    sample_count = min(3, len(unique_values))
                    samples = unique_values[:sample_count].tolist()
                    
                    # Convert to strings for JSON serialization
                    analysis["sample_values"][column] = [str(val) for val in samples]
                else:
                    analysis["sample_values"][column] = []
            
            print(f"📊 DataFrame analysis completed:")
            print(f"   Records: {analysis['record_count']:,}")
            print(f"   Columns: {analysis['column_count']}")
            print(f"   Memory: {analysis['memory_usage_mb']} MB")
            
        except Exception as e:
            print(f"⚠️  Error during DataFrame analysis: {e}")
            
        return analysis
    
    def map_pandas_to_sql_types(self, pandas_dtype: str) -> str:
        """
        Map Pandas data types to SQL-like types.
        
        Args:
            pandas_dtype: Pandas dtype as string
            
        Returns:
            SQL-like type string
        """
        type_mapping = {
            'int64': 'INTEGER',
            'int32': 'INTEGER',
            'float64': 'DECIMAL',
            'float32': 'FLOAT',
            'object': 'VARCHAR',
            'string': 'VARCHAR',
            'bool': 'BOOLEAN',
            'datetime64[ns]': 'TIMESTAMP',
            'category': 'VARCHAR'
        }
        
        # Handle nullable integer types
        if 'Int' in pandas_dtype:
            return 'INTEGER'
        elif 'Float' in pandas_dtype:
            return 'DECIMAL'
        
        return type_mapping.get(pandas_dtype, 'VARCHAR')
    
    def create_detailed_column_info(self, df_analysis: Dict[str, Any], yaml_config: Dict[str, Any]) -> str:
        """
        Create detailed column information JSON by combining technical analysis with YAML config.
        
        Args:
            df_analysis: Technical analysis from DataFrame
            yaml_config: Human-supplied YAML configuration
            
        Returns:
            JSON string with detailed column information
        """
        try:
            columns_info = []
            yaml_columns = yaml_config.get('columns', {})
            
            for column_name in df_analysis['columns_array']:
                # Get technical info
                pandas_type = df_analysis['data_types'].get(column_name, 'object')
                sql_type = self.map_pandas_to_sql_types(pandas_type)
                null_count = df_analysis['null_counts'].get(column_name, 0)
                sample_values = df_analysis['sample_values'].get(column_name, [])
                
                # Get human-supplied info from YAML
                yaml_column_info = yaml_columns.get(column_name, {})
                
                column_info = {
                    "name": column_name,
                    "dataType": sql_type,
                    "pandasType": pandas_type,
                    "nullCount": null_count,
                    "sampleValues": sample_values,
                    
                    # Human-supplied metadata from YAML
                    "description": yaml_column_info.get('description', f"Column {column_name}"),
                    "semanticType": yaml_column_info.get('semantic_type', 'unknown'),
                    "businessName": yaml_column_info.get('business_name', column_name),
                    "dataClassification": yaml_column_info.get('data_classification', 'Internal'),
                    "isPrimaryKey": yaml_column_info.get('is_primary_key', False),
                    "isForeignKeyToTable": yaml_column_info.get('is_foreign_key_to_table'),
                    "isForeignKeyToColumn": yaml_column_info.get('is_foreign_key_to_column')
                }
                
                columns_info.append(column_info)
            
            # Create full structure with column groups
            detailed_info = {
                "columns": columns_info,
                "columnGroups": yaml_config.get('column_groups', {}),
                "totalColumns": len(columns_info),
                "generatedAt": datetime.now(timezone.utc).isoformat()
            }
            
            return json.dumps(detailed_info, indent=2)
            
        except Exception as e:
            print(f"❌ Error creating detailed column info: {e}")
            return json.dumps({"error": str(e), "columns": []})
    
    def create_column_semantics_concatenated(self, detailed_column_info_json: str) -> str:
        """
        Create concatenated string of column names and descriptions for semantic search.
        
        Args:
            detailed_column_info_json: JSON string with detailed column info
            
        Returns:
            Concatenated string for semantic search
        """
        try:
            column_data = json.loads(detailed_column_info_json)
            columns = column_data.get('columns', [])
            
            semantic_parts = []
            
            for column in columns:
                name = column.get('name', '')
                description = column.get('description', '')
                business_name = column.get('businessName', '')
                semantic_type = column.get('semanticType', '')
                
                # Create semantic string for this column
                parts = [f"{name}: {description}"]
                
                if business_name and business_name != name:
                    parts.append(f"({business_name})")
                
                if semantic_type and semantic_type != 'unknown':
                    parts.append(f"[{semantic_type}]")
                
                semantic_parts.append(" ".join(parts))
            
            result = "; ".join(semantic_parts)
            print(f"📝 Created column semantics string ({len(result)} characters)")
            return result
            
        except Exception as e:
            print(f"❌ Error creating column semantics: {e}")
            return "Error processing column semantics"
    
    def extract_metadata(self, csv_file_path: str, yaml_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main method to extract complete metadata from CSV file and YAML config.
        
        Args:
            csv_file_path: Path to CSV file (relative to data_dir)
            yaml_config: Complete YAML configuration dictionary
            
        Returns:
            Complete metadata dictionary ready for Weaviate ingestion
        """
        print(f"\n📁 Extracting metadata from: {csv_file_path}")
        
        # Resolve file path
        if not os.path.isabs(csv_file_path):
            file_path = self.data_dir / csv_file_path
        else:
            file_path = Path(csv_file_path)
        
        print(f"   Full path: {file_path}")
        
        # Read CSV and analyze
        df, file_metadata = self.read_csv_safely(file_path)
        
        if not file_metadata["read_success"]:
            print(f"❌ Failed to read CSV: {file_metadata['error_message']}")
            return {
                "success": False,
                "error": file_metadata["error_message"],
                "file_path": str(file_path)
            }
        
        # Analyze DataFrame
        df_analysis = self.analyze_dataframe(df)
        
        # Extract YAML configuration
        dataset_info = yaml_config.get('dataset_info', {})
        
        # Create detailed column information
        detailed_column_info = self.create_detailed_column_info(df_analysis, yaml_config)
        
        # Create column semantics for search
        column_semantics = self.create_column_semantics_concatenated(detailed_column_info)
        
        # Prepare answerable questions
        answerable_questions_json = json.dumps(
            yaml_config.get('answerable_questions', [])
        )
        
        # Prepare LLM hints
        llm_hints_json = json.dumps(
            yaml_config.get('llm_hints', {})
        )
        
        # Combine everything into final metadata
        complete_metadata = {
            # Technical metadata from CSV
            "originalFileName": file_path.name,
            "recordCount": df_analysis["record_count"],
            "columnsArray": df_analysis["columns_array"],
            "detailedColumnInfo": detailed_column_info,
            "columnSemanticsConcatenated": column_semantics,
            "dataLastModifiedAt": file_metadata["last_modified"],
            "metadataCreatedAt": datetime.now(timezone.utc).isoformat(),
            
            # Business metadata from YAML
            "tableName": dataset_info.get('table_name', file_path.stem),
            "athenaTableName": dataset_info.get('athena_table_name', ''),
            "zone": dataset_info.get('zone', 'Raw'),
            "format": dataset_info.get('format', 'CSV'),
            "description": dataset_info.get('description', ''),
            "businessPurpose": dataset_info.get('business_purpose', ''),
            "tags": dataset_info.get('tags', []),
            "dataOwner": dataset_info.get('data_owner', ''),
            "sourceSystem": dataset_info.get('source_system', ''),
            
            # Enhanced metadata
            "answerableQuestions": answerable_questions_json,
            "llmHints": llm_hints_json,
            
            # Processing metadata
            "success": True,
            "processingStats": {
                "fileSizeBytes": file_metadata["file_size_bytes"],
                "memoryUsageMB": df_analysis["memory_usage_mb"],
                "encodingUsed": file_metadata["encoding_used"],
                "processingTime": datetime.now(timezone.utc).isoformat()
            }
        }
        
        print(f"✅ Metadata extraction completed for {file_path.name}")
        print(f"   Table Name: {complete_metadata['tableName']}")
        print(f"   Zone: {complete_metadata['zone']}")
        print(f"   Records: {complete_metadata['recordCount']:,}")
        print(f"   Columns: {len(complete_metadata['columnsArray'])}")
        
        return complete_metadata
    
    def extract_all_from_config_dir(self, config_dir: str) -> List[Dict[str, Any]]:
        """
        Extract metadata for all datasets defined in config directory.
        
        Args:
            config_dir: Directory containing YAML configuration files
            
        Returns:
            List of metadata dictionaries
        """
        
        config_path = Path(config_dir)
        dataset_configs_path = config_path / 'dataset_configs'
        
        if not dataset_configs_path.exists():
            print(f"❌ Config directory not found: {dataset_configs_path}")
            return []
        
        print(f"🔍 Scanning for YAML configurations in: {dataset_configs_path}")
        
        all_metadata = []
        yaml_files = list(dataset_configs_path.glob('*.yaml')) + list(dataset_configs_path.glob('*.yml'))
        
        print(f"📁 Found {len(yaml_files)} YAML configuration files")
        
        for yaml_file in yaml_files:
            try:
                print(f"\n📄 Processing: {yaml_file.name}")
                
                # Load YAML configuration
                with open(yaml_file, 'r', encoding='utf-8') as f:
                    yaml_config = yaml.safe_load(f)
                
                # Get CSV file path from config
                dataset_info = yaml_config.get('dataset_info', {})
                csv_filename = dataset_info.get('original_file_name')
                
                if not csv_filename:
                    print(f"⚠️  No original_file_name specified in {yaml_file.name}")
                    continue
                
                # Extract metadata
                csv_path = f"raw/{csv_filename}"  # Assuming CSV files are in raw/ subdirectory
                metadata = self.extract_metadata(csv_path, yaml_config)
                
                if metadata.get('success'):
                    all_metadata.append(metadata)
                    print(f"✅ Successfully processed {yaml_file.name}")
                else:
                    print(f"❌ Failed to process {yaml_file.name}: {metadata.get('error')}")
                    
            except Exception as e:
                print(f"❌ Error processing {yaml_file.name}: {e}")
        
        print(f"\n📊 Total successful extractions: {len(all_metadata)}")
        return all_metadata


def main():
    """Test the CSV extractor with sample data."""
    try:
        extractor = CSVExtractor("data_sources")
        
        # Test extracting all configurations
        all_metadata = extractor.extract_all_from_config_dir("config")
        
        print(f"\n🎯 Extraction Summary:")
        print(f"   Total datasets processed: {len(all_metadata)}")
        
        for metadata in all_metadata:
            if metadata.get('success'):
                print(f"   ✅ {metadata['tableName']}: {metadata['recordCount']:,} records")
            else:
                print(f"   ❌ Failed: {metadata.get('error', 'Unknown error')}")
        
    except Exception as e:
        print(f"❌ Error in main: {e}")


if __name__ == "__main__":
    main()