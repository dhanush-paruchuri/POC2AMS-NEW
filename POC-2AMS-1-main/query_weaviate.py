#!/usr/bin/env python3
"""
Simple two-class Weaviate search - DatasetMetadata and DataRelationship
"""

import weaviate

def search_datasets_and_relationships(query, limit=2):
    """Search DatasetMetadata and DataRelationship collections"""
    print(f"Query: {query}")
    print("-" * 50)
    
    client = weaviate.connect_to_local()
    
    try:
        # Search DatasetMetadata
        print("\n=== DATASETS ===")
        dataset_collection = client.collections.get("DatasetMetadata")
        dataset_response = dataset_collection.query.near_text(query=query, limit=limit)
        
        for i, obj in enumerate(dataset_response.objects, 1):
            props = obj.properties
            print(f"\n{i}. {props.get('tableName', 'Unknown')}")
            print(f"   Description: {props.get('description', '')[:80]}...")
            print(f"   Records: {props.get('recordCount', 0):,}")
            
            columns = props.get('columnsArray', [])
            print(f"   Columns ({len(columns)}): {', '.join(columns)}{'...' if len(columns) > 8 else ''}")
        
        # Search DataRelationship  
        print("\n=== RELATIONSHIPS ===")
        rel_collection = client.collections.get("DataRelationship")
        rel_response = rel_collection.query.near_text(query=query, limit=limit)
        
        for i, obj in enumerate(rel_response.objects, 1):
            props = obj.properties
            print(f"\n{i}. {props.get('fromTableName', '?')} → {props.get('toTableName', '?')}")
            print(f"   Join: {props.get('fromColumn', '?')} = {props.get('toColumn', '?')}")
            print(f"   Type: {props.get('suggestedJoinType', '?')} JOIN")
            print(f"   Meaning: {props.get('businessMeaning', '')[:80]}...")
        
    except Exception as e:
        print(f"Error: {e}")
    
    finally:
        client.close()

if __name__ == "__main__":
    search_datasets_and_relationships("what are the total number of moves per month?", limit=2)