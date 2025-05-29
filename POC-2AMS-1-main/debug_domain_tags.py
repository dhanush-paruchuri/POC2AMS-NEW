#!/usr/bin/env python3
"""
Debug script to identify and fix DomainTag validation issues
"""

import yaml
import json
from pathlib import Path

def debug_domain_tags():
    """Debug the domain tags configuration"""
    print("🔍 DEBUGGING DOMAIN TAG VALIDATION ISSUES")
    print("=" * 60)
    
    # Load the domain tags config
    config_path = Path("config/domain_tags_config.yaml")
    
    if not config_path.exists():
        print(f"❌ Config file not found: {config_path}")
        return
    
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        print(f"✅ Loaded config file: {config_path}")
        
        # Check the structure
        domain_tags = config.get('domain_tags', [])
        print(f"📊 Found {len(domain_tags)} domain tags in config")
        
        if not domain_tags:
            print("❌ No domain_tags found in config")
            return
        
        # Check each domain tag
        required_fields = ['tag_name', 'tag_description']
        
        print(f"\n🔍 Validating each domain tag...")
        print(f"Required fields: {required_fields}")
        
        valid_count = 0
        for i, tag in enumerate(domain_tags):
            print(f"\n📋 Tag {i+1}:")
            print(f"   Raw data: {tag}")
            
            errors = []
            warnings = []
            
            # Check required fields
            for field in required_fields:
                if field not in tag:
                    errors.append(f"Missing required field: {field}")
                elif tag[field] is None:
                    errors.append(f"Field '{field}' cannot be null")
                elif isinstance(tag[field], str) and not tag[field].strip():
                    warnings.append(f"Field '{field}' is empty")
            
            # Check field types
            if 'tag_name' in tag and not isinstance(tag['tag_name'], str):
                errors.append("tag_name must be a string")
            
            if 'tag_description' in tag and not isinstance(tag['tag_description'], str):
                errors.append("tag_description must be a string")
            
            # Report results
            if errors:
                print(f"   ❌ INVALID - Errors: {'; '.join(errors)}")
                if warnings:
                    print(f"   ⚠️  Warnings: {'; '.join(warnings)}")
            else:
                print(f"   ✅ VALID")
                if warnings:
                    print(f"   ⚠️  Warnings: {'; '.join(warnings)}")
                valid_count += 1
        
        print(f"\n📊 Validation Summary:")
        print(f"   Total tags: {len(domain_tags)}")
        print(f"   Valid tags: {valid_count}")
        print(f"   Invalid tags: {len(domain_tags) - valid_count}")
        
        if valid_count == 0:
            print(f"\n💡 SOLUTION SUGGESTIONS:")
            print(f"1. Check your config/domain_tags_config.yaml file")
            print(f"2. Ensure each domain tag has 'tag_name' and 'tag_description'")
            print(f"3. Make sure field values are not null or empty")
            print(f"4. Example of valid domain tag:")
            print(f"""
   - tag_name: "Customer_Management"
     tag_description: "Data related to customer information and management"
     business_priority: "High"
     data_sensitivity: "PII Present"
            """)
        
    except Exception as e:
        print(f"❌ Error loading config: {e}")

def show_expected_format():
    """Show the expected format for domain tags"""
    print(f"\n📝 EXPECTED DOMAIN TAG FORMAT:")
    print("=" * 40)
    
    example_config = {
        "domain_tags": [
            {
                "tag_name": "Customer_Management",
                "tag_description": "Data related to customer information, profiles, and management",
                "business_priority": "High", 
                "data_sensitivity": "PII Present"
            },
            {
                "tag_name": "Operations",
                "tag_description": "Operational data covering service delivery and logistics",
                "business_priority": "High",
                "data_sensitivity": "Business Critical"
            }
        ]
    }
    
    print(yaml.dump(example_config, default_flow_style=False, indent=2))

def fix_domain_tags_config():
    """Create a corrected domain tags config"""
    print(f"\n🔧 CREATING CORRECTED CONFIG:")
    print("=" * 40)
    
    corrected_config = {
        "domain_tags": [
            {
                "tag_name": "Customer_Management",
                "tag_description": "Data related to customer information, profiles, contact details, preferences, and customer relationship management. Includes master customer data, contact preferences, communication history, and customer segmentation information.",
                "business_priority": "High",
                "data_sensitivity": "PII Present"
            },
            {
                "tag_name": "Operations",
                "tag_description": "Operational data covering service delivery, logistics, crew management, scheduling, and move execution. Includes move orders, service tracking, resource allocation, and operational efficiency metrics.",
                "business_priority": "High", 
                "data_sensitivity": "Business Critical"
            },
            {
                "tag_name": "Marketing",
                "tag_description": "Marketing and lead generation data including lead sources, campaign tracking, opt-in preferences, and customer acquisition metrics. Used for marketing attribution, campaign effectiveness, and customer acquisition analysis.",
                "business_priority": "Medium",
                "data_sensitivity": "Marketing Sensitive"
            },
            {
                "tag_name": "Sales",
                "tag_description": "Sales process data covering lead management, conversion tracking, sales pipeline, and revenue generation activities. Includes customer status, booking information, and sales performance metrics.",
                "business_priority": "High",
                "data_sensitivity": "Business Critical"
            },
            {
                "tag_name": "Customer_Service", 
                "tag_description": "Customer service and satisfaction data including service ratings, customer feedback, service quality metrics, and customer experience tracking for continuous improvement initiatives.",
                "business_priority": "Medium",
                "data_sensitivity": "Internal"
            },
            {
                "tag_name": "Logistics",
                "tag_description": "Logistics and transportation data covering move distances, routing, crew assignments, resource planning, and operational logistics for service delivery optimization and capacity planning.",
                "business_priority": "High",
                "data_sensitivity": "Internal"
            },
            {
                "tag_name": "Financial",
                "tag_description": "Financial and pricing data related to move costs, pricing models, revenue tracking, and financial performance metrics for business analysis and financial reporting purposes.",
                "business_priority": "High", 
                "data_sensitivity": "Financial Sensitive"
            },
            {
                "tag_name": "Daily_Operations",
                "tag_description": "Daily operational snapshots and reporting data used for day-to-day business operations, daily reporting, and operational decision making. Represents point-in-time operational views of business data.",
                "business_priority": "Medium",
                "data_sensitivity": "Internal"
            }
        ]
    }
    
    # Save corrected config
    corrected_path = Path("config/domain_tags_config_fixed.yaml")
    try:
        with open(corrected_path, 'w') as f:
            yaml.dump(corrected_config, f, default_flow_style=False, indent=2)
        
        print(f"✅ Created corrected config: {corrected_path}")
        print(f"💡 You can replace your original file with this corrected version")
        
    except Exception as e:
        print(f"❌ Error saving corrected config: {e}")
        print(f"\n📝 Here's the corrected YAML content:")
        print(yaml.dump(corrected_config, default_flow_style=False, indent=2))

def main():
    debug_domain_tags()
    show_expected_format()
    fix_domain_tags_config()
    
    print(f"\n🚀 NEXT STEPS:")
    print(f"1. Review the validation errors above")
    print(f"2. Fix your config/domain_tags_config.yaml file")
    print(f"3. Or use the corrected config file generated")
    print(f"4. Re-run the ingestion pipeline: python3 ingestion/main_ingestion_pipeline.py")

if __name__ == "__main__":
    main()