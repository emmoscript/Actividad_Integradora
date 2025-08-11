#!/usr/bin/env python3
"""
Quick API Test Script
"""

import json
import requests
import time

def quick_test():
    """Quick test of the API functionality."""
    api_url = "https://6p16xjty3i.execute-api.us-east-1.amazonaws.com/dev/process"
    
    # Simple test data
    test_data = {
        "data": [1.0, 2.0, 3.0, 4.0, 5.0],
        "pipeline_type": "rdd",
        "spark_config": {
            "executor_instances": 1,
            "executor_memory": "1g"
        }
    }
    
    print("🧪 Quick API Test")
    print("=" * 40)
    print(f"URL: {api_url}")
    print(f"Data: {test_data}")
    
    try:
        print("\n📡 Sending request...")
        start_time = time.time()
        response = requests.post(
            api_url,
            json=test_data,
            headers={'Content-Type': 'application/json'},
            timeout=300
        )
        total_time = time.time() - start_time
        
        print(f"📊 Status: {response.status_code}")
        print(f"⏱️  Time: {total_time:.2f}s")
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Success!")
            print(f"   Job ID: {result.get('job_id', 'N/A')}")
            print(f"   Status: {result.get('status', 'N/A')}")
            
            if 'processing_summary' in result:
                summary = result['processing_summary']
                print(f"   Total Time: {summary.get('total_processing_time', 'N/A')}s")
                print(f"   Data Processed: {summary.get('data_processed', 'N/A')}")
            
            print("\n🎉 API is working correctly!")
            return True
        else:
            print(f"❌ Error: {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {str(e)}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        return False

if __name__ == "__main__":
    success = quick_test()
    if success:
        print("\n🚀 Ready to run the full demo!")
    else:
        print("\n⚠️  Please check the Lambda function and try again.")
