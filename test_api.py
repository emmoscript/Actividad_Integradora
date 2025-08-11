#!/usr/bin/env python3
"""
Simple API Test Script
"""

import json
import requests
import time

def test_api():
    """Test the API with a simple request."""
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
    
    print("🧪 Testing API with simplified request...")
    print(f"URL: {api_url}")
    print(f"Data: {test_data}")
    
    try:
        start_time = time.time()
        response = requests.post(
            api_url,
            json=test_data,
            headers={'Content-Type': 'application/json'},
            timeout=300
        )
        total_time = time.time() - start_time
        
        print(f"\n📊 Response Status: {response.status_code}")
        print(f"⏱️  Response Time: {total_time:.2f}s")
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Success! Job ID: {result.get('job_id', 'N/A')}")
            print(f"📋 Status: {result.get('status', 'N/A')}")
            if 'processing_summary' in result:
                print(f"⚡ Processing Time: {result['processing_summary'].get('total_processing_time', 'N/A')}s")
        else:
            print(f"❌ Error: {response.text}")
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {str(e)}")
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")

if __name__ == "__main__":
    test_api()
