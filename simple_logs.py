#!/usr/bin/env python3
"""
Simplified CloudWatch Logs Checker
"""

import boto3
from datetime import datetime, timedelta

def check_lambda_logs():
    """Check CloudWatch logs for the orchestrator Lambda function."""
    logs_client = boto3.client('logs')
    
    # Lambda function name
    function_name = "bigdata-orchestrator"
    log_group_name = f"/aws/lambda/{function_name}"
    
    print(f"🔍 Checking logs for: {log_group_name}")
    
    try:
        # Get recent log streams
        response = logs_client.describe_log_streams(
            logGroupName=log_group_name,
            orderBy='LastEventTime',
            descending=True,
            limit=5
        )
        
        if not response['logStreams']:
            print("❌ No log streams found")
            return
        
        print(f"📋 Found {len(response['logStreams'])} log streams")
        
        # Get logs from the most recent stream
        latest_stream = response['logStreams'][0]['logStreamName']
        print(f"📄 Latest stream: {latest_stream}")
        
        # Get log events from the last hour
        end_time = int(datetime.now().timestamp() * 1000)
        start_time = int((datetime.now() - timedelta(hours=1)).timestamp() * 1000)
        
        log_response = logs_client.get_log_events(
            logGroupName=log_group_name,
            logStreamName=latest_stream,
            startTime=start_time,
            endTime=end_time,
            startFromHead=False
        )
        
        print(f"\n📊 Recent log events ({len(log_response['events'])} found):")
        print("=" * 80)
        
        for event in log_response['events'][-10:]:  # Show last 10 events
            timestamp = datetime.fromtimestamp(event['timestamp'] / 1000).strftime('%H:%M:%S')
            print(f"[{timestamp}] {event['message']}")
            
    except Exception as e:
        print(f"❌ Error checking logs: {str(e)}")

if __name__ == "__main__":
    print("🚀 Lambda Log Checker")
    print("=" * 50)
    check_lambda_logs()
