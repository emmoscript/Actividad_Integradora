"""
Simplified Spark Launcher Lambda Function - No external dependencies
"""

import json
import time
import logging
import boto3
from typing import Dict, Any, List
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize AWS clients
s3_client = boto3.client('s3')

# Configuration
S3_BUCKET = os.environ.get('S3_BUCKET', 'bigdata-processing-results-emil-3085')


class SimpleSparkLauncher:
    """Simplified Spark launcher that simulates Spark processing."""
    
    def __init__(self):
        self.s3_bucket = S3_BUCKET
        
    def simulate_rdd_processing(self, data: List[float]) -> Dict[str, Any]:
        """Simulate RDD processing."""
        try:
            start_time = time.time()
            
            # Simulate RDD operations
            # Map: square each number
            squared_data = [x * x for x in data]
            
            # Reduce: calculate sum
            total_sum = sum(squared_data)
            
            # Calculate statistics
            mean_val = sum(data) / len(data) if data else 0
            variance = sum((x - mean_val) ** 2 for x in data) / len(data) if data else 0
            std_val = variance ** 0.5
            
            processing_time = time.time() - start_time
            
            return {
                "status": "completed",
                "processing_time": processing_time,
                "total_sum": total_sum,
                "mean": mean_val,
                "std": std_val,
                "count": len(data),
                "sample_results": squared_data[:10]  # First 10 results
            }
            
        except Exception as e:
            logger.error(f"RDD processing failed: {str(e)}")
            return {"status": "failed", "error": str(e)}
    
    def simulate_dataframe_processing(self, data: List[float]) -> Dict[str, Any]:
        """Simulate DataFrame processing."""
        try:
            start_time = time.time()
            
            # Simulate DataFrame operations
            # Filter: keep only positive numbers
            positive_data = [x for x in data if x > 0]
            
            # Group and aggregate: calculate statistics
            if positive_data:
                mean_val = sum(positive_data) / len(positive_data)
                variance = sum((x - mean_val) ** 2 for x in positive_data) / len(positive_data)
                std_val = variance ** 0.5
                min_val = min(positive_data)
                max_val = max(positive_data)
            else:
                mean_val = std_val = min_val = max_val = 0
            
            processing_time = time.time() - start_time
            
            return {
                "status": "completed",
                "processing_time": processing_time,
                "filtered_count": len(positive_data),
                "mean": mean_val,
                "std": std_val,
                "min": min_val,
                "max": max_val,
                "sample_results": positive_data[:10]  # First 10 results
            }
            
        except Exception as e:
            logger.error(f"DataFrame processing failed: {str(e)}")
            return {"status": "failed", "error": str(e)}
    
    def process_data(self, s3_data_path: str, pipeline_type: str, spark_config: Dict[str, Any], job_id: str) -> Dict[str, Any]:
        """Process data through simulated Spark processing."""
        try:
            logger.info(f"Starting Spark processing for job {job_id}")
            
            # Extract data from S3 path (simplified - we'll use sample data)
            # In a real implementation, this would read from S3
            sample_data = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
            
            # Process based on pipeline type
            if pipeline_type == "rdd":
                rdd_result = self.simulate_rdd_processing(sample_data)
                dataframe_result = {"status": "skipped"}
                rdd_time = rdd_result.get("processing_time", 0)
                dataframe_time = 0
                
            elif pipeline_type == "dataframe":
                dataframe_result = self.simulate_dataframe_processing(sample_data)
                rdd_result = {"status": "skipped"}
                dataframe_time = dataframe_result.get("processing_time", 0)
                rdd_time = 0
                
            else:  # "both"
                rdd_result = self.simulate_rdd_processing(sample_data)
                dataframe_result = self.simulate_dataframe_processing(sample_data)
                rdd_time = rdd_result.get("processing_time", 0)
                dataframe_time = dataframe_result.get("processing_time", 0)
            
            # Calculate speedup
            total_time = rdd_time + dataframe_time
            speedup = 1.0  # Simulated speedup
            
            # Store results in S3
            try:
                results_key = f"spark_results/{job_id}/processing_results.json"
                spark_results = {
                    "rdd_results": rdd_result,
                    "dataframe_results": dataframe_result,
                    "rdd_time": rdd_time,
                    "dataframe_time": dataframe_time,
                    "speedup": speedup,
                    "total_time": total_time
                }
                
                s3_client.put_object(
                    Bucket=self.s3_bucket,
                    Key=results_key,
                    Body=json.dumps(spark_results),
                    ContentType='application/json'
                )
                logger.info(f"Spark results stored in S3: s3://{self.s3_bucket}/{results_key}")
            except Exception as e:
                logger.warning(f"Failed to store Spark results in S3: {str(e)}")
            
            result = {
                "status": "completed",
                "job_id": job_id,
                "rdd_results": rdd_result,
                "dataframe_results": dataframe_result,
                "rdd_time": rdd_time,
                "dataframe_time": dataframe_time,
                "speedup": speedup,
                "total_processing_time": total_time
            }
            
            logger.info(f"Spark processing completed for job {job_id} in {total_time:.3f}s")
            return result
            
        except Exception as e:
            logger.error(f"Spark processing failed for job {job_id}: {str(e)}")
            return {
                "status": "failed",
                "error": str(e),
                "job_id": job_id
            }


# Initialize launcher
launcher = SimpleSparkLauncher()


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """AWS Lambda handler for Spark processing."""
    
    try:
        # Parse the event
        if 'body' in event:
            # API Gateway event
            body = event.get('body', '{}')
            if isinstance(body, str):
                request_data = json.loads(body)
            else:
                request_data = body
        else:
            # Direct Lambda invocation
            request_data = event
        
        logger.info(f"Received Spark processing request: {json.dumps(request_data, default=str)}")
        
        # Extract parameters
        s3_data_path = request_data.get('s3_data_path', '')
        pipeline_type = request_data.get('pipeline_type', 'rdd')
        spark_config = request_data.get('spark_config', {})
        job_id = request_data.get('job_id', 'unknown')
        
        if not s3_data_path:
            return {
                "status": "failed",
                "error": "S3 data path is required",
                "job_id": job_id
            }
        
        # Process the data
        result = launcher.process_data(s3_data_path, pipeline_type, spark_config, job_id)
        
        logger.info(f"Spark processing completed with status: {result.get('status')}")
        return result
        
    except Exception as e:
        logger.error(f"Spark Lambda failed: {str(e)}")
        return {
            "status": "failed",
            "error": str(e),
            "job_id": event.get('job_id', 'unknown') if isinstance(event, dict) else 'unknown'
        }
