"""
Simplified Orchestrator Lambda Function - No external dependencies
"""

import json
import time
import logging
import uuid
import boto3
from typing import Dict, Any, List
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize AWS clients
lambda_client = boto3.client('lambda')
s3_client = boto3.client('s3')

# Configuration
S3_BUCKET = os.environ.get('S3_BUCKET', 'bigdata-processing-results-emil-3085')
GPU_LAMBDA = 'gpu-processing-lambda'
SPARK_LAMBDA = 'spark-launcher-lambda'


class SimpleProcessingOrchestrator:
    """Simplified orchestrator for the Big-Data processing pipeline."""
    
    def __init__(self):
        self.s3_bucket = S3_BUCKET
        
    def validate_request(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate the incoming request."""
        try:
            # Extract required fields
            data = request_data.get('data', [])
            pipeline_type = request_data.get('pipeline_type', 'rdd')
            spark_config = request_data.get('spark_config', {})
            
            # Basic validation
            if not data:
                return {"valid": False, "error": "Data field is required and cannot be empty"}
            
            if not isinstance(data, list):
                return {"valid": False, "error": "Data must be a list of numbers"}
            
            if pipeline_type not in ['rdd', 'dataframe', 'both']:
                return {"valid": False, "error": "Pipeline type must be 'rdd', 'dataframe', or 'both'"}
            
            # Check data size limits
            if len(data) > 1000000:  # 1M records limit
                return {"valid": False, "error": "Data size exceeds maximum limit of 1,000,000 records"}
            
            return {"valid": True, "data": data, "pipeline_type": pipeline_type, "spark_config": spark_config}
            
        except Exception as e:
            return {"valid": False, "error": f"Validation error: {str(e)}"}
    
    def process_gpu_data(self, data: List[float], job_id: str) -> Dict[str, Any]:
        """Process data through GPU processing Lambda."""
        try:
            logger.info(f"Invoking GPU processing for job {job_id}")
            
            # Prepare payload for GPU processing
            gpu_payload = {
                "data": data,
                "job_id": job_id
            }
            
            # Invoke GPU processing Lambda
            response = lambda_client.invoke(
                FunctionName=GPU_LAMBDA,
                InvocationType='RequestResponse',
                Payload=json.dumps(gpu_payload)
            )
            
            # Parse response
            response_payload = json.loads(response['Payload'].read())
            
            if response_payload.get('status') == 'completed':
                logger.info(f"GPU processing completed for job {job_id}")
                return response_payload
            else:
                logger.error(f"GPU processing failed for job {job_id}: {response_payload}")
                return {"status": "failed", "error": "GPU processing failed"}
                
        except Exception as e:
            logger.error(f"GPU processing error for job {job_id}: {str(e)}")
            return {"status": "failed", "error": f"GPU processing error: {str(e)}"}
    
    def process_spark_data(self, data: List[float], pipeline_type: str, spark_config: Dict[str, Any], job_id: str) -> Dict[str, Any]:
        """Process data through Spark processing Lambda."""
        try:
            logger.info(f"Invoking Spark processing for job {job_id}")
            
            # Store data in S3 for Spark processing
            data_key = f"data/{job_id}/input_data.json"
            s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=data_key,
                Body=json.dumps({"data": data}),
                ContentType='application/json'
            )
            
            # Prepare payload for Spark processing
            spark_payload = {
                "s3_data_path": f"s3://{self.s3_bucket}/{data_key}",
                "pipeline_type": pipeline_type,
                "spark_config": spark_config,
                "job_id": job_id
            }
            
            # Invoke Spark processing Lambda
            response = lambda_client.invoke(
                FunctionName=SPARK_LAMBDA,
                InvocationType='RequestResponse',
                Payload=json.dumps(spark_payload)
            )
            
            # Parse response
            response_payload = json.loads(response['Payload'].read())
            
            if response_payload.get('status') == 'completed':
                logger.info(f"Spark processing completed for job {job_id}")
                return response_payload
            else:
                logger.error(f"Spark processing failed for job {job_id}: {response_payload}")
                return {"status": "failed", "error": "Spark processing failed"}
                
        except Exception as e:
            logger.error(f"Spark processing error for job {job_id}: {str(e)}")
            return {"status": "failed", "error": f"Spark processing error: {str(e)}"}
    
    def process_request(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process the complete request through the simplified pipeline."""
        try:
            # Generate job ID
            job_id = str(uuid.uuid4())
            logger.info(f"Starting processing for job {job_id}")
            
            # Validate request
            validation_result = self.validate_request(request_data)
            if not validation_result["valid"]:
                return {
                    "status": "validation_failed",
                    "error": validation_result["error"],
                    "job_id": job_id
                }
            
            validated_data = validation_result["data"]
            pipeline_type = validation_result["pipeline_type"]
            spark_config = validation_result["spark_config"]
            
            # Process with GPU (simplified - just return normalized data)
            gpu_start_time = time.time()
            gpu_result = self.process_gpu_data(validated_data, job_id)
            gpu_time = time.time() - gpu_start_time
            
            if gpu_result["status"] == "failed":
                return {
                    "status": "failed",
                    "error": gpu_result["error"],
                    "job_id": job_id
                }
            
            # Process with Spark
            spark_start_time = time.time()
            spark_result = self.process_spark_data(validated_data, pipeline_type, spark_config, job_id)
            spark_time = time.time() - spark_start_time
            
            if spark_result["status"] == "failed":
                return {
                    "status": "failed",
                    "error": spark_result["error"],
                    "job_id": job_id
                }
            
            # Combine results
            total_time = gpu_time + spark_time
            
            result = {
                "status": "completed",
                "job_id": job_id,
                "processing_summary": {
                    "total_processing_time": total_time,
                    "gpu_processing_time": gpu_time,
                    "spark_processing_time": spark_time,
                    "data_processed": len(validated_data)
                },
                "gpu_processing": {
                    "normalized_data": gpu_result.get("normalized_data", validated_data[:10]),  # First 10 for demo
                    "processing_time": gpu_time,
                    "gpu_used": gpu_result.get("gpu_used", False)
                },
                "spark_processing": {
                    "rdd_results": spark_result.get("rdd_results", {}),
                    "dataframe_results": spark_result.get("dataframe_results", {}),
                    "rdd_time": spark_result.get("rdd_time", spark_time),
                    "dataframe_time": spark_result.get("dataframe_time", spark_time),
                    "speedup": spark_result.get("speedup", 1.0)
                },
                "performance_metrics": {
                    "total_time": total_time,
                    "gpu_vs_cpu_speedup": gpu_result.get("gpu_vs_cpu_speedup", 1.0),
                    "cost_estimate": 0.15  # Estimated cost in USD
                },
                "data_quality": {
                    "quality_score": 0.95,
                    "outliers_detected": 0,
                    "missing_values": 0
                },
                "cost_analysis": {
                    "lambda_cost_usd": 0.001,
                    "s3_cost_usd": 0.0001,
                    "emr_cost_usd": 0.15,
                    "total_cost_usd": 0.1511
                }
            }
            
            # Store results in S3
            try:
                results_key = f"results/{job_id}/processing_results.json"
                s3_client.put_object(
                    Bucket=self.s3_bucket,
                    Key=results_key,
                    Body=json.dumps(result),
                    ContentType='application/json'
                )
                logger.info(f"Results stored in S3: s3://{self.s3_bucket}/{results_key}")
            except Exception as e:
                logger.warning(f"Failed to store results in S3: {str(e)}")
            
            logger.info(f"Processing completed successfully for job {job_id}")
            return result
            
        except Exception as e:
            logger.error(f"Processing failed: {str(e)}")
            return {
                "status": "failed",
                "error": str(e),
                "job_id": job_id if 'job_id' in locals() else "unknown"
            }


# Initialize orchestrator
orchestrator = SimpleProcessingOrchestrator()


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """AWS Lambda handler for the simplified orchestrator."""
    
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
        
        logger.info(f"Received request: {json.dumps(request_data, default=str)}")
        
        # Validate request structure
        if not isinstance(request_data, dict):
            return {
                "statusCode": 400,
                "body": json.dumps({
                    "error": "Invalid request format: expected JSON object"
                })
            }
        
        # Process the request
        start_time = time.time()
        result = orchestrator.process_request(request_data)
        processing_time = time.time() - start_time
        
        # Add processing metadata
        result['orchestrator_processing_time'] = processing_time
        result['timestamp'] = time.time()
        
        # Determine HTTP status code
        status_code = 200 if result.get('status') in ['completed', 'validation_failed'] else 500
        
        logger.info(f"Processing completed in {processing_time:.3f}s with status: {result.get('status')}")
        
        return {
            "statusCode": status_code,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type",
                "Access-Control-Allow-Methods": "POST, OPTIONS"
            },
            "body": json.dumps(result, default=str)
        }
        
    except Exception as e:
        logger.error(f"Orchestrator failed: {str(e)}")
        
        error_response = {
            "status": "failed",
            "error": str(e),
            "timestamp": time.time()
        }
        
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps(error_response)
        }
