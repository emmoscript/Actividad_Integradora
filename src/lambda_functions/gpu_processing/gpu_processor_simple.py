"""
Simplified GPU Processing Lambda Function - No external dependencies
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


class SimpleGPUProcessor:
    """Simplified GPU processor that simulates GPU processing."""
    
    def __init__(self):
        self.s3_bucket = S3_BUCKET
        self.use_gpu = False  # Simulate GPU availability
        
    def normalize_data_cpu(self, data: List[float]) -> List[float]:
        """Normalize data using CPU (simulating OpenMP)."""
        try:
            # Calculate mean and standard deviation
            mean_val = sum(data) / len(data)
            variance = sum((x - mean_val) ** 2 for x in data) / len(data)
            std_val = variance ** 0.5
            
            # Avoid division by zero
            if std_val == 0:
                std_val = 1.0
            
            # Normalize data
            normalized_data = [(x - mean_val) / std_val for x in data]
            
            logger.info(f"CPU normalization completed for {len(data)} elements")
            return normalized_data
            
        except Exception as e:
            logger.error(f"CPU normalization failed: {str(e)}")
            return data
    
    def normalize_data_gpu(self, data: List[float]) -> List[float]:
        """Simulate GPU normalization (fallback to CPU)."""
        if not self.use_gpu:
            return self.normalize_data_cpu(data)
        
        try:
            # Simulate GPU processing with some delay
            time.sleep(0.1)  # Simulate GPU computation time
            
            # Use CPU implementation for now
            result = self.normalize_data_cpu(data)
            
            logger.info(f"GPU normalization completed for {len(data)} elements")
            return result
            
        except Exception as e:
            logger.error(f"GPU normalization failed: {str(e)}, falling back to CPU")
            return self.normalize_data_cpu(data)
    
    def process_data(self, data: List[float], job_id: str) -> Dict[str, Any]:
        """Process data through GPU/CPU normalization."""
        try:
            logger.info(f"Starting GPU processing for job {job_id}")
            
            # Measure processing time
            start_time = time.time()
            
            # Normalize data
            normalized_data = self.normalize_data_gpu(data)
            
            processing_time = time.time() - start_time
            
            # Calculate some basic statistics
            if normalized_data:
                min_val = min(normalized_data)
                max_val = max(normalized_data)
                mean_val = sum(normalized_data) / len(normalized_data)
                variance = sum((x - mean_val) ** 2 for x in normalized_data) / len(normalized_data)
                std_val = variance ** 0.5
            else:
                min_val = max_val = mean_val = std_val = 0
            
            # Store results in S3
            try:
                results_key = f"gpu_results/{job_id}/normalized_data.json"
                s3_client.put_object(
                    Bucket=self.s3_bucket,
                    Key=results_key,
                    Body=json.dumps({
                        "normalized_data": normalized_data[:100],  # Store first 100 for demo
                        "statistics": {
                            "min": min_val,
                            "max": max_val,
                            "mean": mean_val,
                            "std": std_val,
                            "count": len(normalized_data)
                        }
                    }),
                    ContentType='application/json'
                )
                logger.info(f"GPU results stored in S3: s3://{self.s3_bucket}/{results_key}")
            except Exception as e:
                logger.warning(f"Failed to store GPU results in S3: {str(e)}")
            
            result = {
                "status": "completed",
                "job_id": job_id,
                "normalized_data": normalized_data[:10],  # Return first 10 for demo
                "processing_time": processing_time,
                "gpu_used": self.use_gpu,
                "gpu_vs_cpu_speedup": 1.0,  # Simulated speedup
                "statistics": {
                    "min": min_val,
                    "max": max_val,
                    "mean": mean_val,
                    "std": std_val,
                    "count": len(normalized_data)
                }
            }
            
            logger.info(f"GPU processing completed for job {job_id} in {processing_time:.3f}s")
            return result
            
        except Exception as e:
            logger.error(f"GPU processing failed for job {job_id}: {str(e)}")
            return {
                "status": "failed",
                "error": str(e),
                "job_id": job_id
            }


# Initialize processor
processor = SimpleGPUProcessor()


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """AWS Lambda handler for GPU processing."""
    
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
        
        logger.info(f"Received GPU processing request: {json.dumps(request_data, default=str)}")
        
        # Extract data and job_id
        data = request_data.get('data', [])
        job_id = request_data.get('job_id', 'unknown')
        
        if not data:
            return {
                "status": "failed",
                "error": "No data provided",
                "job_id": job_id
            }
        
        # Process the data
        result = processor.process_data(data, job_id)
        
        logger.info(f"GPU processing completed with status: {result.get('status')}")
        return result
        
    except Exception as e:
        logger.error(f"GPU Lambda failed: {str(e)}")
        return {
            "status": "failed",
            "error": str(e),
            "job_id": event.get('job_id', 'unknown') if isinstance(event, dict) else 'unknown'
        }
