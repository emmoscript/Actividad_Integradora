"""
Job Manager Actor - Coordinates GPU processing and Spark job launches.
"""

import time
import logging
import json
import boto3
from typing import Any, Dict
from .base_actor import BaseActor, ActorMessage

logger = logging.getLogger(__name__)


class JobManagerActor(BaseActor):
    """Actor responsible for managing GPU processing and Spark job launches."""
    
    def __init__(self):
        super().__init__()
        self.lambda_client = boto3.client('lambda')
        self.s3_client = boto3.client('s3')
        self.emr_client = boto3.client('emr')
        
        # Configuration
        self.gpu_lambda_name = 'gpu-processing-lambda'
        self.spark_lambda_name = 'spark-launcher-lambda'
        self.s3_bucket = 'bigdata-processing-results'
        self.emr_cluster_name = 'bigdata-processing-cluster'
        
    def process_message(self, message: ActorMessage, sender) -> ActorMessage:
        """Manage the job processing pipeline."""
        logger.info(f"Managing job {message.job_id}")
        
        if message.message_type == "validation_success":
            return self._handle_validated_job(message, sender)
        elif message.message_type == "gpu_processing_complete":
            return self._handle_gpu_completion(message, sender)
        elif message.message_type == "spark_processing_complete":
            return self._handle_spark_completion(message, sender)
        else:
            return ActorMessage(
                message_type="error",
                data={
                    "error": f"Unknown message type: {message.message_type}",
                    "job_id": message.job_id
                },
                job_id=message.job_id
            )
    
    def _handle_validated_job(self, message: ActorMessage, sender) -> ActorMessage:
        """Handle a validated job by starting GPU processing."""
        logger.info(f"Starting GPU processing for job {message.job_id}")
        
        validated_data = message.data.get('validated_data', {})
        data = validated_data.get('data', [])
        
        # Prepare GPU processing payload
        gpu_payload = {
            "data": data,
            "job_id": message.job_id,
            "timestamp": time.time()
        }
        
        try:
            # Invoke GPU processing Lambda
            response = self.lambda_client.invoke(
                FunctionName=self.gpu_lambda_name,
                InvocationType='Event',  # Asynchronous
                Payload=json.dumps(gpu_payload)
            )
            
            logger.info(f"GPU processing initiated for job {message.job_id}")
            
            return ActorMessage(
                message_type="gpu_processing_started",
                data={
                    "job_id": message.job_id,
                    "gpu_lambda_response": response,
                    "validated_data": validated_data
                },
                job_id=message.job_id
            )
            
        except Exception as e:
            logger.error(f"Failed to start GPU processing for job {message.job_id}: {str(e)}")
            return ActorMessage(
                message_type="error",
                data={
                    "error": f"GPU processing failed: {str(e)}",
                    "job_id": message.job_id
                },
                job_id=message.job_id
            )
    
    def _handle_gpu_completion(self, message: ActorMessage, sender) -> ActorMessage:
        """Handle GPU processing completion by starting Spark job."""
        logger.info(f"GPU processing completed for job {message.job_id}")
        
        gpu_results = message.data.get('gpu_results', {})
        normalized_data = gpu_results.get('normalized_data', [])
        gpu_processing_time = gpu_results.get('processing_time', 0)
        validated_data = message.data.get('validated_data', {})
        
        # Store normalized data in S3
        s3_key = f"normalized_data/{message.job_id}/data.json"
        try:
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=json.dumps(normalized_data),
                ContentType='application/json'
            )
            
            logger.info(f"Normalized data stored in S3: {s3_key}")
            
        except Exception as e:
            logger.error(f"Failed to store data in S3: {str(e)}")
            return ActorMessage(
                message_type="error",
                data={
                    "error": f"S3 storage failed: {str(e)}",
                    "job_id": message.job_id
                },
                job_id=message.job_id
            )
        
        # Prepare Spark job payload
        spark_payload = {
            "job_id": message.job_id,
            "s3_data_path": f"s3://{self.s3_bucket}/{s3_key}",
            "pipeline_type": validated_data.get('pipeline_type', 'rdd'),
            "spark_config": validated_data.get('spark_config', {}),
            "gpu_processing_time": gpu_processing_time
        }
        
        try:
            # Invoke Spark launcher Lambda
            response = self.lambda_client.invoke(
                FunctionName=self.spark_lambda_name,
                InvocationType='Event',  # Asynchronous
                Payload=json.dumps(spark_payload)
            )
            
            logger.info(f"Spark processing initiated for job {message.job_id}")
            
            return ActorMessage(
                message_type="spark_processing_started",
                data={
                    "job_id": message.job_id,
                    "spark_lambda_response": response,
                    "gpu_results": gpu_results,
                    "s3_data_path": f"s3://{self.s3_bucket}/{s3_key}"
                },
                job_id=message.job_id
            )
            
        except Exception as e:
            logger.error(f"Failed to start Spark processing for job {message.job_id}: {str(e)}")
            return ActorMessage(
                message_type="error",
                data={
                    "error": f"Spark processing failed: {str(e)}",
                    "job_id": message.job_id
                },
                job_id=message.job_id
            )
    
    def _handle_spark_completion(self, message: ActorMessage, sender) -> ActorMessage:
        """Handle Spark processing completion."""
        logger.info(f"Spark processing completed for job {message.job_id}")
        
        spark_results = message.data.get('spark_results', {})
        gpu_results = message.data.get('gpu_results', {})
        
        # Combine results
        combined_results = {
            "job_id": message.job_id,
            "status": "completed",
            "gpu_processing": gpu_results,
            "spark_processing": spark_results,
            "total_processing_time": (
                gpu_results.get('processing_time', 0) + 
                spark_results.get('processing_time', 0)
            )
        }
        
        # Store final results in S3
        s3_key = f"final_results/{message.job_id}/results.json"
        try:
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=json.dumps(combined_results),
                ContentType='application/json'
            )
            
            logger.info(f"Final results stored in S3: {s3_key}")
            
        except Exception as e:
            logger.error(f"Failed to store final results in S3: {str(e)}")
        
        return ActorMessage(
            message_type="job_complete",
            data={
                "job_id": message.job_id,
                "results": combined_results,
                "s3_results_path": f"s3://{self.s3_bucket}/{s3_key}"
            },
            job_id=message.job_id
        )
