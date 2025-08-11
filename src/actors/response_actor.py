"""
Response Actor - Prepares final response for the client.
"""

import time
import logging
import json
from typing import Any, Dict
from .base_actor import BaseActor, ActorMessage

logger = logging.getLogger(__name__)


class ResponseActor(BaseActor):
    """Actor responsible for preparing the final response for the client."""
    
    def __init__(self):
        super().__init__()
        
    def process_message(self, message: ActorMessage, sender) -> ActorMessage:
        """Prepare final response for the client."""
        logger.info(f"Preparing response for job {message.job_id}")
        
        if message.message_type == "analysis_complete":
            return self._prepare_final_response(message, sender)
        elif message.message_type == "error":
            return self._prepare_error_response(message, sender)
        else:
            return ActorMessage(
                message_type="error",
                data={
                    "error": f"Unknown message type: {message.message_type}",
                    "job_id": message.job_id
                },
                job_id=message.job_id
            )
    
    def _prepare_final_response(self, message: ActorMessage, sender) -> ActorMessage:
        """Prepare the final successful response."""
        analysis_results = message.data.get('analysis_results', {})
        job_id = message.job_id
        
        # Extract key metrics
        performance_metrics = analysis_results.get('performance_metrics', {})
        data_quality_metrics = analysis_results.get('data_quality_metrics', {})
        cost_metrics = analysis_results.get('cost_metrics', {})
        
        # Prepare the final response structure
        final_response = {
            "job_id": job_id,
            "status": "completed",
            "timestamp": time.time(),
            "processing_summary": {
                "gpu_processing_time": performance_metrics.get('gpu_processing_time', 0),
                "spark_processing_time": performance_metrics.get('spark_processing_time', 0),
                "total_processing_time": performance_metrics.get('total_processing_time', 0),
                "data_processed": data_quality_metrics.get('data_processed', 0)
            },
            "performance_metrics": {
                "gpu_percentage": performance_metrics.get('gpu_percentage', 0),
                "spark_percentage": performance_metrics.get('spark_percentage', 0),
                "dataframe_speedup": performance_metrics.get('dataframe_speedup', 0),
                "rdd_speedup": performance_metrics.get('rdd_speedup', 0),
                "spark_efficiency": performance_metrics.get('spark_efficiency', 0)
            },
            "data_quality": {
                "quality_score": data_quality_metrics.get('data_quality_score', 0),
                "outliers_detected": data_quality_metrics.get('outliers_detected', 0),
                "missing_values": data_quality_metrics.get('missing_values', 0)
            },
            "cost_analysis": {
                "lambda_cost_usd": cost_metrics.get('lambda_cost', 0),
                "s3_cost_usd": cost_metrics.get('s3_cost', 0),
                "emr_cost_usd": cost_metrics.get('emr_cost', 0),
                "total_cost_usd": cost_metrics.get('total_cost', 0)
            },
            "recommendations": self._generate_recommendations(analysis_results),
            "s3_paths": {
                "analysis_results": message.data.get('s3_analysis_path', ''),
                "final_results": analysis_results.get('s3_results_path', '')
            }
        }
        
        logger.info(f"Final response prepared for job {job_id}")
        
        return ActorMessage(
            message_type="final_response_ready",
            data={
                "job_id": job_id,
                "response": final_response,
                "http_status_code": 200
            },
            job_id=job_id
        )
    
    def _prepare_error_response(self, message: ActorMessage, sender) -> ActorMessage:
        """Prepare error response."""
        error_data = message.data.get('error', 'Unknown error')
        job_id = message.job_id
        
        error_response = {
            "job_id": job_id,
            "status": "failed",
            "timestamp": time.time(),
            "error": {
                "message": error_data,
                "type": "processing_error"
            },
            "http_status_code": 500
        }
        
        logger.error(f"Error response prepared for job {job_id}: {error_data}")
        
        return ActorMessage(
            message_type="error_response_ready",
            data={
                "job_id": job_id,
                "response": error_response,
                "http_status_code": 500
            },
            job_id=job_id
        )
    
    def _generate_recommendations(self, analysis_results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate recommendations based on analysis results."""
        recommendations = {
            "performance": [],
            "cost_optimization": [],
            "data_quality": []
        }
        
        performance_metrics = analysis_results.get('performance_metrics', {})
        cost_metrics = analysis_results.get('cost_metrics', {})
        data_quality_metrics = analysis_results.get('data_quality_metrics', {})
        
        # Performance recommendations
        gpu_percentage = performance_metrics.get('gpu_percentage', 0)
        spark_percentage = performance_metrics.get('spark_percentage', 0)
        
        if gpu_percentage < 10:
            recommendations["performance"].append(
                "Consider increasing GPU utilization by processing larger batches"
            )
        
        if spark_percentage > 80:
            recommendations["performance"].append(
                "Spark processing is taking most of the time. Consider optimizing Spark configuration"
            )
        
        dataframe_speedup = performance_metrics.get('dataframe_speedup', 0)
        if dataframe_speedup > 1.5:
            recommendations["performance"].append(
                "DataFrame shows significant speedup over RDD. Consider using DataFrame for similar operations"
            )
        
        # Cost optimization recommendations
        total_cost = cost_metrics.get('total_cost', 0)
        emr_cost = cost_metrics.get('emr_cost', 0)
        
        if emr_cost > total_cost * 0.7:
            recommendations["cost_optimization"].append(
                "EMR costs are high. Consider using spot instances or optimizing cluster size"
            )
        
        if total_cost > 1.0:  # More than $1
            recommendations["cost_optimization"].append(
                "Total cost is high. Consider batch processing or using reserved instances"
            )
        
        # Data quality recommendations
        quality_score = data_quality_metrics.get('data_quality_score', 0)
        outliers = data_quality_metrics.get('outliers_detected', 0)
        
        if quality_score < 0.9:
            recommendations["data_quality"].append(
                "Data quality score is low. Consider implementing data validation and cleaning"
            )
        
        if outliers > 0:
            recommendations["data_quality"].append(
                f"Detected {outliers} outliers. Consider outlier detection and handling"
            )
        
        return recommendations
