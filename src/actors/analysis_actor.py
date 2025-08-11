"""
Analysis Actor - Analyzes results and calculates performance metrics.
"""

import time
import logging
import json
import boto3
from typing import Any, Dict, List
from .base_actor import BaseActor, ActorMessage

logger = logging.getLogger(__name__)


class AnalysisActor(BaseActor):
    """Actor responsible for analyzing results and calculating performance metrics."""
    
    def __init__(self):
        super().__init__()
        self.s3_client = boto3.client('s3')
        self.s3_bucket = 'bigdata-processing-results'
        
    def process_message(self, message: ActorMessage, sender) -> ActorMessage:
        """Analyze results and calculate performance metrics."""
        logger.info(f"Analyzing results for job {message.job_id}")
        
        if message.message_type == "job_complete":
            return self._analyze_job_results(message, sender)
        elif message.message_type == "performance_comparison":
            return self._compare_performance(message, sender)
        else:
            return ActorMessage(
                message_type="error",
                data={
                    "error": f"Unknown message type: {message.message_type}",
                    "job_id": message.job_id
                },
                job_id=message.job_id
            )
    
    def _analyze_job_results(self, message: ActorMessage, sender) -> ActorMessage:
        """Analyze completed job results and calculate metrics."""
        analysis_start = time.time()
        
        results = message.data.get('results', {})
        job_id = message.job_id
        
        # Extract timing information
        gpu_processing = results.get('gpu_processing', {})
        spark_processing = results.get('spark_processing', {})
        
        gpu_time = gpu_processing.get('processing_time', 0)
        spark_time = spark_processing.get('processing_time', 0)
        total_time = results.get('total_processing_time', 0)
        
        # Calculate performance metrics
        performance_metrics = self._calculate_performance_metrics(
            gpu_time, spark_time, total_time, spark_processing
        )
        
        # Analyze data quality
        data_quality_metrics = self._analyze_data_quality(results)
        
        # Calculate cost metrics
        cost_metrics = self._calculate_cost_metrics(results)
        
        analysis_time = time.time() - analysis_start
        
        # Prepare analysis results
        analysis_results = {
            "job_id": job_id,
            "performance_metrics": performance_metrics,
            "data_quality_metrics": data_quality_metrics,
            "cost_metrics": cost_metrics,
            "analysis_time": analysis_time,
            "timestamp": time.time()
        }
        
        # Store analysis results in S3
        s3_key = f"analysis_results/{job_id}/analysis.json"
        try:
            self.s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=s3_key,
                Body=json.dumps(analysis_results),
                ContentType='application/json'
            )
            logger.info(f"Analysis results stored in S3: {s3_key}")
            
        except Exception as e:
            logger.error(f"Failed to store analysis results: {str(e)}")
        
        return ActorMessage(
            message_type="analysis_complete",
            data={
                "job_id": job_id,
                "analysis_results": analysis_results,
                "s3_analysis_path": f"s3://{self.s3_bucket}/{s3_key}"
            },
            job_id=job_id
        )
    
    def _calculate_performance_metrics(self, gpu_time: float, spark_time: float, 
                                     total_time: float, spark_processing: Dict) -> Dict[str, Any]:
        """Calculate performance metrics including speedup."""
        metrics = {
            "gpu_processing_time": gpu_time,
            "spark_processing_time": spark_time,
            "total_processing_time": total_time,
            "gpu_percentage": (gpu_time / total_time * 100) if total_time > 0 else 0,
            "spark_percentage": (spark_time / total_time * 100) if total_time > 0 else 0
        }
        
        # Calculate speedup metrics if available
        spark_results = spark_processing.get('results', {})
        if 'rdd_processing_time' in spark_results and 'dataframe_processing_time' in spark_results:
            rdd_time = spark_results['rdd_processing_time']
            df_time = spark_results['dataframe_processing_time']
            
            # Calculate speedup (assuming baseline is RDD)
            if rdd_time > 0:
                metrics['dataframe_speedup'] = rdd_time / df_time
            if df_time > 0:
                metrics['rdd_speedup'] = df_time / rdd_time
        
        # Calculate efficiency metrics
        spark_config = spark_processing.get('config', {})
        executor_instances = spark_config.get('executor_instances', 1)
        
        if executor_instances > 1 and spark_time > 0:
            # Estimate sequential time (rough approximation)
            estimated_sequential_time = spark_time * executor_instances * 0.8  # 80% efficiency assumption
            metrics['spark_efficiency'] = estimated_sequential_time / spark_time
            metrics['spark_speedup'] = estimated_sequential_time / spark_time
        
        return metrics
    
    def _analyze_data_quality(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze data quality metrics."""
        quality_metrics = {
            "data_processed": 0,
            "data_quality_score": 0.0,
            "outliers_detected": 0,
            "missing_values": 0
        }
        
        # Extract data information
        gpu_processing = results.get('gpu_processing', {})
        spark_processing = results.get('spark_processing', {})
        
        # Analyze normalized data
        normalized_data = gpu_processing.get('normalized_data', [])
        if normalized_data:
            quality_metrics["data_processed"] = len(normalized_data)
            
            # Calculate data quality score
            import numpy as np
            try:
                data_array = np.array(normalized_data)
                
                # Check for outliers (values outside 3 standard deviations)
                mean_val = np.mean(data_array)
                std_val = np.std(data_array)
                outliers = np.sum(np.abs(data_array - mean_val) > 3 * std_val)
                quality_metrics["outliers_detected"] = int(outliers)
                
                # Check for missing/infinite values
                missing_values = np.sum(np.isnan(data_array) | np.isinf(data_array))
                quality_metrics["missing_values"] = int(missing_values)
                
                # Calculate quality score (0-1)
                total_points = len(data_array)
                valid_points = total_points - missing_values - outliers
                quality_metrics["data_quality_score"] = valid_points / total_points if total_points > 0 else 0.0
                
            except Exception as e:
                logger.warning(f"Error analyzing data quality: {str(e)}")
        
        return quality_metrics
    
    def _calculate_cost_metrics(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate cost metrics for the processing job."""
        # AWS Lambda pricing (approximate)
        LAMBDA_PRICE_PER_100MS = 0.0000002083  # USD per 100ms
        S3_PRICE_PER_GB = 0.023  # USD per GB
        EMR_PRICE_PER_HOUR = 0.27  # USD per hour (m5.xlarge)
        
        cost_metrics = {
            "lambda_cost": 0.0,
            "s3_cost": 0.0,
            "emr_cost": 0.0,
            "total_cost": 0.0
        }
        
        # Calculate Lambda costs
        gpu_time = results.get('gpu_processing', {}).get('processing_time', 0)
        spark_time = results.get('spark_processing', {}).get('processing_time', 0)
        
        # Convert to 100ms units
        gpu_lambda_units = max(1, int(gpu_time * 10))  # Minimum 100ms
        spark_lambda_units = max(1, int(spark_time * 10))
        
        cost_metrics["lambda_cost"] = (gpu_lambda_units + spark_lambda_units) * LAMBDA_PRICE_PER_100MS
        
        # Calculate S3 costs (estimate based on data size)
        data_size_gb = 0.001  # Assume 1MB of data
        cost_metrics["s3_cost"] = data_size_gb * S3_PRICE_PER_GB
        
        # Calculate EMR costs
        spark_time_hours = spark_time / 3600
        cost_metrics["emr_cost"] = spark_time_hours * EMR_PRICE_PER_HOUR
        
        # Total cost
        cost_metrics["total_cost"] = (
            cost_metrics["lambda_cost"] + 
            cost_metrics["s3_cost"] + 
            cost_metrics["emr_cost"]
        )
        
        return cost_metrics
    
    def _compare_performance(self, message: ActorMessage, sender) -> ActorMessage:
        """Compare performance between different pipeline types."""
        logger.info(f"Comparing performance for job {message.job_id}")
        
        # This would typically compare multiple job results
        # For now, return a simple comparison structure
        comparison_results = {
            "job_id": message.job_id,
            "comparison_type": "pipeline_performance",
            "rdd_vs_dataframe": {
                "rdd_advantages": ["More control", "Lower memory overhead"],
                "dataframe_advantages": ["Better optimization", "SQL-like operations"],
                "recommendation": "Use DataFrame for structured data, RDD for custom operations"
            },
            "timestamp": time.time()
        }
        
        return ActorMessage(
            message_type="performance_comparison_complete",
            data={
                "job_id": message.job_id,
                "comparison_results": comparison_results
            },
            job_id=message.job_id
        )
