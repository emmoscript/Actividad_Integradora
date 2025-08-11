"""
Validation Actor - Validates input data and parameters before processing.
"""

import time
import logging
from typing import Any, Dict, List
from .base_actor import BaseActor, ActorMessage

logger = logging.getLogger(__name__)


class ValidationActor(BaseActor):
    """Actor responsible for validating input data and parameters."""
    
    def __init__(self):
        super().__init__()
        self.valid_pipeline_types = ['rdd', 'dataframe']
        self.max_data_size = 1000000  # 1M elements
        self.max_spark_executors = 10
        
    def process_message(self, message: ActorMessage, sender) -> ActorMessage:
        """Validate input data and parameters."""
        logger.info(f"Validating job {message.job_id}")
        
        validation_start = time.time()
        validation_errors = []
        
        # Extract data from message
        data = message.data.get('data', [])
        pipeline_type = message.data.get('pipeline_type', 'rdd')
        spark_config = message.data.get('spark_config', {})
        
        # Validate data
        data_validation = self._validate_data(data)
        if data_validation:
            validation_errors.extend(data_validation)
        
        # Validate pipeline type
        pipeline_validation = self._validate_pipeline_type(pipeline_type)
        if pipeline_validation:
            validation_errors.extend(pipeline_validation)
        
        # Validate Spark configuration
        spark_validation = self._validate_spark_config(spark_config)
        if spark_validation:
            validation_errors.extend(spark_validation)
        
        validation_time = time.time() - validation_start
        
        if validation_errors:
            # Validation failed
            return ActorMessage(
                message_type="validation_failed",
                data={
                    "errors": validation_errors,
                    "validation_time": validation_time,
                    "job_id": message.job_id
                },
                job_id=message.job_id
            )
        else:
            # Validation successful
            return ActorMessage(
                message_type="validation_success",
                data={
                    "validated_data": {
                        "data": data,
                        "pipeline_type": pipeline_type,
                        "spark_config": spark_config
                    },
                    "validation_time": validation_time,
                    "job_id": message.job_id
                },
                job_id=message.job_id
            )
    
    def _validate_data(self, data: List[float]) -> List[str]:
        """Validate input data array."""
        errors = []
        
        if not isinstance(data, list):
            errors.append("Data must be a list")
            return errors
        
        if len(data) == 0:
            errors.append("Data cannot be empty")
        
        if len(data) > self.max_data_size:
            errors.append(f"Data size ({len(data)}) exceeds maximum allowed ({self.max_data_size})")
        
        # Check if all elements are numeric
        for i, value in enumerate(data):
            if not isinstance(value, (int, float)):
                errors.append(f"Element at index {i} is not numeric: {value}")
        
        # Check for NaN or infinite values
        import math
        for i, value in enumerate(data):
            if math.isnan(value) or math.isinf(value):
                errors.append(f"Element at index {i} is NaN or infinite: {value}")
        
        return errors
    
    def _validate_pipeline_type(self, pipeline_type: str) -> List[str]:
        """Validate pipeline type."""
        errors = []
        
        if not isinstance(pipeline_type, str):
            errors.append("Pipeline type must be a string")
        elif pipeline_type not in self.valid_pipeline_types:
            errors.append(f"Invalid pipeline type: {pipeline_type}. Must be one of {self.valid_pipeline_types}")
        
        return errors
    
    def _validate_spark_config(self, spark_config: Dict[str, Any]) -> List[str]:
        """Validate Spark configuration."""
        errors = []
        
        if not isinstance(spark_config, dict):
            errors.append("Spark config must be a dictionary")
            return errors
        
        # Validate executor instances
        executor_instances = spark_config.get('executor_instances', 2)
        if not isinstance(executor_instances, int) or executor_instances <= 0:
            errors.append("executor_instances must be a positive integer")
        elif executor_instances > self.max_spark_executors:
            errors.append(f"executor_instances ({executor_instances}) exceeds maximum ({self.max_spark_executors})")
        
        # Validate executor memory
        executor_memory = spark_config.get('executor_memory', '2g')
        if not isinstance(executor_memory, str):
            errors.append("executor_memory must be a string")
        else:
            # Validate memory format (e.g., '2g', '512m')
            import re
            memory_pattern = r'^\d+[kmgKMG]$'
            if not re.match(memory_pattern, executor_memory):
                errors.append("executor_memory must be in format: <number>[k|m|g] (e.g., '2g', '512m')")
        
        return errors
