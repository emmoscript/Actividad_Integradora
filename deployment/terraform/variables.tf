# Terraform variables for Big-Data Processing Infrastructure

variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "s3_bucket_name" {
  description = "Name for the S3 bucket to store results"
  type        = string
  default     = "bigdata-processing-results"
}

variable "lambda_timeout" {
  description = "Timeout for Lambda functions in seconds"
  type        = number
  default     = 900
}

variable "lambda_memory_size" {
  description = "Memory size for Lambda functions in MB"
  type        = number
  default     = 1024
}

variable "gpu_lambda_memory_size" {
  description = "Memory size for GPU processing Lambda function in MB"
  type        = number
  default     = 2048
}

variable "emr_cluster_name" {
  description = "Name for the EMR cluster"
  type        = string
  default     = "bigdata-processing-cluster"
}

variable "emr_instance_type" {
  description = "Instance type for EMR cluster"
  type        = string
  default     = "m5.xlarge"
}

variable "emr_core_instances" {
  description = "Number of core instances for EMR cluster"
  type        = number
  default     = 2
}
