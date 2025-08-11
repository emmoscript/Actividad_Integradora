# Terraform configuration for Big-Data Processing Infrastructure
terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# S3 Bucket for storing results
resource "aws_s3_bucket" "results_bucket" {
  bucket = var.s3_bucket_name

  tags = {
    Name        = "BigData Processing Results"
    Environment = var.environment
    Project     = "BigData-Processing"
  }
}

resource "aws_s3_bucket_versioning" "results_bucket_versioning" {
  bucket = aws_s3_bucket.results_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "results_bucket_access" {
  bucket = aws_s3_bucket.results_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# IAM Role for Lambda functions
resource "aws_iam_role" "lambda_role" {
  name = "bigdata-processing-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

# IAM Policy for Lambda functions
resource "aws_iam_role_policy" "lambda_policy" {
  name = "bigdata-processing-lambda-policy"
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = "${aws_s3_bucket.results_bucket.arn}/*"
      },
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = "arn:aws:lambda:*:*:function:*"
      },
      {
        Effect = "Allow"
        Action = [
          "elasticmapreduce:*"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "ec2:DescribeInstances",
          "ec2:DescribeSecurityGroups",
          "ec2:DescribeSubnets",
          "ec2:DescribeVpcs"
        ]
        Resource = "*"
      }
    ]
  })
}

# API Gateway
resource "aws_api_gateway_rest_api" "bigdata_api" {
  name        = "bigdata-processing-api"
  description = "API for Big-Data Processing Pipeline"

  endpoint_configuration {
    types = ["REGIONAL"]
  }
}

resource "aws_api_gateway_resource" "process_resource" {
  rest_api_id = aws_api_gateway_rest_api.bigdata_api.id
  parent_id   = aws_api_gateway_rest_api.bigdata_api.root_resource_id
  path_part   = "process"
}

resource "aws_api_gateway_method" "process_method" {
  rest_api_id   = aws_api_gateway_rest_api.bigdata_api.id
  resource_id   = aws_api_gateway_resource.process_resource.id
  http_method   = "POST"
  authorization = "NONE"
}

resource "aws_api_gateway_method" "options_method" {
  rest_api_id   = aws_api_gateway_rest_api.bigdata_api.id
  resource_id   = aws_api_gateway_resource.process_resource.id
  http_method   = "OPTIONS"
  authorization = "NONE"
}

# Lambda function for orchestrator
resource "aws_lambda_function" "orchestrator" {
  filename         = "./orchestrator.zip"
  function_name    = "bigdata-orchestrator"
  role            = aws_iam_role.lambda_role.arn
  handler         = "orchestrator.lambda_handler"
  runtime         = "python3.9"
  timeout         = 900  # 15 minutes
  memory_size     = 1024

  environment {
    variables = {
      S3_BUCKET = aws_s3_bucket.results_bucket.bucket
    }
  }

  tags = {
    Name        = "BigData Orchestrator"
    Environment = var.environment
  }
}

# Lambda function for GPU processing
resource "aws_lambda_function" "gpu_processing" {
  filename         = "./gpu_processing.zip"
  function_name    = "gpu-processing-lambda"
  role            = aws_iam_role.lambda_role.arn
  handler         = "gpu_processor.lambda_handler"
  runtime         = "python3.9"
  timeout         = 300  # 5 minutes
  memory_size     = 2048

  environment {
    variables = {
      S3_BUCKET = aws_s3_bucket.results_bucket.bucket
    }
  }

  tags = {
    Name        = "GPU Processing Lambda"
    Environment = var.environment
  }
}

# Lambda function for Spark launcher
resource "aws_lambda_function" "spark_launcher" {
  filename         = "./spark_launcher.zip"
  function_name    = "spark-launcher-lambda"
  role            = aws_iam_role.lambda_role.arn
  handler         = "spark_launcher.lambda_handler"
  runtime         = "python3.9"
  timeout         = 900  # 15 minutes
  memory_size     = 1024

  environment {
    variables = {
      S3_BUCKET = aws_s3_bucket.results_bucket.bucket
    }
  }

  tags = {
    Name        = "Spark Launcher Lambda"
    Environment = var.environment
  }
}

# API Gateway integration
resource "aws_api_gateway_integration" "process_integration" {
  rest_api_id = aws_api_gateway_rest_api.bigdata_api.id
  resource_id = aws_api_gateway_resource.process_resource.id
  http_method = aws_api_gateway_method.process_method.http_method

  integration_http_method = "POST"
  type                   = "AWS_PROXY"
  uri                    = aws_lambda_function.orchestrator.invoke_arn
}

resource "aws_api_gateway_integration" "options_integration" {
  rest_api_id = aws_api_gateway_rest_api.bigdata_api.id
  resource_id = aws_api_gateway_resource.process_resource.id
  http_method = aws_api_gateway_method.options_method.http_method

  type = "MOCK"
  request_templates = {
    "application/json" = jsonencode({
      statusCode = 200
    })
  }
}

resource "aws_api_gateway_method_response" "options_200" {
  rest_api_id = aws_api_gateway_rest_api.bigdata_api.id
  resource_id = aws_api_gateway_resource.process_resource.id
  http_method = aws_api_gateway_method.options_method.http_method
  status_code = "200"

  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" = true
    "method.response.header.Access-Control-Allow-Methods" = true
    "method.response.header.Access-Control-Allow-Origin"  = true
  }
}

resource "aws_api_gateway_integration_response" "options_integration_response" {
  rest_api_id = aws_api_gateway_rest_api.bigdata_api.id
  resource_id = aws_api_gateway_resource.process_resource.id
  http_method = aws_api_gateway_method.options_method.http_method
  status_code = aws_api_gateway_method_response.options_200.status_code

  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" = "'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token'"
    "method.response.header.Access-Control-Allow-Methods" = "'GET,OPTIONS,POST,PUT'"
    "method.response.header.Access-Control-Allow-Origin"  = "'*'"
  }
}

# Lambda permissions
resource "aws_lambda_permission" "api_gateway" {
  statement_id  = "AllowExecutionFromAPIGateway"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.orchestrator.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.bigdata_api.execution_arn}/*/*"
}

# API Gateway deployment
resource "aws_api_gateway_deployment" "deployment" {
  depends_on = [
    aws_api_gateway_integration.process_integration,
    aws_api_gateway_integration.options_integration,
  ]

  rest_api_id = aws_api_gateway_rest_api.bigdata_api.id
  stage_name  = var.environment
}

# CloudWatch Log Groups
resource "aws_cloudwatch_log_group" "orchestrator_logs" {
  name              = "/aws/lambda/${aws_lambda_function.orchestrator.function_name}"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "gpu_processing_logs" {
  name              = "/aws/lambda/${aws_lambda_function.gpu_processing.function_name}"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "spark_launcher_logs" {
  name              = "/aws/lambda/${aws_lambda_function.spark_launcher.function_name}"
  retention_in_days = 14
}

# Outputs
output "api_gateway_url" {
  value = "https://${aws_api_gateway_rest_api.bigdata_api.id}.execute-api.${var.aws_region}.amazonaws.com/${var.environment}/process"
}

output "s3_bucket_name" {
  value = aws_s3_bucket.results_bucket.bucket
}

output "orchestrator_function_name" {
  value = aws_lambda_function.orchestrator.function_name
}

output "gpu_processing_function_name" {
  value = aws_lambda_function.gpu_processing.function_name
}

output "spark_launcher_function_name" {
  value = aws_lambda_function.spark_launcher.function_name
}
