#!/bin/bash

# Deployment script for Big-Data Processing Lambda Functions
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DEPLOYMENT_DIR="$PROJECT_ROOT/deployment"
LAMBDA_DIR="$PROJECT_ROOT/src/lambda_functions"
BUILD_DIR="$PROJECT_ROOT/build"

echo -e "${GREEN}🚀 Starting Big-Data Processing deployment...${NC}"

# Create build directory
mkdir -p "$BUILD_DIR"

# Function to build Lambda package
build_lambda_package() {
    local function_name=$1
    local source_dir=$2
    local zip_file="$BUILD_DIR/${function_name}.zip"
    
    echo -e "${YELLOW}📦 Building $function_name Lambda package...${NC}"
    
    # Create temporary directory for packaging
    local temp_dir=$(mktemp -d)
    
    # Copy source files
    cp -r "$source_dir"/* "$temp_dir/"
    
    # Install dependencies
    if [ -f "$PROJECT_ROOT/requirements.txt" ]; then
        echo "Installing dependencies..."
        pip install -r "$PROJECT_ROOT/requirements.txt" -t "$temp_dir" --no-deps
    fi
    
    # Create ZIP file
    cd "$temp_dir"
    zip -r "$zip_file" . -x "*.pyc" "__pycache__/*" "*.DS_Store"
    cd - > /dev/null
    
    # Clean up
    rm -rf "$temp_dir"
    
    echo -e "${GREEN}✅ Built $function_name package: $zip_file${NC}"
}

# Build all Lambda packages
echo -e "${YELLOW}🔨 Building Lambda function packages...${NC}"

# Build orchestrator
build_lambda_package "orchestrator" "$LAMBDA_DIR/orchestrator"

# Build GPU processing
build_lambda_package "gpu_processing" "$LAMBDA_DIR/gpu_processing"

# Build Spark launcher
build_lambda_package "spark_launcher" "$LAMBDA_DIR/spark_launcher"

# Copy packages to deployment directory
echo -e "${YELLOW}📁 Copying packages to deployment directory...${NC}"
cp "$BUILD_DIR"/*.zip "$DEPLOYMENT_DIR/terraform/"

# Deploy infrastructure with Terraform
echo -e "${YELLOW}🏗️  Deploying infrastructure with Terraform...${NC}"
cd "$DEPLOYMENT_DIR/terraform"

# Initialize Terraform
echo "Initializing Terraform..."
terraform init

# Plan deployment
echo "Planning deployment..."
terraform plan -out=tfplan

# Apply deployment
echo "Applying deployment..."
terraform apply tfplan

# Get outputs
echo -e "${GREEN}📋 Deployment outputs:${NC}"
terraform output

# Clean up plan file
rm -f tfplan

echo -e "${GREEN}🎉 Deployment completed successfully!${NC}"
echo -e "${GREEN}📡 API Gateway URL: $(terraform output -raw api_gateway_url)${NC}"
echo -e "${GREEN}🪣 S3 Bucket: $(terraform output -raw s3_bucket_name)${NC}"

# Test the deployment
echo -e "${YELLOW}🧪 Testing deployment...${NC}"
API_URL=$(terraform output -raw api_gateway_url)

# Create test data
cat > /tmp/test_data.json << EOF
{
  "data": [1.2, 3.4, 5.6, 7.8, 9.0, 11.2, 13.4, 15.6, 17.8, 19.0],
  "pipeline_type": "rdd",
  "spark_config": {
    "executor_instances": 2,
    "executor_memory": "2g"
  }
}
EOF

# Test API endpoint
echo "Testing API endpoint..."
curl -X POST "$API_URL" \
  -H "Content-Type: application/json" \
  -d @/tmp/test_data.json \
  --max-time 300

echo -e "${GREEN}✅ Test completed!${NC}"

# Clean up test file
rm -f /tmp/test_data.json

echo -e "${GREEN}🎯 Big-Data Processing system is ready!${NC}"
echo -e "${YELLOW}📚 Check the README.md for usage instructions.${NC}"
