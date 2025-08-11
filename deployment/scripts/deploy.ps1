# PowerShell Deployment Script for Big-Data Processing Lambda Functions

# Configuration
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$DeploymentDir = Join-Path $ProjectRoot "deployment"
$LambdaDir = Join-Path $ProjectRoot "src\lambda_functions"
$BuildDir = Join-Path $ProjectRoot "build"

Write-Host "🚀 Starting Big-Data Processing deployment..." -ForegroundColor Green

# Create build directory
if (!(Test-Path $BuildDir)) {
    New-Item -ItemType Directory -Path $BuildDir | Out-Null
}

# Function to build Lambda package
function Build-LambdaPackage {
    param(
        [string]$FunctionName,
        [string]$SourceDir
    )
    
    Write-Host "📦 Building $FunctionName Lambda package..." -ForegroundColor Yellow
    
    # Create temporary directory for packaging
    $TempDir = New-TemporaryFile | ForEach-Object { Remove-Item $_; New-Item -ItemType Directory -Path $_ }
    
    try {
        # Copy source files
        Copy-Item -Path "$SourceDir\*" -Destination $TempDir.FullName -Recurse -Force
        
        # Install dependencies
        if (Test-Path (Join-Path $ProjectRoot "requirements.txt")) {
            Write-Host "Installing dependencies..."
            pip install -r (Join-Path $ProjectRoot "requirements.txt") -t $TempDir.FullName --no-deps
        }
        
        # Create ZIP file
        $ZipFile = Join-Path $BuildDir "$FunctionName.zip"
        Compress-Archive -Path "$($TempDir.FullName)\*" -DestinationPath $ZipFile -Force
        
        Write-Host "✅ Built $FunctionName package: $ZipFile" -ForegroundColor Green
        
    } finally {
        # Clean up
        Remove-Item $TempDir.FullName -Recurse -Force
    }
}

# Build all Lambda packages
Write-Host "🔨 Building Lambda function packages..." -ForegroundColor Yellow

# Build orchestrator
Build-LambdaPackage "orchestrator" (Join-Path $LambdaDir "orchestrator")

# Build GPU processing
Build-LambdaPackage "gpu_processing" (Join-Path $LambdaDir "gpu_processing")

# Build Spark launcher
Build-LambdaPackage "spark_launcher" (Join-Path $LambdaDir "spark_launcher")

# Copy packages to deployment directory
Write-Host "📁 Copying packages to deployment directory..." -ForegroundColor Yellow
Copy-Item -Path "$BuildDir\*.zip" -Destination (Join-Path $DeploymentDir "terraform") -Force

# Deploy infrastructure with Terraform
Write-Host "🏗️  Deploying infrastructure with Terraform..." -ForegroundColor Yellow
Set-Location (Join-Path $DeploymentDir "terraform")

# Initialize Terraform
Write-Host "Initializing Terraform..."
terraform init

# Plan deployment
Write-Host "Planning deployment..."
terraform plan -out=tfplan

# Apply deployment
Write-Host "Applying deployment..."
terraform apply tfplan

# Get outputs
Write-Host "📋 Deployment outputs:" -ForegroundColor Green
terraform output

# Clean up plan file
Remove-Item tfplan -ErrorAction SilentlyContinue

Write-Host "🎉 Deployment completed successfully!" -ForegroundColor Green

# Get API Gateway URL
$ApiUrl = terraform output -raw api_gateway_url
$S3Bucket = terraform output -raw s3_bucket_name

Write-Host "📡 API Gateway URL: $ApiUrl" -ForegroundColor Green
Write-Host "🪣 S3 Bucket: $S3Bucket" -ForegroundColor Green

# Test the deployment
Write-Host "🧪 Testing deployment..." -ForegroundColor Yellow

# Create test data
$TestData = @{
    data = @(1.2, 3.4, 5.6, 7.8, 9.0, 11.2, 13.4, 15.6, 17.8, 19.0)
    pipeline_type = "rdd"
    spark_config = @{
        executor_instances = 2
        executor_memory = "2g"
    }
} | ConvertTo-Json -Depth 3

# Test API endpoint
Write-Host "Testing API endpoint..."
try {
    $Response = Invoke-RestMethod -Uri $ApiUrl -Method POST -Body $TestData -ContentType "application/json" -TimeoutSec 300
    Write-Host "✅ Test completed successfully!" -ForegroundColor Green
    Write-Host "Response: $($Response | ConvertTo-Json -Depth 3)" -ForegroundColor Cyan
} catch {
    Write-Host "❌ Test failed: $($_.Exception.Message)" -ForegroundColor Red
}

Write-Host "🎯 Big-Data Processing system is ready!" -ForegroundColor Green
Write-Host "📚 Check the README.md for usage instructions." -ForegroundColor Yellow
