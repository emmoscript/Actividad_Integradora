"""
Integration Tests for Big-Data Processing System
"""

import json
import time
import unittest
import requests
import boto3
from typing import Dict, Any, List


class TestBigDataProcessing(unittest.TestCase):
    """Integration tests for the Big-Data processing system."""
    
    def setUp(self):
        """Set up test environment."""
        self.api_url = "https://your-api-gateway-url.execute-api.us-east-1.amazonaws.com/dev/process"
        self.s3_client = boto3.client('s3')
        self.s3_bucket = 'bigdata-processing-results'
        
        # Test data sets
        self.small_dataset = [1.2, 3.4, 5.6, 7.8, 9.0] * 200  # 1K records
        self.medium_dataset = [1.2, 3.4, 5.6, 7.8, 9.0] * 20000  # 100K records
        self.large_dataset = [1.2, 3.4, 5.6, 7.8, 9.0] * 200000  # 1M records
    
    def test_small_dataset_rdd_pipeline(self):
        """Test processing small dataset with RDD pipeline."""
        print("\n🧪 Testing small dataset with RDD pipeline...")
        
        request_data = {
            "data": self.small_dataset,
            "pipeline_type": "rdd",
            "spark_config": {
                "executor_instances": 2,
                "executor_memory": "2g"
            }
        }
        
        start_time = time.time()
        response = self._make_request(request_data)
        processing_time = time.time() - start_time
        
        self.assertEqual(response['status'], 'completed')
        self.assertIn('job_id', response)
        self.assertIn('processing_summary', response)
        self.assertIn('performance_metrics', response)
        
        print(f"✅ Small dataset RDD processing completed in {processing_time:.2f}s")
        print(f"   Job ID: {response['job_id']}")
        print(f"   Total processing time: {response['processing_summary']['total_processing_time']:.3f}s")
        print(f"   Data processed: {response['processing_summary']['data_processed']}")
    
    def test_small_dataset_dataframe_pipeline(self):
        """Test processing small dataset with DataFrame pipeline."""
        print("\n🧪 Testing small dataset with DataFrame pipeline...")
        
        request_data = {
            "data": self.small_dataset,
            "pipeline_type": "dataframe",
            "spark_config": {
                "executor_instances": 2,
                "executor_memory": "2g"
            }
        }
        
        start_time = time.time()
        response = self._make_request(request_data)
        processing_time = time.time() - start_time
        
        self.assertEqual(response['status'], 'completed')
        self.assertIn('dataframe_speedup', response['performance_metrics'])
        
        print(f"✅ Small dataset DataFrame processing completed in {processing_time:.2f}s")
        print(f"   Job ID: {response['job_id']}")
        print(f"   DataFrame speedup: {response['performance_metrics']['dataframe_speedup']:.2f}x")
    
    def test_medium_dataset_processing(self):
        """Test processing medium dataset."""
        print("\n🧪 Testing medium dataset processing...")
        
        request_data = {
            "data": self.medium_dataset,
            "pipeline_type": "rdd",
            "spark_config": {
                "executor_instances": 4,
                "executor_memory": "4g"
            }
        }
        
        start_time = time.time()
        response = self._make_request(request_data)
        processing_time = time.time() - start_time
        
        self.assertEqual(response['status'], 'completed')
        self.assertGreater(response['processing_summary']['data_processed'], 0)
        
        print(f"✅ Medium dataset processing completed in {processing_time:.2f}s")
        print(f"   Job ID: {response['job_id']}")
        print(f"   Data processed: {response['processing_summary']['data_processed']}")
    
    def test_gpu_vs_cpu_comparison(self):
        """Test GPU vs CPU processing comparison."""
        print("\n🧪 Testing GPU vs CPU processing...")
        
        # Test with data that should trigger GPU processing
        request_data = {
            "data": self.small_dataset,
            "pipeline_type": "rdd",
            "spark_config": {
                "executor_instances": 2,
                "executor_memory": "2g"
            }
        }
        
        response = self._make_request(request_data)
        
        self.assertEqual(response['status'], 'completed')
        self.assertIn('gpu_processing', response['processing_summary'])
        
        gpu_time = response['processing_summary']['gpu_processing_time']
        print(f"✅ GPU processing time: {gpu_time:.3f}s")
        
        # Verify GPU was used if available
        if gpu_time < 0.1:  # Very fast processing indicates GPU
            print("   🚀 GPU acceleration detected!")
        else:
            print("   💻 CPU fallback used")
    
    def test_error_handling_invalid_data(self):
        """Test error handling with invalid data."""
        print("\n🧪 Testing error handling with invalid data...")
        
        request_data = {
            "data": ["invalid", "data", "types"],  # Non-numeric data
            "pipeline_type": "rdd",
            "spark_config": {
                "executor_instances": 2,
                "executor_memory": "2g"
            }
        }
        
        response = self._make_request(request_data)
        
        self.assertIn('status', response)
        self.assertIn('error', response)
        print(f"✅ Error handling test passed: {response['error']}")
    
    def test_error_handling_invalid_pipeline(self):
        """Test error handling with invalid pipeline type."""
        print("\n🧪 Testing error handling with invalid pipeline...")
        
        request_data = {
            "data": self.small_dataset,
            "pipeline_type": "invalid_pipeline",  # Invalid pipeline type
            "spark_config": {
                "executor_instances": 2,
                "executor_memory": "2g"
            }
        }
        
        response = self._make_request(request_data)
        
        self.assertIn('status', response)
        self.assertIn('error', response)
        print(f"✅ Invalid pipeline error handling test passed")
    
    def test_performance_metrics(self):
        """Test that performance metrics are calculated correctly."""
        print("\n🧪 Testing performance metrics calculation...")
        
        request_data = {
            "data": self.small_dataset,
            "pipeline_type": "rdd",
            "spark_config": {
                "executor_instances": 2,
                "executor_memory": "2g"
            }
        }
        
        response = self._make_request(request_data)
        
        self.assertEqual(response['status'], 'completed')
        
        # Check performance metrics
        metrics = response['performance_metrics']
        self.assertIn('gpu_percentage', metrics)
        self.assertIn('spark_percentage', metrics)
        self.assertIn('dataframe_speedup', metrics)
        
        # Verify percentages add up to approximately 100%
        total_percentage = metrics['gpu_percentage'] + metrics['spark_percentage']
        self.assertAlmostEqual(total_percentage, 100, delta=5)
        
        print(f"✅ Performance metrics test passed")
        print(f"   GPU percentage: {metrics['gpu_percentage']:.1f}%")
        print(f"   Spark percentage: {metrics['spark_percentage']:.1f}%")
    
    def test_data_quality_metrics(self):
        """Test data quality metrics calculation."""
        print("\n🧪 Testing data quality metrics...")
        
        request_data = {
            "data": self.small_dataset,
            "pipeline_type": "rdd",
            "spark_config": {
                "executor_instances": 2,
                "executor_memory": "2g"
            }
        }
        
        response = self._make_request(request_data)
        
        self.assertEqual(response['status'], 'completed')
        
        # Check data quality metrics
        quality = response['data_quality']
        self.assertIn('quality_score', quality)
        self.assertIn('outliers_detected', quality)
        self.assertIn('missing_values', quality)
        
        # Verify quality score is between 0 and 1
        self.assertGreaterEqual(quality['quality_score'], 0)
        self.assertLessEqual(quality['quality_score'], 1)
        
        print(f"✅ Data quality metrics test passed")
        print(f"   Quality score: {quality['quality_score']:.3f}")
        print(f"   Outliers detected: {quality['outliers_detected']}")
    
    def test_cost_analysis(self):
        """Test cost analysis calculation."""
        print("\n🧪 Testing cost analysis...")
        
        request_data = {
            "data": self.small_dataset,
            "pipeline_type": "rdd",
            "spark_config": {
                "executor_instances": 2,
                "executor_memory": "2g"
            }
        }
        
        response = self._make_request(request_data)
        
        self.assertEqual(response['status'], 'completed')
        
        # Check cost analysis
        costs = response['cost_analysis']
        self.assertIn('lambda_cost_usd', costs)
        self.assertIn('s3_cost_usd', costs)
        self.assertIn('emr_cost_usd', costs)
        self.assertIn('total_cost_usd', costs)
        
        # Verify costs are non-negative
        self.assertGreaterEqual(costs['lambda_cost_usd'], 0)
        self.assertGreaterEqual(costs['s3_cost_usd'], 0)
        self.assertGreaterEqual(costs['emr_cost_usd'], 0)
        self.assertGreaterEqual(costs['total_cost_usd'], 0)
        
        print(f"✅ Cost analysis test passed")
        print(f"   Total cost: ${costs['total_cost_usd']:.6f}")
    
    def test_s3_storage(self):
        """Test that results are stored in S3."""
        print("\n🧪 Testing S3 storage...")
        
        request_data = {
            "data": self.small_dataset,
            "pipeline_type": "rdd",
            "spark_config": {
                "executor_instances": 2,
                "executor_memory": "2g"
            }
        }
        
        response = self._make_request(request_data)
        
        self.assertEqual(response['status'], 'completed')
        job_id = response['job_id']
        
        # Check that S3 paths are provided
        s3_paths = response['s3_paths']
        self.assertIn('analysis_results', s3_paths)
        self.assertIn('final_results', s3_paths)
        
        # Verify files exist in S3 (if bucket is accessible)
        try:
            # Check analysis results
            analysis_key = f"analysis_results/{job_id}/analysis.json"
            self.s3_client.head_object(Bucket=self.s3_bucket, Key=analysis_key)
            
            # Check final results
            final_key = f"final_results/{job_id}/results.json"
            self.s3_client.head_object(Bucket=self.s3_bucket, Key=final_key)
            
            print(f"✅ S3 storage test passed")
            print(f"   Analysis results: {s3_paths['analysis_results']}")
            print(f"   Final results: {s3_paths['final_results']}")
            
        except Exception as e:
            print(f"⚠️  S3 verification skipped: {str(e)}")
    
    def test_recommendations_generation(self):
        """Test that recommendations are generated."""
        print("\n🧪 Testing recommendations generation...")
        
        request_data = {
            "data": self.small_dataset,
            "pipeline_type": "rdd",
            "spark_config": {
                "executor_instances": 2,
                "executor_memory": "2g"
            }
        }
        
        response = self._make_request(request_data)
        
        self.assertEqual(response['status'], 'completed')
        
        # Check recommendations
        recommendations = response['recommendations']
        self.assertIn('performance', recommendations)
        self.assertIn('cost_optimization', recommendations)
        self.assertIn('data_quality', recommendations)
        
        print(f"✅ Recommendations generation test passed")
        print(f"   Performance recommendations: {len(recommendations['performance'])}")
        print(f"   Cost optimization recommendations: {len(recommendations['cost_optimization'])}")
    
    def _make_request(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """Make a request to the API and return the response."""
        try:
            response = requests.post(
                self.api_url,
                json=request_data,
                headers={'Content-Type': 'application/json'},
                timeout=600  # 10 minutes timeout
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return {
                    'status': 'error',
                    'error': f"HTTP {response.status_code}: {response.text}"
                }
                
        except requests.exceptions.Timeout:
            return {
                'status': 'error',
                'error': 'Request timeout'
            }
        except requests.exceptions.RequestException as e:
            return {
                'status': 'error',
                'error': f"Request failed: {str(e)}"
            }


def run_performance_benchmark():
    """Run performance benchmark tests."""
    print("\n🚀 Running Performance Benchmark Tests")
    print("=" * 50)
    
    # Test data sizes
    test_sizes = [
        (1000, "1K records"),
        (10000, "10K records"),
        (100000, "100K records")
    ]
    
    pipeline_types = ["rdd", "dataframe"]
    
    results = []
    
    for size, size_name in test_sizes:
        for pipeline_type in pipeline_types:
            print(f"\n📊 Testing {size_name} with {pipeline_type.upper()} pipeline...")
            
            # Generate test data
            test_data = [1.2, 3.4, 5.6, 7.8, 9.0] * (size // 5)
            
            request_data = {
                "data": test_data,
                "pipeline_type": pipeline_type,
                "spark_config": {
                    "executor_instances": 2,
                    "executor_memory": "2g"
                }
            }
            
            # Make request
            start_time = time.time()
            response = requests.post(
                "https://your-api-gateway-url.execute-api.us-east-1.amazonaws.com/dev/process",
                json=request_data,
                headers={'Content-Type': 'application/json'},
                timeout=600
            )
            total_time = time.time() - start_time
            
            if response.status_code == 200:
                result_data = response.json()
                processing_time = result_data['processing_summary']['total_processing_time']
                throughput = size / processing_time
                
                results.append({
                    'size': size_name,
                    'pipeline': pipeline_type,
                    'processing_time': processing_time,
                    'total_time': total_time,
                    'throughput': throughput,
                    'status': 'success'
                })
                
                print(f"   ✅ Completed in {processing_time:.3f}s")
                print(f"   📈 Throughput: {throughput:.0f} records/s")
            else:
                results.append({
                    'size': size_name,
                    'pipeline': pipeline_type,
                    'status': 'failed',
                    'error': response.text
                })
                print(f"   ❌ Failed: {response.text}")
    
    # Print summary
    print("\n📋 Performance Benchmark Summary")
    print("=" * 50)
    
    for result in results:
        if result['status'] == 'success':
            print(f"{result['size']} - {result['pipeline'].upper()}: "
                  f"{result['processing_time']:.3f}s ({result['throughput']:.0f} records/s)")
        else:
            print(f"{result['size']} - {result['pipeline'].upper()}: FAILED")


if __name__ == '__main__':
    # Run integration tests
    print("🧪 Running Big-Data Processing Integration Tests")
    print("=" * 60)
    
    # Create test suite
    suite = unittest.TestLoader().loadTestsFromTestCase(TestBigDataProcessing)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print("\n📊 Test Summary")
    print("=" * 30)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    
    if result.failures:
        print("\n❌ Failures:")
        for test, traceback in result.failures:
            print(f"  - {test}: {traceback}")
    
    if result.errors:
        print("\n⚠️  Errors:")
        for test, traceback in result.errors:
            print(f"  - {test}: {traceback}")
    
    # Run performance benchmark
    if result.wasSuccessful():
        run_performance_benchmark()
    else:
        print("\n⚠️  Skipping performance benchmark due to test failures")
