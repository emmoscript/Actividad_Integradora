"""
RDD Pipeline for Spark Processing
"""

import sys
import json
import time
from pyspark.sql import SparkSession
from pyspark import SparkContext


def process_with_rdd(spark, data_path):
    """Process data using RDD API with detailed performance metrics."""
    start_time = time.time()
    
    # Load data
    data_rdd = spark.sparkContext.textFile(data_path)
    
    # Parse JSON data
    parsed_rdd = data_rdd.map(lambda x: json.loads(x))
    
    # Extract numeric values
    values_rdd = parsed_rdd.flatMap(lambda x: x if isinstance(x, list) else [x])
    
    # Cache the RDD for multiple operations
    values_rdd.cache()
    
    # Calculate basic statistics
    count = values_rdd.count()
    total = values_rdd.reduce(lambda a, b: a + b)
    mean = total / count
    
    # Calculate variance using map-reduce
    squared_diff_rdd = values_rdd.map(lambda x: (x - mean) ** 2)
    variance = squared_diff_rdd.reduce(lambda a, b: a + b) / count
    
    # Calculate additional statistics
    min_val = values_rdd.min()
    max_val = values_rdd.max()
    
    # Calculate percentiles
    sorted_rdd = values_rdd.sortBy(lambda x: x)
    percentiles = {}
    for p in [25, 50, 75, 90, 95, 99]:
        index = int(count * p / 100)
        if index < count:
            percentile_val = sorted_rdd.take(index + 1)[-1]
            percentiles[f'p{p}'] = percentile_val
    
    # Calculate skewness and kurtosis
    cubed_diff_rdd = values_rdd.map(lambda x: ((x - mean) ** 3))
    fourth_power_diff_rdd = values_rdd.map(lambda x: ((x - mean) ** 4))
    
    skewness = cubed_diff_rdd.reduce(lambda a, b: a + b) / (count * (variance ** 1.5))
    kurtosis = fourth_power_diff_rdd.reduce(lambda a, b: a + b) / (count * (variance ** 2)) - 3
    
    processing_time = time.time() - start_time
    
    # Unpersist cached RDD
    values_rdd.unpersist()
    
    return {
        "processing_time": processing_time,
        "statistics": {
            "count": count,
            "mean": mean,
            "variance": variance,
            "std_dev": variance ** 0.5,
            "min": min_val,
            "max": max_val,
            "range": max_val - min_val,
            "percentiles": percentiles,
            "skewness": skewness,
            "kurtosis": kurtosis
        },
        "pipeline_type": "rdd",
        "operations": [
            "textFile",
            "map (JSON parsing)",
            "flatMap (value extraction)",
            "cache",
            "count",
            "reduce (sum)",
            "map (squared differences)",
            "reduce (variance)",
            "min",
            "max",
            "sortBy",
            "take (percentiles)",
            "map (cubed differences)",
            "map (fourth power differences)",
            "reduce (skewness/kurtosis)"
        ]
    }


def main():
    """Main function for RDD pipeline processing."""
    if len(sys.argv) != 3:
        print("Usage: rdd_pipeline.py <data_path> <output_path>")
        sys.exit(1)
    
    data_path = sys.argv[1]
    output_path = sys.argv[2]
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("RDD Pipeline Processing") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    try:
        # Process with RDD
        rdd_results = process_with_rdd(spark, data_path)
        
        # Save results
        spark.sparkContext.parallelize([json.dumps(rdd_results)]).saveAsTextFile(output_path)
        
        print(f"RDD processing completed in {rdd_results['processing_time']:.3f}s")
        print(f"Processed {rdd_results['statistics']['count']} data points")
        print(f"Mean: {rdd_results['statistics']['mean']:.4f}")
        print(f"Standard Deviation: {rdd_results['statistics']['std_dev']:.4f}")
        
    except Exception as e:
        print(f"Error in RDD processing: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
