"""
DataFrame Pipeline for Spark Processing
"""

import sys
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def process_with_dataframe(spark, data_path):
    """Process data using DataFrame API with detailed performance metrics."""
    start_time = time.time()
    
    # Load data as DataFrame
    df = spark.read.json(data_path)
    
    # Flatten the array column
    df_exploded = df.select(explode(df.columns[0]).alias("value"))
    
    # Cache the DataFrame for multiple operations
    df_exploded.cache()
    
    # Calculate basic statistics using DataFrame operations
    stats_df = df_exploded.agg(
        count("*").alias("count"),
        mean("value").alias("mean"),
        variance("value").alias("variance"),
        stddev("value").alias("std_dev"),
        min("value").alias("min"),
        max("value").alias("max")
    )
    
    # Collect basic statistics
    stats = stats_df.collect()[0]
    count = stats["count"]
    mean_val = stats["mean"]
    variance = stats["variance"]
    std_dev = stats["std_dev"]
    min_val = stats["min"]
    max_val = stats["max"]
    
    # Calculate range
    range_val = max_val - min_val
    
    # Calculate percentiles using DataFrame
    percentiles_df = df_exploded.approxQuantile("value", [0.25, 0.5, 0.75, 0.9, 0.95, 0.99], 0.01)
    percentiles = {
        'p25': percentiles_df[0],
        'p50': percentiles_df[1],
        'p75': percentiles_df[2],
        'p90': percentiles_df[3],
        'p95': percentiles_df[4],
        'p99': percentiles_df[5]
    }
    
    # Calculate skewness and kurtosis using DataFrame operations
    # Skewness: E[(X - μ)^3] / σ^3
    # Kurtosis: E[(X - μ)^4] / σ^4 - 3
    
    df_with_stats = df_exploded.withColumn("centered", col("value") - mean_val)
    
    # Calculate moments
    moments_df = df_with_stats.agg(
        avg(pow(col("centered"), 3)).alias("third_moment"),
        avg(pow(col("centered"), 4)).alias("fourth_moment")
    )
    
    moments = moments_df.collect()[0]
    third_moment = moments["third_moment"]
    fourth_moment = moments["fourth_moment"]
    
    # Calculate skewness and kurtosis
    skewness = third_moment / (std_dev ** 3) if std_dev > 0 else 0
    kurtosis = (fourth_moment / (std_dev ** 4)) - 3 if std_dev > 0 else 0
    
    # Calculate additional statistics
    # Mode (most frequent value)
    mode_df = df_exploded.groupBy("value").count().orderBy(col("count").desc()).limit(1)
    mode_result = mode_df.collect()
    mode = mode_result[0]["value"] if mode_result else None
    
    # Median (50th percentile)
    median = percentiles['p50']
    
    # Calculate quartiles
    q1 = percentiles['p25']
    q3 = percentiles['p75']
    iqr = q3 - q1
    
    # Identify outliers (values beyond 1.5 * IQR)
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    
    outliers_df = df_exploded.filter(
        (col("value") < lower_bound) | (col("value") > upper_bound)
    )
    outlier_count = outliers_df.count()
    
    processing_time = time.time() - start_time
    
    # Unpersist cached DataFrame
    df_exploded.unpersist()
    
    return {
        "processing_time": processing_time,
        "statistics": {
            "count": count,
            "mean": mean_val,
            "median": median,
            "mode": mode,
            "variance": variance,
            "std_dev": std_dev,
            "min": min_val,
            "max": max_val,
            "range": range_val,
            "percentiles": percentiles,
            "quartiles": {
                "q1": q1,
                "q3": q3,
                "iqr": iqr
            },
            "skewness": skewness,
            "kurtosis": kurtosis,
            "outliers": {
                "count": outlier_count,
                "lower_bound": lower_bound,
                "upper_bound": upper_bound
            }
        },
        "pipeline_type": "dataframe",
        "operations": [
            "read.json",
            "select (explode)",
            "cache",
            "agg (basic statistics)",
            "approxQuantile (percentiles)",
            "withColumn (centering)",
            "agg (moments)",
            "groupBy (mode)",
            "filter (outliers)",
            "count (outliers)"
        ]
    }


def main():
    """Main function for DataFrame pipeline processing."""
    if len(sys.argv) != 3:
        print("Usage: dataframe_pipeline.py <data_path> <output_path>")
        sys.exit(1)
    
    data_path = sys.argv[1]
    output_path = sys.argv[2]
    
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("DataFrame Pipeline Processing") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m") \
        .getOrCreate()
    
    try:
        # Process with DataFrame
        dataframe_results = process_with_dataframe(spark, data_path)
        
        # Save results
        spark.sparkContext.parallelize([json.dumps(dataframe_results)]).saveAsTextFile(output_path)
        
        print(f"DataFrame processing completed in {dataframe_results['processing_time']:.3f}s")
        print(f"Processed {dataframe_results['statistics']['count']} data points")
        print(f"Mean: {dataframe_results['statistics']['mean']:.4f}")
        print(f"Standard Deviation: {dataframe_results['statistics']['std_dev']:.4f}")
        print(f"Outliers detected: {dataframe_results['statistics']['outliers']['count']}")
        
    except Exception as e:
        print(f"Error in DataFrame processing: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
