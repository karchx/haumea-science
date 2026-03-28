from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("silver-haumea-py").getOrCreate()

def main():
    spark.sparkContext.setLogLevel("WARN")
    print("Reading data from MinIO...")
    df = spark.read.parquet("s3a://gaia-source/bronze/astrophysical_parameters/")
    df.show(5)

if __name__ == "__main__":
    main()
    spark.stop()

