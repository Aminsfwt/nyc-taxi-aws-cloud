import os
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# Load credentials/config from nyc_taxi_aws_cloud/.env regardless of current working directory.
ENV_PATH = Path(__file__).with_name(".env")
load_dotenv(dotenv_path=ENV_PATH)

# Required AWS settings used to authenticate S3A requests from Spark.
aws_access_key = os.environ["AWS_ACCESS_KEY"]
aws_secret_key = os.environ["AWS_SECRET_KEY"]
aws_region = os.environ["AWS_REGION"]
aws_bucket = os.environ["AWS_BUCKET"]

# Target dataset location in S3 (Green Taxi processed parquet partitions).
s3_uri = f"s3a://{aws_bucket}/reports/"

# Build a SparkSession configured for S3A access.
spark = (
    SparkSession.builder
    .appName("Read S3 Data")
    .config(
        "spark.jars.packages",
        # S3A connector + AWS SDK needed to read/write s3a:// paths.
        "org.apache.hadoop:hadoop-aws:3.4.1,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.367",
    )
    .config(
        "spark.driver.extraJavaOptions",
        # Java module opens for driver JVM compatibility on modern Java versions.
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
    )
    .config(
        "spark.executor.extraJavaOptions",
        # Same module opens for executor JVMs.
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
    )
    # Tell Hadoop to use the S3A filesystem implementation.
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        # Use explicit access key + secret key from the environment.
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
    # Regional endpoint helps avoid redirects/signature issues.
    .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com")
    # AWS S3 uses virtual-hosted-style access by default (path-style disabled).
    .config("spark.hadoop.fs.s3a.path.style.access", "false")
    .getOrCreate()
)

# Reduce Spark logs to errors to keep console output focused.
spark.sparkContext.setLogLevel("ERROR")

# Read parquet files from S3 and preview selected columns.
df = spark.read.parquet(s3_uri)
df.show(5, truncate=False)






