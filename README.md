# NYC Taxi AWS Cloud (PySpark + S3A)

This folder contains a minimal PySpark job that reads a Parquet dataset from Amazon S3 using the `s3a://` connector and prints a small preview to the console.

## What `main.py` does

- Loads AWS settings from `nyc_taxi_aws_cloud/.env` (so it works even if you run it from the repo root).
- Creates a `SparkSession` configured with:
  - `hadoop-aws` + AWS SDK JARs (downloaded automatically via `spark.jars.packages`).
  - S3A filesystem + simple access key authentication.
  - An explicit regional S3 endpoint to avoid redirect/signature issues.
- Reads Parquet files from `s3a://$AWS_BUCKET/`.

## Prerequisites

- Python (this repo uses `pyproject.toml`; dependencies include `pyspark` and `python-dotenv`).
- Java installed and available to Spark (set `JAVA_HOME` if needed).
- Network access to:
  - Your S3 bucket.
  - Maven repositories (Spark downloads the `hadoop-aws` and AWS SDK JARs automatically).
- An AWS IAM user/role that can read the target S3 path.

## Setup

1. Install dependencies (choose one):

   - Using `uv` (recommended if you already use it in this repo):
     ```bash
     uv sync
     ```
   - Using `pip`:
     ```bash
     pip install -e .
     ```

2. Create `nyc_taxi_aws_cloud/.env`:

   - Start from the example:
     ```bash
     copy .env.example .env
     ```
   - Fill in your values:
     - `AWS_ACCESS_KEY`
     - `AWS_SECRET_KEY`
     - `AWS_REGION` (example: `us-east-1`)
     - `AWS_BUCKET` (example: `nyc-taxi-pyspark-de-bootcamp`)

## Run

From the repo root:

```bash
python nyc_taxi_aws_cloud/main.py
```

Or from inside the folder:

```bash
cd nyc_taxi_aws_cloud
python main.py
```

If everything is configured correctly, you should see a Spark DataFrame preview printed to the terminal.

## Common issues

- `No FileSystem for scheme: s3a`:
  - The S3A connector JARs did not load. Make sure the machine can download Maven packages and that Spark is allowed to fetch `spark.jars.packages`.
- `SignatureDoesNotMatch` / redirects:
  - Ensure `AWS_REGION` matches your bucket region. This project configures `spark.hadoop.fs.s3a.endpoint` based on `AWS_REGION`.
- `AccessDenied`:
  - Confirm the IAM policy allows `s3:ListBucket` and `s3:GetObject` on the bucket and the `reports/` prefix.

