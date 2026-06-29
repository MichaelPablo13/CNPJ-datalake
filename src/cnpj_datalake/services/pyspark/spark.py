"""SparkSession factory configured for S3A (MinIO) and PostgreSQL JDBC."""

from pyspark.sql import SparkSession

from src.cnpj_datalake.config import DataLakeConfig


def build_spark_session(config: DataLakeConfig) -> SparkSession:
    session = (
        SparkSession.builder.appName(config.spark.app_name)
        .master(config.spark.master)
        .config("spark.driver.memory", config.spark.driver_memory)
        .config("spark.executor.memory", config.spark.executor_memory)
        .config("spark.hadoop.fs.s3a.endpoint", config.minio.endpoint_url)
        .config("spark.hadoop.fs.s3a.access.key", config.minio.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", config.minio.secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(config.minio.secure).lower())
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("WARN")
    return session
