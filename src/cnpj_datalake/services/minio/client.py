"""MinIO client for basic bucket management."""

from minio import Minio

from src.cnpj_datalake.config import MinioConfig


class MinioStorage:
    def __init__(self, config: MinioConfig):
        self._config = config
        self.client = Minio(
            endpoint=config.endpoint,
            access_key=config.access_key,
            secret_key=config.secret_key,
            secure=config.secure,
        )

    def ensure_buckets(self) -> None:
        for bucket in (
            self._config.bucket_bronze,
            self._config.bucket_silver,
            self._config.bucket_gold,
        ):
            if not self.client.bucket_exists(bucket):
                self.client.make_bucket(bucket)
