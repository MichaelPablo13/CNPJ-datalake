#!/bin/sh
set -e

mc alias set cnpj http://minio:9000 "${MINIO_ROOT_USER:-minio_root}" "${MINIO_ROOT_PASSWORD:-minio_root_change_me}"

mc mb --ignore-existing "cnpj/${MINIO_BUCKET_BRONZE:-cnpj-bronze}"
mc mb --ignore-existing "cnpj/${MINIO_BUCKET_SILVER:-cnpj-silver}"
mc mb --ignore-existing "cnpj/${MINIO_BUCKET_GOLD:-cnpj-gold}"

# User creation can fail if it already exists; keep init idempotent.
mc admin user add cnpj "${MINIO_APP_ACCESS_KEY:-cnpj_app_user}" "${MINIO_APP_SECRET_KEY:-cnpj_app_change_me}" || true
mc admin policy attach cnpj readwrite --user "${MINIO_APP_ACCESS_KEY:-cnpj_app_user}"
