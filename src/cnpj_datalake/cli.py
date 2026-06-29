"""CLI empacotável do CNPJ Data Lake."""

from __future__ import annotations

import argparse
from pathlib import Path

from src.cnpj_datalake.services.pyspark.orchestration import run_pipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Executa pipeline CNPJ Data Lake")
    parser.add_argument("--source-file", required=True, help="Arquivo de origem")
    parser.add_argument("--file-type", required=True, help="Tipo de arquivo (ex: estabelecimentos)")
    parser.add_argument(
        "--primary-keys",
        default="",
        help="Colunas separadas por virgula para deduplicacao",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    keys = [item.strip() for item in args.primary_keys.split(",") if item.strip()]
    result = run_pipeline(
        source_file=Path(args.source_file),
        file_type=args.file_type,
        primary_keys=keys or None,
    )
    print(result)


if __name__ == "__main__":
    main()