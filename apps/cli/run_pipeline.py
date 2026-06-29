"""Legacy wrapper for CLI moved to services/pyspark/cli."""

from services.pyspark.cli.run_pipeline import main


if __name__ == "__main__":
    raise SystemExit(main())
