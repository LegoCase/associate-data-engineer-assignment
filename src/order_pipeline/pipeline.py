"""Run with: python -m order_pipeline.pipeline"""
from pathlib import Path
from pyspark.sql import SparkSession
from order_pipeline.transforms import prepare_orders, daily_revenue


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("associate-order-assignment")
        .getOrCreate()
    )
    try:
        raw = spark.read.option("header", True).csv(str(root / "data" / "orders.csv"))
        result = daily_revenue(prepare_orders(raw))
        result.show(truncate=False)
        output = root / "output" / "daily_revenue"
        result.write.mode("overwrite").parquet(str(output))
        print(f"Saved report to {output}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
