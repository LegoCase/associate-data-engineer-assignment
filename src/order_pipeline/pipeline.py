"""Run with: python -m order_pipeline.pipeline"""

from pathlib import Path

from pyspark.sql import SparkSession

from order_pipeline.transforms import clean_and_flag_orders, daily_revenue, prepare_orders


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    spark = SparkSession.builder.master("local[2]").appName("associate-order-assignment").getOrCreate()
    try:
        raw = spark.read.option("header", True).csv(str(root / "data" / "orders.csv"))
        clean = clean_and_flag_orders(raw).cache()
        clean_output = root / "output" / "clean_orders"
        clean.write.mode("overwrite").parquet(str(clean_output))
        print("Cleaned orders (first 5 rows):")
        clean.show(5, truncate=False)

        excluded = clean.where("rejection_reason IS NOT NULL")
        print("Rows left out of revenue, by reason:")
        excluded.groupBy("rejection_reason").count().orderBy("rejection_reason").show(truncate=False)

        result = daily_revenue(prepare_orders(clean))
        result.show(truncate=False)
        output = root / "output" / "daily_revenue"
        result.write.mode("overwrite").parquet(str(output))
        print(f"Saved report to {output}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
