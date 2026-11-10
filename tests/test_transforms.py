from order_pipeline.transforms import prepare_orders, daily_revenue


def test_report_has_expected_columns(spark):
    raw = spark.createDataFrame(
        [("1", "C1", "2026-10-01", "10.00", "completed")],
        ["order_id", "customer_id", "order_date", "amount", "status"],
    )
    result = daily_revenue(prepare_orders(raw))
    assert set(result.columns) == {"order_date", "revenue", "order_count"}


# Add tests that capture business rules and edge cases.
