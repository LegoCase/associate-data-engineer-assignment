from datetime import date
from decimal import Decimal

from order_pipeline.transforms import clean_orders, daily_revenue, flag_conflicts, prepare_orders

COLUMNS = ["order_id", "customer_id", "order_date", "amount", "status"]

RAW_ORDERS = [
    # --- valid orders: must be kept ---
    ("1", "C1", "2026-10-01", "10.00", "completed"),                 # plain valid order
    (" 2 ", " C2 ", " 2026-10-01 ", " 20.50 ", " Completed "),       # valid after trim/lowercase
    ("3", "C3", "2026-10-01", "30", "completed"),                    # whole-number amount
    ("4", "C4", "2026-10-02", "40.00", "completed"),                 # exact duplicate ->
    ("4", "C4", "2026-10-02", "40.00", "completed"),                 #   counted once
    ("5", "C5", "2026-10-01", "85.0", "completed"),                  # same amount, other format ->
    ("5", "C5", "2026-10-01", "85.00", "completed"),                 #   not a conflict
    # --- business rules broken: must be excluded ---
    ("6", "C6", "2026-10-01", "10.00", "cancelled"),                 # not completed
    ("7", "C7", "2026-10-01", "10.00", "pending"),                   # not completed
    (None, "C8", "2026-10-01", "10.00", "completed"),                # missing order_id
    ("   ", "C8", "2026-10-01", "10.00", "completed"),               # blank order_id
    ("8", None, "2026-10-01", "10.00", "completed"),                 # missing customer_id
    ("9", "   ", "2026-10-01", "10.00", "completed"),                # blank customer_id
    ("10", "C10", "2026-10-01", "0", "completed"),                   # zero amount
    ("11", "C11", "2026-10-01", "-45.00", "completed"),              # negative amount
    # --- invalid formats: must be excluded ---
    ("12", "C12", "not-a-date", "10.00", "completed"),               # date is text
    ("13", "C13", None, "10.00", "completed"),                       # date missing
    ("14", "C14", "2026-10", "10.00", "completed"),                  # partial date
    ("15", "C15", "2026-02-30", "10.00", "completed"),               # impossible date
    ("16", "C16", "01/10/2026", "10.00", "completed"),               # wrong date format
    ("17", "C17", "2026-10-01", "not-a-number", "completed"),        # amount is text
    ("18", "C18", "2026-10-01", None, "completed"),                  # amount missing
    ("19", "C19", "2026-10-01", "10.005", "completed"),              # 3 decimals
    ("20", "C20", "2026-10-01", "1e3", "completed"),                 # scientific notation
    ("21", "C21", "2026-10-01", "$10", "completed"),                 # currency symbol
    # --- conflicts: all versions must be excluded ---
    ("22", "C22", "2026-10-01", "200.00", "completed"),              # different amounts
    ("22", "C22", "2026-10-01", "220.00", "completed"),
    ("23", "C23", "2026-10-01", "10.00", "completed"),               # completed + cancelled
    ("23", "C23", "2026-10-01", "10.00", "cancelled"),
]


def test_report_has_expected_columns(spark):
    raw = spark.createDataFrame(
        RAW_ORDERS,
        COLUMNS
    )
    result = daily_revenue(prepare_orders(raw))
    assert set(result.columns) == {"order_date", "revenue", "order_count"}


# Add tests that capture business rules and edge cases.
def test_prepare_orders_keeps_only_valid_orders(spark):
    raw = spark.createDataFrame(RAW_ORDERS, COLUMNS)
    result = prepare_orders(raw)
    assert sorted(row.order_id for row in result.collect()) == ["1", "2", "3", "4", "5"]

def test_daily_revenue_from_mock_orders(spark):
    raw = spark.createDataFrame(RAW_ORDERS, COLUMNS)
    result = daily_revenue(prepare_orders(raw))
    assert [tuple(row) for row in result.collect()] == [
        (date(2026, 10, 1), Decimal("145.50"), 4),  # orders 1, 2, 3, 5: 10 + 20.50 + 30 + 85
        (date(2026, 10, 2), Decimal("40.00"), 1),   # order 4, duplicate counted once
    ]

def test_prepare_orders_excludes_every_version_of_a_conflicting_order(spark):
    raw = spark.createDataFrame(RAW_ORDERS, COLUMNS)
    result_ids = {row.order_id for row in prepare_orders(raw).collect()}
    assert "22" not in result_ids  
    assert "23" not in result_ids  

def test_flag_conflicts_marks_only_orders_with_different_details(spark):
    raw = spark.createDataFrame(RAW_ORDERS, COLUMNS)
    flagged = flag_conflicts(clean_orders(raw))
    conflicting_ids = {row.order_id for row in flagged.where("is_conflict").collect()}
    assert conflicting_ids == {"22", "23"}