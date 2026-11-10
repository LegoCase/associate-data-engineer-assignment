# Solution

The correct report for `data/orders.csv` is:

| order_date | revenue | order_count |
|---|---:|---:|
| 2026-10-01 | 205.50 | 2 |
| 2026-10-02 | 40.00 | 1 |

There is no row for 2026-10-03 because its only order (1009) contains conflicting details and is excluded.

Before the fix (with only safe date parsing added so the job could run):

| order_date | revenue | order_count | Problem |
|---|---:|---:|---|
| NULL | 99.00 | 1 | 1005: invalid date and `pending` status, but still counted |
| 2026-10-01 | 205.50 | 2 | Correct by chance |
| 2026-10-02 | 220.00 | 5 | Negative amount, missing customer, cancelled order and NULL amount were counted |
| 2026-10-03 | 420.00 | 2 | Both conflicting versions of order 1009 were counted |

## Root Causes and Fixes

| Root cause | Fix |
|---|---|
| The job crashed on `not-a-date`, so `cast` and `to_date` raise errors on invalid values instead of returning NULL. | Use `try_cast` together with a regex format check. |
| No filter for `status == "completed"`. | Only completed orders are included. |
| No validation of IDs, dates or amounts. | Each row gets a `rejection_reason`. Only rows without a rejection reason are included in the report. |
| Amounts stored as `double`, with precision issues hidden by `round(sum)`. | Use `decimal(18,2)` throughout the pipeline without additional rounding. |
| `dropDuplicates()` only removes identical rows, so both versions of order 1009 were counted. | Detect conflicting details per `order_id` and exclude all versions of conflicting orders. |
| `count("order_id")` counts rows rather than unique orders. | Use `countDistinct("order_id")` after ensuring one valid row per order. |
| Text values were not normalised, e.g. `" Completed "` and `"  "`. | Trim text fields and convert `status` to lowercase. |

## Design

```text
orders.csv ─► clean_and_flag_orders ─► output/clean_orders
                                          │
                                          │ (typed, raw_amount, raw_order_date,
                                          │  is_conflict, rejection_reason)
                                          ▼
                                   prepare_orders
                                          │
                                          ▼
                                    daily_revenue
                                          │
                                          ▼
                              output/daily_revenue
```

The pipeline is split into the following steps:

- `clean_orders`: Trims text fields, converts status to lowercase and parses `amount` and `order_date`. Invalid values become NULL.
- `flag_conflicts`: Uses a window over `order_id` to identify orders with more than one distinct version of their details.
- `add_rejection_reason`: Assigns the first validation rule that a row fails, or NULL if the row is valid.
- `prepare_orders`: Keeps rows without a rejection reason and ensures one row per `order_id`.
- `daily_revenue`: Calculates revenue and distinct order count for each day.

The clean layer keeps every identifiable row together with its rejection reason and original `amount` and `order_date` values. This makes it possible to investigate excluded records, including conflicting orders where `rejection_reason = 'conflicting_details'`.

The pipeline also prints a count of rejected rows grouped by rejection reason.

## Decisions and Assumptions

- **Conflict definition:** Two or more rows with the same `order_id` but different `customer_id`, `order_date`, `amount` or `status` after cleaning. Exact duplicates are not considered conflicts. For example, `85.0` and `85.00` are treated as equal.
- **No automatic conflict resolution:** There is no timestamp or version column to determine which record is correct. Therefore, all versions of a conflicting order are excluded and kept in the clean layer with a rejection reason.
- **Conflict detection before status filtering:** If an order has both a `completed` and a `cancelled` version, it is treated as a conflict rather than including the completed version.
- **Amounts:** Only plain numeric values with up to two decimal places are accepted. Values such as `10.005`, `1e3` and `$10` are invalid rather than rounded. Zero and negative amounts are also invalid and are not treated as refunds.
- **Dates:** Only the `yyyy-MM-dd` format is accepted.
- **Cancelled and pending orders:** These receive the `not_completed` rejection reason. They are considered normal business states rather than data quality issues.
- **Input and output:** One input file per run, one currency and overwrite on every run. Days without eligible orders are not included as rows with zero revenue.

## Tests

`tests/test_transforms.py` uses a single mock dataset (`RAW_ORDERS`) covering the main business rules and edge cases.

It includes whitespace and case normalisation, whole-number amounts, exact duplicates, equivalent amounts with different formatting, missing and blank IDs, zero and negative amounts, invalid and impossible dates, invalid amount formats and two types of conflicting orders.

The tests verify which orders are kept, the final report values (including decimal precision, daily grouping and distinct order counts), and which records are flagged as conflicts.

## CI

`.github/workflows/ci.yml` runs on every push and pull request.

The workflow includes the following steps:

- `uv sync --locked`
- `ruff check`
- `ruff format --check`
- `pytest`

Each step runs separately, and any failure causes the workflow to fail. The GitHub Actions token uses read-only permissions.

## With More Time

There are a few things I would consider improving further:

- Read the CSV using an explicit schema and validate the header against it.
- Store the different layers as Delta tables, using append or `MERGE` per run and detecting conflicts across multiple runs rather than only within one input file.
- Report rows without an `order_id` instead of simply excluding them.
- Monitor the percentage of rejected rows and trigger an alert if it exceeds a defined threshold.
- Add an end-to-end test using `data/orders.csv` and ensure every rejection reason has dedicated test coverage.
- Extend CI/CD with separate development, QA and production environments, for example using Databricks Asset Bundles, with an approval step before production deployment.