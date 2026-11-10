# Associate Data Engineer — PySpark Pipeline Challenge

**Suggested timebox:** 2–3 hours. **No Databricks workspace required.**

## Scenario
The business uses a daily order revenue report. The current pipeline executes, but stakeholders have raised concerns about its accuracy. Investigate the data and implementation, fix issues, and improve confidence in the report.

## Business rules
- Count only `completed` orders in revenue.
- An eligible order must have a nonempty `order_id`, `customer_id`, valid `order_date`, and strictly positive numeric `amount`.
- Each `order_id` should be counted at most once.
- If the same `order_id` occurs with **conflicting details**, do not silently choose a winner. Document and implement a reasonable handling strategy (for example, exclude conflicting records and report them).
- The output should include `order_date`, `revenue`, and `order_count`.
- Monetary precision matters; avoid floating-point rounding surprises.

## Tasks
1. Run and inspect the existing pipeline.
2. Fix incorrect transformations and explain the root causes.
3. Improve readability, maintainability, and validation of the Python/PySpark code.
4. Add meaningful pytest tests covering normal and edge cases.
5. Extend the GitHub Actions CI workflow to run the tests, a configured Python linter,
   and a formatting check on pushes and pull requests. Add the required development
   dependencies and project configuration. The workflow must fail if any check fails.
6. Add a short `SOLUTION.md` describing fixes, assumptions, and anything you would improve with more time.

## Local setup
Install [uv](https://docs.astral.sh/uv/getting-started/installation/) and
[Java 21](https://adoptium.net/temurin/releases/?version=21) first. The `uv python
install` command below downloads and installs a managed Python 3.14 if it is not
already available; Python does not need to be installed separately. Internet access is
needed to download Python and the project dependencies. Verify Java is available with
`java -version`; set `JAVA_HOME` to your Java 21 installation if PySpark cannot find
it.

```bash
uv python install 3.14
uv sync --locked
uv run python -m order_pipeline.pipeline
uv run pytest -q
```

Output is written to `output/daily_revenue` as Parquet. Remove it and rerun if needed. This assignment deliberately uses Parquet rather than Delta Lake so no extra local Spark extensions are required.

## Submission
Share the completed repository, including code, tests, and `SOLUTION.md`. Do not spend more than 3 hours; note any unfinished work.

## Evaluation
We value correctness, reasoning, clear Python/PySpark, meaningful tests, and the ability to explain trade-offs. You are not expected to build a production platform.
