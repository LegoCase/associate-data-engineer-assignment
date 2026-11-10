import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("order-assignment-tests")
        .getOrCreate()
    )
    yield session
    session.stop()
