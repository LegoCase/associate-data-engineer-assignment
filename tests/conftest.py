import os
import sys

import pytest
from pyspark.sql import SparkSession

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    session = SparkSession.builder.master("local[2]").appName("order-assignment-tests").getOrCreate()
    yield session
    session.stop()
