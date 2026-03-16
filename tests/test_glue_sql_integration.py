"""
Validate Glue SQL transform against local processor output.
Runs only if PySpark is available (e.g. in CI or local with pyspark installed).
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Expected output from local processor on data.sql (canonical)
EXPECTED_ROWS = [
    ("google.com", "ipod classic", 410.0),
    ("bing.com", "zune", 250.0),
    ("search.yahoo.com", "cd player", 180.0),
]


def _has_pyspark():
    try:
        import pyspark  # noqa: F401
        return True
    except ImportError:
        return False


@pytest.mark.skipif(not _has_pyspark(), reason="PySpark not installed")
def test_glue_sql_matches_local_processor():
    """Run Glue SQL on data.sql and assert result equals local processor output."""
    from pyspark.sql import SparkSession
    from glue.scripts.search_keyword_performance import SearchKeywordGlueJob

    data_path = Path(__file__).resolve().parent.parent / "data.sql"
    if not data_path.exists():
        pytest.skip("data.sql not found")

    spark = SparkSession.builder.master("local[1]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark.read.option("header", "true")
        .option("delimiter", "\t")
        .option("quote", '"')
        .csv(str(data_path))
    )
    job = SearchKeywordGlueJob(spark, str(data_path), "s3://dummy/output/")
    result = job.transform_hits(df)

    rows = result.collect()
    assert len(rows) == len(EXPECTED_ROWS), f"Expected {len(EXPECTED_ROWS)} rows, got {len(rows)}"

    # Result has columns "Search Engine Domain", "Search Keyword", "Revenue"
    for i, expected in enumerate(EXPECTED_ROWS):
        exp_domain, exp_keyword, exp_revenue = expected
        row = rows[i]
        assert row["Search Engine Domain"] == exp_domain, f"Row {i} domain"
        assert row["Search Keyword"] == exp_keyword, f"Row {i} keyword"
        assert abs(float(row["Revenue"]) - exp_revenue) < 0.01, f"Row {i} revenue"

    spark.stop()
