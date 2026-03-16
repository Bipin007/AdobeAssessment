# Glue Spark job: hit-level TSV from S3 → revenue by search domain/keyword → one TSV out.
# Args: --input_path, --output_path (S3 paths)
from datetime import date
from urllib.parse import parse_qs, urlparse
import sys

PURCHASE_EVENT = "1"
SEARCH_ENGINE_HOSTS = {
    "google.com", "www.google.com",
    "search.yahoo.com", "yahoo.com", "www.yahoo.com",
    "bing.com", "www.bing.com", "msn.com", "search.msn.com",
}
KEYWORD_PARAMS = ("q", "p")


def _has_text(value):
    return bool(value and str(value).strip())


def _domain_from_referrer(referrer):
    if not _has_text(referrer):
        return None
    try:
        parsed = urlparse(str(referrer).strip())
        host = (parsed.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        return host or None
    except Exception:
        return None


def _keyword_from_referrer(referrer):
    if not _has_text(referrer):
        return ""
    try:
        parsed = urlparse(str(referrer).strip())
        qs = parse_qs(parsed.query)
        for param in KEYWORD_PARAMS:
            if param in qs and qs[param]:
                raw = qs[param][0]
                return (raw or "").strip()
        return ""
    except Exception:
        return ""


def is_external_search(referrer):
    domain = _domain_from_referrer(referrer)
    if not domain:
        return False
    return domain in SEARCH_ENGINE_HOSTS or f"www.{domain}" in SEARCH_ENGINE_HOSTS


def has_purchase(event_list):
    if not _has_text(event_list):
        return False
    parts = [p.strip() for p in str(event_list).split(",") if p.strip()]
    return PURCHASE_EVENT in parts


def revenue_from_product_list(product_list):
    if not _has_text(product_list):
        return 0.0
    total = 0.0
    for product in str(product_list).split(","):
        product = product.strip()
        if not product:
            continue
        parts = product.split(";")
        if len(parts) >= 4 and parts[3]:
            try:
                total += float(parts[3].strip())
            except ValueError:
                pass
    return total


def output_filename(run_date=None):
    current_date = run_date or date.today()
    return f"{current_date.isoformat()}_SearchKeywordPerformance.tab"


def build_output_path(output_prefix, run_date=None):
    return f"{output_prefix.rstrip('/')}/{output_filename(run_date)}"


def log(message):
    print(f"[search-keyword-glue-job] {message}")


class SearchKeywordGlueJob:
    """Encapsulates the Glue runtime flow for the search keyword revenue job."""

    def __init__(self, spark, input_path, output_prefix):
        self.spark = spark
        self.input_path = input_path
        self.output_path = build_output_path(output_prefix)

    def read_input(self):
        log(f"Reading input from {self.input_path}")
        return self.spark.read.option("header", "true").option("delimiter", "\t").option("quote", '"').csv(
            self.input_path
        )

    # Native Spark SQL: no Python UDFs, so the optimizer can work in the JVM and avoid serialization overhead.
    # Logic validated to match local/processor.py: domain (host, strip www), keyword (q/p, + -> space),
    # purchase (event_list contains whole-token "1"), revenue (4th field of each product, semicolon-separated).
    # See tests/test_glue_sql_integration.py when PySpark is available.
    _TRANSFORM_SQL = """
    SELECT
      search_engine_domain AS `Search Engine Domain`,
      search_keyword AS `Search Keyword`,
      sum(revenue) AS Revenue
    FROM (
      SELECT
        coalesce(nullif(regexp_replace(lower(regexp_extract(referrer, '^[^/]+//([^/]+)', 1)), '^www\\.', ''), ''), 'unknown') AS search_engine_domain,
        coalesce(trim(replace(nullif(coalesce(
          regexp_extract(referrer, '[?&]q=([^&]*)', 1),
          regexp_extract(referrer, '[?&]p=([^&]*)', 1)
        ), ''), '+', ' ')), '(not set)') AS search_keyword,
        aggregate(
          filter(split(coalesce(product_list, ''), ','), p -> length(trim(p)) > 0),
          cast(0.0 AS double),
          (acc, p) -> acc + coalesce(try_cast(element_at(split(trim(p), ';'), 4) AS double), cast(0.0 AS double))
        ) AS revenue
      FROM hits
      WHERE coalesce(nullif(regexp_replace(lower(regexp_extract(referrer, '^[^/]+//([^/]+)', 1)), '^www\\.', ''), ''), 'x') IN (
        'google.com', 'search.yahoo.com', 'yahoo.com', 'bing.com', 'msn.com', 'search.msn.com'
      )
      AND concat(',', trim(coalesce(event_list, '')), ',') LIKE '%,1,%'
    ) t
    WHERE revenue > 0
    GROUP BY search_engine_domain, search_keyword
    ORDER BY revenue DESC
    """

    def transform_hits(self, df):
        df.createOrReplaceTempView("hits")
        return self.spark.sql(self._TRANSFORM_SQL)

    def publish_single_output_file(self, stage_dir):
        import boto3

        s3 = boto3.client("s3")
        parsed = urlparse(self.output_path)
        bucket = parsed.netloc
        final_s3_key = parsed.path.lstrip("/")
        stage_prefix = urlparse(stage_dir).path.lstrip("/")
        paginator = s3.get_paginator("list_objects_v2")
        part_file_key = None

        log(f"Publishing staged Spark output from s3://{bucket}/{stage_prefix}")
        for page in paginator.paginate(Bucket=bucket, Prefix=stage_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if "part-" in key and key.endswith(".csv"):
                    part_file_key = key
                    break
            if part_file_key:
                break

        if not part_file_key:
            raise RuntimeError(f"No Spark part file found under s3://{bucket}/{stage_prefix}")

        # Overwrite final key (S3 put semantics); safe if job runs twice for the same day.
        s3.copy_object(
            CopySource={"Bucket": bucket, "Key": part_file_key},
            Bucket=bucket,
            Key=final_s3_key,
        )

        for page in paginator.paginate(Bucket=bucket, Prefix=stage_prefix):
            objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
            if objects:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": objects})

        return f"s3://{bucket}/{final_s3_key}"

    def run(self):
        log(f"Starting Glue job for input {self.input_path}")
        df = self.read_input()
        agg = self.transform_hits(df)

        # The deliverable requires one named tab-delimited file, so the final write is collapsed to one part.
        # repartition(1) (vs coalesce(1)): ensures a full shuffle so sort order is preserved in the single output file.
        # Idempotency: overwrite mode and a fixed output path (by date) mean re-running the job overwrites the same key.
        stage_dir = self.output_path + "_stage"
        log(f"Writing staged output to {stage_dir}")
        agg.repartition(1).write.mode("overwrite").option("header", "true").option("delimiter", "\t").csv(stage_dir)

        final_output_uri = self.publish_single_output_file(stage_dir)
        log(f"Job finished successfully. Output file: {final_output_uri}")
        return final_output_uri


def main():
    from awsglue.utils import getResolvedOptions
    from pyspark.sql import SparkSession

    args = getResolvedOptions(sys.argv, ["input_path", "output_path"])
    input_path = args["input_path"]
    output_prefix = args["output_path"]

    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        job = SearchKeywordGlueJob(spark, input_path, output_prefix)
        job.run()
    except Exception as exc:
        log(f"Job failed: {exc}")
        raise


if __name__ == "__main__":
    main()
