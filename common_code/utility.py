from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def dedupe_latest(df: DataFrame, key_col: str, order_col: str) -> DataFrame:
    """Keep the latest row per key, based on order_col."""
    w = Window.partitionBy(key_col).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def normalize_id(value):
    """Trim and uppercase an ID. Returns '' for None."""
    return (value or "").strip().upper()
