from common_code.utility import dedupe_latest, normalize_id


def test_normalize_id():
    assert normalize_id("  ab12 ") == "AB12"
    assert normalize_id(None) == ""


def test_dedupe_latest(spark):
    df = spark.createDataFrame(
        [("C1", "2024-01-01"), ("C1", "2024-02-01"), ("C2", "2024-01-15")],
        ["claim_id", "load_date"],
    )
    result = dedupe_latest(df, "claim_id", "load_date")
    rows = {r.claim_id: r.load_date for r in result.collect()}
    assert result.count() == 3
    assert rows["C1"] == "2024-02-01"
