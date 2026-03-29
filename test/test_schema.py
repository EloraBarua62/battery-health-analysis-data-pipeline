import pytest

def _get_spark():
    try:
        return spark  
    except Exception:
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()

def _read_first_available(spark_session, table_candidates, path_candidates):
    for table_name in table_candidates:
        try:
            return spark_session.read.table(table_name)
        except Exception:
            pass
    for path in path_candidates:
        try:
            return spark_session.read.format("delta").load(path)
        except Exception:
            pass
    raise AssertionError("Could not read any candidate table/path.")

def test_silver_schema():
    spark_session = _get_spark()
    df = _read_first_available(
        spark_session,
        ["silver_battery_health"],
        ["abfss://battery-data@batteryhealthdatalake.dfs.core.windows.net/silver/battery_health_table"]
    )
    cols = set(df.columns)
    assert "timestamp" in cols
    assert ("voltage" in cols) or ("battery_voltage" in cols)
    assert "temperature" in cols

def test_gold_schema():
    spark_session = _get_spark()
    df = _read_first_available(
        spark_session,
        ["gold_battery_health_summary", "gold_daily_battery_kpis"],
        ["abfss://battery-data@batteryhealthdatalake.dfs.core.windows.net/gold/daily_battery_kpis"]
    )
    cols = set(df.columns)
    assert "date" in cols or "timestamp" in cols or "report_data" in cols
    assert df.count() > 0

def test_predictions_available():
    spark_session = _get_spark()
    df = _read_first_available(
        spark_session,
        ["default.ml_batterydata_predictions", "ml_batterydata_predictions"],
        ["abfss://battery-data@batteryhealthdatalake.dfs.core.windows.net/gold/ml_results"]
    )
    assert df.count() > 0
