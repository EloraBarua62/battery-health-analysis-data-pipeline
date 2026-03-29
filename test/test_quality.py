from pyspark.sql import functions as F

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

def test_silver_quality_basic():
    spark_session = _get_spark()
    df = _read_first_available(
        spark_session,
        ["silver_battery_health"],
        ["abfss://battery-data@batteryhealthdatalake.dfs.core.windows.net/silver/battery_health_table"]
    )
    assert df.filter(F.col("timestamp").isNull()).count() == 0

    voltage_col = "voltage" if "voltage" in df.columns else "battery_voltage"
    if voltage_col in df.columns:
        bad_voltage = df.filter((F.col(voltage_col) < 0) | (F.col(voltage_col) > 1000)).count()
        assert bad_voltage == 0

    if "temperature" in df.columns:
        bad_temp = df.filter((F.col("temperature") < -100) | (F.col("temperature") > 200)).count()
        assert bad_temp == 0

def test_gold_quality_basic():
    spark_session = _get_spark()
    df = _read_first_available(
        spark_session,
        ["gold_battery_health_summary", "gold_daily_battery_kpis"],
        ["abfss://battery-data@batteryhealthdatalake.dfs.core.windows.net/gold/daily_battery_kpis"]
    )
    target_col = next(c for c in ["date", "timestamp", "report_data"] if c in df.columns)
    assert df.filter(F.col(target_col).isNull()).count() == 0
    assert df.count() > 0
