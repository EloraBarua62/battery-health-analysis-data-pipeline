from fastapi import FastAPI, HTTPException
from typing import Any, Dict, List, Optional, Tuple
import os
from pathlib import Path
import math

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None  # type: ignore

app = FastAPI(title="Battery Pipeline API", version="2.0.0")


def get_spark():
    """
    Return an existing Spark session when running in Databricks / PySpark.
    Returns None in plain local environments.
    """
    try:
        return spark  # type: ignore[name-defined]
    except Exception:
        try:
            from pyspark.sql import SparkSession
            return SparkSession.builder.getOrCreate()
        except Exception:
            return None


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _existing_paths(candidates: List[str]) -> List[str]:
    out = []
    for p in candidates:
        try:
            if p and Path(p).exists():
                out.append(str(Path(p).resolve()))
        except Exception:
            continue
    return out


def _gold_csv_candidates() -> List[str]:
    root = project_root()
    env_path = os.getenv("GOLD_CSV_PATH", "")
    candidates = [
        env_path,
        str(root / "data" / "gold_layer_notebook.csv"),
        str(root / "examples" / "gold_layer_notebook.csv"),
        str(root / "gold_layer_notebook.csv"),
        "/mnt/data/gold_layer_notebook.csv",
    ]
    return [p for p in candidates if p]


def _prediction_csv_candidates() -> List[str]:
    root = project_root()
    env_path = os.getenv("PREDICTION_CSV_PATH", "")
    candidates = [
        env_path,
        str(root / "data" / "battery_health_prediction.csv"),
        str(root / "examples" / "battery_health_prediction.csv"),
        str(root / "battery_health_prediction.csv"),
        "/mnt/data/battery_health_prediction.csv",
    ]
    return [p for p in candidates if p]


def _gold_table_candidates() -> List[str]:
    env_name = os.getenv("GOLD_TABLE", "")
    return [x for x in [
        env_name,
        "gold_battery_health_summary",
        "gold_daily_battery_kpis",
        "default.gold_battery_health_summary",
        "default.gold_daily_battery_kpis",
    ] if x]


def _prediction_table_candidates() -> List[str]:
    env_name = os.getenv("PREDICTION_TABLE", "")
    return [x for x in [
        env_name,
        "ml_batterydata_predictions",
        "default.ml_batterydata_predictions",
    ] if x]


def _gold_delta_candidates() -> List[str]:
    env_path = os.getenv("GOLD_DELTA_PATH", "")
    return [x for x in [
        env_path,
        "abfss://battery-data@batteryhealthdatalake.dfs.core.windows.net/gold/daily_battery_kpis",
        "abfss://battery-data@batteryhealthdatalake.dfs.core.windows.net/gold/battery_health_summary",
    ] if x]


def _prediction_delta_candidates() -> List[str]:
    env_path = os.getenv("PREDICTION_DELTA_PATH", "")
    return [x for x in [
        env_path,
        "abfss://battery-data@batteryhealthdatalake.dfs.core.windows.net/gold/ml_results",
    ] if x]


def _normalize_record_values(record: Dict[str, Any]) -> Dict[str, Any]:
    cleaned: Dict[str, Any] = {}
    for key, value in record.items():
        if value is None:
            cleaned[key] = None
        elif pd is not None and hasattr(pd, "isna") and pd.isna(value):
            cleaned[key] = None
        elif hasattr(value, "isoformat"):
            cleaned[key] = value.isoformat()
        elif isinstance(value, float) and math.isnan(value):
            cleaned[key] = None
        else:
            cleaned[key] = value
    return cleaned


def _pdf_to_records(pdf) -> List[Dict[str, Any]]:
    if pd is None:
        return []
    pdf = pdf.copy()
    for col in pdf.columns:
        try:
            if "date" in col.lower() or "time" in col.lower():
                converted = pd.to_datetime(pdf[col], errors="ignore", utc=True)
                if str(getattr(converted, "dtype", "")) != str(getattr(pdf[col], "dtype", "")):
                    pdf[col] = converted.astype("string")
        except Exception:
            continue
    raw_records = pdf.to_dict(orient="records")
    return [_normalize_record_values(r) for r in raw_records]


def _find_date_column(columns: List[str]) -> Optional[str]:
    preferred = ["report_data", "date", "timestamp", "event_date"]
    for col in preferred:
        if col in columns:
            return col
    for col in columns:
        low = col.lower()
        if "date" in low or "time" in low:
            return col
    return None


def try_read_gold_pandas() -> Tuple[Optional["pd.DataFrame"], Optional[str], Dict[str, Any]]:
    diag: Dict[str, Any] = {"mode": None, "source": None, "attempted": {}}
    spark_session = get_spark()

    if spark_session is not None:
        attempted_tables = []
        for table_name in _gold_table_candidates():
            attempted_tables.append(table_name)
            try:
                df = spark_session.read.table(table_name)
                pdf = df.toPandas()
                diag.update({"mode": "spark_table", "source": table_name, "attempted": {"tables": attempted_tables}})
                return pdf, None, diag
            except Exception:
                continue

        attempted_paths = []
        for delta_path in _gold_delta_candidates():
            attempted_paths.append(delta_path)
            try:
                df = spark_session.read.format("delta").load(delta_path)
                pdf = df.toPandas()
                diag.update({"mode": "spark_delta", "source": delta_path, "attempted": {"delta_paths": attempted_paths}})
                return pdf, None, diag
            except Exception:
                continue

        diag["attempted"] = {"tables": attempted_tables, "delta_paths": attempted_paths}

    if pd is None:
        return None, "pandas is not installed, so CSV fallback is unavailable.", diag

    existing = _existing_paths(_gold_csv_candidates())
    diag["attempted"]["csv_paths"] = _gold_csv_candidates()
    for csv_path in existing:
        try:
            pdf = pd.read_csv(csv_path)
            diag.update({"mode": "csv", "source": csv_path})
            return pdf, None, diag
        except Exception:
            continue

    return None, "Gold dataset not found in Spark tables, Delta paths, or CSV fallback.", diag


def try_read_prediction_pandas() -> Tuple[Optional["pd.DataFrame"], Optional[str], Dict[str, Any]]:
    diag: Dict[str, Any] = {"mode": None, "source": None, "attempted": {}}
    spark_session = get_spark()

    if spark_session is not None:
        attempted_tables = []
        for table_name in _prediction_table_candidates():
            attempted_tables.append(table_name)
            try:
                df = spark_session.read.table(table_name)
                pdf = df.toPandas()
                diag.update({"mode": "spark_table", "source": table_name, "attempted": {"tables": attempted_tables}})
                return pdf, None, diag
            except Exception:
                continue

        attempted_paths = []
        for delta_path in _prediction_delta_candidates():
            attempted_paths.append(delta_path)
            try:
                df = spark_session.read.format("delta").load(delta_path)
                pdf = df.toPandas()
                diag.update({"mode": "spark_delta", "source": delta_path, "attempted": {"delta_paths": attempted_paths}})
                return pdf, None, diag
            except Exception:
                continue

        diag["attempted"] = {"tables": attempted_tables, "delta_paths": attempted_paths}

    if pd is None:
        return None, "pandas is not installed, so CSV fallback is unavailable.", diag

    existing = _existing_paths(_prediction_csv_candidates())
    diag["attempted"]["csv_paths"] = _prediction_csv_candidates()
    for csv_path in existing:
        try:
            pdf = pd.read_csv(csv_path)
            diag.update({"mode": "csv", "source": csv_path})
            return pdf, None, diag
        except Exception:
            continue

    return None, "Prediction dataset not found in Spark tables, Delta paths, or CSV fallback.", diag


@app.get("/")
def root() -> Dict[str, Any]:
    return {
        "message": "Battery Pipeline API is running",
        "version": "2.0.0",
        "endpoints": [
            "/health",
            "/kpis/latest",
            "/kpis/date/{date_value}",
            "/kpis/summary",
            "/predictions/sample",
            "/predictions/date/{date_value}",
        ],
    }


@app.get("/health")
def health() -> Dict[str, Any]:
    gold_df, gold_err, gold_diag = try_read_gold_pandas()
    pred_df, pred_err, pred_diag = try_read_prediction_pandas()
    return {
        "status": "ok" if (gold_df is not None or pred_df is not None) else "error",
        "gold_available": gold_df is not None,
        "prediction_available": pred_df is not None,
        "gold_rows": int(len(gold_df)) if gold_df is not None else 0,
        "prediction_rows": int(len(pred_df)) if pred_df is not None else 0,
        "gold_source": gold_diag.get("source"),
        "prediction_source": pred_diag.get("source"),
        "gold_error": gold_err,
        "prediction_error": pred_err,
    }


@app.get("/kpis/latest")
def latest_kpis(limit: int = 20) -> List[Dict[str, Any]]:
    if limit < 1:
        raise HTTPException(status_code=400, detail="limit must be >= 1")

    pdf, err, diag = try_read_gold_pandas()
    if pdf is None:
        raise HTTPException(status_code=500, detail={"message": err, "diagnostics": diag})

    date_col = _find_date_column(list(pdf.columns))
    if date_col:
        try:
            pdf = pdf.assign(_sort_date=pd.to_datetime(pdf[date_col], errors="coerce", utc=True)).sort_values("_sort_date", ascending=False).drop(columns=["_sort_date"])
        except Exception:
            pass

    return _pdf_to_records(pdf.head(limit))


@app.get("/kpis/date/{date_value}")
def kpis_by_date(date_value: str) -> List[Dict[str, Any]]:
    pdf, err, diag = try_read_gold_pandas()
    if pdf is None:
        raise HTTPException(status_code=500, detail={"message": err, "diagnostics": diag})

    date_col = _find_date_column(list(pdf.columns))
    if not date_col:
        raise HTTPException(status_code=400, detail="Gold dataset does not contain a date/timestamp column.")

    s = pdf[date_col].astype("string")
    matched = pdf[(s == date_value) | (s.str[:10] == date_value)]
    return _pdf_to_records(matched)


@app.get("/kpis/summary")
def kpis_summary() -> Dict[str, Any]:
    pdf, err, diag = try_read_gold_pandas()
    if pdf is None:
        raise HTTPException(status_code=500, detail={"message": err, "diagnostics": diag})

    summary: Dict[str, Any] = {
        "row_count": int(len(pdf)),
        "columns": list(pdf.columns),
        "source": diag.get("source"),
    }

    if "avg_voltage" in pdf.columns:
        summary["avg_voltage_mean"] = float(pdf["avg_voltage"].mean())
    if "peak_temp" in pdf.columns:
        summary["peak_temp_max"] = float(pdf["peak_temp"].max())
    if "total_voltage_sags" in pdf.columns:
        summary["total_voltage_sags_sum"] = int(pdf["total_voltage_sags"].sum())
    if "thermal_stress_events" in pdf.columns:
        summary["thermal_stress_events_sum"] = int(pdf["thermal_stress_events"].sum())

    return summary


@app.get("/predictions/sample")
def predictions_sample(limit: int = 20) -> List[Dict[str, Any]]:
    if limit < 1:
        raise HTTPException(status_code=400, detail="limit must be >= 1")

    pdf, err, diag = try_read_prediction_pandas()
    if pdf is None:
        raise HTTPException(status_code=500, detail={"message": err, "diagnostics": diag})

    date_col = _find_date_column(list(pdf.columns))
    if date_col:
        try:
            pdf = pdf.assign(_sort_date=pd.to_datetime(pdf[date_col], errors="coerce", utc=True)).sort_values("_sort_date", ascending=False).drop(columns=["_sort_date"])
        except Exception:
            pass

    return _pdf_to_records(pdf.head(limit))


@app.get("/predictions/date/{date_value}")
def predictions_by_date(date_value: str) -> List[Dict[str, Any]]:
    pdf, err, diag = try_read_prediction_pandas()
    if pdf is None:
        raise HTTPException(status_code=500, detail={"message": err, "diagnostics": diag})

    date_col = _find_date_column(list(pdf.columns))
    if not date_col:
        raise HTTPException(status_code=400, detail="Prediction dataset does not contain a date/timestamp column.")

    s = pdf[date_col].astype("string")
    matched = pdf[(s == date_value) | (s.str[:10] == date_value)]
    return _pdf_to_records(matched)
