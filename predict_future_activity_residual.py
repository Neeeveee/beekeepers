# -*- coding: utf-8 -*-

import json
import math
import sqlite3
from datetime import datetime
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "bee_env.db"
MODEL_PATH = BASE_DIR / "models" / "residual_ridge.json"
OUTPUT_PATH = BASE_DIR / "data" / "future-activity-ml-adjusted.json"


def load_model_bundle():
    if not MODEL_PATH.exists():
        raise FileNotFoundError(f"Residual model not found: {MODEL_PATH}")
    return json.loads(MODEL_PATH.read_text(encoding="utf-8"))


def load_daily_resource_maps():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        flowering_rows = conn.execute(
            """
            SELECT model_date, AVG(flowering_index) AS daily_flowering_index
            FROM flowering_model_daily
            GROUP BY model_date
            ORDER BY model_date ASC
            """
        ).fetchall()
        nectar_rows = conn.execute(
            """
            SELECT model_date, AVG(nectar_supply_index) AS daily_nectar_supply_index
            FROM nectar_supply_model_daily
            GROUP BY model_date
            ORDER BY model_date ASC
            """
        ).fetchall()
    finally:
        conn.close()

    flowering_map = {
        row["model_date"]: float(row["daily_flowering_index"] or 0.0)
        for row in flowering_rows
    }
    nectar_map = {
        row["model_date"]: float(row["daily_nectar_supply_index"] or 0.0)
        for row in nectar_rows
    }
    return flowering_map, nectar_map


def normalize_feature(value, mean_value, std_value):
    if not std_value:
        return 0.0
    return (value - mean_value) / std_value


def build_feature_map(row, flowering_map, nectar_map):
    hour = row["hour"]
    temp = row["temperature_c"]
    humidity = row["humidity_pct"]
    wind = row["wind_speed_ms"]
    precip = row["precip_mm"]
    forecast_date = row["forecast_date"]

    daily_flowering_index = float(flowering_map.get(forecast_date, 0.0))
    daily_nectar_supply_index = float(nectar_map.get(forecast_date, 0.0))
    flower_factor = 0.5 + 0.5 * daily_flowering_index
    nectar_factor = 0.5 + 0.5 * daily_nectar_supply_index
    resource_factor = 0.5 * flower_factor + 0.5 * nectar_factor

    temp_factor = (
        0.0 if temp < 8 else
        0.10 if temp < 10 else
        0.40 if temp < 14 else
        0.75 if temp < 20 else
        1.00 if temp <= 30 else
        0.80 if temp <= 35 else
        0.50
    )
    humidity_factor = 1.00 if 40 <= humidity <= 75 else 0.90 if humidity <= 85 else 0.75
    wind_factor = (
        1.00 if wind < 1.5 else
        0.90 if wind < 3 else
        0.70 if wind < 5 else
        0.45 if wind <= 6.7 else
        0.0
    )
    rain_factor = (
        1.0 if precip == 0 else
        0.7 if precip < 1 else
        0.3 if precip < 5 else
        0.0
    )
    base_activity = 0.0 if hour < 6 or hour > 19 else math.exp(-((hour - 13.0) ** 2) / 12.0)
    weather_modifier = 0.4 * temp_factor + 0.2 * humidity_factor + 0.2 * wind_factor + 0.2 * rain_factor

    return {
        "hour": hour,
        "temperature_c": temp,
        "humidity_pct": humidity,
        "wind_speed_ms": wind,
        "precip_mm": precip,
        "base_activity": round(base_activity, 4),
        "temp_factor": temp_factor,
        "humidity_factor": humidity_factor,
        "wind_factor": wind_factor,
        "rain_factor": rain_factor,
        "weather_modifier": weather_modifier,
        "daily_flowering_index": round(daily_flowering_index, 4),
        "daily_nectar_supply_index": round(daily_nectar_supply_index, 4),
        "flower_factor": round(flower_factor, 4),
        "nectar_factor": round(nectar_factor, 4),
        "resource_factor": round(resource_factor, 4),
        "expected_activity": row["expected_activity"] or 0.0,
    }


def build_feature_vector(feature_map, feature_columns, feature_stats):
    vector = [1.0]
    for column in feature_columns:
        stats = feature_stats[column]
        vector.append(
            normalize_feature(
                float(feature_map[column]),
                float(stats["mean"]),
                float(stats["std"]),
            )
        )
    return vector


def predict(coefficients, vector):
    total = 0.0
    for coef, feature_value in zip(coefficients, vector):
        total += coef * feature_value
    return total


def clamp(value, min_value, max_value):
    return max(min_value, min(max_value, value))


def apply_residual_guard(raw_residual, sample_count, safeguards):
    full_confidence = int(safeguards.get("full_confidence_sample_count", 24))
    low_cap = float(safeguards.get("low_sample_cap", 0.08))
    medium_cap = float(safeguards.get("medium_sample_cap", 0.12))
    high_cap = float(safeguards.get("high_sample_cap", 0.18))

    confidence_scale = min(1.0, max(0.0, sample_count / full_confidence))
    if sample_count < 12:
        cap = low_cap
    elif sample_count < full_confidence:
        cap = medium_cap
    else:
        cap = high_cap

    guarded = clamp(raw_residual * confidence_scale, -cap, cap)
    return guarded, confidence_scale, cap


def guard_meta(sample_count, safeguards):
    full_confidence = int(safeguards.get("full_confidence_sample_count", 24))
    low_cap = float(safeguards.get("low_sample_cap", 0.08))
    medium_cap = float(safeguards.get("medium_sample_cap", 0.12))
    high_cap = float(safeguards.get("high_sample_cap", 0.18))
    confidence_scale = min(1.0, max(0.0, sample_count / full_confidence))

    if sample_count < 12:
        cap = low_cap
    elif sample_count < full_confidence:
        cap = medium_cap
    else:
        cap = high_cap

    return confidence_scale, cap


def main():
    bundle = load_model_bundle()
    feature_columns = bundle["feature_columns"]
    feature_stats = bundle["feature_stats"]
    coefficients = bundle["coefficients"]
    sample_count = int(bundle.get("sample_count", 0))
    safeguards = bundle.get("safeguards", {})
    flowering_map, nectar_map = load_daily_resource_maps()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
                forecast_time,
                forecast_date,
                CAST(substr(forecast_time, 12, 2) AS INTEGER) AS hour,
                temperature_c,
                humidity_pct,
                wind_speed_ms,
                precip_mm,
                expected_activity
            FROM future_expected_activity_hourly
            ORDER BY forecast_time ASC
            """
        ).fetchall()
    finally:
        conn.close()

    payload = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "source_model": bundle["model_type"],
        "items": [],
    }

    for row in rows:
        if any(row[key] is None for key in ["hour", "temperature_c", "humidity_pct", "wind_speed_ms", "precip_mm"]):
            continue

        rule_expected = float(row["expected_activity"] or 0.0)
        hour = int(row["hour"])

        if hour < 8 or hour > 17 or rule_expected <= 0.03:
            raw_residual = 0.0
            residual = 0.0
            confidence_scale, cap = guard_meta(sample_count, safeguards)
            adjusted = rule_expected
        else:
            feature_map = build_feature_map(row, flowering_map, nectar_map)
            vector = build_feature_vector(feature_map, feature_columns, feature_stats)
            raw_residual = predict(coefficients, vector)
            residual, confidence_scale, cap = apply_residual_guard(raw_residual, sample_count, safeguards)
            adjusted = min(1.0, max(0.0, rule_expected + residual))
        effective_adjustment = adjusted - rule_expected

        payload["items"].append(
            {
                "time": row["forecast_time"],
                "date": row["forecast_date"],
                "rule_expected_activity": round(rule_expected, 4),
                "ml_raw_residual_adjustment": round(float(raw_residual), 4),
                "ml_guarded_residual_adjustment": round(float(residual), 4),
                "ml_residual_adjustment": round(float(effective_adjustment), 4),
                "ml_adjusted_activity": round(adjusted, 4),
                "ml_confidence_scale": round(float(confidence_scale), 4),
                "ml_adjustment_cap": round(float(cap), 4),
            }
        )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote ML-adjusted forecast to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
