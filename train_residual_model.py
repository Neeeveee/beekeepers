# -*- coding: utf-8 -*-

import json
import math
import sqlite3
from datetime import datetime
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "bee_env.db"
MODEL_DIR = BASE_DIR / "models"
MODEL_PATH = MODEL_DIR / "residual_ridge.json"
METRICS_PATH = MODEL_DIR / "residual_ridge_metrics.json"
FULL_CONFIDENCE_SAMPLE_COUNT = 24

FEATURE_COLUMNS = [
    "hour",
    "temperature_c",
    "humidity_pct",
    "wind_speed_ms",
    "precip_mm",
    "base_activity",
    "temp_factor",
    "humidity_factor",
    "wind_factor",
    "rain_factor",
    "weather_modifier",
    "daily_flowering_index",
    "daily_nectar_supply_index",
    "flower_factor",
    "nectar_factor",
    "resource_factor",
    "expected_activity",
]


def load_training_rows():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
                aligned_time,
                hour,
                temperature_c,
                humidity_pct,
                wind_speed_ms,
                precip_mm,
                base_activity,
                temp_factor,
                humidity_factor,
                wind_factor,
                rain_factor,
                weather_modifier,
                daily_flowering_index,
                daily_nectar_supply_index,
                flower_factor,
                nectar_factor,
                resource_factor,
                expected_activity,
                actual_activity
            FROM expected_activity_hourly
            WHERE actual_activity IS NOT NULL
              AND expected_activity IS NOT NULL
            ORDER BY aligned_time ASC
            """
        ).fetchall()
    finally:
        conn.close()

    cleaned_rows = []
    for row in rows:
        values = [row[column] for column in FEATURE_COLUMNS]
        if any(value is None for value in values):
            continue
        cleaned_rows.append(row)
    return cleaned_rows


def compute_feature_stats(rows):
    stats = {}
    for column in FEATURE_COLUMNS:
        values = [float(row[column]) for row in rows]
        mean_value = sum(values) / len(values)
        variance = sum((value - mean_value) ** 2 for value in values) / len(values)
        std_value = math.sqrt(variance) if variance > 0 else 1.0
        stats[column] = {"mean": mean_value, "std": std_value}
    return stats


def build_design_matrix(rows, feature_stats):
    matrix = []
    expected = []
    actual = []
    timestamps = []

    for row in rows:
        feature_row = [1.0]
        for column in FEATURE_COLUMNS:
            value = float(row[column])
            mean_value = feature_stats[column]["mean"]
            std_value = feature_stats[column]["std"]
            feature_row.append((value - mean_value) / std_value if std_value else 0.0)

        matrix.append(feature_row)
        expected.append(float(row["expected_activity"]))
        actual.append(float(row["actual_activity"]))
        timestamps.append(row["aligned_time"])

    residual = [a - e for a, e in zip(actual, expected)]
    return matrix, expected, actual, residual, timestamps


def transpose(matrix):
    return [list(row) for row in zip(*matrix)]


def matmul(a, b):
    result = []
    for row in a:
        result_row = []
        for col_idx in range(len(b[0])):
            value = 0.0
            for i in range(len(row)):
                value += row[i] * b[i][col_idx]
            result_row.append(value)
        result.append(result_row)
    return result


def solve_linear_system(matrix, vector):
    n = len(vector)
    augmented = [matrix[i][:] + [vector[i]] for i in range(n)]

    for col in range(n):
        pivot_row = max(range(col, n), key=lambda r: abs(augmented[r][col]))
        if abs(augmented[pivot_row][col]) < 1e-12:
            raise ValueError("Matrix is singular and cannot be solved.")
        augmented[col], augmented[pivot_row] = augmented[pivot_row], augmented[col]

        pivot = augmented[col][col]
        for j in range(col, n + 1):
            augmented[col][j] /= pivot

        for row in range(n):
            if row == col:
                continue
            factor = augmented[row][col]
            for j in range(col, n + 1):
                augmented[row][j] -= factor * augmented[col][j]

    return [augmented[i][n] for i in range(n)]


def fit_ridge_regression(x, y, alpha=1.0):
    x_t = transpose(x)
    x_t_x = matmul(x_t, x)
    for i in range(1, len(x_t_x)):
        x_t_x[i][i] += alpha

    y_column = [[value] for value in y]
    x_t_y = matmul(x_t, y_column)
    coefficients = solve_linear_system(x_t_x, [row[0] for row in x_t_y])
    return coefficients


def predict(coefficients, x):
    values = []
    for row in x:
        total = 0.0
        for coef, feature_value in zip(coefficients, row):
            total += coef * feature_value
        values.append(total)
    return values


def evaluate(actual, predicted):
    errors = [a - p for a, p in zip(actual, predicted)]
    mae = sum(abs(err) for err in errors) / len(errors)
    rmse = math.sqrt(sum(err * err for err in errors) / len(errors))

    mean_actual = sum(actual) / len(actual)
    total_var = sum((value - mean_actual) ** 2 for value in actual)
    if total_var == 0:
        r2 = None
    else:
        residual_var = sum((a - p) ** 2 for a, p in zip(actual, predicted))
        r2 = 1 - residual_var / total_var

    return {
        "mae": round(mae, 4),
        "rmse": round(rmse, 4),
        "r2": None if r2 is None else round(r2, 4),
    }


def main():
    rows = load_training_rows()
    if len(rows) < 6:
        raise SystemExit(f"Not enough training rows. Need at least 6, got {len(rows)}.")

    feature_stats = compute_feature_stats(rows)
    x, expected, actual, residual, timestamps = build_design_matrix(rows, feature_stats)
    coefficients = fit_ridge_regression(x, residual, alpha=1.0)

    residual_pred = predict(coefficients, x)
    adjusted_pred = [min(1.0, max(0.0, exp + res)) for exp, res in zip(expected, residual_pred)]

    model_bundle = {
        "model_type": "residual_ridge_pure_python",
        "trained_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "sample_count": len(rows),
        "feature_columns": FEATURE_COLUMNS,
        "feature_stats": feature_stats,
        "coefficients": coefficients,
        "safeguards": {
            "full_confidence_sample_count": FULL_CONFIDENCE_SAMPLE_COUNT,
            "low_sample_cap": 0.08,
            "medium_sample_cap": 0.12,
            "high_sample_cap": 0.18,
        },
    }

    metrics = {
        "trained_at": model_bundle["trained_at"],
        "sample_count": len(rows),
        "timestamps": timestamps,
        "residual_metrics": evaluate(residual, residual_pred),
        "adjusted_activity_metrics": evaluate(actual, adjusted_pred),
    }

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    MODEL_PATH.write_text(json.dumps(model_bundle, ensure_ascii=False, indent=2), encoding="utf-8")
    METRICS_PATH.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Residual model trained with {len(rows)} rows.")
    print(f"Saved model to {MODEL_PATH}")
    print(f"Saved metrics to {METRICS_PATH}")
    print("Adjusted activity metrics:", metrics["adjusted_activity_metrics"])


if __name__ == "__main__":
    main()
