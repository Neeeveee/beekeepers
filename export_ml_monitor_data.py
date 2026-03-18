# -*- coding: utf-8 -*-

import json
import sqlite3
from datetime import datetime
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "bee_env.db"
MODEL_PATH = BASE_DIR / "models" / "residual_ridge.json"
METRICS_PATH = BASE_DIR / "models" / "residual_ridge_metrics.json"
FUTURE_PATH = BASE_DIR / "data" / "future-activity-ml-adjusted.json"
OUTPUT_PATH = BASE_DIR / "data" / "ml-monitor.json"


def clamp(value, min_value=0.0, max_value=1.0):
    return max(min_value, min(max_value, value))


def load_json(path: Path):
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def normalize_feature(value, mean_value, std_value):
    if not std_value:
        return 0.0
    return (value - mean_value) / std_value


def predict(coefficients, vector):
    total = 0.0
    for coef, feature_value in zip(coefficients, vector):
        total += coef * feature_value
    return total


def apply_residual_guard(raw_residual, model_bundle):
    sample_count = int(model_bundle.get("sample_count", 0))
    safeguards = model_bundle.get("safeguards", {})
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


def load_history_rows():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
                aligned_time,
                aligned_date,
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
    return rows


def build_history_payload(model_bundle):
    rows = load_history_rows()
    feature_columns = model_bundle["feature_columns"]
    feature_stats = model_bundle["feature_stats"]
    coefficients = model_bundle["coefficients"]

    history_items = []
    total_abs_rule_error = 0.0
    total_abs_ml_error = 0.0

    for row in rows:
        if any(row[column] is None for column in feature_columns):
            continue

        vector = [1.0]
        for column in feature_columns:
            stats = feature_stats[column]
            vector.append(
                normalize_feature(
                    float(row[column]),
                    float(stats["mean"]),
                    float(stats["std"]),
                )
            )

        rule_expected = float(row["expected_activity"])
        actual = float(row["actual_activity"])
        raw_residual = predict(coefficients, vector)
        residual, confidence_scale, cap = apply_residual_guard(raw_residual, model_bundle)
        ml_adjusted = clamp(rule_expected + residual)

        rule_error = actual - rule_expected
        ml_error = actual - ml_adjusted
        total_abs_rule_error += abs(rule_error)
        total_abs_ml_error += abs(ml_error)

        history_items.append(
            {
                "time": row["aligned_time"],
                "date": row["aligned_date"],
                "actual_activity": round(actual, 4),
                "rule_expected_activity": round(rule_expected, 4),
                "ml_adjusted_activity": round(ml_adjusted, 4),
                "ml_raw_residual_adjustment": round(raw_residual, 4),
                "ml_residual_adjustment": round(residual, 4),
                "ml_confidence_scale": round(confidence_scale, 4),
                "ml_adjustment_cap": round(cap, 4),
                "rule_error": round(rule_error, 4),
                "ml_error": round(ml_error, 4),
            }
        )

    sample_count = len(history_items)
    rule_mae = round(total_abs_rule_error / sample_count, 4) if sample_count else None
    ml_mae = round(total_abs_ml_error / sample_count, 4) if sample_count else None

    return {
        "items": history_items,
        "summary": {
            "sample_count": sample_count,
            "rule_mae": rule_mae,
            "ml_mae": ml_mae,
            "mae_improvement": None if rule_mae is None or ml_mae is None else round(rule_mae - ml_mae, 4),
        },
    }


def build_alerts(history_summary, future_payload, model_bundle):
    alerts = []
    sample_count = int(history_summary["sample_count"])
    safeguards = model_bundle.get("safeguards", {})
    full_confidence = int(safeguards.get("full_confidence_sample_count", 24))

    if sample_count < full_confidence:
        alerts.append({
            "level": "warning",
            "title": "样本量偏少",
            "message": f"当前仅有 {sample_count} 条可训练样本，系统已自动缩小 ML 修正幅度，现阶段更适合作为辅助参考，而不是直接替代规则模型。"
        })

    improvement = history_summary["mae_improvement"]
    if improvement is not None and improvement <= 0:
        alerts.append({
            "level": "warning",
            "title": "修正收益有限",
            "message": "当前 ML 修正没有明显降低历史误差，建议继续积累样本后，再判断是否进入主预测链路。"
        })

    future_items = future_payload.get("items", []) if future_payload else []
    if future_items:
        max_adjustment = max(abs(item["ml_residual_adjustment"]) for item in future_items)
        max_cap = max(item.get("ml_adjustment_cap", 0.0) for item in future_items)
        if max_adjustment >= max_cap and max_cap > 0:
            alerts.append({
                "level": "warning",
                "title": "未来修正触及保护上限",
                "message": f"未来预测中的修正量已经触及当前保护上限 {round(max_cap, 4)}，说明模型原始修正意图偏大，建议人工复核规则模型与样本质量。"
            })

    if not alerts:
        alerts.append({
            "level": "ok",
            "title": "当前状态稳定",
            "message": "ML 修正链路已运行，当前未发现明显异常，但仍建议持续观察样本量和误差变化。"
        })

    return alerts


def main():
    model_bundle = load_json(MODEL_PATH)
    metrics = load_json(METRICS_PATH)
    future_payload = load_json(FUTURE_PATH)

    if not model_bundle or not metrics:
        raise SystemExit("Residual model artifacts not found. Run train_residual_model.py first.")

    history_payload = build_history_payload(model_bundle)
    alerts = build_alerts(history_payload["summary"], future_payload, model_bundle)

    payload = {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "model": {
            "target_layer": "第3层",
            "target_name": "蜜蜂行为模型",
            "ml_role": "残差修正",
            "model_type": model_bundle["model_type"],
            "trained_at": model_bundle["trained_at"],
            "sample_count": model_bundle.get("sample_count", 0),
            "safeguards": model_bundle.get("safeguards", {}),
        },
        "metrics": metrics,
        "history": history_payload,
        "future": future_payload or {"items": []},
        "alerts": alerts,
    }

    OUTPUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote ML monitor data to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
