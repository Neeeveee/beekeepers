# -*- coding: utf-8 -*-

from flask import Flask, jsonify
from flask_cors import CORS
import sqlite3
from pathlib import Path
from datetime import datetime, date
import json

app = Flask(__name__)
CORS(app)

DB_PATH = Path(__file__).resolve().parent / "bee_env.db"
DATA_RAW_DIR = Path(__file__).resolve().parent / "data_raw"


def get_db_connection() -> sqlite3.Connection:
    db_file = Path(DB_PATH)
    if not db_file.exists():
        raise FileNotFoundError(f"数据库文件不存在：{DB_PATH}")
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def parse_chart_time(value: str | None) -> datetime | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def load_latest_qweather_7d_payload() -> dict | None:
    files = sorted(DATA_RAW_DIR.glob("qweather_7d_*.json"))
    if not files:
        return None

    latest_file = files[-1]
    try:
        return json.loads(latest_file.read_text(encoding="utf-8"))
    except Exception:
        return None


def build_future_daily_weather(conn: sqlite3.Connection) -> list[dict]:
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT
            forecast_date AS date,
            AVG(temperature_c) AS avg_temp_c,
            AVG(humidity_pct) AS avg_humidity_pct,
            AVG(wind_speed_ms) AS wind_speed_ms,
            SUM(COALESCE(precip_mm, 0)) AS precip_mm,
            AVG(expected_activity) AS behavior_index_raw
        FROM future_expected_activity_hourly
        GROUP BY forecast_date
        ORDER BY forecast_date ASC
        """
    )
    merged = {
        row["date"]: {
            "date": row["date"],
            "avg_temp_c": row["avg_temp_c"],
            "avg_humidity_pct": row["avg_humidity_pct"],
            "wind_speed_ms": row["wind_speed_ms"],
            "precip_mm": row["precip_mm"],
            "behavior_index_raw": row["behavior_index_raw"],
            "source": "qweather-24h",
        }
        for row in cursor.fetchall()
    }

    payload = load_latest_qweather_7d_payload()
    if not payload:
        return [merged[key] for key in sorted(merged)]

    today_str = datetime.now().strftime("%Y-%m-%d")
    for item in payload.get("daily", []):
        forecast_date = item.get("fxDate")
        if not forecast_date or forecast_date <= today_str:
            continue

        if forecast_date in merged:
            continue

        temp_max = float(item.get("tempMax") or 0.0)
        temp_min = float(item.get("tempMin") or 0.0)
        avg_temp_c = round((temp_max + temp_min) / 2.0, 2)
        humidity_pct = float(item.get("humidity") or 0.0)
        precip_mm = float(item.get("precip") or 0.0)
        wind_speed_raw = float(item.get("windSpeedDay") or 0.0)
        wind_speed_ms = round(wind_speed_raw / 3.6, 2)

        merged[forecast_date] = {
            "date": forecast_date,
            "avg_temp_c": avg_temp_c,
            "avg_humidity_pct": humidity_pct,
            "wind_speed_ms": wind_speed_ms,
            "precip_mm": precip_mm,
            "behavior_index_raw": None,
            "source": "qweather-7d",
        }

    return [merged[key] for key in sorted(merged)]


def base_hour_activity(hour: int) -> float:
    if hour < 6 or hour > 19:
        return 0.0
    return round(2.718281828 ** (-((hour - 13.0) ** 2) / 12.0), 4)


def calc_behavior_temp_factor(temp_c: float | None) -> float:
    if temp_c is None:
        return 0.0
    if temp_c < 8:
        return 0.0
    if temp_c < 10:
        return 0.10
    if temp_c < 14:
        return 0.40
    if temp_c < 20:
        return 0.75
    if temp_c <= 30:
        return 1.00
    if temp_c <= 35:
        return 0.80
    return 0.50


def calc_behavior_humidity_factor(humidity_pct: float | None) -> float:
    if humidity_pct is None:
        return 0.0
    if 40 <= humidity_pct <= 75:
        return 1.00
    if humidity_pct <= 85:
        return 0.90
    return 0.75


def calc_behavior_wind_factor(wind_speed_ms: float | None) -> float:
    if wind_speed_ms is None:
        return 0.0
    if wind_speed_ms < 1.5:
        return 1.00
    if wind_speed_ms < 3:
        return 0.90
    if wind_speed_ms < 5:
        return 0.70
    if wind_speed_ms <= 6.7:
        return 0.45
    return 0.0


def calc_behavior_rain_factor(precip_mm: float | None) -> float:
    if precip_mm is None:
        return 1.0
    if precip_mm == 0:
        return 1.0
    if precip_mm < 1:
        return 0.7
    if precip_mm < 5:
        return 0.3
    return 0.18


def ensure_nectar_plant_site_weights_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS nectar_plant_site_weights (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id INTEGER NOT NULL,
            plant_name TEXT NOT NULL,
            scenario TEXT NOT NULL DEFAULT 'default',
            display_weight REAL NOT NULL DEFAULT 1.0,
            note TEXT,
            created_at TEXT DEFAULT (datetime('now', 'localtime')),
            UNIQUE(site_id, plant_name, scenario)
        )
        """
    )


def load_site_display_weights(
    conn: sqlite3.Connection,
    site_id: int = 1,
    scenario: str = "farm"
) -> dict[str, float]:
    ensure_nectar_plant_site_weights_table(conn)
    rows = conn.execute(
        """
        SELECT plant_name, display_weight
        FROM nectar_plant_site_weights
        WHERE site_id = ? AND scenario = ?
        """,
        (site_id, scenario),
    ).fetchall()
    return {
        row["plant_name"]: float(row["display_weight"] or 1.0)
        for row in rows
    }


def build_effective_weight(base_weight: float, display_weight: float | None) -> float:
    return round(base_weight * max(0.0, float(display_weight or 1.0)), 4)


def load_plant_meta(conn: sqlite3.Connection) -> tuple[dict[str, dict], dict[str, float]]:
    cursor = conn.cursor()
    display_weights = load_site_display_weights(conn)
    cursor.execute(
        """
        SELECT
            plant_name,
            nectar_grade,
            pollen_grade,
            avg_yield_kg_per_colony,
            confidence,
            bloom_start_mmdd,
            bloom_end_mmdd
        FROM nectar_plants
        ORDER BY plant_name ASC
        """
    )

    plant_meta = {}
    plant_weights = {}
    for plant in cursor.fetchall():
        plant_name = plant["plant_name"]
        plant_meta[plant_name] = {
            "nectar_grade": plant["nectar_grade"],
            "pollen_grade": plant["pollen_grade"],
            "avg_yield_kg_per_colony": plant["avg_yield_kg_per_colony"],
            "confidence": plant["confidence"],
            "bloom_start_mmdd": plant["bloom_start_mmdd"],
            "bloom_end_mmdd": plant["bloom_end_mmdd"],
            "display_weight": display_weights.get(plant_name, 1.0),
        }
        base_weight = calc_nectar_resource_factor(
            plant["nectar_grade"],
            plant["avg_yield_kg_per_colony"],
            plant["confidence"],
        )
        plant_weights[plant_name] = build_effective_weight(
            base_weight,
            display_weights.get(plant_name, 1.0),
        )

    return plant_meta, plant_weights


def calc_future_resource_overview(
    forecast_date: str,
    avg_temp_c: float,
    avg_humidity_pct: float,
    precip_mm: float,
    plant_meta: dict[str, dict],
    plant_weights: dict[str, float],
) -> tuple[float, float]:
    total_weight = 0.0
    flowering_weighted_sum = 0.0
    nectar_weighted_sum = 0.0

    for plant_name, meta in plant_meta.items():
        if not is_date_in_bloom_window(
            forecast_date,
            meta["bloom_start_mmdd"],
            meta["bloom_end_mmdd"],
        ):
            continue

        base_flowering_score = calc_base_season_score(
            model_date=forecast_date,
            bloom_start_mmdd=meta["bloom_start_mmdd"],
            bloom_end_mmdd=meta["bloom_end_mmdd"],
        )
        flowering_resource_factor = calc_resource_factor(
            meta["nectar_grade"],
            meta["pollen_grade"],
            meta["confidence"],
        )
        flowering_index = calc_flowering_index(
            avg_temp_c=avg_temp_c,
            avg_humidity_pct=avg_humidity_pct,
            precip_mm=precip_mm,
            base_flowering_score=base_flowering_score,
            resource_factor=flowering_resource_factor,
        )

        nectar_resource_factor = calc_nectar_resource_factor(
            meta["nectar_grade"],
            meta["avg_yield_kg_per_colony"],
            meta["confidence"],
        )
        nectar_supply_index = calc_nectar_supply_index(
            flowering_index=flowering_index,
            avg_temp_c=avg_temp_c,
            avg_humidity_pct=avg_humidity_pct,
            precip_mm=precip_mm,
            nectar_resource_factor=nectar_resource_factor,
        )

        weight = plant_weights.get(plant_name, 1.0)
        total_weight += weight
        flowering_weighted_sum += flowering_index * weight
        nectar_weighted_sum += nectar_supply_index * weight

    if total_weight <= 0:
        return 0.0, 0.0

    return (
        round(flowering_weighted_sum / total_weight, 3),
        round(nectar_weighted_sum / total_weight, 3),
    )


def calc_daily_behavior_value(avg_temp_c, avg_humidity_pct, wind_speed_ms, precip_mm) -> float:
    daylight_hours = list(range(6, 20))
    hourly_values = []

    tf = calc_behavior_temp_factor(avg_temp_c)
    hf = calc_behavior_humidity_factor(avg_humidity_pct)
    wf = calc_behavior_wind_factor(wind_speed_ms)
    rf = calc_behavior_rain_factor(precip_mm)

    for hour in daylight_hours:
        base = base_hour_activity(hour)
        if base == 0.0 or tf == 0.0 or wf == 0.0 or rf == 0.0:
            hourly_values.append(0.0)
            continue

        weather_modifier = 0.4 * tf + 0.2 * hf + 0.2 * wf + 0.2 * rf
        hourly_values.append(round(base * weather_modifier, 4))

    if not hourly_values:
        return 0.0

    return round(sum(hourly_values) / len(hourly_values), 4)


def build_extended_future_hourly_forecast(conn: sqlite3.Connection) -> list[dict]:
    plant_meta, plant_weights = load_plant_meta(conn)
    future_days = build_future_daily_weather(conn)
    if not future_days:
        return []

    forecast_data = []

    for day in future_days:
        forecast_date = day["date"]

        avg_temp_c = day["avg_temp_c"]
        avg_humidity_pct = day["avg_humidity_pct"]
        wind_speed_ms = day["wind_speed_ms"]
        precip_mm = day["precip_mm"]
        daily_flowering_index, daily_nectar_supply_index = calc_future_resource_overview(
            forecast_date,
            avg_temp_c,
            avg_humidity_pct,
            precip_mm,
            plant_meta,
            plant_weights,
        )

        flower_factor = round(0.5 + 0.5 * clamp(daily_flowering_index), 4)
        nectar_factor = round(0.5 + 0.5 * clamp(daily_nectar_supply_index), 4)
        resource_factor = round(0.5 * flower_factor + 0.5 * nectar_factor, 4)

        tf = calc_behavior_temp_factor(avg_temp_c)
        hf = calc_behavior_humidity_factor(avg_humidity_pct)
        wf = calc_behavior_wind_factor(wind_speed_ms)
        rf = calc_behavior_rain_factor(precip_mm)

        for hour in range(6, 20):
            base = base_hour_activity(hour)
            if base == 0.0 or tf == 0.0 or wf == 0.0 or rf == 0.0:
                expected_activity = 0.0
            else:
                weather_modifier = 0.4 * tf + 0.2 * hf + 0.2 * wf + 0.2 * rf
                expected_activity = round(base * weather_modifier * resource_factor, 4)

            forecast_data.append({
                "time": f"{forecast_date} {hour:02d}:00:00",
                "value": expected_activity,
            })

    forecast_data.sort(key=lambda item: item["time"])
    return forecast_data


# =========================
# 蜜蜂行为模型中的环境因子规则
# =========================

def get_temp_factor(temp):
    if temp is None:
        return None
    if temp < 9:
        return 0.10
    elif 9 <= temp < 14:
        return 0.40
    elif 14 <= temp < 20:
        return 0.75
    elif 20 <= temp <= 30:
        return 1.00
    elif 30 < temp <= 35:
        return 0.80
    else:
        return 0.50


def get_wind_factor(wind):
    # wind 单位：m/s
    if wind is None:
        return None
    if wind < 1.5:
        return 1.00
    elif 1.5 <= wind < 3:
        return 0.90
    elif 3 <= wind < 5:
        return 0.70
    elif 5 <= wind <= 6.7:
        return 0.45
    else:
        return 0.20


def get_humidity_factor(humidity):
    if humidity is None:
        return None
    if 40 <= humidity <= 75:
        return 1.00
    elif 75 < humidity <= 85:
        return 0.90
    elif humidity > 85:
        return 0.75
    else:
        return 0.90


# =========================
# 花期模型规则（与 derive_flowering_index.py 保持一致）
# =========================

def mmdd_to_day_of_year(mmdd: str, ref_year: int = 2026) -> int:
    month, day = map(int, mmdd.split("-"))
    return date(ref_year, month, day).timetuple().tm_yday


def get_date_day_of_year(date_str: str) -> int:
    dt = datetime.strptime(date_str, "%Y-%m-%d").date()
    return dt.timetuple().tm_yday


def is_cross_year(start_mmdd: str, end_mmdd: str) -> bool:
    start_month, start_day = map(int, start_mmdd.split("-"))
    end_month, end_day = map(int, end_mmdd.split("-"))
    return (start_month, start_day) > (end_month, end_day)


def calc_base_season_score(model_date: str, bloom_start_mmdd: str, bloom_end_mmdd: str) -> float:
    doy = get_date_day_of_year(model_date)
    start_doy = mmdd_to_day_of_year(bloom_start_mmdd)
    end_doy = mmdd_to_day_of_year(bloom_end_mmdd)

    if is_cross_year(bloom_start_mmdd, bloom_end_mmdd):
        if doy >= start_doy:
            progress = (doy - start_doy) / ((365 - start_doy) + end_doy)
            in_window = True
        elif doy <= end_doy:
            progress = ((365 - start_doy) + doy) / ((365 - start_doy) + end_doy)
            in_window = True
        else:
            in_window = False
            progress = None
    else:
        if start_doy <= doy <= end_doy:
            progress = (doy - start_doy) / max(1, (end_doy - start_doy))
            in_window = True
        else:
            in_window = False
            progress = None

    if not in_window:
        return 0.12

    if progress < 0.20:
        return 0.45
    elif progress < 0.40:
        return 0.70
    elif progress < 0.70:
        return 1.00
    elif progress < 0.90:
        return 0.75
    else:
        return 0.45


def is_date_in_bloom_window(model_date: str, bloom_start_mmdd: str, bloom_end_mmdd: str) -> bool:
    doy = get_date_day_of_year(model_date)
    start_doy = mmdd_to_day_of_year(bloom_start_mmdd)
    end_doy = mmdd_to_day_of_year(bloom_end_mmdd)

    if is_cross_year(bloom_start_mmdd, bloom_end_mmdd):
        return doy >= start_doy or doy <= end_doy

    return start_doy <= doy <= end_doy


def calc_flowering_temp_factor(avg_temp_c: float) -> float:
    if avg_temp_c is None:
        return 0.55
    elif avg_temp_c < 5:
        return 0.35
    elif avg_temp_c < 10:
        return 0.60
    elif avg_temp_c < 15:
        return 0.82
    elif avg_temp_c <= 22:
        return 1.00
    elif avg_temp_c <= 28:
        return 0.88
    else:
        return 0.65


def calc_flowering_humidity_factor(avg_humidity_pct: float) -> float:
    if avg_humidity_pct is None:
        return 0.90
    elif avg_humidity_pct < 35:
        return 0.85
    elif avg_humidity_pct < 50:
        return 0.95
    elif avg_humidity_pct <= 80:
        return 1.00
    elif avg_humidity_pct <= 90:
        return 0.90
    else:
        return 0.75


def calc_flowering_rain_factor(precip_mm: float) -> float:
    if precip_mm is None:
        return 1.00
    elif precip_mm == 0:
        return 1.00
    elif precip_mm < 1:
        return 0.88
    elif precip_mm < 5:
        return 0.68
    else:
        return 0.45


def calc_flowering_index(avg_temp_c, avg_humidity_pct, precip_mm, base_flowering_score, resource_factor) -> float:
    temp_factor = calc_flowering_temp_factor(avg_temp_c)
    humidity_factor = calc_flowering_humidity_factor(avg_humidity_pct)
    rain_factor = calc_flowering_rain_factor(precip_mm)
    env_modifier = 0.55 * temp_factor + 0.20 * humidity_factor + 0.25 * rain_factor

    return round(
        clamp(base_flowering_score * env_modifier * resource_factor),
        3
    )


def calc_resource_factor(nectar_grade, pollen_grade, confidence) -> float:
    nectar_grade = nectar_grade or 0
    pollen_grade = pollen_grade or 0
    confidence = confidence or 0.5

    factor = 1.0

    if nectar_grade >= 5:
        factor += 0.08
    elif nectar_grade >= 4:
        factor += 0.05
    elif nectar_grade >= 3:
        factor += 0.03

    if pollen_grade >= 4:
        factor += 0.04
    elif pollen_grade >= 3:
        factor += 0.02

    if confidence < 0.6:
        factor -= 0.05

    return round(max(0.85, min(1.15, factor)), 2)


# =========================
# 花蜜量模型规则
# =========================

def calc_nectar_resource_factor(nectar_grade, avg_yield_kg_per_colony, confidence) -> float:
    nectar_grade = nectar_grade or 0
    avg_yield_kg_per_colony = avg_yield_kg_per_colony or 0.0
    confidence = confidence or 0.5

    if nectar_grade >= 5:
        grade_factor = 1.00
    elif nectar_grade == 4:
        grade_factor = 0.90
    elif nectar_grade == 3:
        grade_factor = 0.75
    elif nectar_grade == 2:
        grade_factor = 0.60
    else:
        grade_factor = 0.45

    if avg_yield_kg_per_colony >= 30:
        yield_factor = 1.10
    elif avg_yield_kg_per_colony >= 20:
        yield_factor = 1.00
    elif avg_yield_kg_per_colony >= 10:
        yield_factor = 0.90
    else:
        yield_factor = 0.80

    if confidence >= 0.8:
        confidence_factor = 1.00
    elif confidence >= 0.6:
        confidence_factor = 0.95
    else:
        confidence_factor = 0.90

    factor = grade_factor * yield_factor * confidence_factor
    return round(max(0.35, min(1.10, factor)), 3)


def calc_nectar_temp_factor(avg_temp_c):
    if avg_temp_c is None:
        return 0.75
    elif avg_temp_c < 10:
        return 0.55
    elif avg_temp_c < 15:
        return 0.82
    elif avg_temp_c < 22:
        return 1.00
    elif avg_temp_c < 28:
        return 1.02
    elif avg_temp_c < 32:
        return 0.90
    else:
        return 0.72


def calc_nectar_humidity_factor(avg_humidity_pct):
    if avg_humidity_pct is None:
        return 0.95
    elif avg_humidity_pct < 35:
        return 0.85
    elif avg_humidity_pct < 50:
        return 1.00
    elif avg_humidity_pct <= 85:
        return 1.00
    elif avg_humidity_pct <= 92:
        return 0.94
    else:
        return 0.84


def calc_nectar_rain_factor(precip_mm):
    if precip_mm is None:
        return 1.00
    elif precip_mm == 0:
        return 1.00
    elif precip_mm < 1:
        return 0.92
    elif precip_mm < 5:
        return 0.72
    else:
        return 0.45


def calc_nectar_supply_index(
    flowering_index,
    avg_temp_c,
    avg_humidity_pct,
    precip_mm,
    nectar_resource_factor
):
    temp_factor = calc_nectar_temp_factor(avg_temp_c)
    humidity_factor = calc_nectar_humidity_factor(avg_humidity_pct)
    rain_factor = calc_nectar_rain_factor(precip_mm)
    env_modifier = 0.50 * temp_factor + 0.20 * humidity_factor + 0.30 * rain_factor
    nectar_resource_modifier = 0.80 + 0.30 * clamp(nectar_resource_factor, 0.0, 1.1) / 1.1

    return round(
        clamp(flowering_index * nectar_resource_modifier * env_modifier),
        3
    )


def clamp(value: float, min_value: float = 0.0, max_value: float = 1.0) -> float:
    return max(min_value, min(max_value, value))


def build_bridge_series(actual, forecast):
    actual = actual[:] if actual else []
    forecast = forecast[:] if forecast else []

    if actual and forecast:
        last_actual = actual[-1]
        actual_time = parse_chart_time(last_actual.get("time"))
        forecast_time = parse_chart_time(forecast[0].get("time"))

        # Only bridge adjacent segments; otherwise a long data gap creates a fake corner.
        if actual_time and forecast_time:
            gap_seconds = (forecast_time - actual_time).total_seconds()
            if 0 <= gap_seconds <= 6 * 3600:
                forecast.insert(0, {"time": last_actual["time"], "value": last_actual["value"]})

    return {"actual": actual, "forecast": forecast}


def split_daily_actual_forecast(actual, forecast):
    today_str = datetime.now().strftime("%Y-%m-%d")
    actual = [item for item in (actual or []) if item.get("time") and item["time"] <= today_str]
    forecast = [item for item in (forecast or []) if item.get("time") and item["time"] > today_str]
    return actual, forecast


def split_hourly_actual_forecast(actual, forecast):
    actual = actual[:] if actual else []
    forecast = forecast[:] if forecast else []

    if not actual:
        return actual, forecast

    last_actual_time = parse_chart_time(actual[-1].get("time"))
    if not last_actual_time:
        return actual, forecast

    filtered_forecast = []
    for item in forecast:
        forecast_time = parse_chart_time(item.get("time"))
        if forecast_time and forecast_time > last_actual_time:
            filtered_forecast.append(item)

    return actual, filtered_forecast


# =========================
# 错配判断规则
# =========================

def calc_mismatch_gap(nectar_supply_index, behavior_index_norm):
    if nectar_supply_index is None or behavior_index_norm is None:
        return None
    return round(abs(nectar_supply_index - behavior_index_norm), 3)


def calc_mismatch_risk(raw_gap):
    """
    把原始偏差映射成最终错配风险：
    - 前 0.20 视为可接受波动
    - 用 0.80 做压缩区间，避免风险轻易贴近 1
    """
    if raw_gap is None:
        return None

    risk = (raw_gap - 0.10) / 0.60
    return round(clamp(risk), 3)


def calc_mismatch_type(nectar_supply_index, behavior_index_norm, raw_gap):
    if nectar_supply_index is None or behavior_index_norm is None or raw_gap is None:
        return "no_data"

    if raw_gap < 0.10:
        return "matched"

    if nectar_supply_index > behavior_index_norm:
        return "resource_ahead"

    return "behavior_ahead"


def calc_mismatch_level(mismatch_gap):
    if mismatch_gap is None:
        return "无数据"
    elif mismatch_gap < 0.15:
        return "基本匹配"
    elif mismatch_gap < 0.30:
        return "轻度错配"
    elif mismatch_gap < 0.50:
        return "中度错配"
    else:
        return "显著错配"


@app.route("/")
def home():
    return "chart_api is running"


@app.route("/api/bee-activity-forecast")
def get_bee_activity_forecast():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT aligned_time AS time, actual_activity
            FROM expected_activity_hourly
            ORDER BY aligned_time ASC
            """
        )
        actual_rows = cursor.fetchall()

        actual_data = [{"time": row["time"], "value": row["actual_activity"]} for row in actual_rows]
        forecast_data = build_extended_future_hourly_forecast(conn)
        actual_data, forecast_data = split_hourly_actual_forecast(actual_data, forecast_data)

        return jsonify(build_bridge_series(actual_data, forecast_data))
    finally:
        conn.close()


@app.route("/api/env-impact-forecast")
def get_env_impact_forecast():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT
                substr(event_time, 1, 10) AS model_date,
                AVG(temperature_c) AS avg_temp_c,
                AVG(humidity_pct) AS avg_humidity_pct,
                AVG(wind_speed_ms) AS avg_wind_raw
            FROM eco_time_series
            GROUP BY substr(event_time, 1, 10)
            ORDER BY model_date ASC
            """
        )
        hist_rows = cursor.fetchall()

        actual = []
        for row in hist_rows:
            raw_wind = row["avg_wind_raw"]
            wind_ms = None if raw_wind is None else round(raw_wind / 3.6, 2)

            temp_factor = get_temp_factor(row["avg_temp_c"])
            humidity_factor = get_humidity_factor(row["avg_humidity_pct"])
            wind_factor = get_wind_factor(wind_ms)

            if temp_factor is None or humidity_factor is None or wind_factor is None:
                env_suitability = None
            else:
                env_suitability = round(temp_factor * humidity_factor * wind_factor, 3)

            actual.append({
                "time": row["model_date"],
                "value": env_suitability
            })

        cursor.execute(
            """
            SELECT
                forecast_date,
                AVG(temperature_c) AS avg_temp_c,
                AVG(humidity_pct) AS avg_humidity_pct,
                AVG(wind_speed_ms) AS avg_wind_ms
            FROM future_expected_activity_hourly
            GROUP BY forecast_date
            ORDER BY forecast_date ASC
            """
        )
        future_rows = cursor.fetchall()

        forecast = []
        for row in future_rows:
            avg_wind_ms = row["avg_wind_ms"]
            wind_ms = None if avg_wind_ms is None else round(avg_wind_ms, 2)

            temp_factor = get_temp_factor(row["avg_temp_c"])
            humidity_factor = get_humidity_factor(row["avg_humidity_pct"])
            wind_factor = get_wind_factor(wind_ms)

            if temp_factor is None or humidity_factor is None or wind_factor is None:
                env_suitability = None
            else:
                env_suitability = round(temp_factor * humidity_factor * wind_factor, 3)

            forecast.append({
                "time": row["forecast_date"],
                "value": env_suitability
            })

        return jsonify(build_bridge_series(actual, forecast))
    finally:
        conn.close()


@app.route("/api/flowering-overview")
def get_flowering_overview():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        display_weights = load_site_display_weights(conn)

        cursor.execute(
            """
            SELECT
                plant_name,
                nectar_grade,
                pollen_grade,
                confidence,
                bloom_start_mmdd,
                bloom_end_mmdd
            FROM nectar_plants
            ORDER BY plant_name ASC
            """
        )
        plants = cursor.fetchall()

        plant_meta = {}
        plant_weights = {}
        for plant in plants:
            plant_name = plant["plant_name"]
            base_weight = calc_resource_factor(
                plant["nectar_grade"],
                plant["pollen_grade"],
                plant["confidence"]
            )
            display_weight = display_weights.get(plant_name, 1.0)
            plant_weights[plant_name] = build_effective_weight(base_weight, display_weight)
            plant_meta[plant_name] = {
                "bloom_start_mmdd": plant["bloom_start_mmdd"],
                "bloom_end_mmdd": plant["bloom_end_mmdd"],
                "nectar_grade": plant["nectar_grade"],
                "pollen_grade": plant["pollen_grade"],
                "confidence": plant["confidence"],
                "display_weight": display_weight,
            }

        cursor.execute(
            """
            SELECT
                model_date,
                plant_name,
                flowering_index
            FROM flowering_model_daily
            ORDER BY model_date ASC, plant_name ASC
            """
        )
        hist_rows = cursor.fetchall()

        history_by_date = {}
        for row in hist_rows:
            d = row["model_date"]
            p = row["plant_name"]
            v = row["flowering_index"]
            history_by_date.setdefault(d, []).append((p, v))

        actual = []
        for d, items in history_by_date.items():
            total_weight = 0.0
            weighted_sum = 0.0
            for plant_name, flowering_index in items:
                meta = plant_meta.get(plant_name)
                if not meta or not is_date_in_bloom_window(
                    d,
                    meta["bloom_start_mmdd"],
                    meta["bloom_end_mmdd"]
                ):
                    continue
                w = plant_weights.get(plant_name, 1.0)
                total_weight += w
                weighted_sum += (flowering_index or 0.0) * w

            overview = round(weighted_sum / total_weight, 3) if total_weight > 0 else 0.0
            actual.append({"time": d, "value": overview})

        current_top = []
        if history_by_date:
            latest_hist_date = sorted(history_by_date.keys())[-1]
            ranked = sorted(
                [
                    (
                        plant_name,
                        value,
                        round((value or 0.0) * plant_weights.get(plant_name, 1.0), 3)
                    )
                    for plant_name, value in history_by_date[latest_hist_date]
                    if plant_name in plant_meta and is_date_in_bloom_window(
                        latest_hist_date,
                        plant_meta[plant_name]["bloom_start_mmdd"],
                        plant_meta[plant_name]["bloom_end_mmdd"]
                    )
                ],
                key=lambda x: x[2],
                reverse=True
            )[:3]
            current_top = [
                {
                    "plant_name": plant_name,
                    "flowering_index": round(value, 3),
                    "contribution_value": contribution_value
                }
                for plant_name, value, contribution_value in ranked
            ]

        forecast = []
        future_last_day_scores = []

        for row in build_future_daily_weather(conn):
            forecast_date = row["date"]
            avg_temp_c = row["avg_temp_c"]
            avg_humidity_pct = row["avg_humidity_pct"]
            precip_mm = row["precip_mm"]

            total_weight = 0.0
            weighted_sum = 0.0
            day_scores = []

            for plant_name, meta in plant_meta.items():
                base_flowering_score = calc_base_season_score(
                    model_date=forecast_date,
                    bloom_start_mmdd=meta["bloom_start_mmdd"],
                    bloom_end_mmdd=meta["bloom_end_mmdd"]
                )
                resource_factor = calc_resource_factor(
                    meta["nectar_grade"],
                    meta["pollen_grade"],
                    meta["confidence"]
                )

                flowering_index = calc_flowering_index(
                    avg_temp_c=avg_temp_c,
                    avg_humidity_pct=avg_humidity_pct,
                    precip_mm=precip_mm,
                    base_flowering_score=base_flowering_score,
                    resource_factor=resource_factor
                )

                if not is_date_in_bloom_window(
                    forecast_date,
                    meta["bloom_start_mmdd"],
                    meta["bloom_end_mmdd"]
                ):
                    continue

                weight = plant_weights.get(plant_name, 1.0)
                total_weight += weight
                weighted_sum += flowering_index * weight
                day_scores.append((plant_name, flowering_index))

            overview = round(weighted_sum / total_weight, 3) if total_weight > 0 else 0.0
            forecast.append({"time": forecast_date, "value": overview})
            future_last_day_scores = day_scores

        future_top = []
        if future_last_day_scores:
            ranked = sorted(
                future_last_day_scores,
                key=lambda x: x[1] * plant_weights.get(x[0], 1.0),
                reverse=True
            )[:3]
            future_top = [
                {
                    "plant_name": plant_name,
                    "flowering_index": round(value, 3),
                    "contribution_value": round(value * plant_weights.get(plant_name, 1.0), 3)
                }
                for plant_name, value in ranked
            ]

        actual, forecast = split_daily_actual_forecast(actual, forecast)
        result = build_bridge_series(actual, forecast)
        result["current_top"] = current_top
        result["future_top"] = future_top

        return jsonify(result)
    finally:
        conn.close()


@app.route("/api/nectar-supply-overview")
def get_nectar_supply_overview():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        display_weights = load_site_display_weights(conn)

        cursor.execute(
            """
            SELECT
                plant_name,
                nectar_grade,
                pollen_grade,
                avg_yield_kg_per_colony,
                confidence,
                bloom_start_mmdd,
                bloom_end_mmdd
            FROM nectar_plants
            ORDER BY plant_name ASC
            """
        )
        plants = cursor.fetchall()

        plant_meta = {}
        plant_weights = {}

        for plant in plants:
            plant_name = plant["plant_name"]

            base_weight = calc_nectar_resource_factor(
                plant["nectar_grade"],
                plant["avg_yield_kg_per_colony"],
                plant["confidence"]
            )

            display_weight = display_weights.get(plant_name, 1.0)
            plant_weights[plant_name] = build_effective_weight(base_weight, display_weight)
            plant_meta[plant_name] = {
                "nectar_grade": plant["nectar_grade"],
                "pollen_grade": plant["pollen_grade"],
                "avg_yield_kg_per_colony": plant["avg_yield_kg_per_colony"],
                "confidence": plant["confidence"],
                "bloom_start_mmdd": plant["bloom_start_mmdd"],
                "bloom_end_mmdd": plant["bloom_end_mmdd"],
                "display_weight": display_weight,
            }

        cursor.execute(
            """
            SELECT
                model_date,
                plant_name,
                nectar_supply_index
            FROM nectar_supply_model_daily
            ORDER BY model_date ASC, plant_name ASC
            """
        )
        hist_rows = cursor.fetchall()

        history_by_date = {}
        for row in hist_rows:
            d = row["model_date"]
            p = row["plant_name"]
            v = row["nectar_supply_index"]
            history_by_date.setdefault(d, []).append((p, v))

        actual = []
        for d, items in history_by_date.items():
            total_weight = 0.0
            weighted_sum = 0.0

            for plant_name, nectar_supply_index in items:
                meta = plant_meta.get(plant_name)
                if not meta or not is_date_in_bloom_window(
                    d,
                    meta["bloom_start_mmdd"],
                    meta["bloom_end_mmdd"]
                ):
                    continue
                w = plant_weights.get(plant_name, 1.0)
                total_weight += w
                weighted_sum += (nectar_supply_index or 0.0) * w

            overview = round(weighted_sum / total_weight, 3) if total_weight > 0 else 0.0
            actual.append({"time": d, "value": overview})

        current_top = []
        if history_by_date:
            latest_hist_date = sorted(history_by_date.keys())[-1]
            ranked = sorted(
                [
                    (
                        plant_name,
                        value,
                        round((value or 0.0) * plant_weights.get(plant_name, 1.0), 3)
                    )
                    for plant_name, value in history_by_date[latest_hist_date]
                    if plant_name in plant_meta and is_date_in_bloom_window(
                        latest_hist_date,
                        plant_meta[plant_name]["bloom_start_mmdd"],
                        plant_meta[plant_name]["bloom_end_mmdd"]
                    )
                ],
                key=lambda x: x[2],
                reverse=True
            )[:3]

            current_top = [
                {
                    "plant_name": plant_name,
                    "nectar_supply_index": round(value, 3),
                    "contribution_value": contribution_value
                }
                for plant_name, value, contribution_value in ranked
            ]

        forecast = []
        future_last_day_scores = []

        for row in build_future_daily_weather(conn):
            forecast_date = row["date"]
            avg_temp_c = row["avg_temp_c"]
            avg_humidity_pct = row["avg_humidity_pct"]
            precip_mm = row["precip_mm"]

            total_weight = 0.0
            weighted_sum = 0.0
            day_scores = []

            for plant_name, meta in plant_meta.items():
                base_flowering_score = calc_base_season_score(
                    model_date=forecast_date,
                    bloom_start_mmdd=meta["bloom_start_mmdd"],
                    bloom_end_mmdd=meta["bloom_end_mmdd"]
                )
                resource_factor = calc_resource_factor(
                    meta["nectar_grade"],
                    meta["pollen_grade"],
                    meta["confidence"]
                )
                flowering_index = calc_flowering_index(
                    avg_temp_c=avg_temp_c,
                    avg_humidity_pct=avg_humidity_pct,
                    precip_mm=precip_mm,
                    base_flowering_score=base_flowering_score,
                    resource_factor=resource_factor
                )

                nectar_resource_factor = calc_nectar_resource_factor(
                    meta["nectar_grade"],
                    meta["avg_yield_kg_per_colony"],
                    meta["confidence"]
                )
                nectar_supply_index = calc_nectar_supply_index(
                    flowering_index=flowering_index,
                    avg_temp_c=avg_temp_c,
                    avg_humidity_pct=avg_humidity_pct,
                    precip_mm=precip_mm,
                    nectar_resource_factor=nectar_resource_factor
                )

                if not is_date_in_bloom_window(
                    forecast_date,
                    meta["bloom_start_mmdd"],
                    meta["bloom_end_mmdd"]
                ):
                    continue

                weight = plant_weights.get(plant_name, 1.0)
                total_weight += weight
                weighted_sum += nectar_supply_index * weight
                day_scores.append((plant_name, nectar_supply_index))

            overview = round(weighted_sum / total_weight, 3) if total_weight > 0 else 0.0
            forecast.append({"time": forecast_date, "value": overview})
            future_last_day_scores = day_scores

        future_top = []
        if future_last_day_scores:
            ranked = sorted(
                future_last_day_scores,
                key=lambda x: x[1] * plant_weights.get(x[0], 1.0),
                reverse=True
            )[:3]

            future_top = [
                {
                    "plant_name": plant_name,
                    "nectar_supply_index": round(value, 3),
                    "contribution_value": round(value * plant_weights.get(plant_name, 1.0), 3)
                }
                for plant_name, value in ranked
            ]

        actual, forecast = split_daily_actual_forecast(actual, forecast)
        result = build_bridge_series(actual, forecast)
        result["current_top"] = current_top
        result["future_top"] = future_top

        return jsonify(result)
    finally:
        conn.close()


@app.route("/api/mismatch-overview")
def get_mismatch_overview():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        display_weights = load_site_display_weights(conn)

        # =========================
        # 历史错配结果
        # =========================
        cursor.execute(
            """
            SELECT
                model_date,
                raw_gap,
                mismatch_gap,
                mismatch_type,
                mismatch_level
            FROM mismatch_index_daily
            ORDER BY model_date ASC
            """
        )
        hist_rows = cursor.fetchall()

        actual = []
        history_info = []
        for row in hist_rows:
            actual.append({
                "time": row["model_date"],
                "value": row["mismatch_gap"]
            })
            history_info.append({
                "time": row["model_date"],
                "mismatch_type": row["mismatch_type"],
                "mismatch_level": row["mismatch_level"]
            })

        # =========================
        # 行为归一化基准
        # 不再只用历史最大值，而是用“历史 + 未来”共同最大值
        # 否则未来值一旦超过历史最大值，就会全部被压成 1
        # =========================
        cursor.execute(
            """
            SELECT AVG(expected_activity) AS behavior_index_raw
            FROM expected_activity_hourly
            WHERE hour BETWEEN 6 AND 19
            GROUP BY aligned_date
            """
        )
        hist_behavior_rows = cursor.fetchall()

        all_behavior_values = [
            row["behavior_index_raw"]
            for row in hist_behavior_rows
            if row["behavior_index_raw"] is not None
        ]

        future_days = build_future_daily_weather(conn)
        for row in future_days:
            behavior_index_raw = row["behavior_index_raw"]
            if behavior_index_raw is None:
                behavior_index_raw = calc_daily_behavior_value(
                    row["avg_temp_c"],
                    row["avg_humidity_pct"],
                    row["wind_speed_ms"],
                    row["precip_mm"],
                )
                row["behavior_index_raw"] = behavior_index_raw
            if behavior_index_raw is not None:
                all_behavior_values.append(behavior_index_raw)

        max_behavior = max(all_behavior_values) if all_behavior_values else 1.0
        if max_behavior <= 0:
            max_behavior = 1.0

        # =========================
        # 植物元数据（未来供给推算要用）
        # =========================
        cursor.execute(
            """
            SELECT
                plant_name,
                nectar_grade,
                pollen_grade,
                avg_yield_kg_per_colony,
                confidence,
                bloom_start_mmdd,
                bloom_end_mmdd
            FROM nectar_plants
            ORDER BY plant_name ASC
            """
        )
        plants = cursor.fetchall()

        plant_meta = {}
        plant_weights = {}
        for plant in plants:
            plant_name = plant["plant_name"]

            base_weight = calc_nectar_resource_factor(
                plant["nectar_grade"],
                plant["avg_yield_kg_per_colony"],
                plant["confidence"]
            )

            plant_weights[plant_name] = build_effective_weight(
                base_weight,
                display_weights.get(plant_name, 1.0),
            )
            plant_meta[plant_name] = {
                "nectar_grade": plant["nectar_grade"],
                "pollen_grade": plant["pollen_grade"],
                "avg_yield_kg_per_colony": plant["avg_yield_kg_per_colony"],
                "confidence": plant["confidence"],
                "bloom_start_mmdd": plant["bloom_start_mmdd"],
                "bloom_end_mmdd": plant["bloom_end_mmdd"],
            }

        # =========================
        # 未来天气（日级）
        # =========================
        forecast = []
        forecast_info = []

        for row in future_days:
            forecast_date = row["date"]
            avg_temp_c = row["avg_temp_c"]
            avg_humidity_pct = row["avg_humidity_pct"]
            precip_mm = row["precip_mm"]
            behavior_index_raw = row["behavior_index_raw"]

            if behavior_index_raw is None:
                behavior_index_norm = None
            else:
                behavior_index_norm = round(clamp(behavior_index_raw / max_behavior), 3)

            # 推未来综合蜜源供给
            total_weight = 0.0
            weighted_sum = 0.0

            for plant_name, meta in plant_meta.items():
                if not is_date_in_bloom_window(
                    forecast_date,
                    meta["bloom_start_mmdd"],
                    meta["bloom_end_mmdd"]
                ):
                    continue

                base_flowering_score = calc_base_season_score(
                    model_date=forecast_date,
                    bloom_start_mmdd=meta["bloom_start_mmdd"],
                    bloom_end_mmdd=meta["bloom_end_mmdd"]
                )
                resource_factor = calc_resource_factor(
                    meta["nectar_grade"],
                    meta["pollen_grade"],
                    meta["confidence"]
                )
                flowering_index = calc_flowering_index(
                    avg_temp_c=avg_temp_c,
                    avg_humidity_pct=avg_humidity_pct,
                    precip_mm=precip_mm,
                    base_flowering_score=base_flowering_score,
                    resource_factor=resource_factor
                )

                nectar_resource_factor = calc_nectar_resource_factor(
                    meta["nectar_grade"],
                    meta["avg_yield_kg_per_colony"],
                    meta["confidence"]
                )
                nectar_supply_index = calc_nectar_supply_index(
                    flowering_index=flowering_index,
                    avg_temp_c=avg_temp_c,
                    avg_humidity_pct=avg_humidity_pct,
                    precip_mm=precip_mm,
                    nectar_resource_factor=nectar_resource_factor
                )

                weight = plant_weights.get(plant_name, 1.0)
                total_weight += weight
                weighted_sum += nectar_supply_index * weight

            nectar_supply_overview = round(weighted_sum / total_weight, 3) if total_weight > 0 else 0.0

            # 先算原始偏差，再映射成最终风险
            raw_gap = calc_mismatch_gap(nectar_supply_overview, behavior_index_norm)
            mismatch_gap = calc_mismatch_risk(raw_gap)
            mismatch_type = calc_mismatch_type(nectar_supply_overview, behavior_index_norm, raw_gap)
            mismatch_level = calc_mismatch_level(mismatch_gap)

            forecast.append({
                "time": forecast_date,
                "value": mismatch_gap
            })
            forecast_info.append({
                "time": forecast_date,
                "mismatch_type": mismatch_type,
                "mismatch_level": mismatch_level
            })

        actual, forecast = split_daily_actual_forecast(actual, forecast)
        result = build_bridge_series(actual, forecast)
        result["history_info"] = history_info
        result["forecast_info"] = forecast_info

        return jsonify(result)
    finally:
        conn.close()


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
