# -*- coding: utf-8 -*-

import math
import sqlite3
from datetime import datetime
from pathlib import Path


DB_PATH = Path(__file__).resolve().parent / "bee_env.db"


def get_db_connection() -> sqlite3.Connection:
    db_file = Path(DB_PATH)
    if not db_file.exists():
        raise FileNotFoundError(f"数据库文件不存在：{DB_PATH}")

    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout = 30000;")
    return conn


def clamp(value: float, min_value: float = 0.0, max_value: float = 1.0) -> float:
    return max(min_value, min(max_value, value))


def base_activity(hour: int) -> float:
    if hour < 6 or hour > 19:
        return 0.0
    return round(math.exp(-((hour - 13.0) ** 2) / 12.0), 4)


def temp_factor(temp_c: float | None) -> float:
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


def humidity_factor(humidity_pct: float | None) -> float:
    if humidity_pct is None:
        return 0.0
    if 40 <= humidity_pct <= 75:
        return 1.00
    if humidity_pct <= 85:
        return 0.90
    return 0.75


def wind_factor(wind_speed_ms: float | None) -> float:
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


def rain_factor(precip_mm: float | None) -> float:
    if precip_mm is None:
        return 1.0
    if precip_mm == 0:
        return 1.0
    if precip_mm < 1:
        return 0.7
    if precip_mm < 5:
        return 0.3
    return 0.0


def build_daily_index_map(cursor: sqlite3.Cursor, table_name: str, value_column: str) -> dict[str, float]:
    rows = cursor.execute(
        f"""
        SELECT
            model_date,
            AVG({value_column}) AS daily_value
        FROM {table_name}
        GROUP BY model_date
        ORDER BY model_date ASC
        """
    ).fetchall()

    result = {}
    for row in rows:
        result[row["model_date"]] = round(float(row["daily_value"] or 0.0), 4)
    return result


def rebuild_expected_activity_hourly() -> None:
    conn = get_db_connection()
    try:
        cursor = conn.cursor()

        flowering_map = build_daily_index_map(cursor, "flowering_model_daily", "flowering_index")
        nectar_map = build_daily_index_map(cursor, "nectar_supply_model_daily", "nectar_supply_index")

        cursor.execute("DROP TABLE IF EXISTS expected_activity_hourly")
        cursor.execute(
            """
            CREATE TABLE expected_activity_hourly (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                aligned_time TEXT UNIQUE,
                aligned_date TEXT,
                hour INTEGER,
                temperature_c REAL,
                humidity_pct REAL,
                wind_speed_ms REAL,
                precip_mm REAL,
                base_activity REAL,
                temp_factor REAL,
                humidity_factor REAL,
                wind_factor REAL,
                rain_factor REAL,
                weather_modifier REAL,
                daily_flowering_index REAL,
                daily_nectar_supply_index REAL,
                flower_factor REAL,
                nectar_factor REAL,
                resource_factor REAL,
                expected_activity REAL,
                actual_activity REAL,
                created_at TEXT
            )
            """
        )

        rows = cursor.execute(
            """
            SELECT
                aligned_time,
                aligned_date,
                AVG(avg_activity_value) AS avg_activity_value,
                AVG(temperature_c) AS temperature_c,
                AVG(humidity_pct) AS humidity_pct,
                AVG(wind_speed_ms) AS wind_speed_ms,
                AVG(precip_mm) AS precip_mm
            FROM bee_env_aligned_hourly
            GROUP BY aligned_time, aligned_date
            ORDER BY aligned_time ASC
            """
        ).fetchall()

        max_actual_raw = max(
            [float(row["avg_activity_value"] or 0.0) for row in rows],
            default=1.0,
        )
        if max_actual_raw <= 0:
            max_actual_raw = 1.0

        inserted = 0
        for row in rows:
            aligned_time = row["aligned_time"]
            aligned_date = row["aligned_date"]
            hour = int(aligned_time[11:13])

            b = base_activity(hour)
            tf = temp_factor(row["temperature_c"])
            hf = humidity_factor(row["humidity_pct"])
            wf = wind_factor(row["wind_speed_ms"])
            rf = rain_factor(row["precip_mm"])

            weather_modifier = round(0.4 * tf + 0.2 * hf + 0.2 * wf + 0.2 * rf, 4)

            daily_flowering_index = flowering_map.get(aligned_date, 0.0)
            daily_nectar_supply_index = nectar_map.get(aligned_date, 0.0)
            flower_factor = round(0.5 + 0.5 * clamp(daily_flowering_index), 4)
            nectar_factor = round(0.5 + 0.5 * clamp(daily_nectar_supply_index), 4)
            resource_factor = round(0.5 * flower_factor + 0.5 * nectar_factor, 4)

            if b == 0.0 or tf == 0.0 or wf == 0.0 or rf == 0.0:
                expected_activity = 0.0
            else:
                expected_activity = round(b * weather_modifier * resource_factor, 4)

            act_raw = row["avg_activity_value"]
            actual_activity = (
                round(clamp(float(act_raw) / max_actual_raw), 4)
                if act_raw is not None else None
            )

            cursor.execute(
                """
                INSERT INTO expected_activity_hourly (
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
                    actual_activity,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    aligned_time,
                    aligned_date,
                    hour,
                    row["temperature_c"],
                    row["humidity_pct"],
                    row["wind_speed_ms"],
                    row["precip_mm"],
                    b,
                    tf,
                    hf,
                    wf,
                    rf,
                    weather_modifier,
                    daily_flowering_index,
                    daily_nectar_supply_index,
                    flower_factor,
                    nectar_factor,
                    resource_factor,
                    expected_activity,
                    actual_activity,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )
            inserted += 1

        conn.commit()
        print(f"expected_activity_hourly 重建完成：{inserted} 条")
    finally:
        conn.close()


if __name__ == "__main__":
    rebuild_expected_activity_hourly()
