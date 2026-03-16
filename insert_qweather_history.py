# -*- coding: utf-8 -*-

import json
import sqlite3
from pathlib import Path

DB_PATH = r"D:\homeworks\workshop\s7-8\bee-project\bee_env.db"
JSON_PATH = r"D:\homeworks\workshop\s7-8\bee-project\data_raw\qweather_history_20260307_20260308_160150.json"

# 这里先用你库里“和风天气逐小时”那个传感器编号
QWEATHER_SENSOR_ID = 3


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout = 30000;")
    return conn


def main() -> None:
    json_file = Path(JSON_PATH)
    if not json_file.exists():
        raise FileNotFoundError(f"找不到 JSON 文件：{JSON_PATH}")

    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    # 官方历史天气接口通常返回 weatherHourly 列表
    hourly_list = data.get("weatherHourly", [])
    if not hourly_list:
        print("没有找到 weatherHourly，请检查 JSON 结构。")
        print("顶层 keys =", list(data.keys()))
        return

    conn = get_conn()
    try:
        cur = conn.cursor()
        inserted = 0
        skipped = 0

        for item in hourly_list:
            obs_time = item.get("fxTime") or item.get("obsTime") or item.get("time")
            temperature_c = item.get("temp")
            humidity_pct = item.get("humidity")
            wind_speed_ms = item.get("windSpeed")
            precip_mm = item.get("precip")
            pressure_hpa = item.get("pressure")

            if not obs_time:
                continue

            # 统一成数据库里更接近的时间格式：YYYY-MM-DD HH:MM:SS
            ts = obs_time.replace("T", " ").replace("+08:00", "")
            if len(ts) == 16:
                ts = ts + ":00"

            cur.execute(
                """
                INSERT OR IGNORE INTO measurements
                (
                    sensor_id,
                    timestamp,
                    temperature_c,
                    humidity_pct,
                    wind_speed_ms,
                    precip_mm,
                    pressure_hpa,
                    raw_source
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    QWEATHER_SENSOR_ID,
                    ts,
                    float(temperature_c) if temperature_c not in (None, "") else None,
                    float(humidity_pct) if humidity_pct not in (None, "") else None,
                    float(wind_speed_ms) if wind_speed_ms not in (None, "") else None,
                    float(precip_mm) if precip_mm not in (None, "") else None,
                    float(pressure_hpa) if pressure_hpa not in (None, "") else None,
                    "qweather-history",
                ),
            )

            if cur.rowcount == 0:
                skipped += 1
            else:
                inserted += 1

        conn.commit()
        print(f"历史天气导入完成：新增 {inserted} 条，跳过 {skipped} 条")

    finally:
        conn.close()


if __name__ == "__main__":
    main()