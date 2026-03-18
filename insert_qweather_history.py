# -*- coding: utf-8 -*-

import glob
import json
import sqlite3
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "bee_env.db"
DATA_GLOB = str(BASE_DIR / "data_raw" / "qweather_history_*.json")


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout = 30000;")
    return conn


def normalize_ts(raw_ts: str) -> str:
    ts = raw_ts.split("+")[0].replace("T", " ")
    if len(ts) == 16:
        ts += ":00"
    return ts


def get_qweather_sensor_id(cur: sqlite3.Cursor) -> int:
    row = cur.execute(
        "SELECT id FROM sensors WHERE source = 'qweather' ORDER BY id LIMIT 1"
    ).fetchone()
    if not row:
        raise ValueError("没有 source='qweather' 的传感器，无法导入历史天气。")
    return int(row[0])


def import_history_file(json_path: str) -> tuple[int, int]:
    payload = json.loads(Path(json_path).read_text(encoding="utf-8"))
    hourly_list = payload.get("weatherHourly", [])
    if not hourly_list:
        print(f"[WARN] {Path(json_path).name} 未找到 weatherHourly，跳过。")
        return 0, 0

    conn = get_conn()
    try:
        cur = conn.cursor()
        sensor_id = get_qweather_sensor_id(cur)
        inserted = 0
        skipped = 0

        for item in hourly_list:
            obs_time = item.get("fxTime") or item.get("obsTime") or item.get("time")
            if not obs_time:
                continue

            ts = normalize_ts(obs_time)
            cur.execute(
                """
                SELECT COUNT(*)
                FROM measurements
                WHERE sensor_id = ? AND timestamp = ?
                """,
                (sensor_id, ts),
            )
            if cur.fetchone()[0] > 0:
                skipped += 1
                continue

            wind_kmh = float(item["windSpeed"]) if item.get("windSpeed") not in (None, "") else None
            wind_ms = round(wind_kmh / 3.6, 4) if wind_kmh is not None else None

            cur.execute(
                """
                INSERT INTO measurements
                (
                    sensor_id,
                    timestamp,
                    temperature_c,
                    humidity_pct,
                    wind_speed_ms,
                    precip_mm,
                    pressure_hpa,
                    raw_source,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now', 'localtime'))
                """,
                (
                    sensor_id,
                    ts,
                    float(item["temp"]) if item.get("temp") not in (None, "") else None,
                    float(item["humidity"]) if item.get("humidity") not in (None, "") else None,
                    wind_ms,
                    float(item["precip"]) if item.get("precip") not in (None, "") else None,
                    float(item["pressure"]) if item.get("pressure") not in (None, "") else None,
                    "qweather-history",
                ),
            )
            inserted += 1

        conn.commit()
        print(f"[OK] {Path(json_path).name} 导入完成：新增 {inserted} 条，跳过 {skipped} 条。")
        return inserted, skipped
    finally:
        conn.close()


def main() -> None:
    files = sorted(glob.glob(DATA_GLOB))
    if not files:
        print("[INFO] 未找到 qweather_history_*.json，跳过历史天气导入。")
        return

    total_insert = 0
    total_skip = 0
    for file_path in files:
        inserted, skipped = import_history_file(file_path)
        total_insert += inserted
        total_skip += skipped

    print(f"[INFO] 历史天气导入结束：新增 {total_insert} 条，跳过 {total_skip} 条。")


if __name__ == "__main__":
    main()
