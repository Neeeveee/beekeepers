# -*- coding: utf-8 -*-

import glob
import json
import sqlite3
import time
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "bee_env.db"
DATA_GLOB = str(BASE_DIR / "data_raw" / "qweather_24h_*.json")
MAX_RETRIES = 3
RETRY_DELAY_SEC = 1.0


def get_conn() -> sqlite3.Connection:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"数据库文件不存在：{DB_PATH}")

    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout = 30000;")
    return conn


def normalize_ts(fx_time: str) -> str:
    no_tz = fx_time.split("+", 1)[0]
    value = no_tz.replace("T", " ")
    if len(value) == 16:
        value += ":00"
    return value


def get_qweather_sensor_id(cur: sqlite3.Cursor) -> int:
    cur.execute("SELECT id FROM sensors WHERE source = 'qweather' ORDER BY id LIMIT 1")
    row = cur.fetchone()
    if not row:
        raise ValueError("没有 source='qweather' 的传感器，请先在 sensors 表中建立一条。")
    return row[0]


def insert_qweather_json(filename: str) -> tuple[int, int]:
    with open(filename, "r", encoding="utf-8") as f:
        data = json.load(f)

    hourly = data.get("hourly", [])
    if not hourly:
        print(f"文件 {filename} 没有 hourly 数据，跳过。")
        return 0, 0

    last_error: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        conn = None
        try:
            conn = get_conn()
            cur = conn.cursor()
            sensor_id = get_qweather_sensor_id(cur)

            insert_count = 0
            skip_count = 0

            for h in hourly:
                raw_ts = h.get("fxTime")
                if not raw_ts:
                    continue

                ts = normalize_ts(raw_ts)
                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM measurements
                    WHERE sensor_id = ? AND timestamp = ?
                    """,
                    (sensor_id, ts),
                )
                if cur.fetchone()[0] > 0:
                    skip_count += 1
                    continue

                temp = float(h["temp"]) if h.get("temp") is not None else None
                humidity = float(h["humidity"]) if h.get("humidity") is not None else None
                pressure = float(h["pressure"]) if h.get("pressure") is not None else None
                wind_kmh = float(h["windSpeed"]) if h.get("windSpeed") is not None else None
                wind_ms = wind_kmh / 3.6 if wind_kmh is not None else None
                precip = float(h["precip"]) if h.get("precip") is not None else 0.0

                cur.execute(
                    """
                    INSERT INTO measurements (
                        sensor_id,
                        timestamp,
                        temperature_c,
                        humidity_pct,
                        pressure_hpa,
                        wind_speed_ms,
                        precip_mm,
                        raw_source,
                        created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now', 'localtime'))
                    """,
                    (
                        sensor_id,
                        ts,
                        temp,
                        humidity,
                        pressure,
                        wind_ms,
                        precip,
                        "qweather-24h",
                    ),
                )
                insert_count += 1

            conn.commit()
            print(f"🌤 从 {filename} 写入 {insert_count} 条新记录，跳过 {skip_count} 条已存在时间。")
            return insert_count, skip_count
        except sqlite3.OperationalError as exc:
            last_error = exc
            # Some local tools keep a short-lived handle on the db; retry a few times.
            if conn is not None:
                conn.rollback()
            if "readonly" not in str(exc).lower() and "locked" not in str(exc).lower():
                raise
            if attempt == MAX_RETRIES:
                raise
            print(f"[WARN] 写库失败，第 {attempt} 次重试：{exc}")
            time.sleep(RETRY_DELAY_SEC * attempt)
        finally:
            if conn is not None:
                conn.close()

    raise RuntimeError(f"写入 {filename} 失败：{last_error}")


def main() -> None:
    files = sorted(glob.glob(DATA_GLOB))
    if not files:
        print(f"未找到天气文件：{DATA_GLOB}")
        print("请先运行 fetch_qweather_24h.py 生成 24h 天气 JSON。")
        return

    print(f"共找到 {len(files)} 个 24h 天气文件。")
    total_insert = 0
    total_skip = 0

    for fp in files:
        print("正在导入：", Path(fp).relative_to(BASE_DIR))
        inserted, skipped = insert_qweather_json(fp)
        total_insert += inserted
        total_skip += skipped

    print("--------------------------------------------------")
    print(f"全部导入完成：新增 {total_insert} 条，跳过 {total_skip} 条。")


if __name__ == "__main__":
    main()
