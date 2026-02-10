import sqlite3
import json
import glob
from datetime import datetime

DB_PATH = "bee_env.db"


def normalize_ts(fx_time: str) -> str:
    """
    把 fxTime 统一转换成 'YYYY-MM-DD HH:MM:SS'
    例如:
      2025-12-09T15:00+08:00 -> 2025-12-09 15:00:00
      2025-12-09T15:00      -> 2025-12-09 15:00:00
    """
    # 1) 去掉时区部分
    no_tz = fx_time.split("+")[0]  # "2025-12-09T15:00"
    # 2) 替换 T 为空格
    s = no_tz.replace("T", " ")    # "2025-12-09 15:00"
    # 3) 补秒
    if len(s) == 16:
        s += ":00"                 # "2025-12-09 15:00:00"
    return s


def insert_qweather_json(filename):
    with open(filename, "r", encoding="utf-8") as f:
        data = json.load(f)

    hourly = data.get("hourly", [])
    if not hourly:
        print(f"文件 {filename} 没有 hourly 数据，跳过。")
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # 找到 qweather 对应的 sensor
    cur.execute("SELECT id FROM sensors WHERE source = 'qweather'")
    row = cur.fetchone()
    if not row:
        conn.close()
        raise ValueError("没有 source='qweather' 的传感器，请在 sensors 表中先建一个。")
    sensor_id = row[0]

    insert_count = 0
    skip_count = 0

    for h in hourly:
        raw_ts = h["fxTime"]  # 例如 "2025-12-09T15:00+08:00"
        ts = normalize_ts(raw_ts)

        # 防重复：按 (sensor_id, timestamp) 判断
        cur.execute(
            "SELECT COUNT(*) FROM measurements WHERE sensor_id = ? AND timestamp = ?",
            (sensor_id, ts),
        )
        if cur.fetchone()[0] > 0:
            skip_count += 1
            continue  # 这条时间已经有了，不再插入

        temp = float(h["temp"])
        humidity = float(h["humidity"])
        pressure = float(h["pressure"])
        wind_kmh = float(h["windSpeed"])
        wind_ms = wind_kmh / 3.6

        cur.execute(
            """
            INSERT INTO measurements
            (sensor_id, timestamp,
             temperature_c, humidity_pct, pressure_hpa, wind_speed_ms,
             raw_source, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now', 'localtime'))
            """,
            (
                sensor_id,
                ts,
                temp,
                humidity,
                pressure,
                wind_ms,
                "qweather-24h",
            ),
        )
        insert_count += 1

    conn.commit()
    conn.close()

    print(
        f"🌤 从 {filename} 写入 {insert_count} 条新记录，"
        f"跳过 {skip_count} 条已存在的时间。"
    )


def main():
    files = sorted(glob.glob("data_raw/qweather_*.json"))
    if not files:
        print("未找到 qweather JSON 文件，请先运行 fetch_qweather_24h.py")
        return

    latest = files[-1]
    print("正在导入：", latest)
    insert_qweather_json(latest)


if __name__ == "__main__":
    main()
