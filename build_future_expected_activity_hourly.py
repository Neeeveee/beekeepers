# -*- coding: utf-8 -*-

import sqlite3
import math
from pathlib import Path
from datetime import datetime

DB_PATH = r"D:\homeworks\workshop\s7-8\bee-project\bee_env.db"


def get_conn() -> sqlite3.Connection:
    db_file = Path(DB_PATH)
    if not db_file.exists():
        raise FileNotFoundError(f"数据库文件不存在：{DB_PATH}")
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout = 30000;")
    return conn


def clamp(value: float, min_value: float = 0.0, max_value: float = 1.0) -> float:
    return max(min_value, min(max_value, value))


def base_activity(hour: int) -> float:
    # 加入昼夜硬阻断：晚上 20:00 到 凌晨 5:00 活跃度强制为 0
    if hour < 6 or hour > 19:
        return 0.0
    
    # 使用高斯钟型曲线替代生硬的阶梯，实现平滑过渡
    activity = math.exp(-((hour - 13.0)**2) / 12.0)
    return round(activity, 4)


def temp_factor(t: float | None) -> float:
    if t is None:
        return 0.0
    # 加入低温硬阻断：低于 8℃ 蜜蜂不出巢，强制为 0
    if t < 8:
        return 0.0
    elif t < 10:
        return 0.10
    elif t < 14:
        return 0.40
    elif t < 20:
        return 0.75
    elif t <= 30:
        return 1.00
    elif t <= 35:
        return 0.80
    else:
        return 0.50


def humidity_factor(h: float | None) -> float:
    if h is None:
        return 0.0
    if 40 <= h <= 75:
        return 1.00
    elif h <= 85:
        return 0.90
    else:
        return 0.75


def wind_factor(w: float | None) -> float:
    if w is None:
        return 0.0
    if w < 1.5:
        return 1.00
    elif w < 3:
        return 0.90
    elif w < 5:
        return 0.70
    elif w <= 6.7:
        return 0.45
    else:
        # 大风天（大于5级，约 8m/s 以上）基本停飞
        return 0.0


def rain_factor(r: float | None) -> float:
    if r is None:
        return 1.0
    if r == 0:
        return 1.0
    elif r < 1:
        return 0.7
    elif r < 5:
        return 0.3
    else:
        # 如果是大雨及以上，强制为 0
        return 0.0


def ensure_table(cur: sqlite3.Cursor) -> None:
    cur.execute("""
    CREATE TABLE IF NOT EXISTS future_expected_activity_hourly (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        forecast_time TEXT UNIQUE,
        forecast_date TEXT,
        temperature_c REAL,
        humidity_pct REAL,
        wind_speed_raw REAL,
        wind_speed_ms REAL,
        precip_mm REAL,
        expected_activity REAL,
        source_file TEXT,
        created_at TEXT
    )
    """)


def main() -> None:
    conn = get_conn()
    try:
        cur = conn.cursor()

        ensure_table(cur)

        # 先清空旧预测，避免旧日期残留
        cur.execute("DELETE FROM future_expected_activity_hourly")

        # measurements 表里实际字段是 timestamp，不是 event_time
        rows = cur.execute("""
            SELECT
                timestamp,
                temperature_c,
                humidity_pct,
                wind_speed_ms,
                precip_mm,
                raw_source
            FROM measurements
            WHERE raw_source = 'qweather-24h'
              AND timestamp > datetime('now', 'localtime')
            ORDER BY timestamp ASC
        """).fetchall()

        inserted = 0

        # 定义插入语句
        insert_sql = """
            INSERT INTO future_expected_activity_hourly (
                forecast_time,
                forecast_date,
                temperature_c,
                humidity_pct,
                wind_speed_raw,
                wind_speed_ms,
                precip_mm,
                expected_activity,
                source_file,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        for row in rows:
            forecast_time = row["timestamp"]
            forecast_date = forecast_time[:10]

            temperature_c = row["temperature_c"]
            humidity_pct = row["humidity_pct"]
            wind_speed_ms = row["wind_speed_ms"]
            precip_mm = row["precip_mm"]
            source_file = row["raw_source"]

            hour = int(forecast_time[11:13])

            b = base_activity(hour)
            tf = temp_factor(temperature_c)
            hf = humidity_factor(humidity_pct)
            wf = wind_factor(wind_speed_ms)
            rf = rain_factor(precip_mm) 

            # 与历史行为模型保持一致：天气用加权平均 (加入降雨权重)
            weather_modifier = 0.4 * tf + 0.2 * hf + 0.2 * wf + 0.2 * rf

            # 判断是否有极端条件触发了一票否决（因子为0）
            if b == 0.0 or tf == 0.0 or wf == 0.0 or rf == 0.0:
                expected_activity = 0.0  # 夜间、极低温、大风、大雨直接让活动归零
            else:
                # 移除了 300 的倍数，让数值保持在 0-1 的指数范围内
                expected_activity = round(b * weather_modifier, 4)

            cur.execute(
                insert_sql,
                (
                    forecast_time,
                    forecast_date,
                    temperature_c,
                    humidity_pct,
                    None,
                    wind_speed_ms,
                    precip_mm,
                    expected_activity,
                    source_file,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                )
            )

            inserted += 1

        conn.commit()
        print(f"future_expected_activity_hourly 更新完成：新增 {inserted} 条")

    finally:
        conn.close()


if __name__ == "__main__":
    main()