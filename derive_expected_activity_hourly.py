# -*- coding: utf-8 -*-
import sqlite3
import math
from pathlib import Path
from datetime import datetime

DB_PATH = r"D:\homeworks\workshop\s7-8\bee-project\bee_env.db"

def get_db_connection():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def base_activity(hour: int) -> float:
    if hour < 6 or hour > 19: return 0.0
    # 使用高斯函数实现丝滑的昼夜曲线
    return round(math.exp(-((hour - 13.0)**2) / 12.0), 4)

def rebuild_expected_activity_hourly():
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS expected_activity_hourly")
        cursor.execute("""
            CREATE TABLE expected_activity_hourly (
                id INTEGER PRIMARY KEY AUTOINCREMENT, aligned_time TEXT UNIQUE, aligned_date TEXT,
                hour INTEGER, temperature_c REAL, humidity_pct REAL, wind_speed_ms REAL, precip_mm REAL,
                base_activity REAL, temp_factor REAL, humidity_factor REAL, wind_factor REAL, rain_factor REAL,
                weather_modifier REAL, daily_flowering_index REAL, daily_nectar_supply_index REAL,
                flower_factor REAL, nectar_factor REAL, resource_factor REAL,
                expected_activity REAL, actual_activity REAL, created_at TEXT
            )
        """)

        # 查询并计算
        rows = cursor.execute("SELECT * FROM bee_env_aligned_hourly ORDER BY aligned_time ASC").fetchall()
        for row in rows:
            # 此处省略中间因子计算，直接展示关键缩放逻辑
            # ... (保持之前提供的 temp_factor 等函数不变)
            
            # 关键：将实测值归一化到 0-1
            act_raw = row['avg_activity_value']
            actual_norm = round(float(act_raw) / 60.0, 4) if act_raw is not None else None
            
            # ... (执行插入语句)
        conn.commit()
    finally: conn.close()