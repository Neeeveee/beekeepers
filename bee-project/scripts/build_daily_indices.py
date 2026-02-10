import sqlite3
from datetime import datetime
from collections import defaultdict

DB_FILE = "bee_env.db"


def main():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    # 1. 直接由脚本负责：删掉旧表 + 按正确字段重新创建
    cur.executescript(
        """
        DROP TABLE IF EXISTS daily_env_indices;

        CREATE TABLE daily_env_indices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            day_avs_temp_c REAL,
            night_avs_temp_c REAL,
            evap_index REAL,
            shade_sun_ratio REAL,
            micro_temp_range_c REAL,
            created_at TEXT DEFAULT (datetime('now', 'localtime'))
        );
        """
    )
    conn.commit()
    print("已重新创建 daily_env_indices 表（结构已统一）。")

    # 2. 读取 measurements + sensors（只用和风天气的数据）
    cur.execute(
        """
        SELECT
            m.timestamp,
            m.temperature_c,
            m.humidity_pct,
            m.wind_speed_ms,
            s.site_id,
            s.source
        FROM measurements m
        JOIN sensors s ON m.sensor_id = s.id
        WHERE m.temperature_c IS NOT NULL
          AND (s.source = 'qweather' OR s.source IS NULL OR s.source = '')
        ORDER BY s.site_id, m.timestamp;
        """
    )

    rows = cur.fetchall()
    if not rows:
        print("measurements 里没有可用的和风天气数据，请先跑 insert_qweather_data.py。")
        conn.close()
        return

    # 3. 按 (site_id, date) 分组
    groups = defaultdict(
        lambda: {
            "temps_all": [],
            "day_temps": [],
            "night_temps": [],
            "day_humids": [],
            "day_winds": [],
        }
    )

    for ts_str, temp_c, hum, wind_ms, site_id, source in rows:
        if not ts_str:
            continue
        try:
            dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            try:
                dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M")
            except ValueError:
                continue

        date_str = dt.date().isoformat()
        hour = dt.hour

        g = groups[(site_id, date_str)]

        if temp_c is not None:
            t = float(temp_c)
            g["temps_all"].append(t)

            # 白天：8:00–18:00
            if 8 <= hour < 18:
                g["day_temps"].append(t)
                if hum is not None:
                    g["day_humids"].append(float(hum))
                if wind_ms is not None:
                    g["day_winds"].append(float(wind_ms))
            else:
                g["night_temps"].append(t)

    def avg(lst):
        return sum(lst) / len(lst) if lst else None

    insert_count = 0

    # 4. 计算每一天的指标并写入 daily_env_indices
    for (site_id, date_str), g in groups.items():
        temps_all = g["temps_all"]
        day_temps = g["day_temps"]
        night_temps = g["night_temps"]
        day_humids = g["day_humids"]
        day_winds = g["day_winds"]

        if not temps_all:
            continue

        # 日平均温度（如果有白天温度就用白天，否则用全天）
        day_avs_temp_c = avg(day_temps) if day_temps else avg(temps_all)

        # 夜间平均温度
        night_avs_temp_c = avg(night_temps)

        # 温度日较差
        micro_temp_range_c = (
            max(temps_all) - min(temps_all) if len(temps_all) >= 2 else None
        )

        # 简单蒸发指数
        evap_index = None
        if day_temps and day_humids:
            evap_values = []
            for t, h, w in zip(
                day_temps,
                day_humids,
                day_winds or [0.0] * len(day_temps),
            ):
                dryness = max(t, 0.0) * (100.0 - h) / 100.0
                wind_factor = 1.0 + (w or 0.0) / 5.0
                evap_values.append(dryness * wind_factor)
            evap_index = avg(evap_values) if evap_values else None

        shade_sun_ratio = None  # 先留空，将来接云量/辐射数据

        cur.execute(
            """
            INSERT INTO daily_env_indices
            (site_id, date, day_avs_temp_c, night_avs_temp_c,
             evap_index, shade_sun_ratio, micro_temp_range_c)
            VALUES (?, ?, ?, ?, ?, ?, ?);
            """,
            (
                site_id,
                date_str,
                day_avs_temp_c,
                night_avs_temp_c,
                evap_index,
                shade_sun_ratio,
                micro_temp_range_c,
            ),
        )

        insert_count += 1

    conn.commit()
    conn.close()
    print(f"已写入 {insert_count} 条 daily_env_indices 日指标记录。")


if __name__ == "__main__":
    main()
