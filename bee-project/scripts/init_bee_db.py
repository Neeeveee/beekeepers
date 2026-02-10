import sqlite3
from datetime import datetime

# 创建或连接数据库文件（文件名：bee_env.db）
conn = sqlite3.connect("bee_env.db")
cur = conn.cursor()

# 1. 地点表：存各个蜂场 / 观测点
cur.execute("""
CREATE TABLE IF NOT EXISTS sites (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT UNIQUE,
    name TEXT,
    latitude REAL,
    longitude REAL,
    elevation_m REAL,
    description TEXT
);
""")

# 2. 传感器表：官方站、微气象站等
cur.execute("""
CREATE TABLE IF NOT EXISTS sensors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site_id INTEGER,
    name TEXT,
    source TEXT,
    sensor_type TEXT,
    install_date TEXT,
    notes TEXT,
    FOREIGN KEY (site_id) REFERENCES sites(id)
);
""")

# 3. 原始气象测量表：温度、湿度、降水、风速等
cur.execute("""
CREATE TABLE IF NOT EXISTS measurements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sensor_id INTEGER,
    timestamp TEXT,
    temperature_c REAL,
    humidity_pct REAL,
    wind_speed_ms REAL,
    precip_mm REAL,
    pressure_hpa REAL,
    solar_wm2 REAL,
    soil_moisture_pct REAL,
    raw_source TEXT,
    created_at TEXT,
    FOREIGN KEY (sensor_id) REFERENCES sensors(id)
);
""")

# 4. 日尺度指标：日平均温度、夜温、蒸发指数、土壤等级等
cur.execute("""
CREATE TABLE IF NOT EXISTS daily_env_indices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site_id INTEGER,
    date TEXT,
    day_avg_temp_c REAL,
    night_avg_temp_c REAL,
    evap_index REAL,
    shade_sun_ratio REAL,
    micro_temp_range_c REAL,
    soil_moisture_level INTEGER,
    wind_channel_index REAL,
    created_at TEXT,
    FOREIGN KEY (site_id) REFERENCES sites(id)
);
""")

# 5. 植物物种表
cur.execute("""
CREATE TABLE IF NOT EXISTS plants (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    species_code TEXT UNIQUE,
    scientific_name TEXT,
    common_name_zh TEXT,
    notes TEXT
);
""")

# 6. 植物物候表：开花时间、花期长度、花蜜指数等
cur.execute("""
CREATE TABLE IF NOT EXISTS plant_phenology (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site_id INTEGER,
    plant_id INTEGER,
    year INTEGER,
    flowering_start TEXT,
    flowering_end TEXT,
    nectar_index REAL,
    pollen_viability_index REAL,
    data_source TEXT,
    notes TEXT,
    created_at TEXT,
    FOREIGN KEY (site_id) REFERENCES sites(id),
    FOREIGN KEY (plant_id) REFERENCES plants(id)
);
""")

conn.commit()
conn.close()

print("数据库创建成功：bee_env.db 已生成")
