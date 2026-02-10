
import sqlite3  # 导入SQLite模块
from datetime import datetime  # 导入时间模块

DB_PATH = "bee_env.db"  # 数据库文件名（默认与脚本同目录）

def main():  # 主函数
    conn = sqlite3.connect(DB_PATH)  # 连接数据库
    cur = conn.cursor()  # 创建游标

    # ========== 0) 记录数据库结构版本（以后不改也没关系，但方便你确认“这套是最终版”）==========
    cur.execute("""  -- 创建版本表（如果不存在）
    CREATE TABLE IF NOT EXISTS schema_version (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        module TEXT UNIQUE,
        version TEXT,
        updated_at TEXT
    );
    """)  # 执行SQL

    # ========== 1) 每日天气汇总表（给“规律推导”用，不和你现有 daily_env_indices 冲突）==========
    cur.execute("""  -- 创建每日天气汇总表
    CREATE TABLE IF NOT EXISTS daily_weather_summary (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        site_id INTEGER NOT NULL,
        date TEXT NOT NULL,                 -- YYYY-MM-DD
        day_avg_temp_c REAL,                -- 白天平均温度（8-18）
        night_avg_temp_c REAL,              -- 夜间平均温度
        day_avg_humidity_pct REAL,          -- 白天平均湿度
        day_avg_wind_ms REAL,               -- 白天平均风速
        day_sum_precip_mm REAL,             -- 当天降水累计
        day_avg_pressure_hpa REAL,          -- 白天平均气压
        micro_temp_range_c REAL,            -- 当天温差（max-min）
        created_at TEXT DEFAULT (datetime('now', 'localtime')),
        UNIQUE(site_id, date)               -- 关键：保证同一天只会有一条（可重复跑脚本）
    );
    """)  # 执行SQL

    # ========== 2) 蜂种表（可选，但建议有：方便统一写法）==========
    cur.execute("""  -- 创建蜂种表
    CREATE TABLE IF NOT EXISTS bee_species (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT UNIQUE,                   -- 例如: CHINESE_BEE
        name_zh TEXT,                       -- 例如: 中华蜂
        notes TEXT
    );
    """)  # 执行SQL

    # ========== 3) 蜂群活跃度观测表（你以后接传感器/人工记录都往这里写）==========
    cur.execute("""  -- 创建活跃度观测表
    CREATE TABLE IF NOT EXISTS bee_activity_obs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        site_id INTEGER NOT NULL,
        species_code TEXT NOT NULL,         -- 对应 bee_species.code（为了简单用文本，不强制外键）
        obs_time TEXT NOT NULL,             -- YYYY-MM-DD HH:MM:SS
        activity_index REAL,                -- 活跃度指标（建议统一 0~100）
        source TEXT,                        -- 数据来源：manual / camera / counter / weight 等
        note TEXT,
        created_at TEXT DEFAULT (datetime('now', 'localtime')),
        UNIQUE(site_id, species_code, obs_time)  -- 防止同一时间重复写入
    );
    """)  # 执行SQL

    # ========== 4) 场地“已得行为规律库”（你的核心：环境 → 预期活跃范围/曲线参数）==========
    cur.execute("""  -- 创建行为规律库
    CREATE TABLE IF NOT EXISTS behavior_rule_library (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        site_id INTEGER NOT NULL,
        species_code TEXT NOT NULL,         -- 蜂种代码
        month_from INTEGER,                 -- 生效月份起(1-12，可空)
        month_to INTEGER,                   -- 生效月份止(1-12，可空)
        temp_min REAL,                      -- 温度下限(℃)
        temp_max REAL,                      -- 温度上限(℃)
        rh_min REAL,                        -- 湿度下限(%)
        rh_max REAL,                        -- 湿度上限(%)
        precip_max REAL,                    -- 降水上限(mm)，超过则通常活跃受抑制
        wind_max REAL,                      -- 风速上限(m/s)
        expected_min REAL NOT NULL,         -- 预期活跃下界（0~100）
        expected_max REAL NOT NULL,         -- 预期活跃上界（0~100）
        peak_start_hour INTEGER,            -- 峰值开始小时(0-23)
        peak_end_hour INTEGER,              -- 峰值结束小时(0-23)
        confidence REAL DEFAULT 0.6,        -- 置信度(0-1)
        source TEXT,                        -- 来源：实地总结/文献/专家
        explain_tpl TEXT,                   -- 一句话解释模板：支持{temp}{rh}{rain}{wind}
        created_at TEXT DEFAULT (datetime('now', 'localtime'))
    );
    """)  # 执行SQL

    # ========== 5) 每日“预期输出”（你说的：输入环境 → 输出一个范围/结果/曲线参数）==========
    cur.execute("""  -- 创建每日预期输出表
    CREATE TABLE IF NOT EXISTS expected_activity_daily (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        site_id INTEGER NOT NULL,
        species_code TEXT NOT NULL,
        date TEXT NOT NULL,                 -- YYYY-MM-DD
        env_temp REAL,
        env_rh REAL,
        env_rain REAL,
        env_wind REAL,
        rule_id INTEGER,                    -- 命中的规律ID
        expected_min REAL,
        expected_max REAL,
        peak_start_hour INTEGER,
        peak_end_hour INTEGER,
        confidence REAL,
        explain_text TEXT,
        created_at TEXT DEFAULT (datetime('now', 'localtime')),
        UNIQUE(site_id, species_code, date) -- 允许重复跑脚本（自动覆盖/更新）
    );
    """)  # 执行SQL

    # ========== 6) 每日“验证输出”（你说的：看是否和实地传感器一致）==========
    cur.execute("""  -- 创建每日验证结果表
    CREATE TABLE IF NOT EXISTS validation_activity_daily (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        site_id INTEGER NOT NULL,
        species_code TEXT NOT NULL,
        date TEXT NOT NULL,
        observed_activity REAL,             -- 实测活跃度（日聚合）
        expected_min REAL,
        expected_max REAL,
        match_score REAL,                   -- 0~1
        deviation_tag TEXT,                 -- within_expected / below_expected / above_expected / no_data
        deviation_value REAL,
        explain_text TEXT,
        created_at TEXT DEFAULT (datetime('now', 'localtime')),
        UNIQUE(site_id, species_code, date)
    );
    """)  # 执行SQL

    # ========== 7) 写入版本号（告诉未来的你：这套结构已经“定版”）==========
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # 当前时间
    cur.execute(  # 写入/更新版本
        "INSERT INTO schema_version(module, version, updated_at) VALUES(?,?,?) "
        "ON CONFLICT(module) DO UPDATE SET version=excluded.version, updated_at=excluded.updated_at;",
        ("behavior_module", "v1.0-final", now),
    )  # 执行SQL

    conn.commit()  # 提交
    conn.close()  # 关闭
    print("✅ 扩展表创建完成：daily_weather_summary + behavior_module 已就绪")  # 提示

if __name__ == "__main__":  # 入口
    main()  # 运行主函数
