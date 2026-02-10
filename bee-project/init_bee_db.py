import sqlite3  # 导入SQLite模块（用于操作本地数据库）
from datetime import datetime  # 导入时间模块（用于生成时间戳）

# =========================
# 数据库初始化脚本（安全模式）
# - 只做“创建缺失表/索引”，不删除任何已有表
# - 适用于：已有 bee_env.db 的项目直接补齐结构
# =========================

DB_FILE = "bee_env.db"  # 数据库文件名（默认与脚本同目录）


def main():  # 主函数入口
    conn = sqlite3.connect(DB_FILE)  # 连接/创建数据库
    cur = conn.cursor()  # 创建游标

    # -------------------------
    # 1) 基础表：蜂场 / 传感器 / 原始气象测量
    # -------------------------
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS sites (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE,
            name TEXT,
            latitude REAL,
            longitude REAL,
            elevation_m REAL,
            description TEXT
        );
        """
    )  # 站点表（蜂场/观测点）

    cur.execute(
        """
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
        """
    )  # 传感器表（官方站/微气象站/平台数据等）

    cur.execute(
        """
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
        """
    )  # 原始测量表（逐小时/逐10分钟等）

    # -------------------------
    # 2) 环境日指标表（注意：你们库里已存在 daily_env_indices，这里只补齐不存在的情况）
    # -------------------------
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_env_indices (
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
    )  # 日尺度环境指标（给后续推导/可视化使用）

    # 为 ON CONFLICT 准备唯一约束（SQLite 里用 UNIQUE INDEX 最稳妥）
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_daily_env_indices_site_date
        ON daily_env_indices(site_id, date);
        """
    )  # (site_id, date) 唯一

    # -------------------------
    # 3) 植物与物候表（你们库里已有 plants / plant_phenology，这里保持兼容）
    # -------------------------
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS plants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            species_code TEXT UNIQUE,
            scientific_name TEXT,
            common_name_zh TEXT,
            notes TEXT
        );
        """
    )  # 植物物种表

    cur.execute(
        """
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
        """
    )  # 植物物候表（花期、花蜜指标等）


    # -------------------------
    # 3.1) 新增：蜂场植被清单/权重表（用于后续 FRI/NAI 计算的事实层）
    # -------------------------
    cur.execute(  # 执行建表语句（如果不存在才创建）
        """
        CREATE TABLE IF NOT EXISTS site_plant_inventory (                      -- 蜂场-植被清单表（事实层）
            id INTEGER PRIMARY KEY AUTOINCREMENT,                              -- 自增主键
            site_id INTEGER NOT NULL,                                          -- 蜂场/站点ID（关联 sites.id）
            plant_id INTEGER NOT NULL,                                         -- 植物ID（关联 plants.id）

            coverage_class TEXT CHECK(coverage_class IN ('low','mid','high')), -- 覆盖等级：低/中/高（可先粗略填）
            coverage_pct REAL,                                                 -- 覆盖比例（可空：未来有数据再补）
            distance_km REAL,                                                  -- 大致距离（可空：未来补）
            within_3km INTEGER DEFAULT 1 CHECK(within_3km IN (0,1)),            -- 是否在3km内（默认是）

            importance_weight REAL NOT NULL DEFAULT 0.33 CHECK(importance_weight BETWEEN 0 AND 1), -- 重要性权重（蜂农经验）

            source TEXT CHECK(source IN ('online','interview','field','mixed')) DEFAULT 'online', -- 来源：线上/访谈/现场/混合
            notes TEXT,                                                        -- 备注（可写“占位/假设”等）
            created_at TEXT DEFAULT (datetime('now', 'localtime')),             -- 创建时间

            FOREIGN KEY (site_id) REFERENCES sites(id),                         -- 外键：站点
            FOREIGN KEY (plant_id) REFERENCES plants(id),                       -- 外键：植物

            UNIQUE(site_id, plant_id)                                          -- 同一蜂场同一植物只保留一条（便于更新）
        );
        """
    )  # 站点植被清单表创建结束

    cur.execute(  # 创建索引（加速按 site_id 查询）
        """
        CREATE INDEX IF NOT EXISTS ix_site_plant_inventory_site
        ON site_plant_inventory(site_id);
        """
    )  # 索引创建结束

    # -------------------------
    # 3.2) 新增：花期观察表（用于训练/校验花期模型与花蜜量指数）
    # -------------------------
    cur.execute(  # 执行建表语句（如果不存在才创建）
        """
        CREATE TABLE IF NOT EXISTS flowering_observations (                     -- 花期观察表（事实/标签层）
            id INTEGER PRIMARY KEY AUTOINCREMENT,                               -- 自增主键
            site_id INTEGER NOT NULL,                                           -- 蜂场/站点ID
            plant_id INTEGER NOT NULL,                                          -- 植物ID

            obs_date TEXT NOT NULL,                                             -- 观察日期（YYYY-MM-DD）
            stage INTEGER NOT NULL CHECK(stage IN (0,1,2,3)),                   -- 花期阶段：0无/未见 1初开 2盛开 3衰败
            intensity INTEGER CHECK(intensity BETWEEN 0 AND 5),                 -- 强度：0-5（可空）
            confidence REAL DEFAULT 0.6 CHECK(confidence BETWEEN 0 AND 1),      -- 置信度：0-1（默认0.6）

            observer TEXT,                                                      -- 观察者（你/蜂农/农户）
            photo_path TEXT,                                                    -- 照片路径/URL（可空）
            notes TEXT,                                                         -- 备注
            created_at TEXT DEFAULT (datetime('now', 'localtime')),             -- 创建时间

            FOREIGN KEY (site_id) REFERENCES sites(id),                         -- 外键：站点
            FOREIGN KEY (plant_id) REFERENCES plants(id),                       -- 外键：植物

            UNIQUE(site_id, plant_id, obs_date)                                 -- 同日同植物同站点唯一（便于更新）
        );
        """
    )  # 花期观察表创建结束

    cur.execute(  # 创建索引（加速按站点+日期查询）
        """
        CREATE INDEX IF NOT EXISTS ix_flowering_obs_site_date
        ON flowering_observations(site_id, obs_date);
        """
    )  # 索引创建结束

    # -------------------------
    # 4) 新增：三大核心指数（FRI / NAI / Activity）——派生指标层
    # -------------------------
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS flower_resource_index (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            fri_value INTEGER CHECK(fri_value BETWEEN 0 AND 100),
            data_level TEXT CHECK(data_level IN ('observed','inferred','predicted')),
            source_version TEXT,
            created_at TEXT DEFAULT (datetime('now', 'localtime')),
            FOREIGN KEY (site_id) REFERENCES sites(id)
        );
        """
    )  # 综合花源指数（0-100）

    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_fri_site_ts
        ON flower_resource_index(site_id, timestamp);
        """
    )  # (site_id, timestamp) 唯一，便于 UPSERT

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nectar_availability_index (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            nai_value INTEGER CHECK(nai_value BETWEEN 0 AND 100),
            weather_modifier REAL,
            data_level TEXT CHECK(data_level IN ('inferred','predicted')),
            source_version TEXT,
            created_at TEXT DEFAULT (datetime('now', 'localtime')),
            FOREIGN KEY (site_id) REFERENCES sites(id)
        );
        """
    )  # 可采蜜强度指数（0-100）

    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_nai_site_ts
        ON nectar_availability_index(site_id, timestamp);
        """
    )  # (site_id, timestamp) 唯一

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS activity_index (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            activity_value INTEGER CHECK(activity_value BETWEEN 0 AND 100),
            in_count INTEGER,
            out_count INTEGER,
            temperature REAL,
            humidity REAL,
            data_level TEXT CHECK(data_level IN ('observed','inferred')),
            source_version TEXT,
            created_at TEXT DEFAULT (datetime('now', 'localtime')),
            FOREIGN KEY (site_id) REFERENCES sites(id)
        );
        """
    )  # 蜂群活跃度指数（0-100）

    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_activity_site_ts
        ON activity_index(site_id, timestamp);
        """
    )  # (site_id, timestamp) 唯一

    # -------------------------
    # 5) schema_version（如果不存在则创建；如果存在则记录本次模块版本）
    # -------------------------
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_version (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            module TEXT,
            version TEXT,
            updated_at TEXT
        );
        """
    )  # 结构版本表（轻量追踪）

    # 写一条版本记录（简单起见：同模块多条记录也没关系；你要严格唯一我也可以改）
    cur.execute(
        """
        INSERT INTO schema_version (module, version, updated_at)
        VALUES (?, ?, datetime('now', 'localtime'));
        """,
        ("core_indices_module", "v0.1"),
    )  # 记录核心指数模块版本

    conn.commit()  # 提交
    conn.close()  # 关闭
    print("✅ 数据库初始化完成：已补齐核心指数表（FRI/NAI/Activity）与必要索引。")  # 提示


if __name__ == "__main__":  # 脚本入口
    main()  # 执行主函数
