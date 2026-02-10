import sqlite3  # 引入sqlite3（连接SQLite数据库）
from datetime import datetime  # 引入datetime（解析时间戳）
from collections import defaultdict  # 引入defaultdict（按天分组统计）

DB_FILE = "bee_env.db"  # 数据库文件名（与你项目目录同级）


def avg(lst):  # 定义平均值函数
    return sum(lst) / len(lst) if lst else None  # 列表非空则求平均，否则返回None


def clamp(x, lo, hi):  # 定义夹逼函数（限制范围）
    return max(lo, min(hi, x))  # 将x限制到[lo,hi]范围内


def ensure_tables(cur):  # 确保需要的表存在（只创建缺失的，不删除旧数据）
    cur.execute(  # 创建daily_env_indices（日环境指标表：你已有但这里确保结构存在）
        """
        CREATE TABLE IF NOT EXISTS daily_env_indices (                     -- 日环境指标表
            id INTEGER PRIMARY KEY AUTOINCREMENT,                          -- 自增主键
            site_id INTEGER NOT NULL,                                      -- 站点ID
            date TEXT NOT NULL,                                            -- 日期（YYYY-MM-DD）
            day_avs_temp_c REAL,                                           -- 白天平均温度
            night_avs_temp_c REAL,                                         -- 夜间平均温度
            evap_index REAL,                                               -- 蒸发/干燥指数（简化）
            shade_sun_ratio REAL,                                          -- 阴影/日照比例（占位）
            micro_temp_range_c REAL,                                       -- 日温差
            created_at TEXT DEFAULT (datetime('now', 'localtime'))         -- 创建时间
        );
        """
    )  # daily_env_indices建表结束

    cur.execute(  # 创建唯一索引（用于UPSERT）
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_daily_env_indices_site_date
        ON daily_env_indices(site_id, date);
        """
    )  # daily_env_indices唯一索引结束

    cur.execute(  # 创建FRI表（花源综合指数）
        """
        CREATE TABLE IF NOT EXISTS flower_resource_index (                 -- 花源综合指数表
            id INTEGER PRIMARY KEY AUTOINCREMENT,                          -- 自增主键
            site_id INTEGER NOT NULL,                                      -- 站点ID
            date TEXT NOT NULL,                                            -- 日期（YYYY-MM-DD）
            fri_value INTEGER CHECK(fri_value BETWEEN 0 AND 100),          -- FRI（0-100）
            data_level TEXT CHECK(data_level IN ('observed','inferred','predicted')), -- 数据层级
            source_version TEXT,                                           -- 规则/模型版本
            created_at TEXT DEFAULT (datetime('now', 'localtime'))         -- 创建时间
        );
        """
    )  # FRI表建表结束

    cur.execute(  # 创建FRI唯一索引（用于UPSERT）
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_fri_site_date
        ON flower_resource_index(site_id, date);
        """
    )  # FRI唯一索引结束

    cur.execute(  # 创建NAI表（可采蜜强度）
        """
        CREATE TABLE IF NOT EXISTS nectar_availability_index (             -- 可采蜜强度表
            id INTEGER PRIMARY KEY AUTOINCREMENT,                          -- 自增主键
            site_id INTEGER NOT NULL,                                      -- 站点ID
            date TEXT NOT NULL,                                            -- 日期（YYYY-MM-DD）
            nai_value INTEGER CHECK(nai_value BETWEEN 0 AND 100),          -- NAI（0-100）
            weather_modifier REAL,                                         -- 天气修正系数（0-1）
            data_level TEXT CHECK(data_level IN ('inferred','predicted')), -- 数据层级
            source_version TEXT,                                           -- 规则/模型版本
            created_at TEXT DEFAULT (datetime('now', 'localtime'))         -- 创建时间
        );
        """
    )  # NAI表建表结束

    cur.execute(  # 创建NAI唯一索引（用于UPSERT）
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_nai_site_date
        ON nectar_availability_index(site_id, date);
        """
    )  # NAI唯一索引结束


def compute_daily_env_indices(cur):  # 计算并写入daily_env_indices
    cur.execute(  # 读取measurements + sensors（只取和风天气）
        """
        SELECT
            m.timestamp,               -- 时间戳
            m.temperature_c,           -- 温度
            m.humidity_pct,            -- 湿度
            m.wind_speed_ms,           -- 风速
            s.site_id,                 -- 站点ID
            s.source                   -- 数据来源
        FROM measurements m
        JOIN sensors s ON m.sensor_id = s.id
        WHERE m.temperature_c IS NOT NULL
          AND (s.source = 'qweather' OR s.source IS NULL OR s.source = '')
        ORDER BY s.site_id, m.timestamp;
        """
    )  # 查询结束

    rows = cur.fetchall()  # 获取所有行
    if not rows:  # 如果没有任何数据
        print("❌ measurements 里没有可用的和风天气数据，请先跑 fetch + insert。")  # 打印提示
        return 0  # 返回0条写入

    groups = defaultdict(lambda: {"temps_all": [], "day_temps": [], "night_temps": [], "day_humids": [], "day_winds": []})  # 初始化分组容器

    for ts_str, temp_c, hum, wind_ms, site_id, _source in rows:  # 遍历每条测量
        if not ts_str:  # 如果时间戳为空
            continue  # 跳过
        dt = None  # 初始化dt
        try:  # 尝试解析时间
            dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")  # 标准格式
        except ValueError:  # 如果失败
            try:  # 再试另一种格式
                dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M")  # 分钟级格式
            except ValueError:  # 还是失败
                continue  # 跳过这一行

        date_str = dt.date().isoformat()  # 提取日期字符串
        hour = dt.hour  # 提取小时

        g = groups[(site_id, date_str)]  # 获取该站点该日的组

        if temp_c is None:  # 如果温度缺失
            continue  # 跳过

        t = float(temp_c)  # 转为float
        g["temps_all"].append(t)  # 加入全天温度列表

        if 8 <= hour < 18:  # 白天窗口（8:00–18:00）
            g["day_temps"].append(t)  # 加入白天温度
            if hum is not None:  # 如果湿度存在
                g["day_humids"].append(float(hum))  # 加入白天湿度
            if wind_ms is not None:  # 如果风速存在
                g["day_winds"].append(float(wind_ms))  # 加入白天风速
        else:  # 夜间窗口
            g["night_temps"].append(t)  # 加入夜间温度

    insert_count = 0  # 初始化写入计数

    for (site_id, date_str), g in groups.items():  # 遍历每个站点每一天
        temps_all = g["temps_all"]  # 取全天温度
        day_temps = g["day_temps"]  # 取白天温度
        night_temps = g["night_temps"]  # 取夜间温度
        day_humids = g["day_humids"]  # 取白天湿度
        day_winds = g["day_winds"]  # 取白天风速

        if not temps_all:  # 如果没有温度数据
            continue  # 跳过

        day_avs_temp_c = avg(day_temps) if day_temps else avg(temps_all)  # 白天均温（有白天则用白天，否则全天）
        night_avs_temp_c = avg(night_temps)  # 夜间均温（可能为空）
        micro_temp_range_c = (max(temps_all) - min(temps_all)) if len(temps_all) >= 2 else None  # 日温差（至少2个点）

        evap_index = None  # 初始化蒸发指数
        if day_temps and day_humids:  # 只有白天温度+湿度都有才算
            evap_values = []  # 初始化蒸发值列表
            winds = day_winds if day_winds else [0.0] * len(day_temps)  # 如果没风速就用0填充
            for i in range(min(len(day_temps), len(day_humids), len(winds))):  # 对齐最短长度
                t = float(day_temps[i])  # 取温度
                h = float(day_humids[i])  # 取湿度
                w = float(winds[i]) if winds[i] is not None else 0.0  # 取风速
                dryness = max(t, 0.0) * (100.0 - h) / 100.0  # 干燥度（简化）
                wind_factor = 1.0 + (w / 5.0)  # 风速放大因子（简化）
                evap_values.append(dryness * wind_factor)  # 加入列表
            evap_index = avg(evap_values) if evap_values else None  # 取平均作为evap_index

        shade_sun_ratio = None  # 阴影/日照比例（占位，未来接辐射或云量）

        cur.execute(  # UPSERT写入daily_env_indices（不重复插入，存在则更新）
            """
            INSERT INTO daily_env_indices
            (site_id, date, day_avs_temp_c, night_avs_temp_c, evap_index, shade_sun_ratio, micro_temp_range_c)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(site_id, date) DO UPDATE SET
                day_avs_temp_c=excluded.day_avs_temp_c,
                night_avs_temp_c=excluded.night_avs_temp_c,
                evap_index=excluded.evap_index,
                shade_sun_ratio=excluded.shade_sun_ratio,
                micro_temp_range_c=excluded.micro_temp_range_c;
            """,
            (site_id, date_str, day_avs_temp_c, night_avs_temp_c, evap_index, shade_sun_ratio, micro_temp_range_c),
        )  # UPSERT执行结束

        insert_count += 1  # 写入计数+1

    return insert_count  # 返回写入条数


def coverage_factor(coverage_class):  # 将覆盖等级映射为数值因子
    mapping = {"low": 0.30, "mid": 0.60, "high": 0.90}  # 定义映射表（可后续调参）
    return mapping.get(coverage_class or "", 0.50)  # 默认给0.50（未知时）


def flower_factor_from_obs(stage, intensity):  # 由花期观察推一个花源有效因子
    stage_map = {0: 0.00, 1: 0.40, 2: 1.00, 3: 0.20}  # 花期阶段映射（0无 1初开 2盛开 3衰败）
    sf = stage_map.get(int(stage), 0.50)  # 取阶段因子（默认0.50）
    it = 0.0 if intensity is None else clamp(float(intensity) / 5.0, 0.0, 1.0)  # 将强度0-5归一化到0-1
    return sf * (0.5 + 0.5 * it)  # 将强度作为阶段的加权（0.5~1.0）


def compute_fri_nai_rule_v1(cur):  # 计算FRI/NAI并写入数据库（rule_v1）
    # 1) 找出有哪些站点需要计算（从sites表）
    cur.execute("SELECT id FROM sites;")  # 查询所有站点ID
    site_ids = [r[0] for r in cur.fetchall()]  # 收集站点ID列表
    if not site_ids:  # 如果没有站点
        print("❌ sites 表为空，无法计算 FRI/NAI。")  # 打印提示
        return 0, 0  # 返回0条

    # 2) 找出有哪些日期需要计算（以daily_weather_summary为准；如果没有就退回daily_env_indices）
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='daily_weather_summary';")  # 检查表存在
    has_dws = cur.fetchone() is not None  # 判断daily_weather_summary是否存在

    if has_dws:  # 如果有daily_weather_summary
        cur.execute("SELECT DISTINCT site_id, date FROM daily_weather_summary;")  # 取所有站点-日期
        date_rows = cur.fetchall()  # 获取结果
    else:  # 否则
        cur.execute("SELECT DISTINCT site_id, date FROM daily_env_indices;")  # 退回用daily_env_indices日期
        date_rows = cur.fetchall()  # 获取结果

    if not date_rows:  # 如果没有任何日期
        print("❌ 没有可用的日期（日汇总表为空），请先跑 build_daily_weather_summary.py。")  # 提示
        return 0, 0  # 返回0条

    fri_written = 0  # 记录FRI写入条数
    nai_written = 0  # 记录NAI写入条数

    # 3) 对每个（site_id, date）计算一次FRI/NAI
    for site_id, date_str in date_rows:  # 遍历每个站点每一天
        # 3.1) 读取该站点的植被清单（如果你还没填，会导致FRI无法计算）
        cur.execute(  # 查询该站点在site_plant_inventory的条目
            """
            SELECT
                plant_id,              -- 植物ID
                coverage_class,        -- 覆盖等级
                importance_weight      -- 重要性权重
            FROM site_plant_inventory
            WHERE site_id = ?;
            """,
            (site_id,),
        )  # 查询结束
        inv = cur.fetchall()  # 获取清单

        if not inv:  # 如果该站点没有植被清单
            continue  # 直接跳过（不造假）

        # 3.2) 构建花期观察字典（如果没有观察，则使用默认因子0.5）
        cur.execute(  # 查询该站点该日的花期观察
            """
            SELECT plant_id, stage, intensity
            FROM flowering_observations
            WHERE site_id = ? AND obs_date = ?;
            """,
            (site_id, date_str),
        )  # 查询结束
        obs_rows = cur.fetchall()  # 获取观察行
        obs_map = {pid: (st, it) for (pid, st, it) in obs_rows}  # 转成字典（plant_id -> (stage,intensity)）

        # 3.3) 计算FRI：基础覆盖×权重，再乘花期有效因子
        base_sum = 0.0  # 基础和（不含花期）
        eff_sum = 0.0  # 有效和（含花期）
        for plant_id, cov_cls, w in inv:  # 遍历每种植物
            cf = coverage_factor(cov_cls)  # 覆盖因子
            iw = float(w) if w is not None else 0.0  # 权重
            base = iw * cf  # 基础贡献
            base_sum += base  # 累加基础

            if plant_id in obs_map:  # 如果当天有该植物花期观察
                st, it = obs_map[plant_id]  # 取阶段与强度
                ff = flower_factor_from_obs(st, it)  # 计算花期有效因子
            else:  # 如果没有观察
                ff = 0.50  # 默认给0.50（占位：未知）
            eff_sum += base * ff  # 累加有效贡献

        if base_sum <= 0.0:  # 如果基础和为0
            continue  # 跳过

        # 3.4) 把FRI标准化到0-100（base_sum越大表示潜在花源越多，但上限截断为1）
        base_norm = clamp(base_sum, 0.0, 1.0)  # 将基础和截断到0-1
        flower_norm = clamp(eff_sum / base_sum, 0.0, 1.0)  # 花期有效度（0-1）
        fri_value = int(round(base_norm * flower_norm * 100.0))  # 计算FRI整数值

        # 3.5) 写入FRI表（UPSERT）
        cur.execute(  # UPSERT写入flower_resource_index
            """
            INSERT INTO flower_resource_index
            (site_id, date, fri_value, data_level, source_version)
            VALUES (?, ?, ?, 'inferred', 'rule_v1')
            ON CONFLICT(site_id, date) DO UPDATE SET
                fri_value=excluded.fri_value,
                data_level='inferred',
                source_version='rule_v1';
            """,
            (site_id, date_str, fri_value),
        )  # UPSERT结束
        fri_written += 1  # 写入计数+1

        # 3.6) 读取当天的天气日汇总（用于weather_modifier）
        precip = 0.0  # 初始化降雨
        wind = 0.0  # 初始化风
        temp = None  # 初始化温度

        if has_dws:  # 如果有daily_weather_summary
            cur.execute(  # 读取降雨/风/温度（白天均温即可）
                """
                SELECT day_sum_precip_mm, day_avg_wind_ms, day_avg_temp_c
                FROM daily_weather_summary
                WHERE site_id = ? AND date = ?;
                """,
                (site_id, date_str),
            )  # 查询结束
            r = cur.fetchone()  # 取一行
            if r:  # 如果存在
                precip = float(r[0] or 0.0)  # 取降雨
                wind = float(r[1] or 0.0)  # 取风
                temp = r[2]  # 取温度
                temp = float(temp) if temp is not None else None  # 转float
        else:  # 如果没有daily_weather_summary，则用daily_env_indices的近似变量（较弱）
            cur.execute(  # 用evap_index和day_avs_temp_c做替代
                """
                SELECT day_avs_temp_c, evap_index
                FROM daily_env_indices
                WHERE site_id = ? AND date = ?;
                """,
                (site_id, date_str),
            )  # 查询结束
            r = cur.fetchone()  # 取一行
            if r:  # 如果存在
                temp = float(r[0]) if r[0] is not None else None  # 取温度
                # 注意：无降雨/风数据时modifier只能粗略给默认值
                precip = 0.0  # 设为0
                wind = 0.0  # 设为0

        # 3.7) 计算天气修正系数（0-1）：雨越大、风越大、温度过低 → 可采蜜强度下降
        modifier = 1.0  # 初始化系数

        # 降雨惩罚：10mm以上时强惩罚，但保留底线0.2
        modifier *= clamp(1.0 - (precip / 10.0), 0.2, 1.0)  # 将降雨映射成系数

        # 风速惩罚：6m/s以上开始下降，保留底线0.3
        if wind > 6.0:  # 如果大风
            modifier *= clamp(1.0 - ((wind - 6.0) / 10.0), 0.3, 1.0)  # 风越大越低

        # 温度惩罚：低于12度逐步下降（你后面可按品种/地区调）
        if temp is not None:  # 如果有温度
            if temp < 12.0:  # 偏冷
                modifier *= clamp((temp - 5.0) / 7.0, 0.3, 1.0)  # 5度以下非常低，12度以上不罚

        modifier = clamp(modifier, 0.0, 1.0)  # 最终夹逼到0-1

        # 3.8) 计算NAI并写入（NAI = FRI × modifier）
        nai_value = int(round(fri_value * modifier))  # 计算NAI整数值（0-100）

        cur.execute(  # UPSERT写入nectar_availability_index
            """
            INSERT INTO nectar_availability_index
            (site_id, date, nai_value, weather_modifier, data_level, source_version)
            VALUES (?, ?, ?, ?, 'inferred', 'rule_v1')
            ON CONFLICT(site_id, date) DO UPDATE SET
                nai_value=excluded.nai_value,
                weather_modifier=excluded.weather_modifier,
                data_level='inferred',
                source_version='rule_v1';
            """,
            (site_id, date_str, nai_value, modifier),
        )  # UPSERT结束
        nai_written += 1  # 写入计数+1

    return fri_written, nai_written  # 返回写入条数


def main():  # 主入口
    conn = sqlite3.connect(DB_FILE)  # 连接数据库
    cur = conn.cursor()  # 获取游标

    ensure_tables(cur)  # 确保表存在（只补不删）
    conn.commit()  # 提交建表

    # 1) 先计算日环境指标（从measurements算）
    n_daily = compute_daily_env_indices(cur)  # 计算并UPSERT写入daily_env_indices
    conn.commit()  # 提交写入
    print(f"✅ 已写入/更新 {n_daily} 条 daily_env_indices 日指标记录。")  # 打印结果

    # 2) 再计算FRI/NAI（rule_v1）并写入
    fri_n, nai_n = compute_fri_nai_rule_v1(cur)  # 计算FRI与NAI
    conn.commit()  # 提交写入
    print(f"✅ 已写入/更新 {fri_n} 条 FRI（flower_resource_index）。")  # 打印FRI结果
    print(f"✅ 已写入/更新 {nai_n} 条 NAI（nectar_availability_index）。")  # 打印NAI结果

    conn.close()  # 关闭连接


if __name__ == "__main__":  # 脚本入口判断
    main()  # 执行主函数
