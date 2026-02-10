import os  # 导入操作系统路径模块（用于自动定位数据库文件）  
import sqlite3  # 导入SQLite模块（用于读写数据库）  
from datetime import datetime  # 导入时间模块（用于解析timestamp）  
from collections import defaultdict  # 导入分组容器（用于按天汇总）  

# ====== 1）数据库路径：自动锁定为“本脚本同目录”的 bee_env.db（避免跑错库） ======  
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))  # 获取脚本所在目录（绝对路径）  
DB_PATH = os.path.join(SCRIPT_DIR, "bee_env.db")  # 拼出数据库绝对路径（最稳）  

def avg(lst):  # 定义求平均值函数（列表为空则返回None）  
    return sum(lst) / len(lst) if lst else None  # 计算平均值（避免除0）  

def source_priority(src):  # 定义数据源优先级函数（用于多来源去重）  
    if src == "on_farm":  # 如果是蜂场实地传感器  
        return 3  # 优先级最高  
    if src == "official":  # 如果是官方气象站  
        return 2  # 中等优先级  
    if src == "qweather":  # 如果是和风天气API  
        return 1  # 基础优先级  
    return 0  # 其他/空来源最低优先级  

def parse_ts(ts_str):  # 定义时间解析函数（兼容两种格式）  
    try:  # 尝试解析“带秒”的格式  
        return datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")  # 形如 2026-01-22 16:00:00  
    except Exception:  # 如果失败  
        try:  # 再尝试解析“无秒”的格式  
            return datetime.strptime(ts_str, "%Y-%m-%d %H:%M")  # 形如 2026-01-22 16:00  
        except Exception:  # 仍失败  
            return None  # 返回None表示无法解析  

def main():  # 主函数入口  
    conn = sqlite3.connect(DB_PATH)  # 连接数据库（使用绝对路径）  
    cur = conn.cursor()  # 创建游标对象  

    # ====== 2）确保 daily_weather_summary 表存在（不存在就创建） ======  
    cur.execute(  # 执行建表SQL  
        """  
        CREATE TABLE IF NOT EXISTS daily_weather_summary (  
            id INTEGER PRIMARY KEY AUTOINCREMENT,  
            site_id INTEGER NOT NULL,  
            date TEXT NOT NULL,  
            day_avg_temp_c REAL,  
            night_avg_temp_c REAL,  
            day_avg_humidity_pct REAL,  
            day_avg_wind_ms REAL,  
            day_sum_precip_mm REAL,  
            day_avg_pressure_hpa REAL,  
            micro_temp_range_c REAL,  
            created_at TEXT DEFAULT (datetime('now', 'localtime')),  
            UNIQUE(site_id, date)  
        );  
        """  
    )  # 建表结束  
    conn.commit()  # 提交建表操作  

    # ====== 3）读取 measurements + sensors：不限制数据源（让系统更通用） ======  
    cur.execute(  # 查询逐小时数据（把三类来源都纳入）  
        """  
        SELECT  
            m.timestamp,             -- 时间  
            m.temperature_c,         -- 温度  
            m.humidity_pct,          -- 湿度  
            m.wind_speed_ms,         -- 风速  
            m.precip_mm,             -- 降水  
            m.pressure_hpa,          -- 气压  
            s.site_id,               -- 场地ID  
            s.source                 -- 数据源（qweather/on_farm/official）  
        FROM measurements m  
        JOIN sensors s ON m.sensor_id = s.id  
        WHERE m.timestamp IS NOT NULL  
          AND m.temperature_c IS NOT NULL  
        ORDER BY s.site_id, m.timestamp;  
        """  
    )  # SQL执行结束  

    rows = cur.fetchall()  # 取出所有查询结果  
    if not rows:  # 如果没有可用数据  
        print("⚠ measurements 里没有可用气象数据：请先导入逐小时数据")  # 打印提示  
        conn.close()  # 关闭数据库连接  
        return  # 退出程序  

    # ====== 4）同一 site_id + 同一 timestamp：可能存在多来源重复，先做“选优去重” ======  
    best_by_hour = {}  # 定义字典：key=(site_id, timestamp_str) -> best_record  
    for ts_str, temp_c, hum, wind_ms, precip_mm, pressure_hpa, site_id, source in rows:  # 遍历每条记录  
        dt = parse_ts(ts_str)  # 解析时间字符串  
        if dt is None:  # 如果解析失败  
            continue  # 跳过这条记录  
        key = (site_id, dt.strftime("%Y-%m-%d %H:%M:%S"))  # 统一成带秒格式作为去重key  

        # 统计“这条记录的信息量”（非空字段越多越好）  
        info_score = 0  # 初始化信息量分数  
        info_score += 1 if temp_c is not None else 0  # 温度非空加1  
        info_score += 1 if hum is not None else 0  # 湿度非空加1  
        info_score += 1 if wind_ms is not None else 0  # 风速非空加1  
        info_score += 1 if precip_mm is not None else 0  # 降水非空加1  
        info_score += 1 if pressure_hpa is not None else 0  # 气压非空加1  

        pr = source_priority(source)  # 计算来源优先级  

        record = (key[1], temp_c, hum, wind_ms, precip_mm, pressure_hpa, site_id, source, pr, info_score)  # 组装记录  

        if key not in best_by_hour:  # 如果该小时还没有记录  
            best_by_hour[key] = record  # 直接作为最佳记录  
        else:  # 如果已有记录（需要比较优劣）  
            _, _, _, _, _, _, _, _, old_pr, old_info = best_by_hour[key]  # 取出旧记录评分  
            # 优先规则：来源优先级更高者胜；同优先级则信息量更多者胜  
            if (pr > old_pr) or (pr == old_pr and info_score > old_info):  # 判断是否更好  
                best_by_hour[key] = record  # 替换为更好的记录  

    # ====== 5）按 (site_id, date) 分组累积（允许“只有晚上数据”的天也生成汇总） ======  
    groups = defaultdict(lambda: {  # 初始化每组容器  
        "temps_all": [],  # 全天温度列表  
        "day_temps": [],  # 白天温度列表  
        "night_temps": [],  # 夜间温度列表  
        "day_humids": [],  # 白天湿度列表  
        "day_winds": [],  # 白天风速列表  
        "day_precips": [],  # 全天降水列表（按小时累加）  
        "day_pressures": [],  # 白天气压列表  
    })  # 容器初始化结束  

    for (_, _), rec in best_by_hour.items():  # 遍历每个“去重后的小时记录”  
        ts_norm, temp_c, hum, wind_ms, precip_mm, pressure_hpa, site_id, source, pr, info_score = rec  # 解包记录  
        dt = parse_ts(ts_norm)  # 再解析成datetime  
        if dt is None:  # 理论上不会，但防御一下  
            continue  # 跳过  

        date_str = dt.date().isoformat()  # 取日期 YYYY-MM-DD  
        hour = dt.hour  # 取小时（0-23）  

        g = groups[(site_id, date_str)]  # 获取该天的分组容器  
        t = float(temp_c)  # 温度转float  
        g["temps_all"].append(t)  # 加入全天温度列表  

        # 白天定义：8:00–18:00（与你原脚本保持一致）  
        if 8 <= hour < 18:  # 如果是白天  
            g["day_temps"].append(t)  # 记录白天温度  
            if hum is not None:  # 如果有湿度  
                g["day_humids"].append(float(hum))  # 记录白天湿度  
            if wind_ms is not None:  # 如果有风速  
                g["day_winds"].append(float(wind_ms))  # 记录白天风速  
            if pressure_hpa is not None:  # 如果有气压  
                g["day_pressures"].append(float(pressure_hpa))  # 记录白天气压  
        else:  # 否则算夜间  
            g["night_temps"].append(t)  # 记录夜间温度  

        # 降水：没有就当0（避免NULL导致后续规则匹配失败）  
        g["day_precips"].append(float(precip_mm) if precip_mm is not None else 0.0)  # 累加降水  

    # ====== 6）计算每日汇总并写入（UPSERT：可重复跑，永远只保留最新汇总） ======  
    write_count = 0  # 初始化写入/更新天数  
    for (site_id, date_str), g in groups.items():  # 遍历每个场地的每一天  
        if not g["temps_all"]:  # 如果这一天没有温度数据  
            continue  # 跳过（无法计算任何东西）  

        # 白天温度：如果白天没数据，就用全天温度作为替代（保证“只有晚上数据”也能出结果）  
        day_avg_temp = avg(g["day_temps"]) if g["day_temps"] else avg(g["temps_all"])  # 计算白天平均温度  
        night_avg_temp = avg(g["night_temps"])  # 计算夜间平均温度（可能为空则None）  
        day_avg_hum = avg(g["day_humids"])  # 计算白天平均湿度（可能为空则None）  
        day_avg_wind = avg(g["day_winds"])  # 计算白天平均风速（可能为空则None）  
        day_sum_precip = sum(g["day_precips"]) if g["day_precips"] else 0.0  # 计算日累计降水（确保非空）  
        day_avg_pressure = avg(g["day_pressures"])  # 计算白天平均气压（可能为空则None）  
        temp_range = (max(g["temps_all"]) - min(g["temps_all"])) if len(g["temps_all"]) >= 2 else None  # 计算日温差  

        cur.execute(  # UPSERT 写入 daily_weather_summary  
            """  
            INSERT INTO daily_weather_summary  
              (site_id, date, day_avg_temp_c, night_avg_temp_c, day_avg_humidity_pct,  
               day_avg_wind_ms, day_sum_precip_mm, day_avg_pressure_hpa, micro_temp_range_c)  
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)  
            ON CONFLICT(site_id, date) DO UPDATE SET  
              day_avg_temp_c=excluded.day_avg_temp_c,  
              night_avg_temp_c=excluded.night_avg_temp_c,  
              day_avg_humidity_pct=excluded.day_avg_humidity_pct,  
              day_avg_wind_ms=excluded.day_avg_wind_ms,  
              day_sum_precip_mm=excluded.day_sum_precip_mm,  
              day_avg_pressure_hpa=excluded.day_avg_pressure_hpa,  
              micro_temp_range_c=excluded.micro_temp_range_c,  
              created_at=datetime('now', 'localtime');  
            """,  
            (  
                site_id, date_str,  # 场地与日期  
                day_avg_temp, night_avg_temp, day_avg_hum,  # 温度与湿度  
                day_avg_wind, day_sum_precip, day_avg_pressure, temp_range  # 风、雨、压、温差  
            ),  
        )  # 执行写入结束  
        write_count += 1  # 写入/更新天数加1  

    conn.commit()  # 提交事务  
    conn.close()  # 关闭连接  
    print(f"✅ daily_weather_summary 更新完成：写入/更新 {write_count} 天（允许缺小时/仅夜间数据）")  # 打印完成信息  

if __name__ == "__main__":  # 如果作为主程序运行  
    main()  # 执行主函数  
