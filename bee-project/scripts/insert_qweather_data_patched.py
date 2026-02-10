
import sqlite3  # 导入SQLite模块
import json  # 导入JSON模块
import glob  # 导入文件匹配模块
from datetime import datetime  # 导入时间模块

DB_PATH = "bee_env.db"  # 数据库路径

def normalize_ts(fx_time: str) -> str:  # 统一时间格式
    no_tz = fx_time.split("+")[0]  # 去掉时区部分
    s = no_tz.replace("T", " ")  # 把T替换为空格
    if len(s) == 16:  # 如果没有秒
        s += ":00"  # 补秒
    return s  # 返回统一格式

def insert_qweather_json(filename):  # 把单个JSON写入数据库
    with open(filename, "r", encoding="utf-8") as f:  # 打开文件
        data = json.load(f)  # 读取JSON

    hourly = data.get("hourly", [])  # 取逐小时数组
    if not hourly:  # 如果为空
        print(f"文件 {filename} 没有 hourly 数据，跳过。")  # 提示
        return  # 退出

    conn = sqlite3.connect(DB_PATH)  # 连接数据库
    cur = conn.cursor()  # 创建游标

    # 找到 qweather 对应的 sensor（你原脚本就是这样做的）【turn1file1】
    cur.execute("SELECT id FROM sensors WHERE source = 'qweather'")  # 查询sensor
    row = cur.fetchone()  # 取一行
    if not row:  # 如果没有
        conn.close()  # 关闭
        raise ValueError("没有 source='qweather' 的传感器，请先在 sensors 表中建一个。")  # 抛错
    sensor_id = row[0]  # 取sensor_id

    insert_count = 0  # 新增计数
    skip_count = 0  # 跳过计数

    for h in hourly:  # 遍历每小时数据
        raw_ts = h.get("fxTime")  # 取时间
        if not raw_ts:  # 无时间跳过
            continue  # 跳过
        ts = normalize_ts(raw_ts)  # 统一格式

        # 防重复：按 (sensor_id, timestamp) 判断【turn1file1】
        cur.execute(
            "SELECT COUNT(*) FROM measurements WHERE sensor_id = ? AND timestamp = ?",
            (sensor_id, ts),
        )  # 执行查询
        if cur.fetchone()[0] > 0:  # 如果已存在
            skip_count += 1  # 跳过计数+1
            continue  # 跳过

        # 读取字段（都做安全处理：缺了就给None/0）
        temp = float(h.get("temp")) if h.get("temp") is not None else None  # 温度
        humidity = float(h.get("humidity")) if h.get("humidity") is not None else None  # 湿度
        pressure = float(h.get("pressure")) if h.get("pressure") is not None else None  # 气压

        wind_kmh = float(h.get("windSpeed")) if h.get("windSpeed") is not None else None  # 风速(km/h)
        wind_ms = (wind_kmh / 3.6) if wind_kmh is not None else None  # 转成m/s

        precip = float(h.get("precip")) if h.get("precip") is not None else 0.0  # 降水(mm)（新加）

        cur.execute(  # 写入 measurements
            """
            INSERT INTO measurements
            (sensor_id, timestamp,
             temperature_c, humidity_pct, pressure_hpa, wind_speed_ms, precip_mm,
             raw_source, created_at)
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
        )  # 执行插入
        insert_count += 1  # 新增计数+1

    conn.commit()  # 提交
    conn.close()  # 关闭

    print(f"🌤 从 {filename} 写入 {insert_count} 条新记录，跳过 {skip_count} 条已存在的时间。")  # 输出结果

def main():  # 主函数
    files = sorted(glob.glob("data_raw/qweather_*.json"))  # 找所有JSON文件【turn1file1】
    if not files:  # 没有文件
        print("未找到 qweather JSON 文件，请先运行 fetch_qweather_24h.py")  # 提示
        return  # 退出

    latest = files[-1]  # 取最新文件
    print("正在导入：", latest)  # 打印
    insert_qweather_json(latest)  # 导入

if __name__ == "__main__":  # 入口
    main()  # 运行
