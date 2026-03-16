# -*- coding: utf-8 -*-  # 指定源码编码为 UTF-8，避免中文乱码

import sqlite3  # 导入 SQLite 模块
import json  # 导入 JSON 模块
import glob  # 导入文件匹配模块
from pathlib import Path  # 导入 Path 模块，便于处理路径
from datetime import datetime  # 导入时间模块

DB_PATH = "bee_env.db"  # 数据库路径（默认当前项目目录下）
DATA_GLOB = "data_raw/qweather_24h_*.json"  # 只匹配 24h 天气文件，避免误读 history 文件


def normalize_ts(fx_time: str) -> str:  # 定义统一时间格式函数
    no_tz = fx_time.split("+")[0]  # 去掉时区部分，例如 +08:00
    s = no_tz.replace("T", " ")  # 把时间中的 T 替换为空格
    if len(s) == 16:  # 如果只有到分钟，没有秒
        s += ":00"  # 自动补秒
    return s  # 返回统一后的时间字符串


def get_qweather_sensor_id(cur: sqlite3.Cursor) -> int:  # 定义函数：获取 qweather 对应的 sensor_id
    cur.execute("SELECT id FROM sensors WHERE source = 'qweather' ORDER BY id LIMIT 1")  # 查询 source='qweather' 的传感器
    row = cur.fetchone()  # 取查询结果
    if not row:  # 如果没找到
        raise ValueError("没有 source='qweather' 的传感器，请先在 sensors 表中建一个。")  # 抛出异常
    return row[0]  # 返回传感器 ID


def insert_qweather_json(filename: str) -> tuple[int, int]:  # 定义函数：把单个 JSON 文件写入数据库，返回（新增条数，跳过条数）
    with open(filename, "r", encoding="utf-8") as f:  # 打开 JSON 文件
        data = json.load(f)  # 读取 JSON 内容

    hourly = data.get("hourly", [])  # 读取 hourly 数组
    if not hourly:  # 如果没有 hourly 数据
        print(f"文件 {filename} 没有 hourly 数据，跳过。")  # 打印提示
        return 0, 0  # 返回 0 条新增、0 条跳过

    conn = sqlite3.connect(DB_PATH)  # 连接数据库
    cur = conn.cursor()  # 创建游标

    sensor_id = get_qweather_sensor_id(cur)  # 获取 qweather 对应的 sensor_id

    insert_count = 0  # 初始化新增计数
    skip_count = 0  # 初始化跳过计数

    try:  # 开始写库过程
        for h in hourly:  # 遍历每一个小时的数据
            raw_ts = h.get("fxTime")  # 读取原始时间字段 fxTime
            if not raw_ts:  # 如果没有时间
                continue  # 跳过当前记录

            ts = normalize_ts(raw_ts)  # 统一时间格式

            cur.execute(  # 检查 measurements 中是否已有同一 sensor_id + timestamp 的记录
                """
                SELECT COUNT(*)
                FROM measurements
                WHERE sensor_id = ? AND timestamp = ?
                """,
                (sensor_id, ts),
            )
            exists_count = cur.fetchone()[0]  # 取存在数量

            if exists_count > 0:  # 如果已存在
                skip_count += 1  # 跳过计数 +1
                continue  # 不重复插入

            temp = float(h.get("temp")) if h.get("temp") is not None else None  # 温度（℃）
            humidity = float(h.get("humidity")) if h.get("humidity") is not None else None  # 湿度（%）
            pressure = float(h.get("pressure")) if h.get("pressure") is not None else None  # 气压（hPa）

            wind_kmh = float(h.get("windSpeed")) if h.get("windSpeed") is not None else None  # 风速（km/h）
            wind_ms = (wind_kmh / 3.6) if wind_kmh is not None else None  # 转换为 m/s

            precip = float(h.get("precip")) if h.get("precip") is not None else 0.0  # 降水量（mm）

            cur.execute(  # 向 measurements 表插入新记录
                """
                INSERT INTO measurements
                (
                    sensor_id,
                    timestamp,
                    temperature_c,
                    humidity_pct,
                    pressure_hpa,
                    wind_speed_ms,
                    precip_mm,
                    raw_source,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now', 'localtime'))
                """,
                (
                    sensor_id,  # 传感器 ID
                    ts,  # 时间戳
                    temp,  # 温度
                    humidity,  # 湿度
                    pressure,  # 气压
                    wind_ms,  # 风速（m/s）
                    precip,  # 降水（mm）
                    "qweather-24h",  # 数据来源
                ),
            )
            insert_count += 1  # 新增计数 +1

        conn.commit()  # 提交事务

    finally:  # 无论成功与否都关闭连接
        conn.close()  # 关闭数据库连接

    print(f"🌤 从 {filename} 写入 {insert_count} 条新记录，跳过 {skip_count} 条已存在时间。")  # 输出结果
    return insert_count, skip_count  # 返回新增和跳过数量


def main():  # 定义主函数
    files = sorted(glob.glob(DATA_GLOB))  # 只读取 qweather_24h_*.json 文件，并按文件名排序

    if not files:  # 如果一个文件都没找到
        print(f"未找到天气文件：{DATA_GLOB}")  # 打印提示
        print("请先运行 fetch_qweather_24h.py 生成 24h 天气 JSON。")  # 打印下一步建议
        return  # 结束程序

    print(f"共找到 {len(files)} 个 24h 天气文件。")  # 打印文件总数

    total_insert = 0  # 初始化总新增数
    total_skip = 0  # 初始化总跳过数

    for fp in files:  # 逐个导入每个 24h 文件
        print("正在导入：", fp)  # 打印当前导入文件名
        inserted, skipped = insert_qweather_json(fp)  # 调用函数导入单个文件
        total_insert += inserted  # 累加新增数量
        total_skip += skipped  # 累加跳过数量

    print("--------------------------------------------------")  # 分隔线
    print(f"全部导入完成：新增 {total_insert} 条，跳过 {total_skip} 条。")  # 输出总结果


if __name__ == "__main__":  # 如果当前文件直接运行
    main()  # 执行主函数