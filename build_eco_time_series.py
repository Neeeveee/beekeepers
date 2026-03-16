# -*- coding: utf-8 -*-  # 指定源码编码为 UTF-8

import sqlite3  # 导入 SQLite 数据库模块
from pathlib import Path  # 导入 Path 用于处理文件路径


DB_PATH = Path(__file__).resolve().parent / "bee_env.db"


def get_db_connection() -> sqlite3.Connection:  # 定义获取数据库连接的函数
    db_file = Path(DB_PATH)  # 把数据库路径转成 Path 对象
    if not db_file.exists():  # 如果数据库文件不存在
        raise FileNotFoundError(f"数据库文件不存在：{DB_PATH}")  # 抛出文件不存在错误
    conn = sqlite3.connect(str(DB_PATH), timeout=30)  # 连接 SQLite 数据库，并设置超时时间
    conn.execute("PRAGMA journal_mode=WAL;")  # 开启 WAL 模式，提高稳定性
    conn.execute("PRAGMA busy_timeout = 30000;")  # 数据库繁忙时最多等待 30 秒
    return conn  # 返回数据库连接


def build_eco_time_series() -> None:  # 定义生成生态时间序列表的主函数
    conn = get_db_connection()  # 获取数据库连接
    try:  # 开始数据库操作
        cursor = conn.cursor()  # 创建游标

        cursor.execute(  # 从 measurements 表中读取环境数据
            """
            SELECT
                sensor_id,
                timestamp,
                temperature_c,
                humidity_pct,
                wind_speed_ms,
                precip_mm,
                pressure_hpa,
                raw_source
            FROM measurements
            ORDER BY timestamp ASC
            """
        )

        rows = cursor.fetchall()  # 取出所有查询结果

        inserted_count = 0  # 记录成功插入的条数
        skipped_count = 0  # 记录因重复被跳过的条数

        for row in rows:  # 遍历每一条环境数据
            sensor_id, event_time, temperature_c, humidity_pct, wind_speed_ms, precip_mm, pressure_hpa, raw_source = row  # 解包字段

            if not event_time:  # 如果时间字段为空
                continue  # 直接跳过当前这条记录

            series_date = event_time[:10]  # 从 event_time 中截取日期部分，例如 2026-03-07

            cursor.execute(  # 把整理后的数据写入 eco_time_series 表
                """
                INSERT OR IGNORE INTO eco_time_series
                (
                    sensor_id,
                    series_date,
                    event_time,
                    temperature_c,
                    humidity_pct,
                    wind_speed_ms,
                    precip_mm,
                    pressure_hpa,
                    source
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sensor_id,  # 写入传感器编号
                    series_date,  # 写入时间序列所属日期
                    event_time,  # 写入时间点
                    temperature_c,  # 写入温度
                    humidity_pct,  # 写入湿度
                    wind_speed_ms,  # 写入风速
                    precip_mm,  # 写入降水
                    pressure_hpa,  # 写入气压
                    raw_source if raw_source else "qweather",  # 写入数据来源
                ),
            )

            if cursor.rowcount == 0:  # 如果 rowcount 为 0，说明因唯一索引被跳过
                skipped_count += 1  # 跳过计数加 1
            else:  # 否则说明插入成功
                inserted_count += 1  # 插入计数加 1

        conn.commit()  # 提交事务

        print(f"生成完成：新增 {inserted_count} 条，跳过 {skipped_count} 条")  # 打印处理结果

    finally:  # 无论成功还是失败都关闭数据库连接
        conn.close()  # 关闭连接


if __name__ == "__main__":  # 如果当前文件作为主程序运行
    build_eco_time_series()  # 执行生成函数
