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


def build_bee_env_aligned_hourly() -> None:  # 定义生成蜂群-环境小时对齐表的主函数
    conn = get_db_connection()  # 获取数据库连接
    try:  # 开始数据库操作
        cursor = conn.cursor()  # 创建游标

        cursor.execute(  # 读取蜂群小时级活跃数据，并左连接同一小时的环境数据
            """
            SELECT
                b.device_id,
                e.sensor_id,
                b.hour_time AS aligned_time,
                b.hour_date AS aligned_date,

                b.point_count,
                b.sum_in_count,
                b.sum_out_count,
                b.avg_activity_value,
                b.max_activity_value,
                b.min_activity_value,

                e.temperature_c,
                e.humidity_pct,
                e.wind_speed_ms,
                e.precip_mm,
                e.pressure_hpa,

                b.source AS bee_source,
                e.source AS env_source
            FROM bee_activity_hourly b
        LEFT JOIN eco_time_series e
            ON strftime('%Y-%m-%d %H:00:00', b.hour_time) = strftime('%Y-%m-%d %H:00:00', e.event_time)
            ORDER BY b.hour_time ASC
            """
        )

        rows = cursor.fetchall()  # 取出所有查询结果

        inserted_count = 0  # 记录成功插入的条数
        skipped_count = 0  # 记录因重复被跳过的条数

        for row in rows:  # 遍历每一条对齐结果
            (
                device_id,
                sensor_id,
                aligned_time,
                aligned_date,
                point_count,
                sum_in_count,
                sum_out_count,
                avg_activity_value,
                max_activity_value,
                min_activity_value,
                temperature_c,
                humidity_pct,
                wind_speed_ms,
                precip_mm,
                pressure_hpa,
                bee_source,
                env_source,
            ) = row  # 解包字段

            cursor.execute(  # 把对齐结果写入 bee_env_aligned_hourly 表
                """
                INSERT OR IGNORE INTO bee_env_aligned_hourly
                (
                    device_id,
                    sensor_id,
                    aligned_time,
                    aligned_date,

                    point_count,
                    sum_in_count,
                    sum_out_count,
                    avg_activity_value,
                    max_activity_value,
                    min_activity_value,

                    temperature_c,
                    humidity_pct,
                    wind_speed_ms,
                    precip_mm,
                    pressure_hpa,

                    bee_source,
                    env_source
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    device_id,  # 写入设备编号
                    sensor_id,  # 写入环境传感器编号
                    aligned_time,  # 写入对齐时间
                    aligned_date,  # 写入对齐日期

                    point_count,  # 写入该小时数据点数
                    sum_in_count,  # 写入该小时进入总数
                    sum_out_count,  # 写入该小时离开总数
                    avg_activity_value,  # 写入平均活跃度
                    max_activity_value,  # 写入最大活跃度
                    min_activity_value,  # 写入最小活跃度

                    temperature_c,  # 写入温度
                    humidity_pct,  # 写入湿度
                    wind_speed_ms,  # 写入风速
                    precip_mm,  # 写入降水
                    pressure_hpa,  # 写入气压

                    bee_source,  # 写入蜂群数据来源
                    env_source,  # 写入环境数据来源
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
    build_bee_env_aligned_hourly()  # 执行生成函数
