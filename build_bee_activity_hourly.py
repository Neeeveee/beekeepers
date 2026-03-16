# -*- coding: utf-8 -*-  # 指定源码编码为 UTF-8

import sqlite3  # 导入 SQLite 数据库模块
from pathlib import Path  # 导入 Path 用于处理文件路径


DB_PATH = r"D:\homeworks\workshop\s7-8\bee-project\bee_env.db"  # 你的数据库路径


def get_db_connection() -> sqlite3.Connection:  # 定义获取数据库连接的函数
    db_file = Path(DB_PATH)  # 把数据库路径转成 Path 对象
    if not db_file.exists():  # 如果数据库文件不存在
        raise FileNotFoundError(f"数据库文件不存在：{DB_PATH}")  # 抛出文件不存在错误
    conn = sqlite3.connect(DB_PATH, timeout=30)  # 连接 SQLite 数据库，并设置超时时间
    conn.execute("PRAGMA journal_mode=WAL;")  # 开启 WAL 模式，提高稳定性
    conn.execute("PRAGMA busy_timeout = 30000;")  # 数据库繁忙时最多等待 30 秒
    return conn  # 返回数据库连接


def build_bee_activity_hourly() -> None:  # 定义生成小时级蜂群活跃表的主函数
    conn = get_db_connection()  # 获取数据库连接
    try:  # 开始数据库操作
        cursor = conn.cursor()  # 创建游标

        cursor.execute(  # 按小时聚合 bee_activity_curve
            """
            SELECT
                device_id,
                substr(event_time, 1, 13) || ':00:00' AS hour_time,
                substr(event_time, 1, 10) AS hour_date,
                COUNT(*) AS point_count,
                SUM(in_count) AS sum_in_count,
                SUM(out_count) AS sum_out_count,
                AVG(activity_value) AS avg_activity_value,
                MAX(activity_value) AS max_activity_value,
                MIN(activity_value) AS min_activity_value
            FROM bee_activity_curve
            GROUP BY device_id, hour_time, hour_date
            ORDER BY hour_time ASC
            """
        )

        rows = cursor.fetchall()  # 取出所有聚合结果

        inserted_count = 0  # 记录成功插入的条数
        skipped_count = 0  # 记录因重复被跳过的条数

        for row in rows:  # 遍历每一条小时级聚合结果
            (
                device_id,
                hour_time,
                hour_date,
                point_count,
                sum_in_count,
                sum_out_count,
                avg_activity_value,
                max_activity_value,
                min_activity_value,
            ) = row  # 解包字段

            cursor.execute(  # 把小时级结果写入 bee_activity_hourly 表
                """
                INSERT OR IGNORE INTO bee_activity_hourly
                (
                    device_id,
                    hour_time,
                    hour_date,
                    point_count,
                    sum_in_count,
                    sum_out_count,
                    avg_activity_value,
                    max_activity_value,
                    min_activity_value,
                    source
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    device_id,  # 写入设备编号
                    hour_time,  # 写入小时时间点
                    hour_date,  # 写入日期
                    point_count,  # 写入该小时包含的数据点数
                    sum_in_count,  # 写入该小时进入总数
                    sum_out_count,  # 写入该小时离开总数
                    avg_activity_value,  # 写入该小时平均活跃度
                    max_activity_value,  # 写入该小时最大活跃度
                    min_activity_value,  # 写入该小时最小活跃度
                    "sensor",  # 标记数据来源为传感器
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
    build_bee_activity_hourly()  # 执行生成函数