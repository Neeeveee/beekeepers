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


def clamp(value: float, min_value: float = 0.0, max_value: float = 1.0) -> float:  # 定义截断函数，避免指数超范围
    return max(min_value, min(max_value, value))  # 把值限制在最小值和最大值之间


def build_flowering_index_map(cursor: sqlite3.Cursor) -> dict:  # 构建花期指数映射表
    cursor.execute(
        """
        SELECT
            model_date,
            flowering_index
        FROM flowering_model_daily
        """
    )  # 查询花期模型结果表

    result = {}  # 初始化结果字典
    for model_date, flowering_index in cursor.fetchall():  # 遍历查询结果
        result[str(model_date)] = float(flowering_index or 0.0)  # 保存为 日期 -> 花期指数
    return result  # 返回映射字典


def build_nectar_index_map(cursor: sqlite3.Cursor) -> dict:  # 构建花蜜量指数映射表
    cursor.execute(
        """
        SELECT
            model_date,
            nectar_supply_index
        FROM nectar_supply_model_daily
        """
    )  # 查询花蜜量模型结果表

    result = {}  # 初始化结果字典
    for model_date, nectar_supply_index in cursor.fetchall():  # 遍历查询结果
        result[str(model_date)] = float(nectar_supply_index or 0.0)  # 保存为 日期 -> 花蜜量指数
    return result  # 返回映射字典


def build_bee_activity_curve() -> None:  # 定义生成蜜蜂活跃度曲线表的主函数
    conn = get_db_connection()  # 获取数据库连接
    try:  # 开始数据库操作
        cursor = conn.cursor()  # 创建游标

        flowering_index_map = build_flowering_index_map(cursor)  # 预先读取花期指数
        nectar_index_map = build_nectar_index_map(cursor)  # 预先读取花蜜量指数

        cursor.execute(  # 查询原始蜜蜂计数表中的数据
            """
            SELECT
                device_id,
                event_time,
                in_count,
                out_count,
                daily_in,
                daily_out
            FROM bee_counter_raw
            ORDER BY event_time ASC
            """
        )

        rows = cursor.fetchall()  # 取出所有查询结果

        inserted_count = 0  # 记录成功插入的条数
        skipped_count = 0  # 记录因重复被跳过的条数

        for row in rows:  # 遍历每一条原始数据
            device_id, event_time, in_count, out_count, daily_in, daily_out = row  # 解包字段

            curve_date = str(event_time)[:10]  # 从 event_time 中截取日期部分，格式如 2026-03-07

            in_count = int(in_count or 0)  # 空值按 0 处理
            out_count = int(out_count or 0)  # 空值按 0 处理
            daily_in = int(daily_in or 0)  # 空值按 0 处理
            daily_out = int(daily_out or 0)  # 空值按 0 处理

            base_sensor_activity = in_count + out_count  # 先用传感器原始进出总量作为基础活跃度

            flowering_index = float(flowering_index_map.get(curve_date, 0.0))  # 获取当天花期指数，缺失则按 0
            nectar_index = float(nectar_index_map.get(curve_date, 0.0))  # 获取当天花蜜量指数，缺失则按 0

            flowering_index = clamp(flowering_index)  # 限制到 0~1
            nectar_index = clamp(nectar_index)  # 限制到 0~1

            resource_score = 0.5 * flowering_index + 0.5 * nectar_index  # 花期与花蜜量等权合成为资源得分
            resource_factor = 0.4 + 0.6 * resource_score  # 把资源得分映射为 0.4~1.0 的修正因子

            activity_value = base_sensor_activity * resource_factor  # 用资源因子修正基础活跃度
            activity_value = round(activity_value, 4)  # 保留 4 位小数，便于后续使用

            cursor.execute(  # 把整理后的数据写入 bee_activity_curve 表
                """
                INSERT OR IGNORE INTO bee_activity_curve
                (
                    device_id,
                    curve_date,
                    event_time,
                    in_count,
                    out_count,
                    activity_value,
                    daily_in,
                    daily_out,
                    source
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    device_id,  # 写入设备编号
                    curve_date,  # 写入曲线所属日期
                    event_time,  # 写入时间点
                    in_count,  # 写入进入数量
                    out_count,  # 写入离开数量
                    activity_value,  # 写入计算后的活跃度值
                    daily_in,  # 写入当天累计进入数量
                    daily_out,  # 写入当天累计离开数量
                    "sensor+resource",  # 标记数据来源为传感器+资源修正
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
    build_bee_activity_curve()  # 执行生成函数
