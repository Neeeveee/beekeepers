# -*- coding: utf-8 -*-  # 指定源码编码为 UTF-8，避免中文乱码

import sqlite3  # 导入 sqlite3，用于连接 SQLite 数据库
from pathlib import Path  # 导入 Path，用于处理文件路径
from datetime import datetime  # 导入 datetime，用于写入创建时间


def clamp(value, min_value=0.0, max_value=1.0):  # 定义截断函数，限制范围到 0~1
    return max(min_value, min(max_value, value))  # 返回截断后的值


def calc_nectar_resource_factor(nectar_grade, avg_yield_kg_per_colony, confidence):  # 定义函数：计算植物基础泌蜜能力因子
    nectar_grade = nectar_grade or 0  # 如果泌蜜等级为空则按 0 处理
    avg_yield_kg_per_colony = avg_yield_kg_per_colony or 0.0  # 如果平均产量为空则按 0 处理
    confidence = confidence or 0.5  # 如果可信度为空则按 0.5 处理

    if nectar_grade >= 5:  # 如果泌蜜等级很高
        grade_factor = 1.00  # 设置最高等级因子
    elif nectar_grade == 4:  # 如果泌蜜等级为 4
        grade_factor = 0.90  # 设置较高等级因子
    elif nectar_grade == 3:  # 如果泌蜜等级为 3
        grade_factor = 0.75  # 设置中等级因子
    elif nectar_grade == 2:  # 如果泌蜜等级为 2
        grade_factor = 0.60  # 设置较低等级因子
    else:  # 其余情况
        grade_factor = 0.45  # 设置最低等级因子

    if avg_yield_kg_per_colony >= 30:  # 如果平均产量大于等于 30
        yield_factor = 1.10  # 设置较高产量因子
    elif avg_yield_kg_per_colony >= 20:  # 如果平均产量在 20-30
        yield_factor = 1.00  # 设置标准产量因子
    elif avg_yield_kg_per_colony >= 10:  # 如果平均产量在 10-20
        yield_factor = 0.90  # 设置偏低产量因子
    else:  # 其余情况
        yield_factor = 0.80  # 设置低产量因子

    if confidence >= 0.8:  # 如果可信度较高
        confidence_factor = 1.00  # 不降低
    elif confidence >= 0.6:  # 如果可信度中等
        confidence_factor = 0.95  # 略微降低
    else:  # 如果可信度偏低
        confidence_factor = 0.90  # 进一步降低

    factor = grade_factor * yield_factor * confidence_factor  # 计算综合资源能力因子

    return round(max(0.35, min(1.10, factor)), 3)  # 限制范围并保留三位小数


def calc_nectar_temp_factor(avg_temp_c):  # 定义函数：计算花蜜量温度修正因子
    if avg_temp_c is None:  # 如果温度为空
        return 0.75  # 返回默认值
    elif avg_temp_c < 10:  # 如果温度低于 10℃
        return 0.55  # 返回较低值
    elif avg_temp_c < 15:  # 如果温度在 10-15℃
        return 0.82  # 返回偏低值
    elif avg_temp_c < 22:  # 如果温度在 15-22℃
        return 1.00  # 返回最优值
    elif avg_temp_c < 28:  # 如果温度在 22-28℃
        return 1.02  # 返回最优值
    elif avg_temp_c < 32:  # 如果温度在 28-32℃
        return 0.90  # 返回偏高温修正
    else:  # 如果温度高于 32℃
        return 0.72  # 返回高温抑制值


def calc_nectar_humidity_factor(avg_humidity_pct):  # 定义函数：计算花蜜量湿度修正因子
    if avg_humidity_pct is None:  # 如果湿度为空
        return 0.95  # 返回默认值
    elif avg_humidity_pct < 35:  # 如果湿度低于 35%
        return 0.85  # 返回偏干值
    elif avg_humidity_pct < 50:  # 如果湿度在 35%-50%
        return 1.00  # 返回最优值
    elif avg_humidity_pct <= 85:  # 如果湿度在 50%-85%
        return 1.00  # 返回最优值
    elif avg_humidity_pct <= 92:  # 如果湿度在 85%-92%
        return 0.94  # 返回偏湿值
    else:  # 如果湿度高于 85%
        return 0.84  # 返回高湿抑制值


def calc_nectar_rain_factor(precip_mm):  # 定义函数：计算花蜜量降雨修正因子
    if precip_mm is None:  # 如果降雨为空
        return 1.00  # 按无雨处理
    elif precip_mm == 0:  # 如果无雨
        return 1.00  # 返回最优值
    elif precip_mm < 1:  # 如果是小雨
        return 0.92  # 返回轻度抑制值
    elif precip_mm < 5:  # 如果是中雨
        return 0.72  # 返回明显抑制值
    else:  # 如果是大雨
        return 0.45  # 返回强抑制值


def calc_supply_level(nectar_supply_index):  # 定义函数：根据花蜜量指数映射供给等级
    if nectar_supply_index < 0.15:  # 如果指数低于 0.15
        return "很低"  # 返回很低
    elif nectar_supply_index < 0.35:  # 如果指数低于 0.35
        return "较低"  # 返回较低
    elif nectar_supply_index < 0.55:  # 如果指数低于 0.55
        return "中等"  # 返回中等
    elif nectar_supply_index < 0.75:  # 如果指数低于 0.75
        return "较高"  # 返回较高
    else:  # 其余情况
        return "高"  # 返回高


def create_table_if_not_exists(cur):  # 定义函数：自动创建结果表
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS nectar_supply_model_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id INTEGER,
            plant_name TEXT,
            model_date TEXT,
            flowering_index REAL,
            avg_temp_c REAL,
            avg_humidity_pct REAL,
            precip_mm REAL,
            nectar_resource_factor REAL,
            temp_factor REAL,
            humidity_factor REAL,
            rain_factor REAL,
            nectar_supply_index REAL,
            supply_level TEXT,
            source TEXT,
            created_at TEXT,
            UNIQUE(site_id, plant_name, model_date)
        )
        """
    )  # 建表结束


def main():  # 定义主函数
    base_dir = Path(__file__).resolve().parent  # 获取当前脚本所在目录
    db_path = base_dir / "bee_env.db"  # 拼接数据库路径

    conn = sqlite3.connect(db_path)  # 连接数据库
    conn.row_factory = sqlite3.Row  # 设置返回行为字典风格
    cur = conn.cursor()  # 创建游标

    create_table_if_not_exists(cur)  # 自动创建结果表

    # 定义读取植物资源信息的 SQL
    plant_sql = """
    SELECT
        site_id,
        plant_name,
        nectar_grade,
        pollen_grade,
        avg_yield_kg_per_colony,
        confidence
    FROM nectar_plants
    ORDER BY plant_name ASC
    """
    plant_rows = cur.execute(plant_sql).fetchall()  # 执行查询并获取植物资源数据

    plant_meta = {}  # 初始化植物元数据字典
    for row in plant_rows:  # 遍历每条植物记录
        plant_meta[row["plant_name"]] = {
            "site_id": row["site_id"],
            "nectar_grade": row["nectar_grade"],
            "pollen_grade": row["pollen_grade"],
            "avg_yield_kg_per_colony": row["avg_yield_kg_per_colony"],
            "confidence": row["confidence"],
        }  # 当前植物元数据构建结束

    # 定义读取花期模型结果的 SQL
    flowering_sql = """
    SELECT
        site_id,
        plant_name,
        model_date,
        flowering_index,
        avg_temp_c,
        avg_humidity_pct,
        precip_mm
    FROM flowering_model_daily
    ORDER BY model_date ASC, plant_name ASC
    """
    flowering_rows = cur.execute(flowering_sql).fetchall()  # 执行查询并获取花期结果

    print(f"读取到 {len(plant_rows)} 个植物资源记录")  # 打印植物资源记录数
    print(f"读取到 {len(flowering_rows)} 条花期模型结果")  # 打印花期模型记录数
    print("-" * 70)  # 打印分隔线

    processed_count = 0  # 初始化处理计数

    for row in flowering_rows:  # 遍历每一条花期结果
        site_id = row["site_id"]  # 读取站点 ID
        plant_name = row["plant_name"]  # 读取植物名
        model_date = row["model_date"]  # 读取模型日期
        flowering_index = row["flowering_index"] or 0.0  # 读取花期指数，空值按 0 处理
        avg_temp_c = row["avg_temp_c"]  # 读取平均温度
        avg_humidity_pct = row["avg_humidity_pct"]  # 读取平均湿度
        precip_mm = row["precip_mm"]  # 读取降雨量

        meta = plant_meta.get(plant_name)  # 从植物元数据字典中取出当前植物信息
        if not meta:  # 如果当前植物没找到资源信息
            print(f"⚠ 未找到植物资源信息：{plant_name}，已跳过")  # 打印提示
            continue  # 跳过当前记录

        nectar_resource_factor = calc_nectar_resource_factor(
            meta["nectar_grade"],
            meta["avg_yield_kg_per_colony"],
            meta["confidence"]
        )  # 计算泌蜜资源因子

        temp_factor = calc_nectar_temp_factor(avg_temp_c)  # 计算温度因子
        humidity_factor = calc_nectar_humidity_factor(avg_humidity_pct)  # 计算湿度因子
        rain_factor = calc_nectar_rain_factor(precip_mm)  # 计算降雨因子

        env_modifier = (
            0.50 * temp_factor + 0.20 * humidity_factor + 0.30 * rain_factor
        )  # 计算花蜜量环境综合修正因子

        nectar_resource_modifier = 0.80 + 0.30 * clamp(nectar_resource_factor, 0.0, 1.1) / 1.1  # 把资源能力映射成更符合直觉的修正值

        nectar_supply_index = flowering_index * nectar_resource_modifier * env_modifier  # 计算最终花蜜量指数
        nectar_supply_index = round(clamp(nectar_supply_index), 3)  # 限制在 0~1 并保留三位小数

        supply_level = calc_supply_level(nectar_supply_index)  # 计算供给等级

        print(f"date={model_date} | plant={plant_name}")  # 打印当前日期与植物名
        print(f"  flowering_index         = {flowering_index}")  # 打印花期指数
        print(f"  nectar_resource_factor  = {nectar_resource_factor}")  # 打印泌蜜资源因子
        print(f"  temp_factor             = {temp_factor}")  # 打印温度因子
        print(f"  humidity_factor         = {humidity_factor}")  # 打印湿度因子
        print(f"  rain_factor             = {rain_factor}")  # 打印降雨因子
        print(f"  env_modifier            = {round(env_modifier, 3)}")  # 打印环境综合修正
        print(f"  nectar_resource_mod     = {round(nectar_resource_modifier, 3)}")  # 打印资源综合修正
        print(f"  nectar_supply_index     = {nectar_supply_index}")  # 打印花蜜量指数
        print(f"  supply_level            = {supply_level}")  # 打印供给等级
        print()  # 打印空行

        # 定义写入 nectar_supply_model_daily 的 SQL
        insert_sql = """
        INSERT INTO nectar_supply_model_daily (
            site_id,
            plant_name,
            model_date,
            flowering_index,
            avg_temp_c,
            avg_humidity_pct,
            precip_mm,
            nectar_resource_factor,
            temp_factor,
            humidity_factor,
            rain_factor,
            nectar_supply_index,
            supply_level,
            source,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(site_id, plant_name, model_date) DO UPDATE SET
            flowering_index = excluded.flowering_index,
            avg_temp_c = excluded.avg_temp_c,
            avg_humidity_pct = excluded.avg_humidity_pct,
            precip_mm = excluded.precip_mm,
            nectar_resource_factor = excluded.nectar_resource_factor,
            temp_factor = excluded.temp_factor,
            humidity_factor = excluded.humidity_factor,
            rain_factor = excluded.rain_factor,
            nectar_supply_index = excluded.nectar_supply_index,
            supply_level = excluded.supply_level,
            source = excluded.source,
            created_at = excluded.created_at
        """

        cur.execute(
            insert_sql,
            (
                site_id,
                plant_name,
                model_date,
                flowering_index,
                avg_temp_c,
                avg_humidity_pct,
                precip_mm,
                nectar_resource_factor,
                temp_factor,
                humidity_factor,
                rain_factor,
                nectar_supply_index,
                supply_level,
                "rule_v2_flowering_blend_env_resource_lift",
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
        )  # 执行写入

        processed_count += 1  # 处理计数加一

    conn.commit()  # 提交事务
    conn.close()  # 关闭数据库连接

    print("-" * 70)  # 打印分隔线
    print(f"写入完成，共处理 {processed_count} 条花蜜量结果")  # 打印处理总数


if __name__ == "__main__":  # 如果当前脚本作为主程序执行
    main()  # 执行主函数
