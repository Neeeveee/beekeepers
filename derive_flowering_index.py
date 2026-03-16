# -*- coding: utf-8 -*-  # 指定源码编码为 UTF-8，避免中文乱码

import sqlite3  # 导入 sqlite3，用于连接 SQLite 数据库
from pathlib import Path  # 导入 Path，用于处理文件路径
from datetime import datetime, date  # 导入 datetime 和 date，用于日期计算与写入时间


def mmdd_to_day_of_year(mmdd: str, ref_year: int = 2026) -> int:  # 定义函数：把 MM-DD 转为一年中的第几天
    month, day = map(int, mmdd.split("-"))  # 拆分月和日并转为整数
    return date(ref_year, month, day).timetuple().tm_yday  # 返回该日期对应的年内序号


def get_date_day_of_year(date_str: str) -> int:  # 定义函数：把 YYYY-MM-DD 转为一年中的第几天
    dt = datetime.strptime(date_str, "%Y-%m-%d").date()  # 把字符串解析成日期对象
    return dt.timetuple().tm_yday  # 返回年内序号


def is_cross_year(start_mmdd: str, end_mmdd: str) -> bool:  # 定义函数：判断花期窗口是否跨年
    start_month, start_day = map(int, start_mmdd.split("-"))  # 解析起始日期
    end_month, end_day = map(int, end_mmdd.split("-"))  # 解析结束日期
    return (start_month, start_day) > (end_month, end_day)  # 如果起始晚于结束，说明跨年


def clamp(value: float, min_value: float = 0.0, max_value: float = 1.0) -> float:  # 定义截断函数，限制结果范围
    return max(min_value, min(max_value, value))  # 将 value 限制在最小值和最大值之间


def calc_base_season_score(model_date: str, bloom_start_mmdd: str, bloom_end_mmdd: str) -> float:  # 定义函数：计算基准花期得分
    doy = get_date_day_of_year(model_date)  # 计算当前日期在一年中的位置
    start_doy = mmdd_to_day_of_year(bloom_start_mmdd)  # 计算花期起始日期在一年中的位置
    end_doy = mmdd_to_day_of_year(bloom_end_mmdd)  # 计算花期结束日期在一年中的位置

    in_window = False  # 先假设当前日期不在花期窗口内
    progress = None  # 初始化花期进度为空

    if is_cross_year(bloom_start_mmdd, bloom_end_mmdd):  # 如果是跨年花期
        total_days = (365 - start_doy) + end_doy  # 计算跨年窗口总长度
        if doy >= start_doy:  # 如果当前日期在起始日期之后
            progress = (doy - start_doy) / max(1, total_days)  # 计算窗口进度
            in_window = True  # 标记为在窗口内
        elif doy <= end_doy:  # 如果当前日期在年初且早于结束日期
            progress = ((365 - start_doy) + doy) / max(1, total_days)  # 计算跨年后的进度
            in_window = True  # 标记为在窗口内
    else:  # 如果不是跨年花期
        if start_doy <= doy <= end_doy:  # 如果当前日期在窗口内
            progress = (doy - start_doy) / max(1, (end_doy - start_doy))  # 计算窗口进度
            in_window = True  # 标记为在窗口内

    if not in_window:  # 如果不在花期窗口内
        return 0.12  # 返回一个较低但不至于过度归零的基础值

    if progress < 0.20:  # 如果进度在前 20%
        return 0.45  # 返回启动期基础值
    elif progress < 0.40:  # 如果进度在 20%-40%
        return 0.70  # 返回上升期基础值
    elif progress < 0.70:  # 如果进度在 40%-70%
        return 1.00  # 返回盛花期最高基础值
    elif progress < 0.90:  # 如果进度在 70%-90%
        return 0.75  # 返回回落期基础值
    else:  # 如果进度在最后 10%
        return 0.45  # 返回尾段基础值


def calc_temp_factor(avg_temp_c: float) -> float:  # 定义函数：计算温度修正因子
    if avg_temp_c is None:  # 如果温度为空
        return 0.55  # 返回默认值
    elif avg_temp_c < 5:  # 如果温度低于 5℃
        return 0.35  # 返回低温抑制值
    elif avg_temp_c < 10:  # 如果温度在 5-10℃
        return 0.60  # 返回偏低温修正值
    elif avg_temp_c < 15:  # 如果温度在 10-15℃
        return 0.82  # 返回较适中值
    elif avg_temp_c <= 22:  # 如果温度在 15-22℃
        return 1.00  # 返回最优值
    elif avg_temp_c <= 28:  # 如果温度在 22-28℃
        return 0.88  # 返回偏高温修正值
    else:  # 如果温度大于 28℃
        return 0.65  # 返回高温抑制值


def calc_humidity_factor(avg_humidity_pct: float) -> float:  # 定义函数：计算湿度修正因子
    if avg_humidity_pct is None:  # 如果湿度为空
        return 0.90  # 返回默认值
    elif avg_humidity_pct < 35:  # 如果湿度低于 35%
        return 0.85  # 返回偏干修正值
    elif avg_humidity_pct < 50:  # 如果湿度在 35%-50%
        return 0.95  # 返回较好修正值
    elif avg_humidity_pct <= 80:  # 如果湿度在 50%-80%
        return 1.00  # 返回最优值
    elif avg_humidity_pct <= 90:  # 如果湿度在 80%-90%
        return 0.90  # 返回偏湿修正值
    else:  # 如果湿度高于 90%
        return 0.75  # 返回高湿抑制值


def calc_rain_factor(precip_mm: float) -> float:  # 定义函数：计算降雨修正因子
    if precip_mm is None:  # 如果降雨为空
        return 1.00  # 按无雨处理
    elif precip_mm == 0:  # 如果无雨
        return 1.00  # 返回最优值
    elif precip_mm < 1:  # 如果是小雨
        return 0.88  # 返回轻度抑制值
    elif precip_mm < 5:  # 如果是中雨
        return 0.68  # 返回明显抑制值
    else:  # 如果是大雨
        return 0.45  # 返回强抑制值


def calc_resource_factor(nectar_grade, pollen_grade, confidence) -> float:  # 定义函数：计算植物资源价值轻修正
    nectar_grade = nectar_grade or 0  # 如果泌蜜等级为空则按 0 处理
    pollen_grade = pollen_grade or 0  # 如果花粉等级为空则按 0 处理
    confidence = confidence or 0.5  # 如果可信度为空则按 0.5 处理

    factor = 1.0  # 初始化资源修正因子为 1.0

    if nectar_grade >= 5:  # 如果泌蜜等级很高
        factor += 0.08  # 增加修正
    elif nectar_grade >= 4:  # 如果泌蜜等级较高
        factor += 0.05  # 增加修正
    elif nectar_grade >= 3:  # 如果泌蜜等级中等
        factor += 0.03  # 增加修正

    if pollen_grade >= 4:  # 如果花粉等级较高
        factor += 0.04  # 增加修正
    elif pollen_grade >= 3:  # 如果花粉等级中等
        factor += 0.02  # 增加修正

    if confidence < 0.6:  # 如果资料可信度偏低
        factor -= 0.05  # 略微降低修正

    return round(max(0.88, min(1.15, factor)), 2)  # 限制资源因子范围并保留两位小数


def calc_flowering_stage(flowering_index: float) -> str:  # 定义函数：根据花期指数映射阶段
    if flowering_index < 0.15:  # 如果指数小于 0.15
        return "未开花"  # 返回未开花
    elif flowering_index < 0.40:  # 如果指数小于 0.40
        return "花期启动"  # 返回花期启动
    elif flowering_index < 0.70:  # 如果指数小于 0.70
        return "初花期"  # 返回初花期
    elif flowering_index < 0.90:  # 如果指数小于 0.90
        return "盛花期"  # 返回盛花期
    else:  # 其余情况
        return "最佳花期"  # 返回最佳花期


def main():  # 定义主函数
    base_dir = Path(__file__).resolve().parent  # 获取当前脚本所在目录
    db_path = base_dir / "bee_env.db"  # 拼接数据库路径

    conn = sqlite3.connect(db_path)  # 连接数据库
    conn.row_factory = sqlite3.Row  # 设置返回行为字典风格
    cur = conn.cursor()  # 创建游标

    # 定义读取植物参数的 SQL
    plant_sql = """
    SELECT
        id,
        site_id,
        plant_name,
        bloom_start_mmdd,
        bloom_end_mmdd,
        bloom_days,
        nectar_grade,
        pollen_grade,
        confidence
    FROM nectar_plants
    ORDER BY plant_name ASC
    """
    plants = cur.execute(plant_sql).fetchall()  # 执行查询并取出全部植物记录

    # 定义读取每日环境数据的 SQL
    env_sql = """
    SELECT
        substr(event_time, 1, 10) AS model_date,
        AVG(temperature_c) AS avg_temp_c,
        AVG(humidity_pct) AS avg_humidity_pct,
        SUM(COALESCE(precip_mm, 0)) AS precip_mm
    FROM eco_time_series
    GROUP BY substr(event_time, 1, 10)
    ORDER BY model_date ASC
    """
    env_rows = cur.execute(env_sql).fetchall()  # 执行查询并取出全部环境记录

    print(f"读取到 {len(plants)} 个植物")  # 打印植物数量
    print(f"读取到 {len(env_rows)} 天环境数据")  # 打印环境天数
    print("-" * 70)  # 打印分隔线

    processed_count = 0  # 初始化处理计数

    for plant in plants:  # 遍历每一条植物记录
        site_id = plant["site_id"]  # 读取站点 ID
        plant_name = plant["plant_name"]  # 读取植物名
        bloom_start_mmdd = plant["bloom_start_mmdd"]  # 读取花期开始日期
        bloom_end_mmdd = plant["bloom_end_mmdd"]  # 读取花期结束日期
        nectar_grade = plant["nectar_grade"]  # 读取泌蜜等级
        pollen_grade = plant["pollen_grade"]  # 读取花粉等级
        confidence = plant["confidence"]  # 读取可信度

        for env in env_rows:  # 遍历每一天环境数据
            model_date = env["model_date"]  # 读取模型日期
            avg_temp_c = env["avg_temp_c"]  # 读取平均温度
            avg_humidity_pct = env["avg_humidity_pct"]  # 读取平均湿度
            precip_mm = env["precip_mm"]  # 读取降雨量

            base_flowering_score = calc_base_season_score(
                model_date=model_date,
                bloom_start_mmdd=bloom_start_mmdd,
                bloom_end_mmdd=bloom_end_mmdd
            )  # 计算基准花期得分

            temp_factor = calc_temp_factor(avg_temp_c)  # 计算温度因子
            humidity_factor = calc_humidity_factor(avg_humidity_pct)  # 计算湿度因子
            rain_factor = calc_rain_factor(precip_mm)  # 计算降雨因子
            resource_factor = calc_resource_factor(nectar_grade, pollen_grade, confidence)  # 计算资源价值修正

            env_modifier = (
                0.55 * temp_factor + 0.20 * humidity_factor + 0.25 * rain_factor
            )  # 计算环境综合修正因子

            flowering_index = base_flowering_score * env_modifier * resource_factor  # 计算最终花期指数
            flowering_index = round(clamp(flowering_index), 3)  # 限制在 0~1 并保留三位小数
            flowering_stage = calc_flowering_stage(flowering_index)  # 计算花期阶段

            print(f"date={model_date} | plant={plant_name}")  # 打印当前日期和植物名
            print(f"  base_flowering_score = {base_flowering_score}")  # 打印基准花期得分
            print(f"  temp_factor          = {temp_factor}")  # 打印温度因子
            print(f"  humidity_factor      = {humidity_factor}")  # 打印湿度因子
            print(f"  rain_factor          = {rain_factor}")  # 打印降雨因子
            print(f"  env_modifier         = {round(env_modifier, 3)}")  # 打印环境综合因子
            print(f"  resource_factor      = {resource_factor}")  # 打印资源价值因子
            print(f"  flowering_index      = {flowering_index}")  # 打印花期指数
            print(f"  flowering_stage      = {flowering_stage}")  # 打印花期阶段
            print()  # 打印空行

            # 定义写入 flowering_model_daily 的 SQL
            insert_sql = """
            INSERT INTO flowering_model_daily (
                site_id,
                plant_name,
                model_date,
                avg_temp_c,
                avg_humidity_pct,
                precip_mm,
                base_flowering_score,
                temp_factor,
                humidity_factor,
                rain_factor,
                flowering_index,
                flowering_stage,
                source,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(site_id, plant_name, model_date) DO UPDATE SET
                avg_temp_c = excluded.avg_temp_c,
                avg_humidity_pct = excluded.avg_humidity_pct,
                precip_mm = excluded.precip_mm,
                base_flowering_score = excluded.base_flowering_score,
                temp_factor = excluded.temp_factor,
                humidity_factor = excluded.humidity_factor,
                rain_factor = excluded.rain_factor,
                flowering_index = excluded.flowering_index,
                flowering_stage = excluded.flowering_stage,
                source = excluded.source,
                created_at = excluded.created_at
            """

            cur.execute(
                insert_sql,
                (
                    site_id,
                    plant_name,
                    model_date,
                    avg_temp_c,
                    avg_humidity_pct,
                    precip_mm,
                    base_flowering_score,
                    temp_factor,
                    humidity_factor,
                    rain_factor,
                    flowering_index,
                    flowering_stage,
                    "rule_v2_window_plus_env_blend",
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                )
            )  # 执行写入

            processed_count += 1  # 处理计数加一

    conn.commit()  # 提交事务
    conn.close()  # 关闭数据库连接

    print("-" * 70)  # 打印分隔线
    print(f"写入完成，共处理 {processed_count} 条花期结果")  # 打印处理总数


if __name__ == "__main__":  # 如果当前脚本直接运行
    main()  # 执行主函数