import sqlite3  # 导入SQLite模块
from pathlib import Path  # 导入路径模块
from datetime import datetime  # 导入时间模块
import json  # 导入json模块
import csv  # 导入csv模块
import os  # 导入系统模块

# =========================
# 说明：导出“latest_activity.csv/json”
# - 默认导出 expected_activity_daily 的 expected_min/max（代表采集活跃预期区间）
# - 如果存在扩展字段（nectar_availability、expected_weather_* 等），也一并导出
# =========================

def table_exists(conn, name):  # 判断表是否存在
    row = conn.execute(  # 查询sqlite_master
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1;",
        (name,),
    ).fetchone()  # 获取一行
    return bool(row)  # 返回True/False

def get_columns(conn, table):  # 获取表字段列表
    rows = conn.execute(f"PRAGMA table_info({table});").fetchall()  # 查询表结构
    return [r[1] for r in rows]  # 返回字段名列表

def export_latest(db_path, site_id=1, species_code="CHINESE_BEE"):  # 导出主函数
    print("[1] export_activity_results.py START")  # 输出开始
    conn = sqlite3.connect(db_path)  # 连接数据库

    base_dir = str(Path(db_path).resolve().parent)  # 基准目录
    print(f"[2] base_dir = {base_dir}")  # 输出
    print(f"[3] db_path  = {db_path}")  # 输出

    # 检查表是否存在  # 说明
    has_expected = table_exists(conn, "expected_activity_daily")  # expected表
    has_validation = table_exists(conn, "validation_activity_daily")  # validation表
    has_weather = table_exists(conn, "daily_weather_summary")  # weather表
    has_rule = table_exists(conn, "behavior_rule_library")  # rule表

    print("[4] table exists:")  # 输出
    print("    expected  =", has_expected, "expected_activity_daily")  # 输出
    print("    validation=", has_validation, "validation_activity_daily")  # 输出
    print("    weather   =", has_weather, "daily_weather_summary")  # 输出
    print("    rule      =", has_rule, "behavior_rule_library")  # 输出

    if not has_expected:  # 没有expected表
        print("❌ expected_activity_daily 不存在：请先跑 derive_expected_activity.py")  # 提示
        conn.close()  # 关闭连接
        return  # 退出

    # 统计行数  # 说明
    expected_rows = conn.execute("SELECT COUNT(*) FROM expected_activity_daily;").fetchone()[0]  # 计数
    validation_rows = conn.execute("SELECT COUNT(*) FROM validation_activity_daily;").fetchone()[0] if has_validation else 0  # 计数
    weather_rows = conn.execute("SELECT COUNT(*) FROM daily_weather_summary;").fetchone()[0] if has_weather else 0  # 计数

    print("[5] row counts:")  # 输出
    print("    expected rows  =", expected_rows)  # 输出
    print("    validation rows=", validation_rows)  # 输出
    print("    weather rows   =", weather_rows)  # 输出

    print(f"[6] filter: site_id = {site_id} , species_code = {species_code}")  # 输出

    # 根据字段是否存在，动态拼接SELECT列  # 说明
    exp_cols = get_columns(conn, "expected_activity_daily")  # 获取字段
    extra_cols = []  # 扩展字段列表
    for c in ["nectar_availability", "expected_weather_min", "expected_weather_max", "expected_foraging_min", "expected_foraging_max"]:  # 需要的扩展字段
        if c in exp_cols:  # 如果存在
            extra_cols.append(c)  # 加入

    select_cols = [
        "site_id", "species_code", "date",
        "expected_min", "expected_max",
        "peak_start_hour", "peak_end_hour",
        "rule_id", "confidence", "explain_text"
    ] + extra_cols  # 合并字段

    select_sql = "SELECT " + ", ".join(select_cols) + " FROM expected_activity_daily WHERE site_id=? AND species_code=? ORDER BY date ASC;"  # SQL
    exp = conn.execute(select_sql, (site_id, species_code)).fetchall()  # 查询数据
    print("[7] expected matched rows =", len(exp))  # 输出

    # 读取validation（若存在）  # 说明
    val_map = {}  # validation映射
    if has_validation:  # 有表才读
        val_rows = conn.execute(  # 查询validation
            """
            SELECT date, observed_activity, match_score, deviation_tag, deviation_value, explain_text
            FROM validation_activity_daily
            WHERE site_id=? AND species_code=?
            ORDER BY date ASC;
            """,
            (site_id, species_code),
        ).fetchall()  # 获取多行
        for r in val_rows:  # 遍历
            val_map[r[0]] = {  # 用date做key
                "observed_activity": r[1],  # 实测
                "match_score": r[2],  # 分数
                "deviation_tag": r[3],  # 标签
                "deviation_value": r[4],  # 偏离
                "validation_explain": r[5],  # 解释
            }
        print("[8] validation matched rows =", len(val_rows))  # 输出
    else:
        print("[8] validation matched rows = 0")  # 输出

    # 合并输出  # 说明
    merged = []  # 合并列表
    for row in exp:  # 遍历expected
        data = dict(zip(select_cols, row))  # 转字典
        day = data["date"]  # 日期
        if day in val_map:  # 如果有验证
            data.update(val_map[day])  # 合并
        merged.append(data)  # 加入
    print("[9] merged rows =", len(merged))  # 输出

    # 写json/csv  # 说明
    latest_json = str(Path(base_dir) / "latest_activity.json")  # 最新json
    latest_csv = str(Path(base_dir) / "latest_activity.csv")  # 最新csv

    # 写json  # 说明
    with open(latest_json, "w", encoding="utf-8") as f:  # 打开文件
        json.dump(merged, f, ensure_ascii=False, indent=2)  # 写入

    # 写csv  # 说明
    # csv字段：按 merged 的key集合稳定排序（先按我们定义的，再补剩余）  # 说明
    fieldnames = []  # 字段名
    for k in select_cols + ["observed_activity", "match_score", "deviation_tag", "deviation_value", "validation_explain"]:  # 预设顺序
        if k not in fieldnames:  # 防重
            fieldnames.append(k)  # 添加
    # 补齐其他可能字段  # 说明
    for item in merged:  # 遍历
        for k in item.keys():  # 遍历key
            if k not in fieldnames:  # 不存在则加
                fieldnames.append(k)  # 添加

    with open(latest_csv, "w", newline="", encoding="utf-8-sig") as f:  # 打开文件
        writer = csv.DictWriter(f, fieldnames=fieldnames)  # writer
        writer.writeheader()  # 写表头
        for item in merged:  # 写每行
            writer.writerow(item)  # 写入

    # 额外存档文件  # 说明
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")  # 时间戳
    archive_json = str(Path(base_dir) / f"export_activity_{ts}.json")  # 存档json
    archive_csv = str(Path(base_dir) / f"export_activity_{ts}.csv")  # 存档csv
    with open(archive_json, "w", encoding="utf-8") as f:  # 写存档json
        json.dump(merged, f, ensure_ascii=False, indent=2)  # 写入
    with open(archive_csv, "w", newline="", encoding="utf-8-sig") as f:  # 写存档csv
        writer = csv.DictWriter(f, fieldnames=fieldnames)  # writer
        writer.writeheader()  # 表头
        for item in merged:  # 写每行
            writer.writerow(item)  # 写入

    print("[10] DONE. files created:")  # 输出
    print("     ", latest_json)  # 输出
    print("     ", latest_csv)  # 输出
    print("     ", archive_json)  # 输出
    print("     ", archive_csv)  # 输出

    conn.close()  # 关闭连接

if __name__ == "__main__":  # 入口
    base_dir = Path(__file__).resolve().parent  # 脚本目录
    db_path = str(base_dir / "bee_env.db")  # 默认db路径
    export_latest(db_path=db_path, site_id=1, species_code="CHINESE_BEE")  # 执行导出
