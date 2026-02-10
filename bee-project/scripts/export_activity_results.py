# -*- coding: utf-8 -*-
import os
import csv
import json
import sqlite3
from datetime import datetime

def table_exists(cur, name):
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?;", (name,))
    return cur.fetchone() is not None

def scalar(cur, sql, params=()):
    cur.execute(sql, params)
    row = cur.fetchone()
    return None if row is None else row[0]

def main():
    print("[1] export_activity_results.py START")

    base_dir = os.path.dirname(os.path.abspath(__file__))
    print("[2] base_dir =", base_dir)

    db_path = os.path.join(base_dir, "bee_env.db")
    print("[3] db_path  =", db_path)

    if not os.path.exists(db_path):
        print("[X] ERROR: bee_env.db not found")
        return

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    expected_tbl = "expected_activity_daily"
    validation_tbl = "validation_activity_daily"
    weather_tbl = "daily_weather_summary"
    rule_tbl = "behavior_rule_library"

    # 表存在性
    exists_expected = table_exists(cur, expected_tbl)
    exists_validation = table_exists(cur, validation_tbl)
    exists_weather = table_exists(cur, weather_tbl)
    exists_rule = table_exists(cur, rule_tbl)

    print("[4] table exists:")
    print("    expected  =", exists_expected, expected_tbl)
    print("    validation=", exists_validation, validation_tbl)
    print("    weather   =", exists_weather, weather_tbl)
    print("    rule      =", exists_rule, rule_tbl)

    if not exists_expected:
        print("[X] ERROR: expected_activity_daily missing, cannot export")
        conn.close()
        return

    # 行数
    n_expected = scalar(cur, f"SELECT COUNT(*) FROM {expected_tbl};")
    n_validation = scalar(cur, f"SELECT COUNT(*) FROM {validation_tbl};") if exists_validation else None
    n_weather = scalar(cur, f"SELECT COUNT(*) FROM {weather_tbl};") if exists_weather else None

    print("[5] row counts:")
    print("    expected rows  =", n_expected)
    print("    validation rows=", n_validation)
    print("    weather rows   =", n_weather)

    site_id = 1
    species_code = "CHINESE_BEE"
    print("[6] filter: site_id =", site_id, ", species_code =", species_code)

    # 核心导出：只依赖 expected + validation（如果有）
    # 先查 expected
    sql = f"""
    SELECT
        e.site_id,
        e.species_code,
        e.date,
        e.expected_min,
        e.expected_max
    FROM {expected_tbl} e
    WHERE e.site_id=? AND e.species_code=?
    ORDER BY e.date DESC
    """
    cur.execute(sql, (site_id, species_code))
    expected_rows = [dict(r) for r in cur.fetchall()]
    print("[7] expected matched rows =", len(expected_rows))

    # 再把 validation 拼进去（如果有）
    validation_map = {}
    if exists_validation:
        sql_v = f"""
        SELECT site_id, species_code, date, observed_activity, match_score, deviation_tag, deviation_value, explain_text
        FROM {validation_tbl}
        WHERE site_id=? AND species_code=?
        """
        cur.execute(sql_v, (site_id, species_code))
        vrows = [dict(r) for r in cur.fetchall()]
        print("[8] validation matched rows =", len(vrows))
        for r in vrows:
            key = (r["site_id"], r["species_code"], r["date"])
            validation_map[key] = r

    # 合并
    merged = []
    for r in expected_rows:
        key = (r["site_id"], r["species_code"], r["date"])
        v = validation_map.get(key, {})
        out = dict(r)
        # 这些字段如果没有，就留空
        out["observed_activity"] = v.get("observed_activity")
        out["match_score"] = v.get("match_score")
        out["deviation_tag"] = v.get("deviation_tag")
        out["deviation_value"] = v.get("deviation_value")
        out["explain_text"] = v.get("explain_text", "")
        merged.append(out)

    print("[9] merged rows =", len(merged))

    # 就算没数据，也要生成文件，让你看得见“它运行过”
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    latest_json = os.path.join(base_dir, "latest_activity.json")
    latest_csv = os.path.join(base_dir, "latest_activity.csv")
    backup_json = os.path.join(base_dir, f"export_activity_{ts}.json")
    backup_csv = os.path.join(base_dir, f"export_activity_{ts}.csv")

    with open(latest_json, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)
    with open(backup_json, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    # CSV：如果没有行，也写一个空表头
    fieldnames = ["site_id","species_code","date","expected_min","expected_max",
                  "observed_activity","match_score","deviation_tag","deviation_value","explain_text"]
    with open(latest_csv, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(merged)

    with open(backup_csv, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(merged)

    print("[10] DONE. files created:")
    print("     ", latest_json)
    print("     ", latest_csv)
    print("     ", backup_json)
    print("     ", backup_csv)

    conn.close()

if __name__ == "__main__":
    main()
