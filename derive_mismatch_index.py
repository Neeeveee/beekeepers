# -*- coding: utf-8 -*-

import sqlite3
from pathlib import Path
from datetime import datetime, date


def clamp(value, min_value=0.0, max_value=1.0):
    return max(min_value, min(max_value, value))


def mmdd_to_day_of_year(mmdd: str, ref_year: int = 2026) -> int:
    month, day = map(int, mmdd.split("-"))
    return date(ref_year, month, day).timetuple().tm_yday


def get_date_day_of_year(date_str: str) -> int:
    dt = datetime.strptime(date_str, "%Y-%m-%d").date()
    return dt.timetuple().tm_yday


def is_cross_year(start_mmdd: str, end_mmdd: str) -> bool:
    start_month, start_day = map(int, start_mmdd.split("-"))
    end_month, end_day = map(int, end_mmdd.split("-"))
    return (start_month, start_day) > (end_month, end_day)


def is_date_in_bloom_window(model_date: str, bloom_start_mmdd: str, bloom_end_mmdd: str) -> bool:
    doy = get_date_day_of_year(model_date)
    start_doy = mmdd_to_day_of_year(bloom_start_mmdd)
    end_doy = mmdd_to_day_of_year(bloom_end_mmdd)

    if is_cross_year(bloom_start_mmdd, bloom_end_mmdd):
        return doy >= start_doy or doy <= end_doy

    return start_doy <= doy <= end_doy


def calc_nectar_resource_factor(nectar_grade, avg_yield_kg_per_colony, confidence):
    nectar_grade = nectar_grade or 0
    avg_yield_kg_per_colony = avg_yield_kg_per_colony or 0.0
    confidence = confidence or 0.5

    if nectar_grade >= 5:
        grade_factor = 1.00
    elif nectar_grade == 4:
        grade_factor = 0.90
    elif nectar_grade == 3:
        grade_factor = 0.75
    elif nectar_grade == 2:
        grade_factor = 0.60
    else:
        grade_factor = 0.45

    if avg_yield_kg_per_colony >= 30:
        yield_factor = 1.10
    elif avg_yield_kg_per_colony >= 20:
        yield_factor = 1.00
    elif avg_yield_kg_per_colony >= 10:
        yield_factor = 0.90
    else:
        yield_factor = 0.80

    if confidence >= 0.8:
        confidence_factor = 1.00
    elif confidence >= 0.6:
        confidence_factor = 0.95
    else:
        confidence_factor = 0.90

    factor = grade_factor * yield_factor * confidence_factor
    return round(max(0.35, min(1.10, factor)), 3)


def calc_raw_gap(nectar_supply_index, behavior_index_norm):
    if nectar_supply_index is None or behavior_index_norm is None:
        return None
    return round(abs(nectar_supply_index - behavior_index_norm), 3)


def calc_mismatch_risk(raw_gap):
    if raw_gap is None:
        return None

    risk = (raw_gap - 0.20) / 0.80
    return round(clamp(risk), 3)


def calc_mismatch_type(nectar_supply_index, behavior_index_norm, raw_gap):
    if nectar_supply_index is None or behavior_index_norm is None or raw_gap is None:
        return "no_data"

    if raw_gap < 0.15:
        return "matched"

    if nectar_supply_index > behavior_index_norm:
        return "resource_ahead"

    return "behavior_ahead"


def calc_mismatch_level(mismatch_gap):
    if mismatch_gap is None:
        return "无数据"
    if mismatch_gap < 0.15:
        return "基本匹配"
    if mismatch_gap < 0.30:
        return "轻度错配"
    if mismatch_gap < 0.50:
        return "中度错配"
    return "显著错配"


def create_table_if_not_exists(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS mismatch_index_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            model_date TEXT,
            nectar_supply_index REAL,
            behavior_index_raw REAL,
            behavior_index_norm REAL,
            raw_gap REAL,
            mismatch_gap REAL,
            mismatch_type TEXT,
            mismatch_level TEXT,
            source TEXT,
            created_at TEXT,
            UNIQUE(model_date)
        )
        """
    )

    cur.execute("PRAGMA table_info(mismatch_index_daily)")
    columns = [row[1] for row in cur.fetchall()]

    if "raw_gap" not in columns:
        cur.execute("ALTER TABLE mismatch_index_daily ADD COLUMN raw_gap REAL")


def load_daily_nectar_supply(cur):
    rows = cur.execute(
        """
        SELECT
            n.model_date,
            n.plant_name,
            n.nectar_supply_index,
            p.nectar_grade,
            p.avg_yield_kg_per_colony,
            p.confidence,
            p.bloom_start_mmdd,
            p.bloom_end_mmdd
        FROM nectar_supply_model_daily n
        LEFT JOIN nectar_plants p
          ON n.plant_name = p.plant_name
        ORDER BY n.model_date ASC, n.plant_name ASC
        """
    ).fetchall()

    by_date = {}
    for row in rows:
        if not row["bloom_start_mmdd"] or not row["bloom_end_mmdd"]:
            continue
        if not is_date_in_bloom_window(row["model_date"], row["bloom_start_mmdd"], row["bloom_end_mmdd"]):
            continue

        weight = calc_nectar_resource_factor(
            row["nectar_grade"],
            row["avg_yield_kg_per_colony"],
            row["confidence"]
        )

        by_date.setdefault(row["model_date"], []).append(
            {
                "value": row["nectar_supply_index"] or 0.0,
                "weight": weight
            }
        )

    result = {}
    for model_date, items in by_date.items():
        total_weight = sum(item["weight"] for item in items)
        weighted_sum = sum(item["value"] * item["weight"] for item in items)
        result[model_date] = round(weighted_sum / total_weight, 3) if total_weight > 0 else 0.0

    return result


def load_daily_behavior(cur):
    rows = cur.execute(
        """
        SELECT
            aligned_date AS model_date,
            AVG(expected_activity) AS behavior_index_raw
        FROM expected_activity_hourly
        WHERE hour BETWEEN 6 AND 19
        GROUP BY aligned_date
        ORDER BY model_date ASC
        """
    ).fetchall()

    result = {}
    for row in rows:
        result[row["model_date"]] = round(row["behavior_index_raw"], 3) if row["behavior_index_raw"] is not None else None
    return result


def normalize_behavior(daily_behavior_dict):
    valid_values = [value for value in daily_behavior_dict.values() if value is not None]

    if not valid_values:
        return {}, None

    max_behavior = max(valid_values)
    if max_behavior <= 0:
        max_behavior = 1.0

    normalized = {}
    for model_date, raw_value in daily_behavior_dict.items():
        if raw_value is None:
            normalized[model_date] = None
        else:
            normalized[model_date] = round(clamp(raw_value / max_behavior), 3)

    return normalized, round(max_behavior, 3)


def main():
    db_path = Path(__file__).resolve().parent / "bee_env.db"

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    create_table_if_not_exists(cur)

    daily_nectar_supply = load_daily_nectar_supply(cur)
    daily_behavior_raw = load_daily_behavior(cur)
    daily_behavior_norm, max_behavior = normalize_behavior(daily_behavior_raw)

    common_dates = sorted(set(daily_nectar_supply.keys()) & set(daily_behavior_raw.keys()))

    if not common_dates:
        print("No overlapping dates found between nectar supply and behavior data.")
        conn.close()
        return

    processed_count = 0

    for model_date in common_dates:
        nectar_supply_index = daily_nectar_supply.get(model_date)
        behavior_index_raw = daily_behavior_raw.get(model_date)
        behavior_index_norm = daily_behavior_norm.get(model_date)

        raw_gap = calc_raw_gap(nectar_supply_index, behavior_index_norm)
        mismatch_gap = calc_mismatch_risk(raw_gap)
        mismatch_type = calc_mismatch_type(nectar_supply_index, behavior_index_norm, raw_gap)
        mismatch_level = calc_mismatch_level(mismatch_gap)

        cur.execute(
            """
            INSERT INTO mismatch_index_daily (
                model_date,
                nectar_supply_index,
                behavior_index_raw,
                behavior_index_norm,
                raw_gap,
                mismatch_gap,
                mismatch_type,
                mismatch_level,
                source,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(model_date) DO UPDATE SET
                nectar_supply_index = excluded.nectar_supply_index,
                behavior_index_raw = excluded.behavior_index_raw,
                behavior_index_norm = excluded.behavior_index_norm,
                raw_gap = excluded.raw_gap,
                mismatch_gap = excluded.mismatch_gap,
                mismatch_type = excluded.mismatch_type,
                mismatch_level = excluded.mismatch_level,
                source = excluded.source,
                created_at = excluded.created_at
            """,
            (
                model_date,
                nectar_supply_index,
                behavior_index_raw,
                behavior_index_norm,
                raw_gap,
                mismatch_gap,
                mismatch_type,
                mismatch_level,
                "rule_v3_daytime_inseason_nectar_vs_behavior_gap",
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
        )

        processed_count += 1

    conn.commit()
    conn.close()

    print(f"Updated mismatch_index_daily for {processed_count} dates. max_behavior={max_behavior}")


if __name__ == "__main__":
    main()
