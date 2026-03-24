from __future__ import annotations

import sqlite3
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "bee_env.db"


HANGZHOU_TUNING = {
    "枇杷": {
        "bloom_start_mmdd": "11-10",
        "bloom_end_mmdd": "01-25",
        "bloom_days": 77,
        "nectar_grade": 4,
        "pollen_grade": 4,
        "avg_yield_kg_per_colony": 28.0,
        "confidence": 0.72,
        "note": "Hangzhou localized tuning: late autumn to mid-winter bloom window.",
    },
    "果梅": {
        "bloom_start_mmdd": "02-08",
        "bloom_end_mmdd": "03-25",
        "bloom_days": 46,
        "nectar_grade": 2,
        "pollen_grade": 3,
        "avg_yield_kg_per_colony": 8.0,
        "confidence": 0.68,
        "note": "Hangzhou localized tuning: early spring ornamental and orchard pollen source.",
    },
    "油菜": {
        "bloom_start_mmdd": "02-15",
        "bloom_end_mmdd": "04-10",
        "bloom_days": 55,
        "nectar_grade": 4,
        "pollen_grade": 4,
        "avg_yield_kg_per_colony": 24.0,
        "confidence": 0.8,
        "note": "Hangzhou localized tuning: dominant spring field nectar source.",
    },
    "冬青": {
        "bloom_start_mmdd": "04-18",
        "bloom_end_mmdd": "05-25",
        "bloom_days": 38,
        "nectar_grade": 2,
        "pollen_grade": 1,
        "avg_yield_kg_per_colony": 2.0,
        "confidence": 0.6,
        "note": "Hangzhou localized tuning: late spring nectar source.",
    },
    "五倍子": {
        "bloom_start_mmdd": "08-25",
        "bloom_end_mmdd": "09-25",
        "bloom_days": 32,
        "nectar_grade": 2,
        "pollen_grade": 1,
        "avg_yield_kg_per_colony": 5.0,
        "confidence": 0.6,
        "note": "Hangzhou localized tuning: late summer to early autumn source.",
    },
    "柃木": {
        "bloom_start_mmdd": "10-25",
        "bloom_end_mmdd": "02-10",
        "bloom_days": 109,
        "nectar_grade": 2,
        "pollen_grade": 1,
        "avg_yield_kg_per_colony": 10.0,
        "confidence": 0.6,
        "note": "Hangzhou localized tuning: winter nectar source.",
    },
    "栾树": {
        "bloom_start_mmdd": "08-20",
        "bloom_end_mmdd": "09-20",
        "bloom_days": 32,
        "nectar_grade": 2,
        "pollen_grade": 2,
        "avg_yield_kg_per_colony": 2.0,
        "confidence": 0.6,
        "note": "Hangzhou localized tuning: late summer urban tree source.",
    },
    "苦木诸": {
        "bloom_start_mmdd": "04-05",
        "bloom_end_mmdd": "05-15",
        "bloom_days": 41,
        "nectar_grade": 2,
        "pollen_grade": 1,
        "avg_yield_kg_per_colony": 3.0,
        "confidence": 0.58,
        "note": "Hangzhou localized tuning: placeholder taxonomy retained from original dataset.",
    },
    "紫云英": {
        "bloom_start_mmdd": "03-05",
        "bloom_end_mmdd": "04-20",
        "bloom_days": 47,
        "nectar_grade": 4,
        "pollen_grade": 3,
        "avg_yield_kg_per_colony": 24.0,
        "confidence": 0.78,
        "note": "Hangzhou localized tuning: common spring legume nectar source.",
    },
    "桃": {
        "bloom_start_mmdd": "03-08",
        "bloom_end_mmdd": "03-31",
        "bloom_days": 24,
        "nectar_grade": 2,
        "pollen_grade": 4,
        "avg_yield_kg_per_colony": 6.0,
        "confidence": 0.68,
        "note": "Hangzhou localized tuning: spring orchard pollen source.",
    },
    "李": {
        "bloom_start_mmdd": "03-05",
        "bloom_end_mmdd": "03-28",
        "bloom_days": 24,
        "nectar_grade": 2,
        "pollen_grade": 4,
        "avg_yield_kg_per_colony": 5.0,
        "confidence": 0.68,
        "note": "Hangzhou localized tuning: early spring orchard pollen source.",
    },
    "梨": {
        "bloom_start_mmdd": "03-15",
        "bloom_end_mmdd": "04-05",
        "bloom_days": 22,
        "nectar_grade": 2,
        "pollen_grade": 4,
        "avg_yield_kg_per_colony": 8.0,
        "confidence": 0.72,
        "note": "Hangzhou localized tuning: mid-spring orchard pollen source.",
    },
    "樱桃": {
        "bloom_start_mmdd": "03-10",
        "bloom_end_mmdd": "03-31",
        "bloom_days": 22,
        "nectar_grade": 2,
        "pollen_grade": 4,
        "avg_yield_kg_per_colony": 6.0,
        "confidence": 0.66,
        "note": "Hangzhou localized tuning: fruit cherry spring source.",
    },
    "蚕豆": {
        "bloom_start_mmdd": "03-12",
        "bloom_end_mmdd": "04-18",
        "bloom_days": 38,
        "nectar_grade": 3,
        "pollen_grade": 3,
        "avg_yield_kg_per_colony": 12.0,
        "confidence": 0.68,
        "note": "Hangzhou localized tuning: spring field-edge nectar source.",
    },
    "柑橘": {
        "bloom_start_mmdd": "04-12",
        "bloom_end_mmdd": "05-08",
        "bloom_days": 27,
        "nectar_grade": 4,
        "pollen_grade": 2,
        "avg_yield_kg_per_colony": 20.0,
        "confidence": 0.78,
        "note": "Hangzhou localized tuning: Zhejiang citrus bloom window.",
    },
}


def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        updated = []
        for plant_name, values in HANGZHOU_TUNING.items():
            cur.execute(
                """
                UPDATE nectar_plants
                SET
                    bloom_start_mmdd = ?,
                    bloom_end_mmdd = ?,
                    bloom_days = ?,
                    nectar_grade = ?,
                    pollen_grade = ?,
                    avg_yield_kg_per_colony = ?,
                    note = ?,
                    source = 'hangzhou_curated_v2',
                    confidence = ?
                WHERE plant_name = ?
                """,
                (
                    values["bloom_start_mmdd"],
                    values["bloom_end_mmdd"],
                    values["bloom_days"],
                    values["nectar_grade"],
                    values["pollen_grade"],
                    values["avg_yield_kg_per_colony"],
                    values["note"],
                    values["confidence"],
                    plant_name,
                ),
            )
            if cur.rowcount:
                updated.append(plant_name)

        conn.commit()
        print(f"Hangzhou localized tuning complete. Updated {len(updated)} plants.")
        print(", ".join(updated))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
