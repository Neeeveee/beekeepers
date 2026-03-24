from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "bee_env.db"


HANGZHOU_SPRING_PLANTS = [
    {
        "site_id": 1,
        "plant_name": "紫云英",
        "bloom_start_mmdd": "03-01",
        "bloom_end_mmdd": "04-25",
        "bloom_days": 56,
        "nectar_grade": 4,
        "pollen_grade": 3,
        "avg_yield_kg_per_colony": 25.0,
        "note": "Hangzhou spring supplement; common southern spring nectar source.",
        "source": "hangzhou_curated_v1",
        "confidence": 0.75,
    },
    {
        "site_id": 1,
        "plant_name": "桃",
        "bloom_start_mmdd": "03-08",
        "bloom_end_mmdd": "04-05",
        "bloom_days": 29,
        "nectar_grade": 2,
        "pollen_grade": 4,
        "avg_yield_kg_per_colony": 6.0,
        "note": "Hangzhou spring supplement; orchard pollen source.",
        "source": "hangzhou_curated_v1",
        "confidence": 0.68,
    },
    {
        "site_id": 1,
        "plant_name": "李",
        "bloom_start_mmdd": "03-05",
        "bloom_end_mmdd": "03-30",
        "bloom_days": 26,
        "nectar_grade": 2,
        "pollen_grade": 4,
        "avg_yield_kg_per_colony": 5.0,
        "note": "Hangzhou spring supplement; early spring orchard nectar and pollen source.",
        "source": "hangzhou_curated_v1",
        "confidence": 0.68,
    },
    {
        "site_id": 1,
        "plant_name": "梨",
        "bloom_start_mmdd": "03-15",
        "bloom_end_mmdd": "04-10",
        "bloom_days": 27,
        "nectar_grade": 2,
        "pollen_grade": 4,
        "avg_yield_kg_per_colony": 8.0,
        "note": "Hangzhou spring supplement; common spring orchard pollen source.",
        "source": "hangzhou_curated_v1",
        "confidence": 0.7,
    },
    {
        "site_id": 1,
        "plant_name": "樱桃",
        "bloom_start_mmdd": "03-10",
        "bloom_end_mmdd": "04-05",
        "bloom_days": 27,
        "nectar_grade": 2,
        "pollen_grade": 4,
        "avg_yield_kg_per_colony": 6.0,
        "note": "Hangzhou spring supplement; fruit cherry rather than ornamental sakura.",
        "source": "hangzhou_curated_v1",
        "confidence": 0.65,
    },
    {
        "site_id": 1,
        "plant_name": "蚕豆",
        "bloom_start_mmdd": "03-10",
        "bloom_end_mmdd": "04-20",
        "bloom_days": 42,
        "nectar_grade": 3,
        "pollen_grade": 3,
        "avg_yield_kg_per_colony": 12.0,
        "note": "Hangzhou spring supplement; field-edge spring nectar source.",
        "source": "hangzhou_curated_v1",
        "confidence": 0.66,
    },
    {
        "site_id": 1,
        "plant_name": "柑橘",
        "bloom_start_mmdd": "04-10",
        "bloom_end_mmdd": "05-10",
        "bloom_days": 31,
        "nectar_grade": 4,
        "pollen_grade": 2,
        "avg_yield_kg_per_colony": 20.0,
        "note": "Hangzhou spring supplement; common Zhejiang spring nectar tree crop.",
        "source": "hangzhou_curated_v1",
        "confidence": 0.75,
    },
]


def upsert_plant(cur: sqlite3.Cursor, plant: dict) -> str:
    existing = cur.execute(
        """
        SELECT id
        FROM nectar_plants
        WHERE site_id = ? AND plant_name = ?
        """,
        (plant["site_id"], plant["plant_name"]),
    ).fetchone()

    params = (
        plant["bloom_start_mmdd"],
        plant["bloom_end_mmdd"],
        plant["bloom_days"],
        plant["nectar_grade"],
        plant["pollen_grade"],
        plant["avg_yield_kg_per_colony"],
        plant["note"],
        plant["source"],
        plant["confidence"],
    )

    if existing:
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
                source = ?,
                confidence = ?
            WHERE site_id = ? AND plant_name = ?
            """,
            params + (plant["site_id"], plant["plant_name"]),
        )
        return "updated"

    cur.execute(
        """
        INSERT INTO nectar_plants (
            site_id,
            plant_name,
            bloom_start_mmdd,
            bloom_end_mmdd,
            bloom_days,
            nectar_grade,
            pollen_grade,
            avg_yield_kg_per_colony,
            note,
            source,
            confidence,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            plant["site_id"],
            plant["plant_name"],
            plant["bloom_start_mmdd"],
            plant["bloom_end_mmdd"],
            plant["bloom_days"],
            plant["nectar_grade"],
            plant["pollen_grade"],
            plant["avg_yield_kg_per_colony"],
            plant["note"],
            plant["source"],
            plant["confidence"],
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        ),
    )
    return "inserted"


def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        inserted = []
        updated = []

        for plant in HANGZHOU_SPRING_PLANTS:
            action = upsert_plant(cur, plant)
            if action == "inserted":
                inserted.append(plant["plant_name"])
            else:
                updated.append(plant["plant_name"])

        conn.commit()

        print("Hangzhou spring nectar plant seed complete.")
        print(f"Inserted ({len(inserted)}): {', '.join(inserted) if inserted else '-'}")
        print(f"Updated ({len(updated)}): {', '.join(updated) if updated else '-'}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
