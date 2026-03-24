from __future__ import annotations

import sqlite3
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "bee_env.db"


FARM_WEIGHTS = {
    "油菜": (1.0, "农田主蜜源"),
    "紫云英": (0.9, "农田春季主蜜源"),
    "蚕豆": (0.55, "田边补充蜜源"),
    "果梅": (0.18, "农场周边零散果树"),
    "桃": (0.12, "农场场景下果树占比偏低"),
    "李": (0.12, "农场场景下果树占比偏低"),
    "梨": (0.1, "农场场景下果树占比偏低"),
    "樱桃": (0.08, "农场场景下果树占比偏低"),
    "柑橘": (0.06, "农场场景下果树占比偏低"),
    "枇杷": (0.08, "农场场景下冬季果树占比偏低"),
    "冬青": (0.08, "场边零星绿化树种"),
    "柃木": (0.06, "冬季零星来源"),
    "栾树": (0.05, "夏末零星绿化树种"),
    "五倍子": (0.05, "零星秋季来源"),
    "苦木诸": (0.04, "原始占位条目，按极低覆盖处理"),
}


def main() -> None:
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS nectar_plant_site_weights (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                site_id INTEGER NOT NULL,
                plant_name TEXT NOT NULL,
                scenario TEXT NOT NULL DEFAULT 'default',
                display_weight REAL NOT NULL DEFAULT 1.0,
                note TEXT,
                created_at TEXT DEFAULT (datetime('now', 'localtime')),
                UNIQUE(site_id, plant_name, scenario)
            )
            """
        )

        for plant_name, (display_weight, note) in FARM_WEIGHTS.items():
            cur.execute(
                """
                INSERT INTO nectar_plant_site_weights (
                    site_id,
                    plant_name,
                    scenario,
                    display_weight,
                    note
                )
                VALUES (1, ?, 'farm', ?, ?)
                ON CONFLICT(site_id, plant_name, scenario) DO UPDATE SET
                    display_weight = excluded.display_weight,
                    note = excluded.note
                """,
                (plant_name, display_weight, note),
            )

        conn.commit()
        print(f"Seeded farm site weights for {len(FARM_WEIGHTS)} plants.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
