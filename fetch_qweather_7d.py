# -*- coding: utf-8 -*-

import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path

import requests


SITE_ID = 1
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "bee_env.db"
OUT_DIR = BASE_DIR / "data_raw"
API_URL = "https://nh3yfrdd4v.re.qweatherapi.com/v7/weather/7d"
API_KEY = os.getenv("QWEATHER_API_KEY")


def get_site_lon_lat(db_path: Path, site_id: int) -> tuple[float, float]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT longitude, latitude FROM sites WHERE id = ?",
            (site_id,),
        ).fetchone()
    finally:
        conn.close()

    if not row:
        raise ValueError(f"sites 表中不存在 id={site_id} 的蜂场记录")

    lon = row["longitude"]
    lat = row["latitude"]
    if lon is None or lat is None:
        raise ValueError(f"蜂场 id={site_id} 缺少经纬度")

    return float(lon), float(lat)


def main() -> None:
    if not API_KEY:
        raise SystemExit("未检测到环境变量 QWEATHER_API_KEY")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    lon, lat = get_site_lon_lat(DB_PATH, SITE_ID)
    location = f"{lon},{lat}"

    response = requests.get(
        API_URL,
        params={"location": location, "key": API_KEY},
        timeout=30,
    )
    print("status_code =", response.status_code)
    print("response_text =", response.text[:5000])
    response.raise_for_status()

    payload = response.json()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = OUT_DIR / f"qweather_7d_{timestamp}.json"
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(f"[OK] QWeather 7d 已保存：{output_path}")
    print(f"[INFO] 使用蜂场 SITE_ID={SITE_ID} 坐标：{location}")


if __name__ == "__main__":
    main()
