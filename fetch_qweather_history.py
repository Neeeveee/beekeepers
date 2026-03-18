# -*- coding: utf-8 -*-

import json
import os
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path

import requests


SITE_ID = 1
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "bee_env.db"
OUT_DIR = BASE_DIR / "data_raw"
API_BASE = os.getenv("QWEATHER_API_BASE", "https://nh3yfrdd4v.re.qweatherapi.com")
API_KEY = os.getenv("QWEATHER_API_KEY")
HISTORY_DAYS = int(os.getenv("QWEATHER_HISTORY_DAYS", "7"))


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
        raise ValueError(f"sites 表中不存在 id={site_id} 的蜂场")
    if row["longitude"] is None or row["latitude"] is None:
        raise ValueError(f"蜂场 id={site_id} 缺少经纬度")

    return float(row["longitude"]), float(row["latitude"])


def resolve_location_id(lon: float, lat: float) -> str:
    response = requests.get(
        f"{API_BASE.rstrip('/')}/geo/v2/city/lookup",
        params={
            "location": f"{lon},{lat}",
            "number": 1,
            "key": API_KEY,
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("code") != "200":
        raise RuntimeError(f"GeoAPI 返回异常: {payload}")

    location_list = payload.get("location") or []
    if not location_list:
        raise RuntimeError(f"GeoAPI 未返回 LocationID: {payload}")

    return str(location_list[0]["id"])


def fetch_history_for_day(location_id: str, target_date: date) -> dict:
    response = requests.get(
        f"{API_BASE.rstrip('/')}/v7/historical/weather",
        params={
            "location": location_id,
            "date": target_date.strftime("%Y%m%d"),
            "key": API_KEY,
        },
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("code") != "200":
        raise RuntimeError(f"历史天气接口返回异常: {payload}")
    return payload


def save_history_payload(target_date: date, payload: dict) -> Path:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = OUT_DIR / (
        f"qweather_history_{target_date.strftime('%Y%m%d')}_{target_date.strftime('%Y%m%d')}_{timestamp}.json"
    )
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return output_path


def main() -> None:
    if not API_KEY:
        print("[WARN] 未检测到 QWEATHER_API_KEY，跳过历史天气自动抓取。")
        return

    if not DB_PATH.exists():
        print(f"[WARN] 数据库不存在，跳过历史天气自动抓取：{DB_PATH}")
        return

    try:
        lon, lat = get_site_lon_lat(DB_PATH, SITE_ID)
        location_id = resolve_location_id(lon, lat)
    except Exception as exc:
        print(f"[WARN] 获取 LocationID 失败，跳过历史天气自动抓取：{exc}")
        return

    today = datetime.now().date()
    fetched_count = 0
    skipped_count = 0

    print(f"[INFO] 历史天气自动抓取使用 LocationID={location_id}，回补最近 {HISTORY_DAYS} 天。")

    for days_ago in range(HISTORY_DAYS, 0, -1):
        target_date = today - timedelta(days=days_ago)
        pattern = f"qweather_history_{target_date.strftime('%Y%m%d')}_{target_date.strftime('%Y%m%d')}_*.json"
        if list(OUT_DIR.glob(pattern)):
            skipped_count += 1
            continue

        try:
            payload = fetch_history_for_day(location_id, target_date)
            output_path = save_history_payload(target_date, payload)
            fetched_count += 1
            print(f"[OK] 历史天气已保存：{output_path}")
        except Exception as exc:
            print(f"[WARN] 抓取 {target_date.isoformat()} 历史天气失败：{exc}")

    print(f"[INFO] 历史天气抓取结束：新增 {fetched_count} 天，跳过 {skipped_count} 天。")


if __name__ == "__main__":
    main()
