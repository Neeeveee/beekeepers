# -*- coding: utf-8 -*-

import argparse
import json
import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

import requests


BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "bee_env.db"

SUPABASE_BASE_URL = os.getenv("SUPABASE_BASE_URL", "https://altmyanvtdjgxdvseqqx.supabase.co")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "beehive_10min_raw")
SUPABASE_API_KEY = os.getenv(
    "SUPABASE_API_KEY",
    "sb_publishable_C4WFY7WeLCc0Vfl1IrKnzw_60lnCdhX",
)
SUPABASE_PAGE_SIZE = int(os.getenv("SUPABASE_PAGE_SIZE", "1000"))
ROLLING_BACKFILL_DAYS = int(os.getenv("SUPABASE_BACKFILL_DAYS", "7"))
DEFAULT_FARM_NAME = os.getenv("BEE_FARM_NAME", "当前测试蜂场")
DEFAULT_BUCKET_SEC = 600


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync bee behavior data from Supabase to local SQLite.")
    parser.add_argument("--start", help="Backfill start time, e.g. 2026-03-12 or 2026-03-12 00:00")
    parser.add_argument("--end", help="Backfill end time, e.g. 2026-03-17 or 2026-03-17 23:59")
    parser.add_argument("--full", action="store_true", help="Ignore local latest time and fetch all rows.")
    parser.add_argument(
        "--no-backfill",
        action="store_true",
        help="Disable the default rolling backfill window and only do incremental sync.",
    )
    return parser.parse_args()


def get_conn() -> sqlite3.Connection:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"数据库文件不存在：{DB_PATH}")

    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout = 30000;")
    return conn


def normalize_bucket_time(raw_time: str) -> str:
    value = raw_time.strip().replace("T", " ").replace("Z", "")
    if "+" in value:
        value = value.split("+", 1)[0]
    if len(value) == 10:
        value += " 00:00:00"
    elif len(value) == 16:
        value += ":00"
    return value


def normalize_filter_time(raw_time: str | None, end_of_day: bool = False) -> str | None:
    if not raw_time:
        return None

    value = raw_time.strip().replace("T", " ").replace("Z", "")
    if "+" in value:
        value = value.split("+", 1)[0]
    if len(value) == 10:
        value += " 23:59:59" if end_of_day else " 00:00:00"
    elif len(value) == 16:
        value += ":59" if end_of_day else ":00"
    return value


def fetch_rows(start_time: str | None = None, end_time: str | None = None, full_sync: bool = False) -> list[dict]:
    if not SUPABASE_API_KEY:
        print("[WARN] 未配置 SUPABASE_API_KEY，跳过 Supabase 同步。")
        return []

    url = f"{SUPABASE_BASE_URL.rstrip('/')}/rest/v1/{SUPABASE_TABLE}"
    headers = {
        "apikey": SUPABASE_API_KEY,
        "Authorization": f"Bearer {SUPABASE_API_KEY}",
        "Accept": "application/json",
    }

    all_rows: list[dict] = []
    offset = 0

    while True:
        params = {
            "select": "id,topic,device_id,bucket_time,in_count,out_count,daily_in,daily_out,payload_json,emqx_received_at",
            "order": "bucket_time.asc,id.asc",
            "limit": SUPABASE_PAGE_SIZE,
            "offset": offset,
        }

        if not full_sync:
            if start_time and end_time:
                params["and"] = f"(bucket_time.gte.{start_time[:19]},bucket_time.lte.{end_time[:19]})"
            elif start_time:
                params["bucket_time"] = f"gte.{start_time[:19]}"
            elif end_time:
                params["bucket_time"] = f"lte.{end_time[:19]}"

        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        batch = response.json()

        if not batch:
            break

        all_rows.extend(batch)
        if len(batch) < SUPABASE_PAGE_SIZE:
            break

        offset += SUPABASE_PAGE_SIZE

    return all_rows


def insert_rows(rows: list[dict]) -> tuple[int, int]:
    if not rows:
        return 0, 0

    conn = get_conn()
    try:
        cur = conn.cursor()
        inserted = 0
        skipped = 0

        for item in rows:
            device_id = item.get("device_id") or "hive01"
            topic = item.get("topic") or f"beehive/{device_id}/10min"
            bucket_time = item.get("bucket_time")
            if not bucket_time:
                continue

            event_time = normalize_bucket_time(bucket_time)
            cur.execute(
                """
                SELECT COUNT(*)
                FROM bee_counter_raw
                WHERE device_id = ? AND topic = ? AND event_time = ?
                """,
                (device_id, topic, event_time),
            )
            if cur.fetchone()[0] > 0:
                skipped += 1
                continue

            payload_json = item.get("payload_json")
            raw_json = json.dumps(payload_json, ensure_ascii=False) if payload_json is not None else None

            cur.execute(
                """
                INSERT INTO bee_counter_raw (
                    device_id,
                    hive_id,
                    farm_name,
                    topic,
                    event_time,
                    in_count,
                    out_count,
                    daily_in,
                    daily_out,
                    bucket_sec,
                    status,
                    raw_json,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    device_id,
                    device_id,
                    DEFAULT_FARM_NAME,
                    topic,
                    event_time,
                    int(item["in_count"] or 0),
                    int(item["out_count"] or 0),
                    int(item["daily_in"]) if item.get("daily_in") is not None else None,
                    int(item["daily_out"]) if item.get("daily_out") is not None else None,
                    DEFAULT_BUCKET_SEC,
                    None,
                    raw_json,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                ),
            )
            inserted += 1

        conn.commit()
        return inserted, skipped
    finally:
        conn.close()


def rolling_backfill_range(days: int) -> tuple[str, str]:
    today = datetime.now()
    start = (today - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)
    end = today.replace(hour=23, minute=59, second=59, microsecond=0)
    return start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")


def main() -> None:
    args = parse_args()

    if args.full:
        print("[INFO] Supabase 全量同步模式")
        try:
            rows = fetch_rows(full_sync=True)
        except Exception as exc:
            print(f"[WARN] 从 Supabase 拉取蜂箱数据失败，跳过本轮同步：{exc}")
            return
        inserted, skipped = insert_rows(rows)
        print(f"[INFO] Supabase 行为数据同步完成：拉取 {len(rows)} 条，新增 {inserted} 条，跳过 {skipped} 条。")
        return

    start_time = normalize_filter_time(args.start, end_of_day=False)
    end_time = normalize_filter_time(args.end, end_of_day=True)

    if start_time or end_time:
        print(f"[INFO] Supabase 历史回补范围：{start_time or 'MIN'} -> {end_time or 'MAX'}")
        try:
            rows = fetch_rows(start_time=start_time, end_time=end_time)
        except Exception as exc:
            print(f"[WARN] 从 Supabase 拉取蜂箱数据失败，跳过本轮同步：{exc}")
            return
        inserted, skipped = insert_rows(rows)
        print(f"[INFO] Supabase 行为数据同步完成：拉取 {len(rows)} 条，新增 {inserted} 条，跳过 {skipped} 条。")
        return

    if args.no_backfill:
        backfill_start = None
        backfill_end = None
        print("[INFO] Supabase 仅增量同步模式")
    else:
        backfill_start, backfill_end = rolling_backfill_range(ROLLING_BACKFILL_DAYS)
        print(f"[INFO] Supabase 默认回补最近 {ROLLING_BACKFILL_DAYS} 天：{backfill_start} -> {backfill_end}")

    try:
        rows = fetch_rows(start_time=backfill_start, end_time=backfill_end)
    except Exception as exc:
        print(f"[WARN] 从 Supabase 拉取蜂箱数据失败，跳过本轮同步：{exc}")
        return

    inserted, skipped = insert_rows(rows)
    print(f"[INFO] Supabase 行为数据同步完成：拉取 {len(rows)} 条，新增 {inserted} 条，跳过 {skipped} 条。")


if __name__ == "__main__":
    main()
