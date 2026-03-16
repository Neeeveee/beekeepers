# -*- coding: utf-8 -*-  # 中文注释：声明文件编码，避免中文注释/输出乱码
import os  # 中文注释：读取环境变量、处理路径
import json  # 中文注释：保存天气 JSON
import sqlite3  # 中文注释：从 bee_env.db 读取蜂场经纬度
from datetime import datetime  # 中文注释：生成文件名时间戳
import requests  # 中文注释：发起 HTTP 请求

# =========================
# 基本配置（你只需要改 SITE_ID）
# =========================
SITE_ID = 1  # 中文注释：蜂场 ID（先默认 1；以后多蜂场就改这个）
DB_PATH = os.path.join(os.path.dirname(__file__), "bee_env.db")  # 中文注释：数据库路径（与脚本同目录）
OUT_DIR = os.path.join(os.path.dirname(__file__), "data_raw")  # 中文注释：输出目录（与原逻辑一致）

API_KEY = os.getenv("QWEATHER_API_KEY")  # 中文注释：从环境变量读取和风 Key（更安全）
if not API_KEY:  # 中文注释：如果没有配置 Key，就直接提示并退出
    raise SystemExit(
        "未检测到环境变量 QWEATHER_API_KEY。\n"
        "请先在 PowerShell 设置：\n"
        '  $env:QWEATHER_API_KEY="你的key"\n'
        "或永久设置：\n"
        "  setx QWEATHER_API_KEY \"你的key\"\n"
    )

# 中文注释：和风 24 小时预报接口（你原脚本就是这个）
API_URL = "https://nh3yfrdd4v.re.qweatherapi.com/v7/weather/24h"

# =========================
# 工具函数：从数据库读取蜂场经纬度
# =========================
def get_site_lon_lat(db_path: str, site_id: int) -> tuple[float, float]:
    """中文注释：从 sites 表读取指定蜂场的经纬度（返回 lon, lat）"""
    conn = sqlite3.connect(db_path)  # 中文注释：连接 SQLite 数据库
    conn.row_factory = sqlite3.Row  # 中文注释：让查询结果支持按列名访问
    cur = conn.cursor()  # 中文注释：创建游标
    cur.execute(
        "SELECT longitude, latitude FROM sites WHERE id = ?",
        (site_id,),
    )  # 中文注释：按 site_id 查询经纬度
    row = cur.fetchone()  # 中文注释：取一行结果
    conn.close()  # 中文注释：关闭连接

    if not row:  # 中文注释：找不到蜂场
        raise ValueError(f"sites 表中找不到 id={site_id} 的蜂场记录")

    lon = row["longitude"]  # 中文注释：读取经度
    lat = row["latitude"]  # 中文注释：读取纬度
    if lon is None or lat is None:  # 中文注释：经纬度为空则报错
        raise ValueError(f"蜂场 id={site_id} 的经纬度为空，请先更新 sites.longitude/latitude")

    return float(lon), float(lat)  # 中文注释：返回经纬度（lon, lat）

# =========================
# 主流程：拉取 24h 天气并保存 JSON
# =========================
def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)  # 中文注释：确保输出目录存在

    lon, lat = get_site_lon_lat(DB_PATH, SITE_ID)  # 中文注释：从 DB 获取真实蜂场经纬度
    location = f"{lon},{lat}"  # 中文注释：和风接口 location 参数格式为 "lon,lat"

    params = {
        "location": location,
        "key": API_KEY,
    }

    resp = requests.get(API_URL, params=params, timeout=30)
    print("status_code =", resp.status_code)
    print("response_text =", resp.text)
    resp.raise_for_status()

    data = resp.json()

    # 中文注释：生成输出文件名（与你原先风格一致：时间戳）
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")  # 中文注释：当前时间字符串
    out_path = os.path.join(OUT_DIR, f"qweather_24h_{ts}.json")  # 中文注释：输出路径

    with open(out_path, "w", encoding="utf-8") as f:  # 中文注释：写入 JSON 文件
        json.dump(data, f, ensure_ascii=False, indent=2)  # 中文注释：保留中文、格式化缩进

    print(f"[OK] QWeather 24h 已保存：{out_path}")  # 中文注释：给出成功提示
    print(f"[INFO] 使用蜂场 SITE_ID={SITE_ID} 坐标：{location}")  # 中文注释：打印实际使用的坐标

if __name__ == "__main__":
    main()  # 中文注释：脚本入口
