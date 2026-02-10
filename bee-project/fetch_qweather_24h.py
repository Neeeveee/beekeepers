import os
import json
from datetime import datetime
import requests

# === 来自你控制台的专属 Host & Key ===
API_HOST = "https://nh3yfrdd4v.re.qweatherapi.com"
API_KEY  = "d1054807466c4eae8fd0f263669b6b9e"

# === 杭州西湖区：经度,纬度（你之前用的） ===
LOCATION = "120.125,30.25"

def main():
    url = f"{API_HOST}/v7/weather/24h"
    params = {
        "location": LOCATION,
        "key": API_KEY,
        "unit": "m",
        "lang": "zh",
    }

    print("正在向和风天气请求【24小时逐小时天气数据】…")

    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    # 自动写入 data_raw 目录
    os.makedirs("data_raw", exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M")
    filename = os.path.join("data_raw", f"qweather_{timestamp}.json")

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"✅ 已成功获取最新天气数据 → {filename}")

if __name__ == "__main__":
    main()
