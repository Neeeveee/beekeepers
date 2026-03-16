# -*- coding: utf-8 -*-

import json
from pathlib import Path

import chart_api


OUTPUT_DIR = Path(__file__).resolve().parent / "data"


def write_json(filename: str, payload) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / filename
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    print(f"Wrote {output_path}")


def get_json_payload(view_func):
    with chart_api.app.app_context():
        response = view_func()
        return response.get_json()


def main() -> None:
    exports = {
        "bee-activity-forecast.json": chart_api.get_bee_activity_forecast,
        "flowering-overview.json": chart_api.get_flowering_overview,
        "nectar-supply-overview.json": chart_api.get_nectar_supply_overview,
        "mismatch-overview.json": chart_api.get_mismatch_overview,
    }

    for filename, view_func in exports.items():
        payload = get_json_payload(view_func)
        write_json(filename, payload)


if __name__ == "__main__":
    main()
