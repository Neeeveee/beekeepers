# -*- coding: utf-8 -*-

import subprocess
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent

SCRIPTS = [
    "sync_supabase_to_sqlite.py",
    "fetch_qweather_24h.py",
    "fetch_qweather_7d.py",
    "fetch_qweather_history.py",
    "insert_qweather_data_patched.py",
    "insert_qweather_history.py",
    "build_eco_time_series.py",
    "build_bee_activity_curve.py",
    "build_bee_activity_hourly.py",
    "build_bee_env_aligned_hourly.py",
    "derive_flowering_index.py",
    "derive_nectar_supply.py",
    "derive_expected_activity_hourly.py",
    "derive_mismatch_index.py",
    "build_future_expected_activity_hourly.py",
    "train_residual_model.py",
    "predict_future_activity_residual.py",
    "export_static_json.py",
    "export_ml_monitor_data.py",
]


def run_script(script_name: str) -> None:
    script_path = BASE_DIR / script_name
    if not script_path.exists():
        raise FileNotFoundError(f"Missing script: {script_path}")

    print(f"\n=== Running {script_name} ===")
    subprocess.run([sys.executable, str(script_path)], check=True, cwd=BASE_DIR)


def main() -> None:
    for script_name in SCRIPTS:
        run_script(script_name)

    print("\nAll update steps completed.")


if __name__ == "__main__":
    main()
