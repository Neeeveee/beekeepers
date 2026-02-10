@echo off
REM ===== 1) 切换到你的 bee 项目目录（按你自己的路径改）=====
cd /d "D:\homeworks\workshop\s7-8\bee-project"

REM ===== 2) 第一次用：先初始化扩展表（只需跑一次，跑多次也安全）=====
python init_bee_extension.py

REM ===== 3) 日常更新：拉和风天气 → 写入数据库 → 重算日汇总（更稳定）=====
python fetch_qweather_24h.py
python insert_qweather_data_patched.py
python build_daily_weather_summary.py

REM ===== 4) 生成“已得行为规律推导”的每日预期 + 验证（按你要的日期/蜂种改脚本里的参数）=====
python derive_expected_activity.py

echo.
echo ✅ Bee 数据更新 + 规律推导完成，可以关掉这个窗口啦。
pause
