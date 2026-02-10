@echo off
REM 切换到你的 bee 项目目录（这个路径按你自己的为准）
cd /d "C:\Users\86134\Desktop\homeworks\workshop\s7-8\bee-project"

REM 依次执行三步：拉和风天气 → 写入数据库 → 重算日指标
python fetch_qweather_24h.py
python insert_qweather_data.py
python build_daily_indices.py

echo.
echo ✅ Bee 气象数据更新完成，可以关掉这个窗口啦。
pause
