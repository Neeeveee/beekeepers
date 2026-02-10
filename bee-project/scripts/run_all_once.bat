@echo off
chcp 65001 >nul

REM 切到当前bat所在目录（确保相对路径正确）
cd /d "%~dp0"

echo ==========================================
echo 1) 更新天气逐小时数据（如果你有这一步）
echo ==========================================
REM 如果你们日后要更新和风数据，就把下一行取消注释
REM py fetch_qweather_24h.py

REM 如果需要把抓到的数据写入db，把下一行取消注释
REM py insert_qweather_data_patched.py

echo.
echo ==========================================
echo 2) 生成 daily_weather_summary
echo ==========================================
py build_daily_weather_summary.py

echo.
echo ==========================================
echo 3) 推导 expected + validation
echo ==========================================
py derive_expected_activity.py

echo.
echo ==========================================
echo 4) 导出结果（json + csv）
echo ==========================================
py export_activity_results.py

echo.
echo ✅ 全部完成！你可以去项目目录看导出的 export_activity_*.json / *.csv
pause
