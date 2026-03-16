@echo off
chcp 65001 >nul
setlocal

cd /d D:\homeworks\workshop\s7-8\bee-project

set PYTHON_EXE=C:\Users\86134\AppData\Local\Programs\Python\Python312\python.exe

echo.
echo ==============================
echo 模型快速更新开始
echo ==============================
echo.

"%PYTHON_EXE%" "build_bee_activity_curve.py" || goto :error
"%PYTHON_EXE%" "build_bee_activity_hourly.py" || goto :error
"%PYTHON_EXE%" "build_bee_env_aligned_hourly.py" || goto :error
"%PYTHON_EXE%" "derive_flowering_index.py" || goto :error
"%PYTHON_EXE%" "derive_nectar_supply.py" || goto :error
"%PYTHON_EXE%" "derive_expected_activity_hourly.py" || goto :error

echo.
echo 模型快速更新完成
echo.
pause
exit /b 0

:error
echo.
echo 运行中断，请看上方报错
echo.
pause
exit /b 1