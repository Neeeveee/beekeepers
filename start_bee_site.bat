@echo off
setlocal

cd /d D:\homeworks\workshop\s7-8\bee-project

set PYTHON_EXE=C:\Users\86134\AppData\Local\Programs\Python\Python312\python.exe

if not exist "%PYTHON_EXE%" (
    echo [ERROR] Python not found: %PYTHON_EXE%
    echo Please update PYTHON_EXE in start_bee_site.bat
    pause
    exit /b 1
)

echo.
echo ========================================
echo Starting Bee Project
echo ========================================
echo.

call update_bee_system.bat
if errorlevel 1 goto :error

echo Starting chart_api.py in a new window...
start "Bee Chart API" cmd /k ""%PYTHON_EXE%" "chart_api.py""

echo Waiting for API startup...
timeout /t 3 /nobreak >nul

start "" "D:\homeworks\workshop\s7-8\bee-project\chart_test.html"

echo Opened chart_test.html
echo.
exit /b 0

:error
echo.
echo ========================================
echo Startup failed. Check the error messages above.
echo ========================================
echo.
pause
exit /b 1
