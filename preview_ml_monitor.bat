@echo off
setlocal

cd /d D:\homeworks\workshop\s7-8\bee-project

set PYTHON_EXE=C:\Users\86134\AppData\Local\Programs\Python\Python312\python.exe
set PORT=8080

if not exist "%PYTHON_EXE%" (
    echo [ERROR] Python not found: %PYTHON_EXE%
    echo Please update PYTHON_EXE in preview_ml_monitor.bat
    pause
    exit /b 1
)

start "Bee Static Preview" cmd /k ""%PYTHON_EXE%" -m http.server %PORT%"
timeout /t 2 /nobreak >nul
start "" "http://127.0.0.1:%PORT%/ml_monitor.html"

exit /b 0
