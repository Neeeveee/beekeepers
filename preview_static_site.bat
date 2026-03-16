@echo off
setlocal

cd /d D:\homeworks\workshop\s7-8\bee-project

set PYTHON_EXE=C:\Users\86134\AppData\Local\Programs\Python\Python312\python.exe
set PORT=8080

if not exist "%PYTHON_EXE%" (
    echo [ERROR] Python not found: %PYTHON_EXE%
    echo Please update PYTHON_EXE in preview_static_site.bat
    pause
    exit /b 1
)

echo.
echo ========================================
echo Starting static preview server
echo ========================================
echo.

start "Bee Static Preview" cmd /k ""%PYTHON_EXE%" -m http.server %PORT%"

echo Waiting for static server...
timeout /t 2 /nobreak >nul

start "" "http://127.0.0.1:%PORT%/chart_test.html?mode=static"

echo Opened:
echo http://127.0.0.1:%PORT%/chart_test.html?mode=static
echo.
exit /b 0
