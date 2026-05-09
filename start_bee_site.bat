@echo off
setlocal

cd /d D:\homeworks\workshop\s7-8\bee-project

set PYTHON_EXE=C:\Users\86134\AppData\Local\Programs\Python\Python312\python.exe
set SITE_URL=http://127.0.0.1:3000/scenario_ai.html

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

echo Starting Node scenario service in a new window...
start "Bee Scenario Server" cmd /k "npm start"

echo Waiting for Node scenario service...
powershell -NoProfile -Command ^
  "$deadline = (Get-Date).AddSeconds(20); " ^
  "do { " ^
  "  try { " ^
  "    $resp = Invoke-WebRequest -UseBasicParsing 'http://127.0.0.1:3000/api/scenarios' -TimeoutSec 2; " ^
  "    if ($resp.StatusCode -eq 200) { exit 0 } " ^
  "  } catch { } " ^
  "  Start-Sleep -Milliseconds 800; " ^
  "} while ((Get-Date) -lt $deadline); " ^
  "exit 1"
if errorlevel 1 (
    echo [ERROR] Node scenario service did not become ready on http://127.0.0.1:3000/api/scenarios
    goto :error
)

echo Running data update in a new window...
echo [INFO] If remote weather or Supabase access fails, the local scenario page can still open.
start "Bee Data Update" cmd /k "update_bee_system.bat"

echo Starting chart_api.py in a new window...
start "Bee Chart API" cmd /k ""%PYTHON_EXE%" "chart_api.py""

echo Waiting for API startup...
timeout /t 3 /nobreak >nul

start "" "%SITE_URL%"

echo Opened:
echo %SITE_URL%
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
