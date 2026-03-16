@echo off
setlocal enabledelayedexpansion

cd /d D:\homeworks\workshop\s7-8\bee-project

echo.
echo ========================================
echo Bee Project update started
echo ========================================
echo.

set PYTHON_EXE=C:\Users\86134\AppData\Local\Programs\Python\Python312\python.exe

if not exist "%PYTHON_EXE%" (
    echo [ERROR] Python not found: %PYTHON_EXE%
    echo Please update PYTHON_EXE in update_bee_system.bat
    pause
    exit /b 1
)

call :check_file "fetch_qweather_24h.py"
call :check_file "insert_qweather_data_patched.py"
call :check_file "build_bee_activity_curve.py"
call :check_file "build_bee_activity_hourly.py"
call :check_file "build_bee_env_aligned_hourly.py"
call :check_file "derive_flowering_index.py"
call :check_file "derive_nectar_supply.py"
call :check_file "derive_expected_activity_hourly.py"
call :check_file "derive_mismatch_index.py"
call :check_file "build_future_expected_activity_hourly.py"
call :check_file "export_static_json.py"

echo ----------------------------------------
echo [1/11] Fetch latest 24h weather
echo ----------------------------------------
"%PYTHON_EXE%" "fetch_qweather_24h.py"
if errorlevel 1 goto :error

echo.
echo ----------------------------------------
echo [2/11] Import latest weather into database
echo ----------------------------------------
"%PYTHON_EXE%" "insert_qweather_data_patched.py"
if errorlevel 1 goto :error

echo.
echo ----------------------------------------
echo [3/11] Build bee activity curve
echo ----------------------------------------
"%PYTHON_EXE%" "build_bee_activity_curve.py"
if errorlevel 1 goto :error

echo.
echo ----------------------------------------
echo [4/11] Build hourly bee activity
echo ----------------------------------------
"%PYTHON_EXE%" "build_bee_activity_hourly.py"
if errorlevel 1 goto :error

echo.
echo ----------------------------------------
echo [5/11] Build aligned bee-environment table
echo ----------------------------------------
"%PYTHON_EXE%" "build_bee_env_aligned_hourly.py"
if errorlevel 1 goto :error

echo.
echo ----------------------------------------
echo [6/11] Build flowering model
echo ----------------------------------------
"%PYTHON_EXE%" "derive_flowering_index.py"
if errorlevel 1 goto :error

echo.
echo ----------------------------------------
echo [7/11] Build nectar supply model
echo ----------------------------------------
"%PYTHON_EXE%" "derive_nectar_supply.py"
if errorlevel 1 goto :error

echo.
echo ----------------------------------------
echo [8/11] Build expected bee activity
echo ----------------------------------------
"%PYTHON_EXE%" "derive_expected_activity_hourly.py"
if errorlevel 1 goto :error

echo.
echo ----------------------------------------
echo [9/11] Build historical mismatch index
echo ----------------------------------------
"%PYTHON_EXE%" "derive_mismatch_index.py"
if errorlevel 1 goto :error

echo.
echo ----------------------------------------
echo [10/11] Build future expected bee activity
echo ----------------------------------------
"%PYTHON_EXE%" "build_future_expected_activity_hourly.py"
if errorlevel 1 goto :error

echo.
echo ----------------------------------------
echo [11/11] Export static JSON data
echo ----------------------------------------
"%PYTHON_EXE%" "export_static_json.py"
if errorlevel 1 goto :error

echo.
echo ========================================
echo Update completed successfully
echo ========================================
echo.
echo Generated database tables and data\*.json files.
echo.
pause
exit /b 0

:check_file
if not exist "%~1" (
    echo [ERROR] Missing file: %~1
    pause
    exit /b 1
)
goto :eof

:error
echo.
echo ========================================
echo Update failed. Check the error messages above.
echo ========================================
echo.
pause
exit /b 1
