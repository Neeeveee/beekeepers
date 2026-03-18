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
call :check_file "fetch_qweather_7d.py"
call :check_file "fetch_qweather_history.py"
call :check_file "sync_supabase_to_sqlite.py"
call :check_file "insert_qweather_data_patched.py"
call :check_file "insert_qweather_history.py"
call :check_file "build_eco_time_series.py"
call :check_file "build_bee_activity_curve.py"
call :check_file "build_bee_activity_hourly.py"
call :check_file "build_bee_env_aligned_hourly.py"
call :check_file "derive_flowering_index.py"
call :check_file "derive_nectar_supply.py"
call :check_file "derive_expected_activity_hourly.py"
call :check_file "derive_mismatch_index.py"
call :check_file "build_future_expected_activity_hourly.py"
call :check_file "train_residual_model.py"
call :check_file "predict_future_activity_residual.py"
call :check_file "export_static_json.py"
call :check_file "export_ml_monitor_data.py"

echo ----------------------------------------
echo [1/17] Sync bee behavior data from Supabase
echo ----------------------------------------
"%PYTHON_EXE%" "sync_supabase_to_sqlite.py"
if errorlevel 1 goto :error

echo.
echo ----------------------------------------
echo [2/18] Fetch latest 24h weather
echo ----------------------------------------
"%PYTHON_EXE%" "fetch_qweather_24h.py"
if errorlevel 1 goto :error

echo.
echo ----------------------------------------
echo [3/18] Fetch 7-day forecast weather
echo ----------------------------------------
"%PYTHON_EXE%" "fetch_qweather_7d.py"
if errorlevel 1 goto :error

echo.
echo ----------------------------------------
echo [4/18] Fetch recent historical weather
echo ----------------------------------------
"%PYTHON_EXE%" "fetch_qweather_history.py"
if errorlevel 1 goto :error

echo.
echo ----------------------------------------
echo [5/18] Import latest weather into database
echo ----------------------------------------
"%PYTHON_EXE%" "insert_qweather_data_patched.py"
if errorlevel 1 goto :error

echo.
echo ----------------------------------------
echo [6/18] Import historical weather into database
echo ----------------------------------------
"%PYTHON_EXE%" "insert_qweather_history.py"
if errorlevel 1 goto :error

echo.
echo ----------------------------------------
echo [7/18] Rebuild eco time series
echo ----------------------------------------
"%PYTHON_EXE%" "build_eco_time_series.py"
if errorlevel 1 goto :error

echo.
echo ----------------------------------------
echo [8/18] Build bee activity curve
echo ----------------------------------------
"%PYTHON_EXE%" "build_bee_activity_curve.py"
if errorlevel 1 goto :error

echo.
echo ----------------------------------------
echo [9/18] Build hourly bee activity
echo ----------------------------------------
"%PYTHON_EXE%" "build_bee_activity_hourly.py"
if errorlevel 1 goto :error

echo.
echo ----------------------------------------
echo [10/18] Build aligned bee-environment table
echo ----------------------------------------
"%PYTHON_EXE%" "build_bee_env_aligned_hourly.py"
if errorlevel 1 goto :error

echo.
echo ----------------------------------------
echo [11/18] Build flowering model
echo ----------------------------------------
"%PYTHON_EXE%" "derive_flowering_index.py"
if errorlevel 1 goto :error

echo.
echo ----------------------------------------
echo [12/18] Build nectar supply model
echo ----------------------------------------
"%PYTHON_EXE%" "derive_nectar_supply.py"
if errorlevel 1 goto :error

echo.
echo ----------------------------------------
echo [13/18] Build expected bee activity
echo ----------------------------------------
"%PYTHON_EXE%" "derive_expected_activity_hourly.py"
if errorlevel 1 goto :error

echo.
echo ----------------------------------------
echo [14/18] Build historical mismatch index
echo ----------------------------------------
"%PYTHON_EXE%" "derive_mismatch_index.py"
if errorlevel 1 goto :error

echo.
echo ----------------------------------------
echo [15/18] Build future expected bee activity
echo ----------------------------------------
"%PYTHON_EXE%" "build_future_expected_activity_hourly.py"
if errorlevel 1 goto :error

echo.
echo ----------------------------------------
echo [16/18] Train residual ML model
echo ----------------------------------------
"%PYTHON_EXE%" "train_residual_model.py"
if errorlevel 1 goto :error

echo.
echo ----------------------------------------
echo [17/18] Predict ML-adjusted future activity
echo ----------------------------------------
"%PYTHON_EXE%" "predict_future_activity_residual.py"
if errorlevel 1 goto :error

echo.
echo ----------------------------------------
echo [18/18] Export static JSON data
echo ----------------------------------------
"%PYTHON_EXE%" "export_static_json.py"
if errorlevel 1 goto :error

echo.
echo ----------------------------------------
echo [extra] Export ML monitor data
echo ----------------------------------------
"%PYTHON_EXE%" "export_ml_monitor_data.py"
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
