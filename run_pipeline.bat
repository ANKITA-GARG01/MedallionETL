@echo off
:: =============================================================
::  run_pipeline.bat  —  Medallion ETL Pipeline Runner
::  Called by Windows Task Scheduler
:: =============================================================

set PYTHON=C:\Users\deepa\AppData\Local\Microsoft\WindowsApps\python3.13.exe
set PROJECT=C:\MedallionETL
set LOGFILE=%PROJECT%\logs\scheduler.log

:: Create logs folder if missing
if not exist "%PROJECT%\logs" mkdir "%PROJECT%\logs"

:: Log start time
echo. >> "%LOGFILE%"
echo ========================================== >> "%LOGFILE%"
echo Started: %DATE% %TIME% >> "%LOGFILE%"

:: Run the pipeline, append output to scheduler log
cd /d "%PROJECT%"
"%PYTHON%" etl_pipeline.py >> "%LOGFILE%" 2>&1

:: Log exit code
echo Finished: %DATE% %TIME%  ExitCode=%ERRORLEVEL% >> "%LOGFILE%"
echo ========================================== >> "%LOGFILE%"
