@echo off
setlocal
cd /d "%~dp0"
where py >nul 2>nul
if %ERRORLEVEL% EQU 0 (
    py -3 -X utf8 download.py --profile laptop %*
) else (
    python -X utf8 download.py --profile laptop %*
)
set "run_exit=%ERRORLEVEL%"
echo.
echo Exit code: %run_exit%
pause
exit /b %run_exit%
