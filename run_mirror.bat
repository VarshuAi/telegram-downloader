@echo off
title Telegram Channel Mirror (Local Runner)
cd /d "%~dp0"
echo ========================================================
echo   Telegram Channel Mirror - Local Runner
echo ========================================================
echo.
python -u channel_mirror.py
echo.
echo ========================================================
echo   Mirror process finished or paused!
echo ========================================================
pause
