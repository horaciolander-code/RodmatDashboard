@echo off
title Rodmat Dashboard V2
echo ============================================
echo   Rodmat Dashboard V2 - Starting Services
echo ============================================
echo.

:: Start Backend API (port 8000)
echo [1/4] Starting Backend API...
start "V2-Backend" cmd /c "cd /d %~dp0backend && python run.py"
timeout /t 3 /nobreak >nul

:: Start React Setup Panel (port 3000)
echo [2/4] Starting React Setup Panel...
start "V2-React" cmd /c "cd /d %~dp0frontend\setup-panel && npm run dev"
timeout /t 2 /nobreak >nul

:: Start Streamlit Dashboard (port 8501)
echo [3/4] Starting Streamlit Dashboard...
start "V2-Dashboard" cmd /c "cd /d %~dp0dashboard && python -m streamlit run app.py --server.port 8501"
timeout /t 2 /nobreak >nul

:: Start Scheduler
echo [4/4] Starting Report Scheduler...
start "V2-Scheduler" cmd /c "cd /d %~dp0backend && python scheduler.py"

echo.
echo ============================================
echo   All services started!
echo   Backend API:    http://localhost:8000
echo   Swagger UI:     http://localhost:8000/docs
echo   Setup Panel:    http://localhost:3000
echo   Dashboard:      http://localhost:8501
echo ============================================
echo.
echo Press any key to exit (services keep running)...
pause >nul
