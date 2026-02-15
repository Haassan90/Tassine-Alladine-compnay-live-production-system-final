@echo off
REM ==============================
REM Taco Dashboard - Final Deployment
REM ==============================

REM Activate virtual environment
call venv\Scripts\activate

REM Kill any process on port 8000
for /f "tokens=5" %%a in ('netstat -aon ^| findstr :8000 ^| findstr LISTENING') do (
    echo Killing process %%a using port 3333
    taskkill /PID %%a /F
)

REM Navigate to project folder
cd /d %~dp0

REM Start FastAPI with uvicorn
echo Starting Taco Dashboard...
uvicorn main:app --host 0.0.0.0 --port 3333

pause
