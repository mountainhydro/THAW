@echo off
setlocal
title THAW Dashboard Launcher

set "BASE_DIR=%~dp0"
set "PY=%BASE_DIR%python_portable\python.exe"
set "SITE_PACKAGES=%BASE_DIR%python_portable\Lib\site-packages"
set "REQ_FILE=%BASE_DIR%requirements.txt"

:: Force Python to use the portable site-packages first
set PYTHONPATH=%SITE_PACKAGES%
set PYTHONDONTWRITEBYTECODE=1

echo [1/3] Validating Environment...

if not exist "%SITE_PACKAGES%\streamlit" (
    echo [ACTION] Initial setup: Installing pinned dependencies...
    :: Using --target is critical for portable environments
    "%PY%" -m pip install --target="%SITE_PACKAGES%" --no-cache-dir --only-binary :all: -r "%REQ_FILE%"
    
    if %errorlevel% neq 0 (
        echo [ERROR] Dependency installation failed. Check internet/requirements.txt.
        pause
        exit /b
    )
)

echo [2/3] Preparing temporary environment...
if not exist "%BASE_DIR%temp" (
    mkdir "%BASE_DIR%temp"
    echo Created temp directory.
) else (
    echo Temp directory already exists, skipping cleanup to preserve credentials.
)

echo [3/3] Starting Streamlit...
"%PY%" -m streamlit run "%BASE_DIR%Dashboard\Dashboard.py" --server.port 8501 --server.address 127.0.0.1 --browser.gatherUsageStats false

pause