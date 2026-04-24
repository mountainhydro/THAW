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

:: ── Check Windows Long Path support ────────────────────────────────────────
echo [0/3] Checking Windows Long Path support...

:: Measure the length of BASE_DIR by writing it to a temp file and checking size
echo %BASE_DIR%>"%TEMP%\thaw_path_check.tmp"
for %%F in ("%TEMP%\thaw_path_check.tmp") do set PATH_LEN=%%~zF
del "%TEMP%\thaw_path_check.tmp" 2>nul

:: Warn if path exceeds 250 chars (leaves headroom for deep package paths)
if %PATH_LEN% GTR 250 (
    echo.
    echo [WARNING] Your THAW installation path is too long (%PATH_LEN% characters^).
    echo          This may cause pip to fail installing some packages on Windows.
    echo.
    echo          OPTION A ^(Recommended^): Move the THAW folder to a shorter path,
    echo          for example:  C:\THAW\  or  D:\THAW\
    echo          Then re-run this launcher from the new location.
    echo.
    echo          OPTION B: Enable Windows Long Path support manually.
    echo          Open PowerShell as Administrator and run:
    echo          New-ItemProperty -Path HKLM:\SYSTEM\CurrentControlSet\Control\FileSystem -Name LongPathsEnabled -Value 1 -PropertyType DWORD -Force
    echo          Then restart your computer and re-run this launcher.
    echo.
    echo Press any key to continue anyway, or close this window to abort.
    pause
) else (
    echo [OK] Installation path length is fine (%PATH_LEN% characters^).
)

:: ── Install dependencies ────────────────────────────────────────────────────
echo [1/3] Validating Environment...

if not exist "%SITE_PACKAGES%\streamlit" (
    echo [ACTION] Initial setup: Installing pinned dependencies...
    "%PY%" -m pip install --target="%SITE_PACKAGES%" --no-cache-dir --only-binary :all: -r "%REQ_FILE%"

    if %errorlevel% neq 0 (
        echo [ERROR] Dependency installation failed. Check internet connection and requirements.txt.
        pause
        exit /b
    )
)

:: ── Temp directory ──────────────────────────────────────────────────────────
echo [2/3] Preparing temporary environment...
if not exist "%BASE_DIR%temp" (
    mkdir "%BASE_DIR%temp"
    echo Created temp directory.
) else (
    echo Temp directory already exists, skipping cleanup to preserve credentials.
)

:: ── Launch ──────────────────────────────────────────────────────────────────
echo [3/3] Starting Streamlit... please don't close this window!
"%PY%" -m streamlit run "%BASE_DIR%Dashboard\Dashboard.py" --server.port 8501 --server.address 127.0.0.1 --browser.gatherUsageStats false

pause
