@echo off
setlocal

REM --- EDIT THESE TWO PATHS ---
set "VENV_ACTIVATE=C:\Users\YOUR_USER\venvs\tdinspections_infra\Scripts\activate.bat"
set "REPO_ROOT=C:\path\to\tdinspections_infra"

REM --- Activate venv ---
call "%VENV_ACTIVATE%"
if errorlevel 1 (
  echo Failed to activate venv at: %VENV_ACTIVATE%
  pause
  exit /b 1
)

REM --- Go to repo root ---
cd /d "%REPO_ROOT%"
if errorlevel 1 (
  echo Failed to cd to repo root: %REPO_ROOT%
  pause
  exit /b 1
)

echo.
echo Ready. Venv activated and current directory set to:
cd
echo.
cmd /k
