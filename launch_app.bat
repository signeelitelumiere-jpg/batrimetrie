@echo off
REM Launch Streamlit using the project's bundled Python and kill any process on port 8501 first
setlocal

echo Stopping any process listening on port 8501...
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":8501"') do (
    echo Found PID %%a, attempting to kill...
    taskkill /PID %%a /F >nul 2>&1
)

REM Small pause to allow sockets to free
ping -n 2 127.0.0.1 >nul

echo Starting Streamlit (port 8501)...
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8
REM Use project-local python if present, otherwise rely on PATH
if exist "%~dp0environment\python.exe" (
    start "Streamlit" "%~dp0environment\python.exe" -m streamlit run "%~dp0app.py" --server.port 8501
) else (
    start "Streamlit" python -m streamlit run "%~dp0app.py" --server.port 8501
)

endlocal
echo Launched. Open http://localhost:8501 in your browser.
