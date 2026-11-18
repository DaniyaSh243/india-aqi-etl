@echo off
cd /d "%~dp0"
echo ==============================================================
echo 🇮🇳 India Air Quality & Weather ETL - Streamlit Dashboard
echo ==============================================================
echo.
echo Launching Streamlit at http://localhost:8501 ...
call ".\venv\Scripts\streamlit.exe" run app.py
pause
