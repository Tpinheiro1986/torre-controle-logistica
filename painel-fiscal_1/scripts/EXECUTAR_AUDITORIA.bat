@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo ===========================================
echo   AUDITORIA - CARGA COMPLETA (2026+)
echo ===========================================
python -m pip install supabase >nul 2>&1
python auditoria.py full
pause
