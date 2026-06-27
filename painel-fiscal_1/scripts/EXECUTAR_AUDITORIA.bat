@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo ===========================================
echo   AUDITORIA FISCAL - publicar e atualizar
echo ===========================================
pip install supabase >nul 2>&1
python auditoria.py
pause
