@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo ============================================
echo   AUDITORIA - atualizacao diaria (so novos)
echo ============================================
python -m pip install supabase >nul 2>&1
python auditoria.py sync
echo.
echo Concluido. Pode fechar esta janela.
pause
