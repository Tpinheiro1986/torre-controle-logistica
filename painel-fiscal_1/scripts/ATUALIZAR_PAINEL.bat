@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo Atualizando apenas o painel (sem varrer as pastas)...
python auditoria.py painel
pause
