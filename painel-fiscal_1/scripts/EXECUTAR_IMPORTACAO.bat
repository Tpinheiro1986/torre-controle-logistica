@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo ============================================
echo   Torre de Controle - Importacao Fiscal
echo ============================================
echo.
echo Instalando/atualizando dependencias...
pip install -r requirements.txt
echo.
echo Lendo as pastas e enviando para o painel...
python importar.py
echo.
pause
