@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo ============================================
echo   Publicador do Scorecard - Torre de Controle
echo ============================================
echo.
python publicar_scorecard.py
if errorlevel 1 (
  echo.
  echo *** Ocorreu um erro. Leia as mensagens acima. ***
)
echo.
pause
