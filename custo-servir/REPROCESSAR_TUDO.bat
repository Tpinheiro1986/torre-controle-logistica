@echo off
title Torre de Controle — Reprocessando TUDO...
color 0E
echo.
echo  ====================================================
echo   ATENCAO: Reprocessamento completo
echo   Todos os XMLs serao relidos do zero.
echo   Use somente quando necessario (correcao de dados,
echo   mudanca de CNPJ, etc.)
echo  ====================================================
echo.
set /p confirm="Confirma reprocessamento completo? (S/N): "
if /i not "%confirm%"=="S" (
    echo Cancelado.
    pause
    exit /b 0
)
cd /d "%~dp0"
pip install requests --quiet --disable-pip-version-check
echo.
python processar_cte.py --reprocessar %*
echo.
pause
