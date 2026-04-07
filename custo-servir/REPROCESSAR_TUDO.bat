@echo off
title Torre de Controle - Reprocessando TUDO...
color 0E
cd /d "%~dp0"

echo.
echo  ====================================================
echo   ATENCAO: Reprocessamento completo
echo   Todos os XMLs serao relidos do zero.
echo   Use somente quando necessario:
echo   - Primeira configuracao
echo   - Mudanca de CNPJ
echo   - Correcao de dados historicos
echo  ====================================================
echo.
set /p confirm="Confirma reprocessamento completo? (S/N): "
if /i not "%confirm%"=="S" (
    echo Cancelado.
    pause
    exit /b 0
)

python -m pip install requests --quiet --disable-pip-version-check 2>nul

python processar_cte.py --tudo

pause
