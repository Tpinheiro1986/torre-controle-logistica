@echo off
chcp 65001 >nul
title Aplicar Scorecard - Custo Logistico (R$)
echo ============================================================
echo  Torre de Controle - Aplicar indicadores financeiros (R$)
echo ============================================================
echo.
python "%~dp0aplicar_custo_logistico.py"
echo.
pause
