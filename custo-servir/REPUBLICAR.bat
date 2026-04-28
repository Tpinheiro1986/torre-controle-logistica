@echo off
title Torre de Controle - Republicando...
color 0B
cd /d "%~dp0"

echo.
echo  ====================================================
echo   Republicando dados no dashboard
echo   Nao reprocessa XMLs - usa cte_dados.json existente
echo  ====================================================
echo.

python -m pip install requests --quiet --disable-pip-version-check 2>nul
python processar_cte.py --reclassificar

pause
