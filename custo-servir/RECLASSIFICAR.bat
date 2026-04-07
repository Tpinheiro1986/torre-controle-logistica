@echo off
title Torre de Controle - Reclassificando...
color 0B
cd /d "%~dp0"

echo.
echo  ====================================================
echo   RECLASSIFICACAO RAPIDA
echo   Aplica novas regras de Inbound/Outbound/Reversa
echo   ao historico sem reler os XMLs.
echo   Use sempre que adicionar fornecedores ao Inbound.
echo   Tempo estimado: menos de 1 minuto.
echo  ====================================================
echo.

python -m pip install requests --quiet --disable-pip-version-check 2>nul
python processar_cte.py --reclassificar

pause
