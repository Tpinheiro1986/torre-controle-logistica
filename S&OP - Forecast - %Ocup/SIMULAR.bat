@echo off
chcp 65001 >nul 2>&1
setlocal
title Simulacao - Atualizador S^&OP
cd /d "%~dp0."
echo.
echo  MODO SIMULACAO - le os arquivos e mostra o que faria,
echo  sem gravar nada no banco e sem publicar no painel.
echo.
set "PY="
where py >nul 2>&1 && set "PY=py -3"
if not defined PY where python >nul 2>&1 && set "PY=python"
if not defined PY (echo  [X] Python nao encontrado. Veja o LEIA-ME.txt & pause & exit /b 1)
%PY% "%~dp0atualizador.py" --dry-run
echo.
pause
endlocal
