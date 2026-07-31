@echo off
chcp 65001 >nul 2>&1
setlocal
title Torre de Controle - Atualizador S^&OP

cd /d "%~dp0."

echo.
echo  ================================================
echo   TORRE DE CONTROLE - ATUALIZADOR
echo  ================================================
echo.

set "PY="
where py >nul 2>&1 && set "PY=py -3"
if not defined PY where python >nul 2>&1 && set "PY=python"
if not defined PY goto SEMPYTHON

%PY% "%~dp0atualizador.py" %*
set "RC=%ERRORLEVEL%"
if not "%RC%"=="0" goto ERRO
endlocal
exit /b 0

:SEMPYTHON
echo  [X] Python nao encontrado neste computador.
echo.
echo  Instale em https://www.python.org/downloads/windows/
echo  IMPORTANTE: marque a caixa "Add python.exe to PATH" na
echo  primeira tela do instalador, senao este atalho nao acha o Python.
echo.
echo  Depois de instalar, feche e abra esta janela de novo.
echo.
pause
endlocal
exit /b 1

:ERRO
echo.
echo  [X] A atualizacao terminou com erro (codigo %RC%).
echo      O detalhe completo ficou salvo na pasta _logs.
echo.
pause
endlocal
exit /b %RC%
