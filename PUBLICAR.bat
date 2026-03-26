@echo off
title Torre de Controle — Publicando no GitHub...
color 0A
echo.
echo  ============================================
echo   TORRE DE CONTROLE LOGISTICA
echo   Publicando atualizacoes no GitHub...
echo  ============================================
echo.

cd /d "%~dp0"

echo  [1/4] Verificando alteracoes...
git status

echo.
echo  [2/4] Adicionando todos os arquivos alterados...
git add .

echo.
echo  [3/4] Criando commit com data/hora automatica...
for /f "tokens=2 delims==" %%I in ('wmic os get localdatetime /value') do set dt=%%I
set DT=%dt:~6,2%/%dt:~4,2%/%dt:~0,4% %dt:~8,2%:%dt:~10,2%
git commit -m "Atualizacao automatica - %DT%"

echo.
echo  [4/4] Publicando no GitHub...
git push origin main

echo.
echo  ============================================
echo   Publicado com sucesso!
echo   Site atualizado em 1-2 minutos.
echo   https://tpinheiro1986.github.io/torre-controle-logistica
echo  ============================================
echo.
pause
