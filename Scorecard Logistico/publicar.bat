@echo off
chcp 65001 >nul
title Publicar Scorecard Logistico - Torre de Controle
cd /d "%~dp0"

echo.
echo ============================================
echo    Publicar Scorecard Logistico
echo    Torre de Controle - Genomma Lab
echo ============================================
echo.

rem ---- localizar o Python ----
set "PY="
where py >nul 2>nul && set "PY=py -3"
if not defined PY (
  where python >nul 2>nul && set "PY=python"
)
if not defined PY (
  echo [ERRO] Python 3 nao encontrado no PATH.
  echo        Instale em https://www.python.org e marque "Add Python to PATH".
  echo.
  pause
  exit /b 1
)

rem ---- conferir o .env (senha do admin) ----
if not exist ".env" (
  echo [AVISO] Arquivo .env nao encontrado nesta pasta.
  echo         Copie ".env.example" para ".env" e preencha SCORECARD_ADMIN_PASSWORD.
  echo.
)

rem ---- dependencias ----
echo Checando dependencias (requests, python-dotenv)...
%PY% -m pip install -r requirements.txt --quiet --disable-pip-version-check
echo.

rem ---- PUBLICAR TUDO: dados no Supabase + push de todas as alteracoes no GitHub ----
echo Publicando...
echo.
%PY% publicar_scorecard.py --git-all -m "publica Scorecard Logistico"
set "RC=%ERRORLEVEL%"

echo.
if "%RC%"=="0" (
  echo [OK] Publicacao concluida com sucesso.
) else (
  echo [FALHA] A publicacao terminou com erro (codigo %RC%^). Veja as mensagens acima.
)
echo.
pause
