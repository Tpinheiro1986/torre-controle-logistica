@echo off
chcp 65001 >nul
cd /d "%~dp0"
title Publicar - Torre de Controle Logistica

rem Descobre o lancador do Python (py e o mais confiavel no Windows)
where py >nul 2>nul && (set "PY=py") || (set "PY=python")

:menu
cls
echo ============================================================
echo    PUBLICAR  -  Torre de Controle Logistica
echo ============================================================
echo.
echo    Pasta atual: %cd%
echo.
echo    [1] Pre-visualizar      (dry-run, NAO altera nada)
echo    [2] Publicar TUDO       (Supabase + GitHub)
echo    [3] Publicar so DADOS   (Supabase: tabelas + clientes)
echo    [4] Publicar so SITE    (GitHub Pages: index.html)
echo    [0] Sair
echo.
set "opc="
set /p "opc=Digite o numero e tecle Enter: "

if "%opc%"=="1" ( %PY% deploy.py --dry-run & goto fim )
if "%opc%"=="2" ( %PY% deploy.py & goto fim )
if "%opc%"=="3" ( %PY% deploy.py --so-supabase & goto fim )
if "%opc%"=="4" ( %PY% deploy.py --so-github & goto fim )
if "%opc%"=="0" ( exit /b 0 )
echo.
echo  Opcao invalida. Tente de novo.
timeout /t 2 >nul
goto menu

:fim
echo.
echo ============================================================
echo    Processo finalizado. Confira as mensagens acima.
echo    (Se publicou o site, espere ~1-2 min e de Ctrl+F5 no navegador.)
echo ============================================================
echo.
pause
goto menu
