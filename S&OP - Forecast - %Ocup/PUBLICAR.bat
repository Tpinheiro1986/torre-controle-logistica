@echo off
REM ============================================================================
REM  PUBLICADOR DO SIMULADOR S^&OP — Windows
REM
REM  Duplo-clique neste arquivo para executar.
REM ============================================================================

title Publicador do Simulador S^&OP

REM Forcar UTF-8 no terminal (chcp 65001) para suportar caracteres especiais
chcp 65001 >nul 2>nul

REM Variavel para Python entender que o terminal e UTF-8
set PYTHONIOENCODING=utf-8
set PYTHONUTF8=1

REM Tentar python (Python 3.x do Windows Store ou instalado)
where python >nul 2>nul
if %errorlevel% == 0 (
    python "%~dp0publicar_simulador.py"
    goto :fim
)

REM Tentar py (Python Launcher)
where py >nul 2>nul
if %errorlevel% == 0 (
    py "%~dp0publicar_simulador.py"
    goto :fim
)

REM Não tem Python
echo.
echo  =====================================================================
echo   PYTHON NAO ESTA INSTALADO
echo  =====================================================================
echo.
echo   Este script precisa de Python para funcionar.
echo.
echo   Como instalar:
echo   1. Abra a Microsoft Store
echo   2. Procure por "Python 3.12" (ou versao mais nova)
echo   3. Clique em "Obter" ^(e sim, e gratis^)
echo   4. Depois de instalar, rode este arquivo de novo
echo.
echo   Ou baixe direto em: https://www.python.org/downloads/
echo.
echo  =====================================================================
echo.
pause
goto :eof

:fim
echo.
pause
