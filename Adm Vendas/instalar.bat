@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo Instalando dependencias Python...
pip install -r ingestao\requirements.txt
if not exist config.bat copy config.exemplo.bat config.bat
echo.
echo Pronto! Agora:
echo  1. Edite o config.bat e cole a SERVICE_ROLE key do Supabase
echo     (Dashboard ^> Settings ^> API ^> service_role)
echo  2. Pasta do LST ja configurada: Y:\ERP-12\rpw
echo  3. Rode atualizar_torre.bat para testar
echo  4. Agende atualizar_torre.bat no Task Scheduler (diario)
pause
