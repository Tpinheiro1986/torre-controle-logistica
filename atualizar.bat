@echo off
echo Iniciando atualizacao da Torre de Controle...
git add .
git commit -m "update automatico %date% %time%"
git push
echo.
echo Tudo pronto! Codigo enviado para o GitHub.
pause