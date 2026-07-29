@echo off
chcp 65001 >nul
cd /d "%~dp0"
if not exist config.bat (
  echo [ERRO] config.bat nao encontrado
  pause
  exit /b 1
)
call config.bat

echo [1/3] Rodando ingestao do ESPD022...
python ingestao\ingestao_diaria.py
if errorlevel 1 (
  echo [ERRO] Falha na ingestao. Verifique o LST e a chave do Supabase.
  pause
  exit /b 1
)

echo [2/3] Commit no Git...
cd /d "%~dp0.."
git add adm-vendas
git commit -m "carga diaria %date%"

echo [3/3] Publicando no GitHub...
git pull --rebase origin main
git push
if errorlevel 1 (
  echo [ERRO] Falha no git push.
  pause
  exit /b 1
)
echo Torre atualizada e publicada com sucesso!
exit /b 0
