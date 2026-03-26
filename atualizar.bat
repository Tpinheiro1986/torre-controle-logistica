@echo off
echo [1/4] Sincronizando com o GitHub...
git pull origin main --rebase

echo [2/4] Adicionando todos os arquivos alterados...
git add .

echo [3/4] Criando commit com data/hora automatica...
set dt=%date% %time%
git commit -m "Atualizacao automatica - %dt%"

echo [4/4] Publicando no GitHub...
git push origin main

echo ============================================
echo  Publicado com sucesso!
echo  https://tpinheiro1986.github.io/torre-controle-logistica
echo ============================================
pause