#!/usr/bin/env bash
# Publicar Scorecard Logistico - Torre de Controle (macOS/Linux)
# Duplo-clique pode nao funcionar em todos os sistemas; rode:  ./publicar.sh
set -e
cd "$(dirname "$0")"

echo
echo "============================================"
echo "   Publicar Scorecard Logistico"
echo "   Torre de Controle - Genomma Lab"
echo "============================================"
echo

# localizar python
if command -v python3 >/dev/null 2>&1; then PY=python3
elif command -v python >/dev/null 2>&1; then PY=python
else echo "[ERRO] Python 3 nao encontrado."; exit 1; fi

[ -f .env ] || echo "[AVISO] .env nao encontrado. Copie .env.example para .env e preencha a senha."

echo "Checando dependencias..."
$PY -m pip install -r requirements.txt --quiet --disable-pip-version-check

echo
echo "Publicando..."
$PY publicar_scorecard.py --git-all -m "publica Scorecard Logistico"
echo
echo "[OK] Publicacao concluida."
