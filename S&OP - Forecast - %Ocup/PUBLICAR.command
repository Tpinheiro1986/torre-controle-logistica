#!/bin/bash
# ============================================================================
#  PUBLICADOR DO SIMULADOR S&OP — macOS / Linux
#
#  Duplo-clique neste arquivo para executar (macOS).
#  No Linux: chmod +x e ./PUBLICAR.command
# ============================================================================

cd "$(dirname "$0")"

# Tentar python3 primeiro
if command -v python3 >/dev/null 2>&1; then
    python3 publicar_simulador.py
    echo
    read -p "Pressione Enter para fechar."
    exit 0
fi

# Tentar python (sem versão)
if command -v python >/dev/null 2>&1; then
    python publicar_simulador.py
    echo
    read -p "Pressione Enter para fechar."
    exit 0
fi

# Não tem Python
echo ""
echo "  ====================================================================="
echo "   PYTHON NÃO ESTÁ INSTALADO"
echo "  ====================================================================="
echo ""
echo "   Este script precisa de Python para funcionar."
echo ""
echo "   Como instalar (macOS):"
echo "   • Mais fácil: rode 'brew install python3' no Terminal"
echo "   • Sem Homebrew: baixe em https://www.python.org/downloads/macos/"
echo ""
echo "   Depois de instalar, rode este arquivo de novo."
echo ""
echo "  ====================================================================="
read -p "Pressione Enter para fechar."
