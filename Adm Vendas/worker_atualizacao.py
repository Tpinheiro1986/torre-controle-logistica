# -*- coding: utf-8 -*-
"""
worker_atualizacao.py — Torre de Controle ADM de Vendas
Roda em QUALQUER máquina da rede interna (seu PC ou o servidor Helix) e deixa
tudo automático:

  1) Fica de olho na fila adm_vendas.solicitacao_atualizacao (o botão do
     dashboard insere ali). Viu pedido pendente -> roda a ingestão da API
     LOG023 (modo diário) e marca concluída/erro.
  2) Mesmo sem ninguém apertar o botão, roda a ingestão sozinho a cada
     WORKER_AUTO_MIN minutos (default 60).
  3) Nunca roda duas vezes em menos de 5 minutos (proteção contra clique
     repetido / spam na fila — vários pedidos pendentes viram UMA execução).

Uso (deixe rodando em janela/serviço/tarefa agendada "ao iniciar"):
  python worker_atualizacao.py

Precisa das MESMAS variáveis de ambiente do ingestao_api.py
(LOG023_API_BASE, LOG023_API_USER, LOG023_API_PASS, SUPABASE_URL, SUPABASE_KEY)
e do ingestao_api.py na mesma pasta.

Opcionais:
  WORKER_POLL_SEG   intervalo de checagem da fila em segundos (default 20)
  WORKER_AUTO_MIN   intervalo da atualização automática em minutos (default 60)
"""

import json
import os
import time
import traceback
from datetime import datetime

import ingestao_api as ing

POLL_SEG = int(os.environ.get("WORKER_POLL_SEG", "20"))
AUTO_MIN = int(os.environ.get("WORKER_AUTO_MIN", "60"))
INTERVALO_MINIMO_SEG = 300  # 5 min entre execuções, sempre


def agora_iso():
    return datetime.utcnow().isoformat() + "Z"


def buscar_pendentes():
    r = ing.sb("GET", "solicitacao_atualizacao?status=eq.pendente&select=id&order=id")
    return [x["id"] for x in r.json()]


def marcar(ids, campos):
    if not ids:
        return
    lista = ",".join(str(i) for i in ids)
    ing.sb("PATCH", f"solicitacao_atualizacao?id=in.({lista})",
           headers={"Prefer": "return=minimal"}, data=json.dumps(campos))


def executar_ingestao():
    """Roda o modo diário (mês atual + janelas com pedidos abertos)."""
    ing.modo_diario()


def main():
    ing.checar_config()
    print(f"[worker] iniciado. Fila a cada {POLL_SEG}s | automático a cada {AUTO_MIN}min")
    ultima_execucao = 0.0

    while True:
        try:
            pendentes = buscar_pendentes()
            passou = time.time() - ultima_execucao
            por_botao = bool(pendentes)
            por_agenda = passou >= AUTO_MIN * 60

            if (por_botao or por_agenda) and passou >= INTERVALO_MINIMO_SEG or ultima_execucao == 0:
                if por_botao:
                    print(f"[worker] {len(pendentes)} solicitação(ões) do botão -> executando")
                    marcar(pendentes, {"status": "executando", "iniciado_em": agora_iso()})
                else:
                    print("[worker] atualização automática agendada -> executando")

                try:
                    executar_ingestao()
                    marcar(pendentes, {"status": "concluida", "concluido_em": agora_iso(),
                                       "detalhe": "Ingestão diária executada com sucesso"})
                    print("[worker] execução concluída ✔")
                except SystemExit:
                    raise
                except Exception as e:
                    erro = f"{e.__class__.__name__}: {e}"
                    print(f"[worker] ERRO na ingestão: {erro}")
                    traceback.print_exc()
                    marcar(pendentes, {"status": "erro", "concluido_em": agora_iso(),
                                       "detalhe": erro[:500]})
                ultima_execucao = time.time()

            elif por_botao and passou < INTERVALO_MINIMO_SEG:
                espera = int(INTERVALO_MINIMO_SEG - passou)
                print(f"[worker] pedido na fila, mas última execução foi há pouco — "
                      f"aguardando {espera}s (anti-spam)")

        except KeyboardInterrupt:
            print("[worker] encerrado pelo usuário")
            return
        except Exception as e:
            # Falha de rede com o Supabase etc.: loga e continua vivo
            print(f"[worker] falha no ciclo ({e}); tentando de novo em {POLL_SEG}s")

        time.sleep(POLL_SEG)


if __name__ == "__main__":
    main()
