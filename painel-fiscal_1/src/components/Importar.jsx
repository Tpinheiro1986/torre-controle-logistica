import React, { useState } from "react";
import { supabase } from "../lib/supabase.js";
import { parseNFe, parseManifesto } from "../lib/parsers.js";

export default function Importar({ aoConcluir }) {
  const [log, setLog] = useState([]);
  const [ocupado, setOcupado] = useState(false);

  function add(msg, tipo = "info") { setLog((l) => [...l, { msg, tipo }]); }

  async function processar(arquivos) {
    setOcupado(true); setLog([]);
    for (const file of arquivos) {
      const nome = file.name;
      try {
        const texto = await file.text();
        if (nome.toLowerCase().endsWith(".xml")) {
          const { nota, itens } = parseNFe(texto, nome);
          const { data, error } = await supabase
            .from("nfe_notas").upsert(nota, { onConflict: "chave" }).select("id").single();
          if (error) throw error;
          const notaId = data.id;
          await supabase.from("nfe_itens").delete().eq("nota_id", notaId);
          if (itens.length) {
            const { error: e2 } = await supabase.from("nfe_itens")
              .insert(itens.map((it) => ({ ...it, nota_id: notaId })));
            if (e2) throw e2;
          }
          add(`✓ NF-e ${nota.numero} — ${itens.length} itens`, "ok");
        } else if (/^manife.*\.txt$/i.test(nome) || nome.toLowerCase().endsWith(".txt")) {
          const regs = parseManifesto(texto, nome);
          if (regs.length) {
            const comChave = regs.filter((r) => r.chave_nfe);
            const semChave = regs.filter((r) => !r.chave_nfe);
            if (comChave.length) {
              const { error } = await supabase.from("nfe_manifestacoes")
                .upsert(comChave, { onConflict: "chave_nfe,arquivo_origem" });
              if (error) throw error;
            }
            if (semChave.length) {
              const { error } = await supabase.from("nfe_manifestacoes").insert(semChave);
              if (error) throw error;
            }
          }
          add(`✓ Manifestação ${nome} — ${regs.length} eventos`, "ok");
        } else {
          add(`• Ignorado: ${nome}`, "info");
        }
      } catch (e) {
        add(`✗ Erro em ${nome}: ${e.message}`, "erro");
      }
    }
    setOcupado(false);
    aoConcluir && aoConcluir();
  }

  function onDrop(e) {
    e.preventDefault();
    if (!ocupado) processar([...e.dataTransfer.files]);
  }

  return (
    <div>
      <div
        onDragOver={(e) => e.preventDefault()} onDrop={onDrop}
        className="border-2 border-dashed border-slate-700 rounded-xl p-10 text-center bg-slate-900/30">
        <p className="text-slate-300 font-medium">Arraste os arquivos aqui</p>
        <p className="text-slate-500 text-sm mt-1">XML de NF-e e TXT de manifestação (MANIFE…)</p>
        <label className="inline-block mt-4 px-4 py-2 rounded bg-cyan-600 hover:bg-cyan-500 text-white text-sm cursor-pointer">
          Ou escolher arquivos
          <input type="file" multiple accept=".xml,.txt" className="hidden"
            onChange={(e) => processar([...e.target.files])} />
        </label>
      </div>

      {ocupado && <p className="text-cyan-400 text-sm mt-4">Processando…</p>}

      {log.length > 0 && (
        <div className="mt-4 bg-slate-950 border border-slate-800 rounded-lg p-3 font-mono text-xs space-y-1">
          {log.map((l, i) => (
            <div key={i} className={
              l.tipo === "ok" ? "text-emerald-400" : l.tipo === "erro" ? "text-rose-400" : "text-slate-500"}>
              {l.msg}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
