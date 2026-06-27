import React, { useEffect, useState, useMemo } from "react";
import { supabase } from "../lib/supabase.js";

const brl = (v) => Number(v || 0).toLocaleString("pt-BR", { style: "currency", currency: "BRL" });
const fmtChave = (c) => (c || "").replace(/(\d{4})(?=\d)/g, "$1 ");

export default function PainelFiscal({ recarregar }) {
  const [aba, setAba] = useState("vinculos");
  const [notas, setNotas] = useState([]);
  const [manif, setManif] = useState([]);
  const [ctes, setCtes] = useState([]);
  const [romaneios, setRomaneios] = useState([]);
  const [vinculos, setVinculos] = useState([]);
  const [itens, setItens] = useState({});
  const [aberta, setAberta] = useState(null);
  const [carregando, setCarregando] = useState(true);

  async function carregar() {
    setCarregando(true);
    const [{ data: n }, { data: m }, { data: c }, { data: r }, { data: v }] = await Promise.all([
      supabase.from("nfe_notas").select("*").order("data_emissao", { ascending: false }),
      supabase.from("nfe_manifestacoes").select("*").order("data_arquivo", { ascending: false }),
      supabase.from("cte_conhecimentos").select("*").order("data_emissao", { ascending: false }),
      supabase.from("romaneios").select("*").order("data_arquivo", { ascending: false }),
      supabase.from("vw_nota_vinculos").select("*"),
    ]);
    setNotas(n || []); setManif(m || []); setCtes(c || []); setRomaneios(r || []); setVinculos(v || []);
    setCarregando(false);
  }
  useEffect(() => { carregar(); }, [recarregar]);

  async function abrir(nota) {
    if (aberta === nota.id) return setAberta(null);
    if (!itens[nota.id]) {
      const { data } = await supabase.from("nfe_itens").select("*").eq("nota_id", nota.id).order("num_item");
      setItens((s) => ({ ...s, [nota.id]: data || [] }));
    }
    setAberta(nota.id);
  }

  const totais = useMemo(() => ({
    valor: notas.reduce((s, n) => s + Number(n.valor_total || 0), 0),
    ciencia: manif.filter((m) => m.codigo_evento === "0").length,
    recusa: manif.filter((m) => m.codigo_evento === "2").length,
  }), [notas, manif]);

  if (carregando) return <p className="text-slate-400">Carregando…</p>;

  return (
    <>
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-6">
        <Card label="Notas processadas" value={notas.length} accent="text-cyan-400" />
        <Card label="Com CT-e" value={vinculos.filter((v) => v.tem_cte).length} accent="text-violet-400" />
        <Card label="Confirmadas (saída)" value={vinculos.filter((v) => v.tem_confirmacao).length} accent="text-emerald-400" />
        <Card label="Valor total notas" value={brl(totais.valor)} />
      </div>

      <div className="flex gap-1 mb-4 flex-wrap">
        {[["vinculos", "Vínculos (Nota origem)"], ["notas", "Notas Fiscais"], ["ctes", "CT-e"], ["manif", "Confirmações"], ["romaneios", "Romaneios"]].map(([k, label]) => (
          <button key={k} onClick={() => setAba(k)}
            className={`px-4 py-2 rounded-md text-sm font-medium ${aba === k ? "bg-slate-800 text-cyan-400" : "text-slate-500 hover:text-slate-300"}`}>
            {label}
          </button>
        ))}
      </div>

      {aba === "vinculos" && (
        <div className="bg-slate-900/40 border border-slate-800 rounded-lg overflow-x-auto">
          <div className="px-4 py-3 border-b border-slate-800 text-xs text-slate-500">
            Cada nota é a origem. Os selos mostram se o processo seguiu: <span className="text-slate-400">romaneio → CT-e → confirmação de saída</span>.
          </div>
          <table className="w-full text-sm">
            <thead><tr className="text-left text-[11px] uppercase tracking-wider text-slate-500 border-b border-slate-800">
              <th className="py-3 px-3">Nota</th><th className="py-3 px-3">Emitente → Destinatário</th>
              <th className="py-3 px-3 text-right">Valor</th>
              <th className="py-3 px-3 text-center">Romaneio</th>
              <th className="py-3 px-3 text-center">CT-e</th>
              <th className="py-3 px-3 text-center">Confirmação saída</th>
            </tr></thead>
            <tbody>
              {vinculos.map((v) => (
                <tr key={v.id} className="border-b border-slate-800 hover:bg-slate-800/40">
                  <td className="py-3 px-3"><div className="font-medium text-slate-100">{v.numero}/{v.serie}</div>
                    <div className="text-[10px] text-slate-500">{(v.data_emissao || "").slice(0, 10)}</div></td>
                  <td className="py-3 px-3 text-slate-300 text-xs">{v.nome_emitente}<div className="text-slate-500">→ {v.nome_destinatario} ({v.uf_destinatario})</div></td>
                  <td className="py-3 px-3 text-right font-semibold text-slate-100">{brl(v.valor_total)}</td>
                  <td className="py-3 px-3 text-center"><Pill on={v.tem_romaneio} pend /></td>
                  <td className="py-3 px-3 text-center"><Pill on={v.tem_cte} label={v.ctes ? `CT-e ${v.ctes}` : null} /></td>
                  <td className="py-3 px-3 text-center"><Pill on={v.tem_confirmacao} label={v.tem_confirmacao ? (v.data_confirmacao || "").slice(0, 10) : null} /></td>
                </tr>
              ))}
            </tbody>
          </table>
          {vinculos.length === 0 && <p className="p-6 text-center text-slate-500 text-sm">Nenhuma nota ainda.</p>}
        </div>
      )}

      {aba === "notas" && (
        <div className="bg-slate-900/40 border border-slate-800 rounded-lg overflow-x-auto">
          <table className="w-full text-sm">
            <thead><tr className="text-left text-[11px] uppercase tracking-wider text-slate-500 border-b border-slate-800">
              <th className="py-3 px-3">Nota</th><th className="py-3 px-3">Emitente → Dest.</th>
              <th className="py-3 px-3">Natureza</th><th className="py-3 px-3">Emissão</th>
              <th className="py-3 px-3 text-right">Valor</th></tr></thead>
            <tbody>
              {notas.map((n) => (
                <React.Fragment key={n.id}>
                  <tr onClick={() => abrir(n)} className="border-b border-slate-800 hover:bg-slate-800/40 cursor-pointer">
                    <td className="py-3 px-3"><div className="font-medium text-slate-100">{n.numero}/{n.serie}</div>
                      <div className="font-mono text-[10px] text-slate-500">{fmtChave(n.chave)}</div></td>
                    <td className="py-3 px-3 text-slate-300"><div>{n.nome_emitente}</div>
                      <div className="text-xs text-slate-500">{n.uf_emitente} → {n.uf_destinatario}</div></td>
                    <td className="py-3 px-3 text-slate-400 text-xs">{n.natureza_operacao}</td>
                    <td className="py-3 px-3 text-slate-400">{(n.data_emissao || "").slice(0, 10)}</td>
                    <td className="py-3 px-3 text-right font-semibold text-slate-100">{brl(n.valor_total)}</td>
                  </tr>
                  {aberta === n.id && (
                    <tr className="bg-slate-900/40"><td colSpan={5} className="px-4 py-3">
                      {(itens[n.id] || []).map((it) => (
                        <div key={it.id} className="flex justify-between text-xs py-1 text-slate-300 border-b border-slate-800/40">
                          <span>{it.num_item}. {it.descricao} <span className="text-slate-500">— NCM {it.ncm} · CFOP {it.cfop}</span></span>
                          <span className="tabular-nums">{it.quantidade} {it.unidade} · {brl(it.valor_total)}</span>
                        </div>
                      ))}
                      {n.info_complementar && <div className="text-xs text-slate-500 italic mt-2">ℹ {n.info_complementar}</div>}
                    </td></tr>
                  )}
                </React.Fragment>
              ))}
            </tbody>
          </table>
          {notas.length === 0 && <p className="p-6 text-center text-slate-500 text-sm">Nenhuma nota ainda. Use a aba “Importar”.</p>}
        </div>
      )}

      {aba === "manif" && (
        <div className="bg-slate-900/40 border border-slate-800 rounded-lg overflow-x-auto">
          <table className="w-full text-sm">
            <thead><tr className="text-left text-[11px] uppercase tracking-wider text-slate-500 border-b border-slate-800">
              <th className="py-3 px-4">NF nº</th><th className="py-3 px-4">Chave</th>
              <th className="py-3 px-4">Filial (CNPJ)</th><th className="py-3 px-4">Data</th></tr></thead>
            <tbody>
              {manif.map((m) => (
                <tr key={m.id} className="border-b border-slate-800/60 hover:bg-slate-800/30">
                  <td className="py-3 px-4 font-mono text-slate-300">{m.numero_nf || m.sequencia}</td>
                  <td className="py-3 px-4 font-mono text-[10px] text-slate-500">{fmtChave(m.chave_nfe || "")}</td>
                  <td className="py-3 px-4 font-mono text-slate-400">{m.cnpj_empresa || "—"}</td>
                  <td className="py-3 px-4 text-slate-400">{m.data_arquivo}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {manif.length === 0 && <p className="p-6 text-center text-slate-500 text-sm">Nenhuma manifestação ainda.</p>}
        </div>
      )}

      {aba === "ctes" && (
        <div className="bg-slate-900/40 border border-slate-800 rounded-lg overflow-x-auto">
          <table className="w-full text-sm">
            <thead><tr className="text-left text-[11px] uppercase tracking-wider text-slate-500 border-b border-slate-800">
              <th className="py-3 px-3">CT-e</th><th className="py-3 px-3">Transportadora</th>
              <th className="py-3 px-3">Rota</th><th className="py-3 px-3">Emissão</th>
              <th className="py-3 px-3 text-center">NF-e</th><th className="py-3 px-3 text-right">Frete</th></tr></thead>
            <tbody>
              {ctes.map((c) => (
                <tr key={c.id} className="border-b border-slate-800 hover:bg-slate-800/40">
                  <td className="py-3 px-3"><div className="font-medium text-slate-100">{c.numero}/{c.serie}</div>
                    <div className="font-mono text-[10px] text-slate-500">{fmtChave(c.chave)}</div></td>
                  <td className="py-3 px-3 text-slate-300">{c.nome_emitente}</td>
                  <td className="py-3 px-3 text-slate-400">{c.uf_inicio} → {c.uf_fim}</td>
                  <td className="py-3 px-3 text-slate-400">{(c.data_emissao || "").slice(0, 10)}</td>
                  <td className="py-3 px-3 text-center text-slate-400">{c.qtd_nfe}</td>
                  <td className="py-3 px-3 text-right font-semibold text-slate-100">{brl(c.valor_total)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {ctes.length === 0 && <p className="p-6 text-center text-slate-500 text-sm">Nenhum CT-e ainda.</p>}
        </div>
      )}

      {aba === "romaneios" && (
        <div className="bg-slate-900/40 border border-slate-800 rounded-lg overflow-x-auto">
          <table className="w-full text-sm">
            <thead><tr className="text-left text-[11px] uppercase tracking-wider text-slate-500 border-b border-slate-800">
              <th className="py-3 px-3">Identificador</th><th className="py-3 px-3">Arquivo</th>
              <th className="py-3 px-3">Tipo</th><th className="py-3 px-3">Data</th></tr></thead>
            <tbody>
              {romaneios.map((r) => (
                <tr key={r.id} className="border-b border-slate-800 hover:bg-slate-800/40">
                  <td className="py-3 px-3 font-medium text-slate-100">{r.identificador}</td>
                  <td className="py-3 px-3 text-slate-400 text-xs font-mono">{r.arquivo_origem}</td>
                  <td className="py-3 px-3 text-slate-400 uppercase text-xs">{r.tipo_arquivo}</td>
                  <td className="py-3 px-3 text-slate-400">{(r.data_arquivo || "").slice(0, 10)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {romaneios.length === 0 && <p className="p-6 text-center text-slate-500 text-sm">Nenhum romaneio ainda.</p>}
        </div>
      )}
    </>
  );
}

function Pill({ on, label, pend }) {
  if (pend) return <span className="inline-block px-2 py-0.5 rounded-full text-[11px] bg-slate-800 text-slate-500">pendente</span>;
  if (on) return <span className="inline-block px-2 py-0.5 rounded-full text-[11px] bg-emerald-500/15 text-emerald-400">{label || "✓ sim"}</span>;
  return <span className="inline-block px-2 py-0.5 rounded-full text-[11px] bg-slate-800/60 text-slate-600">—</span>;
}

function Card({ label, value, accent = "text-slate-100" }) {
  return (
    <div className="bg-slate-900/60 border border-slate-700/60 rounded-lg px-5 py-4">
      <div className="text-[11px] uppercase tracking-widest text-slate-400">{label}</div>
      <div className={`text-2xl font-semibold mt-1 tabular-nums ${accent}`}>{value}</div>
    </div>
  );
}
