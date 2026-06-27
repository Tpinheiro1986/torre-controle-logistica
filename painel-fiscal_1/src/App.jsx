import React, { useEffect, useState } from "react";
import { supabase } from "./lib/supabase.js";
import Login from "./components/Login.jsx";
import PainelFiscal from "./components/PainelFiscal.jsx";
import Importar from "./components/Importar.jsx";

export default function App() {
  const [sessao, setSessao] = useState(null);
  const [pronto, setPronto] = useState(false);
  const [aba, setAba] = useState("painel");
  const [versao, setVersao] = useState(0); // forca recarregar apos importar

  useEffect(() => {
    supabase.auth.getSession().then(({ data }) => { setSessao(data.session); setPronto(true); });
    const { data: sub } = supabase.auth.onAuthStateChange((_e, s) => setSessao(s));
    return () => sub.subscription.unsubscribe();
  }, []);

  if (!pronto) return <div className="min-h-screen flex items-center justify-center text-slate-500">Carregando…</div>;
  if (!sessao) return <Login />;

  return (
    <div className="min-h-screen text-slate-200 p-4 md:p-6" style={{ fontFamily: "Inter, system-ui, sans-serif" }}>
      <div className="max-w-6xl mx-auto">
        <header className="flex items-center justify-between mb-6 pb-4 border-b border-slate-800">
          <div className="flex items-center gap-3">
            <div className="w-9 h-9 rounded-lg bg-gradient-to-br from-cyan-500 to-blue-600 flex items-center justify-center text-white font-bold">F</div>
            <div>
              <h1 className="text-lg font-semibold text-slate-100 leading-tight">Torre de Controle <span className="text-cyan-400">· Módulo Fiscal</span></h1>
              <p className="text-xs text-slate-500">{sessao.user.email}</p>
            </div>
          </div>
          <div className="flex items-center gap-1">
            {[["painel", "Painel"], ["importar", "Importar"]].map(([k, label]) => (
              <button key={k} onClick={() => setAba(k)}
                className={`px-3 py-1.5 rounded text-sm ${aba === k ? "bg-slate-800 text-cyan-400" : "text-slate-500 hover:text-slate-300"}`}>{label}</button>
            ))}
            <button onClick={() => supabase.auth.signOut()} className="ml-2 px-3 py-1.5 rounded text-sm text-slate-500 hover:text-rose-400">Sair</button>
          </div>
        </header>

        {aba === "painel" && <PainelFiscal recarregar={versao} />}
        {aba === "importar" && (
          <Importar aoConcluir={() => { setVersao((v) => v + 1); }} />
        )}
      </div>
    </div>
  );
}
