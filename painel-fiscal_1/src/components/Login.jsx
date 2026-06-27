import React, { useState } from "react";
import { supabase } from "../lib/supabase.js";

export default function Login() {
  const [email, setEmail] = useState("");
  const [senha, setSenha] = useState("");
  const [erro, setErro] = useState("");
  const [carregando, setCarregando] = useState(false);

  async function entrar() {
    setErro(""); setCarregando(true);
    const { error } = await supabase.auth.signInWithPassword({ email, password: senha });
    if (error) setErro("E-mail ou senha incorretos.");
    setCarregando(false);
  }

  return (
    <div className="min-h-screen flex items-center justify-center text-slate-200 p-4">
      <div className="w-full max-w-sm bg-slate-900/60 border border-slate-700 rounded-xl p-6">
        <h1 className="text-lg font-semibold mb-1">Torre de Controle</h1>
        <p className="text-sm text-slate-500 mb-5">Módulo Fiscal — entre para continuar</p>
        <label className="text-xs text-slate-400">E-mail</label>
        <input value={email} onChange={(e) => setEmail(e.target.value)} type="email"
          className="w-full mb-3 mt-1 px-3 py-2 rounded bg-slate-950 border border-slate-700 text-sm outline-none focus:border-cyan-500" />
        <label className="text-xs text-slate-400">Senha</label>
        <input value={senha} onChange={(e) => setSenha(e.target.value)} type="password"
          onKeyDown={(e) => e.key === "Enter" && entrar()}
          className="w-full mb-4 mt-1 px-3 py-2 rounded bg-slate-950 border border-slate-700 text-sm outline-none focus:border-cyan-500" />
        {erro && <p className="text-rose-400 text-xs mb-3">{erro}</p>}
        <button onClick={entrar} disabled={carregando}
          className="w-full py-2 rounded bg-cyan-600 hover:bg-cyan-500 text-white text-sm font-medium disabled:opacity-50">
          {carregando ? "Entrando…" : "Entrar"}
        </button>
      </div>
    </div>
  );
}
