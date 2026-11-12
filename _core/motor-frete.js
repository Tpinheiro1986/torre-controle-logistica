/* ============================================================================
 * MOTOR DE FRETE — compartilhado (Torre de Controle)
 * CÓPIA LITERAL das funções de cálculo do Simulador de Frete
 * (simulador-frete/index.html, commit 7b4f6e3): FATOR_M3, CAPACIDADE_VEICULO,
 * getCapacidadeVeiculo, norm, calcularPesoConsiderado, calcularFretePeso,
 * aplicarTaxas, simular, fmtBR, fmtKg.
 * EMBUTIDO em custo-servir/index.html (o painel é publicado como arquivo único):
 * ao mudar este arquivo, atualize também a cópia dentro do index.html.
 * Usado pela aba Auditoria do Custo de Servir para garantir o MESMO cálculo do
 * simulador. Se a regra do simulador mudar, atualize este arquivo junto.
 * Nenhuma linha das funções foi alterada; só foram embrulhadas num escopo
 * próprio e expostas em window.MotorFrete.
 * ========================================================================== */
(function(){
let TABELAS = null;
const FATOR_M3      = 300;  // kg/m³ — padrão das tabelas

// Capacidade máxima por tipo de veículo (em kg).
// A detecção é feita por palavra-chave no NOME da tabela.
// Tabelas que não casam com nenhuma palavra-chave (ex: ATIVA 2026, FL BRASIL 2025)
// são tratadas como FRACIONADAS — sem limite de peso (sempre aparecem).
const CAPACIDADE_VEICULO = [
  { chave: 'FIORINO',  kg:    700, label: 'Fiorino'  },
  { chave: 'VAN',      kg:   1400, label: 'Van'      },
  { chave: 'VUC',      kg:   2200, label: 'VUC'      },
  { chave: ' 34',      kg:   4500, label: '3/4'      },   // ALA CARGO 34
  { chave: 'TOCO',     kg:   7500, label: 'Toco'     },
  { chave: 'TRUCK',    kg:  13500, label: 'Truck'    },
  { chave: 'CARRETA',  kg:  30000, label: 'Carreta'  },
];

function getCapacidadeVeiculo(nomeTabela){
  const up = (nomeTabela||'').toUpperCase();
  for(const v of CAPACIDADE_VEICULO){
    if(up.includes(v.chave)) return v;
  }
  return null; // fracionada — sem limite
}

function norm(s){
  return (s||'').toString().normalize('NFD').replace(/[\u0300-\u036f]/g,'').toLowerCase().trim();
}

function calcularPesoConsiderado(pesoKg, m3){
  const cubado = (m3||0) * FATOR_M3;
  return Math.max(pesoKg||0, cubado);
}

// Aplica regras de peso da rota e devolve o frete-peso base.
// Lida com 2 modelos de tabela:
//  (a) FRACIONADA: faixas progressivas de peso (Até 50kg: R$ X, Até 100kg: R$ Y, Acima de 100kg: × Z/kg)
//  (b) CARGA FECHADA: "Até 1kg: R$ X" + "Acima de 1kg: × 0" -> valor fixo R$ X independente do peso
function calcularFretePeso(regras, pesoConsiderado){
  if(!regras || !regras.length) return null;
  // Faixas "até X kg": podem ser R$ fixo (ATE_KG) OU multiplicador (ATE_KG_MULT)
  const faixas = regras.filter(r=>r.tipo==='ATE_KG' || r.tipo==='ATE_KG_MULT').sort((a,b)=>a.kg-b.kg);
  const ate = regras.filter(r=>r.tipo==='ATE_KG').sort((a,b)=>a.kg-b.kg);
  const acima = regras.find(r=>r.tipo==='ACIMA_KG_MULT');

  // Detectar carga fechada: ACIMA com multiplicador 0 (qualquer peso superior à faixa cai em 0)
  const cargaFechada = acima && (acima.multiplicador === 0 || acima.multiplicador < 0.0001);
  if(cargaFechada && ate.length){
    // Carga fechada: usa o valor da maior faixa ATE (geralmente única)
    const faixa = ate[ate.length-1];
    return { valor: faixa.valor, modo:`Carga fechada (veículo dedicado): R$ ${fmtBR(faixa.valor)}`, cargaFechada:true };
  }

  // Tabela fracionada: primeira faixa cujo limite cobre o peso considerado
  for(const r of faixas){
    if(pesoConsiderado <= r.kg){
      if(r.tipo==='ATE_KG'){
        return { valor: r.valor, modo:`Faixa "até ${fmtKg(r.kg)}": R$ ${fmtBR(r.valor)} (peso considerado: ${fmtKg(pesoConsiderado)})` };
      }
      const valor = pesoConsiderado * r.multiplicador;
      return { valor, modo:`Faixa "até ${fmtKg(r.kg)}": ${fmtKg(pesoConsiderado)} × R$ ${fmtBR(r.multiplicador,4)}/kg = R$ ${fmtBR(valor)}` };
    }
  }
  // Peso acima de todas as faixas "até": usa a regra "Acima de X"
  if(acima){
    const valor = pesoConsiderado * acima.multiplicador;
    return { valor, modo:`Acima de ${fmtKg(acima.kg)}: ${fmtKg(pesoConsiderado)} × R$ ${fmtBR(acima.multiplicador,4)}/kg = R$ ${fmtBR(valor)}` };
  }
  if(faixas.length){
    const ult = faixas[faixas.length-1];
    const valor = ult.tipo==='ATE_KG' ? ult.valor : pesoConsiderado * ult.multiplicador;
    return { valor, modo:`R$ ${fmtBR(valor)} (peso ≤ ${fmtKg(ult.kg)})` };
  }
  return null;
}

// Calcula taxas aplicáveis: combina taxas_gerais (da tabela) + taxas da rota.
// Modelo de base (igual ao da transportadora):
//  base do frete = frete-peso + taxas absolutas (% NF, por documento, por fração)
//  taxas "% sobre valor do frete" (TRT, TDE...) incidem sobre essa base e NÃO compõem entre si
//  ADICIONAL FRETE ("marca de cálculo adicional") incide por último, sobre base + demais % frete
function aplicarTaxas(tab, rota, fretePeso, valorNF, pesoConsiderado, destino){
  const detalhes = [];
  const restritas = [];   // taxas que podem aplicar mas dependem de CNPJ específico do cliente
  const ufDestino = (destino.split('-').pop() || '').trim().toUpperCase();
  const taxasCombo = [...(tab.taxas_gerais||[]), ...(rota.taxas||[])];

  // Taxas de cenário (devolução/reentrega/agendamento) — não entram no frete normal
  const ehCenario = (nome) => nome.includes('DEVOLUC') || nome.includes('REENTREGA') || nome === 'TDA';
  const ehAdicional = (nome) => nome.includes('ADICIONAL');

  const aplicaveis = [];
  for(const t of taxasCombo){
    const nome = (t.nome||'').toUpperCase();
    if(ehCenario(nome)) continue;

    // Restrita SOMENTE por CNPJ (sem cidade nem UF) → "pode incidir, depende do cliente"
    if(t.aplicar_cnpjs_count && !(t.aplicar_cidades && t.aplicar_cidades.length) && !(t.aplicar_ufs && t.aplicar_ufs.length)){
      restritas.push({ _t:t, nome:t.nome, valor:null, motivo:`Aplica apenas a ${t.aplicar_cnpjs_count} CNPJ(s) específico(s)` });
      continue;
    }
    // Filtro por cidade/UF aplicável
    if(t.aplicar_cidades && t.aplicar_cidades.length){
      const cidadeMatch = t.aplicar_cidades.some(c => norm(c) === norm(destino));
      const ufMatch = t.aplicar_ufs && t.aplicar_ufs.includes(ufDestino);
      if(!cidadeMatch && !ufMatch) continue;
    } else if(t.aplicar_ufs && t.aplicar_ufs.length){
      if(!t.aplicar_ufs.includes(ufDestino)) continue;
    }
    aplicaveis.push(t);
  }

  // FASE A — base do frete (frete-peso + taxas absolutas)
  let base = fretePeso || 0;
  const pctFrete = [];
  let adicional = null;
  for(const t of aplicaveis){
    if(t.tipo==='PERCENT_NF'){
      let val = (valorNF||0) * (t.percentual/100);
      if(t.minimo && val < t.minimo) val = t.minimo;
      detalhes.push({nome:t.nome, valor:val, desc:`${fmtBR(t.percentual,4)}% × R$ ${fmtBR(valorNF)}` + (t.minimo && (valorNF*t.percentual/100) < t.minimo ? ` (mínimo R$ ${fmtBR(t.minimo)})` : '')});
      base += val;
    } else if(t.tipo==='VALOR_DOC'){
      const val = t.valor||0;
      detalhes.push({nome:t.nome, valor:val, desc:'R$ por documento'});
      base += val;
    } else if(t.tipo==='POR_FRACAO'){
      const fracoes = Math.ceil((pesoConsiderado||0) / (t.fracao_kg||100));
      const val = fracoes * (t.valor||0);
      detalhes.push({nome:t.nome, valor:val, desc:`${fracoes} × R$ ${fmtBR(t.valor)} (fração ${fmtKg(t.fracao_kg)})`});
      base += val;
    } else if(t.tipo==='PERCENT_FRETE'){
      if(ehAdicional((t.nome||'').toUpperCase())) adicional = t; else pctFrete.push(t);
    }
  }

  // FASE B — "% sobre valor total do frete" sobre a base (não compõem entre si)
  let somaPct = 0;
  for(const t of pctFrete){
    let val = base * (t.percentual/100);
    if(t.minimo && val < t.minimo) val = t.minimo;
    detalhes.push({nome:t.nome, valor:val, desc:`${fmtBR(t.percentual,2)}% × R$ ${fmtBR(base)} (valor total do frete)` + (t.minimo && (base*t.percentual/100) < t.minimo ? ` (mínimo R$ ${fmtBR(t.minimo)})` : '')});
    somaPct += val;
  }

  // FASE C — ADICIONAL FRETE (marca de cálculo adicional): sobre base + demais % frete
  let valAdic = 0;
  if(adicional){
    const baseAd = base + somaPct;
    valAdic = baseAd * (adicional.percentual/100);
    if(adicional.minimo && valAdic < adicional.minimo) valAdic = adicional.minimo;
    detalhes.push({nome:adicional.nome, valor:valAdic, desc:`${fmtBR(adicional.percentual,2)}% × R$ ${fmtBR(baseAd)} (frete + taxas)`});
  }

  const totalTaxas = (base - (fretePeso||0)) + somaPct + valAdic;

  // Valor potencial de cada restrita (agora que a base existe)
  for(const r of restritas){
    const t = r._t;
    if(t.tipo==='PERCENT_NF') r.valor = Math.max((valorNF||0)*(t.percentual/100), t.minimo||0);
    else if(t.tipo==='PERCENT_FRETE') r.valor = Math.max(base*(t.percentual/100), t.minimo||0);
    else if(t.tipo==='VALOR_DOC') r.valor = t.valor||0;
    else if(t.tipo==='POR_FRACAO') r.valor = Math.ceil((pesoConsiderado||0)/(t.fracao_kg||100))*(t.valor||0);
    else r.valor = 0;
  }

  // Cenário "com taxas do cliente": aplica as restritas e RECALCULA o ADICIONAL FRETE
  // (a base do adicional cresce quando TRT/TDE entram)
  let basePlus = base, somaPctPlus = somaPct;
  for(const r of restritas){
    if(r._t.tipo==='PERCENT_FRETE') somaPctPlus += r.valor; else basePlus += r.valor;
  }
  let valAdicPlus = valAdic;
  if(adicional){
    const baseAdPlus = basePlus + somaPctPlus;
    valAdicPlus = baseAdPlus * (adicional.percentual/100);
    if(adicional.minimo && valAdicPlus < adicional.minimo) valAdicPlus = adicional.minimo;
  }
  const totalComRestritas = basePlus + somaPctPlus + valAdicPlus;
  const ajusteAdicional = valAdicPlus - valAdic;

  for(const r of restritas){ delete r._t; }

  return { detalhes, totalTaxas, restritas, totalComRestritas, ajusteAdicional };
}

function simular(origem, destino, pesoKg, m3, valorNF){
  const pesoConsiderado = calcularPesoConsiderado(pesoKg, m3);
  const resultados = [];
  const motivosExclusao = [];

  TABELAS.tabelas.forEach((tab, ti)=>{
    // Encontrar rota matching: origem em rota.origens E destino em rota.destinos
    const rota = (tab.rotas||[]).find(r =>
      (r.origens||[]).some(o => norm(o)===norm(origem)) &&
      (r.destinos||[]).some(d => norm(d)===norm(destino))
    );
    if(!rota){ return; }
    if(!rota.regras || !rota.regras.length){
      motivosExclusao.push(`${tab.nome}: rota sem regras de preço cadastradas`);
      return;
    }
    const fp = calcularFretePeso(rota.regras, pesoConsiderado);
    if(!fp){ motivosExclusao.push(`${tab.nome}: não foi possível calcular o frete-peso`); return; }
    const tx = aplicarTaxas(tab, rota, fp.valor, valorNF, pesoConsiderado, destino);

    resultados.push({
      tabela: tab.nome,
      transportador: tab.transportador,
      modalidade: tab.modalidade,
      validade: tab.inicio_validade,
      status: tab.status,
      tipo_calculo: tab.tipo_calculo,
      tipo_peso: tab.tipo_peso,
      tipo_frete: tab.tipo_frete,
      rota_id: rota.id,
      rota_desc: rota.descricao,
      regras_aplicadas: rota.regras,
      fretePeso: fp.valor,
      fretePesoModo: fp.modo,
      taxasDetalhe: tx.detalhes,
      taxasRestritas: tx.restritas,
      totalTaxas: tx.totalTaxas,
      total: fp.valor + tx.totalTaxas,
      totalComRestritas: tx.totalComRestritas,
      ajusteAdicional: tx.ajusteAdicional,
      pesoConsiderado,
      veiculo: getCapacidadeVeiculo(tab.nome),
      excedeCapacidade: (function(){
        const cap = getCapacidadeVeiculo(tab.nome);
        return !!(cap && pesoConsiderado > cap.kg);
      })()
    });
  });

  return { resultados, motivosExclusao, pesoConsiderado };
}

function fmtBR(v, casas=2){
  if(v===null||v===undefined||isNaN(v)) return '0,00';
  return v.toLocaleString('pt-BR',{minimumFractionDigits:casas, maximumFractionDigits:casas});
}
function fmtKg(v){
  if(v===null||v===undefined||isNaN(v)) return '0 kg';
  if(v<1) return v.toLocaleString('pt-BR',{minimumFractionDigits:3, maximumFractionDigits:3}) + ' kg';
  return v.toLocaleString('pt-BR',{minimumFractionDigits:0, maximumFractionDigits:2}) + ' kg';
}

window.MotorFrete = {
  versao: 'simulador@7b4f6e3',
  norm,
  // simula usando o conjunto de tabelas informado (mesma função simular do simulador)
  simular: function(tabelas, origem, destino, pesoKg, m3, valorNF){
    TABELAS = tabelas;
    try { return simular(origem, destino, pesoKg, m3, valorNF); } finally { TABELAS = null; }
  }
};
})();
