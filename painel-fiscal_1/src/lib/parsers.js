// Parsers em JavaScript (rodam no navegador) para NF-e (XML) e Manifestacao (TXT).

// Ajuste conforme os codigos de evento do seu ERP:
export const EVENTOS = {
  "0": "Ciencia da Operacao",
  "1": "Confirmacao da Operacao",
  "2": "Desconhecimento / Op. nao realizada",
  "3": "Operacao nao Realizada",
};

const num = (v) => (v == null || v === "" ? null : Number(v));

export function parseNFe(xmlString, filename) {
  const doc = new DOMParser().parseFromString(xmlString, "application/xml");
  const g = (el, tag) => {
    const e = el && el.getElementsByTagName(tag)[0];
    return e ? e.textContent : null;
  };
  const inf = doc.getElementsByTagName("infNFe")[0];
  if (!inf) throw new Error("XML nao parece ser uma NF-e");
  const ide = inf.getElementsByTagName("ide")[0];
  const emit = inf.getElementsByTagName("emit")[0];
  const dest = inf.getElementsByTagName("dest")[0];
  const enderEmit = emit && emit.getElementsByTagName("enderEmit")[0];
  const enderDest = dest && dest.getElementsByTagName("enderDest")[0];
  const total = doc.getElementsByTagName("ICMSTot")[0];
  const prot = doc.getElementsByTagName("infProt")[0];
  const adic = inf.getElementsByTagName("infAdic")[0];

  const nota = {
    chave: (inf.getAttribute("Id") || "").replace("NFe", ""),
    numero: g(ide, "nNF"),
    serie: g(ide, "serie"),
    modelo: g(ide, "mod"),
    natureza_operacao: g(ide, "natOp"),
    tipo_operacao: g(ide, "tpNF") === "1" ? "Saida" : "Entrada",
    finalidade: g(ide, "finNFe"),
    data_emissao: g(ide, "dhEmi"),
    uf_emitente: g(enderEmit, "UF"),
    cnpj_emitente: g(emit, "CNPJ"),
    nome_emitente: g(emit, "xNome"),
    ie_emitente: g(emit, "IE"),
    municipio_emitente: g(enderEmit, "xMun"),
    cnpj_destinatario: g(dest, "CNPJ"),
    nome_destinatario: g(dest, "xNome"),
    uf_destinatario: g(enderDest, "UF"),
    municipio_destinatario: g(enderDest, "xMun"),
    valor_produtos: num(g(total, "vProd")),
    valor_total: num(g(total, "vNF")),
    valor_icms: num(g(total, "vICMS")),
    valor_frete: num(g(total, "vFrete")),
    valor_desconto: num(g(total, "vDesc")),
    protocolo: g(prot, "nProt"),
    status_codigo: g(prot, "cStat"),
    status_motivo: g(prot, "xMotivo"),
    data_autorizacao: g(prot, "dhRecbto"),
    info_complementar: g(adic, "infCpl"),
    arquivo_origem: filename,
  };

  const itens = [];
  const dets = inf.getElementsByTagName("det");
  for (let i = 0; i < dets.length; i++) {
    const det = dets[i];
    const prod = det.getElementsByTagName("prod")[0];
    const icmsWrap = det.getElementsByTagName("ICMS")[0];
    const icms = icmsWrap ? icmsWrap.children[0] : null;
    itens.push({
      num_item: parseInt(det.getAttribute("nItem"), 10),
      codigo_produto: g(prod, "cProd"),
      ean: g(prod, "cEAN"),
      descricao: g(prod, "xProd"),
      ncm: g(prod, "NCM"),
      cfop: g(prod, "CFOP"),
      unidade: g(prod, "uCom"),
      quantidade: num(g(prod, "qCom")),
      valor_unitario: num(g(prod, "vUnCom")),
      valor_total: num(g(prod, "vProd")),
      cst_icms: g(icms, "CST"),
      aliquota_icms: num(g(icms, "pICMS")),
      valor_icms_item: num(g(icms, "vICMS")),
    });
  }
  return { nota, itens };
}

export function parseManifesto(text, filename) {
  const linhas = text.split(/\r?\n/).filter((l) => l.trim());
  const primeira = linhas[0] || "";
  const posicional = primeira.length === 60 && primeira[0] === "0";

  if (posicional) {
    const h = linhas[0];
    const d = h.slice(13, 21); // ddmmyyyy
    const data_iso = d.length === 8 ? `${d.slice(4, 8)}-${d.slice(2, 4)}-${d.slice(0, 2)}` : null;
    const cnpj_empresa = h.slice(25, 39);
    const serie = (filename.replace(/\.txt$/i, "").split("_").pop()) || null;
    return linhas.slice(1)
      .filter((l) => l.length >= 60 && l.startsWith("001"))
      .map((l) => ({
        numero_nf: String(parseInt(l.slice(3, 12), 10)),
        campo_aux: l.slice(12, 16),
        chave_nfe: l.slice(16, 60),
        sequencia: l.slice(3, 12),
        cnpj_empresa, serie, data_arquivo: data_iso,
        codigo_evento: null, evento: null, arquivo_origem: filename,
      }));
  }

  // formato antigo (com espacos)
  let header = {};
  const regs = [];
  for (const l of linhas) {
    const c = l.trim().split(/\s+/);
    if (!c[0]) continue;
    if (c[0] === "1") header = { data: c[2], lote: c[3], serie: c[4] };
    else if (c[0] === "2" && c.length >= 3)
      regs.push({ sequencia: c[1], codigo_evento: c[2], evento: EVENTOS[c[2]] || `Codigo ${c[2]}` });
  }
  const d = header.data;
  const data_iso = d && d.length === 8 ? `${d.slice(4, 8)}-${d.slice(2, 4)}-${d.slice(0, 2)}` : null;
  return regs.map((r) => ({ ...r, lote: header.lote, serie: header.serie, data_arquivo: data_iso, arquivo_origem: filename }));
}
