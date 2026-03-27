// ============================================================
// TORRE DE CONTROLE — UTILS CORE
// Funções utilitárias compartilhadas
// ============================================================

const fmtM  = v => 'R$ ' + (v / 1e6).toFixed(1) + 'M';
const fmtMd = v => 'R$ ' + (v / 1e6).toFixed(2) + 'M';
const fmtK  = v => v >= 1000 ? (v/1000).toFixed(1)+'K' : v.toLocaleString('pt-BR');
const fmtP  = (a, b) => b ? ((a / b) * 100).toFixed(1) + '%' : '—';
const fmtPct= v => v.toFixed(1) + '%';

const NOME_MES = ['','Jan','Fev','Mar','Abr','Mai','Jun','Jul','Ago','Set','Out','Nov','Dez'];

const REGIOES_MAP = {
  'AC':'Norte','AM':'Norte','AP':'Norte','PA':'Norte','RO':'Norte','RR':'Norte','TO':'Norte',
  'AL':'Nordeste','BA':'Nordeste','CE':'Nordeste','MA':'Nordeste','PB':'Nordeste',
  'PE':'Nordeste','PI':'Nordeste','RN':'Nordeste','SE':'Nordeste',
  'DF':'Centro-Oeste','GO':'Centro-Oeste','MS':'Centro-Oeste','MT':'Centro-Oeste',
  'ES':'Sudeste','MG':'Sudeste','RJ':'Sudeste','SP':'Sudeste',
  'PR':'Sul','RS':'Sul','SC':'Sul'
};

const REG_CORES = {
  'Sudeste':'#185FA5','Nordeste':'#EF9F27',
  'Centro-Oeste':'#1D9E75','Norte':'#5DCAA5','Sul':'#7F77DD'
};

const MES_PALETTE = ['#378ADD','#185FA5','#0C447C','#042C53','#85B7EB',
                     '#5AA87B','#3B6D11','#EF9F27','#E24B4A','#7F77DD','#5DCAA5','#854F0B'];

function parseDate(v) {
  if (!v) return null;
  if (v instanceof Date) return isNaN(v) ? null : v;
  if (typeof v === 'number') return new Date(Math.round((v - 25569) * 86400 * 1000));
  const d = new Date(v); return isNaN(d) ? null : d;
}

function colFind(r, ...names) {
  for (const n of names) {
    if (r[n] !== undefined && r[n] !== null) return r[n];
  }
  const keys = Object.keys(r);
  for (const n of names) {
    const base = n.replace(/[^a-zA-Z0-9 ]/g, '').toLowerCase().substring(0, 8);
    const found = keys.find(k => k.replace(/[^a-zA-Z0-9 ]/g, '').toLowerCase().includes(base));
    if (found && r[found] !== undefined) return r[found];
  }
  return null;
}

function readExcel(file) {
  return new Promise((res, rej) => {
    const reader = new FileReader();
    reader.onload = e => {
      try {
        const wb = XLSX.read(e.target.result, { type: 'array', cellDates: true });
        const ws = wb.Sheets[wb.SheetNames[0]];
        const data = XLSX.utils.sheet_to_json(ws, { defval: null });
        res(data);
      } catch(err) { rej(err); }
    };
    reader.onerror = rej;
    reader.readAsArrayBuffer(file);
  });
}

// Log helper
function logLine(boxId, msg, type = '') {
  const box = document.getElementById(boxId);
  if (!box) return;
  box.style.display = 'block';
  const line = document.createElement('div');
  line.className = 'log-line ' + type;
  line.textContent = '[' + new Date().toLocaleTimeString('pt-BR') + '] ' + msg;
  box.appendChild(line);
  box.scrollTop = box.scrollHeight;
}
