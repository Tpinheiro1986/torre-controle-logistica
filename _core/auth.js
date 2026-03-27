// ============================================================
// TORRE DE CONTROLE — AUTH CORE (blindado)
// Compartilhado entre todos os módulos
// NÃO EDITAR — qualquer mudança reflete em todo o projeto
// ============================================================

const SUPABASE_URL  = 'https://ennsbpibfnuwlvtodukg.supabase.co';
const SUPABASE_ANON = 'sb_publishable_ExShUMyhsoGRab_RdySuZg_1uqONyI5';
const ADMIN_EMAIL   = 'thiago_balao@yahoo.com.br';

const { createClient } = supabase;
const sb = createClient(SUPABASE_URL, SUPABASE_ANON, {
  auth: {
    persistSession: true,
    autoRefreshToken: true,
    detectSessionInUrl: true,
    flowType: 'implicit',
    storageKey: 'torre-controle-auth'
  }
});

let currentUser = null;

async function authInit(onSuccess) {
  try {
    if (window.location.hash && window.location.hash.includes('access_token')) {
      await new Promise(r => setTimeout(r, 800));
      window.history.replaceState(null, '', window.location.pathname);
    }
    const { data: { session }, error } = await sb.auth.getSession();
    if (error || !session) { authShowLogin(); return; }
    authShowApp(session.user, onSuccess);
  } catch(e) { authShowLogin(); }
}

async function authDoLogin() {
  const email = document.getElementById('l-email').value.trim();
  const pass  = document.getElementById('l-pass').value;
  const btn   = document.getElementById('btn-login');
  const err   = document.getElementById('login-err');
  err.style.display = 'none';
  if (!email || !pass) { authShowErr(err, 'Preencha e-mail e senha.'); return; }
  btn.disabled = true; btn.textContent = 'Entrando…';
  try {
    const { data, error } = await sb.auth.signInWithPassword({ email, password: pass });
    if (error) {
      let msg = error.message;
      if (msg.includes('Invalid login') || msg.includes('invalid_credentials')) msg = 'E-mail ou senha incorretos.';
      else if (msg.includes('Email not confirmed')) msg = 'Confirme seu e-mail antes de entrar.';
      else if (msg.includes('fetch') || msg.includes('NetworkError')) msg = 'Erro de conexão. Verifique sua internet.';
      authShowErr(err, msg);
      btn.disabled = false; btn.textContent = 'Entrar';
    } else {
      authShowApp(data.user, window._authOnSuccess);
    }
  } catch(e) {
    authShowErr(err, 'Erro: ' + e.message);
    btn.disabled = false; btn.textContent = 'Entrar';
  }
}

async function authDoLogout() {
  await sb.auth.signOut();
  currentUser = null;
  authShowLogin();
}

function authShowLogin() {
  document.getElementById('screen-login').style.display = 'flex';
  document.getElementById('screen-app').style.display = 'none';
}

function authShowApp(user, onSuccess) {
  currentUser = user;
  document.getElementById('screen-login').style.display = 'none';
  document.getElementById('screen-app').style.display = 'block';
  const av = document.getElementById('user-av');
  const em = document.getElementById('user-em');
  if (av) av.textContent = user.email.slice(0, 2).toUpperCase();
  if (em) em.textContent = user.email;
  const isAdmin = user.email === ADMIN_EMAIL;
  const uploadTab = document.getElementById('tab-upload');
  const adminBadge = document.getElementById('admin-badge');
  if (isAdmin && uploadTab) uploadTab.style.display = 'block';
  if (isAdmin && adminBadge) adminBadge.style.display = 'inline-block';
  if (onSuccess) onSuccess(user, isAdmin);
}

function authShowErr(el, msg) { el.textContent = msg; el.style.display = 'block'; }

sb.auth.onAuthStateChange((event, session) => {
  if (event === 'SIGNED_IN' && session && !currentUser) authShowApp(session.user, window._authOnSuccess);
  else if (event === 'SIGNED_OUT') authShowLogin();
});

// Storage helpers
async function storageUpload(path, payload) {
  const json = JSON.stringify(payload);
  const blob = new Blob([json], { type: 'application/json' });
  const { error } = await sb.storage.from('dashboards').upload(path, blob, {
    upsert: true, contentType: 'application/json', cacheControl: '0'
  });
  return error;
}

async function storageDownload(path) {
  const { data, error } = await sb.storage.from('dashboards').download(path);
  if (error || !data) return null;
  const text = await data.text();
  return JSON.parse(text);
}
