// core/sb.js — acceso a datos (Supabase REST) + autenticación (Supabase Auth).
// Modelo de seguridad: el front habla con Supabase directo, pero con el TOKEN del usuario
// logueado (no con la anon key). RLS en la base exige ese token (usuario logueado = todo,
// anon = nada). El backend valida el mismo token. Ver ADARA-AUTH.md.

const DOMINIO = '@adara.local';            // email interno, invisible para el usuario
const LS_KEY  = 'adara_session';

let SB_URL = '';
let SB_KEY = '';                            // anon key (solo el handshake de auth; sin sesión no lee nada)
let SESSION = null;                         // { access_token, refresh_token, expires_at(ms), user:{id,email,usuario,must_change} }

// ── Sesión en localStorage ──────────────────────────────────────────
function loadSession() {
  try { SESSION = JSON.parse(localStorage.getItem(LS_KEY) || 'null'); } catch { SESSION = null; }
}
function saveSession(s) { SESSION = s; localStorage.setItem(LS_KEY, JSON.stringify(s)); }
function clearSession() { SESSION = null; localStorage.removeItem(LS_KEY); }

function parseUser(u) {
  const meta = (u && u.user_metadata) || {};
  return {
    id: u && u.id,
    email: u && u.email,
    usuario: meta.usuario || (u && u.email ? u.email.replace(DOMINIO, '') : ''),
    must_change: meta.must_change_password === true
  };
}
function setSessionFromToken(data) {
  saveSession({
    access_token: data.access_token,
    refresh_token: data.refresh_token,
    expires_at: Date.now() + (Number(data.expires_in || 3600) * 1000),
    user: parseUser(data.user)
  });
}

// ── Init: trae /config, carga sesión y parchea fetch para el backend ─
export async function initSB() {
  patchFetch();                             // antes de cualquier llamada de datos
  const r = await fetch('/config');
  if (!r.ok) throw new Error('No se pudo obtener /config del backend (status ' + r.status + ')');
  const cfg = await r.json();
  SB_URL = (cfg.supabase_url || '').replace(/\/$/, '');
  SB_KEY = cfg.supabase_anon_key || '';
  if (!SB_URL || !SB_KEY) throw new Error('Faltan SUPABASE_URL o SUPABASE_ANON_KEY en las env vars de Railway');
  loadSession();
}

// ── Token válido (refresca solo si está por vencer) ─────────────────
async function getValidToken() {
  if (!SESSION) return null;
  if (Date.now() < SESSION.expires_at - 120000) return SESSION.access_token;   // > 2 min de margen
  if (!SESSION.refresh_token) { authExpired(); return null; }
  try {
    const r = await fetch(`${SB_URL}/auth/v1/token?grant_type=refresh_token`, {
      method: 'POST',
      headers: { 'apikey': SB_KEY, 'Content-Type': 'application/json' },
      body: JSON.stringify({ refresh_token: SESSION.refresh_token })
    });
    if (!r.ok) { authExpired(); return null; }
    const data = await r.json();
    setSessionFromToken(data);
    return SESSION.access_token;
  } catch { authExpired(); return null; }
}

function authExpired() {
  clearSession();
  window.dispatchEvent(new CustomEvent('adara-auth-expired'));
}

// ── Auth API (usada por la pantalla de login) ───────────────────────
export function hasSession() { return !!(SESSION && SESSION.refresh_token); }
export function sessionUser() { return SESSION ? SESSION.user : null; }

export async function login(usuario, password) {
  const email = String(usuario || '').trim().toLowerCase() + DOMINIO;
  try {
    const r = await fetch(`${SB_URL}/auth/v1/token?grant_type=password`, {
      method: 'POST',
      headers: { 'apikey': SB_KEY, 'Content-Type': 'application/json' },
      body: JSON.stringify({ email, password })
    });
    const data = await r.json();
    if (!r.ok) return { ok: false, error: data.error_description || data.msg || 'Usuario o contraseña incorrectos' };
    setSessionFromToken(data);
    return { ok: true, mustChange: SESSION.user.must_change };
  } catch (e) { return { ok: false, error: e.message }; }
}

export async function changePassword(nueva) {
  const tok = await getValidToken();
  if (!tok) return { ok: false, error: 'Sesión vencida, ingresá de nuevo' };
  try {
    const r = await fetch(`${SB_URL}/auth/v1/user`, {
      method: 'PUT',
      headers: { 'apikey': SB_KEY, 'Authorization': 'Bearer ' + tok, 'Content-Type': 'application/json' },
      body: JSON.stringify({ password: nueva, data: { must_change_password: false } })
    });
    const data = await r.json();
    if (!r.ok) return { ok: false, error: data.error_description || data.msg || 'No se pudo cambiar la contraseña' };
    if (SESSION) { SESSION.user.must_change = false; saveSession(SESSION); }
    return { ok: true };
  } catch (e) { return { ok: false, error: e.message }; }
}

export async function logout() {
  const tok = SESSION && SESSION.access_token;
  if (tok) {
    try { await fetch(`${SB_URL}/auth/v1/logout`, { method: 'POST', headers: { 'apikey': SB_KEY, 'Authorization': 'Bearer ' + tok } }); } catch {}
  }
  clearSession();
}

// ── Parche de fetch: agrega el token del usuario a las llamadas al backend ──
// (las llamadas a Supabase usan URL absoluta y ya ponen sus headers; el parche solo
//  toca rutas relativas '/...' que van al backend, salvo las públicas).
let _fetchPatched = false;
function patchFetch() {
  if (_fetchPatched) return;
  _fetchPatched = true;
  const _fetch = window.fetch.bind(window);
  const PUB = /^\/(config|health|ml\/auth|ml\/callback)/;
  window.fetch = async (input, init = {}) => {
    const url = typeof input === 'string' ? input : (input && input.url) || '';
    const esBackend = url.startsWith('/') && !url.startsWith('//');
    if (esBackend && !PUB.test(url)) {
      const tok = await getValidToken();
      if (tok) init = { ...init, headers: { ...(init.headers || {}), 'Authorization': 'Bearer ' + tok } };
    }
    const res = await _fetch(input, init);
    if (esBackend && res.status === 401) authExpired();
    return res;
  };
}

// ── Headers para las llamadas de datos a Supabase ───────────────────
async function authHeaders(extra = {}) {
  const tok = await getValidToken();
  return {
    'apikey': SB_KEY,
    'Authorization': 'Bearer ' + (tok || SB_KEY),
    'Content-Type': 'application/json',
    'Prefer': 'return=representation',
    ...extra
  };
}

// GET con paginación obligatoria (Supabase corta en 1000 filas sin avisar)
export async function sbGet(table, query = '') {
  if (!SB_URL) throw new Error('SB no inicializado');
  const all = [];
  const PAGE = 1000;
  let from = 0;
  while (true) {
    const h = await authHeaders({ 'Range-Unit': 'items', 'Range': `${from}-${from + PAGE - 1}` });
    const r = await fetch(`${SB_URL}/rest/v1/${table}?${query}`, { headers: h });
    if (!r.ok) {
      if (r.status === 401) authExpired();
      const txt = await r.text().catch(() => '');
      throw new Error(`SB GET ${table}: ${r.status} ${r.statusText} ${txt}`);
    }
    const batch = await r.json();
    all.push(...batch);
    if (batch.length < PAGE) break;
    from += PAGE;
  }
  return all;
}

// COUNT puro (no descarga filas)
export async function sbCount(table, query = '') {
  if (!SB_URL) throw new Error('SB no inicializado');
  const h = await authHeaders({ 'Prefer': 'count=exact', 'Range': '0-0' });
  const r = await fetch(`${SB_URL}/rest/v1/${table}?${query}`, { headers: h });
  if (!r.ok) {
    if (r.status === 401) authExpired();
    const txt = await r.text().catch(() => '');
    throw new Error(`SB COUNT ${table}: ${r.status} ${r.statusText} ${txt}`);
  }
  const range = r.headers.get('content-range');
  return range ? parseInt(range.split('/')[1]) || 0 : 0;
}

export async function sbPost(table, data) {
  const r = await fetch(`${SB_URL}/rest/v1/${table}`, { method: 'POST', headers: await authHeaders(), body: JSON.stringify(data) });
  if (!r.ok) { if (r.status === 401) authExpired(); throw new Error(`SB POST ${table}: ${r.status} ${r.statusText}`); }
  return r.json();
}

export async function sbPatch(table, query, data) {
  const r = await fetch(`${SB_URL}/rest/v1/${table}?${query}`, { method: 'PATCH', headers: await authHeaders(), body: JSON.stringify(data) });
  if (!r.ok) { if (r.status === 401) authExpired(); throw new Error(`SB PATCH ${table}: ${r.status} ${r.statusText}`); }
  return r.json();
}

export async function sbDelete(table, query) {
  const r = await fetch(`${SB_URL}/rest/v1/${table}?${query}`, { method: 'DELETE', headers: await authHeaders() });
  if (!r.ok) { if (r.status === 401) authExpired(); throw new Error(`SB DELETE ${table}: ${r.status} ${r.statusText}`); }
}
