let SB_URL = '';
let SB_KEY = '';

export async function initSB() {
  const r = await fetch('/config');
  if (!r.ok) throw new Error('No se pudo obtener /config del backend (status ' + r.status + ')');
  const cfg = await r.json();
  SB_URL = (cfg.supabase_url || '').replace(/\/$/, '');
  SB_KEY = cfg.supabase_anon_key || '';
  if (!SB_URL || !SB_KEY) {
    throw new Error('Faltan SUPABASE_URL o SUPABASE_ANON_KEY en las env vars de Railway');
  }
}

function headers() {
  return {
    'apikey': SB_KEY,
    'Authorization': `Bearer ${SB_KEY}`,
    'Content-Type': 'application/json',
    'Prefer': 'return=representation'
  };
}

// GET con paginación obligatoria (Supabase corta en 1000 filas sin avisar)
export async function sbGet(table, query = '') {
  if (!SB_URL) throw new Error('SB no inicializado');
  const all = [];
  const PAGE = 1000;
  let from = 0;
  while (true) {
    const h = { ...headers(), 'Range-Unit': 'items', 'Range': `${from}-${from + PAGE - 1}` };
    const r = await fetch(`${SB_URL}/rest/v1/${table}?${query}`, { headers: h });
    if (!r.ok) {
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
  const h = { ...headers(), 'Prefer': 'count=exact', 'Range': '0-0' };
  const r = await fetch(`${SB_URL}/rest/v1/${table}?${query}`, { headers: h });
  if (!r.ok) {
    const txt = await r.text().catch(() => '');
    throw new Error(`SB COUNT ${table}: ${r.status} ${r.statusText} ${txt}`);
  }
  const range = r.headers.get('content-range');
  return range ? parseInt(range.split('/')[1]) || 0 : 0;
}

export async function sbPost(table, data) {
  const r = await fetch(`${SB_URL}/rest/v1/${table}`, {
    method: 'POST',
    headers: headers(),
    body: JSON.stringify(data)
  });
  if (!r.ok) throw new Error(`SB POST ${table}: ${r.status} ${r.statusText}`);
  return r.json();
}

export async function sbPatch(table, query, data) {
  const r = await fetch(`${SB_URL}/rest/v1/${table}?${query}`, {
    method: 'PATCH',
    headers: headers(),
    body: JSON.stringify(data)
  });
  if (!r.ok) throw new Error(`SB PATCH ${table}: ${r.status} ${r.statusText}`);
  return r.json();
}

export async function sbDelete(table, query) {
  const r = await fetch(`${SB_URL}/rest/v1/${table}?${query}`, {
    method: 'DELETE',
    headers: headers()
  });
  if (!r.ok) throw new Error(`SB DELETE ${table}: ${r.status} ${r.statusText}`);
}
