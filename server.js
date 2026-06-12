/**
 * ADARA APP — Servidor Railway (versión simplificada)
 * ────────────────────────────────────────────────────
 * ✓ Mercado Libre   → trae ventas automáticamente
 * ✓ Mercado Pago    → parsea el extracto que bajás vos
 * ✓ Tango Factura   → LEE las facturas que ya emitiste
 * ✓ Supervielle     → parsea extracto bancario XLSX
 * ✓ Supabase        → guarda todo
 */

const express = require('express');
const cors    = require('cors');
const cron    = require('node-cron');
const fetch   = require('node-fetch');
const multer  = require('multer');
const XLSX    = require('xlsx');
const forge   = require('node-forge');
const crypto  = require('crypto');

const app    = express();
const upload = multer({ storage: multer.memoryStorage() });

app.use(cors());
app.use(express.json({ limit: '20mb' }));
// Frontend desde /public. 'no-cache' = el navegador puede guardar el archivo
// pero SIEMPRE revalida con el server antes de usarlo (ETag): si no cambió → 304
// instantáneo; si cambió → baja la versión nueva sola. Evita servir JS/HTML viejos.
app.use(express.static('public', {
  etag: true,
  lastModified: true,
  setHeaders: (res) => res.setHeader('Cache-Control', 'no-cache')
}));

// ─── Variables de entorno (se cargan desde Railway) ─────────────────
const {
  SUPABASE_URL,
  SUPABASE_KEY,
  SUPABASE_ANON_KEY,
  SUPABASE_JWT_SECRET,
  ML_CLIENT_ID,
  ML_CLIENT_SECRET,
  ML_REDIRECT_URI,
  TF_APP_KEY,
  TF_USERNAME,
  TF_PASSWORD,
  TF_USER_ID,
  PORT = 3000
} = process.env;

// Candado de autenticacion: todo endpoint exige el token del usuario logueado (Supabase Auth,
// HS256 con el JWT secret del proyecto). Publicos: estaticos (ya servidos), /health, /config y
// el OAuth de ML. Si SUPABASE_JWT_SECRET no esta cargado, NO bloquea (transicion); el candado
// real es RLS. Ver ADARA-AUTH.md.
function b64urlToBuf(x) { return Buffer.from(String(x).replace(/-/g, '+').replace(/_/g, '/'), 'base64'); }
function verifySupabaseJWT(token) {
  if (!SUPABASE_JWT_SECRET) return null;
  const parts = String(token).split('.');
  if (parts.length !== 3) return null;
  const [h, p, sig] = parts;
  let header, payload;
  try { header = JSON.parse(b64urlToBuf(h).toString('utf8')); payload = JSON.parse(b64urlToBuf(p).toString('utf8')); } catch { return null; }
  if (header.alg !== 'HS256') return null;
  const expected = crypto.createHmac('sha256', SUPABASE_JWT_SECRET).update(h + '.' + p).digest();
  const got = b64urlToBuf(sig);
  if (expected.length !== got.length || !crypto.timingSafeEqual(expected, got)) return null;
  if (payload.exp && Date.now() / 1000 > payload.exp) return null;
  return payload;
}
const AUTH_PUBLIC = [/^\/health/, /^\/config/, /^\/ml\/auth/, /^\/ml\/callback/];
app.use((req, res, next) => {
  if (req.method === 'OPTIONS') return next();
  if (AUTH_PUBLIC.some(re => re.test(req.path))) return next();
  if (!SUPABASE_JWT_SECRET) {
    if (!global.__warnedNoJwt) { console.warn('SUPABASE_JWT_SECRET sin configurar: candado del backend INACTIVO (cargala en Railway).'); global.__warnedNoJwt = true; }
    return next();
  }
  const authH = req.headers.authorization || '';
  const tok = authH.startsWith('Bearer ') ? authH.slice(7) : '';
  const payload = tok && verifySupabaseJWT(tok);
  if (!payload || payload.role !== 'authenticated') return res.status(401).json({ error: 'No autorizado' });
  req.user = { id: payload.sub, email: payload.email };
  next();
});

// ─── Helper Supabase ─────────────────────────────────────────────────
const SB_H = {
  'apikey': SUPABASE_KEY,
  'Authorization': `Bearer ${SUPABASE_KEY}`,
  'Content-Type': 'application/json',
  'Prefer': 'resolution=merge-duplicates,return=representation'
};

async function sb(method, table, body, query = '') {
  const res = await fetch(`${SUPABASE_URL}/rest/v1/${table}${query ? '?' + query : ''}`, {
    method,
    headers: SB_H,
    body: body ? JSON.stringify(body) : undefined
  });
  if (!res.ok) throw new Error(`SB ${method} ${table}: ${await res.text()}`);
  return method === 'DELETE' ? null : res.json();
}

const sbGet    = (t, q)    => sb('GET',   t, null, q);
const sbUpsert = (t, body, onConflict) => {
  const q = onConflict ? `on_conflict=${onConflict}` : '';
  return sb('POST', t, body, q);
};
const sbPatch  = (t, q, b) => sb('PATCH', t, b, q);
// Llamada a funciones RPC (Postgres) vía PostgREST: POST /rest/v1/rpc/<fn> con los params en el body.
const sbRpc    = (fn, params) => sb('POST', `rpc/${fn}`, params);

// ─── Token Mercado Libre (se persiste en Supabase) ──────────────────
let ML = { access: null, refresh: null, expires: 0 };

async function loadMLToken() {
  try {
    const r = await sbGet('workspace_config', 'select=ml_access_token,ml_refresh_token,ml_token_expires&limit=1');
    if (r?.[0]?.ml_access_token) {
      ML = { access: r[0].ml_access_token, refresh: r[0].ml_refresh_token, expires: r[0].ml_token_expires || 0 };
      console.log('✓ Token ML cargado desde Supabase');
    }
  } catch (e) { console.warn('loadMLToken:', e.message); }
}

async function saveMLToken(t) {
  ML = t;
  await sbPatch('workspace_config', 'id=not.is.null', {
    ml_access_token: t.access, ml_refresh_token: t.refresh, ml_token_expires: t.expires
  }).catch(() => {});
}

let _mlRefreshing = null;
async function refreshML() {
  if (!ML.refresh) return;
  // Single-flight: un solo refresh a la vez. El sync dispara hasta 10 llamadas
  // en paralelo; sin esto, todas refrescan con el MISMO refresh_token (que en ML
  // es de un solo uso) y se invalidan entre sí → "ML se desconecta solo".
  if (_mlRefreshing) return _mlRefreshing;
  _mlRefreshing = (async () => {
    try {
      const r = await fetch('https://api.mercadolibre.com/oauth/token', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ grant_type: 'refresh_token', client_id: ML_CLIENT_ID, client_secret: ML_CLIENT_SECRET, refresh_token: ML.refresh })
      });
      const data = await r.json();
      if (data.access_token) {
        // ML rota el refresh_token; si por algún motivo no manda uno nuevo, conservamos el actual.
        await saveMLToken({ access: data.access_token, refresh: data.refresh_token || ML.refresh, expires: Date.now() + data.expires_in * 1000 });
        console.log('✓ ML token refrescado');
      } else {
        // No pisamos el token con null: puede ser un error transitorio y se reintenta
        // en la próxima llamada. Si el refresh_token está realmente revocado, hay que
        // reconectar ML desde la app (/ml/auth).
        console.error('✗ ML refresh falló (no renovó):', JSON.stringify(data));
      }
    } catch (e) {
      console.error('✗ ML refresh error de red:', e.message);
    }
  })().finally(() => { _mlRefreshing = null; });
  return _mlRefreshing;
}

async function mlGet(path) {
  if (Date.now() > ML.expires - 60000) await refreshML();
  let r = await fetch(`https://api.mercadolibre.com${path}`, {
    headers: { 'Authorization': `Bearer ${ML.access}` }
  });
  // Si el token fue rechazado (vencido/rotado), refrescamos UNA vez y reintentamos.
  if (r.status === 401) {
    await refreshML();
    r = await fetch(`https://api.mercadolibre.com${path}`, {
      headers: { 'Authorization': `Bearer ${ML.access}` }
    });
  }
  if (!r.ok) throw new Error(`ML ${path}: ${r.status}`);
  return r.json();
}

// ─── Token Tango Factura (se renueva cada 18 min) ───────────────────
let TF_TOKEN = null, TF_EXP = 0;

async function getTFToken() {
  if (TF_TOKEN && Date.now() < TF_EXP) return TF_TOKEN;
  if (!TF_APP_KEY) throw new Error('Tango Factura no configurado');
  const r = await fetch('https://www.tangofactura.com/Services/Autorizacion/GetToken', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ ApplicationPublicKey: TF_APP_KEY, UserIdentifier: TF_USER_ID, Username: TF_USERNAME, Password: TF_PASSWORD })
  });
  const data = await r.json();
  if (!data?.Data?.Token) throw new Error('Tango auth falló: ' + JSON.stringify(data?.Error || data));
  TF_TOKEN = data.Data.Token;
  TF_EXP   = Date.now() + 18 * 60 * 1000;
  return TF_TOKEN;
}

async function tfPost(endpoint, body) {
  const token = await getTFToken();
  const r = await fetch(`https://www.tangofactura.com/Services/Facturacion/${endpoint}`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ ...body, ApplicationPublicKey: TF_APP_KEY, UserIdentifier: TF_USER_ID, Token: token })
  });
  const data = await r.json();
  if (data.CodigoError && data.CodigoError !== 0) {
    throw new Error(`TF ${endpoint}: ${(data.Error || []).map(e => e.Mensaje).join(' | ')}`);
  }
  return data;
}

// ════════════════════════════════════════════════════════════════════
// RUTAS
// ════════════════════════════════════════════════════════════════════

// ── Salud del servidor ───────────────────────────────────────────────
// Config para el frontend: le pasa al index.html las credenciales públicas
// de Supabase (anon key) leyéndolas de las env vars de Railway.
app.get('/config', (_, res) => res.json({
  supabase_url: SUPABASE_URL,
  supabase_anon_key: SUPABASE_ANON_KEY || SUPABASE_KEY
}));

app.get('/health', async (_, res) => {
  const c = { supabase: false, ml_token: false, tango: false };
  try { await sbGet('lineas_negocio', 'limit=1'); c.supabase = true; } catch (_) {}
  c.ml_token = !!ML.access && Date.now() < ML.expires;
  try { if (TF_APP_KEY) { await getTFToken(); c.tango = true; } } catch (_) {}
  res.json({ ok: Object.values(c).every(Boolean), checks: c });
});


// ── DEBUG — Inspección directa de payment via MP API (mismo path que sync) ──
// GET /debug/payment/:paymentId
app.get('/debug/payment/:paymentId', async (req, res) => {
  try {
    const r = await fetch(`https://api.mercadopago.com/v1/payments/${req.params.paymentId}`, {
      headers: { 'Authorization': 'Bearer ' + ML.access }
    });
    if (!r.ok) return res.status(r.status).json({ error: `MP ${r.status}` });
    const data = await r.json();
    const charges = (data.charges_details || []).map(ch => ({
      type: ch.type, name: ch.name,
      original_amount: ch.amounts?.original,
    }));
    res.json({
      payment_id:          data.id,
      installments:        data.installments,
      transaction_amount:  data.transaction_amount,
      net_received_amount: data.net_received_amount,
      shipping_amount:     data.shipping_amount,
      charges_details:     charges,
      _analisis: {
        financing: charges.filter(c => c.type==='fee' && ['financing','interest'].some(n=>c.name?.includes(n))).reduce((s,c)=>s+(c.original_amount||0),0),
        add_on:    charges.filter(c => c.type==='fee' && c.name?.includes('add_on')).reduce((s,c)=>s+(c.original_amount||0),0),
        fee:       charges.filter(c => c.type==='fee' && !['financing','interest','add_on'].some(n=>c.name?.includes(n))).reduce((s,c)=>s+(c.original_amount||0),0),
        tax:       charges.filter(c => c.type==='tax').reduce((s,c)=>s+(c.original_amount||0),0),
      }
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── DEBUG — Inspección de charges_details de un payment ─────────────
// GET /debug/charges/:paymentId
// Retorna: amounts, net, installments y charges_details crudos de ML
// Útil para distinguir financiero real (financing/interest) vs informativo (add_on)
app.get('/debug/charges/:paymentId', async (req, res) => {
  try {
    const data = await mlGet(`/v1/payments/${req.params.paymentId}`);
    const charges = (data.charges_details || []).map(ch => ({
      type: ch.type,
      name: ch.name,
      original_amount: ch.amounts?.original,
      payer: ch.client_id,
    }));
    res.json({
      payment_id:          data.id,
      order_id:            data.order?.id,
      status:              data.status,
      installments:        data.installments,
      transaction_amount:  data.transaction_amount,
      net_received_amount: data.net_received_amount,
      shipping_amount:     data.shipping_amount,
      charges_details:     charges,
      // Cálculo manual para comparar con por_cobrar de ADARA
      _analisis: {
        fee:        charges.filter(c => c.type === 'fee' && !['financing','interest','add_on'].some(n => c.name?.includes(n))).reduce((s, c) => s + (c.original_amount || 0), 0),
        financing:  charges.filter(c => c.type === 'fee' && ['financing','interest'].some(n => c.name?.includes(n))).reduce((s, c) => s + (c.original_amount || 0), 0),
        add_on:     charges.filter(c => c.type === 'fee' && c.name?.includes('add_on')).reduce((s, c) => s + (c.original_amount || 0), 0),
        tax:        charges.filter(c => c.type === 'tax').reduce((s, c) => s + (c.original_amount || 0), 0),
        shipping:   charges.filter(c => c.type === 'shipping').reduce((s, c) => s + (c.original_amount || 0), 0),
      }
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});


// ── DEBUG — Inspección de order + payments con charges_details ───────
// GET /debug/order/:orderId
// Llama a /orders/:id para obtener los payments y luego a cada payment
// para ver charges_details, installments y net_received_amount real.
app.get('/debug/order/:orderId', async (req, res) => {
  try {
    const order = await mlGet(`/orders/${req.params.orderId}`);
    const payments = order.payments || [];
    const result = [];
    for (const p of payments) {
      let payDetail = null;
      try {
        payDetail = await mlGet(`/payments/${p.id}`);
      } catch(e) {
        payDetail = { error: e.message };
      }
      const charges = (payDetail.charges_details || []).map(ch => ({
        type: ch.type,
        name: ch.name,
        original_amount: ch.amounts?.original,
      }));
      result.push({
        payment_id:          p.id,
        status:              p.status,
        installments:        payDetail.installments,
        transaction_amount:  p.total_paid_amount || payDetail.transaction_amount,
        net_received_amount: payDetail.net_received_amount,
        shipping_amount:     payDetail.shipping_amount,
        charges_details:     charges,
        _analisis: {
          fee:       charges.filter(c => c.type==='fee' && !['financing','interest','add_on'].some(n=>c.name?.includes(n))).reduce((s,c)=>s+(c.original_amount||0),0),
          financing: charges.filter(c => c.type==='fee' && ['financing','interest'].some(n=>c.name?.includes(n))).reduce((s,c)=>s+(c.original_amount||0),0),
          add_on:    charges.filter(c => c.type==='fee' && c.name?.includes('add_on')).reduce((s,c)=>s+(c.original_amount||0),0),
          tax:       charges.filter(c => c.type==='tax').reduce((s,c)=>s+(c.original_amount||0),0),
          shipping:  charges.filter(c => c.type==='shipping').reduce((s,c)=>s+(c.original_amount||0),0),
        }
      });
    }
    res.json({ order_id: order.id, status: order.status, total_amount: order.total_amount, payments: result });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Debug: testear Claims search por resource_id
app.get('/debug/claim/:orderId', async (req, res) => {
  try {
    if (!ML.access) return res.status(401).json({ error: 'ML no autenticado' });
    const h = { 'Authorization': 'Bearer ' + ML.access };
    const orderId = req.params.orderId;
    
    // Intentar varias búsquedas
    const results = {};
    
    // 1. Search by resource_id
    const r1 = await fetch(`https://api.mercadolibre.com/post-purchase/v1/claims/search?resource_id=${orderId}&limit=5`, { headers: h });
    results.by_resource_id = await r1.json();
    
    // 2. Search by resource_id con status=closed
    const r2 = await fetch(`https://api.mercadolibre.com/post-purchase/v1/claims/search?resource_id=${orderId}&status=closed&limit=5`, { headers: h });
    results.by_resource_id_closed = await r2.json();
    
    // 3. Shipment info
    const order = await mlGet(`/orders/${orderId}`);
    if (order.shipping?.id) {
      const ship = await mlGet(`/shipments/${order.shipping.id}`);
      results.shipment = {
        id: ship.id,
        status: ship.status,
        substatus: ship.substatus,
        status_detail: ship.status_detail,
        logistic_type: ship.logistic_type,
        status_history: ship.status_history,
      };
    }
    
    res.json({ order_id: orderId, order_status: order.status, ...results });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── MERCADO LIBRE — OAuth ────────────────────────────────────────────
app.get('/ml/auth', (_, res) => {
  const url = `https://auth.mercadolibre.com.ar/authorization?response_type=code&client_id=${ML_CLIENT_ID}&redirect_uri=${encodeURIComponent(ML_REDIRECT_URI)}`;
  res.redirect(url);
});

app.get('/ml/callback', async (req, res) => {
  const { code } = req.query;
  if (!code) return res.status(400).send('Sin código de autorización');
  try {
    const r = await fetch('https://api.mercadolibre.com/oauth/token', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ grant_type: 'authorization_code', client_id: ML_CLIENT_ID, client_secret: ML_CLIENT_SECRET, code, redirect_uri: ML_REDIRECT_URI })
    });
    const data = await r.json();
    if (!data.access_token) return res.status(400).json(data);
    await saveMLToken({ access: data.access_token, refresh: data.refresh_token, expires: Date.now() + data.expires_in * 1000 });
    syncMLVentas(30).catch(console.error); // primera sync: últimos 30 días
    res.send(`
      <html><body style="font-family:sans-serif;text-align:center;padding:60px">
        <h2>✅ Mercado Libre conectado correctamente</h2>
        <p>Ya podés cerrar esta ventana y volver a la app ADARA.</p>
        <script>setTimeout(() => window.close(), 3000)</script>
      </body></html>
    `);
  } catch (e) { res.status(500).send(e.message); }
});

app.get('/ml/status', (_, res) => res.json({
  conectado:  !!ML.access,
  expira_en:  ML.expires ? Math.round((ML.expires - Date.now()) / 60000) + ' min' : 'n/a'
}));

// ── MERCADO LIBRE — Sync ventas ──────────────────────────────────────

// ── Feriados Argentina (API nolaborables.com.ar + fallback hardcodeado) ───
let FERIADOS_CACHE = {}; // { '2026': Set(['2026-02-16','2026-02-17',...]) }

// Fallback: feriados nacionales Argentina (actualizar anualmente si la API falla)
const FERIADOS_HARDCODED = {
  '2025': ['01-01','02-03','02-04','03-24','04-02','04-18','04-19','05-01','05-25','06-16','06-20','07-09','08-17','10-12','11-20','11-24','12-08','12-25'],
  '2026': ['01-01','02-16','02-17','03-24','04-02','04-03','04-04','05-01','05-25','06-15','06-20','07-09','08-17','10-12','11-23','12-08','12-25']
};

async function loadFeriados(year) {
  if (FERIADOS_CACHE[year]) return FERIADOS_CACHE[year];
  try {
    const r = await fetch(`https://nolaborables.com.ar/api/v2/feriados/${year}`);
    if (!r.ok) throw new Error(`API feriados: ${r.status}`);
    const data = await r.json();
    const set = new Set();
    for (const f of data) {
      const mm = String(f.mes).padStart(2, '0');
      const dd = String(f.dia).padStart(2, '0');
      set.add(`${year}-${mm}-${dd}`);
    }
    FERIADOS_CACHE[year] = set;
    console.log(`✓ Feriados ${year}: ${set.size} días cargados (API)`);
    return set;
  } catch (e) {
    console.warn('loadFeriados API falló:', e.message, '→ usando fallback hardcodeado');
    const set = new Set();
    const hc = FERIADOS_HARDCODED[String(year)];
    if (hc) hc.forEach(d => set.add(`${year}-${d}`));
    FERIADOS_CACHE[year] = set;
    console.log(`✓ Feriados ${year}: ${set.size} días cargados (hardcoded)`);
    return set;
  }
}

function esFeriado(fechaStr) {
  const year = fechaStr.substring(0, 4);
  const set = FERIADOS_CACHE[year];
  return set ? set.has(fechaStr) : false;
}

// Avanzar al siguiente día hábil (no finde, no feriado)
function siguienteDiaHabil(d) {
  let intentos = 0;
  while (intentos < 10) {
    const dia = d.getDay();
    const fechaStr = d.toISOString().split('T')[0];
    if (dia !== 0 && dia !== 6 && !esFeriado(fechaStr)) break;
    d.setDate(d.getDate() + 1);
    intentos++;
  }
  return d;
}

// Calcular fecha de despacho Flex según reglas de MEF:
// - Lun a Vie (hábil) antes de 12:00 → mismo día
// - Lun a Vie (hábil) después de 12:00 → siguiente día hábil
// - Finde / feriado → siguiente día hábil
function calcFechaDespachoFlex(fechaISO, horaStr) {
  if (!fechaISO) return null;
  const d = new Date(fechaISO + 'T' + (horaStr || '12:00:00'));
  const dia = d.getDay();
  const hora = d.getHours();
  const fechaStr = d.toISOString().split('T')[0];

  if (dia === 0 || dia === 6 || esFeriado(fechaStr)) {
    // Finde o feriado → siguiente día hábil
    d.setDate(d.getDate() + 1);
    siguienteDiaHabil(d);
  } else if (hora >= 12) {
    // Día hábil después de 12:00 → siguiente día hábil
    d.setDate(d.getDate() + 1);
    siguienteDiaHabil(d);
  }
  // Día hábil antes de 12:00 → mismo día (no hace nada)

  return d.toISOString().split('T')[0];
}

// Convierte un timestamp ISO de ML (venga en UTC "...Z" o con offset "...-03:00")
// a fecha + hora en horario de Argentina. Así la app coincide con lo que muestra
// Mercado Libre (que usa hora local argentina en el panel de ventas y los reportes).
function fechaHoraARG(iso) {
  if (!iso) return { fecha: null, hora: null };
  const d = new Date(iso);
  if (isNaN(d.getTime())) return { fecha: null, hora: null };
  const partes = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'America/Argentina/Buenos_Aires',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false
  }).formatToParts(d).reduce((o, p) => (o[p.type] = p.value, o), {});
  const hh = partes.hour === '24' ? '00' : partes.hour; // medianoche normalizada
  return {
    fecha: `${partes.year}-${partes.month}-${partes.day}`,
    hora: `${hh}:${partes.minute}:${partes.second}`
  };
}

async function syncMLVentas(diasAtras = 7, fechaDesde = null, fechaHasta = null) {
  if (!ML.access) throw new Error('ML no autenticado. Conectá ML primero desde la app.');

  const me     = await mlGet('/users/me');
  const userId = me.id;

  // Fechas: si se pasan explícitas, usarlas. Si no, usar diasAtras.
  const desde = fechaDesde || new Date(Date.now() - diasAtras * 86400000).toISOString().split('T')[0];
  const hasta = fechaHasta || new Date().toISOString().split('T')[0];

  // Cargar feriados para los años del rango
  const yearDesde = parseInt(desde.substring(0, 4));
  const yearHasta = parseInt(hasta.substring(0, 4));
  for (let y = yearDesde; y <= yearHasta; y++) {
    await loadFeriados(y);
  }

  // Asignación de línea de negocio: DESACOPLADA del sync (defensiva).
  // El esquema viejo (catalogo_skus / lineas_negocio.activa/nombre) ya no existe.
  // La conciliación NO necesita la línea, así que NO la asignamos acá para
  // evitar (a) que el sync se cuelgue contra el esquema nuevo y (b) ensuciar
  // datos con líneas mal asignadas. La línea se asigna en el paso de P&L,
  // mapeando familia del SKU × canal 'ml' → línea (modelo v22).
  // matchLinea queda como stub para no romper las llamadas existentes.
  function matchLinea(_sellerSku, _itemTitle) {
    return null;
  }

  // ML API tiene un tope de offset=1000. Para traer más, partimos por rangos de fecha.
  // Estrategia: iterar por intervalos de 7 días (~500 ventas max por chunk).
  const dateChunks = [];
  let chunkStart = new Date(desde);
  const endDate  = new Date(hasta);
  while (chunkStart <= endDate) {
    let chunkEnd = new Date(chunkStart);
    chunkEnd.setDate(chunkEnd.getDate() + 6); // 7 días por chunk
    if (chunkEnd > endDate) chunkEnd = new Date(endDate);
    dateChunks.push({
      from: chunkStart.toISOString().split('T')[0],
      to:   chunkEnd.toISOString().split('T')[0]
    });
    chunkStart = new Date(chunkEnd);
    chunkStart.setDate(chunkStart.getDate() + 1);
  }

  let totalInsertados = 0, totalOrdenes = 0;

  for (const chunk of dateChunks) {
    let offset = 0, chunkTotal = 0;

    do {
      const data = await mlGet(
        `/orders/search?seller=${userId}` +
        `&order.date_created.from=${chunk.from}T00:00:00.000-03:00` +
        `&order.date_created.to=${chunk.to}T23:59:59.999-03:00` +
        `&offset=${offset}&limit=50&sort=date_asc`
      );
      chunkTotal = data.paging?.total || 0;
      const orders = data.results || [];
      if (!orders.length) break;

      const rows = [];

      // Build base rows from orders
      const orderData = orders.map(o => {
        const item    = o.order_items?.[0] || {};
        // Split payments: recoger TODOS los pagos aprobados
        const approvedPayments = o.payments?.filter(p => p.status === 'approved') || [];
        const payment = approvedPayments[0] || o.payments?.[0] || {};
        const allPaymentIds = approvedPayments.map(p => String(p.id));
        const bruto   = o.total_amount     || 0;
        // Fecha y hora SIEMPRE en horario de Argentina, para que coincidan
        // exactamente con lo que muestra Mercado Libre (panel de ventas/envíos).
        // date_created puede venir en UTC o con offset; convertimos el instante
        // a America/Argentina/Buenos_Aires antes de derivar fecha / hora / período.
        const { fecha: fechaVenta, hora: horaVenta } = fechaHoraARG(o.date_created);

        const comisionReal = payment.marketplace_fee != null && payment.marketplace_fee !== 0
          ? Math.abs(payment.marketplace_fee)
          : (item.sale_fee || bruto * 0.1375);

        const sellerSku = item.item?.seller_sku || item.item?.seller_custom_field || null;
        const lineaId = matchLinea(sellerSku, item.item?.title);
        const motivoCancelacion = o.status === 'cancelled'
          ? (o.cancel_detail?.reason || o.status_detail || null) : null;
        const montoDevuelto = payment.amount_refunded || 0;

        return {
          shippingId: o.shipping?.id || null,
          paymentIds: allPaymentIds,
          paymentId:  payment.id ? String(payment.id) : null,
          bruto,
          comision: comisionReal,
          row: {
            ml_order_id:      String(o.id),
            periodo:          fechaVenta ? fechaVenta.substring(0, 7) : null,
            periodo_cobro:    fechaVenta ? fechaVenta.substring(0, 7) : null,
            fecha:            fechaVenta,
            titulo:           item.item?.title || '',
            sku:              sellerSku,
            cantidad:         item.quantity || 1,
            importe_bruto:    bruto,
            cargo_venta:      -Math.abs(comisionReal),
            cargo_envio:      0,
            costo_financiero: 0,
            impuestos:        0,
            por_cobrar:       bruto - comisionReal,  // provisional, se actualiza con charges
            mp_payment_id:    payment.id ? String(payment.id) : null,
            mp_payment_ids:   allPaymentIds.length > 1 ? allPaymentIds.join(',') : null,
            pagos_cantidad:   allPaymentIds.length || 1,
            ml_status:        o.status || 'unknown',
            motivo_cancelacion: motivoCancelacion,
            monto_devuelto:   montoDevuelto,
            pack_id:          o.pack_id ? String(o.pack_id) : null,
            shipment_id:      o.shipping?.id ? String(o.shipping.id) : null,
            linea_negocio_id: lineaId,
            tipo_envio:       null,
            fecha_entrega:    null,
            ciudad_destino:   null,
            partido:          null,
            enviado:          false,
            hora_venta:       horaVenta,
            fecha_despacho_flex: null,
            estado_envio:     'no_preparado',
            fecha_cobro:      null,
          }
        };
      });

      // Fetch shipment + payment data in parallel (batches of 10)
      // Shipments → tipo envío, ciudad, fecha entrega
      // Collections → net_received_amount (neto real) → calculate taxes
      const PARALLEL = 10;
      for (let i = 0; i < orderData.length; i += PARALLEL) {
        const batch = orderData.slice(i, i + PARALLEL);
        await Promise.all(batch.map(async (od) => {
          const promises = [];

          // 1. Shipment (tipo_envio, ciudad, fecha + bonificación Flex)
          if (od.shippingId) {
            promises.push(
              mlGet(`/shipments/${od.shippingId}`).then(ship => {
                od.row.tipo_envio = ship.logistic_type === 'self_service' ? 'flex'
                  : ship.logistic_type === 'fulfillment' ? 'fulfillment'
                  : ship.logistic_type === 'xd_drop_off' ? 'colecta'
                  : ship.logistic_type || 'otro';
                od.row.fecha_entrega = ship.status_history?.date_delivered?.split('T')[0] || null;
                
                // Clasificar partido + ciudad_destino para Flex
                // ML manda: state.id = "AR-C" para CABA, city.name = partido para GBA
                // Para La Matanza: usar neighborhood para distinguir norte (GBA1) vs sur (GBA2)
                const stateId = ship.receiver_address?.state?.id;
                const cityName = ship.receiver_address?.city?.name || ship.receiver_address?.city || null;
                const neighborhoodName = ship.receiver_address?.neighborhood?.name || null;
                
                const LA_MATANZA_SUR = ['isidro casanova', 'gregorio de laferrere', 'rafael castillo',
                  'gonzález catán', 'gonzalez catan', 'gonzlez catan', 'ciudad evita', 'virrey del pino'];
                
                if (stateId === 'AR-C' || (cityName && ['capital federal', 'caba', 'buenos aires'].includes(cityName.toLowerCase()) && !neighborhoodName)) {
                  // CABA: city.name es el barrio, no el partido
                  od.row.partido = 'CABA';
                  od.row.ciudad_destino = neighborhoodName || cityName;
                } else if (cityName && cityName.toLowerCase() === 'la matanza') {
                  // La Matanza: split norte/sur por localidad
                  const localidad = (neighborhoodName || '').toLowerCase();
                  od.row.partido = LA_MATANZA_SUR.includes(localidad) ? 'La Matanza Sur' : 'La Matanza Norte';
                  od.row.ciudad_destino = neighborhoodName || 'La Matanza';
                } else {
                  // GBA: city.name es el partido (Quilmes, Morón, Vicente López, etc.)
                  od.row.partido = cityName;
                  od.row.ciudad_destino = neighborhoodName || cityName;
                }
                
                od.row.enviado = ship.status === 'delivered';

                // Estado físico del envío para cancelaciones/devoluciones
                const ss = ship.status || 'pending';
                od.row.estado_envio = ss === 'delivered' ? 'entregado'
                  : ['shipped', 'not_delivered'].includes(ss) ? 'despachado'
                  : ss === 'ready_to_ship' ? 'preparado'
                  : 'no_preparado';

                // Para cancelled + despachado/entregado: capturar substatus como motivo
                if (od.row.ml_status === 'cancelled' && ['despachado','entregado'].includes(od.row.estado_envio)) {
                  const sub = ship.substatus || ship.status_detail?.substatus || '';
                  const SUBSTATUS_MAP = {
                    'returning_to_sender': 'Devuelto al vendedor',
                    'returned_to_sender': 'Devuelto al vendedor',
                    'receiver_absent': 'Destinatario ausente',
                    'receiver_rejected': 'Destinatario rechazó',
                    'damaged': 'Paquete dañado',
                    'lost': 'Paquete extraviado',
                    'stolen': 'Paquete robado',
                    'not_delivered': 'No entregado',
                    'cancelled': 'Cancelado en tránsito',
                    'out_of_zone': 'Fuera de zona',
                    'wrong_address': 'Dirección incorrecta',
                    'buyer_absent': 'Comprador ausente',
                  };
                  const motivoShip = SUBSTATUS_MAP[sub] || sub || 'Entrega fallida';
                  // Solo setear si no tiene ya un motivo mejor del cancel_detail
                  if (!od.row.motivo_cancelacion || od.row.motivo_cancelacion === 'cancelled') {
                    od.row.motivo_cancelacion = motivoShip;
                  }
                }

                // Bonificación Flex: ML devuelve base_cost - list_cost como bonificación
                // Es un ingreso (positivo) porque el vendedor pone logística propia
                if (ship.logistic_type === 'self_service') {
                  const baseCost = ship.base_cost || 0;
                  const listCost = ship.shipping_option?.list_cost || 0;
                  if (baseCost > 0 && listCost > 0) {
                    od.flexBonificacion = Math.round((baseCost - listCost) * 100) / 100;
                  }
                }
              }).catch(() => { od.row.tipo_envio = 'otro'; od.row.estado_envio = 'no_preparado'; })
            );
          }

          // 2. MP Payment details — iterar TODOS los payments aprobados (split payment)
          //    Los cargos se distribuyen entre payments, hay que sumarlos todos
          if (od.paymentIds.length && od.row.ml_status !== 'cancelled') {
            od._comision = 0; od._impuestos = 0; od._financiero = 0; od._financiero_info = 0; od._envio = 0;
            od._shippingBuyerContrib = 0; // contribución del comprador al envío

            for (const pid of od.paymentIds) {
              promises.push(
                fetch(`https://api.mercadopago.com/v1/payments/${pid}`, {
                  headers: { 'Authorization': 'Bearer ' + ML.access }
                }).then(r => r.ok ? r.json() : null).then(pay => {
                  if (!pay) return;

                  // Parse charges_details de ESTE payment
                  const charges = pay.charges_details || [];

                  for (const ch of charges) {
                    const amt = ch.amounts?.original || 0;
                    if (amt === 0) continue;
                    const type = (ch.type || '').toLowerCase();
                    const name = (ch.name || '').toLowerCase();

                    // Skip buyer-side charges (coupons, discounts)
                    if (type === 'coupon' || type === 'discount') continue;
                    if (name.includes('coupon') || name.includes('rebate')) continue;
                    // tax_withholding_payer-* = retencion del COMPRADOR, no del vendedor → ignorar
                    if (type === 'tax' && name.includes('_payer')) continue;

                    if (type === 'tax') {
                      od._impuestos += amt;
                    } else if (type === 'fee') {
                      if (name === 'financing_add_on_fee') {
                        // VOS ofrecés las cuotas: ML te descuenta este costo de la liquidación.
                        // Nombre exacto: "financing_add_on_fee" → va en costo_financiero y en por_cobrar.
                        od._financiero += amt;
                      } else if (name === 'financing_fee') {
                        // ML ofrece las cuotas: ML lo absorbe internamente.
                        // Nombre exacto: "financing_fee" → informativo, NO afecta por_cobrar.
                        od._financiero_info += amt;
                      } else {
                        od._comision += amt;
                      }
                    } else if (type === 'shipping') {
                      od._envio += amt;
                    }
                  }

                  // Acumular contribución del comprador al envío (shipping_amount)
                  // Este monto se resta del cargo de envío para obtener el neto real
                  if (pay.shipping_amount > 0) {
                    od._shippingBuyerContrib += pay.shipping_amount;
                  }

                  // fecha_cobro: tomar del primer payment que tenga
                  if (!od.row.fecha_cobro) {
                    od.row.fecha_cobro = fechaHoraARG(pay.money_release_date || pay.date_approved).fecha;
                  }
                }).catch(err => {
                  console.warn(`⚠ Payment ${pid} fetch failed:`, err.message);
                })
              );
            }
          }

          await Promise.all(promises);

          // Aplicar totales acumulados de TODOS los payments (split payment support)
          if (od._comision > 0) od.row.cargo_venta = -od._comision;
          if (od._impuestos > 0) od.row.impuestos = -od._impuestos;

          // financing_add_on_fee: el VENDEDOR ofrece cuotas sin interés. ML descuenta este costo
          //   de la liquidación → afecta por_cobrar. Se guarda en costo_financiero.
          // financing_fee: ML ofrece cuotas (MSI plataforma). ML lo absorbe internamente.
          //   ML NO lo descuenta de la liquidación del vendedor → NO afecta por_cobrar.
          //   Se acumula en _financiero_info solo con fines informativos.
          // Validado con datos reales (19 marzo 2026). Ver ADARA-DECISIONES.md.
          if (od._financiero > 0) od.row.costo_financiero = -od._financiero;

          // Envío neto = cargo de envío - contribución del comprador
          // Ej: cargo $10,729.58 - buyer $5,346.59 = neto $5,382.99
          if (od._envio > 0) {
            const envioNeto = od._envio - (od._shippingBuyerContrib || 0);
            od.row.cargo_envio = -Math.round(envioNeto * 100) / 100;
          }

          // por_cobrar = lo que ML efectivamente liquida al vendedor
          // Fórmula: importe_bruto + cargo_venta + cargo_envio + impuestos - financing_add_on_fee
          // financing_add_on_fee (_financiero) SÍ se descuenta: ML lo retiene de la liquidación.
          // financing_fee (_financiero_info) NO se descuenta: ML lo absorbe. No entra en la fórmula.
          if (od._comision > 0 || od._impuestos > 0 || od._financiero > 0 || od._envio > 0) {
            od.row.por_cobrar = od.row.importe_bruto
              + od.row.cargo_venta
              + od.row.cargo_envio
              + od.row.impuestos
              + (od._financiero > 0 ? -od._financiero : 0);
          }

          // Después de resolver shipment + payment:
          // Si es Flex, cargo_envio = solo la bonificación Flex (para que el matching de bonificaciones funcione)
          //   - flexBonificacion: ML te paga por usar logística propia
          //   - _shippingBuyerContrib: lo que pagó el comprador; ML también te lo transfiere en la liquidación
          //     Se suma DIRECTO en por_cobrar, NO en cargo_envio (no afecta el matching de bonificaciones)
          //   - Si _shippingBuyerContrib = 0, por_cobrar es idéntico al caso sin shipping (sin cambio)
          // Si es colecta/full, cargo_envio ya viene de charges_details (negativo, es un costo)
          if (od.flexBonificacion && od.flexBonificacion > 0) {
            od.row.cargo_envio = od.flexBonificacion;

            // Imp. Créd. y Déb. solo sobre la bonificación Flex (0.6%)
            // El shipping del comprador no genera este impuesto adicional
            const impBonificacion = Math.round(od.flexBonificacion * 0.006 * 100) / 100;
            od.row.impuestos = (od.row.impuestos || 0) - impBonificacion;

            // por_cobrar = bruto + comisión + bonif_flex + shipping_comprador + impuestos - financiero
            // shipping_comprador se suma directo (ML te lo transfiere en la liquidación)
            od.row.por_cobrar = od.row.importe_bruto
              + od.row.cargo_venta
              + od.row.cargo_envio
              + (od._shippingBuyerContrib || 0)
              + od.row.impuestos
              + (od._financiero > 0 ? -od._financiero : 0);
          }

          // Calcular fecha de despacho Flex según reglas MEF
          // (antes de 12hs = mismo día, después = siguiente hábil, finde = lunes)
          if (od.row.tipo_envio === 'flex') {
            od.row.fecha_despacho_flex = calcFechaDespachoFlex(od.row.fecha, od.row.hora_venta);
          }
        }));
      }

      const dbRows = orderData.map(d => d.row);
      if (dbRows.length) {
        // Normalizar: todas las filas deben tener exactamente las mismas keys
        // Supabase PGRST102: "All object keys must match"
        const allKeys = new Set();
        dbRows.forEach(r => Object.keys(r).forEach(k => allKeys.add(k)));
        // Remover campos que no deben pisarse en re-sync
        allKeys.delete('conciliado');
        allKeys.delete('estado_cancelacion');
        // periodo_cobro SE INCLUYE en el upsert (se setea = periodo en línea ~486)
        // Si se hizo → Pasar, un re-sync lo resetea — es aceptable y preferible a dejarlo null
        const keyList = [...allKeys];
        const normalized = dbRows.map(r => {
          const obj = {};
          keyList.forEach(k => obj[k] = r[k] !== undefined ? r[k] : null);
          return obj;
        });
        await sbUpsert('ventas_ml', normalized, 'ml_order_id');
        totalInsertados += dbRows.length;
      }

      offset += 50;
      totalOrdenes += orders.length;
    } while (offset < chunkTotal && offset < 1000);

    // Si hay más de 1000 en este chunk, loguear warning
    if (chunkTotal > 1000) {
      console.warn(`⚠ Chunk ${chunk.from}→${chunk.to}: ${chunkTotal} órdenes (>1000). Puede faltar data. Reducir intervalo.`);
    }
  }

  console.log(`✓ ML sync: ${totalInsertados} órdenes (${desde} → ${hasta}) en ${dateChunks.length} chunks`);
  return { insertados: totalInsertados, total: totalOrdenes, desde, hasta, chunks: dateChunks.length };
}

app.post('/ml/sync', async (req, res) => {
  try {
    const { dias, desde, hasta } = { ...req.query, ...req.body };
    const result = await syncMLVentas(
      parseInt(dias) || 7,
      desde || null,
      hasta || null
    );

    // Auto-descartar cancelaciones sin entrega (no_preparado/preparado)
    // Nunca salieron del depósito → no generan movimientos financieros → no son conciliables
    const descartadas = await sbPatch('ventas_ml',
      `ml_status=eq.cancelled&estado_envio=in.(no_preparado,preparado)&estado_conciliacion=neq.descartada`,
      { estado_conciliacion: 'descartada', conciliado: false, balance_conciliacion: 0 }
    ).catch(() => null);
    if (descartadas) {
      console.log(`  → Auto-descartadas: cancelaciones sin entrega`);
    }

    // Ingesta de retenciones IIBB en segundo plano (no bloquea; idempotente; throttle 6h)
    kickRetenciones();

    // ── Costeo (capa CMV): proyectar ventas ML → consumir FIFO → congelar CMV estimado ──
    // Tras el sync, las ventas nuevas de `ventas_ml` se proyectan a `ventas`/`venta_items`
    // (fn_proyectar_ml, idempotente por (canal,referencia_externa)), consumen lotes por
    // FIFO (fn_consumir_fifo, idempotente por unidades ya consumidas) y, por último, se
    // congela el CMV estimado de las ventas SIN FIFO real (fn_congelar_cmv_estimado,
    // idempotente por ON CONFLICT (venta_item_id) DO NOTHING). Orden obligatorio:
    // proyectar → consumir → congelar (el FIFO necesita los venta_items; el congelado
    // necesita saber qué quedó sin consumo FIFO). El FIFO solo consume ventas con fecha
    // >= fecha_alta del lote (seed); las ventas sin lote se costean por costo_referencia
    // y su CMV se congela acá para que un lote futuro NO recalcule meses ya cerrados
    // (CF3/CF4). En un sync normal congela 0 (ya estaban congeladas). No bloquea el sync:
    // si el costeo falla, se loguea y se sigue. Ver ADARA-COSTEO-FIFO.md.
    let costeo = null;
    try {
      const efDesde = desde || new Date(Date.now() - (parseInt(dias) || 7) * 86400000).toISOString().split('T')[0];
      const efHasta = hasta || new Date().toISOString().split('T')[0];
      const proyectadas = await sbRpc('fn_proyectar_ml',          { p_desde: efDesde, p_hasta: efHasta });
      const fifo        = await sbRpc('fn_consumir_fifo',         { p_desde: efDesde, p_hasta: efHasta });
      const congelados  = await sbRpc('fn_congelar_cmv_estimado', {});
      costeo = {
        desde: efDesde, hasta: efHasta,
        ventas_proyectadas: proyectadas,
        fifo: Array.isArray(fifo) ? fifo[0] : fifo,
        cmv_congelados: congelados
      };
      console.log(`  → Costeo ${efDesde}…${efHasta}: ${proyectadas} ventas proyectadas; FIFO ${JSON.stringify(costeo.fifo)}; ${congelados} CMV congelados`);
    } catch (e) {
      console.warn('  → Costeo (proyección/FIFO/congelado) falló (no bloquea el sync):', e.message);
      costeo = { error: e.message };
    }

    res.json({ ok: true, ...result, costeo });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── DEBUG ENDPOINTS (temporales, para diagnosticar) ──────────────────
app.get('/debug-payment-full/:paymentId', async (req, res) => {
  try {
    if (!ML.access) return res.status(401).json({ error: 'ML no autenticado' });
    const r = await fetch(`https://api.mercadopago.com/v1/payments/${req.params.paymentId}`, {
      headers: { 'Authorization': 'Bearer ' + ML.access }
    });
    const data = await r.json();
    res.json(data);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/debug-order/:orderId', async (req, res) => {
  try {
    if (!ML.access) return res.status(401).json({ error: 'ML no autenticado' });
    const order = await mlGet(`/orders/${req.params.orderId}`);
    let shipment = null;
    if (order.shipping?.id) {
      shipment = await mlGet(`/shipments/${order.shipping.id}`);
    }
    res.json({
      order_id: order.id,
      status: order.status,
      shipping_id: order.shipping?.id,
      receiver_address: shipment?.receiver_address || null,
      logistic_type: shipment?.logistic_type || null,
      shipment_status: shipment?.status || null,
      base_cost: shipment?.base_cost || null,
      list_cost: shipment?.shipping_option?.list_cost || null
    });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── DEVOLUCIONES ML (Claims API) ──────────────────────────────────────
app.get('/ml/devoluciones', async (req, res) => {
  try {
    if (!ML.access) return res.status(401).json({ error: 'ML no autenticado' });
    const headers = { 'Authorization': 'Bearer ' + ML.access };
    
    // ── PASO 1: Claims abiertos (devoluciones activas) ──
    const r1 = await fetch(`https://api.mercadolibre.com/post-purchase/v1/claims/search?status=opened&sort=date_created:desc&limit=50`, { headers });
    const openedData = await r1.json();
    const openedClaims = openedData.data || [];
    console.log(`Claims abiertos: ${openedClaims.length}`);
    
    // ── PASO 2: Cancelled+entregado SIN claim_id (devoluciones no detectadas) ──
    let huerfanas = [];
    let hOffset = 0;
    while (true) {
      const page = await sbGet('ventas_ml', `ml_status=eq.cancelled&claim_id=is.null&estado_envio=in.(despachado,entregado)&aprobada=neq.true&select=ml_order_id&limit=200&offset=${hOffset}`);
      if (!page?.length) break;
      huerfanas.push(...page);
      if (page.length < 200) break;
      hOffset += 200;
    }
    console.log(`Canceladas sin claim (huérfanas): ${huerfanas.length}`);
    
    // Para cada huérfana, buscar claim por resource_id (order_id)
    const huerfanaClaims = [];
    for (let i = 0; i < huerfanas.length; i += 5) {
      const batch = huerfanas.slice(i, i + 5);
      await Promise.all(batch.map(async (h) => {
        try {
          const rS = await fetch(`https://api.mercadolibre.com/post-purchase/v1/claims/search?resource_id=${h.ml_order_id}&limit=1`, { headers });
          const sData = await rS.json();
          if (sData.data?.length) huerfanaClaims.push(sData.data[0]);
        } catch (e) {
          console.warn(`Claim search order ${h.ml_order_id}:`, e.message);
        }
      }));
    }
    console.log(`Claims encontrados para huérfanas: ${huerfanaClaims.length}`);
    
    // ── PASO 3: Unificar (deduplicar por claim.id) ──
    const seenIds = new Set();
    const allClaims = [];
    for (const c of [...openedClaims, ...huerfanaClaims]) {
      if (!seenIds.has(c.id)) { seenIds.add(c.id); allClaims.push(c); }
    }
    
    if (!allClaims.length) {
      return res.json({ ok: true, total: 0, updated: 0, openedClaims: openedClaims.length, huerfanas: huerfanas.length, huerfanasConClaim: 0, devoluciones: [] });
    }
    
    // ── PASO 4: Detalle de cada claim + return ──
    const results = [];
    for (let i = 0; i < allClaims.length; i += 5) {
      const batch = allClaims.slice(i, i + 5);
      await Promise.all(batch.map(async (claim) => {
        try {
          const rClaim = await fetch(`https://api.mercadolibre.com/post-purchase/v1/claims/${claim.id}`, { headers });
          const claimDetail = await rClaim.json();
          
          let returnDetail = null;
          if (claimDetail.related_entities?.includes('return')) {
            const rRet = await fetch(`https://api.mercadolibre.com/post-purchase/v2/claims/${claim.id}/returns`, { headers });
            returnDetail = await rRet.json();
          }
          
          results.push({
            claim_id: claim.id,
            order_id: String(claim.resource_id),
            type: claim.type,
            stage: claimDetail.stage || claim.stage,
            reason_id: claim.reason_id,
            date_created: claim.date_created,
            fulfilled: claimDetail.fulfilled,
            quantity_type: claimDetail.quantity_type,
            return_status: returnDetail?.status || null,
            return_ship_status: returnDetail?.shipments?.[0]?.status || null,
            money_status: returnDetail?.status_money || null,
            seller_actions: (claimDetail.players || [])
              .find(p => p.type === 'seller')?.available_actions?.map(a => a.action) || [],
          });
        } catch (e) {
          console.warn(`Claim ${claim.id} fetch error:`, e.message);
        }
      }));
    }
    
    // ── PASO 5: Actualizar ventas_ml ──
    let updated = 0;
    for (const dev of results) {
      let claimStatus;
      if (dev.stage === 'closed' || dev.stage === 'resolved') {
        claimStatus = 'cerrado';
      } else if (dev.return_ship_status === 'delivered') {
        claimStatus = 'producto_recibido';
      } else if (dev.return_ship_status === 'shipped') {
        claimStatus = 'en_transito';
      } else if (dev.return_status === 'label_generated') {
        claimStatus = 'etiqueta_generada';
      } else {
        claimStatus = 'abierto';
      }
      
      try {
        const existing = await sbGet('ventas_ml', `ml_order_id=eq.${dev.order_id}&select=claim_id,claim_status&limit=1`);
        const venta = existing?.[0];
        if (!venta) continue;
        if (venta.claim_id && venta.claim_id !== String(dev.claim_id)) continue;
        if (['reingresado', 'perdida'].includes(venta.claim_status)) continue;
        
        await sbPatch('ventas_ml', `ml_order_id=eq.${dev.order_id}`, {
          claim_id: String(dev.claim_id),
          claim_status: claimStatus,
          motivo_devolucion: dev.reason_id,
          fecha_devolucion: dev.date_created ? dev.date_created.substring(0, 10) : null,
        });
        updated++;
      } catch (e) {
        console.warn(`Patch venta ${dev.order_id}:`, e.message);
      }
    }
    
    // ── PASO 6: Enriquecer respuesta ──
    const orderIds = results.map(r => r.order_id);
    let ventasMap = {};
    if (orderIds.length) {
      for (let i = 0; i < orderIds.length; i += 20) {
        const chunk = orderIds.slice(i, i + 20);
        const ventas = await sbGet('ventas_ml', `ml_order_id=in.(${chunk.join(',')})&select=ml_order_id,titulo,sku,importe_bruto,tipo_envio,fecha,linea_negocio_id`);
        for (const v of (ventas || [])) ventasMap[v.ml_order_id] = v;
      }
    }
    
    const devoluciones = results.map(r => ({
      ...r,
      titulo: ventasMap[r.order_id]?.titulo || null,
      sku: ventasMap[r.order_id]?.sku || null,
      importe: ventasMap[r.order_id]?.importe_bruto || null,
      tipo_envio: ventasMap[r.order_id]?.tipo_envio || null,
      fecha_venta: ventasMap[r.order_id]?.fecha || null,
    }));
    
    console.log(`✓ Devoluciones: ${openedClaims.length} abiertos + ${huerfanaClaims.length} huérfanas = ${results.length} total, ${updated} actualizadas`);
    res.json({ ok: true, total: results.length, updated, openedClaims: openedClaims.length, huerfanas: huerfanas.length, huerfanasConClaim: huerfanaClaims.length, devoluciones });
    
  } catch (e) {
    console.error('Devoluciones ML:', e);
    res.status(500).json({ error: e.message });
  }
});


// ── Recepción de producto devuelto / cancelado ───────────────────────
// Registra condición del producto al llegar al depósito.
// condicion = 'ok'           → reverso FIFO: devuelve unidades al lote original
//                              (consumo_lote.reverso_devolucion) + revierte CMV + aprobada=true
// condicion = 'no_disponible' → sin reverso → la unidad es pérdida (P3), nota obligatoria
// En ambos casos: inserta en stock_devoluciones + actualiza ventas_ml
app.post('/ml/recepcion', async (req, res) => {
  try {
    const { venta_id, condicion, nota } = req.body;
    if (!venta_id) return res.status(400).json({ error: 'Falta venta_id' });
    if (!['ok', 'no_disponible'].includes(condicion)) return res.status(400).json({ error: 'condicion debe ser ok o no_disponible' });
    if (condicion === 'no_disponible' && !nota?.trim()) return res.status(400).json({ error: 'Nota obligatoria para producto no disponible' });

    // Traer venta
    const ventas = await sbGet('ventas_ml', `id=eq.${venta_id}&select=id,sku,titulo,periodo,claim_status,recepcion_condicion,ml_order_id&limit=1`);
    if (!ventas?.length) return res.status(404).json({ error: 'Venta no encontrada' });
    const venta = ventas[0];

    const hoy = new Date().toISOString().split('T')[0];
    const nuevoClaimStatus = condicion === 'ok' ? 'reingresado' : 'perdida';

    // 1. Actualizar ventas_ml
    await sbPatch('ventas_ml', `id=eq.${venta_id}`, {
      recepcion_condicion: condicion,
      recepcion_fecha: hoy,
      recepcion_nota: nota?.trim() || null,
      claim_status: nuevoClaimStatus,
      aprobada: true,
    });

    // 2. Insertar en stock_devoluciones
    await sb('POST', 'stock_devoluciones', [{
      venta_ml_id: venta_id,
      sku: venta.sku || null,
      titulo: venta.titulo || null,
      condicion,
      nota: nota?.trim() || null,
      fecha_recepcion: hoy,
      periodo: venta.periodo || null,
    }]);

    // 3. Si ok → revertir el consumo FIFO: devuelve las unidades al lote original
    //    (consumo_lote.reverso_devolucion) con el costo snapshot, y revierte el CMV.
    //    Reemplaza el viejo +1 a `catalogo_skus` (modelo v21 muerto). Solo afecta
    //    ventas que ya consumieron stock (fecha >= seed); idempotente por neto.
    //    Si 'no_disponible' no hay reverso → la unidad es pérdida (P3).
    //    El reverso se fecha en `hoy` (= recepcion_fecha); la plata se imputa
    //    aparte al mes del movimiento MP (O10). Ver ADARA-COSTEO-FIFO.md Fase 3.
    let reversoStock = null, stockActualizado = false;
    if (condicion === 'ok' && venta.ml_order_id) {
      try {
        const rev = await sbRpc('fn_revertir_devolucion', { p_ml_order_id: venta.ml_order_id, p_fecha: hoy });
        reversoStock = Array.isArray(rev) ? rev[0] : rev;
        stockActualizado = (reversoStock?.reversos_creados || 0) > 0;
        if (stockActualizado) {
          console.log(`  → Reverso FIFO devolución order ${venta.ml_order_id}: +${reversoStock.unidades_devueltas}u al lote original`);
        }
      } catch (e) {
        console.warn('  → Reverso FIFO falló (no bloquea la recepción):', e.message);
        reversoStock = { error: e.message };
      }
    }

    res.json({ ok: true, claim_status: nuevoClaimStatus, stock_actualizado: stockActualizado, reverso_stock: reversoStock });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── DEBUG: Return reasons ──────────────────────────────────────────────
// ── ML RAW API PROXY (debug/exploración) ─────────────────────────────
app.get('/ml/raw', async (req, res) => {
  try {
    if (!ML.access) return res.status(401).json({ error: 'ML no autenticado' });
    const path = req.query.path;
    if (!path) return res.status(400).json({ error: 'Falta ?path=/v1/...' });
    const url = path.startsWith('http') ? path : `https://api.mercadolibre.com${path}`;
    const r = await fetch(url, { headers: { 'Authorization': 'Bearer ' + ML.access } });
    const data = await r.json();
    res.json(data);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/debug-return-reasons', async (req, res) => {
  try {
    if (!ML.access) return res.status(401).json({ error: 'ML no autenticado' });
    
    const reasonIds = ['PDD9939', 'PDD9942', 'PDD9946', 'PDD9949', 'PDD9953'];
    const results = {};
    
    for (const rid of reasonIds) {
      try {
        const r = await fetch(`https://api.mercadolibre.com/post-purchase/v1/claims/reasons/${rid}`, {
          headers: { 'Authorization': 'Bearer ' + ML.access }
        });
        results[rid] = await r.json();
      } catch (e) {
        results[rid] = { error: e.message };
      }
    }
    
    res.json({ ok: true, reasons: results });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── MERCADOPAGO — Settlement Report (desglose real de fees) ─────────
// Helper: call MP API
async function mpApi(path, opts = {}) {
  const url = 'https://api.mercadopago.com' + path;
  const r = await fetch(url, {
    ...opts,
    headers: { 'Authorization': 'Bearer ' + ML.access, 'Content-Type': 'application/json', ...(opts.headers || {}) }
  });
  if (!r.ok) throw new Error(`MP ${path}: ${r.status}`);
  return r;
}

// ── DEBUG (read-only) — Settlement report: header + filas que matcheen ────
// Baja el settlement report de MP para el rango dado y devuelve las COLUMNAS
// reales del CSV + las filas cuyo SOURCE_ID / ORDER_ID / SHIPPING_ID / etc.
// coincida con los ids buscados (busca el id en CUALQUIER columna, así
// descubrimos cuál liga la liquidación del envío a la orden). También permite
// inspeccionar TAXES_DISAGGREGATED para saber qué es la retención del envío.
// NO escribe nada en la base — es solo lectura/diagnóstico.
// Uso: GET /debug/settlement?desde=2026-04-01&hasta=2026-04-30&source=152079747157,152079796875
app.get('/debug/settlement', async (req, res) => {
  try {
    if (!ML.access) return res.status(401).json({ error: 'ML no autenticado' });

    // Modos de uso:
    //  - Crear + esperar:  ?desde=YYYY-MM-DD&hasta=YYYY-MM-DD
    //  - Reutilizar uno ya creado (solo descarga): ?reportId=NNN
    //  - Listar reportes disponibles con su estado: ?list=1
    const { desde, hasta } = req.query || {};
    const reportIdParam = String(req.query.reportId || '').trim();
    const fileParam = String(req.query.file || '').trim();

    // ids a buscar en CUALQUIER columna (source, order, shipping, external_reference…)
    const buscados = new Set(
      [...String(req.query.source || '').split(','), ...String(req.query.order || '').split(',')]
        .map(s => s.trim()).filter(Boolean)
    );

    // Modo lista: ver qué reportes hay, su estado, formato y nombre de archivo
    if (req.query.list) {
      const listRes = await mpApi('/v1/account/settlement_report/list');
      const list = await listRes.json();
      const arr = Array.isArray(list) ? list : [];
      return res.json({ reportes: arr.map(r => ({ id: r.id, status: r.status, format: r.format, file_name: r.file_name, begin: r.begin_date, end: r.end_date })) });
    }

    // 1. Determinar el file_name a descargar.
    //    MP descarga el settlement por NOMBRE DE ARCHIVO (file_name), NO por id
    //    (el GET por id numérico devuelve 403). El listado trae el file_name y el
    //    status ('processed' cuando está listo). No existe 'download_url'.
    let fileName = fileParam || null;
    let reportId = reportIdParam || null;

    if (!fileName) {
      // Si no pasaron ?file=, hay que resolverlo: por reportId existente, o creando uno.
      if (!reportId) {
        if (!desde || !hasta) return res.status(400).json({ error: 'Pasá ?file=NOMBRE.csv, ?reportId=NNN, ?desde=YYYY-MM-DD&hasta=YYYY-MM-DD, o ?list=1' });
        const createRes = await mpApi('/v1/account/settlement_report', {
          method: 'POST',
          body: JSON.stringify({ begin_date: `${desde}T00:00:00Z`, end_date: `${hasta}T23:59:59Z` })
        });
        const createData = await createRes.json();
        reportId = createData.id;
        if (!reportId) return res.status(400).json({ error: 'No se pudo crear reporte', detail: createData });
      }

      // Polling corto del listado hasta que el reporte esté 'processed' con file_name.
      const maxTries = reportIdParam ? 6 : 18;   // menos vueltas si reutilizamos uno ya pedido
      for (let i = 0; i < maxTries; i++) {
        const listRes = await mpApi('/v1/account/settlement_report/list');
        const list = await listRes.json();
        const arr = Array.isArray(list) ? list : [];
        const found = arr.find(r => String(r.id) === String(reportId));
        if (found && (found.status === 'error' || found.status === 'failed')) {
          return res.status(400).json({ error: 'Reporte falló', detail: found });
        }
        if (found?.status === 'processed' && found?.file_name) { fileName = found.file_name; break; }
        await new Promise(r => setTimeout(r, 5000));
      }
      if (!fileName) return res.status(202).json({ error: 'Reporte aún generándose', reportId, reintentar_con: `?reportId=${reportId}&source=${[...buscados].join(',')}` });
    }

    // Este parser es CSV. Si el reporte es XLSX, avisar (no intentar parsear binario).
    if (/\.xlsx?$/i.test(fileName)) {
      return res.status(400).json({ error: 'Ese reporte es XLSX; este endpoint parsea CSV. Elegí/generá uno en formato CSV.', file: fileName });
    }

    // 2. Descargar el archivo por nombre: GET /v1/account/settlement_report/{file_name}
    const csvRes = await mpApi(`/v1/account/settlement_report/${encodeURIComponent(fileName)}`);
    const csvText = await csvRes.text();
    const lines = csvText.split('\n').filter(l => l.trim());
    if (lines.length < 2) return res.status(400).json({ error: 'Reporte vacío' });

    // 4. Parsear CSV. El settlement de la API de MP viene separado por ';'
    //    (y los campos JSON TAXES_DISAGGREGATED/METADATA traen ',' adentro, por eso
    //    NO se puede splitear por coma). Autodetecta: ';' si está en el header, si no ','.
    //    Se puede forzar con ?sep=; o ?sep=,
    const sep = (req.query.sep && String(req.query.sep)) || (lines[0].includes(';') ? ';' : ',');
    const parseLine = (line) => {
      const out = []; let cur = '', q = false;
      for (const ch of line) {
        if (ch === '"') { q = !q; }
        else if (ch === sep && !q) { out.push(cur.trim()); cur = ''; }
        else { cur += ch; }
      }
      out.push(cur.trim());
      return out;
    };
    const clean = (v) => String(v == null ? '' : v).replace(/"/g, '').trim();
    const headers = parseLine(lines[0]).map(clean);
    const idxType = headers.indexOf('TRANSACTION_TYPE');
    const idxTax = headers.indexOf('TAXES_DISAGGREGATED');
    const toObj = (vals) => headers.reduce((o, h, idx) => (o[h] = clean(vals[idx]), o), {});

    // Parser del campo TAXES_DISAGGREGATED. NO es JSON válido (claves/valores sin
    // comillas): ej. [{financial_entity:caba,amount:-107.73,detail:tax_withholding_sirtac}, {...}]
    const parseTaxes = (raw) => {
      const out = [];
      const objs = String(raw || '').match(/\{[^}]*\}/g) || [];
      for (const o of objs) {
        const fe = (o.match(/financial_entity:([^,}]*)/) || [])[1];
        const am = (o.match(/amount:([^,}]*)/) || [])[1];
        const de = (o.match(/detail:([^,}]*)/) || [])[1];
        if ((fe && fe.trim()) || (de && de.trim())) {
          out.push({ financial_entity: (fe || '').trim(), amount: parseFloat(am) || 0, detail: (de || '').trim() });
        }
      }
      return out;
    };

    // Modo diagnóstico de tipos: ?types=1 → conteo por TRANSACTION_TYPE + una fila
    // de muestra de cada tipo que NO sea SETTLEMENT (ahí caen refunds/reversos).
    if (req.query.types) {
      const counts = {}; const ejemplos = {};
      for (let i = 1; i < lines.length; i++) {
        const vals = parseLine(lines[i]);
        const t = clean(vals[idxType]) || '(vacío)';
        counts[t] = (counts[t] || 0) + 1;
        if (t !== 'SETTLEMENT' && !ejemplos[t]) ejemplos[t] = toObj(vals);
      }
      return res.json({ file: fileName, sep, total_lines: lines.length - 1, tipos: counts, ejemplos_no_settlement: ejemplos });
    }

    // Modo mapa de impuestos: ?taxmap=1 → recorre TODO el archivo y arma
    // transaction_type → "detail | financial_entity" → { count, sum }.
    // Sirve para validar qué impuesto trae cada tipo (p.ej. si algún CASHBACK
    // trae IIBB, o solo imp. al cheque) sobre el universo completo, no por muestra.
    if (req.query.taxmap) {
      const map = {};
      const r2 = (n) => Math.round(n * 100) / 100;
      for (let i = 1; i < lines.length; i++) {
        const vals = parseLine(lines[i]);
        const tt = clean(vals[idxType]) || '(vacío)';
        const taxes = parseTaxes(vals[idxTax]);
        for (const t of taxes) {
          const key = `${t.detail} | ${t.financial_entity}`;
          map[tt] = map[tt] || {};
          map[tt][key] = map[tt][key] || { count: 0, sum: 0 };
          map[tt][key].count++;
          map[tt][key].sum = r2(map[tt][key].sum + t.amount);
        }
      }
      return res.json({ file: fileName, sep, total_lines: lines.length - 1, taxmap: map });
    }

    // 5. Filas que matcheen alguno de los ids (en cualquier columna)
    const matched = [];
    for (let i = 1; i < lines.length && matched.length < 40; i++) {
      const vals = parseLine(lines[i]);
      if (buscados.size && vals.some(v => buscados.has(clean(v)))) matched.push(toObj(vals));
    }

    // 6. Muestra de las primeras filas (para ver el formato general)
    const sample = lines.slice(1, 4).map(l => toObj(parseLine(l)));

    res.json({
      reportId,
      file: fileName,
      sep,
      rango: { desde, hasta },
      total_lines: lines.length - 1,
      headers,
      buscados: [...buscados],
      matched_count: matched.length,
      matched,
      sample
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// ── MERCADOPAGO — Ingesta de RETENCIONES IIBB desde el settlement (paso 2a) ──
// Ingiere un reporte YA generado (?file=NOMBRE.csv o ?reportId=NNN) a la tabla
// `retenciones`. Idempotente (sbUpsert on_conflict). NO toca por_cobrar ni la
// conciliación — solo puebla `retenciones`. Ver ADARA-RETENCIONES-IIBB.md.
//   ?dry=1 → no escribe; devuelve el resumen y el acumulado para verificar.
//
// Clasificación detail + financial_entity → tipo:
//   tax_withholding_sirtac + <prov>                → iibb_provincial (jur=prov)  [SIRTAC]
//   tax_withholding        + <prov>                → iibb_provincial (jur=prov)  [régimen propio: santa_fe, corrientes]
//   tax_withholding_collector + iibb_tucuman       → iibb_tucuman
//   tax_withholding_collector + debitos_creditos   → impuesto_cheque
//   tax_withholding_payout                         → payout
//   tax_withholding_payer*                         → IGNORAR (es del comprador)
// Excluye transaction_type CASHBACK / CASHBACK_CANCEL.
//   Por defecto NO escribe (dry-run / preview). Para escribir: agregar ?write=1
// ── RETENCIONES IIBB — estado + ingesta compartida (paso 2b) ─────────────────
let retencionesEstado = {
  estado: 'al_dia',          // al_dia | actualizando | error
  desde: null, hasta: null,
  ultimo_resumen: null,
  ultimo_ok: null,           // ISO del último OK
  error: null,
  corriendo: false,
  pending_report_id: null    // reporte generándose; se reanuda en la próxima sync
};

// Núcleo de la ingesta: descarga el CSV por file_name, parsea, clasifica,
// resuelve venta_id e (si write) hace upsert idempotente. Devuelve el resumen.
async function ingestRetenciones(fileName, write) {
  const csvRes = await mpApi(`/v1/account/settlement_report/${encodeURIComponent(fileName)}`);
  const csvText = await csvRes.text();
  const lines = csvText.split('\n').filter(l => l.trim());
  if (lines.length < 2) throw new Error('Reporte vacío');

  const sep = lines[0].includes(';') ? ';' : ',';
  const parseLine = (line) => {
    const out = []; let cur = '', q = false;
    for (const ch of line) {
      if (ch === '"') q = !q;
      else if (ch === sep && !q) { out.push(cur.trim()); cur = ''; }
      else cur += ch;
    }
    out.push(cur.trim());
    return out;
  };
  const clean = (v) => String(v == null ? '' : v).replace(/"/g, '').trim();
  const headers = parseLine(lines[0]).map(clean);
  const H = {}; headers.forEach((h, i) => H[h] = i);
  const get = (vals, name) => clean(vals[H[name]]);

  const parseTaxes = (raw) => {
    const out = [];
    const objs = String(raw || '').match(/\{[^}]*\}/g) || [];
    for (const o of objs) {
      const fe = (o.match(/financial_entity:([^,}]*)/) || [])[1];
      const am = (o.match(/amount:([^,}]*)/) || [])[1];
      const de = (o.match(/detail:([^,}]*)/) || [])[1];
      if ((fe && fe.trim()) || (de && de.trim())) {
        out.push({ financial_entity: (fe || '').trim(), amount: parseFloat(am) || 0, detail: (de || '').trim() });
      }
    }
    return out;
  };

  const r2 = (n) => Math.round(n * 100) / 100;
  const TIPOS_EXCLUIDOS = new Set(['CASHBACK', 'CASHBACK_CANCEL']);
  const clasificar = (detail, fe) => {
    if (detail.startsWith('tax_withholding_payer')) return null;                 // comprador → ignorar
    if (detail === 'tax_withholding_sirtac') return { tipo: 'iibb_provincial', jur: fe };
    if (detail === 'tax_withholding')        return { tipo: 'iibb_provincial', jur: fe };
    if (detail === 'tax_withholding_collector' && fe === 'iibb_tucuman')     return { tipo: 'iibb_tucuman',    jur: null };
    if (detail === 'tax_withholding_collector' && fe === 'debitos_creditos') return { tipo: 'impuesto_cheque', jur: null };
    if (detail === 'tax_withholding_payout') return { tipo: 'payout', jur: null };
    return undefined;                                                            // desconocido → reportar
  };

  const filas = [];
  const desconocidos = {};
  const acum = {};            // acumulado IIBB por provincia (NETO, todas las transaction_type)
  const sourceSet = new Set();
  const packSet = new Set();

  for (let i = 1; i < lines.length; i++) {
    const vals = parseLine(lines[i]);
    const tt = get(vals, 'TRANSACTION_TYPE');
    if (!tt || TIPOS_EXCLUIDOS.has(tt)) continue;
    const taxes = parseTaxes(get(vals, 'TAXES_DISAGGREGATED'));
    if (!taxes.length) continue;
    const sourceId = get(vals, 'SOURCE_ID');
    const orderId  = get(vals, 'ORDER_ID');
    const packId   = get(vals, 'PACK_ID');
    const fecha = (get(vals, 'MONEY_RELEASE_DATE') || get(vals, 'SETTLEMENT_DATE') || get(vals, 'TRANSACTION_DATE') || '').substring(0, 10) || null;
    for (const t of taxes) {
      const cls = clasificar(t.detail, t.financial_entity);
      if (cls === null) continue;
      if (cls === undefined) { const k = `${t.detail} | ${t.financial_entity}`; desconocidos[k] = (desconocidos[k] || 0) + 1; continue; }
      filas.push({
        mp_source_id: sourceId || null, transaction_type: tt, tipo: cls.tipo, jurisdiccion: cls.jur,
        detail: t.detail, financial_entity: t.financial_entity, monto: r2(t.amount),
        venta_id: null, order_id: orderId || null, pack_id: packId || null, fecha, settlement_file: fileName
      });
      if (cls.tipo === 'iibb_provincial') acum[cls.jur] = r2((acum[cls.jur] || 0) + t.amount);
      if (sourceId) sourceSet.add(sourceId);
      if (packId)   packSet.add(packId);
    }
  }

  const chunk = (arr, n) => { const o = []; for (let i = 0; i < arr.length; i += n) o.push(arr.slice(i, i + n)); return o; };
  const ventaPorSource = {};
  const ventaPorPack = {};
  for (const grp of chunk([...sourceSet], 150)) {
    const rows = await sbGet('ventas_ml', `select=id,mp_payment_id&mp_payment_id=in.(${grp.join(',')})`).catch(() => []);
    for (const v of (rows || [])) if (v.mp_payment_id) ventaPorSource[String(v.mp_payment_id)] = v.id;
  }
  const sinSource = [...sourceSet].filter(s => !ventaPorSource[s]);
  for (const s of sinSource) {
    const rows = await sbGet('ventas_ml', `select=id&mp_payment_ids=like.*${s}*&limit=1`).catch(() => []);
    if (rows && rows[0]) ventaPorSource[s] = rows[0].id;
  }
  for (const grp of chunk([...packSet], 150)) {
    const rows = await sbGet('ventas_ml', `select=id,pack_id&pack_id=in.(${grp.join(',')})`).catch(() => []);
    for (const v of (rows || [])) if (v.pack_id) ventaPorPack[String(v.pack_id)] = v.id;
  }
  let conMatch = 0, sinMatch = 0;
  for (const f of filas) {
    const vid = ventaPorSource[f.mp_source_id] || (f.pack_id ? ventaPorPack[f.pack_id] : null) || null;
    f.venta_id = vid;
    if (vid) conMatch++; else sinMatch++;
  }
  let upserted = 0;
  if (write) {
    for (const grp of chunk(filas, 200)) {
      const r = await sbUpsert('retenciones', grp, 'mp_source_id,transaction_type,detail,financial_entity');
      upserted += Array.isArray(r) ? r.length : grp.length;
    }
  }
  return {
    file: fileName, total_lines: lines.length - 1, retenciones_armadas: filas.length, upserted,
    venta_con_match: conMatch, venta_sin_match: sinMatch, desconocidos, acumulado_iibb_por_provincia_neto: acum
  };
}

// Núcleo del background: si hay un reporte pendiente lo retoma; si no, crea uno
// para [mes anterior → hoy]. Poll largo (~12 min). Si MP todavía no terminó,
// deja el reporte como pendiente (se reanuda en la próxima sync, sin regenerar).
// Devuelve { ok:true, resumen } si ingirió, o { ok:false } si sigue generándose.
async function correrRetenciones() {
  let reportId = retencionesEstado.pending_report_id;

  if (!reportId) {
    const hoy = new Date();
    const hasta = hoy.toISOString().substring(0, 10);
    const desde = new Date(hoy.getFullYear(), hoy.getMonth() - 1, 1).toISOString().substring(0, 10);  // 1° del mes anterior
    const createRes = await mpApi('/v1/account/settlement_report', {
      method: 'POST',
      body: JSON.stringify({ begin_date: `${desde}T00:00:00Z`, end_date: `${hasta}T23:59:59Z` })
    });
    const createData = await createRes.json();
    reportId = createData.id;
    if (!reportId) throw new Error('No se pudo crear el reporte de settlement');
    retencionesEstado.pending_report_id = String(reportId);
    retencionesEstado.desde = desde;
    retencionesEstado.hasta = hasta;
  }

  // Poll del listado hasta ~12 min (48 × 15s)
  let fileName = null;
  for (let i = 0; i < 48; i++) {
    const listRes = await mpApi('/v1/account/settlement_report/list');
    const arr = await listRes.json();
    const list = Array.isArray(arr) ? arr : [];
    const found = list.find(r => String(r.id) === String(reportId));
    if (found && (found.status === 'error' || found.status === 'failed')) {
      retencionesEstado.pending_report_id = null;
      throw new Error(`El reporte ${reportId} falló`);
    }
    if (found?.status === 'processed' && found?.file_name) { fileName = found.file_name; break; }
    await new Promise(r => setTimeout(r, 15000));
  }

  if (!fileName) return { ok: false };          // sigue generándose → queda pendiente
  if (/\.xlsx?$/i.test(fileName)) {
    retencionesEstado.pending_report_id = null;
    throw new Error('El reporte salió XLSX, se esperaba CSV');
  }

  const resumen = await ingestRetenciones(fileName, true);
  retencionesEstado.pending_report_id = null;
  return { ok: true, resumen };
}

// Dispara la ingesta en segundo plano (sin bloquear). Si hay un reporte pendiente,
// lo retoma (sin throttle). Si no, aplica throttle de 6 h antes de generar uno nuevo.
function kickRetenciones() {
  if (retencionesEstado.corriendo) return;
  if (!ML.access) return;
  if (!retencionesEstado.pending_report_id) {
    const ahora = Date.now();
    if (retencionesEstado.ultimo_ok && (ahora - new Date(retencionesEstado.ultimo_ok).getTime()) < 6 * 3600 * 1000) return;
  }

  retencionesEstado.corriendo = true;
  retencionesEstado.estado = 'actualizando';
  retencionesEstado.error = null;

  correrRetenciones()
    .then((r) => {
      if (r.ok) {
        retencionesEstado.estado = 'al_dia';
        retencionesEstado.ultimo_resumen = r.resumen;
        retencionesEstado.ultimo_ok = new Date().toISOString();
        console.log(`✓ Retenciones (bg): ${r.resumen.upserted} filas (${retencionesEstado.desde} → ${retencionesEstado.hasta})`);
      } else {
        // Sigue generándose: queda pendiente, se reanuda en la próxima sync.
        retencionesEstado.estado = 'actualizando';
        console.log(`… Retenciones (bg): reporte ${retencionesEstado.pending_report_id} aún generándose; se reanuda en la próxima sync`);
      }
    })
    .catch((e) => {
      retencionesEstado.estado = 'error';
      retencionesEstado.error = e.message;
      console.error('Retenciones (bg):', e.message);
    })
    .finally(() => { retencionesEstado.corriendo = false; });
}

// Endpoint manual / diagnóstico de la ingesta (paso 2a). Por defecto dry-run.
//   ?file=NOMBRE.csv | ?reportId=NNN   ·   escribir: &write=1
app.get('/mp/retenciones-sync', async (req, res) => {
  try {
    if (!ML.access) return res.status(401).json({ error: 'ML no autenticado' });
    const fileParam     = String(req.query.file     || req.body?.file     || '').trim();
    const reportIdParam = String(req.query.reportId || req.body?.reportId || '').trim();
    const dry = !(req.query.write || req.body?.write);

    let fileName = fileParam || null;
    if (!fileName) {
      if (!reportIdParam) return res.status(400).json({ error: 'Pasá ?file=NOMBRE.csv o ?reportId=NNN' });
      const listRes = await mpApi('/v1/account/settlement_report/list');
      const arr = await listRes.json();
      const list = Array.isArray(arr) ? arr : [];
      const found = list.find(r => String(r.id) === String(reportIdParam));
      if (!found) return res.status(404).json({ error: 'reportId no encontrado', reportId: reportIdParam });
      if (found.status !== 'processed' || !found.file_name) return res.status(202).json({ error: 'Reporte aún no procesado', reportId: reportIdParam, status: found.status });
      fileName = found.file_name;
    }
    if (/\.xlsx?$/i.test(fileName)) return res.status(400).json({ error: 'El reporte es XLSX; necesito CSV', file: fileName });

    const out = await ingestRetenciones(fileName, !dry);
    res.json({ ok: true, dry, ...out });
  } catch (e) {
    console.error('retenciones-sync:', e);
    res.status(500).json({ error: e.message });
  }
});

// Estado de la ingesta en background (para el indicador "al día / actualizando")
app.get('/mp/retenciones-estado', (_, res) => res.json(retencionesEstado));

// Dispara/reanuda la ingesta en background a demanda (respeta throttle y pendiente).
app.get('/mp/retenciones-refresh', (_, res) => {
  kickRetenciones();
  res.json({ ok: true, ...retencionesEstado });
});

app.post('/mp/settlement-sync', async (req, res) => {
  try {
    if (!ML.access) return res.status(401).json({ error: 'ML no autenticado' });

    const { desde, hasta } = req.body || {};
    if (!desde || !hasta) return res.status(400).json({ error: 'Faltan desde/hasta (YYYY-MM-DD)' });

    console.log(`MP Settlement: generando reporte ${desde} → ${hasta}...`);

    // 1. Crear reporte
    const createRes = await mpApi('/v1/account/settlement_report', {
      method: 'POST',
      body: JSON.stringify({
        begin_date: `${desde}T00:00:00Z`,
        end_date: `${hasta}T23:59:59Z`
      })
    });
    const createData = await createRes.json();
    const reportId = createData.id;
    if (!reportId) return res.status(400).json({ error: 'No se pudo crear reporte', detail: createData });

    console.log(`MP Settlement: reporte creado id=${reportId}, esperando...`);

    // 2. Esperar a que esté listo (polling cada 5s, máx 2 min)
    let fileUrl = null;
    for (let i = 0; i < 24; i++) {
      await new Promise(r => setTimeout(r, 5000));
      try {
        const checkRes = await mpApi(`/v1/account/settlement_report/${reportId}`);
        const checkData = await checkRes.json();
        if (checkData.status === 'ready' && checkData.download_url) {
          fileUrl = checkData.download_url;
          break;
        }
        if (checkData.status === 'error') {
          return res.status(400).json({ error: 'Reporte falló', detail: checkData });
        }
        console.log(`MP Settlement: esperando... (${i+1}) status=${checkData.status}`);
      } catch(e) {
        // Some APIs return the file list differently
        break;
      }
    }

    // Alternative: list recent reports to find the file
    if (!fileUrl) {
      const listRes = await mpApi('/v1/account/settlement_report/list');
      const list = await listRes.json();
      const found = list.find(r => r.id === reportId) || list[0];
      if (found?.download_url) fileUrl = found.download_url;
    }

    if (!fileUrl) return res.status(400).json({ error: 'Reporte no disponible aún, intentar de nuevo en unos minutos', reportId });

    // 3. Descargar CSV
    console.log(`MP Settlement: descargando ${fileUrl}...`);
    const csvRes = await fetch(fileUrl, {
      headers: { 'Authorization': 'Bearer ' + ML.access }
    });
    const csvText = await csvRes.text();
    const lines = csvText.split('\n').filter(l => l.trim());
    if (lines.length < 2) return res.status(400).json({ error: 'Reporte vacío' });

    // 4. Parsear CSV
    const headers = lines[0].split(',').map(h => h.trim().replace(/"/g, ''));
    const col = (name) => headers.indexOf(name);

    const iSourceId = col('SOURCE_ID');
    const iType = col('TRANSACTION_TYPE');
    const iAmount = col('TRANSACTION_AMOUNT');
    const iNetAmount = col('SETTLEMENT_NET_AMOUNT');
    const iFee = col('FEE_AMOUNT');
    const iMkpFee = col('MKP_FEE_AMOUNT');
    const iFinancing = col('FINANCING_FEE_AMOUNT');
    const iShipping = col('SHIPPING_FEE_AMOUNT');
    const iTaxes = col('TAXES_AMOUNT');
    const iTaxDetail = col('TAXES_DISAGGREGATED');

    const parseNum = (v) => {
      if (!v) return 0;
      const s = String(v).replace(/"/g, '').trim();
      return parseFloat(s) || 0;
    };

    let updated = 0, skipped = 0;

    for (let i = 1; i < lines.length; i++) {
      // CSV parse (handle quoted values with commas)
      const vals = [];
      let current = '', inQuotes = false;
      for (const ch of lines[i]) {
        if (ch === '"') { inQuotes = !inQuotes; }
        else if (ch === ',' && !inQuotes) { vals.push(current.trim()); current = ''; }
        else { current += ch; }
      }
      vals.push(current.trim());

      const tipo = (vals[iType] || '').replace(/"/g, '');
      // Solo procesar pagos de ventas (no transferencias, etc.)
      if (!tipo.includes('SETTLEMENT') && !tipo.includes('payment')) continue;

      const sourceId = (vals[iSourceId] || '').replace(/"/g, '').trim();
      if (!sourceId) continue;

      const fee = parseNum(vals[iFee]);
      const mkpFee = parseNum(vals[iMkpFee]);
      const financing = parseNum(vals[iFinancing]);
      const shipping = parseNum(vals[iShipping]);
      const taxes = parseNum(vals[iTaxes]);
      const netAmount = parseNum(vals[iNetAmount]);
      const taxDetail = (vals[iTaxDetail] || '').replace(/"/g, '');

      // Comisión = fee + mkp_fee (son complementarios)
      const comisionTotal = Math.abs(fee) + Math.abs(mkpFee);

      // Buscar venta por mp_payment_id = sourceId (o en mp_payment_ids para split payments)
      let ventas = await sbGet('ventas_ml', `mp_payment_id=eq.${sourceId}&limit=1`);
      if (!ventas?.length) {
        // Fallback: buscar en split payments
        ventas = await sbGet('ventas_ml', `mp_payment_ids=like.*${sourceId}*&limit=1`).catch(() => []);
      }
      if (!ventas?.length) { skipped++; continue; }

      // Actualizar con desglose real
      // Para split payments (pagos_cantidad > 1), los fees vienen en líneas separadas
      // del CSV → hay que ACUMULAR en vez de sobrescribir
      const isSplit = ventas[0].pagos_cantidad > 1;
      const existing = ventas[0];

      // por_cobrar correcto = lo que ML realmente paga en el Account Statement
      // SETTLEMENT_NET_AMOUNT ya descuenta el financing (incluye add_on informativo),
      // pero el Account Statement paga sin ese descuento.
      // Fix: revertir el descuento del financing para obtener el neto real pagado.
      // Aplica tanto a add_on informativo (no cobrado) como a financing real (cuotas):
      // - add_on informativo: financing en Settlement > 0, pero AS paga sin descontarlo → revertir ✅
      // - financing real (cuotas): el sync normal ya lo excluye de por_cobrar → consistente ✅
      const porCobrarReal = netAmount > 0
        ? Math.round((netAmount + Math.abs(financing)) * 100) / 100
        : 0;

      let updateData;
      if (isSplit && existing.mp_payment_id !== sourceId) {
        // Es el segundo (o N-ésimo) pago del split → acumular sobre lo existente
        updateData = {
          cargo_venta: comisionTotal
            ? (existing.cargo_venta || 0) + (-Math.abs(comisionTotal))
            : existing.cargo_venta,
          cargo_envio: shipping
            ? (existing.cargo_envio || 0) + (-Math.abs(shipping))
            : existing.cargo_envio,
          costo_financiero: financing
            ? (existing.costo_financiero || 0) + (-Math.abs(financing))
            : existing.costo_financiero,
          impuestos: taxes
            ? (existing.impuestos || 0) + (-Math.abs(taxes))
            : existing.impuestos,
          por_cobrar: porCobrarReal
            ? (existing.por_cobrar || 0) + porCobrarReal
            : existing.por_cobrar,
        };
      } else {
        // Pago único o primer pago del split → sobrescribir como antes
        updateData = {
          cargo_venta: comisionTotal ? -Math.abs(comisionTotal) : existing.cargo_venta,
          cargo_envio: shipping ? -Math.abs(shipping) : existing.cargo_envio,
          costo_financiero: financing ? -Math.abs(financing) : existing.costo_financiero,
          impuestos: taxes ? -Math.abs(taxes) : existing.impuestos,
          por_cobrar: porCobrarReal || existing.por_cobrar,
        };
      }

      // Si la venta no estaba conciliada, marcarla
      if (netAmount > 0 && !ventas[0].conciliado) {
        updateData.conciliado = true;
        updateData.fecha_cobro = new Date().toISOString().split('T')[0];
      }

      await sbPatch('ventas_ml', `id=eq.${ventas[0].id}`, updateData);
      updated++;
    }

    console.log(`✓ MP Settlement: ${updated} ventas actualizadas, ${skipped} no encontradas`);
    res.json({ ok: true, updated, skipped, total_lines: lines.length - 1 });

  } catch(e) {
    console.error('MP Settlement:', e);
    res.status(500).json({ error: e.message });
  }
});

// ── MERCADO PAGO — Parseo extracto XLSX ─────────────────────────────
app.post('/mp/extracto', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'Falta el archivo' });

    const wb   = XLSX.read(req.file.buffer, { type: 'buffer', cellDates: true });
    const ws   = wb.Sheets[wb.SheetNames[0]];
    
    // ─── Detectar formato del archivo ─────────────────────────────
    // MP tiene múltiples formatos de reporte. Buscamos la fila de headers.
    const allRows = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null });
    if (!allRows.length) return res.status(400).json({ error: 'El archivo está vacío' });

    // Buscar la fila que contiene los headers reales
    let headerRow = -1;
    let headerCols = {};
    
    for (let i = 0; i < Math.min(allRows.length, 15); i++) {
      const row = (allRows[i] || []).map(c => String(c || '').trim().toUpperCase());
      const joined = row.join('|');
      
      // Formato Account Statement: RELEASE_DATE, TRANSACTION_TYPE, REFERENCE_ID, TRANSACTION_NET_AMOUNT
      if (joined.includes('RELEASE_DATE') || joined.includes('REFERENCE_ID')) {
        headerRow = i;
        headerCols = {
          fecha: row.findIndex(c => c.includes('RELEASE_DATE') || c.includes('DATE')),
          tipo:  row.findIndex(c => c.includes('TRANSACTION_TYPE') || c.includes('TYPE')),
          ref:   row.findIndex(c => c.includes('REFERENCE_ID') || c.includes('REFERENCE')),
          neto:  row.findIndex(c => c.includes('NET_AMOUNT') || c.includes('TRANSACTION_NET')),
          saldo: row.findIndex(c => c.includes('BALANCE') || c.includes('PARTIAL')),
        };
        headerCols.format = 'account_statement';
        break;
      }
      
      // Formato detallado viejo: tiene columnas como descripción, bruto, comisión
      if (joined.includes('FECHA') && (joined.includes('MONTO') || joined.includes('DESCRIPCI'))) {
        headerRow = i;
        headerCols = {
          fecha: row.findIndex(c => c.includes('FECHA') || c.includes('DATE')),
          tipo:  row.findIndex(c => c.includes('TIPO') || c.includes('OPERACI')),
          desc:  row.findIndex(c => c.includes('DESCRIPCI') || c.includes('CONCEPTO') || c.includes('DETALLE')),
          neto:  row.findIndex(c => c.includes('MONTO') || c.includes('NETO') || c.includes('AMOUNT')),
          bruto: row.findIndex(c => c.includes('BRUTO') || c.includes('INGRESADO') || c.includes('GROSS')),
          com:   row.findIndex(c => c.includes('COMISI') || c.includes('COMMISSION') || c.includes('CARGO')),
          ref:   row.findIndex(c => c.includes('REFERENCIA') || c.includes('NRO DE REF') || c.includes('ID OPERAC') || c.includes('REFERENCE')),
          estado:row.findIndex(c => c.includes('ESTADO') || c.includes('STATUS')),
        };
        headerCols.format = 'detailed';
        break;
      }
    }

    if (headerRow < 0) {
      return res.status(400).json({ 
        error: 'No reconozco el formato del archivo. Headers encontrados: ' + 
               JSON.stringify(allRows.slice(0, 5).map(r => (r||[]).slice(0,5)))
      });
    }

    console.log(`MP extracto: formato=${headerCols.format}, headerRow=${headerRow}, cols=`, headerCols);

    // ─── Parsear números argentinos (23.142,34 → 23142.34) ────────
    const parseARS = (v) => {
      if (v == null) return 0;
      if (typeof v === 'number') return v;
      const s = String(v).replace(/\s/g, '').replace(/\./g, '').replace(',', '.');
      return parseFloat(s) || 0;
    };

    // ─── Parsear fecha (DD-MM-YYYY o DD/MM/YYYY → YYYY-MM-DD) ────
    const parseFecha = (v) => {
      if (!v) return null;
      if (v instanceof Date) return v.toISOString().split('T')[0];
      const s = String(v).trim();
      // DD-MM-YYYY or DD/MM/YYYY
      const m = s.match(/^(\d{2})[-\/](\d{2})[-\/](\d{4})/);
      if (m) return `${m[3]}-${m[2]}-${m[1]}`;
      // YYYY-MM-DD
      const m2 = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
      if (m2) return `${m2[1]}-${m2[2]}-${m2[3]}`;
      // Excel serial number
      if (typeof v === 'number' && v > 40000) {
        return new Date(Math.round((v - 25569) * 86400000)).toISOString().split('T')[0];
      }
      return s.substring(0, 10);
    };

    // ─── Categorizar tipo de movimiento ───────────────────────────
    const categorizarTipo = (tipo) => {
      const t = (tipo || '').toLowerCase();
      if (t.includes('liquidación de dinero') && !t.includes('cancelada'))  return 'venta_ml';
      if (t.includes('liquidación') && t.includes('cancelada'))             return 'venta_cancelada';
      if (t.includes('bonificación') && t.includes('cancelada'))            return 'bonificacion_cancelada';
      if (t.includes('bonificación'))                                       return 'bonificacion_envio';
      // Cargo envío devolución ANTES del check genérico de devolución
      if (t.includes('cargo') && t.includes('devolución'))                  return 'cargo_envio_devolucion';
      if (t.includes('devolución'))                                         return 'devolucion';
      if (t.includes('transferencia enviada') || t.includes('transferencia programada')) return 'transferencia_salida';
      if (t.includes('transferencia recibida') || t.includes('entrada'))    return 'transferencia_entrada';
      if (t.includes('débito'))                                             return 'debito';
      if (t.includes('rendimiento'))                                        return 'rendimiento';
      if (t.includes('impuesto'))                                           return 'impuesto';
      if (t.includes('dinero retenido'))                                    return 'retencion';
      if (t.includes('dinero recibido'))                                    return 'dinero_recibido';
      if (t.includes('pago') || t.includes('compra'))                       return 'pago';
      return 'otro';
    };

    // ─── Iterar filas de datos ────────────────────────────────────
    const movs = [];
    for (let i = headerRow + 1; i < allRows.length; i++) {
      const row = allRows[i] || [];
      
      const fecha = parseFecha(row[headerCols.fecha]);
      if (!fecha) continue; // skip empty rows
      
      const tipo = String(row[headerCols.tipo] || '').trim();
      if (!tipo) continue;
      
      const ref  = String(row[headerCols.ref] || '').trim();
      const neto = parseARS(row[headerCols.neto]);
      const categoria = categorizarTipo(tipo);

      // Para formato account_statement, no hay bruto/comisión separados
      const bruto = headerCols.format === 'detailed' && headerCols.bruto >= 0
        ? parseARS(row[headerCols.bruto]) : null;
      const com = headerCols.format === 'detailed' && headerCols.com >= 0
        ? Math.abs(parseARS(row[headerCols.com])) : 0;

      movs.push({
        fecha,
        descripcion: tipo,  // en account_statement el tipo ES la descripción
        tipo_operacion: categoria,
        categoria,
        monto_bruto: bruto,
        comision: com,
        monto_neto: neto,
        estado: 'approved',
        referencia_mp: ref || null,
        conciliado: false,
        posicion: movs.length + 1
      });
    }

    console.log(`MP extracto: ${movs.length} movimientos parseados, ${movs.filter(m => m.categoria === 'venta_ml').length} liquidaciones`);

    if (!movs.length) {
      return res.status(400).json({ error: 'No se encontraron movimientos válidos en el archivo' });
    }

    // ─── Auto-asignar línea de negocio ML a categorías ML ─────────
    const ML_CATS = ['venta_ml', 'bonificacion_envio', 'devolucion', 'venta_cancelada', 'cargo_envio_devolucion'];
    let mlLineaId = null;
    try {
      const lineas = await sbGet('lineas_negocio', 'nombre=ilike.*mercado libre*&limit=1');
      if (lineas?.length) mlLineaId = lineas[0].id;
    } catch(e) { console.warn('No se pudo obtener línea ML:', e.message); }

    if (mlLineaId) {
      for (const m of movs) {
        if (ML_CATS.includes(m.categoria)) m.linea_negocio_id = mlLineaId;
      }
      console.log(`MP extracto: línea ML ${mlLineaId} asignada a ${movs.filter(m => m.linea_negocio_id).length} movimientos`);
    }

    // ─── Insertar en Supabase ─────────────────────────────────────
    // Primero limpiamos movimientos previos del mismo período para evitar duplicados
    const fechaMin = movs.reduce((min, m) => m.fecha < min ? m.fecha : min, movs[0].fecha);
    const fechaMax = movs.reduce((max, m) => m.fecha > max ? m.fecha : max, movs[0].fecha);
    
    // Resetear estado de conciliación SOLO de ventas que pierden sus movimientos
    // Al borrar movimientos del rango, algunas ventas quedan con links huérfanos
    // Resetear solo esas, no todo el período (para no romper conciliaciones de extractos anteriores)
    
    // 1. ANTES de borrar: guardar los venta_ml_id de movimientos que se van a eliminar
    let ventasAResetear = [];
    try {
      let rOffset = 0;
      while (true) {
        const page = await sbGet('movimientos_mp', 
          `fecha=gte.${fechaMin}&fecha=lte.${fechaMax}&venta_ml_id=not.is.null&select=venta_ml_id&limit=1000&offset=${rOffset}&order=id`
        );
        if (!page?.length) break;
        for (const m of page) { if (m.venta_ml_id) ventasAResetear.push(m.venta_ml_id); }
        if (page.length < 1000) break;
        rOffset += 1000;
      }
      ventasAResetear = [...new Set(ventasAResetear)];
      console.log(`MP extracto: ${ventasAResetear.length} ventas vinculadas a movimientos que se van a borrar`);
    } catch(e) { console.warn('Pre-reset query:', e.message); }

    // 2. Borrar movimientos previos del rango
    try {
      await sb('DELETE', 'movimientos_mp', null, `fecha=gte.${fechaMin}&fecha=lte.${fechaMax}`);
      console.log(`MP extracto: borrados movimientos previos ${fechaMin} → ${fechaMax}`);
    } catch(e) { console.warn('Delete previos:', e.message); }

    // 3. Resetear SOLO las ventas afectadas (de cualquier período)
    if (ventasAResetear.length) {
      const resetData = {
        estado_conciliacion: 'pendiente',
        conciliado: false,
        balance_conciliacion: 0,
        devuelta: false,
        monto_reembolso: 0,
        cargo_envio_devolucion: 0
      };
      for (let i = 0; i < ventasAResetear.length; i += 50) {
        const ids = ventasAResetear.slice(i, i + 50).join(',');
        await sbPatch('ventas_ml', `id=in.(${ids})`, resetData).catch(e => {
          console.warn('Reset ventas afectadas:', e.message);
        });
      }
      console.log(`MP extracto: reset ${ventasAResetear.length} ventas afectadas`);
    }

    // Normalizar keys (PGRST102: all object keys must match)
    const allKeys = new Set();
    for (const m of movs) { for (const k of Object.keys(m)) allKeys.add(k); }
    for (const m of movs) { for (const k of allKeys) { if (!(k in m)) m[k] = null; } }

    // Insertar todos
    let insertados = 0;
    for (let i = 0; i < movs.length; i += 100) {
      const batch = movs.slice(i, i + 100);
      await sb('POST', 'movimientos_mp', batch);
      insertados += batch.length;
    }

    // NO auto-conciliar acá (tarda demasiado con miles de registros y hace timeout)
    // El usuario ejecuta la conciliación por separado con POST /mp/conciliar

    res.json({ 
      ok: true, 
      formato: headerCols.format,
      total: movs.length, 
      liquidaciones: movs.filter(m => m.categoria === 'venta_ml').length,
      insertados, 
      periodo: `${fechaMin} → ${fechaMax}`,
      ventas_reseteadas: ventasAResetear.length
    });
  } catch (e) {
    console.error('MP extracto:', e);
    res.status(500).json({ error: e.message });
  }
});

async function autoConciliarMP(fechaDesde = null, fechaHasta = null) {
  // ─── V2: vincular TODO movimiento que matchee por referencia con un payment_id ──
  // Ya no filtramos por categoría — si la referencia coincide con un payment_id, se vincula.
  // Esto captura: liquidaciones, devoluciones, débitos por reclamo, reintegros, cargo envío, etc.
  // Solo quedan afuera: bonificaciones (otra referencia → segundo pase por monto) y movimientos sin referencia.
  
  // Solo movimientos sin venta vinculada del rango del AS actual (no pisar links manuales)
  // Filtrar por rango de fechas evita procesar movimientos huérfanos de meses anteriores
  // que podrían asignarse incorrectamente a ventas del mes nuevo.
  // Paginado para evitar límite de 1000 rows de Supabase
  let movs = [];
  let movOffset = 0;
  let baseFilter = `conciliado=eq.false&venta_ml_id=is.null`;
  if (fechaDesde && fechaHasta) {
    baseFilter += `&fecha=gte.${fechaDesde}&fecha=lte.${fechaHasta}`;
  }
  while (true) {
    const page = await sbGet('movimientos_mp', `${baseFilter}&limit=1000&offset=${movOffset}&order=id`);
    if (!page?.length) break;
    movs.push(...page);
    if (page.length < 1000) break;
    movOffset += 1000;
  }
  console.log(`  → ${movs.length} movimientos sin conciliar cargados${fechaDesde ? ` (${fechaDesde} → ${fechaHasta})` : ' (todos los meses)'}`);
  
  // Traer TODAS las ventas (para matchear devoluciones de ventas ya conciliadas)
  // Supabase puede limitar a 1000 por default → paginamos si es necesario
  let allVentas = [];
  let offset = 0;
  const VPS = 1000; // ventas per page
  while (true) {
    const page = await sbGet('ventas_ml', `select=id,mp_payment_id,mp_payment_ids,conciliado,devuelta,linea_negocio_id,por_cobrar,ml_status,fecha_entrega&limit=${VPS}&offset=${offset}&order=id`);
    if (!page?.length) break;
    allVentas.push(...page);
    if (page.length < VPS) break;
    offset += VPS;
  }
  console.log(`  → ${allVentas.length} ventas cargadas para matching`);

  let nMovs = 0, nVentas = 0, nDevueltas = 0, nBonif = 0, nOtrosVinculados = 0;

  // Lookup línea ML
  let mlLineaId = null;
  try {
    const lineas = await sbGet('lineas_negocio', 'nombre=ilike.*mercado libre*&limit=1');
    if (lineas?.length) mlLineaId = lineas[0].id;
  } catch(e) {}

  // Índice de ventas por payment_id
  const ventasByPayment = {};
  for (const v of (allVentas || [])) {
    if (v.mp_payment_id) {
      if (!ventasByPayment[v.mp_payment_id]) ventasByPayment[v.mp_payment_id] = [];
      ventasByPayment[v.mp_payment_id].push(v);
    }
    if (v.mp_payment_ids) {
      for (const pid of v.mp_payment_ids.split(',')) {
        const trimmed = pid.trim();
        if (trimmed) {
          if (!ventasByPayment[trimmed]) ventasByPayment[trimmed] = [];
          ventasByPayment[trimmed].push(v);
        }
      }
    }
  }

  // ─── Matching: vincular movimientos a ventas por referencia ────────
  const movToVenta = [];          // {mov_id, venta_id, linea_id}
  const ventaIdsToConc = new Set();
  const ventaDevuelta = {};       // venta_id → monto acumulado
  const ventaCargoEnvio = {};     // venta_id → monto
  const ventasTocadas = new Set(); // para recalcular balance después

  for (const mov of (movs || [])) {
    if (!mov.referencia_mp) continue;
    const matches = ventasByPayment[mov.referencia_mp] || [];
    if (!matches.length) continue;

    const venta = matches[0];
    const lineaId = venta.linea_negocio_id || mlLineaId || null;
    movToVenta.push({ mov_id: mov.id, venta_id: venta.id, linea_id: lineaId });
    ventasTocadas.add(venta.id);

    if (mov.categoria === 'cargo_envio_devolucion') {
      ventaCargoEnvio[venta.id] = (ventaCargoEnvio[venta.id] || 0) + (mov.monto_neto || 0);
    } else if (['devolucion', 'venta_cancelada'].includes(mov.categoria)) {
      ventaDevuelta[venta.id] = (ventaDevuelta[venta.id] || 0) + Math.abs(mov.monto_neto || 0);
      nDevueltas++;
    } else if (mov.categoria === 'bonificacion_envio') {
      nBonif++;
    } else if (mov.categoria === 'venta_ml') {
      if (!venta.conciliado && !ventaIdsToConc.has(venta.id)) {
        ventaIdsToConc.add(venta.id);
        nVentas++;
      }
    } else {
      // Débitos, reintegros, u otros movimientos vinculados por referencia
      nOtrosVinculados++;
    }
    nMovs++;
  }

  // ─── Helper: ejecutar promises en paralelo con límite de concurrencia ──
  const parallel = async (tasks, concurrency = 15) => {
    const results = [];
    for (let i = 0; i < tasks.length; i += concurrency) {
      const batch = tasks.slice(i, i + concurrency);
      const batchResults = await Promise.allSettled(batch.map(fn => fn()));
      results.push(...batchResults);
    }
    return results;
  };

  // ─── Batch update movimientos_mp: conciliado + venta_ml_id ────────
  const movGroups = {};
  for (const m of movToVenta) {
    const key = `${m.venta_id}|${m.linea_id || ''}`;
    if (!movGroups[key]) movGroups[key] = { venta_id: m.venta_id, linea_id: m.linea_id, ids: [] };
    movGroups[key].ids.push(m.mov_id);
  }
  const movTasks = [];
  for (const g of Object.values(movGroups)) {
    const patchData = { conciliado: true, venta_ml_id: g.venta_id };
    if (g.linea_id) patchData.linea_negocio_id = g.linea_id;
    for (let i = 0; i < g.ids.length; i += 50) {
      const chunk = g.ids.slice(i, i + 50).join(',');
      movTasks.push(() => sbPatch('movimientos_mp', `id=in.(${chunk})`, patchData).catch(e => {
        console.warn('Batch patch movimientos_mp:', e.message);
      }));
    }
  }
  await parallel(movTasks);
  console.log(`  → ${movTasks.length} patches movimientos_mp`);

  // ─── Marcar devueltas + cargo envío ─────────────────────────────────
  const devTasks = [];
  for (const [ventaId, monto] of Object.entries(ventaDevuelta)) {
    devTasks.push(() => sbPatch('ventas_ml', `id=eq.${ventaId}`, { 
      devuelta: true, monto_reembolso: monto
    }).catch(e => console.warn('Patch devuelta:', e.message)));
  }
  for (const [ventaId, monto] of Object.entries(ventaCargoEnvio)) {
    devTasks.push(() => sbPatch('ventas_ml', `id=eq.${ventaId}`, { 
      cargo_envio_devolucion: monto 
    }).catch(e => console.warn('Patch cargo_envio:', e.message)));
  }
  await parallel(devTasks);
  console.log(`  → ${devTasks.length} patches devueltas+cargo`);

  // ─── SEGUNDO PASE: bonificaciones por posición en AS + fallback por monto ──
  // Las bonificaciones Flex no usan payment_id como referencia.
  // En el Account Statement, la bonificación aparece justo después de su liquidación.
  // Paso A: matching por posición (determinístico, sin ambigüedad)
  // Paso B: fallback por monto neto para las que no matchearon por posición
  let nBonifPos = 0, nBonifMonto = 0;
  try {
    // 1. Bonificaciones sin vincular: filtrar desde movs (batch actual) para evitar
    //    procesar bonificaciones de meses anteriores que quedaron sin matchear en la tabla.
    //    movs ya tiene todos los campos necesarios (id, monto_neto, posicion, categoria).
    let bonifSinVinc = movs.filter(m => m.categoria === 'bonificacion_envio');
    
    if (bonifSinVinc.length) {
      // IMPORTANTE: bonifSinVinc se construye desde movs (el batch actual), NO con query independiente.
      // Esto evita procesar bonificaciones viejas de meses anteriores que quedaron sin matchear
      // en la tabla (conciliado=false) y que podrían asignarse a ventas del mes incorrecto.
      // 2. Movimientos vinculados del batch actual: posicion → venta_ml_id
      //    Solo usamos movimientos del primer pase (movToVenta) para construir el índice de posiciones.
      //    Esto evita colisiones entre posiciones de distintos meses (ambos empiezan en 1).
      const posByVenta = {}; // posicion → venta_ml_id
      // Primero indexar los del primer pase (ya vinculados en esta corrida)
      for (const link of movToVenta) {
        // Buscar la posicion de este mov en movs
        const mov = movs.find(m => m.id === link.mov_id);
        if (mov?.posicion != null) posByVenta[mov.posicion] = link.venta_id;
      }
      // También incluir los ya vinculados previamente (conciliaciones manuales del mismo AS)
      // usando solo los IDs del batch actual como referencia de posiciones
      const movIdsActual = new Set(movs.map(m => m.id));
      let lpOffset = 0;
      while (true) {
        const page = await sbGet('movimientos_mp', 
          `venta_ml_id=not.is.null&posicion=not.is.null&select=id,posicion,venta_ml_id&limit=1000&offset=${lpOffset}&order=id`
        );
        if (!page?.length) break;
        for (const m of page) {
          if (movIdsActual.has(m.id)) posByVenta[m.posicion] = m.venta_ml_id;
        }
        if (page.length < 1000) break;
        lpOffset += 1000;
      }
      
      // 3. Ventas Flex con cargo_envio (para validar destino + monto)
      const ventasFlexMap = {}; // id → cargo_envio
      let vfOffset = 0;
      while (true) {
        const page = await sbGet('ventas_ml', 
          `tipo_envio=eq.flex&cargo_envio=gt.0&select=id,cargo_envio&limit=1000&offset=${vfOffset}&order=id`
        );
        if (!page?.length) break;
        for (const v of page) ventasFlexMap[v.id] = v.cargo_envio;
        if (page.length < 1000) break;
        vfOffset += 1000;
      }
      
      // 4. Ventas que YA tienen bonificación vinculada (paginado)
      const ventasConBonif = new Set();
      let cbOffset = 0;
      while (true) {
        const page = await sbGet('movimientos_mp', 
          `categoria=eq.bonificacion_envio&venta_ml_id=not.is.null&select=venta_ml_id&limit=1000&offset=${cbOffset}&order=id`
        );
        if (!page?.length) break;
        for (const m of page) { if (m.venta_ml_id) ventasConBonif.add(m.venta_ml_id); }
        if (page.length < 1000) break;
        cbOffset += 1000;
      }
      
      // Helper: calcular neto esperado de bonificación desde cargo_envio bruto
      const netoEsperado = (cargoEnvio) => {
        const bruto = Math.abs(cargoEnvio || 0);
        const imp = Math.round(bruto * 0.006 * 100) / 100;
        return Math.round((bruto - imp) * 100) / 100;
      };
      
      // ─── PASO A: matching por posición + validación de monto ───
      // La bonificación en posición N → buscar liquidación vinculada cercana
      // PERO solo vincular si el monto de la bonificación coincide con el neto esperado
      // del cargo_envio de la venta candidata. Esto evita vincular a la venta equivocada
      // cuando hay varias liquidaciones consecutivas de distintas zonas.
      const bonifByVenta = {}; // venta_id → bonif_mov_id
      const bonifSinMatch = []; // para fallback por monto
      
      for (const bonif of bonifSinVinc) {
        if (bonif.posicion == null) { bonifSinMatch.push(bonif); continue; }
        
        const bonifMonto = Math.round(Math.abs(bonif.monto_neto || 0) * 100); // centavos
        let ventaId = null;
        for (const delta of [-1, -2, 1, -3, 2, -4, 3]) {
          const candidata = posByVenta[bonif.posicion + delta];
          // Validar: es Flex + tiene liquidación del primer pase + no tiene bonif ya + monto coincide
          if (!candidata || !ventasFlexMap[candidata] || ventasConBonif.has(candidata)) continue;
          if (!ventasTocadas.has(candidata)) continue; // solo ventas con liquidación vinculada
          const netoExp = Math.round(netoEsperado(ventasFlexMap[candidata]) * 100);
          if (netoExp === bonifMonto) {
            ventaId = candidata;
            break;
          }
        }
        
        if (ventaId) {
          bonifByVenta[ventaId] = bonif.id;
          ventasConBonif.add(ventaId);
          ventasTocadas.add(ventaId);
          nBonifPos++;
        } else {
          bonifSinMatch.push(bonif);
        }
      }
      
      // ─── PASO B: fallback por monto neto para las que no matchearon ───
      // SOLO ventas que ya tienen liquidación del primer pase (ventasTocadas)
      // Esto evita asignar bonificaciones a ventas de otros meses
      if (bonifSinMatch.length) {
        // Usar ventasFlexMap (ya cargado) filtrado por ventasTocadas y sin bonificación
        const ventasFlexSinBonif = [];
        for (const [id, cargoEnvio] of Object.entries(ventasFlexMap)) {
          if (ventasTocadas.has(id) && !ventasConBonif.has(id)) {
            ventasFlexSinBonif.push({ id, cargo_envio: cargoEnvio });
          }
        }
        
        // Agrupar por monto neto esperado
        const ventasByMonto = {};
        for (const v of ventasFlexSinBonif) {
          const neto = netoEsperado(v.cargo_envio);
          const key = Math.round(neto * 100);
          if (!ventasByMonto[key]) ventasByMonto[key] = [];
          ventasByMonto[key].push(v);
        }
        
        for (const bonif of bonifSinMatch) {
          const bonifKey = Math.round(Math.abs(bonif.monto_neto || 0) * 100);
          const candidatas = ventasByMonto[bonifKey];
          if (!candidatas?.length) continue;
          const venta = candidatas.shift();
          if (candidatas.length === 0) delete ventasByMonto[bonifKey];
          bonifByVenta[venta.id] = bonif.id;
          ventasConBonif.add(venta.id);
          ventasTocadas.add(venta.id);
          nBonifMonto++;
        }
      }
      
      // 7. Batch update: vincular bonificaciones a ventas (paralelizado)
      const bonifTasks = [];
      const entries = Object.entries(bonifByVenta);
      for (let i = 0; i < entries.length; i += 50) {
        const batch = entries.slice(i, i + 50);
        for (const [ventaId, movId] of batch) {
          const patchData = { conciliado: true, venta_ml_id: ventaId };
          if (mlLineaId) patchData.linea_negocio_id = mlLineaId;
          bonifTasks.push(() => sbPatch('movimientos_mp', `id=eq.${movId}`, patchData).catch(e => {
            console.warn('Patch bonif:', e.message);
          }));
        }
      }
      await parallel(bonifTasks);
      
      console.log(`✓ Bonificaciones: ${nBonifPos} por posición, ${nBonifMonto} por monto, ${bonifSinVinc.length - nBonifPos - nBonifMonto} sin match`);
    }
  } catch(e) {
    console.warn('Segundo pase bonificaciones:', e.message);
  }

  // ─── PASO FINAL: calcular balance y estado para TODAS las ventas tocadas ──
  // Un solo paso que consulta la DB directamente, sin depender de estado previo.
  // Esto evita problemas de pasos que se sobreescriben entre sí.
  
  let nBalanceOk = 0, nBalanceDiff = 0;
  const allTocadas = [...ventasTocadas];
  console.log(`  → ${allTocadas.length} ventas tocadas, calculando balance...`);

  if (allTocadas.length) {
    // 1. Traer movimientos vinculados agrupados por venta (paginado)
    // Incluye categoria para calcular esperado dinámicamente (devoluciones reducen el esperado)
    const ventaMontosMap = {}; // venta_id → suma total de montos
    const ventaDevMap = {};    // venta_id → suma de movs devolucion/cancelada/cargo_envio (negativos)
    const DEV_CATS = ['devolucion', 'venta_cancelada', 'cargo_envio_devolucion'];
    for (let i = 0; i < allTocadas.length; i += 100) {
      const chunk = allTocadas.slice(i, i + 100).join(',');
      let mOffset = 0;
      while (true) {
        const movsChunk = await sbGet('movimientos_mp', `venta_ml_id=in.(${chunk})&select=venta_ml_id,monto_neto,categoria&limit=1000&offset=${mOffset}&order=id`);
        if (!movsChunk?.length) break;
        for (const m of movsChunk) {
          ventaMontosMap[m.venta_ml_id] = (ventaMontosMap[m.venta_ml_id] || 0) + (m.monto_neto || 0);
          if (DEV_CATS.includes(m.categoria)) {
            ventaDevMap[m.venta_ml_id] = (ventaDevMap[m.venta_ml_id] || 0) + (m.monto_neto || 0);
          }
        }
        if (movsChunk.length < 1000) break;
        mOffset += 1000;
      }
    }
    
    // 2. Traer por_cobrar de las ventas tocadas (fresh from DB, paginado)
    const ventaPorCobrar = {};
    const ventaStatus = {};
    for (let i = 0; i < allTocadas.length; i += 200) {
      const chunk = allTocadas.slice(i, i + 200).join(',');
      let pcOffset = 0;
      while (true) {
        const ventasChunk = await sbGet('ventas_ml', `id=in.(${chunk})&select=id,por_cobrar,ml_status&limit=1000&offset=${pcOffset}&order=id`);
        if (!ventasChunk?.length) break;
        for (const v of ventasChunk) {
          ventaPorCobrar[v.id] = v.por_cobrar || 0;
          ventaStatus[v.id] = v.ml_status;
        }
        if (ventasChunk.length < 1000) break;
        pcOffset += 1000;
      }
    }
    
    // 3. Calcular balance y agrupar
    // Para cancelled: esperado = 0 (ciclo financiero neta a 0, residual queda como balance)
    // Para normales: esperado = por_cobrar + sumDev
    const conciliadoIds = [];
    const parcialUpdates = []; // {id, balance}
    
    for (const ventaId of allTocadas) {
      const sumMovs = ventaMontosMap[ventaId] || 0;
      const sumDev = Math.min(0, ventaDevMap[ventaId] || 0);
      const esperado = ventaStatus[ventaId] === 'cancelled' ? 0 : (ventaPorCobrar[ventaId] || 0) + sumDev;
      const balance = Math.round((sumMovs - esperado) * 100) / 100;
      
      if (Math.abs(balance) < 0.02) {
        conciliadoIds.push(ventaId);
        nBalanceOk++;
      } else {
        parcialUpdates.push({ id: ventaId, balance });
        nBalanceDiff++;
      }
    }
    
    // 4. Batch update conciliadas
    const concTasks = [];
    for (let i = 0; i < conciliadoIds.length; i += 20) {
      const ids = conciliadoIds.slice(i, i + 20).join(',');
      concTasks.push(() => sbPatch('ventas_ml', `id=in.(${ids})`, {
        balance_conciliacion: 0, estado_conciliacion: 'conciliado', conciliado: true
      }).catch(e => console.warn('Final conc:', e.message)));
    }
    await parallel(concTasks);
    
    // 5. Batch update parciales
    const parcTasks = parcialUpdates.map(upd => () => 
      sbPatch('ventas_ml', `id=eq.${upd.id}`, {
        balance_conciliacion: upd.balance, estado_conciliacion: 'parcial', conciliado: false
      }).catch(e => console.warn('Final parcial:', e.message))
    );
    await parallel(parcTasks);
    
    console.log(`  → balance: ${nBalanceOk} conciliadas, ${nBalanceDiff} parciales`);
  }

  const result = { 
    movimientos: nMovs, ventas: nVentas, devoluciones: nDevueltas, 
    bonificaciones: nBonif, bonif_posicion: nBonifPos, bonif_monto: nBonifMonto,
    otros_vinculados: nOtrosVinculados,
    cargos_envio: Object.keys(ventaCargoEnvio).length,
    balance_ok: nBalanceOk, balance_diff: nBalanceDiff
  };
  console.log(`✓ Auto-conciliación: ${nMovs} movs → ${nVentas} ventas, ${nDevueltas} devol, ${nBonifPos}+${nBonifMonto} bonif (pos+monto), ${nOtrosVinculados} otros, balance ok=${nBalanceOk} diff=${nBalanceDiff}`);
  return result;
}

app.post('/mp/conciliar', async (req, res) => {
  try {
    // fechaDesde y fechaHasta opcionales — si vienen, filtran movimientos al rango del AS
    const { fechaDesde, fechaHasta } = req.body || {};
    res.json({ ok: true, conciliados: await autoConciliarMP(fechaDesde || null, fechaHasta || null) });
  }
  catch (e) { res.status(500).json({ error: e.message }); }
});


// ── Recalcular balance de todas las ventas conciliadas ───────────────
// Útil cuando por_cobrar cambia (ej: re-sync corrige add_on) sin re-subir el AS.
// Recorre TODAS las ventas con movimientos vinculados y recalcula balance vs por_cobrar actual.
app.post('/mp/recalcular-balance', async (_, res) => {
  try {
    console.log('Recalculando balance de ventas con movimientos vinculados...');

    // 1. Traer todas las ventas que tienen al menos 1 movimiento vinculado
    const ventaIds = new Set();
    let offset = 0;
    while (true) {
      const page = await sbGet('movimientos_mp',
        `venta_ml_id=not.is.null&select=venta_ml_id&limit=1000&offset=${offset}&order=id`
      );
      if (!page?.length) break;
      for (const m of page) ventaIds.add(m.venta_ml_id);
      if (page.length < 1000) break;
      offset += 1000;
    }
    const ids = [...ventaIds];
    console.log(`  → ${ids.length} ventas con movimientos vinculados`);

    // 2. Para cada venta: sumar movimientos y comparar con por_cobrar fresco
    let nConciliadas = 0, nParciales = 0, nSinCambio = 0, nDescartadas = 0;
    const concTasks = [], parcTasks = [];
    const DEV_CATS = ['devolucion', 'venta_cancelada', 'cargo_envio_devolucion'];

    for (let i = 0; i < ids.length; i += 200) {
      const chunk = ids.slice(i, i + 200);
      const chunkStr = chunk.join(',');

      // Sumar movimientos por venta (total + devolucion categories) — paginado
      const sumByVenta = {};
      const devByVenta = {};
      let mOffset = 0;
      while (true) {
        const movs = await sbGet('movimientos_mp',
          `venta_ml_id=in.(${chunkStr})&select=venta_ml_id,monto_neto,categoria&limit=1000&offset=${mOffset}`
        );
        if (!movs?.length) break;
        for (const m of movs) {
          sumByVenta[m.venta_ml_id] = (sumByVenta[m.venta_ml_id] || 0) + (m.monto_neto || 0);
          if (DEV_CATS.includes(m.categoria)) {
            devByVenta[m.venta_ml_id] = (devByVenta[m.venta_ml_id] || 0) + (m.monto_neto || 0);
          }
        }
        if (movs.length < 1000) break;
        mOffset += 1000;
      }

      // Traer por_cobrar y estado actual
      const ventas = await sbGet('ventas_ml',
        `id=in.(${chunkStr})&select=id,por_cobrar,estado_conciliacion,ml_status&limit=1000`
      );
      for (const v of (ventas || [])) {
        // NUNCA tocar descartadas — son cancelaciones sin entrega ya procesadas
        if (v.estado_conciliacion === 'descartada') { nDescartadas++; continue; }

        const sumMovs = Math.round((sumByVenta[v.id] || 0) * 100) / 100;
        const sumDev = Math.min(0, devByVenta[v.id] || 0);
        const esperado = v.ml_status === 'cancelled' ? 0 : (v.por_cobrar || 0) + sumDev;
        const balance = Math.round((sumMovs - esperado) * 100) / 100;
        const nuevoEstado = Math.abs(balance) < 0.02 ? 'conciliado' : 'parcial';

        if (nuevoEstado === v.estado_conciliacion) { nSinCambio++; continue; }

        if (nuevoEstado === 'conciliado') {
          concTasks.push(() => sbPatch('ventas_ml', `id=eq.${v.id}`, {
            balance_conciliacion: 0, estado_conciliacion: 'conciliado', conciliado: true
          }).catch(e => console.warn('recalc conc:', e.message)));
          nConciliadas++;
        } else {
          parcTasks.push(() => sbPatch('ventas_ml', `id=eq.${v.id}`, {
            balance_conciliacion: balance, estado_conciliacion: 'parcial', conciliado: false
          }).catch(e => console.warn('recalc parcial:', e.message)));
          nParciales++;
        }
      }
    }

    // 3. Aplicar updates en paralelo
    const parallel = async (tasks, concurrency = 15) => {
      for (let i = 0; i < tasks.length; i += concurrency) {
        await Promise.allSettled(tasks.slice(i, i + concurrency).map(fn => fn()));
      }
    };
    await parallel(concTasks);
    await parallel(parcTasks);

    console.log(`✓ Recalcular balance: ${nConciliadas} nuevas conciliadas, ${nParciales} parciales, ${nSinCambio} sin cambio, ${nDescartadas} descartadas ignoradas`);
    res.json({ ok: true, conciliadas: nConciliadas, parciales: nParciales, sin_cambio: nSinCambio, descartadas_ignoradas: nDescartadas });
  } catch (e) {
    console.error('recalcular-balance:', e);
    res.status(500).json({ error: e.message });
  }
});

// ── Movimientos vinculados a una venta ────────────────────────────────
app.get('/mp/movimientos-venta/:ventaId', async (req, res) => {
  try {
    const movs = await sbGet('movimientos_mp', `venta_ml_id=eq.${req.params.ventaId}&order=fecha.asc`);
    res.json({ ok: true, movimientos: movs || [] });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Conciliación manual: vincular movimiento a venta ──────────────────
app.post('/mp/vincular', async (req, res) => {
  try {
    const { movimiento_id, venta_id } = req.body;
    if (!movimiento_id || !venta_id) return res.status(400).json({ error: 'Faltan movimiento_id y venta_id' });

    // Vincular movimiento
    await sbPatch('movimientos_mp', `id=eq.${movimiento_id}`, { 
      conciliado: true, 
      venta_ml_id: venta_id 
    });

    // Recalcular balance de la venta
    const DEV_CATS = ['devolucion', 'venta_cancelada', 'cargo_envio_devolucion'];
    const movsVenta = await sbGet('movimientos_mp', `venta_ml_id=eq.${venta_id}&select=monto_neto,categoria`);
    const sumMovs = (movsVenta || []).reduce((s, m) => s + (m.monto_neto || 0), 0);
    const sumDev = Math.min(0, (movsVenta || []).filter(m => DEV_CATS.includes(m.categoria)).reduce((s, m) => s + (m.monto_neto || 0), 0));
    const venta = await sbGet('ventas_ml', `id=eq.${venta_id}&select=por_cobrar,conciliado,ml_status`);
    
    if (venta?.length) {
      const esperado = venta[0].ml_status === 'cancelled' ? 0 : (venta[0].por_cobrar || 0) + sumDev;
      const balance = Math.round((sumMovs - esperado) * 100) / 100;
      const estado = Math.abs(balance) < 0.02 ? 'conciliado' : 'parcial';
      await sbPatch('ventas_ml', `id=eq.${venta_id}`, {
        balance_conciliacion: balance,
        estado_conciliacion: estado,
        conciliado: estado === 'conciliado'
      });
    }

    res.json({ ok: true, balance: sumMovs });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Conciliación manual: desvincular movimiento de venta ──────────────
app.post('/mp/desvincular', async (req, res) => {
  try {
    const { movimiento_id, venta_id } = req.body;
    if (!movimiento_id) return res.status(400).json({ error: 'Falta movimiento_id' });

    await sbPatch('movimientos_mp', `id=eq.${movimiento_id}`, { 
      conciliado: false, 
      venta_ml_id: null 
    });

    // Recalcular balance si se pasó venta_id
    if (venta_id) {
      const DEV_CATS = ['devolucion', 'venta_cancelada', 'cargo_envio_devolucion'];
      const movsVenta = await sbGet('movimientos_mp', `venta_ml_id=eq.${venta_id}&select=monto_neto,categoria`);
      const sumMovs = (movsVenta || []).reduce((s, m) => s + (m.monto_neto || 0), 0);
      const sumDev = Math.min(0, (movsVenta || []).filter(m => DEV_CATS.includes(m.categoria)).reduce((s, m) => s + (m.monto_neto || 0), 0));
      const venta = await sbGet('ventas_ml', `id=eq.${venta_id}&select=por_cobrar,ml_status`);
      if (venta?.length) {
        const esperado = venta[0].ml_status === 'cancelled' ? 0 : (venta[0].por_cobrar || 0) + sumDev;
        const balance = Math.round((sumMovs - esperado) * 100) / 100;
        const hasMov = (movsVenta || []).length > 0;
        const estado = !hasMov ? 'pendiente' : (Math.abs(balance) < 0.02 ? 'conciliado' : 'parcial');
        await sbPatch('ventas_ml', `id=eq.${venta_id}`, {
          balance_conciliacion: balance,
          estado_conciliacion: estado,
          conciliado: estado === 'conciliado'
        });
      }
    }

    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Descartar movimiento (irrelevante/duplicado) ──────────────────────
app.post('/mp/descartar', async (req, res) => {
  try {
    const { movimiento_id } = req.body;
    if (!movimiento_id) return res.status(400).json({ error: 'Falta movimiento_id' });
    // Marca conciliado=true sin venta_ml_id → descartado
    // Se distingue de "pasado a gasto" porque ese tiene movimiento_mp_id en tabla gastos
    await sbPatch('movimientos_mp', `id=eq.${movimiento_id}`, { conciliado: true });
    res.json({ ok: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── Pasar venta pendiente al mes siguiente ───────────────────────────
// Mueve periodo_cobro al mes siguiente para que aparezca en la conciliación de ese mes.
// No toca periodo (mes de venta) ni el P&L.
app.post('/mp/pasar-mes', async (req, res) => {
  try {
    const { venta_id } = req.body;
    if (!venta_id) return res.status(400).json({ error: 'Falta venta_id' });

    // Traer la venta
    const ventas = await sbGet('ventas_ml', `id=eq.${venta_id}&select=id,periodo,periodo_cobro,estado_conciliacion,aprobada&limit=1`);
    if (!ventas?.length) return res.status(404).json({ error: 'Venta no encontrada' });

    const venta = ventas[0];
    if (!venta.aprobada) return res.status(400).json({ error: 'La venta no está aprobada' });
    if (!['pendiente', 'parcial'].includes(venta.estado_conciliacion)) {
      return res.status(400).json({ error: `La venta ya está ${venta.estado_conciliacion}` });
    }

    // Calcular mes siguiente desde periodo_cobro actual (o periodo si no tiene)
    const base = venta.periodo_cobro || venta.periodo;
    if (!base || !/^\d{4}-\d{2}$/.test(base)) {
      return res.status(400).json({ error: 'periodo_cobro inválido: ' + base });
    }
    const [y, m] = base.split('-').map(Number);
    const nextMonth = m === 12 ? `${y + 1}-01` : `${y}-${String(m + 1).padStart(2, '0')}`;

    // Validar que el salto no supere 3 meses desde el periodo original de la venta
    // Más de 3 meses sin cobrar requiere revisión manual, no seguir pasando automáticamente
    const [yOrig, mOrig] = venta.periodo.split('-').map(Number);
    const [yNew, mNew] = nextMonth.split('-').map(Number);
    const mesesDiferencia = (yNew - yOrig) * 12 + (mNew - mOrig);
    if (mesesDiferencia > 3) {
      return res.status(400).json({ 
        error: `No se puede pasar más de 3 meses desde la fecha original de la venta (${venta.periodo}). Revisá manualmente.`
      });
    }

    await sbPatch('ventas_ml', `id=eq.${venta_id}`, { periodo_cobro: nextMonth });

    res.json({ ok: true, periodo_cobro_anterior: base, periodo_cobro_nuevo: nextMonth });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── EXTRACTO BANCARIO (Supervielle / Galicia) ────────────────────────
app.post('/banco/extracto', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'Falta el archivo' });
    const banco = (req.body.banco || 'supervielle').toLowerCase();

    const wb  = XLSX.read(req.file.buffer, { type: 'buffer', cellDates: true });
    const ws  = wb.Sheets[wb.SheetNames[0]];
    const raw = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null });

    // Buscar fila de cabecera
    let hRow = -1;
    for (let i = 0; i < Math.min(raw.length, 15); i++) {
      const joined = (raw[i] || []).join('|').toLowerCase();
      if (joined.includes('fecha') && (joined.includes('débito') || joined.includes('concepto') || joined.includes('crédito'))) {
        hRow = i; break;
      }
    }
    if (hRow < 0) return res.status(400).json({ error: 'No encontré la fila de cabecera en el Excel. ¿Es el formato correcto de Supervielle?' });

    const headers = (raw[hRow] || []).map(h => String(h || '').toLowerCase().trim());
    const idx = (opts) => {
      for (const o of opts) { const i = headers.findIndex(h => h.includes(o)); if (i >= 0) return i; }
      return -1;
    };
    const iF   = idx(['fecha']);
    const iC   = idx(['concepto', 'descripción', 'descripcion']);
    const iD   = idx(['detalle', 'referencia', 'información adicional']);
    const iDeb = idx(['débito', 'debito', 'cargo']);
    const iCr  = idx(['crédito', 'credito', 'abono', 'haber']);
    const iSal = idx(['saldo']);

    const cuentas = await sbGet('cuentas', `nombre=ilike.*${banco}*&limit=1`);
    const cuentaId = cuentas?.[0]?.id || null;
    const CUIT_PROPIO = '30717476472';

    const toDate = v => {
      if (v instanceof Date) return v.toISOString().split('T')[0];
      if (typeof v === 'number' && v > 40000) return new Date(Math.round((v - 25569) * 86400000)).toISOString().split('T')[0];
      return v ? String(v).substring(0, 10) : null;
    };
    const toNum = v => parseFloat(String(v || 0).replace(/\./g, '').replace(',', '.').trim()) || 0;

    const categorizar = (c, d) => {
      const t = (c + ' ' + d).toLowerCase();
      if (t.includes('iibb') || t.includes('ingresos brutos') || t.includes('rentas')) return 'iibb_percepcion';
      if (t.includes('idc') || (t.includes('débito') && t.includes('directo')))        return 'idc';
      if (t.includes('comex') || t.includes('exterior') || t.includes('fob'))          return 'fob_pago';
      if (t.includes('percep') && t.includes('iva'))                                   return 'iva_percepcion';
      if (t.includes('sueldo') || t.includes('salario') || t.includes('remuner'))      return 'sueldo';
      if (t.includes('axoft') || t.includes('tango'))                                  return 'admin';
      if (t.includes(CUIT_PROPIO) || t.includes('adara rs'))                           return 'interno';
      if (t.includes('transfer') || t.includes('tcr') || t.includes('tdb'))            return 'transferencia';
      return 'otro';
    };

    const movs = [];
    for (let i = hRow + 1; i < raw.length; i++) {
      const row  = raw[i] || [];
      const fecha = toDate(row[iF]);
      if (!fecha || fecha < '2020-01-01') continue;
      const concepto = String(row[iC] || '').trim();
      const detalle  = String(iD >= 0 ? row[iD] || '' : '').trim();
      const debito   = iDeb >= 0 ? Math.abs(toNum(row[iDeb])) : 0;
      const credito  = iCr  >= 0 ? Math.abs(toNum(row[iCr]))  : 0;
      if (!concepto && !debito && !credito) continue;
      const cat = categorizar(concepto, detalle);
      movs.push({ cuenta_id: cuentaId, banco, fecha, concepto, detalle, debito: debito || null, credito: credito || null, saldo: iSal >= 0 ? toNum(row[iSal]) : null, categoria: cat, es_interno: cat === 'interno', conciliado: false });
    }

    let insertados = 0;
    for (let i = 0; i < movs.length; i += 50) {
      await sbUpsert('movimientos_bancarios', movs.slice(i, i + 50));
      insertados += Math.min(50, movs.length - i);
    }

    res.json({ ok: true, banco, total: movs.length, insertados });
  } catch (e) {
    console.error('Banco extracto:', e);
    res.status(500).json({ error: e.message });
  }
});

// ── TANGO FACTURA — Solo lectura ─────────────────────────────────────

// Traer facturas emitidas en un período y sincronizar a Supabase
app.get('/tango/sync', async (req, res) => {
  try {
    const { desde, hasta } = req.query;
    const fDesde = desde || new Date(Date.now() - 30 * 86400000).toISOString().split('T')[0];
    const fHasta = hasta || new Date().toISOString().split('T')[0];

    // ObtenerInfoMovimientosPorNroFactura con rango de fechas
    const result = await tfPost('ObtenerInfoMovimientosPorNroFactura', {
      FechaComprobante:   `${fDesde}T00:00:00`,
      FechaServicioHasta: `${fHasta}T23:59:59`
    });

    const facturas = Array.isArray(result.Data) ? result.Data : (result.Data ? [result.Data] : []);
    if (!facturas.length) return res.json({ ok: true, sincronizadas: 0, mensaje: 'Sin facturas en ese período' });

    // Guardar en tabla facturas_tango
    const rows = facturas.map(f => ({
      movimiento_id:    String(f.MovimientoId || ''),
      nro_factura:      f.NroFactura ? String(f.NroFactura) : null,
      cai_cae:          f.CAICAE || null,
      fecha_emision:    f.FechaEmision ? f.FechaEmision.split('T')[0] : null,
      fecha_vencimiento: f.FechaVencimiento ? f.FechaVencimiento.split('T')[0] : null,
      total:            f.Total || 0,
      total_iva:        f.TotalIVA || 0,
      subtotal:         f.Subtotal || 0,
      estado_id:        f.EstadoId || 0,
      electronico:      f.Electronico || false,
      grabado:          f.Grabado || false
    }));

    await sbUpsert('facturas_tango', rows, 'movimiento_id');

    // Intentar cruzar con ventas ML por número de factura
    let cruzadas = 0;
    for (const f of rows) {
      if (!f.nro_factura) continue;
      const ventas = await sbGet('ventas_ml', `factura_tango=eq.${f.nro_factura}&limit=5`).catch(() => []);
      if (ventas?.length) cruzadas++;
    }

    res.json({ ok: true, sincronizadas: rows.length, cruzadas_con_ml: cruzadas, desde: fDesde, hasta: fHasta });
  } catch (e) {
    console.error('Tango sync:', e);
    res.status(500).json({ error: e.message });
  }
});

// Listar facturas Tango guardadas en Supabase
app.get('/tango/facturas', async (req, res) => {
  try {
    const { desde, hasta, limit = 100 } = req.query;
    let q = `order=fecha_emision.desc&limit=${limit}`;
    if (desde) q += `&fecha_emision=gte.${desde}`;
    if (hasta) q += `&fecha_emision=lte.${hasta}`;
    const facturas = await sbGet('facturas_tango', q);
    res.json(facturas);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── CRON — Sync automático ───────────────────────────────────────────
// Cada hora: traer ventas ML de las últimas 2hs
cron.schedule('0 * * * *', async () => {
  if (!ML.access) return;
  console.log('⏰ Cron: sync ML...');
  try { await syncMLVentas(1); } catch (e) { console.error('Cron ML:', e.message); }
});

// Cada 6 horas: renovar token ML
cron.schedule('0 */6 * * *', async () => {
  if (!ML.refresh) return;
  console.log('⏰ Cron: refresh token ML...');
  try { await refreshML(); } catch (e) { console.error('Cron refresh:', e.message); }
});

// ── Gastos: alta atómica (gasto + renglones fiscales + caja sin_factura) ──
// Opción A (ver ADARA-GASTOS.md): el front SIEMPRE postea acá.
//   1) Inserta el gasto.
//   2) Inserta los renglones fiscales (retenciones/percepciones) en gasto_fiscal.
//   3) Si es sin_factura + efectivo, dispara el movimiento de caja
//      (origen='sin_factura_auto') + vínculo op_tipo='gasto', así queda pagado
//      al instante y la caja refleja la salida.
// PostgREST no da transacción nativa: si algo falla después de crear el gasto,
// se hace rollback best-effort borrando lo creado (gasto recién nacido, sin
// historia que preservar).
app.post('/gastos', async (req, res) => {
  const RET_TIPOS  = ['ret_ganancias', 'ret_iva', 'ret_iibb', 'ret_suss', 'otro_ret'];
  const PERC_TIPOS = ['perc_iva', 'perc_iibb', 'otro_perc'];
  const TIPOS_FISCALES = [...RET_TIPOS, ...PERC_TIPOS];
  const round2 = n => Math.round((Number(n) || 0) * 100) / 100;

  let gastoId = null;
  let movAutoId = null;
  try {
    const { gasto, fiscal } = req.body || {};
    if (!gasto) return res.status(400).json({ error: 'Falta el objeto gasto' });

    const requeridos = ['fecha', 'linea_id', 'categoria_codigo', 'descripcion', 'tipo_comprobante', 'moneda', 'monto_neto'];
    for (const f of requeridos) {
      if (gasto[f] === undefined || gasto[f] === null || gasto[f] === '') {
        return res.status(400).json({ error: `Falta ${f}` });
      }
    }
    if (!(Number(gasto.monto_neto) > 0)) return res.status(400).json({ error: 'monto_neto debe ser mayor a 0' });

    // Normalización coherente con los CHECK de la tabla
    const esFacturaA = gasto.tipo_comprobante === 'factura_a';
    const esUSD = gasto.moneda === 'USD';
    const monto_iva = esFacturaA ? round2(gasto.monto_iva) : 0;                          // chk_iva_solo_factura_a
    const tc = (esUSD && gasto.tc != null && gasto.tc !== '') ? Number(gasto.tc) : null;  // chk_tc_solo_usd / chk_tc_positivo

    const lineasFiscales = (Array.isArray(fiscal) ? fiscal : [])
      .filter(x => x && TIPOS_FISCALES.includes(x.tipo) && Number(x.monto) > 0)
      .map(x => ({ tipo: x.tipo, monto: round2(x.monto) }));

    // Efectivo sin factura en USD necesita TC (el pago ocurre ahora — B1)
    if (gasto.tipo_comprobante === 'sin_factura' && gasto.forma_pago === 'efectivo' && esUSD && tc == null) {
      return res.status(400).json({ error: 'Efectivo sin factura en USD: falta el TC' });
    }

    // 1) Gasto
    const filaGasto = {
      fecha: gasto.fecha,
      linea_id: gasto.linea_id,
      categoria_codigo: gasto.categoria_codigo,
      proveedor_id: gasto.proveedor_id || null,
      descripcion: gasto.descripcion,
      tipo_comprobante: gasto.tipo_comprobante,
      nro_comprobante: gasto.nro_comprobante || null,
      moneda: gasto.moneda,
      tc,
      monto_neto: round2(gasto.monto_neto),
      monto_iva,
      cuenta_origen_intencion: gasto.cuenta_origen_intencion || null,
      forma_pago: gasto.forma_pago || null,
      estado: 'activo'
    };
    const insGasto = await sbUpsert('gastos', filaGasto);
    const g = Array.isArray(insGasto) ? insGasto[0] : insGasto;
    if (!g || !g.id) throw new Error('No se obtuvo el id del gasto');
    gastoId = g.id;

    // 2) Renglones fiscales
    if (lineasFiscales.length) {
      await sbUpsert('gasto_fiscal', lineasFiscales.map(x => ({ gasto_id: gastoId, tipo: x.tipo, monto: x.monto })));
    }

    // 3) Caja automática (sin_factura efectivo)
    if (gasto.tipo_comprobante === 'sin_factura' && gasto.forma_pago === 'efectivo') {
      const bruto   = round2(filaGasto.monto_neto + filaGasto.monto_iva);
      const sumPerc = lineasFiscales.filter(x => PERC_TIPOS.includes(x.tipo)).reduce((s, x) => s + x.monto, 0);
      const sumRet  = lineasFiscales.filter(x => RET_TIPOS.includes(x.tipo)).reduce((s, x) => s + x.monto, 0);
      const aPagarOrigen = round2(bruto + sumPerc - sumRet);

      const codigoCaja = gasto.cuenta_origen_intencion || (esUSD ? 'caja_usd' : 'caja_ars');
      const cuentas = await sbGet('cuentas', `codigo=eq.${codigoCaja}&select=id,moneda`);
      if (!cuentas || !cuentas.length) throw new Error(`No existe la cuenta de caja '${codigoCaja}'`);
      const cuenta = cuentas[0];

      // El movimiento se guarda en la moneda de la cuenta (USD nativo si caja_usd).
      const movRows = await sbUpsert('movimientos', {
        cuenta_id: cuenta.id,
        fecha: gasto.fecha,
        monto: -aPagarOrigen,
        origen: 'sin_factura_auto',
        referencia_externa: 'sin_factura_auto-' + gastoId,   // NOT NULL + UNIQUE(origen, ref)
        categoria: 'gasto',
        descripcion: gasto.descripcion,
        conciliado_auto: false
      });
      const mov = Array.isArray(movRows) ? movRows[0] : movRows;
      movAutoId = mov.id;

      // El vínculo se expresa en ARS (v_gastos_ap concilia en ARS).
      const montoVinculoArs = esUSD ? round2(aPagarOrigen * tc) : aPagarOrigen;
      await sbUpsert('vinculos', {
        movimiento_id: mov.id,
        op_tipo: 'gasto',
        op_id: gastoId,
        monto: montoVinculoArs
      });
    }

    return res.json({ ok: true, id: gastoId, movimiento_auto: movAutoId });
  } catch (e) {
    // Rollback best-effort
    try {
      if (movAutoId) {
        await sb('DELETE', 'vinculos', null, `movimiento_id=eq.${movAutoId}&op_tipo=eq.gasto`);
        await sb('DELETE', 'movimientos', null, `id=eq.${movAutoId}`);
      }
      if (gastoId) {
        await sb('DELETE', 'gasto_fiscal', null, `gasto_id=eq.${gastoId}`);
        await sb('DELETE', 'gastos', null, `id=eq.${gastoId}`);
      }
    } catch (e2) { console.error('Rollback /gastos falló:', e2.message); }
    return res.status(500).json({ error: e.message });
  }
});

// ── Importador de extracto Supervielle (Excel/CSV → movimientos) ──────────
// Parsea el export de movimientos de Supervielle (xlsx o csv), lo mapea a la
// tabla nueva `movimientos` (capa 4), auto-clasifica el ruido (impuestos,
// comisiones, intereses, FCI) y verifica la continuidad del saldo.
//
// Idempotencia: referencia_externa = fecha|hora|monto|saldo (el saldo corre y
// es único por movimiento, así re-importar no duplica — UNIQUE(origen, ref)).
// Clasificación: las líneas "ruido" quedan conciliado_auto=true (fuera de
// pendientes); las reales quedan conciliado_auto=false (van a conciliar).
app.post('/supervielle/import', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'Falta el archivo' });

    // Resolver cuenta Supervielle (las 3 subcuentas se agregan en una sola, CB10)
    const cuentas = await sbGet('cuentas', `codigo=eq.supervielle_ars&select=id`);
    const cuentaId = cuentas?.[0]?.id;
    if (!cuentaId) return res.status(400).json({ error: "No existe la cuenta 'supervielle_ars'" });

    // Parseo: xlsx por buffer; csv decodificado latin1 → string (Supervielle exporta acentos como '?')
    const fname = (req.file.originalname || '').toLowerCase();
    const wb = fname.endsWith('.csv')
      ? XLSX.read(req.file.buffer.toString('latin1'), { type: 'string', raw: true })
      : XLSX.read(req.file.buffer, { type: 'buffer', cellDates: true });
    const ws  = wb.Sheets[wb.SheetNames[0]];
    const raw = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null });

    // Buscar fila de cabecera
    let hRow = -1;
    for (let i = 0; i < Math.min(raw.length, 12); i++) {
      const j = (raw[i] || []).join('|').toLowerCase();
      if (j.includes('fecha') && (j.includes('saldo') || j.includes('concepto'))) { hRow = i; break; }
    }
    if (hRow < 0) return res.status(400).json({ error: 'No encontré la cabecera. ¿Es el export de movimientos de Supervielle?' });

    const headers = (raw[hRow] || []).map(h => String(h || '').toLowerCase().trim());
    const col = re => headers.findIndex(h => re.test(h));
    const iF = col(/fecha/), iH = col(/hora/), iC = col(/concepto|descrip/),
          iDet = col(/detalle|referenc/), iDeb = col(/d.?bito/),
          iCr = col(/cr.?dito/), iSal = col(/saldo/);

    const num = v => {
      if (v == null || v === '') return 0;
      if (typeof v === 'number') return Math.abs(v) < 1e-6 ? 0 : v;
      let s = String(v).trim().replace(/\./g, '').replace(',', '.');
      const n = parseFloat(s);
      return isNaN(n) ? 0 : n;
    };
    const toISO = v => {
      if (v instanceof Date) return v.toISOString().slice(0, 10);
      if (typeof v === 'number' && v > 40000) return new Date(Math.round((v - 25569) * 86400000)).toISOString().slice(0, 10);
      const m = String(v || '').trim().match(/^(\d{2})\/(\d{2})\/(\d{4})/);
      return m ? `${m[3]}-${m[2]}-${m[1]}` : null;
    };

    // Clasificador: devuelve { categoria, auto }. El orden importa (lo fiscal antes que lo genérico).
    const clasificar = (concepto, detalle, monto) => {
      const t = (concepto + ' ' + detalle).toLowerCase();
      const entra = monto > 0;
      if (/impuesto d.?bitos y cr.?ditos/.test(t)) return { categoria: 'impuesto', auto: true };
      if (/sellos/.test(t)) return { categoria: 'impuesto', auto: true };
      if (/ganancias/.test(t)) return { categoria: 'impuesto', auto: true };
      if (/iibb|ingresos brutos|sircreb/.test(t)) return { categoria: 'impuesto', auto: true };
      if (/percep/.test(t)) return { categoria: 'impuesto', auto: true };
      if (/\biva\b|i\.v\.a/.test(t)) return { categoria: 'impuesto', auto: true };
      if (/remuneraci.?n de saldo/.test(t)) return { categoria: 'interes', auto: true };
      if (/intereses de sobregiro|contras.*ints.*sobreg/.test(t)) return { categoria: 'interes', auto: true };
      if (/comisi.?n|comis\.|com\.cheque/.test(t)) return { categoria: 'comision_bancaria', auto: true };
      if (/rescate fci|suscripci.?n.*fci|\bfci\b/.test(t)) return { categoria: 'transferencia_interna', auto: true };
      if (/comex/.test(t)) return { categoria: 'pago_proveedor', auto: false };
      if (/resumenvisa|resumen visa/.test(t)) return { categoria: 'gasto', auto: false };
      if (/pago de servicios|d.?bito autom.?tic/.test(t)) {
        if (/afip|vep/.test(t)) return { categoria: 'impuesto', auto: false };
        if (/axoft|tango/.test(t)) return { categoria: 'gasto', auto: false };
        return { categoria: 'pago_proveedor', auto: false };
      }
      if (/cr.?dito por transferencia|debin|acreditaci.?n cheque|cheque de c.?mara|cred bca electronica/.test(t))
        return { categoria: entra ? 'cobro_venta' : 'pago_proveedor', auto: false };
      if (/transferencia|trf\.|porcbu|pago.prov/.test(t))
        return { categoria: entra ? 'cobro_venta' : 'pago_proveedor', auto: false };
      return { categoria: 'sin_clasificar', auto: false };
    };

    const filas = [];
    for (let i = hRow + 1; i < raw.length; i++) {
      const row = raw[i] || [];
      const fecha = toISO(row[iF]);
      if (!fecha || fecha < '2020-01-01') continue;
      const concepto = String(row[iC] || '').trim();
      const detalle  = iDet >= 0 ? String(row[iDet] || '').trim() : '';
      const hora     = iH >= 0 ? String(row[iH] || '').trim() : '';
      const deb = iDeb >= 0 ? num(row[iDeb]) : 0;
      const cr  = iCr  >= 0 ? num(row[iCr])  : 0;
      const saldo = iSal >= 0 ? num(row[iSal]) : 0;
      if (!concepto && !deb && !cr) continue;
      const monto = Math.round((cr - deb) * 100) / 100;
      filas.push({ fecha, hora, concepto, detalle, deb, cr, saldo, monto });
    }
    if (!filas.length) return res.status(400).json({ error: 'No encontré movimientos en el archivo.' });

    // Chequeo de continuidad de saldo (el export viene más-reciente-primero):
    // saldo[i] == saldo[i+1] + monto[i]
    const breaks = [];
    for (let i = 0; i < filas.length - 1; i++) {
      const esperado = Math.round((filas[i + 1].saldo + filas[i].monto) * 100) / 100;
      if (Math.abs(esperado - filas[i].saldo) > 0.01) {
        breaks.push({ fecha: filas[i].fecha, concepto: filas[i].concepto, saldo: filas[i].saldo, esperado });
      }
    }
    const saldoOk = breaks.length === 0;

    // Construir movimientos + clasificación
    let auto = 0, pendientes = 0;
    const movs = filas.map(f => {
      const { categoria, auto: esAuto } = clasificar(f.concepto, f.detalle, f.monto);
      if (esAuto) auto++; else pendientes++;
      const desc = (f.concepto + (f.detalle ? ' — ' + f.detalle : '')).slice(0, 300);
      return {
        cuenta_id: cuentaId,
        fecha: f.fecha,
        monto: f.monto,
        origen: 'supervielle',
        referencia_externa: `${f.fecha}|${f.hora}|${f.monto.toFixed(2)}|${f.saldo.toFixed(2)}`,
        categoria,
        descripcion: desc,
        conciliado_auto: esAuto
      };
    });

    // Upsert idempotente (UNIQUE origen+referencia_externa → no duplica al re-importar)
    let importados = 0;
    for (let i = 0; i < movs.length; i += 50) {
      const lote = movs.slice(i, i + 50);
      await sbUpsert('movimientos', lote, 'origen,referencia_externa');
      importados += lote.length;
    }

    const fechas = filas.map(f => f.fecha).sort();
    res.json({
      ok: true,
      importados, auto, pendientes,
      rango: { desde: fechas[0], hasta: fechas[fechas.length - 1] },
      saldo_check: { ok: saldoOk, breaks: breaks.slice(0, 5), total_breaks: breaks.length }
    });
  } catch (e) {
    console.error('Supervielle import:', e);
    res.status(500).json({ error: e.message });
  }
});

// ── Borrar movimiento (solo los cargados a mano) ─────────────────────────
// Guarda: nunca borra importados (banco/MP) — esos se restauran al re-importar.
app.delete('/movimientos/:id', async (req, res) => {
  try {
    const id = req.params.id;
    const rows = await sbGet('movimientos', `id=eq.${id}&select=id,origen`);
    const mov = rows && rows[0];
    if (!mov) return res.status(404).json({ error: 'No existe el movimiento' });
    if (mov.origen !== 'manual') {
      return res.status(403).json({ error: 'Solo se borran movimientos cargados a mano. Los importados se restauran al re-importar.' });
    }
    await sb('DELETE', 'vinculos', null, `movimiento_id=eq.${id}`); // limpia vínculos por las dudas
    await sb('DELETE', 'movimientos', null, `id=eq.${id}`);
    res.json({ ok: true });
  } catch (e) {
    console.error('DELETE movimiento:', e);
    res.status(500).json({ error: e.message });
  }
});


// ── Importador de extracto Mercado Pago (Resumen de cuenta .xlsx) ─────────
// El archivo trae un bloque de resumen arriba (INITIAL/FINAL balance) y la tabla
// real con cabecera RELEASE_DATE/TRANSACTION_TYPE/REFERENCE_ID/
// TRANSACTION_NET_AMOUNT/PARTIAL_BALANCE (más-viejo-primero, fechas DD-MM-AAAA).
// Liquidaciones/bonificaciones/devoluciones entran SIN conciliar: se atan a su
// venta con el sync de ML. Solo rendimientos e impuestos se cierran solos.
app.post('/mp/import', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'Falta el archivo' });

    const cuentas = await sbGet('cuentas', `codigo=eq.mp_ars&select=id`);
    const cuentaId = cuentas?.[0]?.id;
    if (!cuentaId) return res.status(400).json({ error: "No existe la cuenta 'mp_ars'" });

    const wb  = XLSX.read(req.file.buffer, { type: 'buffer', raw: true });
    const ws  = wb.Sheets[wb.SheetNames[0]];
    const raw = XLSX.utils.sheet_to_json(ws, { header: 1, defval: null });

    const num = v => {
      if (v == null || v === '') return 0;
      if (typeof v === 'number') return v;
      const s = String(v).trim().replace(/\./g, '').replace(',', '.');
      const n = parseFloat(s);
      return isNaN(n) ? 0 : n;
    };
    const toISO = v => {
      const m = String(v || '').trim().match(/^(\d{2})[\/\-](\d{2})[\/\-](\d{4})/);
      return m ? `${m[3]}-${m[2]}-${m[1]}` : null;
    };

    // Resumen (INITIAL/FINAL balance) para anclar el chequeo de saldo
    let ini = null, fin = null;
    for (let i = 0; i < Math.min(raw.length, 8); i++) {
      const j = (raw[i] || []).join('|').toUpperCase();
      if (j.includes('INITIAL_BALANCE')) { ini = num((raw[i + 1] || [])[0]); fin = num((raw[i + 1] || [])[3]); break; }
    }

    // Cabecera real de la tabla de movimientos
    let hRow = -1;
    for (let i = 0; i < Math.min(raw.length, 12); i++) {
      const j = (raw[i] || []).join('|').toUpperCase();
      if (j.includes('TRANSACTION_TYPE') && j.includes('RELEASE_DATE')) { hRow = i; break; }
    }
    if (hRow < 0) return res.status(400).json({ error: 'No encontré la cabecera. ¿Es el Resumen de cuenta de Mercado Pago?' });

    const H = (raw[hRow] || []).map(h => String(h || '').toUpperCase().trim());
    const col = re => H.findIndex(h => re.test(h));
    const iF = col(/RELEASE_DATE|FECHA/), iT = col(/TRANSACTION_TYPE/), iR = col(/REFERENCE_ID/),
          iM = col(/NET_AMOUNT|AMOUNT/), iS = col(/PARTIAL_BALANCE|BALANCE/);

    // Clasificador MP. Auto (no atado a venta) = solo rendimientos e impuestos
    // que MP cobra directo (ej. impuesto por extracción). Todo lo demás —incluidas
    // las devoluciones/reintegros de impuestos y comisiones— se ata a una venta/
    // compra/gasto y entra SIN conciliar.
    const clasificar = tipo => {
      const s = tipo.toLowerCase();
      if (/rendimiento/.test(s)) return { categoria: 'interes', auto: true };
      if (/^impuesto/.test(s)) return { categoria: 'impuesto', auto: true }; // ej. "Impuesto por extracción"
      if (/devoluci.n|reintegro|dinero retenido/.test(s)) return { categoria: 'devolucion', auto: false };
      if (/liquidaci.n de dinero/.test(s)) return { categoria: 'cobro_venta', auto: false };
      if (/bonificaci.n por env/.test(s)) return { categoria: 'cobro_venta', auto: false };
      if (/d.bito por deuda/.test(s)) return { categoria: 'devolucion', auto: false };
      if (/transferencia enviada/.test(s)) return { categoria: 'pago_proveedor', auto: false };
      if (/transferencia recibida|dinero recibido/.test(s)) return { categoria: 'cobro_venta', auto: false };
      if (/pago de suscripci|^pago |compra mercado/.test(s)) return { categoria: 'gasto', auto: false };
      return { categoria: 'sin_clasificar', auto: false };
    };

    const filas = [];
    for (let i = hRow + 1; i < raw.length; i++) {
      const r = raw[i] || [];
      const fecha = toISO(r[iF]);
      if (!fecha || fecha < '2020-01-01') continue;
      const tipo = String(r[iT] || '').trim();
      if (!tipo) continue;
      const ref = String(r[iR] || '').trim();
      const monto = Math.round(num(r[iM]) * 100) / 100;
      const saldo = num(r[iS]);
      filas.push({ fecha, tipo, ref, monto, saldo });
    }
    if (!filas.length) return res.status(400).json({ error: 'No encontré movimientos en el archivo.' });

    // Continuidad de saldo (más-viejo-primero): saldo[i] == saldo[i-1] + monto[i]
    const breaks = [];
    let prev = ini != null ? ini : Math.round((filas[0].saldo - filas[0].monto) * 100) / 100;
    for (let i = 0; i < filas.length; i++) {
      const esperado = Math.round((prev + filas[i].monto) * 100) / 100;
      if (Math.abs(esperado - filas[i].saldo) > 0.01) breaks.push({ fecha: filas[i].fecha, tipo: filas[i].tipo, saldo: filas[i].saldo, esperado });
      prev = filas[i].saldo;
    }
    if (fin != null && Math.abs(filas[filas.length - 1].saldo - fin) > 0.01) {
      breaks.push({ fecha: 'FINAL', tipo: 'saldo final', saldo: filas[filas.length - 1].saldo, esperado: fin });
    }
    const saldoOk = breaks.length === 0;

    let auto = 0, pendientes = 0;
    const movs = filas.map(f => {
      const { categoria, auto: esAuto } = clasificar(f.tipo);
      if (esAuto) auto++; else pendientes++;
      return {
        cuenta_id: cuentaId,
        fecha: f.fecha,
        monto: f.monto,
        origen: 'mp_account_statement',
        referencia_externa: `${f.fecha}|${f.ref}|${f.monto.toFixed(2)}|${f.saldo.toFixed(2)}`,
        categoria,
        descripcion: (f.tipo + (f.ref ? ' · ' + f.ref : '')).slice(0, 300),
        conciliado_auto: esAuto
      };
    });

    let importados = 0;
    for (let i = 0; i < movs.length; i += 500) {
      const lote = movs.slice(i, i + 500);
      await sbUpsert('movimientos', lote, 'origen,referencia_externa');
      importados += lote.length;
    }

    const fechas = filas.map(f => f.fecha).sort();
    res.json({
      ok: true,
      importados, auto, pendientes,
      rango: { desde: fechas[0], hasta: fechas[fechas.length - 1] },
      saldo_check: { ok: saldoOk, breaks: breaks.slice(0, 5), total_breaks: breaks.length }
    });
  } catch (e) {
    console.error('MP import:', e);
    res.status(500).json({ error: e.message });
  }
});

// ── Conciliación: crear / borrar vínculo movimiento ↔ operación ──────────
// vinculos.monto es la MAGNITUD POSITIVA imputada (convención del sistema:
// v_gastos_ap / v_compras_ap / v_ventas_ar restan Σ vínculos, y
// v_movimientos_estado usa abs(monto) − Σ vínculos). Ver ADARA-SCHEMA.md.
app.post('/vincular', async (req, res) => {
  try {
    const { movimiento_id, op_tipo, op_id, monto } = req.body || {};
    const tiposOk = ['venta', 'venta_ml', 'compra', 'gasto', 'reclamo', 'transferencia', 'ajuste'];
    if (!movimiento_id || !op_tipo || !op_id) return res.status(400).json({ error: 'Faltan movimiento_id, op_tipo u op_id' });
    if (!tiposOk.includes(op_tipo)) return res.status(400).json({ error: 'op_tipo inválido' });
    const m = Math.round(Number(monto) * 100) / 100;
    if (!(m > 0)) return res.status(400).json({ error: 'El monto del vínculo debe ser > 0' });

    const row = await sbUpsert('vinculos', {
      movimiento_id, op_tipo, op_id, monto: m
    }, 'movimiento_id,op_tipo,op_id');
    res.json({ ok: true, vinculo: Array.isArray(row) ? row[0] : row });
  } catch (e) {
    console.error('vincular:', e);
    res.status(500).json({ error: e.message });
  }
});

// Conciliación en lote: inserta muchos vínculos de una sola vez (para "Conciliar
// todas"). Recibe { vinculos: [{ movimiento_id, op_tipo, op_id, monto }, ...] }.
// Valida cada uno igual que /vincular y hace un único upsert (idempotente por
// la clave movimiento_id,op_tipo,op_id, así que reintentar no duplica).
app.post('/vincular-lote', async (req, res) => {
  try {
    const { vinculos } = req.body || {};
    if (!Array.isArray(vinculos) || !vinculos.length) {
      return res.status(400).json({ error: 'Faltan vínculos' });
    }
    const tiposOk = ['venta', 'venta_ml', 'compra', 'gasto', 'reclamo', 'transferencia', 'ajuste'];
    const lote = [];
    for (const v of vinculos) {
      const { movimiento_id, op_tipo, op_id, monto } = v || {};
      if (!movimiento_id || !op_tipo || !op_id) return res.status(400).json({ error: 'Vínculo incompleto en el lote' });
      if (!tiposOk.includes(op_tipo)) return res.status(400).json({ error: 'op_tipo inválido: ' + op_tipo });
      const m = Math.round(Number(monto) * 100) / 100;
      if (!(m > 0)) return res.status(400).json({ error: 'monto inválido en el lote' });
      lote.push({ movimiento_id, op_tipo, op_id, monto: m });
    }
    const rows = await sbUpsert('vinculos', lote, 'movimiento_id,op_tipo,op_id');
    res.json({ ok: true, insertados: Array.isArray(rows) ? rows.length : lote.length });
  } catch (e) {
    console.error('vincular-lote:', e);
    res.status(500).json({ error: e.message });
  }
});

app.delete('/vincular/:id', async (req, res) => {
  try {
    await sb('DELETE', 'vinculos', null, `id=eq.${req.params.id}`);
    res.json({ ok: true });
  } catch (e) {
    console.error('desvincular:', e);
    res.status(500).json({ error: e.message });
  }
});

// ── Devoluciones: resolución de bundles del Account Statement por op_id ───
// Cada devolución del AS comparte un op_id (campo 2 de referencia_externa:
// "fecha|op_id|monto|saldo") que es el ID de la LIQUIDACIÓN. Ese op_id NO es
// necesariamente el mp_payment_id de la venta: suele ser un source_id distinto.
// Resolución en capas (validada 212/213 contra datos reales; ver
// ADARA-CANCELACIONES-DEVOLUCIONES.md §"Match en capas"):
//   Capa 1: op_id == ventas_ml.mp_payment_id (o dentro de mp_payment_ids)
//   Capa 2: op_id == retenciones.mp_source_id → COALESCE(retenciones.venta_id,
//           ventas_ml por ml_order_id = retenciones.order_id)
//   Capa 3: op_id < 12 dígitos → agregado ML (facturas vencidas, reintegros
//           batcheados) → cola "Cargos ML" (no es devolución de venta)
//   Capa 4: resto → cola "Revisión" (residual, típicamente venta 2025 pre-snapshot)
// Match SIEMPRE por op_id, NUNCA por monto. vinculos.monto = abs(monto) (>0).
const _r2    = (n) => Math.round(Number(n) * 100) / 100;
const _chunk = (arr, n) => { const o = []; for (let i = 0; i < arr.length; i += n) o.push(arr.slice(i, i + n)); return o; };
const _opId  = (ref) => String(ref || '').split('|')[1] || '';

app.get('/devoluciones/resolver', async (_req, res) => {
  try {
    // 1. Traer TODAS las líneas categoría 'devolucion' (paginado: cap silencioso de 1000)
    let movs = [], off = 0; const PAGE = 1000;
    while (true) {
      const r = await sbGet('movimientos',
        `categoria=eq.devolucion&order=id.asc&limit=${PAGE}&offset=${off}` +
        `&select=id,fecha,monto,descripcion,categoria,referencia_externa`);
      movs = movs.concat(r);
      if (r.length < PAGE) break;
      off += PAGE;
    }

    // 2. Agrupar por op_id
    const bundles = new Map();
    for (const m of movs) {
      const op = _opId(m.referencia_externa);
      if (!op) continue;
      if (!bundles.has(op)) bundles.set(op, []);
      bundles.get(op).push(m);
    }
    const opIds = [...bundles.keys()];

    // 3. Líneas ya vinculadas a una venta (op_tipo=venta_ml)
    const vincSet = new Set();
    for (const ch of _chunk(movs.map(m => m.id), 200)) {
      if (!ch.length) continue;
      const vs = await sbGet('vinculos',
        `op_tipo=eq.venta_ml&movimiento_id=in.(${ch.join(',')})&select=movimiento_id`);
      vs.forEach(v => vincSet.add(v.movimiento_id));
    }

    // 4. CAPA 1 — op_id == ventas_ml.mp_payment_id
    const ventaByOp = new Map();
    for (const ch of _chunk(opIds, 100)) {
      if (!ch.length) continue;
      const vs = await sbGet('ventas_ml',
        `mp_payment_id=in.(${ch.join(',')})` +
        `&select=id,ml_order_id,mp_payment_id,fecha,por_cobrar,devuelta`);
      vs.forEach(v => ventaByOp.set(String(v.mp_payment_id), { v, capa: 1 }));
    }

    // 5. CAPA 2 — puente por retenciones (mp_source_id) → venta_id u order_id
    const pend = opIds.filter(op => !ventaByOp.has(op));
    const retByOp = new Map();
    for (const ch of _chunk(pend, 100)) {
      if (!ch.length) continue;
      const rs = await sbGet('retenciones',
        `mp_source_id=in.(${ch.join(',')})&select=mp_source_id,venta_id,order_id`);
      rs.forEach(r => {
        const cur = retByOp.get(r.mp_source_id) || { venta_id: null, order_id: null };
        if (!cur.venta_id && r.venta_id) cur.venta_id = r.venta_id;
        if (!cur.order_id && r.order_id) cur.order_id = r.order_id;
        retByOp.set(r.mp_source_id, cur);
      });
    }
    // 5a. resolver venta por venta_id directo
    const vIds = [...new Set([...retByOp.values()].map(x => x.venta_id).filter(Boolean))];
    const ventaById = new Map();
    for (const ch of _chunk(vIds, 100)) {
      if (!ch.length) continue;
      const vs = await sbGet('ventas_ml',
        `id=in.(${ch.join(',')})&select=id,ml_order_id,mp_payment_id,fecha,por_cobrar,devuelta`);
      vs.forEach(v => ventaById.set(v.id, v));
    }
    // 5b. resolver venta por order_id (cuando retenciones.venta_id es null)
    const ords = [...new Set([...retByOp.values()].filter(x => !x.venta_id && x.order_id).map(x => String(x.order_id)))];
    const ventaByOrder = new Map();
    for (const ch of _chunk(ords, 100)) {
      if (!ch.length) continue;
      const vs = await sbGet('ventas_ml',
        `ml_order_id=in.(${ch.join(',')})&select=id,ml_order_id,mp_payment_id,fecha,por_cobrar,devuelta`);
      vs.forEach(v => ventaByOrder.set(String(v.ml_order_id), v));
    }
    for (const op of pend) {
      const r = retByOp.get(op);
      if (!r) continue;
      let v = null;
      if (r.venta_id && ventaById.has(r.venta_id)) v = ventaById.get(r.venta_id);
      else if (r.order_id && ventaByOrder.has(String(r.order_id))) v = ventaByOrder.get(String(r.order_id));
      if (v) ventaByOp.set(op, { v, capa: 2 });
    }

    // 6. Armar bundles clasificados
    const out = [];
    for (const [op, lineas] of bundles) {
      const neto    = _r2(lineas.reduce((s, l) => s + Number(l.monto), 0));
      const algVinc = lineas.some(l => vincSet.has(l.id));
      const todVinc = lineas.every(l => vincSet.has(l.id));
      const hit     = ventaByOp.get(op) || null;
      let estado, capa;
      if (hit)                 { capa = hit.capa; estado = todVinc ? 'vinculada' : (algVinc ? 'parcial' : 'pendiente'); }
      else if (op.length < 12) { capa = 3; estado = 'agregado'; }
      else                     { capa = 4; estado = 'revision'; }
      out.push({
        op_id: op, neto, capa, estado,
        venta: hit ? {
          id: hit.v.id, ml_order_id: hit.v.ml_order_id, mp_payment_id: hit.v.mp_payment_id,
          fecha: hit.v.fecha, por_cobrar: hit.v.por_cobrar, devuelta: hit.v.devuelta
        } : null,
        lineas: lineas
          .slice()
          .sort((a, b) => String(a.fecha).localeCompare(String(b.fecha)))
          .map(l => ({ id: l.id, fecha: l.fecha, monto: Number(l.monto), descripcion: l.descripcion, ya_vinculada: vincSet.has(l.id) }))
      });
    }
    const ord = { pendiente: 0, parcial: 1, vinculada: 2, agregado: 3, revision: 4 };
    out.sort((a, b) => (ord[a.estado] - ord[b.estado]) || (a.op_id < b.op_id ? -1 : 1));

    const cnt = (e) => out.filter(b => b.estado === e).length;
    res.json({
      ok: true,
      resumen: {
        total: out.length, pendiente: cnt('pendiente'), parcial: cnt('parcial'),
        vinculada: cnt('vinculada'), agregado: cnt('agregado'), revision: cnt('revision')
      },
      bundles: out
    });
  } catch (e) {
    console.error('devoluciones/resolver:', e);
    res.status(500).json({ error: e.message });
  }
});

// Vincula TODAS las líneas de devolución de un bundle (op_id) a su venta.
// Recibe { op_id, venta_id }. Recalcula montos desde movimientos (fuente de
// verdad, precisión al centavo), inserta vínculos op_tipo='venta_ml' con
// monto = abs(monto) vía upsert idempotente (reintentar no duplica), y marca la
// venta devuelta SOLO si el neto del bundle < 0 (pérdida real de plata; neto 0 =
// retención + re-crédito que se cancelan, no hubo devolución efectiva).
app.post('/devoluciones/vincular', async (req, res) => {
  try {
    const { op_id, venta_id } = req.body || {};
    if (!op_id || !venta_id) return res.status(400).json({ error: 'Faltan op_id o venta_id' });

    const movs = await sbGet('movimientos',
      `categoria=eq.devolucion&referencia_externa=like.*${op_id}*` +
      `&select=id,monto,referencia_externa&limit=1000`);
    const lineas = movs.filter(m => _opId(m.referencia_externa) === String(op_id));
    if (!lineas.length) return res.status(404).json({ error: 'Sin líneas de devolución para ese op_id' });

    const vinculos = lineas
      .map(l => ({ movimiento_id: l.id, op_tipo: 'venta_ml', op_id: venta_id, monto: _r2(Math.abs(Number(l.monto))) }))
      .filter(v => v.monto > 0);
    if (vinculos.length) await sbUpsert('vinculos', vinculos, 'movimiento_id,op_tipo,op_id');

    const neto = _r2(lineas.reduce((s, l) => s + Number(l.monto), 0));
    let marcada = false;
    if (neto < 0) {
      await sbPatch('ventas_ml', `id=eq.${venta_id}`, { devuelta: true, monto_reembolso: Math.abs(neto) });
      marcada = true;
    }
    res.json({ ok: true, vinculados: vinculos.length, neto, marcada_devuelta: marcada });
  } catch (e) {
    console.error('devoluciones/vincular:', e);
    res.status(500).json({ error: e.message });
  }
});

// Detalle completo de una venta: junta toda su "vida de plata" en una llamada.
// Devuelve { venta, cobros[], devoluciones[], retenciones[] }:
//   cobros        = movimientos categoria 'cobro_venta' vinculados a la venta
//   devoluciones  = movimientos de devolución/cancelación vinculados a la venta
//   retenciones   = filas de la tabla retenciones por venta_id / order_id / mp_source_id
// No modifica nada (solo lectura).
app.get('/venta/:id/detalle', async (req, res) => {
  try {
    const id = req.params.id;
    const vs = await sbGet('ventas_ml', `id=eq.${id}&limit=1`);
    const venta = vs[0];
    if (!venta) return res.status(404).json({ error: 'Venta no encontrada' });

    // Movimientos vinculados a la venta (op_tipo='venta_ml', op_id=venta.id)
    const vincs = await sbGet('vinculos', `op_tipo=eq.venta_ml&op_id=eq.${id}&select=movimiento_id`);
    let movs = [];
    for (const ch of _chunk(vincs.map(v => v.movimiento_id), 200)) {
      if (!ch.length) continue;
      const r = await sbGet('movimientos',
        `id=in.(${ch.join(',')})&select=id,fecha,categoria,monto,descripcion`);
      movs = movs.concat(r);
    }
    const byFecha = (a, b) => String(a.fecha).localeCompare(String(b.fecha));
    const DEVCATS = ['devolucion', 'venta_cancelada', 'cargo_envio_devolucion'];
    const cobros       = movs.filter(m => m.categoria === 'cobro_venta').sort(byFecha);
    const devoluciones = movs.filter(m => DEVCATS.includes(m.categoria)).sort(byFecha);

    // Retenciones: por venta_id, por orden, o por source del pago (dedup por id)
    const retSel = 'select=id,tipo,jurisdiccion,detail,monto,fecha,mp_source_id,order_id';
    const retMap = new Map();
    const addRets = rows => rows.forEach(r => { if (!retMap.has(r.id)) retMap.set(r.id, r); });
    addRets(await sbGet('retenciones', `venta_id=eq.${id}&${retSel}`));
    if (venta.ml_order_id)  addRets(await sbGet('retenciones', `order_id=eq.${encodeURIComponent(venta.ml_order_id)}&${retSel}`));
    if (venta.mp_payment_id) addRets(await sbGet('retenciones', `mp_source_id=eq.${encodeURIComponent(venta.mp_payment_id)}&${retSel}`));
    const retenciones = [...retMap.values()].sort((a, b) => String(a.tipo || '').localeCompare(String(b.tipo || '')));

    res.json({ ok: true, venta, cobros, devoluciones, retenciones });
  } catch (e) {
    console.error('venta/detalle:', e);
    res.status(500).json({ error: e.message });
  }
});

// ── Conciliación: transferencia interna entre cuentas propias (CB8) ──────
// Empareja dos movimientos (salida en una cuenta, entrada en otra) con vínculos
// cruzados op_tipo='transferencia' (op_id = el OTRO movimiento). Cada lado
// concilia contra el otro y NO impacta P&L ni AR/AP. Si no hay contrapartida en
// el sistema, se marca "interna sin par" (vínculo a sí mismo).
app.post('/transferencia-interna', async (req, res) => {
  try {
    const { movimiento_a, movimiento_b } = req.body || {};
    if (!movimiento_a) return res.status(400).json({ error: 'Falta movimiento_a' });

    const ids = [movimiento_a, movimiento_b].filter(Boolean).join(',');
    const movs = await sbGet('movimientos', `id=in.(${ids})&select=id,monto`);
    const ma = movs.find(m => String(m.id) === String(movimiento_a));
    if (!ma) return res.status(404).json({ error: 'No existe movimiento_a' });
    const mag = x => Math.round(Math.abs(Number(x)) * 100) / 100;

    if (movimiento_b) {
      const mb = movs.find(m => String(m.id) === String(movimiento_b));
      if (!mb) return res.status(404).json({ error: 'No existe movimiento_b' });
      await sbUpsert('vinculos', [
        { movimiento_id: movimiento_a, op_tipo: 'transferencia', op_id: movimiento_b, monto: mag(ma.monto) },
        { movimiento_id: movimiento_b, op_tipo: 'transferencia', op_id: movimiento_a, monto: mag(mb.monto) },
      ], 'movimiento_id,op_tipo,op_id');
    } else {
      // Interna sin contrapartida en el sistema → vínculo a sí mismo
      await sbUpsert('vinculos', {
        movimiento_id: movimiento_a, op_tipo: 'transferencia', op_id: movimiento_a, monto: mag(ma.monto)
      }, 'movimiento_id,op_tipo,op_id');
    }
    res.json({ ok: true });
  } catch (e) {
    console.error('transferencia interna:', e);
    res.status(500).json({ error: e.message });
  }
});

// ── Proveedores: alta rápida (desde Compras o Gastos) ───────────────────
app.post('/proveedores', async (req, res) => {
  try {
    const { nombre, cuit } = req.body || {};
    if (!nombre || !String(nombre).trim()) return res.status(400).json({ error: 'Falta el nombre del proveedor' });
    const nombreClean = String(nombre).trim().replace(/\s+/g, ' ');
    const cuitNorm = String(cuit || '').replace(/\D/g, '');
    // El CUIT es OPCIONAL (proveedores informales / compras sin factura). Si se carga, debe ser válido.
    if (cuitNorm.length && cuitNorm.length !== 11) {
      return res.status(400).json({ error: 'Si cargás CUIT, debe tener 11 dígitos' });
    }
    // Dedup: con CUIT, por CUIT; sin CUIT, por nombre normalizado (solo entre los que tampoco tienen CUIT),
    // para no duplicar el mismo proveedor informal. ilike sin comodines = match exacto case-insensitive.
    let existentes;
    if (cuitNorm.length === 11) {
      existentes = await sbGet('proveedores', `cuit=eq.${cuitNorm}`);
    } else {
      existentes = await sbGet('proveedores', `cuit=is.null&nombre=ilike.${encodeURIComponent(nombreClean)}`);
    }
    if (existentes && existentes[0]) return res.json(existentes[0]);
    const ins = await sbUpsert('proveedores', {
      nombre: nombreClean,
      cuit: cuitNorm.length === 11 ? cuitNorm : null,
      activo: true
    });
    const p = Array.isArray(ins) ? ins[0] : ins;
    res.json(p);
  } catch (e) {
    console.error('POST /proveedores:', e);
    res.status(500).json({ error: e.message });
  }
});

// ── Compras LOCALES: alta atómica (cabecera + componentes + lotes) ──────
// Solo tipo='local', moneda='ARS'. Las importaciones (USD, prorrateo de
// nacionalización) son otro flujo, pendiente. Ver ADARA-COMPRAS-IMPORTACIONES.md.
// El costo del lote = SOLO los productos (neto). IVA / IIBB / Ganancias son
// componentes fiscales (crédito), entran en el total a pagar pero NO en el costo.
app.post('/compras', async (req, res) => {
  const round2 = n => Math.round((Number(n) || 0) * 100) / 100;
  let compraId = null;
  try {
    const { compra, items, fiscales, gastos } = req.body || {};
    if (!compra) return res.status(400).json({ error: 'Falta el objeto compra' });
    for (const f of ['fecha', 'linea_id']) {
      if (compra[f] === undefined || compra[f] === null || compra[f] === '') {
        return res.status(400).json({ error: `Falta ${f}` });
      }
    }

    // Moneda de la factura. USD requiere TC; el costo del lote se congela en ARS al TC del día.
    const moneda = compra.moneda === 'USD' ? 'USD' : 'ARS';
    const tc = round2(compra.tc_blue);
    if (moneda === 'USD' && !(tc > 0)) {
      return res.status(400).json({ error: 'Compra en USD: falta el tipo de cambio (tc_blue)' });
    }
    // Lleva un monto de la moneda de la factura a ARS (para el costo del lote, que vive en ARS).
    const aARS = m => moneda === 'USD' ? round2(round2(m) * tc) : round2(m);

    const prods = (Array.isArray(items) ? items : [])
      .filter(x => x && x.sku_id && Number(x.cantidad) > 0 && Number(x.costo_unitario) >= 0)
      .map(x => ({ sku_id: +x.sku_id, cantidad: Number(x.cantidad), costo_unitario: round2(x.costo_unitario), extra: round2(x.extra_directo) }));
    if (!prods.length) return res.status(400).json({ error: 'Cargá al menos un producto con cantidad y costo' });

    // Gastos que van al COSTO del lote pero NO son crédito fiscal ni deuda con el proveedor del
    // producto (flete/comisión/despacho a terceros). Dos formas, que conviven:
    //   - extra_directo por producto: suma sólo a ese lote (p.extra, total de la línea).
    //   - prorrateables compartidos: se reparten entre todos por criterio (costo neto / unidades).
    // Todo en la moneda de la factura; el lote se congela a ARS al TC. Ver ADARA-COMPRAS-IMPORTACIONES.md.
    const gastosIn = (gastos && Array.isArray(gastos.prorrateables)) ? gastos.prorrateables : [];
    const prorrateables = gastosIn
      .map(g => ({ concepto: (g && g.concepto) ? String(g.concepto).trim() : null, monto: round2(g && g.monto) }))
      .filter(g => g.monto > 0);
    const criterio = (gastos && gastos.criterio === 'unidades') ? 'unidades' : 'costo';
    const totalProrr = round2(prorrateables.reduce((acc, g) => acc + g.monto, 0));
    const sumBase = prods.reduce((acc, p) => acc + p.costo_unitario * p.cantidad, 0);
    const sumCant = prods.reduce((acc, p) => acc + p.cantidad, 0);
    // Base del reparto: por costo neto (default) o por unidades. Si la base de costo es 0, cae a unidades.
    const usarCosto = criterio === 'costo' && sumBase > 0;
    const baseReparto = usarCosto ? sumBase : sumCant;
    // Costo unitario FINAL de cada lote (moneda factura): base + extra/u + prorrateo/u, redondeado a ARS una sola vez.
    const loteCosto = prods.map(p => {
      const baseLinea = p.costo_unitario * p.cantidad;
      const peso = baseReparto > 0 ? (usarCosto ? baseLinea : p.cantidad) / baseReparto : 0;
      const totalLinea = baseLinea + p.extra + totalProrr * peso;   // moneda de la factura
      const unitInvoice = totalLinea / p.cantidad;
      const unitARS = moneda === 'USD' ? round2(unitInvoice * tc) : round2(unitInvoice);
      return { sku_id: p.sku_id, cantidad: p.cantidad, unitARS };
    });

    // Componentes fiscales: IVA (automático) + N percepciones (IIBB/Ganancias) por jurisdicción.
    // Se guardan en la moneda de la factura; v_compras_ap los lleva a ARS con tc_blue.
    const fisc = [];
    const ivaMonto = round2(fiscales && fiscales.iva);
    if (ivaMonto > 0) fisc.push({ tipo: 'iva', monto: ivaMonto, descripcion: null });
    const PERC_MAP = { iibb: 'iibb_percepcion', ganancias: 'ganancias_percepcion' };
    const perceps = Array.isArray(fiscales && fiscales.percepciones) ? fiscales.percepciones : [];
    for (const p of perceps) {
      const tipo = PERC_MAP[p && p.tipo];
      const monto = round2(p && p.monto);
      if (tipo && monto !== 0) {
        fisc.push({ tipo, monto, descripcion: (p.jurisdiccion ? String(p.jurisdiccion).trim() : '') || null });
      }
    }

    // N° de factura → columna dedicada. Puede quedar NULL (factura diferida: se asigna luego).
    const nroFactura = compra.nro_factura ? String(compra.nro_factura).trim() : null;
    const notas = compra.notas ? String(compra.notas).trim() : null;

    // 1) Cabecera
    const insCompra = await sbUpsert('compras', {
      proveedor_id: compra.proveedor_id || null,
      linea_id: compra.linea_id,
      tipo: 'local',
      moneda,
      tc_blue: moneda === 'USD' ? tc : null,
      fecha: compra.fecha,
      estado: 'activa',
      nro_factura: nroFactura,
      notas
    });
    const c = Array.isArray(insCompra) ? insCompra[0] : insCompra;
    if (!c || !c.id) throw new Error('No se obtuvo el id de la compra');
    compraId = c.id;

    // 2) Componentes de producto (en la moneda de la factura) + lotes (costo unitario en ARS, congelado al TC)
    await sbUpsert('compra_componentes', prods.map(p => ({
      compra_id: compraId, tipo: 'producto', sku_id: p.sku_id,
      cantidad: p.cantidad, moneda, monto: round2(p.costo_unitario * p.cantidad)
    })));
    await sbUpsert('lotes', loteCosto.map(p => ({
      sku_id: p.sku_id, compra_id: compraId,
      cantidad_inicial: p.cantidad, cantidad_actual: p.cantidad,
      costo_unitario: p.unitARS, fecha_alta: compra.fecha
    })));

    // 3) Componentes fiscales (crédito — no suman al costo del lote), en la moneda de la factura
    if (fisc.length) {
      await sbUpsert('compra_componentes', fisc.map(x => ({
        compra_id: compraId, tipo: x.tipo, moneda, monto: x.monto, descripcion: x.descripcion
      })));
    }

    // 3b) Costos no-proveedor: van al COSTO del lote (ya están dentro de costo_unitario) pero NO a
    //     la cuenta corriente del proveedor. Se guardan como rastro auditable. v_compras_ap los excluye.
    const costoExtra = [];
    for (const p of prods) {
      if (p.extra > 0) costoExtra.push({ compra_id: compraId, tipo: 'extra_directo', clase: 'directo', sku_id: p.sku_id, moneda, monto: p.extra, descripcion: null });
    }
    for (const g of prorrateables) {
      costoExtra.push({ compra_id: compraId, tipo: 'gasto_prorrateable', clase: criterio, moneda, monto: g.monto, descripcion: g.concepto });
    }
    if (costoExtra.length) await sbUpsert('compra_componentes', costoExtra);

    res.json({ ok: true, compra_id: compraId });
  } catch (e) {
    console.error('POST /compras:', e);
    if (compraId) { // rollback best-effort
      try { await sb('DELETE', 'lotes', null, `compra_id=eq.${compraId}`); } catch {}
      try { await sb('DELETE', 'compra_componentes', null, `compra_id=eq.${compraId}`); } catch {}
      try { await sb('DELETE', 'compras', null, `id=eq.${compraId}`); } catch {}
    }
    res.status(500).json({ error: e.message });
  }
});

// ── ARCA: consulta de padrón (constancia de inscripción) ────────────────
// CUIT -> razón social, vía WSAA (LoginCms) + servicio ws_sr_constancia_inscripcion.
// Variables de entorno necesarias: ARCA_CERT, ARCA_KEY, ARCA_CUIT.
// El Ticket de Acceso (TA) se cachea en memoria y, si existe la tabla opcional
// arca_ta, también en Supabase (sobrevive a reinicios y evita el error de ARCA
// "el CEE ya posee un TA válido"). Si la tabla no existe, usa solo memoria.
const ARCA_WSAA   = 'https://wsaa.afip.gov.ar/ws/services/LoginCms';
const ARCA_PADRON = 'https://aws.afip.gov.ar/sr-padron/webservices/personaServiceA5';
let ARCA_TA_MEM = null; // { token, sign, exp(ms) }

function cuitValido(cuit) {
  if (!/^\d{11}$/.test(cuit)) return false;
  const m = [5, 4, 3, 2, 7, 6, 5, 4, 3, 2];
  let s = 0; for (let i = 0; i < 10; i++) s += parseInt(cuit[i], 10) * m[i];
  let v = 11 - (s % 11); if (v === 11) v = 0; if (v === 10) v = 9;
  return v === parseInt(cuit[10], 10);
}

function arcaPem(name) {
  const raw = process.env[name];
  if (!raw) return null;
  // Reconstruye un PEM válido aunque Railway haya aplastado los saltos de línea
  // (los convierta en espacios, los elimine, o los deje como \n literales).
  let s = String(raw).trim().replace(/\\r\\n|\\n|\\r/g, '\n').replace(/\r/g, '');
  const m = s.match(/-----BEGIN ([A-Z0-9 ]+)-----([\s\S]*?)-----END \1-----/);
  if (!m) return s; // no parece PEM: dejamos que forge tire el error claro
  const body = (m[2].match(/[A-Za-z0-9+/=]+/g) || []).join('');
  const wrapped = body.replace(/.{1,64}/g, '$&\n').trimEnd();
  return `-----BEGIN ${m[1]}-----\n${wrapped}\n-----END ${m[1]}-----\n`;
}

function arcaBuildTRA(service) {
  const now = Date.now();
  const fmt = d => new Date(d).toISOString().replace(/\.\d{3}Z$/, 'Z'); // UTC
  return `<?xml version="1.0" encoding="UTF-8"?>
<loginTicketRequest version="1.0">
<header><uniqueId>${Math.floor(now / 1000)}</uniqueId><generationTime>${fmt(now - 600000)}</generationTime><expirationTime>${fmt(now + 600000)}</expirationTime></header>
<service>${service}</service>
</loginTicketRequest>`;
}

function arcaSignTRA(tra) {
  const certPem = arcaPem('ARCA_CERT'), keyPem = arcaPem('ARCA_KEY');
  if (!certPem || !keyPem) throw new Error('Faltan ARCA_CERT / ARCA_KEY en las variables de entorno');
  let cert, key;
  try { cert = forge.pki.certificateFromPem(certPem); } catch { throw new Error('ARCA_CERT no es un certificado PEM válido (revisá que se hayan respetado los saltos de línea)'); }
  try { key = forge.pki.privateKeyFromPem(keyPem); } catch { throw new Error('ARCA_KEY no es una clave PEM válida (revisá que se hayan respetado los saltos de línea)'); }
  const p7 = forge.pkcs7.createSignedData();
  p7.content = forge.util.createBuffer(tra, 'utf8');
  p7.addCertificate(cert);
  p7.addSigner({
    key, certificate: cert, digestAlgorithm: forge.pki.oids.sha256,
    authenticatedAttributes: [
      { type: forge.pki.oids.contentType, value: forge.pki.oids.data },
      { type: forge.pki.oids.messageDigest },
      { type: forge.pki.oids.signingTime, value: new Date() }
    ]
  });
  p7.sign();
  return forge.util.encode64(forge.asn1.toDer(p7.toAsn1()).getBytes());
}

const arcaUnesc = s => String(s).replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&apos;/g, "'").replace(/&amp;/g, '&');

async function arcaGetTA() {
  // 1) memoria
  if (ARCA_TA_MEM && ARCA_TA_MEM.exp > Date.now() + 60000) return ARCA_TA_MEM;
  // 2) Supabase (si existe la tabla arca_ta)
  try {
    const rows = await sbGet('arca_ta', 'id=eq.1');
    if (rows && rows[0] && rows[0].expiration && Date.parse(rows[0].expiration) > Date.now() + 60000) {
      ARCA_TA_MEM = { token: rows[0].token, sign: rows[0].sign, exp: Date.parse(rows[0].expiration) };
      return ARCA_TA_MEM;
    }
  } catch (_) { /* tabla inexistente: seguimos solo con memoria */ }

  // 3) login nuevo
  const cms = arcaSignTRA(arcaBuildTRA('ws_sr_constancia_inscripcion'));
  const env = `<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:wsaa="http://wsaa.view.sua.dvadac.desein.afip.gov">
<soapenv:Header/><soapenv:Body><wsaa:loginCms><wsaa:in0>${cms}</wsaa:in0></wsaa:loginCms></soapenv:Body></soapenv:Envelope>`;
  const r = await fetch(ARCA_WSAA, { method: 'POST', headers: { 'Content-Type': 'text/xml; charset=utf-8', 'SOAPAction': '' }, body: env });
  const xml = await r.text();
  const fault = xml.match(/<faultstring>([\s\S]*?)<\/faultstring>/i);
  if (fault) throw new Error('WSAA rechazó el login: ' + arcaUnesc(fault[1]).trim());
  const ta = arcaUnesc(xml);
  const token = (ta.match(/<token>([^<]+)<\/token>/) || [])[1];
  const sign = (ta.match(/<sign>([^<]+)<\/sign>/) || [])[1];
  const exp = (ta.match(/<expirationTime>([^<]+)<\/expirationTime>/) || [])[1];
  if (!token || !sign) throw new Error('WSAA no devolvió token/sign. Respuesta: ' + xml.slice(0, 300));
  ARCA_TA_MEM = { token, sign, exp: exp ? Date.parse(exp) : Date.now() + 11 * 3600 * 1000 };
  try { await sbUpsert('arca_ta', { id: 1, token, sign, expiration: new Date(ARCA_TA_MEM.exp).toISOString() }, 'id'); } catch (_) {}
  return ARCA_TA_MEM;
}

// Diagnóstico seguro de las variables ARCA (no expone el cuerpo de la clave)
app.get('/padron-diag', (req, res) => {
  const out = {};
  for (const n of ['ARCA_CERT', 'ARCA_KEY']) {
    const raw = process.env[n];
    if (!raw) { out[n] = { presente: false }; continue; }
    const s = String(raw);
    const norm = arcaPem(n);
    const m = String(norm).match(/-----BEGIN ([A-Z0-9 ]+)-----/);
    let parse = 'no intentado';
    try {
      if (n === 'ARCA_KEY') forge.pki.privateKeyFromPem(norm);
      else forge.pki.certificateFromPem(norm);
      parse = 'OK';
    } catch (e) { parse = 'ERROR: ' + e.message; }
    out[n] = {
      presente: true,
      largo: s.length,
      tieneBEGIN: /-----BEGIN/.test(s),
      tieneEND: /-----END/.test(s),
      saltos_reales: (s.match(/\n/g) || []).length,
      barra_n_literales: (s.match(/\\n/g) || []).length,
      tipo_detectado: m ? m[1] : null,
      parseForge: parse
    };
  }
  out.ARCA_CUIT = process.env.ARCA_CUIT || null;
  res.json(out);
});

app.get('/padron/:cuit', async (req, res) => {
  try {
    const cuit = String(req.params.cuit).replace(/\D/g, '');
    if (!cuitValido(cuit)) return res.status(400).json({ error: 'CUIT inválido (no pasa el dígito verificador)', cuit });
    const repres = (process.env.ARCA_CUIT || '').replace(/\D/g, '');
    if (!repres) return res.status(500).json({ error: 'Falta ARCA_CUIT en las variables de entorno' });

    const ta = await arcaGetTA();
    const env = `<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:a5="http://a5.soap.ws.server.puc.sr/">
<soapenv:Header/><soapenv:Body><a5:getPersona_v2>
<token>${ta.token}</token><sign>${ta.sign}</sign>
<cuitRepresentada>${repres}</cuitRepresentada><idPersona>${cuit}</idPersona>
</a5:getPersona_v2></soapenv:Body></soapenv:Envelope>`;
    const r = await fetch(ARCA_PADRON, { method: 'POST', headers: { 'Content-Type': 'text/xml; charset=utf-8', 'SOAPAction': '' }, body: env });
    const xml = await r.text();

    const fault = xml.match(/<faultstring>([\s\S]*?)<\/faultstring>/i);
    if (fault) {
      const msg = arcaUnesc(fault[1]).trim();
      if (/no existe persona/i.test(msg)) return res.status(404).json({ error: 'No se encontró ese CUIT en el padrón', cuit });
      return res.status(502).json({ error: 'ARCA (padrón) devolvió: ' + msg });
    }

    const grab = t => { const m = xml.match(new RegExp(`<${t}>([^<]*)</${t}>`)); return m ? m[1].trim() : null; };
    const razon = grab('razonSocial');
    const nombre = grab('nombre'), apellido = grab('apellido');
    const display = razon || [apellido, nombre].filter(Boolean).join(', ') || null;
    if (!display) return res.status(404).json({ error: 'No se pudo leer la razón social del padrón', cuit, raw: xml.slice(0, 400) });

    res.json({ cuit, nombre: display, tipoPersona: grab('tipoPersona'), estado: grab('estadoClave') });
  } catch (e) {
    console.error('GET /padron:', e);
    res.status(502).json({ error: e.message });
  }
});

// ── Compras: anular (soft-delete con reversa de stock) ──────────────────
// Marca la compra como 'anulada', borra sus lotes (revierte el stock) y sus
// componentes. La cabecera queda como tombstone. Bloquea si ya se vendió stock
// de esos lotes o si tiene pagos vinculados (hay que desvincular primero).
app.post('/compras/:id/anular', async (req, res) => {
  try {
    const id = +req.params.id;
    if (!id) return res.status(400).json({ error: 'Falta el id de la compra' });
    const { motivo } = req.body || {};

    const lotes = await sbGet('lotes', `compra_id=eq.${id}&select=id,cantidad_inicial,cantidad_actual`);
    if ((lotes || []).some(l => Number(l.cantidad_actual) < Number(l.cantidad_inicial))) {
      return res.status(409).json({ error: 'No se puede anular: ya se vendió/consumió stock de esta compra' });
    }
    const vinc = await sbGet('vinculos', `op_tipo=eq.compra&op_id=eq.${id}&select=id`);
    if (vinc && vinc.length) {
      return res.status(409).json({ error: 'No se puede anular: tiene pagos vinculados. Desvinculá primero desde Conciliación.' });
    }

    await sbPatch('compras', `id=eq.${id}`, { estado: 'anulada', motivo: motivo ? String(motivo).slice(0, 500) : 'anulada manualmente' });
    await sb('DELETE', 'lotes', null, `compra_id=eq.${id}`);
    await sb('DELETE', 'compra_componentes', null, `compra_id=eq.${id}`);
    res.json({ ok: true });
  } catch (e) {
    console.error('POST /compras/:id/anular:', e);
    res.status(500).json({ error: e.message });
  }
});

// Asignar / actualizar el N° de factura de una compra (factura diferida).
// Edición acotada: solo toca nro_factura, no recalcula lotes/costos/IVA.
app.post('/compras/:id/factura', async (req, res) => {
  try {
    const id = +req.params.id;
    if (!id) return res.status(400).json({ error: 'Falta el id de la compra' });
    const nro = req.body && req.body.nro_factura ? String(req.body.nro_factura).trim() : '';
    if (!nro) return res.status(400).json({ error: 'Falta el número de factura' });

    const rows = await sbGet('compras', `id=eq.${id}&select=id,estado`);
    const compra = rows && rows[0];
    if (!compra) return res.status(404).json({ error: 'Compra no encontrada' });
    if (compra.estado === 'anulada') return res.status(409).json({ error: 'La compra está anulada' });

    await sbPatch('compras', `id=eq.${id}`, { nro_factura: nro });
    res.json({ ok: true });
  } catch (e) {
    console.error('POST /compras/:id/factura:', e);
    res.status(500).json({ error: e.message });
  }
});


// ── START ────────────────────────────────────────────────────────────
app.listen(PORT, async () => {
  console.log(`\n🚀 ADARA Backend corriendo — puerto ${PORT}`);
  console.log(`   Supabase : ${SUPABASE_URL  ? '✓' : '✗ FALTA variable SUPABASE_URL'}`);
  console.log(`   ML keys  : ${ML_CLIENT_ID  ? '✓' : '✗ FALTA variable ML_CLIENT_ID'}`);
  console.log(`   Tango    : ${TF_APP_KEY    ? '✓' : '✗ FALTA variable TF_APP_KEY (opcional)'}`);
  await loadMLToken();
  await loadFeriados(new Date().getFullYear());
});
