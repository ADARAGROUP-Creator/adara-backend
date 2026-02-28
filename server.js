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

const app    = express();
const upload = multer({ storage: multer.memoryStorage() });

app.use(cors());
app.use(express.json({ limit: '20mb' }));

// ─── Variables de entorno (se cargan desde Railway) ─────────────────
const {
  SUPABASE_URL,
  SUPABASE_KEY,
  ML_CLIENT_ID,
  ML_CLIENT_SECRET,
  ML_REDIRECT_URI,
  TF_APP_KEY,
  TF_USERNAME,
  TF_PASSWORD,
  TF_USER_ID,
  PORT = 3000
} = process.env;

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
const sbUpsert = (t, body) => sb('POST',  t, body);
const sbPatch  = (t, q, b) => sb('PATCH', t, b, q);

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

async function refreshML() {
  if (!ML.refresh) return;
  const r    = await fetch('https://api.mercadolibre.com/oauth/token', {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ grant_type: 'refresh_token', client_id: ML_CLIENT_ID, client_secret: ML_CLIENT_SECRET, refresh_token: ML.refresh })
  });
  const data = await r.json();
  if (data.access_token) {
    await saveMLToken({ access: data.access_token, refresh: data.refresh_token, expires: Date.now() + data.expires_in * 1000 });
    console.log('✓ ML token refrescado');
  }
}

async function mlGet(path) {
  if (Date.now() > ML.expires - 60000) await refreshML();
  const r = await fetch(`https://api.mercadolibre.com${path}`, {
    headers: { 'Authorization': `Bearer ${ML.access}` }
  });
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
app.get('/', (_, res) => res.json({ ok: true, servicio: 'ADARA Backend', ts: new Date().toISOString() }));

app.get('/health', async (_, res) => {
  const c = { supabase: false, ml_token: false, tango: false };
  try { await sbGet('lineas_negocio', 'limit=1'); c.supabase = true; } catch (_) {}
  c.ml_token = !!ML.access && Date.now() < ML.expires;
  try { if (TF_APP_KEY) { await getTFToken(); c.tango = true; } } catch (_) {}
  res.json({ ok: Object.values(c).every(Boolean), checks: c });
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
async function syncMLVentas(diasAtras = 7) {
  if (!ML.access) throw new Error('ML no autenticado. Conectá ML primero desde la app.');

  const me       = await mlGet('/users/me');
  const userId   = me.id;
  const desde    = new Date(Date.now() - diasAtras * 86400000).toISOString().split('T')[0];
  const lineas   = await sbGet('lineas_negocio', 'activa=eq.true');
  const linTech  = lineas?.find(l => l.nombre.toLowerCase().includes('tecnol')) || lineas?.[0];

  let offset = 0, total = 0, insertados = 0;

  do {
    const data   = await mlGet(`/orders/search?seller=${userId}&order.date_created.from=${desde}T00:00:00.000-03:00&offset=${offset}&limit=50&sort=date_asc`);
    total        = data.paging?.total || 0;
    const orders = data.results || [];
    if (!orders.length) break;

    const rows = orders.map(o => {
      const item    = o.order_items?.[0] || {};
      const payment = o.payments?.[0]    || {};
      const bruto   = o.total_amount     || 0;
      return {
        ml_order_id:      String(o.id),
        linea_negocio_id: linTech?.id,
        fecha:            o.date_created?.split('T')[0],
        sku_codigo:       item.item?.seller_sku || null,
        producto_desc:    item.item?.title || '',
        cantidad:         item.quantity || 1,
        ingreso_bruto:    bruto,
        comision_ml:      bruto * 0.1375,
        iibb:             bruto * 0.015,
        neto_mp:          bruto * (1 - 0.1375 - 0.015),
        estado_pago:      payment.status || 'pending',
        mp_payment_id:    payment.id ? String(payment.id) : null,
        ml_status:        o.status || 'unknown',
        conciliado:       false
      };
    });

    await sbUpsert('ventas_ml', rows);
    insertados += rows.length;
    offset     += 50;
  } while (offset < total);

  console.log(`✓ ML sync: ${insertados} órdenes (${desde} → hoy)`);
  return { insertados, desde };
}

app.post('/ml/sync', async (req, res) => {
  try {
    const result = await syncMLVentas(req.body?.dias || 7);
    res.json({ ok: true, ...result });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── MERCADO PAGO — Parseo extracto XLSX ─────────────────────────────
app.post('/mp/extracto', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'Falta el archivo' });

    const wb   = XLSX.read(req.file.buffer, { type: 'buffer', cellDates: true });
    const ws   = wb.Sheets[wb.SheetNames[0]];
    const rows = XLSX.utils.sheet_to_json(ws, { defval: null });
    if (!rows.length) return res.status(400).json({ error: 'El archivo está vacío' });

    // Detectar columnas (MP cambia los nombres según el tipo de reporte)
    const col = (r, ...opts) => {
      const keys = Object.keys(r);
      for (const opt of opts) {
        const found = keys.find(k => k.toLowerCase().includes(opt.toLowerCase()));
        if (found) return found;
      }
      return null;
    };
    const s = rows[0];
    const cFecha  = col(s, 'fecha', 'date');
    const cDesc   = col(s, 'descripción', 'descripcion', 'concepto', 'detalle');
    const cTipo   = col(s, 'tipo de operación', 'tipo de opera', 'tipo');
    const cMonto  = col(s, 'monto', 'amount', 'neto', 'total');
    const cBruto  = col(s, 'dinero ingresado', 'bruto', 'gross');
    const cCom    = col(s, 'comisión', 'commission', 'cargo mp');
    const cRef    = col(s, 'referencia', 'nro de referencia', 'id operac', 'reference');
    const cEstado = col(s, 'estado', 'status');

    const movs = rows.map(row => {
      const fecha_raw = row[cFecha];
      if (!fecha_raw) return null;
      const fecha   = fecha_raw instanceof Date ? fecha_raw.toISOString().split('T')[0] : String(fecha_raw).substring(0, 10);
      const desc    = String(row[cDesc]  || '').trim();
      const tipo    = String(row[cTipo]  || '').toLowerCase();
      const monto   = parseFloat(String(row[cMonto]  || 0).replace(/\./g, '').replace(',', '.')) || 0;
      const bruto   = parseFloat(String(row[cBruto]  || 0).replace(/\./g, '').replace(',', '.')) || 0;
      const com     = parseFloat(String(row[cCom]    || 0).replace(/\./g, '').replace(',', '.')) || 0;
      const ref     = String(row[cRef]   || '').trim();
      const estado  = String(row[cEstado]|| '').toLowerCase();

      let categoria = 'otro';
      if (tipo.includes('venta') || desc.toLowerCase().includes('mercado libre')) categoria = 'venta_ml';
      else if (tipo.includes('retiro') || tipo.includes('transfer'))              categoria = 'retiro';
      else if (tipo.includes('reembolso') || tipo.includes('devoluc'))            categoria = 'devolucion';
      else if (tipo.includes('impuesto') || desc.toLowerCase().includes('percep'))categoria = 'impuesto';
      else if (tipo.includes('comisión') || tipo.includes('cargo'))               categoria = 'comision';
      else if (tipo.includes('qr') || tipo.includes('cobro'))                     categoria = 'cobro_qr';

      return { fecha, descripcion: desc, tipo_operacion: tipo, categoria, monto_bruto: bruto || null, comision: Math.abs(com), monto_neto: monto, estado, referencia_mp: ref || null, conciliado: false };
    }).filter(Boolean);

    // Upsert por lotes
    let insertados = 0;
    for (let i = 0; i < movs.length; i += 100) {
      await sbUpsert('movimientos_mp', movs.slice(i, i + 100));
      insertados += Math.min(100, movs.length - i);
    }

    // Auto-conciliar con ventas ML
    const conciliados = await autoConciliarMP();

    res.json({ ok: true, total: movs.length, insertados, conciliados_automaticamente: conciliados });
  } catch (e) {
    console.error('MP extracto:', e);
    res.status(500).json({ error: e.message });
  }
});

async function autoConciliarMP() {
  const movs   = await sbGet('movimientos_mp', 'conciliado=eq.false&categoria=eq.venta_ml&limit=500');
  const ventas = await sbGet('ventas_ml', 'conciliado=eq.false&limit=500');
  let n = 0;
  for (const mov of (movs || [])) {
    const match = (ventas || []).find(v => v.mp_payment_id && mov.referencia_mp?.includes(v.mp_payment_id));
    if (match) {
      await sbPatch('movimientos_mp', `id=eq.${mov.id}`, { conciliado: true, venta_ml_id: match.id });
      await sbPatch('ventas_ml', `id=eq.${match.id}`, { conciliado: true });
      n++;
    }
  }
  console.log(`✓ Auto-conciliación MP: ${n} movimientos`);
  return n;
}

app.post('/mp/conciliar', async (_, res) => {
  try { res.json({ ok: true, conciliados: await autoConciliarMP() }); }
  catch (e) { res.status(500).json({ error: e.message }); }
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

    await sbUpsert('facturas_tango', rows);

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

// ── START ────────────────────────────────────────────────────────────
app.listen(PORT, async () => {
  console.log(`\n🚀 ADARA Backend corriendo — puerto ${PORT}`);
  console.log(`   Supabase : ${SUPABASE_URL  ? '✓' : '✗ FALTA variable SUPABASE_URL'}`);
  console.log(`   ML keys  : ${ML_CLIENT_ID  ? '✓' : '✗ FALTA variable ML_CLIENT_ID'}`);
  console.log(`   Tango    : ${TF_APP_KEY    ? '✓' : '✗ FALTA variable TF_APP_KEY (opcional)'}`);
  await loadMLToken();
});
