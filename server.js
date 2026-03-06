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
const sbUpsert = (t, body, onConflict) => {
  const q = onConflict ? `on_conflict=${onConflict}` : '';
  return sb('POST', t, body, q);
};
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
async function syncMLVentas(diasAtras = 7, fechaDesde = null, fechaHasta = null) {
  if (!ML.access) throw new Error('ML no autenticado. Conectá ML primero desde la app.');

  const me     = await mlGet('/users/me');
  const userId = me.id;

  // Fechas: si se pasan explícitas, usarlas. Si no, usar diasAtras.
  const desde = fechaDesde || new Date(Date.now() - diasAtras * 86400000).toISOString().split('T')[0];
  const hasta = fechaHasta || new Date().toISOString().split('T')[0];

  // Cargar maestros para matching de línea por SKU
  const lineas = await sbGet('lineas_negocio', 'activa=eq.true');
  const skus   = await sbGet('catalogo_skus', 'limit=500').catch(() => []) || [];
  const linDefault = lineas?.find(l => l.nombre.toLowerCase().includes('tecnol')) || lineas?.[0];

  // Función para matchear SKU → linea_negocio_id
  function matchLinea(sellerSku, itemTitle) {
    if (sellerSku) {
      // 1. Buscar por seller_sku exacto en tabla sku_adara
      const skuMatch = skus.find(s => s.sku === sellerSku);
      if (skuMatch?.linea_negocio_id) return skuMatch.linea_negocio_id;
    }
    // 2. Heurística por título del producto
    const t = (itemTitle || '').toLowerCase();
    for (const l of (lineas || [])) {
      const nombre = l.nombre.toLowerCase();
      if (nombre.includes('mochila') && (t.includes('mochila') || t.includes('bolso') || t.includes('backpack'))) return l.id;
      if (nombre.includes('vaso')    && (t.includes('vaso')    || t.includes('botella') || t.includes('termo'))) return l.id;
      if (nombre.includes('luminar') && (t.includes('luminar') || t.includes('lámpara') || t.includes('lampara') || t.includes('led') || t.includes('foco'))) return l.id;
      if (nombre.includes('tecnol')  && (t.includes('xiaomi')  || t.includes('redmi')   || t.includes('smartwatch') || t.includes('auricular') || t.includes('bluetooth'))) return l.id;
    }
    // 3. Default
    return linDefault?.id || null;
  }

  // ML API tiene un tope de offset=1000. Para traer más, partimos por rangos de fecha.
  // Estrategia: iterar por intervalos de 15 días.
  const dateChunks = [];
  let chunkStart = new Date(desde);
  const endDate  = new Date(hasta);
  while (chunkStart <= endDate) {
    let chunkEnd = new Date(chunkStart);
    chunkEnd.setDate(chunkEnd.getDate() + 14); // 15 días por chunk
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
        const payment = o.payments?.[0]    || {};
        const bruto   = o.total_amount     || 0;
        const fechaVenta = o.date_created?.split('T')[0];

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
          paymentId:  payment.id ? String(payment.id) : null,
          bruto,
          comision: comisionReal,
          row: {
            ml_order_id:      String(o.id),
            periodo:          fechaVenta ? fechaVenta.substring(0, 7) : null,
            fecha:            fechaVenta,
            titulo:           item.item?.title || '',
            sku:              sellerSku,
            cantidad:         item.quantity || 1,
            importe_bruto:    bruto,
            cargo_venta:      -Math.abs(comisionReal),
            cargo_envio:      0,
            costo_financiero: 0,
            impuestos:        0,
            por_cobrar:       bruto - comisionReal,  // provisional, se actualiza con collections
            mp_payment_id:    payment.id ? String(payment.id) : null,
            ml_status:        o.status || 'unknown',
            motivo_cancelacion: motivoCancelacion,
            monto_devuelto:   montoDevuelto,
            pack_id:          o.pack_id ? String(o.pack_id) : null,
            linea_negocio_id: lineaId,
            tipo_envio:       null,
            fecha_entrega:    null,
            ciudad_destino:   null,
            enviado:          false,
            conciliado:       false,
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

          // 1. Shipment (only for tipo_envio, ciudad, fecha — NOT for costs)
          if (od.shippingId) {
            promises.push(
              mlGet(`/shipments/${od.shippingId}`).then(ship => {
                od.row.tipo_envio = ship.logistic_type === 'self_service' ? 'flex'
                  : ship.logistic_type === 'fulfillment' ? 'fulfillment'
                  : ship.logistic_type === 'xd_drop_off' ? 'colecta'
                  : ship.logistic_type || 'otro';
                od.row.fecha_entrega = ship.status_history?.date_delivered?.split('T')[0] || null;
                od.row.ciudad_destino = ship.receiver_address?.city?.name || ship.receiver_address?.city || null;
                od.row.enviado = ship.status === 'delivered';
                // NOTE: Do NOT use ship cost — it's list price, not seller cost
              }).catch(() => { od.row.tipo_envio = 'otro'; })
            );
          }

          // 2. MP Payment details (real fee breakdown via charges_details)
          if (od.paymentId && od.row.ml_status !== 'cancelled') {
            promises.push(
              fetch(`https://api.mercadopago.com/v1/payments/${od.paymentId}`, {
                headers: { 'Authorization': 'Bearer ' + ML.access }
              }).then(r => r.ok ? r.json() : null).then(pay => {
                if (!pay) return;

                // Net received (the real final number)
                const netoReal = pay.net_received_amount || 0;
                if (netoReal > 0) od.row.por_cobrar = netoReal;

                // Parse charges_details
                const charges = pay.charges_details || [];
                let comision = 0, impuestos = 0, financiero = 0;

                for (const ch of charges) {
                  const amt = ch.amounts?.original || 0;
                  if (amt === 0) continue;
                  const type = (ch.type || '').toLowerCase();
                  const name = (ch.name || '').toLowerCase();

                  // Skip buyer-side charges (coupons, discounts)
                  if (type === 'coupon' || type === 'discount') continue;
                  if (name.includes('coupon') || name.includes('rebate')) continue;

                  if (type === 'tax') {
                    impuestos += amt;
                  } else if (type === 'fee') {
                    if (name.includes('financing') || name.includes('interest') || name.includes('add_on')) {
                      financiero += amt;
                    } else {
                      comision += amt;
                    }
                  } else if (type === 'shipping') {
                    // Explicit shipping charge (colecta/full) — handled via residual
                  }
                }

                if (comision > 0) od.row.cargo_venta = -comision;
                if (impuestos > 0) od.row.impuestos = -impuestos;
                if (financiero > 0) od.row.costo_financiero = -financiero;

                // Envío/bonificación = residual (what's left after all known charges)
                // Positive = bonification (Flex pays you), Negative = shipping charge
                if (netoReal > 0) {
                  const totalCargos = comision + financiero + impuestos;
                  const envioResidual = netoReal - (od.bruto - totalCargos);
                  od.row.cargo_envio = Math.round(envioResidual * 100) / 100;
                }

                // Conciliation data
                od.row.fecha_cobro = pay.money_release_date?.split('T')[0] || pay.date_approved?.split('T')[0] || null;
                if (netoReal > 0 && pay.status === 'approved') od.row.conciliado = true;
              }).catch(() => {})
            );
          }

          await Promise.all(promises);
        }));
      }

      const dbRows = orderData.map(d => d.row);
      if (dbRows.length) {
        await sbUpsert('ventas_ml', dbRows, 'ml_order_id');
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
    res.json({ ok: true, ...result });
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

      // Buscar venta por mp_payment_id = sourceId
      const ventas = await sbGet('ventas_ml', `mp_payment_id=eq.${sourceId}&limit=1`);
      if (!ventas?.length) { skipped++; continue; }

      // Actualizar con desglose real
      const updateData = {
        cargo_venta: comisionTotal ? -Math.abs(comisionTotal) : ventas[0].cargo_venta,
        cargo_envio: shipping ? -Math.abs(shipping) : ventas[0].cargo_envio,
        costo_financiero: financing ? -Math.abs(financing) : ventas[0].costo_financiero,
        impuestos: taxes ? -Math.abs(taxes) : ventas[0].impuestos,
        por_cobrar: netAmount || ventas[0].por_cobrar,
      };

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
      if (t.includes('bonificación'))                                       return 'bonificacion_envio';
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
        conciliado: false
      });
    }

    console.log(`MP extracto: ${movs.length} movimientos parseados, ${movs.filter(m => m.categoria === 'venta_ml').length} liquidaciones`);

    if (!movs.length) {
      return res.status(400).json({ error: 'No se encontraron movimientos válidos en el archivo' });
    }

    // ─── Insertar en Supabase (sin on_conflict porque no hay PK única en referencia_mp, puede repetir) ────
    // Primero limpiamos movimientos previos del mismo período para evitar duplicados
    const fechaMin = movs.reduce((min, m) => m.fecha < min ? m.fecha : min, movs[0].fecha);
    const fechaMax = movs.reduce((max, m) => m.fecha > max ? m.fecha : max, movs[0].fecha);
    
    // Delete previos del mismo rango
    try {
      await sb('DELETE', 'movimientos_mp', null, `fecha=gte.${fechaMin}&fecha=lte.${fechaMax}`);
      console.log(`MP extracto: borrados movimientos previos ${fechaMin} → ${fechaMax}`);
    } catch(e) { console.warn('Delete previos:', e.message); }

    // Insertar todos
    let insertados = 0;
    for (let i = 0; i < movs.length; i += 100) {
      const batch = movs.slice(i, i + 100);
      await sb('POST', 'movimientos_mp', batch);
      insertados += batch.length;
    }

    // Auto-conciliar con ventas ML
    const conciliados = await autoConciliarMP();

    res.json({ 
      ok: true, 
      formato: headerCols.format,
      total: movs.length, 
      liquidaciones: movs.filter(m => m.categoria === 'venta_ml').length,
      insertados, 
      conciliados_automaticamente: conciliados,
      periodo: `${fechaMin} → ${fechaMax}`
    });
  } catch (e) {
    console.error('MP extracto:', e);
    res.status(500).json({ error: e.message });
  }
});

async function autoConciliarMP() {
  // Traer movimientos MP tipo venta no conciliados
  const movs   = await sbGet('movimientos_mp', 'conciliado=eq.false&categoria=eq.venta_ml&limit=5000');
  // Traer TODAS las ventas ML no conciliadas
  const ventas = await sbGet('ventas_ml', 'conciliado=eq.false&limit=5000');
  let nMovs = 0, nVentas = 0;

  // Crear índice de ventas por mp_payment_id para lookup rápido
  const ventasByPayment = {};
  for (const v of (ventas || [])) {
    if (v.mp_payment_id) {
      if (!ventasByPayment[v.mp_payment_id]) ventasByPayment[v.mp_payment_id] = [];
      ventasByPayment[v.mp_payment_id].push(v);
    }
  }

  for (const mov of (movs || [])) {
    if (!mov.referencia_mp) continue;

    // Buscar ventas con este payment_id (exacto)
    const matches = ventasByPayment[mov.referencia_mp] || [];

    if (matches.length > 0) {
      // Marcar movimiento MP como conciliado
      await sbPatch('movimientos_mp', `id=eq.${mov.id}`, {
        conciliado: true,
        venta_ml_id: matches[0].id
      });

      // Marcar TODAS las ventas ML del pack como conciliadas
      for (const match of matches) {
        if (!match.conciliado) {
          await sbPatch('ventas_ml', `id=eq.${match.id}`, { conciliado: true });
          match.conciliado = true;
          nVentas++;
        }
      }
      nMovs++;
    }
  }

  console.log(`✓ Auto-conciliación MP: ${nMovs} movimientos → ${nVentas} ventas ML`);
  return { movimientos: nMovs, ventas: nVentas };
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

// ── START ────────────────────────────────────────────────────────────
app.listen(PORT, async () => {
  console.log(`\n🚀 ADARA Backend corriendo — puerto ${PORT}`);
  console.log(`   Supabase : ${SUPABASE_URL  ? '✓' : '✗ FALTA variable SUPABASE_URL'}`);
  console.log(`   ML keys  : ${ML_CLIENT_ID  ? '✓' : '✗ FALTA variable ML_CLIENT_ID'}`);
  console.log(`   Tango    : ${TF_APP_KEY    ? '✓' : '✗ FALTA variable TF_APP_KEY (opcional)'}`);
  await loadMLToken();
});
