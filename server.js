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
        const fechaVenta = o.date_created?.split('T')[0];
        // Extraer hora de venta de ML (viene como "2026-02-26T14:30:00.000-03:00")
        // Tomamos solo HH:MM:SS (hora Argentina)
        const horaMatch = o.date_created ? o.date_created.match(/T(\d{2}:\d{2}:\d{2})/) : null;
        const horaVenta = horaMatch ? horaMatch[1] : null;

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
            od._comision = 0; od._impuestos = 0; od._financiero = 0; od._envio = 0;
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

                    if (type === 'tax') {
                      od._impuestos += amt;
                    } else if (type === 'fee') {
                      if (name.includes('financing') || name.includes('interest') || name.includes('add_on')) {
                        od._financiero += amt;
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
                    od.row.fecha_cobro = pay.money_release_date?.split('T')[0] || pay.date_approved?.split('T')[0] || null;
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
          if (od._financiero > 0) od.row.costo_financiero = -od._financiero;
          // Envío neto = cargo de envío - contribución del comprador
          // Ej: cargo $10,729.58 - buyer $5,346.59 = neto $5,382.99
          if (od._envio > 0) {
            const envioNeto = od._envio - (od._shippingBuyerContrib || 0);
            od.row.cargo_envio = -Math.round(envioNeto * 100) / 100;
          }

          // por_cobrar = bruto menos todos los cargos netos
          if (od._comision > 0 || od._impuestos > 0 || od._financiero > 0 || od._envio > 0) {
            od.row.por_cobrar = od.row.importe_bruto
              + od.row.cargo_venta
              + od.row.cargo_envio
              + od.row.costo_financiero
              + od.row.impuestos;
          }

          // Después de resolver shipment + payment:
          // Si es Flex, cargo_envio = bonificación (positivo, es ingreso)
          // Si es colecta/full, cargo_envio ya viene de charges_details (negativo)
          if (od.flexBonificacion && od.flexBonificacion > 0) {
            od.row.cargo_envio = od.flexBonificacion;

            // Imp. Créd. y Déb. sobre bonificación de envío (0.6% de la bonificación)
            // Este impuesto no viene en charges_details de MP, hay que calcularlo
            const impBonificacion = Math.round(od.flexBonificacion * 0.006 * 100) / 100;
            od.row.impuestos = (od.row.impuestos || 0) - impBonificacion;

            // Recalcular por_cobrar incluyendo bonificación e impuesto
            od.row.por_cobrar = od.row.importe_bruto
              + od.row.cargo_venta
              + od.row.cargo_envio
              + od.row.costo_financiero
              + od.row.impuestos;
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
    res.json({ ok: true, ...result });
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
    
    // 1. Buscar claims abiertos (limit 50)
    const searchUrl = `https://api.mercadolibre.com/post-purchase/v1/claims/search?status=opened&sort=date_created:desc&limit=50`;
    const r1 = await fetch(searchUrl, { headers: { 'Authorization': 'Bearer ' + ML.access } });
    const searchData = await r1.json();
    const claims = searchData.data || [];
    
    if (!claims.length) {
      return res.json({ ok: true, total: 0, devoluciones: [] });
    }
    
    // 2. Para cada claim, traer detalle de return (en paralelo, batches de 5)
    const results = [];
    for (let i = 0; i < claims.length; i += 5) {
      const batch = claims.slice(i, i + 5);
      await Promise.all(batch.map(async (claim) => {
        try {
          // Detalle del claim
          const rClaim = await fetch(`https://api.mercadolibre.com/post-purchase/v1/claims/${claim.id}`, {
            headers: { 'Authorization': 'Bearer ' + ML.access }
          });
          const claimDetail = await rClaim.json();
          
          // Detalle del return
          let returnDetail = null;
          if (claimDetail.related_entities?.includes('return')) {
            const rRet = await fetch(`https://api.mercadolibre.com/post-purchase/v2/claims/${claim.id}/returns`, {
              headers: { 'Authorization': 'Bearer ' + ML.access }
            });
            returnDetail = await rRet.json();
          }
          
          const orderId = String(claim.resource_id);
          const returnStatus = returnDetail?.status || null;
          const returnShipStatus = returnDetail?.shipments?.[0]?.status || null;
          const moneyStatus = returnDetail?.status_money || null;
          
          results.push({
            claim_id: claim.id,
            order_id: orderId,
            type: claim.type,
            stage: claimDetail.stage || claim.stage,
            reason_id: claim.reason_id,
            date_created: claim.date_created,
            fulfilled: claimDetail.fulfilled,
            quantity_type: claimDetail.quantity_type,
            return_status: returnStatus,
            return_ship_status: returnShipStatus,
            money_status: moneyStatus,
            seller_actions: (claimDetail.players || [])
              .find(p => p.type === 'seller')?.available_actions?.map(a => a.action) || [],
          });
        } catch (e) {
          console.warn(`Claim ${claim.id} fetch error:`, e.message);
        }
      }));
    }
    
    // 3. Actualizar ventas_ml con datos de claims
    let updated = 0;
    for (const dev of results) {
      const claimStatus = dev.return_ship_status === 'delivered' ? 'producto_recibido'
        : dev.return_ship_status === 'shipped' ? 'en_transito'
        : dev.return_status === 'label_generated' ? 'etiqueta_generada'
        : 'abierto';
      
      try {
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
    
    // 4. Enriquecer con datos de ventas_ml
    const orderIds = results.map(r => r.order_id);
    let ventasMap = {};
    if (orderIds.length) {
      // Fetch in chunks of 20
      for (let i = 0; i < orderIds.length; i += 20) {
        const chunk = orderIds.slice(i, i + 20);
        const ventas = await sbGet('ventas_ml', `ml_order_id=in.(${chunk.join(',')})&select=ml_order_id,titulo,sku,importe_bruto,tipo_envio,fecha,linea_negocio_id`);
        for (const v of (ventas || [])) {
          ventasMap[v.ml_order_id] = v;
        }
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
    
    console.log(`✓ Devoluciones ML: ${results.length} claims, ${updated} ventas actualizadas`);
    res.json({ ok: true, total: results.length, updated, devoluciones });
    
  } catch (e) {
    console.error('Devoluciones ML:', e);
    res.status(500).json({ error: e.message });
  }
});

// ── DEBUG: Return reasons ──────────────────────────────────────────────
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
          por_cobrar: netAmount
            ? (existing.por_cobrar || 0) + netAmount
            : existing.por_cobrar,
        };
      } else {
        // Pago único o primer pago del split → sobrescribir como antes
        updateData = {
          cargo_venta: comisionTotal ? -Math.abs(comisionTotal) : existing.cargo_venta,
          cargo_envio: shipping ? -Math.abs(shipping) : existing.cargo_envio,
          costo_financiero: financing ? -Math.abs(financing) : existing.costo_financiero,
          impuestos: taxes ? -Math.abs(taxes) : existing.impuestos,
          por_cobrar: netAmount || existing.por_cobrar,
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

async function autoConciliarMP() {
  // ─── V2: vincular TODO movimiento que matchee por referencia con un payment_id ──
  // Ya no filtramos por categoría — si la referencia coincide con un payment_id, se vincula.
  // Esto captura: liquidaciones, devoluciones, débitos por reclamo, reintegros, cargo envío, etc.
  // Solo quedan afuera: bonificaciones (otra referencia → segundo pase por monto) y movimientos sin referencia.
  
  // Solo movimientos sin venta vinculada (no pisar links manuales)
  // Paginado para evitar límite de 1000 rows de Supabase
  let movs = [];
  let movOffset = 0;
  while (true) {
    const page = await sbGet('movimientos_mp', `conciliado=eq.false&venta_ml_id=is.null&limit=1000&offset=${movOffset}&order=id`);
    if (!page?.length) break;
    movs.push(...page);
    if (page.length < 1000) break;
    movOffset += 1000;
  }
  console.log(`  → ${movs.length} movimientos sin conciliar cargados`);
  
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

  // ─── SEGUNDO PASE: bonificaciones por monto ────────────────────────
  // Las bonificaciones Flex no usan payment_id. Cada venta Flex tiene exactamente
  // 1 bonificación en el Account Statement. Matcheamos por monto: |bonif| ≈ |cargo_envio|.
  // Bonificaciones de ventas que no están en el sistema (ej: diciembre) quedan pendientes.
  let nBonifMonto = 0;
  try {
    // 1. Bonificaciones sin vincular (paginado)
    let bonifSinVinc = [];
    let bOffset = 0;
    while (true) {
      const page = await sbGet('movimientos_mp', 
        `conciliado=eq.false&venta_ml_id=is.null&categoria=eq.bonificacion_envio&select=id,monto_neto,posicion&limit=1000&offset=${bOffset}&order=id`
      );
      if (!page?.length) break;
      bonifSinVinc.push(...page);
      if (page.length < 1000) break;
      bOffset += 1000;
    }
    
    if (bonifSinVinc.length) {
      // 2. Ventas Flex con cargo_envio > 0 (paginado)
      let ventasFlex = [];
      let vfOffset = 0;
      while (true) {
        const page = await sbGet('ventas_ml', 
          `tipo_envio=eq.flex&cargo_envio=gt.0&select=id,cargo_envio&limit=1000&offset=${vfOffset}&order=id`
        );
        if (!page?.length) break;
        ventasFlex.push(...page);
        if (page.length < 1000) break;
        vfOffset += 1000;
      }
      
      // 3. Ventas que YA tienen bonificación vinculada (paginado)
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
      
      // 4. Filtrar: ventas Flex que NO tienen bonificación todavía
      const ventasSinBonif = (ventasFlex || []).filter(v => !ventasConBonif.has(v.id));
      
      // 5. Agrupar por monto NETO de bonificación para matching
      // El AS trae bonificación neta (después del impuesto Créd/Déb 0.6%)
      // cargo_envio en ventas_ml es bruto. Fórmula: neto = cargo_envio - round(cargo_envio * 0.006, 2)
      const ventasByMonto = {};
      for (const v of ventasSinBonif) {
        const bruto = Math.abs(v.cargo_envio || 0);
        const impuesto = Math.round(bruto * 0.006 * 100) / 100;
        const neto = Math.round((bruto - impuesto) * 100) / 100;
        const key = Math.round(neto * 100); // centavos como key
        if (!ventasByMonto[key]) ventasByMonto[key] = [];
        ventasByMonto[key].push(v);
      }
      
      // 6. Matching 1:1 por monto
      const bonifByVenta = {}; // venta_id → bonif_mov_id
      
      for (const bonif of bonifSinVinc) {
        const bonifKey = Math.round(Math.abs(bonif.monto_neto || 0) * 100); // centavos
        const candidatas = ventasByMonto[bonifKey];
        if (!candidatas?.length) continue;
        
        // Tomar la primera disponible (pop para no reutilizar)
        const venta = candidatas.shift();
        if (candidatas.length === 0) delete ventasByMonto[bonifKey];
        
        bonifByVenta[venta.id] = bonif.id;
        ventasTocadas.add(venta.id);
        nBonifMonto++;
      }
      
      // 7. Batch update: vincular bonificaciones a ventas (paralelizado)
      const bonifTasks = [];
      // Agrupar por bloques de 50 para batch
      const entries = Object.entries(bonifByVenta);
      for (let i = 0; i < entries.length; i += 50) {
        const batch = entries.slice(i, i + 50);
        for (const [ventaId, movId] of batch) {
          const patchData = { conciliado: true, venta_ml_id: ventaId };
          if (mlLineaId) patchData.linea_negocio_id = mlLineaId;
          bonifTasks.push(() => sbPatch('movimientos_mp', `id=eq.${movId}`, patchData).catch(e => {
            console.warn('Patch bonif monto:', e.message);
          }));
        }
      }
      await parallel(bonifTasks);
      
      console.log(`✓ Bonificaciones por monto: ${nBonifMonto} de ${bonifSinVinc.length} vinculadas (${ventasSinBonif.length} ventas Flex sin bonif)`);
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
    // Incluye categoria para separar bonificaciones del esperado
    const ventaMontosMap = {}; // venta_id → suma de montos
    const ventaBonifMap = {};  // venta_id → suma de bonificaciones
    for (let i = 0; i < allTocadas.length; i += 100) {
      const chunk = allTocadas.slice(i, i + 100).join(',');
      let mOffset = 0;
      while (true) {
        const movsChunk = await sbGet('movimientos_mp', `venta_ml_id=in.(${chunk})&select=venta_ml_id,monto_neto,categoria&limit=1000&offset=${mOffset}&order=id`);
        if (!movsChunk?.length) break;
        for (const m of movsChunk) {
          ventaMontosMap[m.venta_ml_id] = (ventaMontosMap[m.venta_ml_id] || 0) + (m.monto_neto || 0);
          if (m.categoria === 'bonificacion_envio') {
            ventaBonifMap[m.venta_ml_id] = (ventaBonifMap[m.venta_ml_id] || 0) + (m.monto_neto || 0);
          }
        }
        if (movsChunk.length < 1000) break;
        mOffset += 1000;
      }
    }
    
    // 2. Traer por_cobrar de las ventas tocadas (fresh from DB, paginado)
    const ventaPorCobrar = {};
    for (let i = 0; i < allTocadas.length; i += 200) {
      const chunk = allTocadas.slice(i, i + 200).join(',');
      let pcOffset = 0;
      while (true) {
        const ventasChunk = await sbGet('ventas_ml', `id=in.(${chunk})&select=id,por_cobrar&limit=1000&offset=${pcOffset}&order=id`);
        if (!ventasChunk?.length) break;
        for (const v of ventasChunk) {
          ventaPorCobrar[v.id] = v.por_cobrar || 0;
        }
        if (ventasChunk.length < 1000) break;
        pcOffset += 1000;
      }
    }
    
    // 3. Calcular balance y agrupar
    // esperado = por_cobrar + bonificación vinculada (la bonificación Flex es ingreso real
    // pero no está incluida en por_cobrar porque es un pago separado de ML)
    const conciliadoIds = [];
    const parcialUpdates = []; // {id, balance}
    
    for (const ventaId of allTocadas) {
      const sumMovs = ventaMontosMap[ventaId] || 0;
      const porCobrar = ventaPorCobrar[ventaId] || 0;
      const bonif = ventaBonifMap[ventaId] || 0;
      const esperado = porCobrar + bonif;
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
    bonificaciones: nBonif, bonificaciones_monto: nBonifMonto,
    otros_vinculados: nOtrosVinculados,
    cargos_envio: Object.keys(ventaCargoEnvio).length,
    balance_ok: nBalanceOk, balance_diff: nBalanceDiff
  };
  console.log(`✓ Auto-conciliación: ${nMovs} movs → ${nVentas} ventas, ${nDevueltas} devol, ${nBonifMonto} bonif, ${nOtrosVinculados} otros, balance ok=${nBalanceOk} diff=${nBalanceDiff}`);
  return result;
}

app.post('/mp/conciliar', async (_, res) => {
  try { res.json({ ok: true, conciliados: await autoConciliarMP() }); }
  catch (e) { res.status(500).json({ error: e.message }); }
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
    const movsVenta = await sbGet('movimientos_mp', `venta_ml_id=eq.${venta_id}&select=monto_neto`);
    const sumMovs = (movsVenta || []).reduce((s, m) => s + (m.monto_neto || 0), 0);
    const venta = await sbGet('ventas_ml', `id=eq.${venta_id}&select=por_cobrar,conciliado`);
    
    if (venta?.length) {
      const esperado = venta[0].por_cobrar || 0;
      const balance = Math.round((sumMovs - esperado) * 100) / 100;
      const estado = balance === 0 ? 'conciliado' : 'parcial';
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
      const movsVenta = await sbGet('movimientos_mp', `venta_ml_id=eq.${venta_id}&select=monto_neto`);
      const sumMovs = (movsVenta || []).reduce((s, m) => s + (m.monto_neto || 0), 0);
      const venta = await sbGet('ventas_ml', `id=eq.${venta_id}&select=por_cobrar`);
      if (venta?.length) {
        const esperado = venta[0].por_cobrar || 0;
        const balance = Math.round((sumMovs - esperado) * 100) / 100;
        const hasMov = (movsVenta || []).length > 0;
        const estado = !hasMov ? 'pendiente' : (balance === 0 ? 'conciliado' : 'parcial');
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
  await loadFeriados(new Date().getFullYear());
});
