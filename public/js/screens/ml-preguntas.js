// ── Pantalla: Preguntas ML (bot de preventa) ────────────────────────────────
// Sub-pestaña del grupo "Mercado Libre". Sólo lee/escribe tablas bot_ml_* — no
// toca ninguna tabla del negocio. Ver ADARA-BOT-PREGUNTAS-ML.md.
//
// Dos sub-vistas:
//   · Histórico → tablero de KPIs + historial de preguntas en tarjetas
//   · Fichas    → alta y edición de fichas (el circuito de mantenimiento)

import { sbGet, sbPost, sbPatch } from '../core/sb.js';
import { mlTabs } from '../core/mlTabs.js';

let PREGUNTAS = [];
let FICHAS = [];        // una por producto (clave = SKU, o MLA si no hay)
let PUBLICACIONES = []; // mapeo MLA -> ficha
let CONVERSION = null;

let SUB     = 'historico';  // historico | fichas
let DIAS    = 14;           // ventana del tablero
let FILTRO  = 'todas';      // todas | escalada | sombra | auto | manual
let ORDEN   = 'recientes';  // recientes | antiguas | lentas
let BUSCA   = '';
let LIMITE  = 25;
let ABIERTA = null;         // question_id expandido (detalle)
let FICHA_SEL = null;       // codigo de ficha en edición ('__nueva__' para alta)
let CAPA = 'fichas';        // dentro de la solapa: fichas | publicaciones

const SLA_SEG = 60;         // objetivo de la spec: respuesta en menos de 60 s

const MODOS = {
  auto:       { lbl: 'Respondida por IA',   color: '#0F6E56', bg: '#DCFCE7' },
  sombra:     { lbl: 'Propuesta en sombra', color: '#6D28D9', bg: '#EDE9FE' },
  escalada:   { lbl: 'Escalada a humano',   color: '#B91C1C', bg: '#FEE2E2' },
  manual:     { lbl: 'Respuesta manual',    color: '#57534E', bg: '#F5F5F4' },
  procesando: { lbl: 'En curso',            color: '#B45309', bg: '#FEF3C7' }
};

// ── Helpers ────────────────────────────────────────────────────────────────
function esc(s) {
  return String(s ?? '').replace(/[&<>"']/g, c =>
    ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}
const MESES = ['enero','febrero','marzo','abril','mayo','junio','julio','agosto','septiembre','octubre','noviembre','diciembre'];
function fechaLarga(iso) {
  if (!iso) return '—';
  const d = new Date(iso);
  return `${d.getDate()} ${MESES[d.getMonth()]} ${d.getFullYear()} - ${String(d.getHours()).padStart(2,'0')}:${String(d.getMinutes()).padStart(2,'0')}`;
}
function dur(seg) {
  if (seg == null) return '—';
  if (seg < 60) return `${seg}s`;
  if (seg < 3600) return `${Math.floor(seg / 60)} min ${seg % 60}s`;
  const h = Math.floor(seg / 3600);
  return `${h} h ${Math.round((seg - h * 3600) / 60)} min`;
}
function mediana(arr) {
  if (!arr.length) return null;
  const a = [...arr].sort((x, y) => x - y);
  const m = Math.floor(a.length / 2);
  return a.length % 2 ? a[m] : Math.round((a[m - 1] + a[m]) / 2);
}
function chip(txt, color, bg) {
  return `<span style="display:inline-block;padding:2px 7px;border-radius:5px;font-size:10.5px;font-weight:700;color:${color};background:${bg};letter-spacing:.2px">${esc(txt)}</span>`;
}
function ring(pct, color) {
  const r = 30, c = 2 * Math.PI * r, off = c * (1 - Math.min(100, Math.max(0, pct)) / 100);
  return `<svg width="76" height="76" viewBox="0 0 76 76">
    <circle cx="38" cy="38" r="${r}" fill="none" stroke="#E7E5E4" stroke-width="8"/>
    <circle cx="38" cy="38" r="${r}" fill="none" stroke="${color}" stroke-width="8" stroke-linecap="round"
      stroke-dasharray="${c}" stroke-dashoffset="${off}" transform="rotate(-90 38 38)"/>
    <text x="38" y="43" text-anchor="middle" font-size="17" font-weight="700" fill="#1C1917">${Math.round(pct)}%</text>
  </svg>`;
}
function barra(pct, color) {
  return `<div style="height:6px;background:#F5F5F4;border-radius:99px;overflow:hidden">
    <div style="height:100%;width:${Math.min(100, pct)}%;background:${color};border-radius:99px"></div></div>`;
}
function linkML(p) {
  if (p.permalink) return p.permalink;
  return p.mla && !String(p.mla).startsWith('FALTA_MLA_')
    ? `https://articulo.mercadolibre.com.ar/${p.mla}` : null;
}
function tituloDe(p) {
  if (p.titulo) return p.titulo;
  const pub = PUBLICACIONES.find(x => x.mla === p.mla);
  return pub ? (pub.titulo || p.mla) : (p.mla || '(sin ítem)');
}
/** De un MLA al código de su ficha (para el botón "Editar ficha" del histórico). */
function codigoDeMla(mla) {
  const pub = PUBLICACIONES.find(x => x.mla === mla);
  return pub ? pub.codigo_ficha : null;
}

// ── Carga ──────────────────────────────────────────────────────────────────
export async function loadMLPreguntas() {
  const root = document.getElementById('app-screens');
  root.innerHTML = `<div class="loading">Cargando preguntas…</div>`;
  SUB = 'historico'; ABIERTA = null; FICHA_SEL = null; LIMITE = 25; BUSCA = '';
  try {
    await recargar();
    render();
  } catch (e) {
    console.error('Preguntas ML falló:', e);
    root.innerHTML = `<div class="error"><strong>No se pudieron cargar las preguntas.</strong><br>${esc(e.message)}
      <br><br>Si dice que <code>bot_ml_preguntas</code> no existe, todavía no se corrió la migración del bot.</div>`;
  }
}

async function recargar() {
  const desde = new Date(Date.now() - DIAS * 86400000).toISOString();
  // Regla sbGet: orden con columna única de desempate (question_id).
  [PREGUNTAS, FICHAS, PUBLICACIONES] = await Promise.all([
    sbGet('bot_ml_preguntas', `recibida=gte.${desde}&order=recibida.desc,question_id.desc`),
    sbGet('bot_ml_fichas', 'order=titulo.asc,codigo.asc'),
    sbGet('bot_ml_publicaciones', 'order=titulo.asc,mla.asc')
  ]);
  try {
    const c = await sbGet('v_bot_ml_conversion', '');
    CONVERSION = c && c[0] ? c[0] : null;
  } catch { CONVERSION = null; }
}

// ── Render ─────────────────────────────────────────────────────────────────
function render() {
  const root = document.getElementById('app-screens');
  root.innerHTML = `
    ${mlTabs('ml_preguntas')}
    <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:6px">
      <button class="btn ${SUB === 'historico' ? 'btn-primary' : 'btn-ghost'}" data-sub="historico">📊 Histórico de respuestas</button>
      <button class="btn ${SUB === 'fichas' ? 'btn-primary' : 'btn-ghost'}" data-sub="fichas">📋 Fichas (${FICHAS.length}) · Publicaciones (${PUBLICACIONES.length})</button>
      ${SUB === 'historico' ? `
        <select id="bmp-dias" style="margin-left:12px;padding:7px 10px;border:1px solid #E7E5E4;border-radius:8px;font-size:13px">
          <option value="1"  ${DIAS === 1  ? 'selected' : ''}>Hoy</option>
          <option value="7"  ${DIAS === 7  ? 'selected' : ''}>Últimos 7 días</option>
          <option value="14" ${DIAS === 14 ? 'selected' : ''}>Últimos 14 días</option>
          <option value="30" ${DIAS === 30 ? 'selected' : ''}>Últimos 30 días</option>
        </select>
        <button class="btn btn-ghost" id="bmp-csv">📥 Excel (CSV)</button>` : ''}
      <button class="btn btn-ghost" id="bmp-reload" style="margin-left:auto">⟳ Actualizar</button>
    </div>
    ${SUB === 'historico' ? renderHistorico() : renderFichas()}`;
  bind();
}

// ── Sub-vista: histórico ───────────────────────────────────────────────────
function renderHistorico() {
  const total = PREGUNTAS.length;
  if (!total) {
    return `<div class="empty" style="margin-top:20px">Todavía no entró ninguna pregunta en esta ventana.</div>`;
  }

  const cont = { auto: 0, sombra: 0, escalada: 0, manual: 0, procesando: 0 };
  const lats = [];
  const porHora = {};
  for (const p of PREGUNTAS) {
    if (cont[p.modo] != null) cont[p.modo]++;
    if (p.latencia_seg != null && p.modo !== 'escalada') lats.push(p.latencia_seg);
    if (p.recibida) {
      const h = new Date(p.recibida).getHours();
      porHora[h] = (porHora[h] || 0) + 1;
    }
  }
  const delBot  = cont.auto + cont.sombra;
  const humanas = cont.manual + cont.escalada;
  const prom    = lats.length ? Math.round(lats.reduce((a, b) => a + b, 0) / lats.length) : null;
  const med     = mediana(lats);
  const autoPct = total ? delBot / total * 100 : 0;
  const slaPct  = lats.length ? lats.filter(s => s <= SLA_SEG).length / lats.length * 100 : 0;
  const horaPico = Object.entries(porHora).sort((a, b) => b[1] - a[1])[0];
  const modoSombra = cont.sombra > 0 && cont.auto === 0;

  const motivos = {};
  for (const p of PREGUNTAS) {
    if (p.modo !== 'escalada') continue;
    const k = (p.motivo_escalada || 'sin motivo').slice(0, 90);
    motivos[k] = (motivos[k] || 0) + 1;
  }
  const motivosTop = Object.entries(motivos).sort((a, b) => b[1] - a[1]).slice(0, 6);

  const card = (color, titulo, cuerpo) => `
    <div style="flex:1 1 240px;min-width:230px;border:1px solid #E7E5E4;border-top:3px solid ${color};border-radius:12px;padding:16px 18px;background:#fff">
      <div style="font-size:11px;font-weight:800;letter-spacing:.6px;color:#78716C;text-transform:uppercase;margin-bottom:10px">${titulo}</div>
      ${cuerpo}</div>`;

  const tablero = `
    <div style="display:flex;gap:14px;flex-wrap:wrap;margin:14px 0 18px">
      ${card('#7C3AED', 'Total de respuestas', `
        <div style="font-size:34px;font-weight:800;color:#1C1917;line-height:1">${total}
          <span style="font-size:13px;font-weight:500;color:#78716C"> preguntas</span></div>
        <div style="margin-top:12px;font-size:12px;color:#57534E">
          <div style="display:flex;justify-content:space-between"><span>Humanas</span><span>${humanas} · ${Math.round(humanas / total * 100)}%</span></div>
          ${barra(humanas / total * 100, '#A8A29E')}
          <div style="display:flex;justify-content:space-between;margin-top:8px"><span>${modoSombra ? 'Propuestas del bot' : 'Respondidas por IA'}</span><span>${delBot} · ${Math.round(autoPct)}%</span></div>
          ${barra(autoPct, '#7C3AED')}
        </div>`)}

      ${card('#0F6E56', 'Tiempo promedio de respuesta', `
        <div style="font-size:34px;font-weight:800;color:#1C1917;line-height:1">${prom != null ? dur(prom) : '—'}</div>
        <div style="margin-top:12px;font-size:12px;color:#57534E">
          <div style="display:flex;justify-content:space-between"><span>Mediana</span><span>${med != null ? dur(med) : '—'}</span></div>
          <div style="display:flex;justify-content:space-between;margin-top:4px"><span>Más lenta</span><span>${lats.length ? dur(Math.max(...lats)) : '—'}</span></div>
          <div style="display:flex;justify-content:space-between;margin-top:4px"><span>Medidas</span><span>${lats.length} de ${total}</span></div>
        </div>`)}

      ${card('#0EA5E9', 'Tasa de automatización', `
        <div style="display:flex;align-items:center;gap:14px">
          ${ring(autoPct, '#0EA5E9')}
          <div style="font-size:12px;color:#57534E">
            <div>${modoSombra ? 'Propuestas vs. humanas' : 'Automáticas vs. manuales'}</div>
            <div style="font-size:15px;font-weight:700;color:#1C1917;margin-top:2px">${delBot} resueltas por el bot</div>
            <div style="margin-top:6px">${cont.escalada} escaladas · ${cont.manual} manuales</div>
          </div>
        </div>`)}

      ${card('#D97706', 'Nivel de servicio', `
        <div style="font-size:34px;font-weight:800;color:#1C1917;line-height:1">${Math.round(slaPct)}%
          <span style="font-size:12px;font-weight:600;color:#0F6E56"> en menos de ${SLA_SEG}s</span></div>
        <div style="margin-top:10px">${barra(slaPct, '#0F6E56')}</div>
        <div style="display:flex;gap:10px;margin-top:12px;font-size:12px;color:#57534E">
          <div style="flex:1"><div style="color:#78716C">Hora pico</div><div style="font-weight:700;color:#1C1917">${horaPico ? String(horaPico[0]).padStart(2, '0') + ':00' : '—'}</div></div>
          <div style="flex:1"><div style="color:#78716C">Conversión 14d</div><div style="font-weight:700;color:#1C1917">${
            CONVERSION && CONVERSION.conversion_pct != null ? Number(CONVERSION.conversion_pct).toFixed(1) + '%' : '—'}
            <span style="font-weight:400;color:#78716C">/ base 10%</span></div></div>
        </div>`)}
    </div>

    <div style="margin:0 0 16px;padding:9px 13px;border-radius:8px;background:${modoSombra ? '#EDE9FE' : '#F5F5F4'};font-size:13px;color:#44403C">
      ${modoSombra
        ? '🌓 <strong>Modo sombra</strong> — el bot propone por Telegram y <strong>no postea nada</strong> en Mercado Libre.'
        : (cont.auto > 0 ? '🟢 <strong>Modo producción</strong> — el bot está respondiendo en Mercado Libre.' : 'Sin actividad del bot en esta ventana.')}
    </div>

    ${motivosTop.length ? `
      <div style="margin:0 0 16px;padding:13px 15px;border:1px solid #E7E5E4;border-radius:10px">
        <div style="font-size:13px;font-weight:700;color:#1C1917;margin-bottom:8px">Motivos de escalada — esta es la lista de fichas a completar</div>
        ${motivosTop.map(([m, n]) =>
          `<div style="font-size:13px;color:#57534E;padding:3px 0">· ${esc(m)} <strong>(${n})</strong></div>`).join('')}
      </div>` : ''}`;

  // ── Historial ──
  const q = BUSCA.trim().toLowerCase();
  let lista = PREGUNTAS.filter(p => {
    if (FILTRO !== 'todas' && p.modo !== FILTRO) return false;
    if (!q) return true;
    return [tituloDe(p), p.mla, p.sku, p.pregunta, p.comprador_nick]
      .some(v => String(v ?? '').toLowerCase().includes(q));
  });
  if (ORDEN === 'antiguas') lista = [...lista].reverse();
  if (ORDEN === 'lentas') lista = [...lista].sort((a, b) => (b.latencia_seg ?? -1) - (a.latencia_seg ?? -1));

  const visibles = lista.slice(0, LIMITE);
  const pill = (f, lbl, n) =>
    `<button class="pill ${FILTRO === f ? 'active' : ''}" data-f="${f}">${lbl}${n != null ? ` (${n})` : ''}</button>`;

  const controles = `
    <div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:12px">
      <input id="bmp-busca" value="${esc(BUSCA)}" placeholder="Buscar por título, MLA, SKU, comprador o texto…"
        style="flex:1 1 300px;padding:9px 12px;border:1px solid #E7E5E4;border-radius:8px;font-size:13px">
      <select id="bmp-orden" style="padding:9px 10px;border:1px solid #E7E5E4;border-radius:8px;font-size:13px">
        <option value="recientes" ${ORDEN === 'recientes' ? 'selected' : ''}>Más recientes primero</option>
        <option value="antiguas"  ${ORDEN === 'antiguas'  ? 'selected' : ''}>Más antiguas primero</option>
        <option value="lentas"    ${ORDEN === 'lentas'    ? 'selected' : ''}>Más lentas primero</option>
      </select>
    </div>
    <div class="vml-pills" style="display:flex;gap:6px;flex-wrap:wrap;margin-bottom:14px">
      ${pill('todas', 'Todas', total)}
      ${pill('escalada', 'Escaladas', cont.escalada)}
      ${pill('sombra', 'Sombra', cont.sombra)}
      ${pill('auto', 'IA', cont.auto)}
      ${pill('manual', 'Manuales', cont.manual)}
    </div>`;

  const cuerpo = visibles.length === 0
    ? `<div class="empty">No hay preguntas con ese filtro.</div>`
    : `<div id="bmp-lista">${visibles.map(tarjetaHTML).join('')}</div>
       ${lista.length > visibles.length
          ? `<div style="text-align:center;margin-top:14px">
               <button class="btn btn-ghost" id="bmp-mas">Ver más (${lista.length - visibles.length} restantes)</button></div>`
          : ''}`;

  return tablero + controles + cuerpo;
}

function tarjetaHTML(p) {
  const m = MODOS[p.modo] || MODOS.manual;
  const url = linkML(p);
  const titulo = tituloDe(p);
  const thumb = p.thumbnail
    ? `<img src="${esc(p.thumbnail)}" alt="" style="width:62px;height:62px;object-fit:contain;border-radius:8px;border:1px solid #E7E5E4;background:#fff">`
    : `<div style="width:62px;height:62px;border-radius:8px;border:1px solid #E7E5E4;background:#FAFAF9;display:flex;align-items:center;justify-content:center;font-size:20px;color:#A8A29E">📦</div>`;
  const abierta = ABIERTA === p.question_id;

  return `
    <div style="border:1px solid #E7E5E4;border-left:3px solid ${m.color};border-radius:10px;padding:14px 16px;margin-bottom:10px;background:#fff">
      <div style="display:flex;gap:12px;align-items:flex-start">
        ${thumb}
        <div style="flex:1;min-width:0">
          <div style="font-size:14px;font-weight:700;color:#1C1917;margin-bottom:5px">
            ${url ? `<a href="${esc(url)}" target="_blank" rel="noopener" style="color:inherit;text-decoration:none">${esc(titulo)}</a>` : esc(titulo)}
          </div>
          <div style="display:flex;gap:5px;flex-wrap:wrap;align-items:center">
            ${chip(p.mla || '—', '#1D4ED8', '#DBEAFE')}
            ${p.sku ? chip('SKU: ' + p.sku, '#B45309', '#FEF3C7') : ''}
            ${p.comprador_nick ? chip('👤 ' + p.comprador_nick, '#0F6E56', '#DCFCE7') : ''}
            ${p.flex ? chip('Flex', '#6D28D9', '#EDE9FE') : ''}
            ${p.envio_gratis ? chip('Envío gratis', '#0F6E56', '#DCFCE7') : ''}
          </div>
          <div style="font-size:11.5px;color:#78716C;margin-top:6px">${fechaLarga(p.recibida)}</div>
        </div>
      </div>

      <div style="margin-top:12px;padding:10px 12px;background:#FAFAF9;border-radius:8px;font-size:13px;color:#44403C">
        💬 ${esc(p.pregunta || '(sin texto)')}
      </div>

      ${p.respuesta
        ? `<div style="margin-top:8px;padding:10px 12px;background:#fff;border:1px solid #E7E5E4;border-radius:8px;font-size:13px;color:#1C1917">
             ✍️ ${esc(p.respuesta)}
             <div style="font-size:11px;color:#A8A29E;margin-top:6px">${p.respuesta.length} caracteres${p.modo === 'sombra' ? ' · NO posteada en ML' : ''}</div>
           </div>`
        : `<div style="margin-top:8px;padding:10px 12px;background:#FEF2F2;border:1px solid #FECACA;border-radius:8px;font-size:13px;color:#991B1B">
             Sin responder — ${esc(p.motivo_escalada || 'motivo no registrado')}
           </div>`}

      <div style="display:flex;gap:10px;align-items:center;margin-top:10px;flex-wrap:wrap">
        ${chip(m.lbl, m.color, m.bg)}
        ${codigoDeMla(p.mla) ? `<button class="bmp-ficha-btn" data-ficha="${esc(codigoDeMla(p.mla))}"
          style="border:1px solid #E7E5E4;background:#fff;border-radius:7px;padding:4px 9px;font-size:11.5px;cursor:pointer;color:#57534E">Editar ficha</button>` : ''}
        <span style="margin-left:auto;font-size:11.5px;color:#78716C">Tiempo: <strong style="color:#1C1917">${dur(p.latencia_seg)}</strong></span>
        <button class="bmp-det-btn" data-q="${p.question_id}"
          style="border:0;background:none;color:#78716C;font-size:11.5px;cursor:pointer">${abierta ? 'Ocultar' : 'Detalle'}</button>
      </div>

      ${abierta ? `
        <div style="margin-top:10px;padding-top:10px;border-top:1px dashed #E7E5E4;font-size:11.5px;color:#78716C;display:flex;gap:18px;flex-wrap:wrap">
          <span>Pregunta #${p.question_id}</span>
          <span>Comprador ${p.comprador_id ?? '—'}</span>
          <span>Categoría ${esc(p.categoria_ml || '—')}</span>
          <span>Respondida ${fechaLarga(p.respondida)}</span>
          ${p.motivo_escalada ? `<span>Motivo: ${esc(p.motivo_escalada)}</span>` : ''}
        </div>` : ''}
    </div>`;
}

// ── Sub-vista: fichas ──────────────────────────────────────────────────────
function renderFichas() {
  const sinMlaReal = PUBLICACIONES.filter(p => String(p.mla).startsWith('FALTA_MLA_'));
  const automap    = PUBLICACIONES.filter(p => p.origen === 'auto_sku');
  const sub = (k, lbl, n) =>
    `<button class="btn ${CAPA === k ? 'btn-primary' : 'btn-ghost'}" data-capa="${k}">${lbl} (${n})</button>`;

  const avisos = `
    ${sinMlaReal.length ? `
      <div style="margin:10px 0 6px;padding:10px 14px;border-radius:8px;background:#FEF3C7;font-size:13px;color:#78350F">
        ⚠️ <strong>${sinMlaReal.length} publicaciones sin MLA real</strong> (id que empieza con <code>FALTA_MLA_</code>).
        Mientras no se corrija, esas publicaciones escalan siempre.
      </div>` : ''}
    ${automap.length ? `
      <div style="margin:6px 0 10px;padding:10px 14px;border-radius:8px;background:#DBEAFE;font-size:13px;color:#1E3A8A">
        🔗 <strong>${automap.length} publicaciones se mapearon solas por SKU.</strong>
        Confirmá que el SKU cargado en ML sea el correcto: si estuviera mal, el bot responde con la ficha de otro producto.
      </div>` : ''}`;

  return avisos + `
    <div style="display:flex;gap:8px;margin:10px 0 14px">
      ${sub('fichas', '📋 Fichas por producto', FICHAS.length)}
      ${sub('publicaciones', '🔗 Publicaciones', PUBLICACIONES.length)}
    </div>
    ${CAPA === 'fichas' ? renderCapaFichas() : renderCapaPublicaciones()}`;
}

function renderCapaFichas() {
  const sel = FICHA_SEL === '__nueva__'
    ? { codigo: '', titulo: '', modelo_fabricante: '', familia: '', ficha: '' }
    : FICHAS.find(f => f.codigo === FICHA_SEL);
  const usos = c => PUBLICACIONES.filter(p => p.codigo_ficha === c).length;

  return `
    <div style="display:flex;gap:16px;align-items:flex-start;flex-wrap:wrap">
      <div style="flex:1 1 320px;min-width:300px">
        <button class="btn btn-primary bmp-ficha-btn" data-ficha="__nueva__" style="margin-bottom:10px">+ Nueva ficha</button>
        <div class="table-wrap" style="max-height:520px;overflow:auto"><table class="t">
          <thead><tr><th style="width:90px">Código</th><th>Producto</th><th style="width:60px">Publ.</th></tr></thead>
          <tbody>${FICHAS.map(f => `
            <tr class="bmp-ficha-btn" data-ficha="${esc(f.codigo)}" style="cursor:pointer;${FICHA_SEL === f.codigo ? 'background:#F5F5F4' : ''}">
              <td style="font-size:11px;font-weight:700">${esc(f.codigo)}</td>
              <td style="font-size:12px">${esc(f.titulo || '—')}</td>
              <td style="font-size:11px;text-align:center">${usos(f.codigo)}</td>
            </tr>`).join('')}</tbody>
        </table></div>
      </div>

      <div style="flex:1 1 420px;min-width:340px">
        ${!sel ? `<div class="empty">Elegí una ficha para verla o editarla. Se edita una sola vez y vale para todas sus publicaciones.</div>` : `
          <div style="border:1px solid #E7E5E4;border-radius:10px;padding:16px">
            ${FICHA_SEL !== '__nueva__' ? `
              <div style="font-size:12px;color:#57534E;margin-bottom:10px;padding:8px 10px;background:#FAFAF9;border-radius:6px">
                Esta ficha la usan <strong>${usos(sel.codigo)}</strong> publicaciones:
                ${PUBLICACIONES.filter(p => p.codigo_ficha === sel.codigo).map(p => esc(p.mla)).join(' · ')}
              </div>` : ''}
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:10px">
              <label style="font-size:12px;color:#78716C">Código (SKU)
                <input id="f-codigo" value="${esc(sel.codigo)}" ${FICHA_SEL === '__nueva__' ? '' : 'readonly'}
                  style="width:100%;box-sizing:border-box;padding:8px;border:1px solid #E7E5E4;border-radius:8px;font-size:13px;${FICHA_SEL === '__nueva__' ? '' : 'background:#FAFAF9'}"></label>
              <label style="font-size:12px;color:#78716C">Título
                <input id="f-titulo" value="${esc(sel.titulo || '')}" style="width:100%;box-sizing:border-box;padding:8px;border:1px solid #E7E5E4;border-radius:8px;font-size:13px"></label>
              <label style="font-size:12px;color:#78716C">Modelo de fábrica
                <input id="f-modelo" value="${esc(sel.modelo_fabricante || '')}" style="width:100%;box-sizing:border-box;padding:8px;border:1px solid #E7E5E4;border-radius:8px;font-size:13px"></label>
              <label style="font-size:12px;color:#78716C">Familia
                <input id="f-familia" value="${esc(sel.familia || '')}" style="width:100%;box-sizing:border-box;padding:8px;border:1px solid #E7E5E4;border-radius:8px;font-size:13px"></label>
            </div>
            <label style="font-size:12px;color:#78716C">Ficha (lo único que lee la IA)
              <textarea id="f-ficha" rows="16" style="width:100%;box-sizing:border-box;padding:10px;border:1px solid #E7E5E4;border-radius:8px;font-size:13px;font-family:ui-monospace,monospace;line-height:1.45">${esc(sel.ficha || '')}</textarea></label>
            <div style="display:flex;gap:8px;align-items:center;margin-top:10px">
              <button class="btn btn-primary" id="f-guardar">Guardar ficha</button>
              <button class="btn btn-ghost" id="f-cancelar">Cancelar</button>
              <span style="font-size:11px;color:#78716C;margin-left:auto">Mismo nombre ≠ mismo producto: usá el modelo de fábrica.</span>
            </div>
          </div>`}
      </div>
    </div>`;
}

function renderCapaPublicaciones() {
  return `
    <div style="font-size:12px;color:#78716C;margin-bottom:8px">
      Cada publicación apunta a una ficha. Para dar de alta una publicación nueva de un producto ya fichado,
      alcanza con el MLA y el código — el bot también la resuelve solo si el SKU está bien cargado en Mercado Libre.
    </div>
    <div style="display:flex;gap:8px;align-items:flex-end;flex-wrap:wrap;margin-bottom:12px">
      <label style="font-size:12px;color:#78716C">MLA
        <input id="p-mla" placeholder="MLA123456789" style="display:block;padding:8px;border:1px solid #E7E5E4;border-radius:8px;font-size:13px"></label>
      <label style="font-size:12px;color:#78716C">Ficha
        <select id="p-codigo" style="display:block;padding:8px;border:1px solid #E7E5E4;border-radius:8px;font-size:13px;max-width:320px">
          ${FICHAS.map(f => `<option value="${esc(f.codigo)}">${esc(f.codigo)} — ${esc(f.titulo || '')}</option>`).join('')}
        </select></label>
      <button class="btn btn-primary" id="p-agregar">Agregar publicación</button>
    </div>
    <div class="table-wrap" style="max-height:520px;overflow:auto"><table class="t">
      <thead><tr><th style="width:150px">MLA</th><th>Título</th><th style="width:80px">SKU</th><th style="width:90px">Ficha</th><th style="width:70px">Origen</th></tr></thead>
      <tbody>${PUBLICACIONES.map(p => `
        <tr>
          <td style="font-size:11px;${String(p.mla).startsWith('FALTA_MLA_') ? 'color:#B45309;font-weight:700' : ''}">${esc(p.mla)}</td>
          <td style="font-size:12px">${esc(p.titulo || '—')}</td>
          <td style="font-size:11px">${esc(p.sku || '—')}</td>
          <td style="font-size:11px"><button class="bmp-ficha-btn" data-ficha="${esc(p.codigo_ficha)}"
            style="border:0;background:none;color:#1D4ED8;cursor:pointer;font-size:11px;padding:0">${esc(p.codigo_ficha)}</button></td>
          <td>${p.origen === 'auto_sku' ? chip('auto', '#1E3A8A', '#DBEAFE') : chip('manual', '#57534E', '#F5F5F4')}</td>
        </tr>`).join('')}</tbody>
    </table></div>`;
}

// ── Eventos ────────────────────────────────────────────────────────────────
function bind() {
  const root = document.getElementById('app-screens');

  root.querySelectorAll('[data-sub]').forEach(b =>
    b.addEventListener('click', () => { SUB = b.dataset.sub; ABIERTA = null; render(); }));

  const dias = document.getElementById('bmp-dias');
  if (dias) dias.addEventListener('change', async e => {
    DIAS = Number(e.target.value); LIMITE = 25; await recargar(); render();
  });

  const rl = document.getElementById('bmp-reload');
  if (rl) rl.addEventListener('click', async () => { await recargar(); render(); });

  const csv = document.getElementById('bmp-csv');
  if (csv) csv.addEventListener('click', exportarCSV);

  const busca = document.getElementById('bmp-busca');
  if (busca) busca.addEventListener('input', e => {
    BUSCA = e.target.value; LIMITE = 25;
    render();
    const n = document.getElementById('bmp-busca');
    if (n) { n.focus(); n.setSelectionRange(n.value.length, n.value.length); }
  });

  const orden = document.getElementById('bmp-orden');
  if (orden) orden.addEventListener('change', e => { ORDEN = e.target.value; render(); });

  const mas = document.getElementById('bmp-mas');
  if (mas) mas.addEventListener('click', () => { LIMITE += 25; render(); });

  root.querySelectorAll('.pill').forEach(p =>
    p.addEventListener('click', () => { FILTRO = p.dataset.f; LIMITE = 25; ABIERTA = null; render(); }));

  root.querySelectorAll('.bmp-det-btn').forEach(b =>
    b.addEventListener('click', () => {
      const id = Number(b.dataset.q);
      ABIERTA = ABIERTA === id ? null : id;
      render();
    }));

  root.querySelectorAll('.bmp-ficha-btn').forEach(el =>
    el.addEventListener('click', () => { SUB = 'fichas'; CAPA = 'fichas'; FICHA_SEL = el.dataset.ficha; render(); }));

  root.querySelectorAll('[data-capa]').forEach(b =>
    b.addEventListener('click', () => { CAPA = b.dataset.capa; render(); }));

  const pAdd = document.getElementById('p-agregar');
  if (pAdd) pAdd.addEventListener('click', agregarPublicacion);

  const guardar = document.getElementById('f-guardar');
  if (guardar) guardar.addEventListener('click', guardarFicha);

  const cancelar = document.getElementById('f-cancelar');
  if (cancelar) cancelar.addEventListener('click', () => { FICHA_SEL = null; render(); });
}

function exportarCSV() {
  const cols = ['recibida', 'modo', 'mla', 'sku', 'titulo', 'comprador_nick', 'pregunta', 'respuesta', 'motivo_escalada', 'latencia_seg'];
  const q = v => `"${String(v ?? '').replace(/"/g, '""').replace(/\r?\n/g, ' ')}"`;
  const filas = [cols.join(';'), ...PREGUNTAS.map(p => cols.map(c => q(p[c])).join(';'))];
  const blob = new Blob(['\uFEFF' + filas.join('\n')], { type: 'text/csv;charset=utf-8' });
  const a = document.createElement('a');
  a.href = URL.createObjectURL(blob);
  a.download = `preguntas-ml-${new Date().toISOString().slice(0, 10)}.csv`;
  a.click();
  URL.revokeObjectURL(a.href);
}

async function guardarFicha() {
  const val = id => (document.getElementById(id)?.value ?? '').trim();
  const codigo = val('f-codigo');
  const ficha = document.getElementById('f-ficha').value.trim();
  if (!codigo) { window.toast('Falta el código (SKU)', 'err'); return; }
  if (!ficha)  { window.toast('La ficha no puede quedar vacía', 'err'); return; }

  const body = {
    titulo: val('f-titulo') || null,
    modelo_fabricante: val('f-modelo') || null,
    familia: val('f-familia') || null,
    ficha,
    actualizado: new Date().toISOString()
  };

  const btn = document.getElementById('f-guardar');
  btn.disabled = true; btn.textContent = 'Guardando…';
  try {
    if (FICHA_SEL === '__nueva__') await sbPost('bot_ml_fichas', { codigo, ...body });
    else await sbPatch('bot_ml_fichas', `codigo=eq.${encodeURIComponent(FICHA_SEL)}`, body);
    await recargar();
    FICHA_SEL = codigo;
    window.toast('Ficha guardada');
  } catch (e) {
    console.error(e);
    window.toast('No se pudo guardar: ' + e.message, 'err');
  } finally {
    render();
  }
}

async function agregarPublicacion() {
  const mla = (document.getElementById('p-mla')?.value ?? '').trim();
  const codigo = document.getElementById('p-codigo')?.value;
  if (!mla) { window.toast('Falta el MLA', 'err'); return; }
  if (!codigo) { window.toast('Elegí una ficha', 'err'); return; }
  const btn = document.getElementById('p-agregar');
  btn.disabled = true; btn.textContent = 'Agregando…';
  try {
    const f = FICHAS.find(x => x.codigo === codigo);
    await sbPost('bot_ml_publicaciones', {
      mla, codigo_ficha: codigo, sku: codigo, titulo: f ? f.titulo : null, origen: 'manual'
    });
    await recargar();
    window.toast('Publicación agregada');
  } catch (e) {
    console.error(e);
    window.toast('No se pudo agregar: ' + e.message, 'err');
  } finally {
    render();
  }
}
