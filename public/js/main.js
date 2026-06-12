import { initSB, login, changePassword, logout, hasSession, sessionUser } from './core/sb.js';
import { loadHome } from './screens/home.js';
import { loadSkus } from './screens/skus.js';
import { loadMovimientos } from './screens/movimientos.js';
import { loadSaldos } from './screens/saldos.js';
import { loadVentasML } from './screens/ventas-ml.js?v=dev5';
import { loadGastos } from './screens/gastos.js';
import { loadConciliacion } from './screens/conciliacion.js';
import { loadCompras } from './screens/compras.js';
import { loadPSI } from './screens/psi.js';
import { loadCosteo } from './screens/costeo.js';
import { loadResultado } from './screens/resultado.js';
import { loadPosicionFiscal } from './screens/posicion-fiscal.js';

const screens = {
  home: { fn: loadHome, title: 'Home', sub: 'Resumen del estado actual' },
  skus: { fn: loadSkus, title: 'SKUs', sub: 'Catálogo de productos' },
  movimientos: { fn: loadMovimientos, title: 'Movimientos', sub: 'Extracto unificado de cuentas' },
  saldos: { fn: loadSaldos, title: 'Saldos', sub: 'Saldo por cuenta y carga del saldo de arranque' },
  ventas_ml: { fn: loadVentasML, title: 'Ventas ML', sub: 'Ventas de Mercado Libre y su cobro' },
  conciliacion: { fn: loadConciliacion, title: 'Conciliación', sub: 'Vincular movimientos con operaciones' },
  gastos: { fn: loadGastos, title: 'Gastos', sub: 'Gastos operativos' },
  compras: { fn: loadCompras, title: 'Compras', sub: 'Facturas de mercadería y cuenta corriente' },
  psi: { fn: loadPSI, title: 'PSI Recompra', sub: 'Planificación de recompra por SKU' },
  costeo: { fn: loadCosteo, title: 'Inventario', sub: 'Valor del stock y productos sin costo' },
  resultado: { fn: loadResultado, title: 'Resultado', sub: 'Estado de resultado mensual por línea' },
  posicion_fiscal: { fn: loadPosicionFiscal, title: 'Posición Fiscal', sub: 'IVA, IIBB y pagos a cuenta del período' }
};

async function route() {
  const hash = (location.hash || '#home').slice(1);
  const screen = screens[hash] || screens.home;
  // Actualizar nav activo
  document.querySelectorAll('.nav-item').forEach(a => {
    a.classList.toggle('active', a.getAttribute('href') === '#' + hash);
  });
  // Actualizar topbar
  document.getElementById('topbar-title').textContent = screen.title;
  document.getElementById('topbar-sub').textContent = screen.sub;
  // Cargar pantalla
  try {
    await screen.fn();
  } catch (e) {
    console.error('Pantalla falló:', e);
    document.getElementById('app-screens').innerHTML =
      `<div class="error"><strong>Error al cargar la pantalla.</strong><br>${e.message}</div>`;
  }
}

// Toast global
window.toast = function(msg, type='ok') {
  const host = document.getElementById('toast-host');
  const el = document.createElement('div');
  el.className = 'toast ' + type;
  el.textContent = msg;
  host.appendChild(el);
  setTimeout(() => { el.style.opacity='0'; el.style.transition='opacity 0.2s'; setTimeout(()=>el.remove(), 200); }, 2200);
};

// ── Login / cambio de contraseña ────────────────────────────────────
function authOverlay(inner) {
  let ov = document.getElementById('auth-overlay');
  if (!ov) {
    ov = document.createElement('div');
    ov.id = 'auth-overlay';
    ov.style.cssText = 'position:fixed;inset:0;z-index:9999;display:flex;align-items:center;justify-content:center;background:#1C1917';
    document.body.appendChild(ov);
  }
  ov.innerHTML = `<div style="background:#fff;border-radius:14px;padding:28px;width:320px;box-shadow:0 10px 40px rgba(0,0,0,.3);font-family:system-ui,sans-serif">${inner}</div>`;
  return ov;
}
function closeAuthOverlay() { const ov = document.getElementById('auth-overlay'); if (ov) ov.remove(); }

function uiLogin() {
  return new Promise(resolve => {
    const ov = authOverlay(`
      <h2 style="margin:0 0 4px;font-size:20px;color:#1C1917">ADARA</h2>
      <p style="margin:0 0 18px;color:#78716C;font-size:13px">Ingresá con tu usuario</p>
      <input id="au-user" placeholder="Usuario" autocomplete="username" style="width:100%;box-sizing:border-box;padding:10px;margin-bottom:10px;border:1px solid #E7E5E4;border-radius:8px;font-size:14px">
      <input id="au-pass" type="password" placeholder="Contraseña" autocomplete="current-password" style="width:100%;box-sizing:border-box;padding:10px;margin-bottom:6px;border:1px solid #E7E5E4;border-radius:8px;font-size:14px">
      <div id="au-err" style="color:#B91C1C;font-size:12px;min-height:16px;margin-bottom:8px"></div>
      <button id="au-go" style="width:100%;padding:11px;border:0;border-radius:8px;background:#1C1917;color:#fff;font-size:14px;font-weight:600;cursor:pointer">Ingresar</button>`);
    const user = ov.querySelector('#au-user'), pass = ov.querySelector('#au-pass'), err = ov.querySelector('#au-err'), go = ov.querySelector('#au-go');
    user.focus();
    const intentar = async () => {
      err.textContent = ''; go.disabled = true; go.textContent = 'Ingresando…';
      const res = await login(user.value, pass.value);
      go.disabled = false; go.textContent = 'Ingresar';
      if (!res.ok) { err.textContent = res.error; pass.value = ''; pass.focus(); return; }
      if (res.mustChange) { await uiChangePassword(); }
      closeAuthOverlay(); resolve();
    };
    go.addEventListener('click', intentar);
    pass.addEventListener('keydown', e => { if (e.key === 'Enter') intentar(); });
    user.addEventListener('keydown', e => { if (e.key === 'Enter') pass.focus(); });
  });
}

function uiChangePassword() {
  return new Promise(resolve => {
    const ov = authOverlay(`
      <h2 style="margin:0 0 4px;font-size:18px;color:#1C1917">Cambiá tu contraseña</h2>
      <p style="margin:0 0 18px;color:#78716C;font-size:13px">Es tu primer ingreso. Elegí una nueva.</p>
      <input id="au-p1" type="password" placeholder="Nueva contraseña" style="width:100%;box-sizing:border-box;padding:10px;margin-bottom:10px;border:1px solid #E7E5E4;border-radius:8px;font-size:14px">
      <input id="au-p2" type="password" placeholder="Repetir contraseña" style="width:100%;box-sizing:border-box;padding:10px;margin-bottom:6px;border:1px solid #E7E5E4;border-radius:8px;font-size:14px">
      <div id="au-err" style="color:#B91C1C;font-size:12px;min-height:16px;margin-bottom:8px"></div>
      <button id="au-go" style="width:100%;padding:11px;border:0;border-radius:8px;background:#1C1917;color:#fff;font-size:14px;font-weight:600;cursor:pointer">Guardar</button>`);
    const p1 = ov.querySelector('#au-p1'), p2 = ov.querySelector('#au-p2'), err = ov.querySelector('#au-err'), go = ov.querySelector('#au-go');
    p1.focus();
    go.addEventListener('click', async () => {
      err.textContent = '';
      if (p1.value.length < 6) { err.textContent = 'Mínimo 6 caracteres'; return; }
      if (p1.value !== p2.value) { err.textContent = 'No coinciden'; return; }
      go.disabled = true; go.textContent = 'Guardando…';
      const res = await changePassword(p1.value);
      go.disabled = false; go.textContent = 'Guardar';
      if (!res.ok) { err.textContent = res.error; return; }
      resolve();
    });
  });
}

async function requireAuth() {
  if (!hasSession()) { await uiLogin(); }
  else if (sessionUser() && sessionUser().must_change) { await uiChangePassword(); }
}

function pintarUsuario() {
  const u = sessionUser(); if (!u) return;
  let chip = document.getElementById('user-chip');
  if (!chip) {
    chip = document.createElement('div');
    chip.id = 'user-chip';
    chip.style.cssText = 'position:fixed;top:12px;right:16px;z-index:50;display:flex;align-items:center;gap:10px;font-size:13px;color:#57534E';
    document.body.appendChild(chip);
  }
  chip.innerHTML = `<span>${u.usuario}</span><button id="user-logout" style="border:1px solid #E7E5E4;background:#fff;border-radius:8px;padding:5px 10px;font-size:12px;cursor:pointer">Salir</button>`;
  chip.querySelector('#user-logout').addEventListener('click', async () => { await logout(); location.reload(); });
}

window.addEventListener('adara-auth-expired', () => { location.reload(); });

(async () => {
  try {
    await initSB();
    await requireAuth();
    pintarUsuario();
    window.addEventListener('hashchange', route);
    await route();
  } catch (e) {
    console.error('Bootstrap falló:', e);
    document.getElementById('app-screens').innerHTML =
      `<div class="error"><strong>No se pudo inicializar la app.</strong><br>Abrí F12 → Consola.<br><br>Detalle: ${e.message}</div>`;
  }
})();
