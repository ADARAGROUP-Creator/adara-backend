// ── Selector de SKU con búsqueda ───────────────────────────────────────
// Reemplaza al <select> de SKUs, que con ~200 productos es inusable: hay que
// scrollear a ciegas y el nombre completo no entra en el ancho del combo.
//
// Integración pensada para NO tocar la lógica existente: el componente mantiene
// un <input type="hidden"> con la clase que le pidas (`hiddenClass`) y dispara
// un evento 'change' que burbujea. O sea, un `row.querySelector('.com-it-sku').value`
// y un listener delegado de 'change' siguen funcionando igual que con el <select>.
//
// Uso:
//   const p = crearSkuPicker(contenedor, { skus: SKUS, value: it.sku_id, hiddenClass: 'com-it-sku' });
//   p.value            // id del SKU seleccionado ('' si ninguno)
//   p.destroy()        // saca el listener global de cierre
//
// Búsqueda: por código Y por descripción, sin acentos, todos los términos deben
// aparecer (escribir "jbl 520" encuentra "JBL520BT — Auricular JBL 520 BT").

const norm = s => String(s || '')
  .toLowerCase()
  .normalize('NFD').replace(/[\u0300-\u036f]/g, '');

const etiqueta = s => `${s.codigo}${s.descripcion ? ' — ' + s.descripcion : ''}`;

const MAX_RESULTADOS = 60;

function filtrar(skus, q) {
  const t = norm(q).split(/\s+/).filter(Boolean);
  if (!t.length) return skus.slice(0, MAX_RESULTADOS);

  const scored = [];
  for (const s of skus) {
    const cod = norm(s.codigo);
    const txt = cod + ' ' + norm(s.descripcion);
    if (!t.every(x => txt.includes(x))) continue;
    // Orden de relevancia: código exacto → código empieza con → código contiene → resto
    const j = t.join(' ');
    const score = cod === j ? 0 : cod.startsWith(t[0]) ? 1 : cod.includes(t[0]) ? 2 : 3;
    scored.push({ s, score });
  }
  scored.sort((a, b) => a.score - b.score || String(a.s.codigo).localeCompare(String(b.s.codigo)));
  return scored.slice(0, MAX_RESULTADOS).map(x => x.s);
}

export function crearSkuPicker(host, opts = {}) {
  const {
    skus = [],
    value = '',
    hiddenClass = '',
    placeholder = 'Buscá por código o nombre…',
    onChange = null
  } = opts;

  inyectarEstilo();

  const byId = new Map(skus.map(s => [String(s.id), s]));
  let sel = byId.get(String(value)) || null;
  let activo = -1;
  let lista = [];

  const raiz = document.createElement('div');
  raiz.className = 'skp';

  const hidden = document.createElement('input');
  hidden.type = 'hidden';
  if (hiddenClass) hidden.className = hiddenClass;
  hidden.value = sel ? String(sel.id) : '';

  const input = document.createElement('input');
  input.type = 'text';
  input.className = 'input skp-input';
  input.autocomplete = 'off';
  input.spellcheck = false;
  input.placeholder = placeholder;
  input.value = sel ? etiqueta(sel) : '';

  // El menú vive en <body> con position:fixed, NO dentro del componente.
  // Motivo: `.modal` tiene overflow-y:auto, así que un dropdown absoluto adentro se
  // recorta apenas la fila está cerca del borde inferior — que es justo el caso de la
  // última fila de ítems de una compra.
  const menu = document.createElement('div');
  menu.className = 'skp-menu';
  menu.hidden = true;
  document.body.appendChild(menu);

  raiz.append(hidden, input);
  host.innerHTML = '';
  host.appendChild(raiz);

  const ubicar = () => {
    const r = input.getBoundingClientRect();
    const ancho = Math.max(r.width, 340);
    const espacioAbajo = window.innerHeight - r.bottom;
    const alto = Math.min(280, menu.scrollHeight || 280);
    const arriba = espacioAbajo < alto + 12 && r.top > espacioAbajo;
    menu.style.width = ancho + 'px';
    menu.style.left = Math.max(8, Math.min(r.left, window.innerWidth - ancho - 8)) + 'px';
    if (arriba) { menu.style.top = ''; menu.style.bottom = (window.innerHeight - r.top + 4) + 'px'; }
    else        { menu.style.bottom = ''; menu.style.top = (r.bottom + 4) + 'px'; }
  };

  const pintar = (q) => {
    lista = filtrar(skus, q);
    menu.innerHTML = '';
    if (!lista.length) {
      const vacio = document.createElement('div');
      vacio.className = 'skp-vacio';
      vacio.textContent = skus.length ? 'Sin resultados' : 'No hay SKUs cargados';
      menu.appendChild(vacio);
      activo = -1;
      return;
    }
    lista.forEach((s, i) => {
      const it = document.createElement('div');
      it.className = 'skp-op' + (sel && String(sel.id) === String(s.id) ? ' sel' : '');
      it.dataset.i = String(i);
      const cod = document.createElement('span');
      cod.className = 'skp-cod';
      cod.textContent = s.codigo;                    // textContent: nada de HTML inyectado
      const des = document.createElement('span');
      des.className = 'skp-des';
      des.textContent = s.descripcion || '';
      it.append(cod, des);
      menu.appendChild(it);
    });
    activo = lista.findIndex(s => sel && String(s.id) === String(sel.id));
    if (activo < 0) activo = 0;
    marcarActivo(false);
  };

  const marcarActivo = (scroll = true) => {
    menu.querySelectorAll('.skp-op').forEach((el, i) => el.classList.toggle('act', i === activo));
    if (!scroll) return;
    const el = menu.querySelector('.skp-op.act');
    if (el) el.scrollIntoView({ block: 'nearest' });
  };

  const abrir = () => { if (menu.hidden) { pintar(''); menu.hidden = false; ubicar(); } };
  const cerrar = () => { menu.hidden = true; };
  // Con el menú en <body> hay que reubicarlo si algo scrollea debajo (el modal, la página).
  // `true` = fase de captura: agarra el scroll de cualquier contenedor, no solo el de window.
  const reubicar = () => { if (!menu.hidden) ubicar(); };
  window.addEventListener('scroll', reubicar, true);
  window.addEventListener('resize', reubicar);

  const elegir = (s) => {
    sel = s || null;
    hidden.value = sel ? String(sel.id) : '';
    input.value = sel ? etiqueta(sel) : '';
    cerrar();
    // Burbujea para que los listeners delegados que antes escuchaban al <select>
    // sigan andando sin cambios.
    hidden.dispatchEvent(new Event('change', { bubbles: true }));
    if (onChange) onChange(sel);
  };

  input.addEventListener('focus', () => { input.select(); abrir(); });
  input.addEventListener('input', () => { menu.hidden = false; pintar(input.value); ubicar(); });

  input.addEventListener('keydown', e => {
    if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
      e.preventDefault();
      if (menu.hidden) { abrir(); return; }
      if (!lista.length) return;
      activo = (activo + (e.key === 'ArrowDown' ? 1 : -1) + lista.length) % lista.length;
      marcarActivo();
    } else if (e.key === 'Enter') {
      if (!menu.hidden && lista[activo]) { e.preventDefault(); elegir(lista[activo]); }
    } else if (e.key === 'Escape') {
      if (!menu.hidden) { e.stopPropagation(); cerrar(); restaurar(); }
    }
  });

  // Si el texto quedó a medio escribir, vuelve a mostrar lo que está realmente
  // seleccionado: el input nunca puede mentir sobre el valor guardado.
  const restaurar = () => { input.value = sel ? etiqueta(sel) : ''; };
  input.addEventListener('blur', () => { setTimeout(() => { if (menu.hidden) restaurar(); }, 0); });

  menu.addEventListener('mousedown', e => {
    const op = e.target.closest('.skp-op');
    if (!op) return;
    e.preventDefault();                    // que no dispare el blur antes de elegir
    elegir(lista[+op.dataset.i]);
  });

  const afuera = e => {
    if (raiz.contains(e.target) || menu.contains(e.target)) return;
    cerrar(); restaurar();
  };
  document.addEventListener('mousedown', afuera);

  return {
    get value() { return hidden.value; },
    set value(v) { elegir(byId.get(String(v)) || null); },
    get sku() { return sel; },
    focus() { input.focus(); },
    // Obligatorio al re-renderizar o cerrar el modal: el menú vive en <body>,
    // así que si no se destruye queda un huérfano suelto en el DOM.
    destroy() {
      document.removeEventListener('mousedown', afuera);
      window.removeEventListener('scroll', reubicar, true);
      window.removeEventListener('resize', reubicar);
      menu.remove();
    }
  };
}

function inyectarEstilo() {
  if (document.getElementById('skp-style')) return;
  const s = document.createElement('style');
  s.id = 'skp-style';
  s.textContent = `
    .skp{position:relative}
    .skp-input{width:100%}
    /* z-index por encima de .modal-overlay (200): el menú es hijo de <body>, no del modal */
    .skp-menu{position:fixed;z-index:9999;max-height:280px;overflow-y:auto;
      background:#fff;border:1px solid #D4D4D8;border-radius:8px;box-shadow:0 8px 24px rgba(0,0,0,.14)}
    .skp-menu[hidden]{display:none}
    .skp-op{display:flex;gap:8px;align-items:baseline;padding:6px 10px;cursor:pointer;font-size:12.5px;line-height:1.35}
    .skp-op.act{background:#EFF6FF}
    .skp-op.sel .skp-cod{color:#1D4ED8}
    .skp-cod{font-family:'JetBrains Mono',ui-monospace,monospace;font-weight:600;white-space:nowrap}
    .skp-des{color:#52525B;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
    .skp-vacio{padding:10px;font-size:12.5px;color:#71717A}
  `;
  document.head.appendChild(s);
} 
