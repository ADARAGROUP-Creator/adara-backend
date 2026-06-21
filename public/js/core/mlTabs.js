// ── Sub-pestañas del grupo "Mercado Libre" (Ventas ML | Flex) ───────────
// Barra compartida que se muestra arriba de ambas pantallas, para que el grupo
// se sienta una sola sección. `active` = 'ventas_ml' | 'flex'.

export function mlTabs(active) {
  injectMlTabsStyle();
  const tab = (hash, label) =>
    `<a class="ml-tab ${active === hash ? 'active' : ''}" href="#${hash}">${label}</a>`;
  return `<div class="ml-tabs">${tab('ventas_ml', 'Ventas ML')}${tab('flex', 'Flex')}</div>`;
}

function injectMlTabsStyle() {
  if (document.getElementById('ml-tabs-style')) return;
  const css = `
    .ml-tabs{display:flex;gap:4px;border-bottom:1px solid #E7E5E4;margin-bottom:16px}
    .ml-tab{padding:8px 16px;font-size:14px;font-weight:600;color:#78716C;text-decoration:none;border-bottom:2px solid transparent;margin-bottom:-1px}
    .ml-tab:hover{color:#1C1917}
    .ml-tab.active{color:#1C1917;border-bottom-color:#1C1917}`;
  const s = document.createElement('style');
  s.id = 'ml-tabs-style';
  s.textContent = css;
  document.head.appendChild(s);
}
