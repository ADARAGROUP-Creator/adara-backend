-- Pricing en ADARA APP — paso 2b: tablas propias de pricing (25/9/2026).
-- Port del modelo de pricing-adara-online (products, tax_settings, mercadolibre_installment_fees,
-- mercadolibre_category_fees, product_channel_margins, product_cost_history), atado a skus.id.
-- PRC1/PRC2: son datos de SIMULACIÓN. Nunca se escriben en skus, lotes ni iibb_parametros.
-- Tasas siempre en PORCENTAJE (21 = 21 %), igual que public/js/core/pricing.js.

-- ── Timestamp de actualización ─────────────────────────────────────────
create or replace function public.fn_pricing_actualizado()
returns trigger language plpgsql as $$
begin
  new.actualizado_en = now();
  return new;
end;
$$;

-- ── Costo de simulación por SKU (products) ─────────────────────────────
create table public.pricing_producto (
  sku_id           bigint primary key references public.skus(id) on delete cascade,
  costo_sin_iva    numeric(14,2) not null default 0 check (costo_sin_iva >= 0),
  iva_pct          numeric(5,2)  not null default 21 check (iva_pct in (21, 10.5)),
  categoria        text,                 -- une con pricing_comision_categoria.categoria
  -- Envío y cargo fijo de ML, BRUTOS (con IVA), cargados a mano hasta tener el sync de
  -- publicaciones (paso 3). En pricing salen de mercadolibre_shipping_costs.
  envio_ml_monto   numeric(14,2) not null default 0 check (envio_ml_monto >= 0),
  cargo_fijo_ml_monto numeric(14,2) not null default 0 check (cargo_fijo_ml_monto >= 0),
  estado           text not null default 'active' check (estado in ('active', 'paused', 'discontinued')),
  notas            text,
  creado_en        timestamptz not null default now(),
  actualizado_en   timestamptz not null default now()
);
create trigger trg_pricing_producto_actualizado before update on public.pricing_producto
  for each row execute function public.fn_pricing_actualizado();

-- ── Historia del costo de simulación (product_cost_history) ────────────
create table public.pricing_costo_historial (
  id               bigint generated always as identity primary key,
  sku_id           bigint not null references public.skus(id) on delete cascade,
  costo_anterior   numeric(14,2),
  costo_nuevo      numeric(14,2) not null,
  iva_anterior     numeric(5,2),
  iva_nuevo        numeric(5,2) not null,
  cambiado_por     uuid default auth.uid(),
  cambiado_en      timestamptz not null default now()
);
create index pricing_costo_historial_sku_idx on public.pricing_costo_historial (sku_id, cambiado_en desc, id desc);

create or replace function public.fn_pricing_costo_historial()
returns trigger language plpgsql as $$
begin
  if tg_op = 'INSERT'
     or new.costo_sin_iva is distinct from old.costo_sin_iva
     or new.iva_pct is distinct from old.iva_pct then
    insert into public.pricing_costo_historial (sku_id, costo_anterior, costo_nuevo, iva_anterior, iva_nuevo)
    values (new.sku_id,
            case when tg_op = 'UPDATE' then old.costo_sin_iva end, new.costo_sin_iva,
            case when tg_op = 'UPDATE' then old.iva_pct end, new.iva_pct);
  end if;
  return new;
end;
$$;
create trigger trg_pricing_costo_historial after insert or update on public.pricing_producto
  for each row execute function public.fn_pricing_costo_historial();

-- ── Tasas globales (tax_settings) — una sola fila ──────────────────────
create table public.pricing_tasas (
  id               smallint primary key default 1 check (id = 1),
  iibb_pct         numeric(8,4) not null default 0 check (iibb_pct >= 0),
  idc_pct          numeric(8,4) not null default 0 check (idc_pct >= 0),
  iigg_pct         numeric(8,4) not null default 0 check (iigg_pct >= 0 and iigg_pct < 100),
  estructura_pct   numeric(8,4) not null default 0 check (estructura_pct >= 0),
  notas            text,
  actualizado_en   timestamptz not null default now()
);
create trigger trg_pricing_tasas_actualizado before update on public.pricing_tasas
  for each row execute function public.fn_pricing_actualizado();

-- ── Canales de precio (mercadolibre_installment_fees) ──────────────────
-- aplica_*: NULL = default de normalizeOption (ML: sí; directo: no). Igual que pricing.
create table public.pricing_canal (
  codigo           text primary key,
  nombre           text not null,
  tipo             text not null default 'directo' check (tipo in ('mercadolibre', 'directo', 'web', 'posnet', 'otro')),
  cuotas           integer check (cuotas is null or cuotas > 0),
  costo_financiacion_pct numeric(8,4) not null default 0 check (costo_financiacion_pct >= 0),
  margen_default_pct numeric(8,4) not null default 10,
  redondeo_a       integer not null default 100 check (redondeo_a >= 1),
  modo_redondeo    text not null default 'nearest' check (modo_redondeo in ('nearest', 'up', 'down')),
  aplica_comision_ml boolean,
  aplica_envio     boolean,
  aplica_iibb      boolean,
  aplica_idc       boolean,
  aplica_iigg      boolean,
  aplica_estructura boolean,
  aplica_iva       boolean,
  orden            integer not null default 100,
  activo           boolean not null default true,
  notas            text,
  actualizado_en   timestamptz not null default now()
);
create trigger trg_pricing_canal_actualizado before update on public.pricing_canal
  for each row execute function public.fn_pricing_actualizado();

-- ── Comisión de ML por categoría (mercadolibre_category_fees) ──────────
create table public.pricing_comision_categoria (
  id               bigint generated always as identity primary key,
  categoria        text not null,
  comision_pct     numeric(8,4) not null default 0 check (comision_pct >= 0 and comision_pct < 100),
  ml_category_ids  text[],
  activo           boolean not null default true,
  notas            text,
  actualizado_en   timestamptz not null default now()
);
create unique index pricing_comision_categoria_uk on public.pricing_comision_categoria (lower(categoria));
create trigger trg_pricing_comision_categoria_actualizado before update on public.pricing_comision_categoria
  for each row execute function public.fn_pricing_actualizado();

-- ── Configuración por SKU × canal (product_channel_margins) ───────────
create table public.pricing_margen (
  id               bigint generated always as identity primary key,
  sku_id           bigint not null references public.skus(id) on delete cascade,
  canal_codigo     text not null references public.pricing_canal(codigo) on update cascade,
  margen_pct       numeric(8,4) not null default 5,
  utilidad_neta    numeric(14,2),                -- si está, manda sobre margen_pct
  pvp_manual       numeric(14,2) check (pvp_manual is null or pvp_manual > 0),  -- si está, manda sobre todo
  estructura_monto numeric(14,2) not null default 0,
  envio_manual_monto numeric(14,2) not null default 0,
  comision_venta_pct numeric(8,4) not null default 0,
  venta_con_iva    boolean,                      -- NULL = lo que diga el canal
  iva_costo_pct    numeric(10,2) not null default 0,
  descuento_promo_pct numeric not null default 0,
  notas            text,
  actualizado_en   timestamptz not null default now(),
  unique (sku_id, canal_codigo)
);
create trigger trg_pricing_margen_actualizado before update on public.pricing_margen
  for each row execute function public.fn_pricing_actualizado();

-- ── Seguridad (patrón post-A17) ────────────────────────────────────────
do $$
declare t text;
begin
  foreach t in array array['pricing_producto', 'pricing_costo_historial', 'pricing_tasas',
                           'pricing_canal', 'pricing_comision_categoria', 'pricing_margen'] loop
    execute format('alter table public.%I enable row level security', t);
    execute format('create policy "%s_authenticated" on public.%I for all to authenticated using (true) with check (true)', t, t);
    execute format('grant all on public.%I to authenticated', t);
    execute format('revoke all on public.%I from anon', t);
  end loop;
end $$;

-- ── Datos iniciales ────────────────────────────────────────────────────
-- Tasas: sólo IIBB 5 % (el de pricing, ADARA-PRICING.md). El resto en 0 hasta tener
-- los valores reales de la base de pricing (pregunta 10 del handoff).
insert into public.pricing_tasas (id, iibb_pct, notas)
values (1, 5, 'IIBB 5 % tomado de pricing. IDC, Ganancias y estructura pendientes de copiar de pricing.');

-- Canales: los 9 de pricing. Costos de cuotas MP de su migración 037.
insert into public.pricing_canal (codigo, nombre, tipo, cuotas, costo_financiacion_pct, orden, aplica_iva, notas) values
  ('MC',   'Mercado Libre Clásica',          'mercadolibre', null, 0,    1, null, null),
  ('MP3',  'Mercado Libre Premium 3 cuotas', 'mercadolibre', 3,    8.4,  2, null, 'Costo de cuotas de pricing (migración 037). Confirmar vigente.'),
  ('MP6',  'Mercado Libre Premium 6 cuotas', 'mercadolibre', 6,    12.3, 3, null, 'Costo de cuotas de pricing (migración 037). Confirmar vigente.'),
  ('MP9',  'Mercado Libre Premium 9 cuotas', 'mercadolibre', 9,    15.7, 4, null, 'Costo de cuotas de pricing (migración 037). Confirmar vigente.'),
  ('MP12', 'Mercado Libre Premium 12 cuotas','mercadolibre', 12,   19.2, 5, null, 'Costo de cuotas de pricing (migración 037). Confirmar vigente.'),
  ('TN',   'Tienda Nube contado',            'web',          null, 0,    6, null, 'Flags y costos pendientes de copiar de pricing.'),
  ('TN6',  'Tienda Nube 6 cuotas',           'web',          6,    0,    7, null, 'Costo de cuotas pendiente de copiar de pricing.'),
  ('EF',   'Efectivo / directo',             'directo',      null, 0,    8, false, 'Sin comisión ni envío de ML; normalmente sin IVA de venta.'),
  ('TR',   'Transferencia',                  'directo',      null, 0,    9, null, 'Flags pendientes de copiar de pricing.');
