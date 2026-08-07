-- F1 — DDL brands + columnas brand_id (Rodmat multi-brand)
-- Fecha: 2026-08-07
-- Target: BRANCH staging (ytfqsdmkynnokncpeiew)

BEGIN;

-- 1) Tabla brands
CREATE TABLE IF NOT EXISTS public.brands (
    id                    VARCHAR(36) PRIMARY KEY,
    store_id              VARCHAR(36) NOT NULL REFERENCES public.stores(id) ON DELETE CASCADE,
    slug                  VARCHAR(50) NOT NULL,
    display_name          VARCHAR(100) NOT NULL,
    sku_prefixes_note     TEXT,
    brand_color           VARCHAR(20),
    email_sender          VARCHAR(200),
    absorbs_shared_costs  BOOLEAN NOT NULL DEFAULT FALSE,
    is_active             BOOLEAN NOT NULL DEFAULT TRUE,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_brands_store_slug UNIQUE (store_id, slug)
);
CREATE INDEX IF NOT EXISTS ix_brands_store_id ON public.brands(store_id);

-- 2) Feature flag
ALTER TABLE public.stores ADD COLUMN IF NOT EXISTS brands_enabled BOOLEAN NOT NULL DEFAULT FALSE;

-- 3) brand_id en tablas
ALTER TABLE public.products             ADD COLUMN IF NOT EXISTS brand_id VARCHAR(36) REFERENCES public.brands(id);
ALTER TABLE public.combos               ADD COLUMN IF NOT EXISTS brand_id VARCHAR(36) REFERENCES public.brands(id);
ALTER TABLE public.incoming_stock       ADD COLUMN IF NOT EXISTS brand_id VARCHAR(36) REFERENCES public.brands(id);
ALTER TABLE public.sales_orders         ADD COLUMN IF NOT EXISTS brand_id VARCHAR(36) REFERENCES public.brands(id);
ALTER TABLE public.sales_orders         ADD COLUMN IF NOT EXISTS shipping_brand_id VARCHAR(36) REFERENCES public.brands(id);
ALTER TABLE public.finance_custom_lines ADD COLUMN IF NOT EXISTS brand_id VARCHAR(36) REFERENCES public.brands(id);
ALTER TABLE public.agent_runs           ADD COLUMN IF NOT EXISTS brand_id VARCHAR(36) REFERENCES public.brands(id);
ALTER TABLE public.users                ADD COLUMN IF NOT EXISTS brand_id VARCHAR(36) REFERENCES public.brands(id);

-- 4) Índices
CREATE INDEX IF NOT EXISTS ix_products_brand_id       ON public.products(brand_id)              WHERE brand_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_combos_brand_id         ON public.combos(brand_id)                WHERE brand_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_incoming_brand_id       ON public.incoming_stock(brand_id)        WHERE brand_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_sales_brand_id          ON public.sales_orders(brand_id)          WHERE brand_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_sales_shipping_brand    ON public.sales_orders(shipping_brand_id) WHERE shipping_brand_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_finance_brand_id        ON public.finance_custom_lines(brand_id)  WHERE brand_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_agent_runs_brand_id     ON public.agent_runs(brand_id)            WHERE brand_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_users_brand_id          ON public.users(brand_id)                 WHERE brand_id IS NOT NULL;

-- 5) Overhead allocation
CREATE TABLE IF NOT EXISTS public.overhead_allocation_rules (
    id               VARCHAR(36) PRIMARY KEY,
    store_id         VARCHAR(36) NOT NULL REFERENCES public.stores(id),
    concept_pattern  VARCHAR(200) NOT NULL,
    split_type       VARCHAR(20) NOT NULL,
    brand_shares     JSONB NOT NULL,
    effective_from   DATE NOT NULL,
    effective_to     DATE,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

COMMIT;
