-- ============================================================
-- Import History — Migration
-- Safe to re-run (idempotent)
-- ============================================================

CREATE TABLE IF NOT EXISTS import_history (
    id           VARCHAR(36)  PRIMARY KEY DEFAULT gen_random_uuid()::text,
    store_id     VARCHAR(36)  NOT NULL REFERENCES stores(id),
    import_type  VARCHAR(30)  NOT NULL,
    filename     VARCHAR(255),
    rows_imported INTEGER NOT NULL DEFAULT 0,
    rows_deleted  INTEGER NOT NULL DEFAULT 0,
    imported_by  VARCHAR(100),
    imported_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_import_history_store_date
    ON import_history(store_id, imported_at DESC);

ALTER TABLE sales_orders
    ADD COLUMN IF NOT EXISTS import_batch_id VARCHAR(36)
    REFERENCES import_history(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS ix_sales_orders_batch_id
    ON sales_orders(import_batch_id)
    WHERE import_batch_id IS NOT NULL;
