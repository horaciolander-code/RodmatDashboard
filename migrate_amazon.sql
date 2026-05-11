-- ============================================================
-- Amazon Multi-Platform Integration — Phase 1 DB Migration
-- Run this in Supabase SQL Editor (or any PostgreSQL client)
-- All statements are idempotent — safe to re-run
-- ============================================================

-- Step 1: platform column on sales_orders
ALTER TABLE sales_orders ADD COLUMN IF NOT EXISTS platform VARCHAR(20) NOT NULL DEFAULT 'tiktok';

-- Step 2: amazon_sku_map table
CREATE TABLE IF NOT EXISTS amazon_sku_map (
    id               VARCHAR(36)  PRIMARY KEY DEFAULT gen_random_uuid()::text,
    store_id         VARCHAR(36)  NOT NULL REFERENCES stores(id),
    amazon_sku       VARCHAR(100) NOT NULL,
    asin             VARCHAR(20),
    amazon_product_name VARCHAR(255),
    product_id       VARCHAR(36)  REFERENCES products(id),
    units_per_sale   INTEGER      NOT NULL DEFAULT 1,
    created_at       TIMESTAMPTZ  DEFAULT NOW(),
    UNIQUE(store_id, amazon_sku)
);
CREATE INDEX IF NOT EXISTS ix_amazon_sku_map_store_id ON amazon_sku_map(store_id);

-- Step 3: new products (Stayfree and TRESemmé)
INSERT INTO products (id, store_id, sku, name, category, units_per_box, status, created_at, updated_at)
SELECT gen_random_uuid()::text, s.id, 'STAYFREE-MAXI-24-12',
       'Stayfree Regular Maxi Pad 24ct Pack of 12', 'Higiene', 12, 'active', NOW(), NOW()
FROM stores s WHERE s.name = 'Rodmat'
ON CONFLICT (store_id, sku) DO NOTHING;

INSERT INTO products (id, store_id, sku, name, category, units_per_box, status, created_at, updated_at)
SELECT gen_random_uuid()::text, s.id, 'TRESEMME-HEAT-TAMER-8OZ',
       'TRESemmé Heat Tamer Spray 8oz', 'Higiene', 1, 'active', NOW(), NOW()
FROM stores s WHERE s.name = 'Rodmat'
ON CONFLICT (store_id, sku) DO NOTHING;

-- Step 4: Amazon SKU map entries (10 products)
-- AV-ID/5: Imari Deodorant x5
INSERT INTO amazon_sku_map (id, store_id, amazon_sku, asin, amazon_product_name, product_id, units_per_sale)
SELECT gen_random_uuid()::text, s.id, 'AV-ID/5', 'B00DPX7NIQ',
       'Avon Imari Deodorant roll-on', p.id, 5
FROM stores s
LEFT JOIN products p ON p.store_id = s.id
    AND p.name ILIKE '%Imari%' AND p.name ILIKE '%Deod%'
WHERE s.name = 'Rodmat'
ON CONFLICT (store_id, amazon_sku) DO NOTHING;

-- AV-SHC/4: Sweet Honesty Skin Softener x4
INSERT INTO amazon_sku_map (id, store_id, amazon_sku, asin, amazon_product_name, product_id, units_per_sale)
SELECT gen_random_uuid()::text, s.id, 'AV-SHC/4', 'B06XJYR245',
       'Avon Sweet Honesty Classic Skin Softener', p.id, 4
FROM stores s
LEFT JOIN products p ON p.store_id = s.id
    AND p.name ILIKE '%Sweet%Honesty%' AND p.name ILIKE '%Skin%'
WHERE s.name = 'Rodmat'
ON CONFLICT (store_id, amazon_sku) DO NOTHING;

-- AV-HC: Haiku Skin Softener x1
INSERT INTO amazon_sku_map (id, store_id, amazon_sku, asin, amazon_product_name, product_id, units_per_sale)
SELECT gen_random_uuid()::text, s.id, 'AV-HC', 'B00NJRECG4',
       'Avon Haiku Skin Softener', p.id, 1
FROM stores s
LEFT JOIN products p ON p.store_id = s.id
    AND p.name ILIKE '%Haiku%' AND p.name ILIKE '%Softener%'
WHERE s.name = 'Rodmat'
ON CONFLICT (store_id, amazon_sku) DO NOTHING;

-- SF-MPR24/12.M: Stayfree x12 (uses new product sku lookup)
INSERT INTO amazon_sku_map (id, store_id, amazon_sku, asin, amazon_product_name, product_id, units_per_sale)
SELECT gen_random_uuid()::text, s.id, 'SF-MPR24/12.M', 'B01IAI2XH6',
       'Stayfree Regular Maxi Pad 24ct Pack of 12', p.id, 12
FROM stores s
LEFT JOIN products p ON p.store_id = s.id AND p.sku = 'STAYFREE-MAXI-24-12'
WHERE s.name = 'Rodmat'
ON CONFLICT (store_id, amazon_sku) DO NOTHING;

-- AV-CC: Candid Skin Softener x1
INSERT INTO amazon_sku_map (id, store_id, amazon_sku, asin, amazon_product_name, product_id, units_per_sale)
SELECT gen_random_uuid()::text, s.id, 'AV-CC', 'B0055802L4',
       'Avon Candid Skin Softener', p.id, 1
FROM stores s
LEFT JOIN products p ON p.store_id = s.id AND p.name ILIKE '%Candid%'
WHERE s.name = 'Rodmat'
ON CONFLICT (store_id, amazon_sku) DO NOTHING;

-- AV-IC: Imari Skin Softener x1
INSERT INTO amazon_sku_map (id, store_id, amazon_sku, asin, amazon_product_name, product_id, units_per_sale)
SELECT gen_random_uuid()::text, s.id, 'AV-IC', 'B006FB8TCY',
       'Avon Imari Skin Softener', p.id, 1
FROM stores s
LEFT JOIN products p ON p.store_id = s.id
    AND p.name ILIKE '%Imari%' AND p.name ILIKE '%Skin%'
WHERE s.name = 'Rodmat'
ON CONFLICT (store_id, amazon_sku) DO NOTHING;

-- AV-BSS/2: Black Suede x2
INSERT INTO amazon_sku_map (id, store_id, amazon_sku, asin, amazon_product_name, product_id, units_per_sale)
SELECT gen_random_uuid()::text, s.id, 'AV-BSS/2', 'B0FCX5NJLR',
       'Avon Black Suede Eau de Toilette', p.id, 2
FROM stores s
LEFT JOIN products p ON p.store_id = s.id AND p.name ILIKE '%Black Suede%'
WHERE s.name = 'Rodmat'
ON CONFLICT (store_id, amazon_sku) DO NOTHING;

-- TS-HTS8: TRESemmé x1 (uses new product sku lookup)
INSERT INTO amazon_sku_map (id, store_id, amazon_sku, asin, amazon_product_name, product_id, units_per_sale)
SELECT gen_random_uuid()::text, s.id, 'TS-HTS8', 'B00IAJFBTI',
       'TRESemmé Heat Tamer Spray 8oz', p.id, 1
FROM stores s
LEFT JOIN products p ON p.store_id = s.id AND p.sku = 'TRESEMME-HEAT-TAMER-8OZ'
WHERE s.name = 'Rodmat'
ON CONFLICT (store_id, amazon_sku) DO NOTHING;

-- AV-TC: Timeless Classic Cologne x1
INSERT INTO amazon_sku_map (id, store_id, amazon_sku, asin, amazon_product_name, product_id, units_per_sale)
SELECT gen_random_uuid()::text, s.id, 'AV-TC', 'B00DIRJ9P4',
       'Avon Timeless Classic Collection Cologne', p.id, 1
FROM stores s
LEFT JOIN products p ON p.store_id = s.id AND p.name ILIKE '%Timeless%'
WHERE s.name = 'Rodmat'
ON CONFLICT (store_id, amazon_sku) DO NOTHING;

-- AV-SD/12: Sweet Honesty Deodorant x12
INSERT INTO amazon_sku_map (id, store_id, amazon_sku, asin, amazon_product_name, product_id, units_per_sale)
SELECT gen_random_uuid()::text, s.id, 'AV-SD/12', 'B00A012EDK',
       'Avon Sweet Honesty Classic Deodorant roll-on', p.id, 12
FROM stores s
LEFT JOIN products p ON p.store_id = s.id
    AND p.name ILIKE '%Sweet%Honesty%' AND p.name ILIKE '%Deod%'
WHERE s.name = 'Rodmat'
ON CONFLICT (store_id, amazon_sku) DO NOTHING;

-- ============================================================
-- Verification queries — run these separately to check results
-- ============================================================

-- Check amazon_sku_map (product_id NULL = name ILIKE didn't match, fix manually)
-- SELECT m.amazon_sku, m.asin, m.amazon_product_name, m.units_per_sale,
--        p.name AS matched_product
-- FROM amazon_sku_map m
-- LEFT JOIN products p ON p.id = m.product_id
-- WHERE m.store_id = (SELECT id FROM stores WHERE name = 'Rodmat')
-- ORDER BY m.amazon_sku;

-- Check new products
-- SELECT sku, name, category, units_per_box FROM products
-- WHERE store_id = (SELECT id FROM stores WHERE name = 'Rodmat')
-- AND sku IN ('STAYFREE-MAXI-24-12', 'TRESEMME-HEAT-TAMER-8OZ');

-- Check platform column
-- SELECT platform, COUNT(*) FROM sales_orders
-- WHERE store_id = (SELECT id FROM stores WHERE name = 'Rodmat')
-- GROUP BY platform;
