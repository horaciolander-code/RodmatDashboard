# BRANDS multi-brand rollout — LuxPerfumes

**Rama:** `feature/brands`
**Objetivo:** habilitar múltiples marcas dentro del store Rodmat (Avon + LuxPerfumes que agrupa Atralia/Lattafa/futuras).

## Cambios de esquema (F1)
Ver `migrations/2026_08_07_brands_multibrand.sql`. Todo aditivo (ADD COLUMN / CREATE TABLE), backward-compatible. Feature-flag `stores.brands_enabled` OFF por defecto — Nokal intacta.

## Cambios de código
- **Backend**
  - `models/brand.py` — nueva entidad Brand
  - `models/store.py` — campo `brands_enabled`
  - `models/user.py` — campo `brand_id` (nullable = admin ve todo)
  - `schemas/user.py::UserResponse` — expone brand info + available_brands
  - `api/auth.py::get_me` — enriquece con brand info del user + brands disponibles del store
  - `api/brands.py` — nuevos endpoints `GET /api/brands`, `GET /api/brands/product-map`
  - `main.py` — registra brands router
- **Dashboard Streamlit**
  - `dashboard/app.py` — selector marca en sidebar (bloqueado si user tiene brand_id, dropdown si admin), cache marker `email:store:brand`, helpers `fetch_brands`, `fetch_product_brand_map`, `get_current_brand_slug`, `filter_df_by_brand`.

## Rollout prod (día D)
1. Aplicar `migrations/2026_08_07_brands_multibrand.sql` en prod
2. Seed brands Rodmat: avon + luxperfumes (ver script F2)
3. UPDATE products SET brand_id=(avon) WHERE store_id=Rodmat AND brand_id IS NULL
4. INSERT productos LuxPerfumes iniciales (11 SKUs pedido Lux America LXASO105730)
5. INSERT incoming_stock inicial + delivery finance line
6. UPDATE stores SET brands_enabled=TRUE WHERE id=Rodmat
7. Merge PR feature/brands
8. Alta user socia machadoyb@gmail.com con brand_id=luxperfumes, role=viewer

## Postpone next week (no bloquea go-live)
- F5: agentes IA brand-aware (prism/haiku/faraway/mesmerize/timeless por brand)
- F6: P&L per brand + overhead_allocation_rules
- ETL brand-aware (auto-tagging brand_id en sales_orders al importar — hoy queda deducible por lookup SKU→product.brand_id)
