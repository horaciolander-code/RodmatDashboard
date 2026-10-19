"""
Enable Row Level Security on all Supabase public tables.

Safe to run multiple times (ALTER TABLE ... ENABLE RLS is idempotent).
The FastAPI backend connects as the postgres/service role which bypasses RLS,
so enabling this does NOT affect backend behaviour. It only protects the
PostgREST (anon) endpoint from unauthenticated direct access.

Usage:
    railway run python scripts/enable_rls.py
    # or locally with DATABASE_URL set
"""

import os
import sys

import psycopg2

# La lista se quedo en 14 tablas mientras la aplicacion crecia a 19.
# Quedaban sin RLS brands, walmart_sku_map, agent_runs,
# finance_custom_lines y las dos de statements de TikTok: datos de
# dinero expuestos al endpoint anonimo de PostgREST.
#
# Al final el script pregunta a la BASE que tablas hay y avisa de las
# que falten aqui, para que no se vuelva a quedar atras en silencio.
TABLES = [
    "affiliate_sales",
    "agent_runs",
    "amazon_sku_map",
    "brands",
    "combo_items",
    "combos",
    "fbt_inventory",
    "finance_custom_lines",
    "import_history",
    "incoming_stock",
    "initial_inventory",
    "products",
    "report_logs",
    "sales_orders",
    "stores",
    "tiktok_statement_lines",
    "tiktok_statements",
    "users",
    "walmart_sku_map",
]

DB_URL = os.environ.get("DATABASE_URL")
if not DB_URL:
    print("ERROR: DATABASE_URL env var not set.")
    sys.exit(1)

conn = psycopg2.connect(DB_URL)
conn.autocommit = True
cur = conn.cursor()

print("Enabling Row Level Security on all public tables...\n")
ok = 0
for table in TABLES:
    try:
        cur.execute(f"ALTER TABLE public.{table} ENABLE ROW LEVEL SECURITY;")
        print(f"  OK  {table}")
        ok += 1
    except Exception as e:
        print(f"  FAIL {table}: {e}")

cur.close()
conn.close()
print(f"\nDone: {ok}/{len(TABLES)} tables with RLS enabled.")
print("Note: no POLICY created — the postgres role bypasses RLS automatically.")

# Lo que de verdad evita que esto se quede atras otra vez: preguntar a la
# base que tablas tiene y comparar. Una tabla nueva sin RLS queda expuesta
# al endpoint anonimo de PostgREST y nadie se entera.
conn2 = psycopg2.connect(DB_URL)
cur2 = conn2.cursor()
cur2.execute("""
    SELECT c.relname, c.relrowsecurity
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public' AND c.relkind = 'r'
    ORDER BY c.relname
""")
reales = cur2.fetchall()
cur2.close()
conn2.close()

fuera = [t for t, _ in reales if t not in TABLES]
sin_rls = [t for t, rls in reales if not rls]
print(f"\nTablas en la base: {len(reales)}")
if fuera:
    print(f"  AVISO: {len(fuera)} no estan en la lista de este script: {', '.join(fuera)}")
if sin_rls:
    print(f"  AVISO: {len(sin_rls)} siguen SIN RLS: {', '.join(sin_rls)}")
    sys.exit(1)
print("  Todas con RLS.")
