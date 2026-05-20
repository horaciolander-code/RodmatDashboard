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

TABLES = [
    "stores",
    "users",
    "products",
    "combos",
    "combo_items",
    "initial_inventory",
    "incoming_stock",
    "sales_orders",
    "affiliate_sales",
    "import_history",
    "amazon_sku_map",
    "fbt_inventory",
    "report_logs",
    "bank_transactions",
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
