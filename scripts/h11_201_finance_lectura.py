"""
H11 + #201 — análisis de SOLO LECTURA contra producción (Supabase Data API, solo GET).

Credenciales: SOLO desde el entorno, nunca desde fichero (regla 5).
    PowerShell:  $env:SUPABASE_URL='https://swrjjyumuqgihjuomgte.supabase.co'; $env:SUPABASE_SECRET='<clave ROTADA>'
    python scripts/h11_201_finance_lectura.py

Parte A · H11 — ¿cuánto ha destruido ya el guardado de Finance?
  finance_service.replace_custom_lines borra TODO el mes (store_id + year_month, sin brand_id)
  y reinserta sin brand_id. No hay papelera ni log de borrados, así que el daño se mide por huella:
    · created_at: cada guardado recrea el mes entero → todas sus líneas llevan la fecha del guardado.
      Un mes ene-jun con created_at posterior al 11-jul = mes re-guardado (evento de riesgo).
    · Comparación contra dos líneas base documentadas:
        - carga del CSV bancario 11-jul (PROJECT_MEMORY.md §2026-07-11): totales por mes
        - foto del 21-sep (ESTADO_ATRALIA_LUXPERFUMES.md §4): 77 líneas, 76 sin marca + 1 LuxPerfumes
          (delivery LXASO105730, −$395), última en 2026-07
    · La línea de Atralia es el chivato: es la ÚNICA con marca. Si un mes que la contenía se guardó
      con Avon/Todas en pantalla, desaparece; con LuxPerfumes en pantalla, sobrevive pero sin marca.

Parte B · #201 — hueco de COGS entre el P&L de Finance y el cálculo por combos del statement.
  Las finance_custom_lines NO entran en ninguna de las dos fórmulas de COGS (verificado en código),
  así que H11 no puede explicar el hueco. Se descompone en:
    F  = fórmula P&L (finance_service._cogs_for_platform) · universo = sales_orders por order_date
    X  = fórmula P&L aplicada al universo del statement (pedidos de tiktok_statement_lines por order_paid_date)
    S  = fórmula statement (api/tiktok_statements.py /kpis cogs_real) · mismo universo que X
    X − F = efecto UNIVERSO (fecha de pedido vs fecha de pago, pedidos aún sin statement)
    S − X = efecto FÓRMULA (doble conteo SKU en products y combos · SKU sin match por sku pero sí por nombre)
"""
import collections
import json
import os
import sys
import urllib.parse
import urllib.request

STORE = "13c02ce8-7761-43b7-9525-fb5aad6f0a09"  # Rodmat
YEAR = "2026"
LOAD_11JUL = "2026-07-11"
BASE_11JUL = {  # "Custom" de la tabla P&L del 11-jul (gastos, redondeado a $)
    "2026-01": -10120, "2026-02": -13581, "2026-03": -15792, "2026-04": -11098,
    "2026-05": -14779, "2026-06": -11829, "2026-07": -10203,
}
# Ojo: esa tabla suma −87.402 y el texto de la misma carga dice −82.609,02 (71 filas).
# La propia línea base trae 4.793 $ de incoherencia; se reporta, no se corrige.

URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
KEY = os.environ.get("SUPABASE_SECRET", "")
if not URL or not KEY:
    sys.exit("Faltan SUPABASE_URL / SUPABASE_SECRET en el entorno. No se leen de ningún fichero.")


def get(table, params):
    """GET paginado. Nunca escribe."""
    out, off = [], 0
    qs = urllib.parse.urlencode(params, safe="*,.()")
    while True:
        req = urllib.request.Request(
            f"{URL}/rest/v1/{table}?{qs}",
            headers={"apikey": KEY, "Authorization": f"Bearer {KEY}", "Range": f"{off}-{off + 999}"},
            method="GET",
        )
        page = json.load(urllib.request.urlopen(req))
        out += page
        if len(page) < 1000:
            return out
        off += 1000


def money(x):
    return f"{x:,.2f}"


def not_cancel(status):
    # SQL: status NOT ILIKE '%cancel%' → NULL también queda fuera
    return status is not None and "cancel" not in status.lower()


brands = {b["id"]: b["slug"] for b in get("brands", {"store_id": f"eq.{STORE}", "select": "id,slug"})}
slug = lambda bid: brands.get(bid, "SIN MARCA") if bid else "SIN MARCA"

# ───────────────────────────── PARTE A · H11 ─────────────────────────────
print("=" * 78, "\nPARTE A · H11 — daño del guardado de Finance\n" + "=" * 78)
lines = get("finance_custom_lines", {
    "store_id": f"eq.{STORE}",
    "select": "id,year_month,description,amount,brand_id,created_at,updated_at",
})
other = get("finance_custom_lines", {"store_id": f"neq.{STORE}", "select": "store_id"})
print(f"Líneas Rodmat: {len(lines)}  (21-sep eran 77)   · líneas de otros stores: {len(other)} (esperado 0)")
print("Por marca:", dict(collections.Counter(slug(x["brand_id"]) for x in lines)),
      " (21-sep: SIN MARCA 76 · luxperfumes 1)")

by_m = collections.defaultdict(list)
for x in lines:
    by_m[x["year_month"]].append(x)
print(f"\n{'mes':8} {'n':>3} {'suma':>12} {'base 11-jul':>12} {'dif':>10}  marcas / fechas de creación")
for ym in sorted(by_m):
    r = by_m[ym]
    s = sum(float(x["amount"]) for x in r)
    base = BASE_11JUL.get(ym)
    created = sorted({(x["created_at"] or "")[:10] for x in r})
    resaved = ym < "2026-07" and all((x["created_at"] or "") > LOAD_11JUL + "T23:59" for x in r)
    marcas = dict(collections.Counter(slug(x["brand_id"]) for x in r))
    print(f"{ym:8} {len(r):>3} {money(s):>12} {money(base) if base is not None else '—':>12} "
          f"{money(s - base) if base is not None else '—':>10}  {marcas} {created}"
          + ("  ⚠ RE-GUARDADO tras la carga" if resaved else ""))
missing = [m for m in BASE_11JUL if m not in by_m]
print("Meses de la base que ya NO tienen líneas:", missing or "ninguno")

atr = [x for x in lines if "LXASO105730" in (x["description"] or "") or abs(float(x["amount"]) + 395) < 0.01]
print("\nChivato Atralia (LXASO105730, −$395):")
if not atr:
    print("  🔴 NO EXISTE → borrado por un guardado. Daño de H11 confirmado.")
for x in atr:
    print(f"  {x['year_month']} · {x['description']} · {x['amount']} · marca={slug(x['brand_id'])} · creada {x['created_at']}"
          + ("  🟠 sobrevivió SIN marca (guardado con LuxPerfumes en pantalla)" if not x["brand_id"] else "  ✅ intacta"))

events = collections.Counter((x["created_at"] or "")[:16] for x in lines)
print("\nEventos de escritura (created_at a minuto · nº líneas). Cada uno es un guardado o una carga:")
for ts, n in sorted(events.items()):
    print(f"  {ts}  {n:>3}")
print("""
Qué espero ver (hipótesis, a confirmar):
  · Daño HISTÓRICO pequeño: hasta el 21-sep solo 1 de 77 líneas tenía marca, y con cualquier marca en
    pantalla se ven también las sin marca → re-guardar un mes las recrea todas. Lo único destruible
    era la línea de Atralia. Si existe con marca → no ha habido pérdida; si no existe → la hubo.
  · El riesgo de verdad es FUTURO: en cuanto se carguen ago-sep por marca, cada guardado borra la otra.
  · Si algún mes ene-jun difiere de la base sin evento de escritura posterior → no es H11, es otra cosa.
  · Fuente adicional de quién/cuándo guardó: logs HTTP de Railway (PUT /api/finance/custom-lines).
""")

# ───────────────────────────── PARTE B · #201 ─────────────────────────────
print("=" * 78, "\nPARTE B · #201 — hueco de COGS Finance vs cálculo por combos\n" + "=" * 78)
products = get("products", {"store_id": f"eq.{STORE}", "select": "id,sku,name,price_cost,brand_id"})
cost = {p["id"]: float(p["price_cost"] or 0) for p in products}
by_sku = {}
for p in products:
    if p["sku"]:
        by_sku.setdefault(p["sku"], []).append(p["id"])  # JOIN: si hay duplicados, suma todos
by_name = {}
for p in products:
    by_name[(p["name"] or "").strip().lower()] = p["id"]  # igual que el dict de Python: gana el último
combos = get("combos", {"store_id": f"eq.{STORE}", "select": "id,combo_sku"})
items = get("combo_items", {"combo_id": "in.(" + ",".join(c["id"] for c in combos) + ")",
                            "select": "combo_id,product_id,quantity"}) if combos else []
cid2sku = {c["id"]: c["combo_sku"] for c in combos}
combo_map = collections.defaultdict(list)
for it in items:
    combo_map[cid2sku[it["combo_id"]]].append((it["product_id"], int(it["quantity"])))
amz = {a["amazon_sku"]: a["product_id"] for a in get("amazon_sku_map", {"store_id": f"eq.{STORE}",
                                                                        "select": "amazon_sku,product_id"})}

so = get("sales_orders", {"store_id": f"eq.{STORE}", "platform": "eq.tiktok",
                          "order_date": f"gte.{YEAR}-01-01",
                          "select": "tiktok_order_id,seller_sku,product_name,quantity,status,order_date,brand_id"})
st = get("tiktok_statement_lines", {"store_id": f"eq.{STORE}", "select": "order_id,order_paid_date,brand_id"})


def cogs_f(r):
    """finance_service._cogs_for_platform, línea a línea → (importe, vía)."""
    sku = (r["seller_sku"] or "").strip()
    q = int(r["quantity"] or 0)
    if sku in combo_map:
        return q * sum(qpc * cost.get(pid, 0) for pid, qpc in combo_map[sku]), "combo"
    if sku in amz:
        return q * cost.get(amz[sku], 0), "amazon_map"
    pid = by_name.get((r["product_name"] or "").strip().lower())
    return (q * cost[pid], "nombre") if pid else (0.0, "SIN MATCH")


def cogs_s(r):
    """tiktok_statements /kpis cogs_real (cogs_direct + cogs_combo, SIN dedupe)."""
    sku = r["seller_sku"]
    q = r["quantity"] or 0
    d = sum(q * cost.get(pid, 0) for pid in by_sku.get(sku, []))
    c = q * sum(qpc * cost.get(pid, 0) for pid, qpc in combo_map.get(sku, []))
    return d + c


lines_by_order = collections.defaultdict(list)
for r in so:
    if not_cancel(r["status"]):
        lines_by_order[r["tiktok_order_id"]].append(r)

paid_month = {}
st_brands = collections.defaultdict(set)
for x in st:
    if x["order_paid_date"] and x["order_paid_date"].startswith(YEAR):
        paid_month.setdefault(x["order_id"], x["order_paid_date"][:7])
    st_brands[x["order_id"]].add(x["brand_id"])

for brand_label, bid in [("TODAS", None)] + [(s, b) for b, s in brands.items()]:
    F = collections.Counter()
    X = collections.Counter()
    S = collections.Counter()
    via = collections.Counter()
    for r in so:
        if not not_cancel(r["status"]) or (bid and r["brand_id"] != bid):
            continue
        v, how = cogs_f(r)
        F[r["order_date"][:7]] += v
        via[how] += 1
    for oid, m in paid_month.items():
        if bid and bid not in st_brands[oid]:
            continue
        for r in lines_by_order.get(oid, []):
            S[m] += cogs_s(r)  # el statement NO filtra la línea por marca → arrastra la otra marca
            if not bid or r["brand_id"] == bid:
                X[m] += cogs_f(r)[0]
    print(f"\n── {brand_label} ──  {'mes':7} {'F (P&L)':>11} {'X':>11} {'S (stmt)':>11} {'X−F univ.':>11} {'S−X fórm.':>11}")
    for m in sorted(set(F) | set(S)):
        print(f"{'':16}{m:7} {money(F[m]):>11} {money(X[m]):>11} {money(S[m]):>11} "
              f"{money(X[m] - F[m]):>11} {money(S[m] - X[m]):>11}")
    tf, tx, ts = sum(F.values()), sum(X.values()), sum(S.values())
    print(f"{'':16}{'TOTAL':7} {money(tf):>11} {money(tx):>11} {money(ts):>11} {money(tx - tf):>11} {money(ts - tx):>11}")
    print(f"{'':16}vías fórmula P&L (líneas): {dict(via)}")

dup = sorted(s for s in combo_map if s in by_sku)
print("\nSKU que están en products.sku Y en combos (el statement los cuenta DOS veces):", dup or "ninguno")
nomatch = collections.Counter()
nameonly = collections.Counter()
for r in so:
    if not not_cancel(r["status"]):
        continue
    sku = r["seller_sku"]
    v, how = cogs_f(r)
    if how == "SIN MATCH":
        nomatch[sku] += int(r["quantity"] or 0)
    if how == "nombre" and sku not in by_sku:
        nameonly[sku] += v
print("SKU sin coste en NINGUNA fórmula (uds):", dict(nomatch.most_common(15)) or "ninguno")
print("SKU que solo casan por NOMBRE (cuenta el P&L, no el statement) ($):",
      {k: round(v, 2) for k, v in nameonly.most_common(15)} or "ninguno")
mixed = [o for o, ls in lines_by_order.items() if len({l["brand_id"] for l in ls}) > 1]
print(f"Pedidos mixtos Avon+Lux (el statement por marca arrastra el COGS de la otra): {len(mixed)}")
print("""
Lectura del resultado:
  · Si el hueco de $145 aparece en 'X−F' → es de calendario (fecha de pedido vs fecha de pago). No es error:
    son dos preguntas distintas y hay que etiquetarlas.
  · Si aparece en 'S−X' → es de fórmula: doble conteo de la lista 'dup' y/o SKU que solo casan por nombre.
  · En la vista por marca, 'S' incluye además el COGS de la otra marca en pedidos mixtos.
""")
