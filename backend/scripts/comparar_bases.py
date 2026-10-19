"""Compara dos bases antes de dar por buena una migracion. SOLO LECTURAS.

Es el paso que evita el fallo caro de una mudanza: que el restore se deje
una constraint o unas filas y no se note hasta la siguiente carga diaria,
cuando el importador empieza a duplicar en silencio.

    set ORIGEN_URL=postgresql://...   (la base vieja)
    set DESTINO_URL=postgresql://...  (la nueva)
    python backend/scripts/comparar_bases.py

Las dos cadenas se leen del entorno en el momento de usarlas y no se
escriben a disco, ni se imprimen, ni se pasan por linea de comandos
(regla 5). El script no modifica NADA en ninguna de las dos.

Sale con codigo 1 si encuentra cualquier diferencia, para poder encadenarlo.
"""
import os
import sys

import psycopg2

# Sin estas dos, el UPSERT de los importadores deja de deduplicar y empieza
# a duplicar pedidos en cada carga. No se nota hasta el dia siguiente.
CONSTRAINTS_CRITICAS = (
    "uq_store_order_sku",
    "uq_store_affiliate_order_sku",
    "uq_combo_product",
)


def _conn(var: str):
    url = os.environ.get(var)
    if not url:
        print(f"ERROR: falta la variable de entorno {var}")
        sys.exit(2)
    return psycopg2.connect(url)


def _consulta(cur, sql, params=None):
    cur.execute(sql, params or ())
    return cur.fetchall()


def tablas_y_filas(cur) -> dict:
    """Cuenta exacta por tabla. No se usa reltuples: es una estimacion del
    planificador y puede estar muy lejos justo despues de un restore."""
    nombres = [r[0] for r in _consulta(cur, """
        SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relkind = 'r' ORDER BY c.relname
    """)]
    out = {}
    for t in nombres:
        out[t] = _consulta(cur, f'SELECT COUNT(*) FROM public."{t}"')[0][0]
    return out


def constraints(cur) -> set:
    return {r[0] for r in _consulta(cur, """
        SELECT conname FROM pg_constraint c
        JOIN pg_namespace n ON n.oid = c.connamespace
        WHERE n.nspname = 'public'
    """)}


def indices(cur) -> set:
    return {r[0] for r in _consulta(cur, """
        SELECT indexname FROM pg_indexes WHERE schemaname = 'public'
    """)}


def secuencias(cur) -> dict:
    """Una secuencia que vuelve a empezar en 1 provoca claves duplicadas en
    la primera insercion. Es un fallo clasico de restore."""
    out = {}
    for (s,) in _consulta(cur, """
        SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relkind = 'S' ORDER BY c.relname
    """):
        try:
            out[s] = _consulta(cur, f'SELECT last_value FROM public."{s}"')[0][0]
        except Exception:
            out[s] = None
    return out


def rls(cur) -> dict:
    return {r[0]: r[1] for r in _consulta(cur, """
        SELECT c.relname, c.relrowsecurity
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relkind = 'r'
    """)}


def main() -> int:
    o, d = _conn("ORIGEN_URL"), _conn("DESTINO_URL")
    co, cd = o.cursor(), d.cursor()
    fallos = 0

    print("=" * 62)
    print("FILAS POR TABLA")
    print("=" * 62)
    fo, fd = tablas_y_filas(co), tablas_y_filas(cd)
    for t in sorted(set(fo) | set(fd)):
        a, b = fo.get(t), fd.get(t)
        if a == b:
            print(f"  OK    {t:<28} {a:>9,}")
        else:
            fallos += 1
            av = "(no existe)" if a is None else f"{a:,}"
            bv = "(no existe)" if b is None else f"{b:,}"
            print(f"  FALLA {t:<28} origen {av} != destino {bv}")

    print()
    print("=" * 62)
    print("CONSTRAINTS")
    print("=" * 62)
    cons_o, cons_d = constraints(co), constraints(cd)
    for c in sorted(CONSTRAINTS_CRITICAS):
        if c in cons_d:
            print(f"  OK    {c}")
        elif c not in cons_o:
            print(f"  --    {c} (tampoco esta en el origen)")
        else:
            fallos += 1
            print(f"  FALLA {c} NO esta en el destino. El UPSERT dejara de "
                  f"deduplicar y los importadores empezaran a duplicar.")
    faltan = cons_o - cons_d
    if faltan:
        fallos += 1
        print(f"  FALLA faltan {len(faltan)} constraints mas: "
              f"{', '.join(sorted(faltan)[:12])}")

    print()
    print("=" * 62)
    print("INDICES Y SECUENCIAS")
    print("=" * 62)
    fi = indices(co) - indices(cd)
    if fi:
        fallos += 1
        print(f"  FALLA faltan {len(fi)} indices: {', '.join(sorted(fi)[:12])}")
    else:
        print("  OK    todos los indices del origen estan en el destino")

    so, sd = secuencias(co), secuencias(cd)
    malas = [s for s in so if sd.get(s) is not None and so[s] is not None
             and sd[s] < so[s]]
    if malas:
        fallos += 1
        print(f"  FALLA {len(malas)} secuencias por detras del origen: "
              f"{', '.join(malas[:8])}")
    else:
        print(f"  OK    {len(so)} secuencias al dia")

    print()
    print("=" * 62)
    print("RLS EN EL DESTINO")
    print("=" * 62)
    sin = [t for t, on in rls(cd).items() if not on]
    if sin:
        fallos += 1
        print(f"  FALLA {len(sin)} tablas sin RLS: {', '.join(sorted(sin))}")
        print("        pasa backend/scripts/enable_rls.py contra el destino")
    else:
        print("  OK    todas las tablas con RLS")

    for c in (co, cd):
        c.close()
    for c in (o, d):
        c.close()

    print()
    print("=" * 62)
    if fallos:
        print(f"{fallos} PROBLEMA(S). NO cambies DATABASE_URL todavia.")
        return 1
    print("Todo cuadra. El destino es apto para recibir el trafico.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
