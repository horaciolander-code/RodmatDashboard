"""Parte un CSV de pedidos de TikTok en trozos < N MB, repitiendo la cabecera.

El import del panel (/api/import/orders) hace UPSERT por (store_id, order_id, sku),
asi que subir el fichero en varios trozos equivale a subirlo entero.

Uso:  python split_orders_csv.py <fichero.csv> <carpeta_salida> [max_mb]
Salida (stdout): una linea por trozo generado con su ruta absoluta.
"""
import csv
import os
import sys

csv.field_size_limit(10 * 1024 * 1024)


def detect_sep(path: str) -> str:
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        first = fh.readline()
    return max([",", ";", "\t"], key=first.count)


def main() -> int:
    src = sys.argv[1]
    outdir = sys.argv[2]
    max_bytes = int(float(sys.argv[3]) * 1024 * 1024) if len(sys.argv) > 3 else 8 * 1024 * 1024

    os.makedirs(outdir, exist_ok=True)
    sep = detect_sep(src)
    base = os.path.splitext(os.path.basename(src))[0]

    parts, part_no, rows_total = [], 0, 0
    with open(src, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh, delimiter=sep)
        header = next(reader)

        out = None
        writer = None
        written = 0

        def open_part():
            nonlocal out, writer, written, part_no
            part_no += 1
            path = os.path.join(outdir, f"{base}__part{part_no}.csv")
            out = open(path, "w", encoding="utf-8", newline="")
            writer = csv.writer(out, delimiter=sep, lineterminator="\r\n")
            writer.writerow(header)
            out.flush()
            written = out.tell()
            parts.append(path)

        open_part()
        for row in reader:
            rows_total += 1
            writer.writerow(row)
            written = out.tell()
            if written >= max_bytes:
                out.close()
                open_part()
        out.close()

    # Si el ultimo trozo quedo vacio (solo cabecera), lo eliminamos
    if len(parts) > 1 and sum(1 for _ in open(parts[-1], encoding="utf-8")) <= 1:
        os.remove(parts[-1])
        parts.pop()

    for p in parts:
        print(p)
    print(f"# filas={rows_total} trozos={len(parts)} sep={sep!r}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
