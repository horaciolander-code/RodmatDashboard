"""Convierte el catalogo de fotos de la tienda en el paquete que lee el almacen.

ENTRADA   una carpeta con una subcarpeta por anuncio, llamada "SKU - titulo",
          y dentro la foto. Junto a ellas, `_INDICE_FOTOS.csv` con las
          columnas sku;nombre;product_id;carpeta;estado.

SALIDA    dashboard/fotos/<product_id>.jpg  y  dashboard/fotos/_indice.csv

Por que el nombre del fichero es el product_id y no el SKU: el seller SKU NO
identifica un anuncio. Hay cinco SKU colgando de dos anuncios con contenidos
distintos (AV-BMBW, AV-FGS/3, AV-HP/3, AV-NMS/3, AV-TTAI/2) y uno mas,
AV-FM/2, que ademas vendio con dos titulos distintos. Si la foto se guardara
por SKU, el almacen empaquetaria el pack equivocado en esos casos. El
product_id es unico por anuncio — comprobado sobre los 208.

Las fotos se reescalan a 800 px de lado como mucho: en la pantalla del
almacen se ven a 300 px, y el catalogo original pesa 8,6 MB con imagenes de
hasta 1728 px que no aportan nada y engordan el repositorio.

Necesita Pillow, que NO es dependencia del dashboard: esto se ejecuta a mano
cuando cambia el catalogo, no en produccion.

    python scripts/generar_fotos_almacen.py
    python scripts/generar_fotos_almacen.py --origen "D:\otra\carpeta"
"""
import argparse
import csv
import pathlib
import sys

ORIGEN_POR_DEFECTO = r"C:\Users\horac\Pictures\Atralia UGC\_fotos_tienda"
EXTENSIONES = {".jpg", ".jpeg", ".png", ".webp"}
LADO_MAXIMO = 800
CALIDAD = 82


def leer_indice(origen: pathlib.Path) -> list[dict]:
    fichero = origen / "_INDICE_FOTOS.csv"
    if not fichero.exists():
        sys.exit(f"No encuentro el indice: {fichero}")
    with open(fichero, encoding="utf-8-sig", newline="") as f:
        return [{k: (v or "").strip() for k, v in fila.items()}
                for fila in csv.DictReader(f, delimiter=";")]


def comprobar(filas: list[dict]) -> None:
    """Para antes de escribir nada si el indice no es usable."""
    sin_id = [f for f in filas if not f.get("product_id")]
    if sin_id:
        sys.exit(f"{len(sin_id)} filas sin product_id: no se pueden nombrar.")
    vistos: dict[str, str] = {}
    for f in filas:
        pid = f["product_id"]
        if pid in vistos:
            sys.exit(f"product_id repetido {pid}: '{vistos[pid]}' y '{f['nombre']}'. "
                     "Dos anuncios no pueden compartir id; revisa el indice.")
        vistos[pid] = f["nombre"]


def foto_de(origen: pathlib.Path, fila: dict) -> pathlib.Path | None:
    carpeta = origen / fila["carpeta"]
    if not carpeta.is_dir():
        return None
    fotos = sorted(p for p in carpeta.iterdir()
                   if p.is_file() and p.suffix.lower() in EXTENSIONES)
    return fotos[0] if fotos else None


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--origen", default=ORIGEN_POR_DEFECTO)
    args = p.parse_args()

    from PIL import Image

    origen = pathlib.Path(args.origen)
    destino = pathlib.Path(__file__).resolve().parents[1] / "dashboard" / "fotos"
    destino.mkdir(parents=True, exist_ok=True)

    filas = leer_indice(origen)
    comprobar(filas)
    print(f"Indice: {len(filas)} anuncios")

    escritas, sin_foto, bytes_antes, bytes_despues = 0, [], 0, 0
    generados = set()

    for fila in filas:
        src = foto_de(origen, fila)
        if src is None:
            sin_foto.append(fila)
            continue
        bytes_antes += src.stat().st_size
        salida = destino / f"{fila['product_id']}.jpg"
        with Image.open(src) as im:
            im = im.convert("RGB")
            if max(im.size) > LADO_MAXIMO:
                im.thumbnail((LADO_MAXIMO, LADO_MAXIMO), Image.LANCZOS)
            im.save(salida, "JPEG", quality=CALIDAD, optimize=True)
        bytes_despues += salida.stat().st_size
        generados.add(salida.name)
        escritas += 1

    with open(destino / "_indice.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["product_id", "sku", "nombre"])
        for fila in sorted(filas, key=lambda x: (x["sku"], x["nombre"])):
            if (destino / f"{fila['product_id']}.jpg").exists():
                w.writerow([fila["product_id"], fila["sku"], fila["nombre"]])

    sobran = [p for p in destino.glob("*.jpg") if p.name not in generados]
    for p in sobran:
        p.unlink()

    print(f"Escritas: {escritas} fotos")
    print(f"Peso: {bytes_antes/1e6:.1f} MB -> {bytes_despues/1e6:.1f} MB")
    if sobran:
        print(f"Borradas por no estar ya en el catalogo: {len(sobran)}")
    if sin_foto:
        print(f"\nSIN FOTO ({len(sin_foto)}), el almacen los vera solo con texto:")
        for f in sin_foto:
            print(f"   {f['sku'] or '(sin sku)'}  {f['nombre'][:60]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
