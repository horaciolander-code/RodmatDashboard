"""Que foto corresponde a una linea de pedido. Sin adivinar.

El almacen empaqueta mirando la foto. Una foto equivocada es un paquete
equivocado, asi que aqui una respuesta dudosa NO es una foto: es un aviso
para que la persona lo mire.

Tres formas de identificar un anuncio, de mas fiable a menos:

  product_id   el identificador del anuncio en la plataforma. Exacto.
               Cinco seller SKU del catalogo cuelgan de DOS anuncios con
               contenidos distintos (AV-BMBW, AV-FGS/3, AV-HP/3, AV-NMS/3,
               AV-TTAI/2) y solo esto los distingue.

  nombre       el titulo del anuncio. Unico en el catalogo: 208 titulos
               para 208 anuncios, sin repetidos.

  seller SKU   NO identifica un anuncio. Se acepta solo si ademas el titulo
               se parece al del catalogo. Sin esa condicion, las ventas de
               AV-FM/2 reciben la foto de Far Away Moroccan Nights cuando
               en realidad son Far Away Original + Mesmerize Blue.

El indice se genero del catalogo de TikTok, y en Temu el MISMO pack se
anuncia con otro titulo. Por eso hay un segundo fichero, `_alias.csv`: otro
titulo para un anuncio que ya esta, con su foto, comprobado pieza a pieza.
Va aparte de `_indice.csv` porque el indice lo reescribe entero
`scripts/generar_fotos_almacen.py` cada vez que cambia el catalogo, y una
linea anadida a mano ahi se perderia en silencio. Un alias NUNCA tapa un
titulo que ya existe: si chocara, se descarta y el anuncio sigue sin foto.

Esto vale para una LINEA de pedido, que es un anuncio. Las PIEZAS de dentro
de un pack NO llevan foto, a proposito: de muchos productos solo existe la
foto del set en que se venden, y ensenar dos frascos donde hay que meter uno
es peor que no ensenar nada. Ver `pagina_almacen._pintar_pieza`.

Modulo puro a proposito: sin pandas, sin sqlalchemy, sin red. Asi corre en
el job de CI de aislamiento, donde solo hay pytest.
"""
import csv
import pathlib
import re
import unicodedata

CARPETA = pathlib.Path(__file__).resolve().parent / "fotos"

# Cuantas de las palabras del titulo mas corto tienen que aparecer en el otro
# para aceptar una coincidencia por seller SKU. Medido sobre 18.403 lineas de
# pedido reales: a 0,70 entran las 5.843 que son erratas de titulo (una coma,
# un espacio de mas, "permume" por "perfume", el titulo recortado) y se queda
# fuera el unico caso de verdad distinto.
PARECIDO_MINIMO = 0.70

SEGURA = "segura"
DUDOSA = "dudosa"
NINGUNA = "ninguna"


def normalizar(texto) -> str:
    """Minusculas, sin acentos, sin puntuacion, un espacio entre palabras."""
    s = unicodedata.normalize("NFKD", str(texto or ""))
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"[^a-z0-9]+", " ", s.lower())
    return " ".join(s.split())


def parecido(a: str, b: str) -> float:
    """Que parte de las palabras del titulo mas corto esta en el otro.

    No es distancia de edicion: aqui lo que cambia entre dos titulos del
    mismo anuncio son palabras enteras que se anaden o se quitan al recortar,
    no letras sueltas. Se cuenta por palabras repetidas, no por conjunto,
    para que un titulo que repite "far away" no puntue de mas.
    """
    pa, pb = normalizar(a).split(), normalizar(b).split()
    if not pa or not pb:
        return 0.0
    corto, largo = (pa, pb) if len(pa) <= len(pb) else (pb, pa)
    restantes = list(largo)
    comunes = 0
    for palabra in corto:
        if palabra in restantes:
            restantes.remove(palabra)
            comunes += 1
    return comunes / len(corto)


class Catalogo:
    """El indice de fotos, ya ordenado para buscar."""

    def __init__(self, filas):
        self.por_id, self.por_nombre, self.por_sku = {}, {}, {}
        for f in filas:
            pid = str(f.get("product_id") or "").strip()
            sku = str(f.get("sku") or "").strip()
            nombre = str(f.get("nombre") or "").strip()
            if not pid:
                continue
            entrada = {"product_id": pid, "sku": sku, "nombre": nombre,
                       "foto": f"{pid}.jpg"}
            self.por_id[pid] = entrada
            if nombre:
                self.por_nombre.setdefault(normalizar(nombre), []).append(entrada)
            if sku:
                self.por_sku.setdefault(normalizar(sku), []).append(entrada)

    def __len__(self):
        return len(self.por_id)

    def anadir_alias(self, product_id, titulo) -> str:
        """Otro titulo para un anuncio que ya esta. Devuelve el motivo del
        rechazo, o cadena vacia si se acepto.

        Se rechaza si el anuncio no esta en el indice (la foto no existiria)
        o si ese titulo ya esta cogido por otro anuncio: un alias que pisa un
        titulo bueno convierte en dudosas las ventas que hoy salen seguras.
        """
        pid = str(product_id or "").strip()
        clave = normalizar(titulo)
        if not pid or pid not in self.por_id:
            return f"el anuncio {pid or '(vacio)'} no esta en el indice"
        if not clave:
            return "alias sin titulo"
        ya = self.por_nombre.get(clave, [])
        if ya:
            if all(e["product_id"] == pid for e in ya):
                return ""
            return f"el titulo ya es de otro anuncio ({ya[0]['product_id']})"
        self.por_nombre[clave] = [self.por_id[pid]]
        return ""

    def resolver(self, product_id=None, nombre=None, seller_sku=None) -> dict:
        """Devuelve {foto, certeza, motivo, alternativas}.

        `foto` solo viene con valor cuando la certeza es 'segura'. Con
        'dudosa' se devuelven las alternativas para que decida la persona.
        """
        pid = str(product_id or "").strip()
        if pid and pid in self.por_id:
            e = self.por_id[pid]
            return {"foto": e["foto"], "certeza": SEGURA,
                    "motivo": "id del anuncio", "alternativas": []}

        iguales = self.por_nombre.get(normalizar(nombre), [])
        if len(iguales) == 1:
            return {"foto": iguales[0]["foto"], "certeza": SEGURA,
                    "motivo": "titulo del anuncio", "alternativas": []}
        if len(iguales) > 1:
            return {"foto": None, "certeza": DUDOSA,
                    "motivo": "dos anuncios con el mismo titulo",
                    "alternativas": iguales}

        candidatos = self.por_sku.get(normalizar(seller_sku), [])
        if not candidatos:
            return {"foto": None, "certeza": NINGUNA,
                    "motivo": "no esta en el catalogo de fotos",
                    "alternativas": []}
        if len(candidatos) > 1:
            return {"foto": None, "certeza": DUDOSA,
                    "motivo": f"el SKU {seller_sku} tiene {len(candidatos)} anuncios",
                    "alternativas": candidatos}

        unico = candidatos[0]
        if parecido(unico["nombre"], nombre) >= PARECIDO_MINIMO:
            return {"foto": unico["foto"], "certeza": SEGURA,
                    "motivo": "seller SKU y titulo parecido", "alternativas": []}
        return {"foto": None, "certeza": DUDOSA,
                "motivo": f"el SKU {seller_sku} apunta a otro anuncio",
                "alternativas": [unico]}


def _leer(fichero) -> list:
    if not fichero.exists():
        return []
    with open(fichero, encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f, delimiter=";"))


def alias_rechazados(catalogo: Catalogo, filas) -> list:
    """Los alias que no se pudieron aplicar, con su motivo.

    Separado de `cargar` para que el test pueda exigir que el fichero
    publicado no tenga ninguno. En el almacen un alias malo se ignora y ya:
    el pedido se tiene que poder empaquetar igual, sin foto si hace falta.
    """
    malos = []
    for fila in filas:
        motivo = catalogo.anadir_alias(fila.get("product_id"),
                                       fila.get("titulo"))
        if motivo:
            malos.append({"titulo": (fila.get("titulo") or "")[:60],
                          "motivo": motivo})
    return malos


def cargar(carpeta=None) -> Catalogo:
    """Lee dashboard/fotos/_indice.csv y, si esta, _alias.csv."""
    carpeta = pathlib.Path(carpeta) if carpeta else CARPETA
    catalogo = Catalogo(_leer(carpeta / "_indice.csv"))
    alias_rechazados(catalogo, _leer(carpeta / "_alias.csv"))
    return catalogo
