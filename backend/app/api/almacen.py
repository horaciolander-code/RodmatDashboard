"""Lo que el almacen necesita para empaquetar. Nada mas.

Quien entra aqui prepara cajas, no lleva la tienda. Por eso este router no
devuelve dinero en ningun campo: ni coste, ni precio, ni valor de inventario,
ni comisiones. No es una cuestion de que el dashboard no lo pinte — es que no
sale del backend, y `backend/tests/test_almacen_sin_dinero.py` lo comprueba
sobre el codigo.

Las columnas que SI salen van en una lista blanca (COLUMNAS_DE_STOCK). Lista
blanca y no lista negra a proposito: si manana el calculo de stock gana una
columna de dinero, aqui no aparece sola.
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.database import get_db
from app.dependencies import require_almacen
from app.models.sales import SalesOrder
from app.models.product import Product
from app.models.user import User
from app.services.empaquetado import (anuncios_de_un_solo_producto,
                                      componentes_de, sku_limpio)

router = APIRouter(prefix="/api/almacen", tags=["almacen"])

# Lo unico del calculo de stock que ve el almacen: unidades, nunca importes.
COLUMNAS_DE_STOCK = {
    "ProductoNombre": "producto",
    "Stock_Warehouse": "en_almacen",
    "Stock_FBT": "en_fbt",
    "StockActualizado": "total",
    "PendienteDeEnviar": "pendiente_de_enviar",
    "StockDisponible": "disponible",
}

LARGO_MINIMO_BUSQUEDA = 6
MAXIMO_COINCIDENCIAS = 25


def _mapas_de_sku(db: Session, store_id: str) -> set:
    """SKU cuya cantidad ya expandio el importador de Amazon o Walmart."""
    from sqlalchemy import text
    claves = set()
    for tabla, columna in (("amazon_sku_map", "amazon_sku"),
                           ("walmart_sku_map", "walmart_sku")):
        filas = db.execute(
            text(f"SELECT {columna} FROM {tabla} WHERE store_id = :sid"),
            {"sid": store_id}).fetchall()
        claves.update(f[0] for f in filas)
    return claves


@router.get("/orden/{numero}")
def buscar_orden(
    numero: str,
    user: User = Depends(require_almacen),
    db: Session = Depends(get_db),
):
    """Que lleva dentro un pedido, con los packs ya desmontados en piezas.

    No se filtra por marca aunque el usuario tenga una asignada: el pedido
    se empaqueta entero o no se empaqueta. El filtro de tienda (store_id) si
    es inamovible — son clientes distintos.
    """
    numero = (numero or "").strip()
    if len(numero) < LARGO_MINIMO_BUSQUEDA:
        raise HTTPException(
            status_code=400,
            detail=f"Escribe al menos {LARGO_MINIMO_BUSQUEDA} digitos del numero de orden")

    filas = (db.query(SalesOrder)
             .filter(SalesOrder.store_id == user.store_id,
                     SalesOrder.tiktok_order_id == numero)
             .all())

    if not filas:
        # Cada parecida va con su fecha, y las mas nuevas primero. Dos razones
        # medidas sobre produccion el 27-oct-2026: 41 de cada 150 colas de seis
        # digitos apuntan a MAS DE UN pedido, y 26.397 de los 28.119 pedidos
        # tienen mas de un mes. Una lista de numeros pelados ordenada por texto
        # pone lo viejo arriba y no da forma de distinguirlos.
        parecidas = (db.query(SalesOrder.tiktok_order_id,
                              func.max(SalesOrder.order_date).label("fecha"),
                              func.min(SalesOrder.platform).label("plataforma"))
                     .filter(SalesOrder.store_id == user.store_id,
                             SalesOrder.tiktok_order_id.like(f"%{numero}%"))
                     .group_by(SalesOrder.tiktok_order_id)
                     .order_by(func.max(SalesOrder.order_date).desc())
                     .limit(MAXIMO_COINCIDENCIAS).all())
        return {"orden": numero, "encontrada": False,
                "parecidas": [{"orden": p[0],
                               "fecha": p[1].isoformat() if p[1] else None,
                               "plataforma": p[2]}
                              for p in parecidas if p[0]],
                "lineas": [], "total_piezas": 0}

    from app.services.stock_calculator import _build_combo_dict
    combos = _build_combo_dict(db, user.store_id)
    skus_de_mapa = _mapas_de_sku(db, user.store_id)
    sku_por_nombre = {p.name.strip().lower(): p.sku
                      for p in db.query(Product)
                      .filter(Product.store_id == user.store_id).all()}
    # Por que: ver anuncios_de_un_solo_producto. Sin esto las piezas de un
    # pack salen sin foto, que es justo lo que el almacen mira.
    anuncios_sueltos = anuncios_de_un_solo_producto(combos)

    lineas, piezas = [], 0
    for f in filas:
        clave = sku_limpio(f.seller_sku, f.sku)
        trozos = componentes_de(clave, f.product_name, f.quantity, combos,
                                plataforma=f.platform, skus_de_mapa=skus_de_mapa)
        piezas += sum(t["cantidad"] for t in trozos)
        lineas.append({
            "seller_sku": f.seller_sku,
            "sku_plataforma": f.sku,
            # Reservado: hoy siempre None porque sales_orders todavia no
            # guarda el ID de anuncio. En cuanto exista la columna, el
            # dashboard ya sabe usarlo y acierta la foto sin heuristica.
            "product_id": None,
            "producto": f.product_name,
            "cantidad": f.quantity,
            "plataforma": f.platform,
            "estado": f.status,
            "es_combo": any(t["de_combo"] for t in trozos),
            "componentes": [{
                "producto": t["producto"],
                "sku": sku_por_nombre.get(t["producto"].strip().lower()),
                "cantidad": t["cantidad"],
                "anuncios": [{"sku": s, "unidades": u} for u, s in
                             anuncios_sueltos.get(t["producto"], ())],
            } for t in trozos],
        })

    primera = filas[0]
    return {
        "orden": numero,
        "encontrada": True,
        "plataforma": primera.platform,
        "fecha": primera.order_date.isoformat() if primera.order_date else None,
        "estado": primera.status,
        "lineas": lineas,
        "total_piezas": piezas,
    }


@router.get("/resumen")
def resumen_de_existencias(
    brand_slug: str | None = Query(None),
    user: User = Depends(require_almacen),
    db: Session = Depends(get_db),
):
    """Producto y cantidad, para que el almacen avise si algo no cuadra."""
    from app.api.analytics import _resolve_brand_slug
    from app.services.analytics_service import _filter_stock_df_by_brand, _get_stock_df

    # Si el usuario tiene marca asignada manda la suya, no lo que pida la URL.
    marca = _resolve_brand_slug(user, db, brand_slug)
    stock = _get_stock_df(db, user.store_id, 30)
    stock = _filter_stock_df_by_brand(db, user.store_id, stock, marca)
    if stock.empty:
        return []

    presentes = [c for c in COLUMNAS_DE_STOCK if c in stock.columns]
    recortado = stock[presentes].fillna(0)
    return [{COLUMNAS_DE_STOCK[c]: fila[c] for c in presentes}
            for _, fila in recortado.iterrows()]
