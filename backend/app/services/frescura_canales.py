"""Que canales llevan datos viejos, uno por uno.

Mirar la fecha mas reciente de TODAS las plataformas juntas no sirve cuando
hay varias: con TEMU subido hoy, el conjunto parece fresco aunque TikTok
lleve dos dias sin cargarse. El informe sale completo de aspecto y le falta
el canal que mas vende.

Modulo puro a proposito: sin pandas, sin sqlalchemy, sin modelos. Asi se
prueba en el job de aislamiento del CI, que solo tiene pytest y pandas.
"""
from __future__ import annotations

from datetime import date


NOMBRES = {"tiktok": "TikTok", "temu": "TEMU",
           "amazon": "Amazon", "walmart": "Walmart"}

# De que fichero del panel sale cada canal de venta.
#
# OJO con TEMU: el tipo de import "temu" es el EXTRACTO (dinero), el mirror
# del merchant statement de TikTok. Los pedidos llegan en "temu_orders".
# Si aqui pusiera "temu", subir solo el extracto haria creer al sistema que
# los pedidos del dia ya estan cargados — y no lo estarian.
TIPO_POR_CANAL = {
    "tiktok":  "tiktok",
    "amazon":  "amazon",
    "walmart": "walmart",
    "temu":    "temu_orders",
}


def canales_pendientes(canales_activos, tipos_de_hoy) -> list:
    """Canales que venden estos dias y cuyo fichero NO ha llegado hoy.

    El informe diario sumaba lo que hubiera cargado en el momento de salir.
    Con un solo canal eso daba igual; con TikTok y TEMU significa que el
    email de las 8:00 sale con TikTok y sin TEMU si TEMU se sube despues, y
    el dia queda marcado como "ya enviado": nadie reenvia la cifra buena.

    `canales_activos` son los canales con ventas recientes (lo decide quien
    llama, con su ventana). `tipos_de_hoy` son los import_type cargados hoy.

    Un canal cuyo fichero no sabemos identificar NO bloquea el informe:
    preferimos un informe de mas a quedarnos sin informe por un canal que
    nadie ha mapeado todavia.
    """
    hoy = {str(t).strip().lower() for t in tipos_de_hoy if t}
    pendientes = []
    for c in sorted({str(x).strip().lower() for x in canales_activos if x}):
        tipo = TIPO_POR_CANAL.get(c)
        if tipo is not None and tipo not in hoy:
            pendientes.append(c)
    return pendientes


def texto_canales(canales) -> str:
    """'TEMU' o 'TikTok y TEMU' — sin dias, estos no llevan retraso aun."""
    partes = [NOMBRES.get(c, c) for c in canales]
    if not partes:
        return ""
    if len(partes) == 1:
        return partes[0]
    return ", ".join(partes[:-1]) + " y " + partes[-1]


def canales_desactualizados(ventas, hoy: date, umbral_dias: int = 2,
                            dias_actividad: int = 14) -> list:
    """Devuelve [(canal, dias_de_retraso), ...] de los canales que van viejos.

    `ventas` es un iterable de (canal, fecha_de_pedido).

    Solo se avisa de un canal si ha vendido en los ultimos `dias_actividad`:
    Walmart lleva desde junio sin una venta, y avisar de el cada dia seria
    ruido que acaba tapando el aviso que si importa.

    El umbral es el mismo que ya usaba el informe (2 dias), para no cambiar
    de criterio por el camino.
    """
    ultima = {}
    for canal, f in ventas:
        if not canal or f is None:
            continue
        c = str(canal).strip().lower()
        d = f.date() if hasattr(f, "date") else f
        if c not in ultima or d > ultima[c]:
            ultima[c] = d

    fuera = []
    for canal, d in sorted(ultima.items()):
        retraso = (hoy - d).days
        if retraso >= umbral_dias and retraso <= dias_actividad:
            fuera.append((canal, retraso))
    return fuera


def texto_aviso(canales: list) -> str:
    """'TikTok (2 dias)' o 'TikTok (2 dias) y Amazon (3 dias)'."""
    if not canales:
        return ""
    partes = [f"{NOMBRES.get(c, c)} ({d} día{'s' if d != 1 else ''})"
              for c, d in canales]
    if len(partes) == 1:
        return partes[0]
    return ", ".join(partes[:-1]) + " y " + partes[-1]
