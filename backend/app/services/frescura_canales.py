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
    nombres = {"tiktok": "TikTok", "temu": "TEMU",
               "amazon": "Amazon", "walmart": "Walmart"}
    partes = [f"{nombres.get(c, c)} ({d} día{'s' if d != 1 else ''})"
              for c, d in canales]
    if len(partes) == 1:
        return partes[0]
    return ", ".join(partes[:-1]) + " y " + partes[-1]
