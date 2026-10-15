"""Aviso de los SKU que aparecen en ventas y NO descuentan inventario.

Aqui vive solo la parte PURA: a quien se avisa, de que se avisa y como se
pinta el correo. La parte que habla con la base de datos y manda el email
esta en `scheduled_jobs.avisar_skus_sin_asignar`.

Separado a proposito: asi se puede probar sin sqlalchemy ni modelos ORM. El
job de CI de aislamiento solo tiene pytest y pandas, y un test que para
arrancar necesita stubear media aplicacion acaba probando los stubs.
"""
from __future__ import annotations


def destinatarios_aviso(store) -> list:
    """A quien se avisa. SOLO `settings.alert_recipients`, sin respaldo.

    A proposito no cae a `report_recipients`: este aviso es operativo y va a
    quien tenga que asignar los SKU, no a toda la lista del informe diario.
    Sin configurar no se manda nada — mejor callado que mandando un correo
    tecnico a quien no lo pidio.
    """
    dest = (getattr(store, "settings", None) or {}).get("alert_recipients") or []
    if isinstance(dest, str):
        dest = [dest]
    return [d.strip() for d in dest if d and str(d).strip()]


def calcular_nuevos(actuales: set, avisados: set) -> tuple[list, set]:
    """Devuelve (los que hay que avisar, la lista de avisados ya al dia).

    Las cargas diarias son cinco (tres trozos de TikTok, el statement y
    TEMU). Avisando en cada una serian cinco correos identicos al dia, y un
    aviso que se repite se deja de leer.

    Un SKU que deja de estar sin asignar SALE de la lista de avisados, asi
    que si reaparece vuelve a avisar. Sin esto quedaria marcado como "ya
    avisado" para siempre y pasaria desapercibido justo la segunda vez.
    """
    avisados = {str(s).strip() for s in avisados} & {str(s).strip() for s in actuales}
    nuevos = sorted({str(s).strip() for s in actuales} - avisados)
    return nuevos, avisados


def html_aviso_skus(sin_asignar: list, nuevos: list, tienda: str) -> str:
    """El correo. El numero que mueve a actuar no es cuantos SKU hay, sino
    cuantas unidades se han vendido sin descontarse."""
    nuevos = set(nuevos)
    filas = []
    for r in sorted(sin_asignar, key=lambda x: -(x.get("total_qty") or 0)):
        sku = str(r.get("seller_sku") or "")
        marca = ('<span style="background:#FCEBEB;color:#A32D2D;font-size:11px;'
                 'padding:2px 6px;border-radius:4px;margin-left:6px;">NUEVO</span>'
                 if sku in nuevos else "")
        filas.append(
            '<tr style="border-bottom:1px solid #e1e0d9;">'
            f'<td style="padding:8px 10px;font-family:monospace;font-size:13px;">{sku}{marca}</td>'
            f'<td style="padding:8px 10px;font-size:13px;color:#52514e;">'
            f'{str(r.get("product_name") or "")[:70]}</td>'
            f'<td style="padding:8px 10px;text-align:right;font-size:13px;">{r.get("order_count") or 0}</td>'
            f'<td style="padding:8px 10px;text-align:right;font-size:13px;font-weight:600;">'
            f'{int(r.get("total_qty") or 0)}</td></tr>')

    unidades = sum(int(r.get("total_qty") or 0) for r in sin_asignar)
    return f"""
    <div style="font-family:-apple-system,Segoe UI,Arial,sans-serif;max-width:720px;margin:0 auto;">
      <div style="background:#0b1020;color:#fff;padding:18px 22px;border-radius:10px 10px 0 0;">
        <div style="font-size:19px;font-weight:600;">{len(sin_asignar)} SKU sin asignar en {tienda}</div>
        <div style="font-size:13px;color:#9aa4bf;margin-top:4px;">
          No estan descontando inventario. Son {unidades:,} unidades vendidas que el
          sistema no ha restado del stock.
        </div>
      </div>
      <div style="border:1px solid #e1e0d9;border-top:0;border-radius:0 0 10px 10px;padding:18px 22px;">
        <p style="font-size:14px;color:#52514e;margin:0 0 14px;">
          {len(nuevos)} de ellos han aparecido hoy. Mientras no tengan combo asignado,
          su stock queda mas alto de lo real y el margen sale mal.
        </p>
        <table style="width:100%;border-collapse:collapse;">
          <tr style="background:#f1efe8;">
            <th style="text-align:left;padding:8px 10px;font-size:12px;">SKU</th>
            <th style="text-align:left;padding:8px 10px;font-size:12px;">Producto segun la plataforma</th>
            <th style="text-align:right;padding:8px 10px;font-size:12px;">Pedidos</th>
            <th style="text-align:right;padding:8px 10px;font-size:12px;">Unidades</th>
          </tr>
          {''.join(filas)}
        </table>
        <p style="margin:18px 0 0;">
          <a href="https://dashboard.rodmatcenter.com"
             style="background:#2a78d6;color:#fff;text-decoration:none;padding:10px 18px;
                    border-radius:8px;font-size:14px;display:inline-block;">
            Asignarlos en Gestion &gt; Gestion Combos
          </a>
        </p>
      </div>
    </div>"""
