"""
Operational alerts to ops/admin (separate from customer-facing daily reports).

Recipient = env OPERATIONS_EMAIL (single address) or hardcoded fallback
'rodmatwh@gmail.com' so we never silently drop alerts. The customer-facing
recipient list in `stores.settings.report_recipients` is NEVER used here.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Optional

from app.services.agents._base import send_email

logger = logging.getLogger("rodmat.alerts")

OPS_EMAIL_FALLBACK = "rodmatwh@gmail.com"


def _ops_recipient() -> list[str]:
    addr = (os.getenv("OPERATIONS_EMAIL") or OPS_EMAIL_FALLBACK).strip().lower()
    return [addr] if addr else []


def send_freshness_alert(
    *,
    store_name: str,
    skipped: list[dict],
    latest_import_at: Optional[datetime],
) -> bool:
    """One consolidated alert listing every item skipped this cycle.

    skipped: list of {"kind": "daily_report" | "agent",
                      "name": str,
                      "reason": str}
    """
    recipients = _ops_recipient()
    if not recipients:
        logger.warning("No OPERATIONS_EMAIL configured; cannot send freshness alert.")
        return False
    if not skipped:
        return False

    rows = "".join(
        "<tr>"
        f"<td style='padding:6px 10px;border:1px solid #eee;'>{s['kind']}</td>"
        f"<td style='padding:6px 10px;border:1px solid #eee;'>{s['name']}</td>"
        f"<td style='padding:6px 10px;border:1px solid #eee;'>{s['reason']}</td>"
        "</tr>"
        for s in skipped
    )
    latest_html = (
        f"&Uacute;ltimo import: <strong>{latest_import_at.isoformat()}</strong>"
        if latest_import_at else "No hay imports registrados todav&iacute;a."
    )

    html = f"""<!DOCTYPE html>
<html><body style="font-family:'Segoe UI',Arial,sans-serif;max-width:640px;margin:0 auto;padding:20px;">
  <div style="background:#fff3cd;border:2px solid #f1c40f;border-radius:8px;padding:18px;margin-bottom:18px;">
    <h2 style="color:#856404;margin:0 0 8px;">&#9888; {store_name} &mdash; Reporte diario NO enviado</h2>
    <p style="color:#856404;margin:0;">
      No se subi&oacute; el fichero de &oacute;rdenes de hoy. Los siguientes elementos se saltaron
      y se dispararan autom&aacute;ticamente cuando subas el fichero.
    </p>
  </div>
  <p style="color:#555;font-size:13px;margin:0 0 12px;">{latest_html}</p>
  <table style="width:100%;border-collapse:collapse;font-size:13px;">
    <thead><tr style="background:#34495e;color:#fff;">
      <th style="padding:8px 10px;text-align:left;">Tipo</th>
      <th style="padding:8px 10px;text-align:left;">Nombre</th>
      <th style="padding:8px 10px;text-align:left;">Motivo</th>
    </tr></thead>
    <tbody>{rows}</tbody>
  </table>
  <p style="color:#888;font-size:12px;margin-top:18px;">
    Sube el CSV de &oacute;rdenes del d&iacute;a desde el panel y los reportes se enviar&aacute;n solos.
    Para forzar el env&iacute;o con datos antiguos: <code>POST /api/reports/send-now?force=true</code>.
  </p>
  <div style="text-align:center;color:#aaa;font-size:10px;margin-top:20px;">
    Rodmat Dashboard V2 &middot; {datetime.now().strftime('%Y-%m-%d %H:%M')} UTC
  </div>
</body></html>"""

    subject = f"⚠️ [{store_name}] Daily report NO enviado — falta fichero del día"
    ok = send_email(html, subject, recipients)
    logger.info("Freshness alert to %s: %s (items=%d)", recipients, "sent" if ok else "failed", len(skipped))
    return ok
