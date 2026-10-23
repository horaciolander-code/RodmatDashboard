"""Una tienda en pausa no gasta, no avisa y no deja entrar.

Nokal tiene la cuenta bloqueada y no usa el sistema desde el 25-ago-2026.
Hasta ahora eso no se podia declarar en ningun sitio, y el sistema seguia
tratandola como una tienda viva:

  - el informe diario intentaba salir cada manana, se saltaba por datos
    viejos y mandaba un aviso de "NO enviado". 30 avisos en 30 dias, todos
    sobre algo que ya sabemos.
  - los agentes, igual, en sus dias.
  - y sus 7 usuarios admin (5 de fuera de la casa) seguian pudiendo entrar
    al panel con normalidad.

Poner `agents_enabled` a false NO bastaba: el planificador registra el salto
y manda el aviso ANTES de llegar a consultar esa opcion. Por eso hace falta
un interruptor propio, que ademas se lee en el login.

Modulo puro a proposito: sin sqlalchemy ni modelos, para que corra en el job
de aislamiento del CI.
"""
from __future__ import annotations

CLAVE = "on_hold"


def en_pausa(settings) -> bool:
    """True si la tienda esta en pausa.

    Acepta lo que de verdad llega de la base: un dict, None, o el texto
    'true'/'false' si alguien lo edito a mano en el JSON.
    """
    if not settings:
        return False
    v = settings.get(CLAVE) if hasattr(settings, "get") else None
    if isinstance(v, str):
        return v.strip().lower() in ("true", "1", "si", "sí", "yes")
    return bool(v)


def motivo(settings) -> str:
    """Texto que se le ensena a quien intente entrar."""
    if not en_pausa(settings):
        return ""
    nota = (settings.get("on_hold_reason") or "").strip() if settings else ""
    base = "Esta cuenta esta en pausa. Ponte en contacto con Rodmat."
    return f"{base} ({nota})" if nota else base
