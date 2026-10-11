"""Stubs compartidos para probar servicios sin base de datos.

Varios tests necesitan importar `app.services.*`, que arrastra sqlalchemy y los
modelos ORM. En el job de CI de aislamiento solo hay pytest y pandas, así que
se inyectan módulos falsos.

Vive aquí y no dentro de cada test por una razón concreta: los stubs van a
`sys.modules` y duran toda la sesión de pytest. Si cada fichero instalaba el
suyo, el primero en orden alfabético ganaba y los demás se encontraban un
módulo incompleto — eso tumbó el CI el 11-oct con
`ImportError: cannot import name 'and_' from 'sqlalchemy'`.

Reglas: si el módulo real se puede importar, NO se toca. Si ya hay un stub,
se le añaden los atributos que le falten en vez de descartarlo.
"""
import importlib
import sys
import types

# Todo lo que los servicios importan por nombre de estos módulos.
_ATRIBUTOS = (
    "Session", "and_", "text", "or_", "func",
    "SalesOrder", "AffiliateSale", "Product", "Combo", "ComboItem",
    "InitialInventory", "IncomingStock", "FBTInventory",
)

_MODULOS = (
    "sqlalchemy", "sqlalchemy.orm",
    "app.models.sales", "app.models.product",
    "app.models.combo", "app.models.inventory",
)


def _falso(nombre: str) -> None:
    mod = sys.modules.get(nombre)
    if mod is None:
        try:
            importlib.import_module(nombre)
            return                      # el real existe: no se toca
        except Exception:
            mod = types.ModuleType(nombre)
            sys.modules[nombre] = mod
    elif getattr(mod, "__file__", None):
        return                          # módulo real ya importado: no se toca
    for attr in _ATRIBUTOS:
        if not hasattr(mod, attr):
            setattr(mod, attr, object)


def instalar() -> None:
    """Deja importables los servicios sin sqlalchemy ni base de datos."""
    for nombre in _MODULOS:
        _falso(nombre)
