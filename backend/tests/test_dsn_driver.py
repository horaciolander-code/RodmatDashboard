"""El 11-oct el servicio dejó de arrancar porque la URL de Postgres no decía
qué driver usar: SQLAlchemy 2.1 cambió el suyo por defecto a psycopg v3, que
no está instalado. Tres despliegues caídos con el build en verde.

Estos tests fijan que el driver va SIEMPRE escrito en la URL.
    python backend/tests/test_dsn_driver.py
"""
import pathlib
import sys

_BACKEND = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_BACKEND))

from app.config import con_driver_explicito

POOLER = "postgresql://postgres.abc:clave@aws-0-eu-west-1.pooler.supabase.com:6543/postgres"


def test_postgres_lleva_siempre_el_driver_escrito():
    assert con_driver_explicito(POOLER).startswith("postgresql+psycopg2://")


def test_no_se_pierde_nada_del_resto_de_la_url():
    """Usuario, contraseña, host, puerto y base de datos intactos."""
    salida = con_driver_explicito(POOLER)
    assert salida.endswith("postgres.abc:clave@aws-0-eu-west-1.pooler.supabase.com:6543/postgres")
    assert salida.count("@") == 1


def test_parametros_de_conexion_intactos():
    url = "postgresql://u:p@host:5432/db?sslmode=require&channel_binding=require"
    assert con_driver_explicito(url).endswith("?sslmode=require&channel_binding=require")


def test_la_forma_antigua_postgres_tambien_se_normaliza():
    """Algunos proveedores siguen entregando la URL como postgres://."""
    assert con_driver_explicito("postgres://u:p@host/db") == "postgresql+psycopg2://u:p@host/db"


def test_si_ya_trae_driver_no_se_toca():
    for url in ("postgresql+psycopg2://u:p@host/db", "postgresql+psycopg://u:p@host/db"):
        assert con_driver_explicito(url) == url


def test_sqlite_no_se_toca():
    assert con_driver_explicito("sqlite:///data/rodmat_v2.db") == "sqlite:///data/rodmat_v2.db"


if __name__ == "__main__":
    fallos = 0
    for _n, _f in sorted(globals().items()):
        if _n.startswith("test_") and callable(_f):
            try:
                _f()
                print(f"  OK    {_n}")
            except AssertionError as e:
                fallos += 1
                print(f"  FALLA {_n}: {e}")
    print("TODO OK" if not fallos else f"{fallos} fallos")
    sys.exit(1 if fallos else 0)
