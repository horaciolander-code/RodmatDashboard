"""Operaciones de GitHub sobre horaciolander-code/RodmatDashboard.

El token se lee de Git Credential Manager en memoria: nunca se escribe a disco,
ni se imprime, ni viaja por la línea de comandos (regla 5 de CLAUDE.md).

    python scripts/gh_ops.py estado 16 17 18 19    # CI y mergeabilidad
    python scripts/gh_ops.py merge  16 17 18 19    # mergea, en ese orden

`merge` se niega si el PR tiene checks en rojo, checks sin terminar o
conflictos. No fuerza, no salta comprobaciones y no mergea a ciegas.
"""
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request

REPO = "horaciolander-code/RodmatDashboard"
API = f"https://api.github.com/repos/{REPO}"
# Un check que no bloquea el merge pero cuyo rojo sí quiero respetar:
CONCLUSIONES_OK = (None, "success", "skipped", "neutral")


def _token() -> str:
    out = subprocess.run(["git", "credential", "fill"],
                         input="protocol=https\nhost=github.com\n\n",
                         capture_output=True, text=True).stdout
    for linea in out.splitlines():
        if linea.startswith("password="):
            return linea[9:]
    raise SystemExit("sin credencial de GitHub en Git Credential Manager")


def llamar(metodo: str, ruta: str, cuerpo=None):
    datos = json.dumps(cuerpo).encode() if cuerpo is not None else None
    req = urllib.request.Request(API + ruta, data=datos, method=metodo)
    req.add_header("Authorization", f"Bearer {_token()}")
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=90) as r:
            crudo = r.read().decode()
            return r.status, (json.loads(crudo) if crudo.strip() else {})
    except urllib.error.HTTPError as e:
        crudo = e.read().decode()
        try:
            return e.code, json.loads(crudo)
        except Exception:
            return e.code, {"raw": crudo[:300]}


def estado(n: int):
    _, pr = llamar("GET", f"/pulls/{n}")
    if "number" not in pr:
        return None
    _, d = llamar("GET", f"/commits/{pr['head']['sha']}/check-runs")
    pr["_checks"] = [(c["name"], c["status"], c.get("conclusion")) for c in d.get("check_runs", [])]
    return pr


def mostrar(n: int):
    pr = estado(n)
    if not pr:
        print(f"PR #{n}: no existe")
        return
    print(f"PR #{n}  {pr['head']['ref']}  ·  {pr['title'][:58]}")
    print(f"   mergeado={pr.get('merged')}  estado={pr.get('mergeable_state')}  "
          f"conflictos={'SI' if pr.get('mergeable') is False else 'no'}")
    for nombre, st, con in pr["_checks"]:
        print(f"   {nombre[:46]:46} {st}/{con or '—'}")


def mergear(n: int) -> bool:
    pr = estado(n)
    if not pr:
        print(f"PR #{n}: no existe")
        return False
    if pr.get("merged"):
        print(f"PR #{n}: ya estaba mergeado")
        return True
    rojos = [c[0] for c in pr["_checks"] if c[2] not in CONCLUSIONES_OK]
    pendientes = [c[0] for c in pr["_checks"] if c[1] != "completed"]
    if rojos:
        print(f"PR #{n}: NO se mergea — checks en rojo: {rojos}")
        return False
    if pendientes:
        print(f"PR #{n}: NO se mergea — checks sin terminar: {pendientes}")
        return False
    if pr.get("mergeable") is False:
        print(f"PR #{n}: NO se mergea — tiene conflictos")
        return False
    code, r = llamar("PUT", f"/pulls/{n}/merge",
                     {"merge_method": "merge", "commit_title": f"{pr['title']} (#{n})"})
    if code == 200 and r.get("merged"):
        print(f"PR #{n}: MERGEADO · {str(r.get('sha'))[:7]}")
        return True
    print(f"PR #{n}: FALLO · {r.get('message')}")
    return False


if __name__ == "__main__":
    if len(sys.argv) < 3 or sys.argv[1] not in ("estado", "merge"):
        print(__doc__)
        sys.exit(1)
    accion, numeros = sys.argv[1], [int(x) for x in sys.argv[2:]]
    for i, n in enumerate(numeros):
        if accion == "estado":
            mostrar(n)
            print()
        else:
            if i:
                time.sleep(5)   # GitHub recalcula mergeabilidad tras cada merge
            mergear(n)
