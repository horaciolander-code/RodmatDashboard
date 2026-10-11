"""Baja el log de un check fallido de un PR y enseña el trozo del error.

    python scripts/gh_log.py 19
"""
import json
import subprocess
import sys
import urllib.error
import urllib.request

REPO = "horaciolander-code/RodmatDashboard"


def _token() -> str:
    out = subprocess.run(["git", "credential", "fill"],
                         input="protocol=https\nhost=github.com\n\n",
                         capture_output=True, text=True).stdout
    for l in out.splitlines():
        if l.startswith("password="):
            return l[9:]
    raise SystemExit("sin credencial")


def api(ruta):
    req = urllib.request.Request(f"https://api.github.com/repos/{REPO}{ruta}")
    req.add_header("Authorization", f"Bearer {_token()}")
    req.add_header("Accept", "application/vnd.github+json")
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode())


class _Redirige(Exception):
    pass


class _SinRedirigir(urllib.request.HTTPRedirectHandler):
    """GitHub redirige el log a Azure; si se reenvía la cabecera de
    autorización, Azure la rechaza con un 401."""
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise _Redirige(newurl)


def log_de(job_id):
    req = urllib.request.Request(
        f"https://api.github.com/repos/{REPO}/actions/jobs/{job_id}/logs")
    req.add_header("Authorization", f"Bearer {_token()}")
    try:
        with urllib.request.build_opener(_SinRedirigir).open(req, timeout=90) as r:
            return r.read().decode("utf-8", "replace")
    except _Redirige as e:
        with urllib.request.urlopen(str(e.args[0]), timeout=90) as r:
            return r.read().decode("utf-8", "replace")


if __name__ == "__main__":
    n = int(sys.argv[1])
    pr = api(f"/pulls/{n}")
    for c in api(f"/commits/{pr['head']['sha']}/check-runs")["check_runs"]:
        if c.get("conclusion") != "failure":
            continue
        print(f"=== {c['name']} ===")
        lineas = log_de(c["id"]).splitlines()
        marcas = ("Error", "error", "FAILED", "Traceback", "assert", "exit code")
        i = next((k for k, l in enumerate(lineas) if any(m in l for m in marcas)),
                 max(0, len(lineas) - 40))
        for l in lineas[max(0, i - 10):i + 40]:
            print("   " + (l[29:200] if len(l) > 29 else l))
