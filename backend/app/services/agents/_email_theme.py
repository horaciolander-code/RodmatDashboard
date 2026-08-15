"""
Rodmat Email Theme — Dark Futurista Neon
Módulo compartido para todos los agentes (PRISM, HAIKU, FARAWAY, MESMERIZE, TIMELESS, KHAMRAH).

Paleta:
  bg base:        #0a0e27
  bg card:        linear-gradient(135deg,#0f142f,#141a3d)
  text primary:   #e4e9ff
  text muted:     #8892b0
  text dim:       #576177
  cyan:           #00D4FF   (settled / info / links)
  green:          #00FF88   (positive / margen)
  orange:         #FF9F45   (warning / pending)
  purple:         #7B61FF   (accents / brand secondary)
  red:            #FF6B35   (negative / alerts)
  crimson:        #FF3D6B   (critical)

Uso desde un agente:
    from ._email_theme import (
        BASE_CSS, wrap_html, header, section_title,
        kpi_card, kpi_row, kv_row, table_open, table_row,
        alert_box, footer, badge, waterfall
    )
"""

# =========== PALETA ===========
COLORS = {
    "bg":          "#0a0e27",
    "card_from":   "#0f142f",
    "card_to":     "#141a3d",
    "text":        "#e4e9ff",
    "text_muted":  "#8892b0",
    "text_dim":    "#576177",
    "border":      "rgba(123,97,255,0.2)",
    "border_soft": "rgba(123,97,255,0.08)",
    "cyan":        "#00D4FF",
    "green":       "#00FF88",
    "orange":      "#FF9F45",
    "purple":      "#7B61FF",
    "red":         "#FF6B35",
    "crimson":     "#FF3D6B",
    "brand_avon":  "#E31837",
    "brand_lux":   "#8B4A9C",
}

# =========== CSS base (mismo para todos los agentes) ===========
BASE_CSS = """
<style>
  body { margin:0; padding:20px; background:#0a0e27; color:#e4e9ff;
         font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif; }
  .card { max-width:720px; margin:0 auto;
          background:linear-gradient(135deg,#0f142f,#141a3d);
          border-radius:14px; padding:28px;
          border:1px solid rgba(123,97,255,0.2);
          box-shadow:0 8px 32px rgba(0,212,255,0.05); }
  .h { font-size:22px; font-weight:700;
       background:linear-gradient(90deg,#00D4FF,#7B61FF);
       -webkit-background-clip:text; -webkit-text-fill-color:transparent;
       background-clip:text;
       margin:0 0 4px; }
  .sub { color:#8892b0; font-size:12px; text-transform:uppercase; letter-spacing:1px; margin-bottom:22px; }
  .section-title { color:#8892b0; font-size:11px; text-transform:uppercase; letter-spacing:1px;
                   margin:20px 0 10px; font-weight:700;
                   padding-bottom:6px; border-bottom:1px solid rgba(123,97,255,0.15); }
  .kpi-row { display:flex; gap:8px; margin-bottom:14px; flex-wrap:wrap; }
  .kpi { flex:1 1 22%; background:rgba(15,20,47,0.7);
         border:1px solid rgba(123,97,255,0.15); border-radius:10px;
         padding:12px 14px; min-width:140px; }
  .kpi-l { color:#8892b0; font-size:10px; text-transform:uppercase; letter-spacing:0.5px; }
  .kpi-v { color:#fff; font-size:22px; font-weight:700; margin-top:4px;
           font-family:'SF Mono',Menlo,Consolas,monospace; }
  .kpi-hint { font-size:10px; color:#8892b0; margin-top:2px; }
  .kpi.cyan  .kpi-v { color:#00D4FF; }
  .kpi.green .kpi-v { color:#00FF88; }
  .kpi.orange .kpi-v { color:#FF9F45; }
  .kpi.red .kpi-v { color:#FF6B35; }
  .kpi.purple .kpi-v { color:#7B61FF; }

  .block { background:rgba(15,20,47,0.5); border-radius:10px; padding:14px 18px;
           border:1px solid rgba(123,97,255,0.1); margin-bottom:12px; }
  .kv { display:flex; justify-content:space-between; padding:6px 0;
        border-bottom:1px solid rgba(123,97,255,0.08); font-size:13px; }
  .kv:last-child { border:none; }
  .kv-l { color:#c5cdd6; }
  .kv-v { font-family:'SF Mono',Menlo,Consolas,monospace; font-weight:600; color:#e4e9ff; }
  .kv-v.pos { color:#00FF88; } .kv-v.neg { color:#FF6B35; }
  .kv-v.warn { color:#FF9F45; } .kv-v.info { color:#00D4FF; }
  .kv-v.final { color:#00D4FF; font-size:15px; }

  table.rdmt { width:100%; font-size:13px; border-collapse:collapse;
               background:rgba(15,20,47,0.5); border-radius:8px; overflow:hidden; }
  table.rdmt th { padding:10px 12px; color:#8892b0; font-size:10px;
                  text-transform:uppercase; letter-spacing:0.5px;
                  text-align:left; border-bottom:1px solid rgba(123,97,255,0.15);
                  font-weight:600; }
  table.rdmt td { padding:8px 12px; color:#e4e9ff;
                  border-bottom:1px solid rgba(123,97,255,0.05); }
  table.rdmt tr:last-child td { border-bottom:none; }
  table.rdmt tr:hover td { background:rgba(0,212,255,0.03); }

  .badge { display:inline-block; padding:3px 10px; border-radius:12px;
           font-size:10px; font-weight:700; text-transform:uppercase;
           letter-spacing:0.5px; }
  .badge.green { background:rgba(0,255,136,0.15); color:#00FF88;
                 border:1px solid rgba(0,255,136,0.3); }
  .badge.orange { background:rgba(255,159,69,0.15); color:#FF9F45;
                  border:1px solid rgba(255,159,69,0.3); }
  .badge.red { background:rgba(255,107,53,0.15); color:#FF6B35;
               border:1px solid rgba(255,107,53,0.3); }
  .badge.cyan { background:rgba(0,212,255,0.15); color:#00D4FF;
                border:1px solid rgba(0,212,255,0.3); }
  .badge.purple { background:rgba(123,97,255,0.15); color:#7B61FF;
                  border:1px solid rgba(123,97,255,0.3); }

  .alert { border-radius:10px; padding:14px 18px; margin-bottom:14px;
           border:1px solid; font-size:13px; }
  .alert.warn { background:rgba(255,159,69,0.08); border-color:rgba(255,159,69,0.3); color:#FFD9A8; }
  .alert.info { background:rgba(0,212,255,0.08); border-color:rgba(0,212,255,0.3); color:#B8ECFF; }
  .alert.crit { background:rgba(255,61,107,0.08); border-color:rgba(255,61,107,0.3); color:#FFB8CC; }

  .foot { color:#576177; font-size:11px; text-align:center; margin-top:24px;
          padding-top:16px; border-top:1px solid rgba(123,97,255,0.1); }
  .foot a { color:#8892b0; text-decoration:none; }
</style>
"""

# =========== HELPERS ===========
def wrap_html(inner: str, title: str = "Rodmat Report") -> str:
    """Envuelve el contenido en HTML completo con paleta dark."""
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"/><title>{title}</title>
{BASE_CSS}
</head><body><div class="card">{inner}</div></body></html>"""


def header(title: str, subtitle: str = "") -> str:
    return f'<div class="h">{title}</div><div class="sub">{subtitle}</div>'


def section_title(text: str) -> str:
    return f'<div class="section-title">{text}</div>'


def kpi_card(label: str, value: str, hint: str = "", tone: str = "") -> str:
    """tone in {'', 'cyan','green','orange','red','purple'}"""
    cls = f"kpi {tone}".strip()
    hint_html = f'<div class="kpi-hint">{hint}</div>' if hint else ""
    return f'<div class="{cls}"><div class="kpi-l">{label}</div><div class="kpi-v">{value}</div>{hint_html}</div>'


def kpi_row(cards: list[str]) -> str:
    return f'<div class="kpi-row">{"".join(cards)}</div>'


def kv_row(label: str, value: str, tone: str = "") -> str:
    """tone in {'', 'pos','neg','warn','info','final'}"""
    return f'<div class="kv"><span class="kv-l">{label}</span><span class="kv-v {tone}">{value}</span></div>'


def block_open() -> str:
    return '<div class="block">'


def block_close() -> str:
    return '</div>'


def alert_box(msg: str, tone: str = "info") -> str:
    """tone in {'info','warn','crit'}"""
    return f'<div class="alert {tone}">{msg}</div>'


def badge(text: str, tone: str = "cyan") -> str:
    return f'<span class="badge {tone}">{text}</span>'


def table_open(headers: list[str]) -> str:
    th = "".join(f'<th>{h}</th>' for h in headers)
    return f'<table class="rdmt"><thead><tr>{th}</tr></thead><tbody>'


def table_row(cells: list[str], align_right: list[int] = None) -> str:
    align_right = align_right or []
    tds = "".join(
        f'<td style="text-align:right;font-family:\'SF Mono\',Menlo,monospace;">{c}</td>'
        if i in align_right else f'<td>{c}</td>'
        for i, c in enumerate(cells)
    )
    return f'<tr>{tds}</tr>'


def table_close() -> str:
    return '</tbody></table>'


def waterfall(rows: list[tuple[str, str, str]]) -> str:
    """rows = [(label, value, tone)] tone: '','pos','neg','warn','final'"""
    html = ['<div class="block">']
    for label, value, tone in rows:
        html.append(kv_row(label, value, tone))
    html.append('</div>')
    return "".join(html)


def footer(text: str = "Rodmat Dashboard · agentes IA") -> str:
    return f'<div class="foot">{text}</div>'
