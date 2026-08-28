"""Chat AI service — hybrid architecture (DEMO, single-user gated).

Path 1: 10 pre-defined tools (fast + safe, brand-scoped).
Path 2: text-to-SQL fallback (constrained SELECT-only, brand-scoped).
Path 3: off-topic guardrail (canned response, zero LLM cost).
"""
from __future__ import annotations
import os, json, re, logging
from datetime import datetime, timedelta, date
from typing import Any, Optional
from sqlalchemy import text
from sqlalchemy.orm import Session
import httpx

log = logging.getLogger("rodmat.chat_ai")

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL = os.getenv("GROQ_MODEL", "openai/gpt-oss-20b")
GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"

OFF_TOPIC_MSG = (
    "Soy el asistente de datos del dashboard. Solo respondo preguntas sobre tu "
    "operación: ventas, inventario, creators, órdenes y finanzas. Para preguntas "
    "generales prueba con ChatGPT o Claude."
)

ROUTER_SYSTEM = """Eres un router para un dashboard de e-commerce (Rodmat). Tu única tarea es clasificar la pregunta del usuario.

TOOLS DISPONIBLES:
1. get_sales_summary — "cuánto vendimos" / GMV / ventas totales en un periodo
2. get_top_products — "producto más vendido" / bestseller / top productos
3. get_product_stock — "cuánto stock queda" / cajas / inventario de un producto
4. get_low_stock_alerts — "qué reponer" / stock bajo / alerta inventario
5. get_top_creators — "mejores creators/afiliados/influencers"
6. get_sales_by_day — "ventas por día" / evolución diaria
7. get_platform_split — "TikTok vs Amazon" / desglose por plataforma
8. get_finance_pnl — "P&L" / margen / beneficios / rentabilidad
9. get_growth_vs_prev — "vs mes/semana pasado" / crecimiento / comparativa
10. get_recent_orders — "últimas órdenes" / órdenes recientes

RESPUESTA — EXACTAMENTE una línea con este formato:
TOOL:<nombre_tool>:<periodo>
o
SQL
o
OFF_TOPIC

Donde <periodo> es: today, yesterday, week, month, year (o "auto" si no se especifica).

REGLAS:
- Si la pregunta pide un dato del negocio (ventas, inventario, creators, órdenes, finanzas, productos, combos) → usa TOOL o SQL
- OFF_TOPIC SOLO si la pregunta NO tiene NINGUNA relación con e-commerce/datos del negocio (saludos, opiniones, código, temas generales)
- Preferir TOOL sobre SQL cuando algún tool encaje

EJEMPLOS:
"cuánto vendimos esta semana" → TOOL:get_sales_summary:week
"cuál es el producto más vendido del año" → TOOL:get_top_products:year
"cuánto stock queda de Imari" → TOOL:get_product_stock:auto
"mejores creators del mes" → TOOL:get_top_creators:month
"ventas por día últimos 15 días" → TOOL:get_sales_by_day:auto
"cuántos combos incluyen Far Away" → SQL
"hola cómo estás" → OFF_TOPIC
"escríbeme un email" → OFF_TOPIC

Devuelve SOLO la línea de clasificación, sin explicaciones ni JSON.
"""


def _groq_call(messages, response_format=None, max_tokens=800, temperature=0.2):
    if not GROQ_API_KEY:
        raise RuntimeError("GROQ_API_KEY not set")
    body = {"model": GROQ_MODEL, "messages": messages, "max_tokens": max_tokens, "temperature": temperature}
    if response_format: body["response_format"] = response_format
    r = httpx.post(GROQ_URL,
        headers={"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"},
        json=body, timeout=15)
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"]


def _route(question: str) -> dict:
    """Robust router — parses text response, falls back to keyword matching if LLM fails."""
    raw = ""
    try:
        raw = _groq_call(
            [{"role":"system","content":ROUTER_SYSTEM},{"role":"user","content":question}],
            max_tokens=60, temperature=0.0).strip()
        # Parse "TOOL:name:period" | "SQL" | "OFF_TOPIC"
        first_line = raw.split("\n")[0].strip().upper()
        if first_line.startswith("OFF_TOPIC"):
            return {"path":"OFF_TOPIC","tool":None,"reason":"llm-classified","period":None}
        if first_line.startswith("SQL"):
            return {"path":"SQL","tool":None,"reason":"llm-classified","period":None}
        if first_line.startswith("TOOL:"):
            parts = first_line.split(":")
            tool_name = parts[1].lower().strip() if len(parts) > 1 else None
            period = parts[2].lower().strip() if len(parts) > 2 else None
            if tool_name and tool_name in TOOLS:
                if period == "AUTO".lower(): period = None
                return {"path":"TOOL","tool":tool_name,"reason":"llm-classified","period":period}
        # Fallthrough: LLM returned unexpected format → try keyword match
        log.warning(f"Router unparseable: raw={raw!r}")
    except Exception as e:
        log.warning(f"Router LLM failed: {e}")

    # Keyword fallback — better than defaulting to OFF_TOPIC
    q = question.lower()
    if any(w in q for w in ["hola","que tal","cómo estás","como estas","gracias","chao","adios","adiós"]):
        return {"path":"OFF_TOPIC","tool":None,"reason":"kw-chitchat","period":None}
    if any(w in q for w in ["vend","gmv","factur","ingres"]):
        return {"path":"TOOL","tool":"get_sales_summary","reason":"kw-ventas","period":None}
    if any(w in q for w in ["producto más","producto mas","bestseller","best seller","top produc","más vendido","mas vendido"]):
        return {"path":"TOOL","tool":"get_top_products","reason":"kw-top-products","period":None}
    if any(w in q for w in ["stock","queda","caja","inventario","unidades"]):
        return {"path":"TOOL","tool":"get_product_stock","reason":"kw-stock","period":None}
    if any(w in q for w in ["reponer","stock bajo","alerta","urgente","cobertura"]):
        return {"path":"TOOL","tool":"get_low_stock_alerts","reason":"kw-lowstock","period":None}
    if any(w in q for w in ["creator","afiliad","influenc","tiktoker"]):
        return {"path":"TOOL","tool":"get_top_creators","reason":"kw-creators","period":None}
    if any(w in q for w in ["p&l","pnl","margen","beneficio","rentabil","pérdida","perdida"]):
        return {"path":"TOOL","tool":"get_finance_pnl","reason":"kw-pnl","period":None}
    if any(w in q for w in ["orden","order","pedido"]):
        return {"path":"TOOL","tool":"get_recent_orders","reason":"kw-orders","period":None}
    # Last resort: assume it's a data question → try SQL
    return {"path":"SQL","tool":None,"reason":f"fallback-sql (raw={raw[:80]!r})","period":None}


def _period_dates(period: Optional[str]) -> tuple[date,date]:
    today = date.today()
    p = (period or "week").lower()
    if p == "today": return today, today
    if p == "yesterday": return today-timedelta(days=1), today-timedelta(days=1)
    if p in ("week","last_7_days","7d"): return today-timedelta(days=7), today
    if p in ("month","last_30_days","30d"): return today-timedelta(days=30), today
    if p == "prev_week": return today-timedelta(days=14), today-timedelta(days=7)
    if p == "prev_month": return today-timedelta(days=60), today-timedelta(days=30)
    return today-timedelta(days=7), today


def _brand_where(brand_id: Optional[str], alias: str = "so") -> str:
    return f" AND {alias}.brand_id = :brand_id" if brand_id else ""


# ─── TOOLS ──────────────────────────────────────────────

def tool_get_sales_summary(db, store_id, brand_id, period="week", **kw):
    d1,d2 = _period_dates(period)
    q = text(f"""SELECT COUNT(DISTINCT tiktok_order_id), COALESCE(SUM(sku_subtotal_after_discount),0)::numeric(12,2),
                        COALESCE(SUM(quantity),0)
                 FROM sales_orders so
                 WHERE so.store_id=:store_id AND so.created_time::date BETWEEN :d1 AND :d2
                   AND (so.status IS NULL OR so.status NOT IN ('CANCELLED','Cancelled','Canceled'))
                   {_brand_where(brand_id)}""")
    params = {"store_id":store_id,"d1":d1,"d2":d2}
    if brand_id: params["brand_id"] = brand_id
    r = db.execute(q, params).fetchone()
    return {"period":f"{d1} → {d2}","orders":int(r[0] or 0),"gmv_usd":float(r[1] or 0),
            "units_sold":int(r[2] or 0),
            "avg_ticket_usd":round(float(r[1] or 0)/max(int(r[0] or 1),1),2)}


def tool_get_top_products(db, store_id, brand_id, period="month", limit=5, **kw):
    d1,d2 = _period_dates(period)
    q = text(f"""SELECT so.seller_sku, COALESCE(so.product_name, so.seller_sku), SUM(so.quantity),
                        SUM(so.sku_subtotal_after_discount)::numeric(12,2)
                 FROM sales_orders so
                 WHERE so.store_id=:store_id AND so.created_time::date BETWEEN :d1 AND :d2
                   AND (so.status IS NULL OR so.status NOT IN ('CANCELLED','Cancelled','Canceled'))
                   {_brand_where(brand_id)}
                 GROUP BY so.seller_sku, so.product_name
                 ORDER BY SUM(so.quantity) DESC LIMIT :lim""")
    params = {"store_id":store_id,"d1":d1,"d2":d2,"lim":int(limit)}
    if brand_id: params["brand_id"] = brand_id
    rows = db.execute(q, params).fetchall()
    return {"period":f"{d1} → {d2}",
            "top_products":[{"sku":r[0],"name":(r[1] or "")[:60],"units_sold":int(r[2] or 0),
                             "gmv_usd":float(r[3] or 0)} for r in rows]}


def tool_get_product_stock(db, store_id, brand_id, query="", **kw):
    if not query:
        return {"error":"Necesito el nombre o SKU del producto."}
    q = text(f"""SELECT p.sku, p.name, p.units_per_box,
                   COALESCE((SELECT SUM(quantity)::int FROM initial_inventory WHERE product_id=p.id),0),
                   COALESCE((SELECT SUM(qty_ordered)::int FROM incoming_stock
                              WHERE product_id=p.id AND status IN ('Recibido','Ajuste')),0),
                   COALESCE((SELECT SUM(quantity)::int FROM sales_orders s
                              WHERE s.seller_sku=p.sku AND s.store_id=:store_id
                                AND (s.status IS NULL OR s.status NOT IN ('CANCELLED','Cancelled','Canceled'))),0)
                 FROM products p
                 WHERE p.store_id=:store_id
                   AND (LOWER(p.name) LIKE :qlike OR LOWER(p.sku) LIKE :qlike)
                   {_brand_where(brand_id, alias='p')}
                 ORDER BY p.name LIMIT 5""")
    params = {"store_id":store_id,"qlike":f"%{query.lower()}%"}
    if brand_id: params["brand_id"] = brand_id
    rows = db.execute(q, params).fetchall()
    prods = []
    for r in rows:
        stock = max(0, int(r[3] or 0) + int(r[4] or 0) - int(r[5] or 0))
        upb = int(r[2] or 1)
        prods.append({"sku":r[0],"name":r[1],"units_per_box":upb,"current_stock":stock,
                      "boxes":stock // upb if upb else 0})
    return {"query":query,"products":prods}


def tool_get_low_stock_alerts(db, store_id, brand_id, threshold_days=14, **kw):
    q = text(f"""WITH sold30 AS (
                    SELECT s.seller_sku, SUM(s.quantity)::int as qty
                      FROM sales_orders s
                     WHERE s.store_id=:store_id AND s.created_time > NOW() - INTERVAL '30 days'
                       AND (s.status IS NULL OR s.status NOT IN ('CANCELLED','Cancelled','Canceled'))
                     GROUP BY s.seller_sku)
                 SELECT p.sku, p.name,
                   COALESCE((SELECT SUM(quantity)::int FROM initial_inventory WHERE product_id=p.id),0)
                 + COALESCE((SELECT SUM(qty_ordered)::int FROM incoming_stock
                              WHERE product_id=p.id AND status IN ('Recibido','Ajuste')),0)
                 - COALESCE((SELECT qty FROM sold30 WHERE seller_sku=p.sku),0) as stock,
                   COALESCE((SELECT qty FROM sold30 WHERE seller_sku=p.sku),0) as sold30
                 FROM products p
                 WHERE p.store_id=:store_id {_brand_where(brand_id, alias='p')}""")
    params = {"store_id":store_id}
    if brand_id: params["brand_id"] = brand_id
    rows = db.execute(q, params).fetchall()
    alerts = []
    for r in rows:
        stock, sold30 = int(r[2] or 0), int(r[3] or 0)
        if sold30 == 0: continue
        daily = sold30 / 30.0
        days = stock / daily if daily > 0 else 999
        if days < threshold_days:
            alerts.append({"sku":r[0],"name":r[1],"stock":stock,"days_cover":round(days,1)})
    alerts.sort(key=lambda x: x["days_cover"])
    return {"threshold_days":threshold_days,"alerts_count":len(alerts),"top_10_urgent":alerts[:10]}


def tool_get_top_creators(db, store_id, brand_id, period="month", limit=5, **kw):
    d1,d2 = _period_dates(period)
    q = text("""SELECT creator_username, COUNT(*), SUM(payment_amount)::numeric(12,2),
                       SUM(commission)::numeric(12,2)
                FROM affiliate_sales
                WHERE store_id=:store_id AND created_time::date BETWEEN :d1 AND :d2
                  AND order_status='COMPLETED'
                GROUP BY creator_username
                ORDER BY SUM(payment_amount) DESC NULLS LAST LIMIT :lim""")
    rows = db.execute(q, {"store_id":store_id,"d1":d1,"d2":d2,"lim":int(limit)}).fetchall()
    return {"period":f"{d1} → {d2}",
            "top_creators":[{"creator":r[0],"orders":int(r[1] or 0),
                             "gmv_usd":float(r[2] or 0),"commission_usd":float(r[3] or 0)} for r in rows]}


def tool_get_sales_by_day(db, store_id, brand_id, days_back=7, **kw):
    d1 = date.today() - timedelta(days=int(days_back))
    q = text(f"""SELECT created_time::date, COUNT(DISTINCT tiktok_order_id),
                        SUM(sku_subtotal_after_discount)::numeric(12,2)
                 FROM sales_orders so
                 WHERE so.store_id=:store_id AND so.created_time::date >= :d1
                   AND (so.status IS NULL OR so.status NOT IN ('CANCELLED','Cancelled','Canceled'))
                   {_brand_where(brand_id)}
                 GROUP BY 1 ORDER BY 1""")
    params = {"store_id":store_id,"d1":d1}
    if brand_id: params["brand_id"] = brand_id
    rows = db.execute(q, params).fetchall()
    return {"days":[{"date":str(r[0]),"orders":int(r[1] or 0),"gmv_usd":float(r[2] or 0)} for r in rows]}


def tool_get_platform_split(db, store_id, brand_id, period="month", **kw):
    d1,d2 = _period_dates(period)
    q = text(f"""SELECT COALESCE(platform,'unknown'), COUNT(DISTINCT tiktok_order_id),
                        SUM(sku_subtotal_after_discount)::numeric(12,2)
                 FROM sales_orders so
                 WHERE so.store_id=:store_id AND so.created_time::date BETWEEN :d1 AND :d2
                   AND (so.status IS NULL OR so.status NOT IN ('CANCELLED','Cancelled','Canceled'))
                   {_brand_where(brand_id)}
                 GROUP BY 1 ORDER BY 3 DESC NULLS LAST""")
    params = {"store_id":store_id,"d1":d1,"d2":d2}
    if brand_id: params["brand_id"] = brand_id
    rows = db.execute(q, params).fetchall()
    return {"period":f"{d1} → {d2}",
            "by_platform":[{"platform":r[0],"orders":int(r[1] or 0),"gmv_usd":float(r[2] or 0)} for r in rows]}


def tool_get_finance_pnl(db, store_id, brand_id, month=None, **kw):
    if not month: month = date.today().strftime("%Y-%m")
    q = text(f"""SELECT COUNT(DISTINCT order_id), SUM(order_income)::numeric(12,2),
                        SUM(order_cost)::numeric(12,2), SUM(net_order_margin)::numeric(12,2)
                 FROM tiktok_statement_lines
                 WHERE store_id=:store_id AND TO_CHAR(order_paid_date,'YYYY-MM')=:m
                   {" AND brand_id = :brand_id" if brand_id else ""}""")
    params = {"store_id":store_id,"m":month}
    if brand_id: params["brand_id"] = brand_id
    r = db.execute(q, params).fetchone()
    net = float(r[1] or 0)
    earn = float(r[3] or 0)
    return {"month":month,"orders":int(r[0] or 0),"net_sales_usd":net,
            "total_cost_usd":float(r[2] or 0),"net_earnings_usd":earn,
            "margin_pct":round(earn/net*100,1) if net else 0}


def tool_get_growth_vs_prev(db, store_id, brand_id, period="week", **kw):
    curr = tool_get_sales_summary(db, store_id, brand_id, period)
    prev = tool_get_sales_summary(db, store_id, brand_id, "prev_week" if period=="week" else "prev_month")
    def pct(a,b): return round((a-b)/b*100,1) if b else 0
    return {"current":curr,"previous":prev,
            "gmv_growth_pct":pct(curr["gmv_usd"],prev["gmv_usd"]),
            "orders_growth_pct":pct(curr["orders"],prev["orders"])}


def tool_get_recent_orders(db, store_id, brand_id, hours=24, limit=10, **kw):
    q = text(f"""SELECT tiktok_order_id, created_time, product_name, quantity,
                        sku_subtotal_after_discount, buyer_username, city, state
                 FROM sales_orders so
                 WHERE so.store_id=:store_id AND so.created_time > NOW() - INTERVAL '{int(hours)} hours'
                   AND (so.status IS NULL OR so.status NOT IN ('CANCELLED','Cancelled','Canceled'))
                   {_brand_where(brand_id)}
                 ORDER BY so.created_time DESC LIMIT :lim""")
    params = {"store_id":store_id,"lim":int(limit)}
    if brand_id: params["brand_id"] = brand_id
    rows = db.execute(q, params).fetchall()
    return {"hours":int(hours),
            "orders":[{"order_id":r[0],"when":str(r[1]),"product":(r[2] or "")[:50],
                       "qty":int(r[3] or 0),"usd":float(r[4] or 0),"buyer":r[5],
                       "city":r[6],"state":r[7]} for r in rows]}


TOOLS = {
    "get_sales_summary": tool_get_sales_summary,
    "get_top_products": tool_get_top_products,
    "get_product_stock": tool_get_product_stock,
    "get_low_stock_alerts": tool_get_low_stock_alerts,
    "get_top_creators": tool_get_top_creators,
    "get_sales_by_day": tool_get_sales_by_day,
    "get_platform_split": tool_get_platform_split,
    "get_finance_pnl": tool_get_finance_pnl,
    "get_growth_vs_prev": tool_get_growth_vs_prev,
    "get_recent_orders": tool_get_recent_orders,
}


# ─── SQL FALLBACK ──────────────────────────────────────

ALLOWED_TABLES = {
    "products","sales_orders","combos","combo_items","affiliate_sales",
    "incoming_stock","initial_inventory","tiktok_statements",
    "tiktok_statement_lines","finance_custom_lines","amazon_sku_map","walmart_sku_map",
}
BANNED = re.compile(r'\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|GRANT|REVOKE|EXEC|EXECUTE|MERGE|COPY|CALL)\b', re.I)

SQL_SYSTEM = """Generas UNA sola query PostgreSQL SELECT para responder la pregunta.

Tablas permitidas:
- products (id, sku, name, price_cost, units_per_box, store_id, brand_id)
- sales_orders (tiktok_order_id, seller_sku, product_name, quantity, sku_subtotal_after_discount, created_time, status, store_id, brand_id)
- combos (id, store_id, combo_sku, combo_name, brand_id)
- combo_items (combo_id, product_id, quantity)
- affiliate_sales (creator_username, payment_amount, commission, order_status, created_time, store_id, brand_id)
- incoming_stock (product_id, qty_ordered, status, store_id)
- initial_inventory (product_id, quantity)
- tiktok_statement_lines (order_id, order_income, order_cost, net_order_margin, order_paid_date, store_id, brand_id)

REGLAS:
- SOLO SELECT
- SIEMPRE incluir WHERE store_id = :store_id (obligatorio)
- Si tabla tiene brand_id, incluir AND brand_id = :brand_id
- Añadir LIMIT 100
- Sin comentarios

Devuelve JSON: {"sql":"SELECT ...","explanation":"una frase"}
"""


def sql_fallback(db, store_id, brand_id, question):
    try:
        raw = _groq_call(
            [{"role":"system","content":SQL_SYSTEM},{"role":"user","content":question}],
            response_format={"type":"json_object"}, max_tokens=400)
        d = json.loads(raw)
        sql = d.get("sql","").strip().rstrip(";")
        expl = d.get("explanation","")

        if BANNED.search(sql):
            return {"error":"Contiene palabras clave prohibidas","sql_generated":sql}
        tables = set(re.findall(r'\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*)', sql, re.I))
        illegal = tables - ALLOWED_TABLES
        if illegal:
            return {"error":f"Tablas no permitidas: {illegal}","sql_generated":sql}
        if not sql.upper().lstrip().startswith("SELECT"):
            return {"error":"Solo SELECT","sql_generated":sql}
        if "LIMIT" not in sql.upper():
            sql += " LIMIT 100"

        params = {"store_id":store_id}
        if brand_id: params["brand_id"] = brand_id

        # execute with timeout
        db.execute(text("SET LOCAL statement_timeout = '5s'"))
        rows = db.execute(text(sql), params).fetchall()
        data = [dict(r._mapping) for r in rows[:100]]
        for row in data:
            for k,v in list(row.items()):
                if hasattr(v,"isoformat"): row[k] = v.isoformat()
        return {"sql_executed":sql,"explanation":expl,"rows_count":len(data),"data":data}
    except Exception as e:
        log.warning(f"SQL fallback: {e}")
        return {"error":f"Consulta falló: {str(e)[:200]}"}


# ─── FORMATTER ─────────────────────────────────────────

FORMAT_SYSTEM = """Formatea la respuesta a la pregunta del usuario en español, tono conversacional y conciso (2-4 frases máx).
Usa **negritas** para números clave. Si hay datos NULL/vacíos, dilo claramente.
NO inventes datos que no estén en el JSON. Sin disclaimers largos."""


def format_response(question, data):
    try:
        raw = _groq_call(
            [{"role":"system","content":FORMAT_SYSTEM},
             {"role":"user","content":f"Pregunta: {question}\n\nDatos:\n{json.dumps(data, default=str, indent=2)[:3000]}"}],
            max_tokens=400, temperature=0.3)
        return raw.strip()
    except Exception as e:
        return f"Datos: {json.dumps(data, default=str)[:500]}"


def _extract_period(question: str, hint: Optional[str]) -> str:
    q = question.lower()
    if hint and hint != "null": return hint
    if any(w in q for w in ["hoy","today"]): return "today"
    if any(w in q for w in ["ayer","yesterday"]): return "yesterday"
    if any(w in q for w in ["semana","week","7 día","7 dias","7 días"]): return "week"
    if any(w in q for w in ["mes","month","30 día","30 dias","30 días"]): return "month"
    return "week"


def _extract_query_text(question: str) -> str:
    """Extract product name/SKU from question."""
    q = question.lower()
    for prefix in ["de ","del ","para ","stock de ","cajas de ","perfume ","producto "]:
        if prefix in q:
            after = q.split(prefix,1)[1].strip()
            after = re.sub(r'[?¿.,!¡]+', '', after).strip()
            return after[:50]
    words = re.findall(r'\b[a-zA-Z0-9]+\b', question)
    return " ".join(words[-3:]) if words else ""


def answer_question(db: Session, store_id: str, brand_id: Optional[str], question: str) -> dict:
    routed = _route(question)
    path = routed["path"]

    if path == "OFF_TOPIC":
        return {"answer":OFF_TOPIC_MSG,"path":"OFF_TOPIC","tool_used":None,
                "raw_data":None,"sql_executed":None,"router_reason":routed.get("reason")}

    if path == "TOOL":
        tool_name = routed.get("tool")
        if tool_name not in TOOLS:
            return {"answer":f"Tool no reconocido. Reformula la pregunta.",
                    "path":"ERROR","tool_used":tool_name,"raw_data":None,"sql_executed":None}
        try:
            fn = TOOLS[tool_name]
            kwargs = {}
            if tool_name in ("get_sales_summary","get_top_products","get_top_creators",
                             "get_platform_split","get_growth_vs_prev"):
                kwargs["period"] = _extract_period(question, routed.get("period"))
            if tool_name == "get_product_stock":
                kwargs["query"] = _extract_query_text(question)
            data = fn(db, store_id, brand_id, **kwargs)
        except Exception as e:
            log.exception(f"Tool {tool_name} failed")
            return {"answer":f"Error ejecutando {tool_name}: {str(e)[:150]}",
                    "path":"ERROR","tool_used":tool_name,"raw_data":None,"sql_executed":None}
        answer = format_response(question, data)
        return {"answer":answer,"path":"TOOL","tool_used":tool_name,
                "raw_data":data,"sql_executed":None,"router_reason":routed.get("reason")}

    if path == "SQL":
        result = sql_fallback(db, store_id, brand_id, question)
        if "error" in result:
            return {"answer":f"No pude ejecutar la consulta: {result['error']}",
                    "path":"SQL_FAILED","tool_used":None,
                    "raw_data":result,"sql_executed":result.get("sql_generated")}
        answer = format_response(question, result)
        return {"answer":answer,"path":"SQL","tool_used":None,
                "raw_data":result.get("data"),"sql_executed":result.get("sql_executed"),
                "router_reason":routed.get("reason")}

    return {"answer":"No pude procesar tu pregunta.","path":"UNKNOWN",
            "tool_used":None,"raw_data":None,"sql_executed":None}
