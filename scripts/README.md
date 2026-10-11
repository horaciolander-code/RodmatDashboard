# scripts/ — herramientas de operación

Esto **no es la aplicación**. No se despliega, no sirve tráfico y ningún
cliente lo toca. Son las herramientas con las que se opera la plataforma desde
fuera: hablar con la API de GitHub, trocear un CSV antes de subirlo, leer datos
de producción para un análisis.

Viven en el repositorio y no en el escritorio de nadie. Antes estaban sueltas en
`ProjectRodmat/scripts/`, una carpeta sin versionar: si ese disco se moría, se
perdían, no había historial y nadie más podía usarlas ni revisarlas.

## Qué hay aquí

| Script | Para qué |
|---|---|
| `gh_ops.py` | Estado y merge de pull requests. **Se niega a mergear** si hay algún check en rojo o sin terminar |
| `gh_log.py` | Baja el log de un check fallido y enseña el trozo del error |
| `split_orders_csv.py` | Parte el CSV de pedidos de TikTok en trozos para subirlo por el panel. El import hace UPSERT por `(store_id, order_id, sku)`, así que subirlo a trozos equivale a subirlo entero |
| `h11_201_finance_lectura.py` | Análisis de **solo lectura** contra producción por la Data API de Supabase |

## Reglas

**Credenciales: nunca en un fichero.** Se leen del entorno en el momento de
usarlas, o del gestor de credenciales del sistema. Ninguno de estos scripts
escribe un secreto a disco, lo imprime ni lo pasa por la línea de comandos.
Es la regla 5 de `CLAUDE.md`.

**Escrituras en base de datos: se proponen, no se ejecutan por iniciativa
propia.** Regla 4. Un script que escriba debe poder ejecutarse en seco primero
y enseñar exactamente qué va a hacer.

**Nada de levantar la aplicación en local.** Regla 3: estas herramientas hablan
con producción o con GitHub, no montan una copia de la plataforma.

## Cómo se ejecutan

Desde la raíz del repositorio:

```bash
python scripts/gh_ops.py estado 19 20
python scripts/gh_ops.py merge 20
python scripts/gh_log.py 19
```
