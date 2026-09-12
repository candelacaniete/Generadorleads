# Generadorleads — SDR Autónomo Katem / Guía Pilar

Panel unificado en Streamlit para el ciclo de prospección B2B:

1. Sourcing de leads (Places / SerpAPI / Outscraper / Bright Data Maps·IG·FB·LinkedIn / Apollo / Clay API / Meta Graph / Directorios AR, o simulación)
2. Enrichment (email + LinkedIn) y scrape web → dolores / icebreaker con Claude
3. Scoring supervisado + despacho outbound (Instantly / webhook)
4. CRM local con pipeline y exportación CSV/Excel

## Producto multi-cliente (vertical Katem)

El panel es una **vertical de Katem** para ofrecer SDR Autónomo a múltiples clientes.
Cada workspace (`clients/<id>/`) tiene su CRM, logs y config. Guía Pilar es un cliente ejemplo.

Verticales template: directorios locales, B2B servicios, profesionales, retail.

## Pipeline supervisado

El flujo **no se ejecuta todo junto**: cada etapa tiene una puerta de aprobación.

1. **Sourcing** → Places / SerpAPI / Outscraper / Bright Data (Maps, IG, FB, LinkedIn) / Apollo / Clay API / Meta Graph / Directorios AR  
2. **Enrichment** → email (Hunter/Snov/heurística) + LinkedIn (webhook opcional)  
3. **Web / Dolores** → scrape del sitio + dolores/ángulo/icebreaker con Claude (o heurística)  
4. **Scoring** → uno a uno o lote confirmado (usa dolores del scrape)  
5. **Despacho** → Instantly / webhook supervisado  
6. **CRM** → pipeline + export  

## Inicio rápido

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env       # completar API keys opcionales
streamlit run app.py
```

Sin API keys la app sigue siendo usable: sourcing simulado, scrape+dolores heurísticos, scoring local y validación de payloads de despacho. Con `ANTHROPIC_API_KEY`, el paso Web/Dolores usa Claude sobre el texto scrapado.

## Clay API directa (sin Make)

1. Creá una **Public API key** en Clay → Settings → Account → API keys.
2. En la tabla: **Enable for API** (requiere Enterprise para `/tables/query`).
3. Copiá el table ID de la URL (`/tables/t_…`) a `CLAY_TABLE_ID`.
4. En el panel: fuente **Clay (API)**; ajustá nombres de columnas en el expander del sidebar si no coinciden con los defaults.
5. `CLAY_WEBHOOK_URL` queda solo como fallback legado si no hay table ID.

## Bright Data (Maps + social)

En **Scraper APIs** creá un collector por red y pegá:

| Env | Scraper |
|---|---|
| `BRIGHTDATA_TOKEN` | API token (uno solo) |
| `BRIGHTDATA_MAPS_URL` | Google Maps |
| `BRIGHTDATA_IG_URL` | Instagram |
| `BRIGHTDATA_FB_URL` | Facebook |
| `BRIGHTDATA_LINKEDIN_URL` | LinkedIn companies |

En el panel: fuentes **Bright Data Maps / Instagram / Facebook / LinkedIn**. Meta Graph queda como fallback.
