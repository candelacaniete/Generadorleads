# Generadorleads — SDR Autónomo Katem / Guía Pilar

Panel unificado en Streamlit para el ciclo de prospección B2B:

1. Sourcing de leads (Google Places / Apollo.io / Clay, o simulación)
2. Scoring e icebreakers con OpenAI / Claude
3. Despacho outbound a Instantly o webhook Make/n8n
4. CRM local con pipeline y exportación CSV/Excel

## Producto multi-cliente (vertical Katem)

El panel es una **vertical de Katem** para ofrecer SDR Autónomo a múltiples clientes.
Cada workspace (`clients/<id>/`) tiene su CRM, logs y config. Guía Pilar es un cliente ejemplo.

Verticales template: directorios locales, B2B servicios, profesionales, retail.

## Pipeline supervisado

El flujo **no se ejecuta todo junto**: cada etapa tiene una puerta de aprobación.

1. **Sourcing** → Places / Apollo / Clay · seleccioná y aprobá  
2. **Enrichment** → email (Hunter/Snov/heurística) + LinkedIn vía Clay  
3. **Scoring** → uno a uno o lote confirmado  
4. **Despacho** → Instantly / webhook supervisado  
5. **CRM** → pipeline + export  

## Inicio rápido

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env       # completar API keys opcionales
streamlit run app.py
```

Sin API keys la app sigue siendo usable: sourcing simulado, scoring heurístico local y validación de payloads de despacho.
