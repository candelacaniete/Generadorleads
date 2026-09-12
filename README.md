# Generadorleads — SDR Autónomo Katem / Guía Pilar

Panel unificado en Streamlit para el ciclo de prospección B2B:

1. Sourcing de leads (Google Places o simulación)
2. Scoring e icebreakers con OpenAI / Claude
3. Despacho outbound a Instantly o webhook Make/n8n
4. CRM local con pipeline y exportación CSV/Excel

## Inicio rápido

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env       # completar API keys opcionales
streamlit run app.py
```

Sin API keys la app sigue siendo usable: sourcing simulado, scoring heurístico local y validación de payloads de despacho.
