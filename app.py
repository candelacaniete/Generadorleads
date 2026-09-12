"""
Katem / Guía Pilar — Panel de Control SDR Autónomo B2B
=======================================================
Aplicación Streamlit unificada para el ciclo completo de prospección:
Sourcing → Scoring IA → Despacho Outbound → CRM Local.

Ejecutar:  streamlit run app.py
"""

from __future__ import annotations

import json
import os
import re
import time
import uuid
from datetime import datetime, timezone
from html.parser import HTMLParser
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx
import pandas as pd
import requests
import streamlit as st
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Bootstrap: entorno y constantes
# ---------------------------------------------------------------------------
load_dotenv()

APP_TITLE = "SDR Autónomo · Katem"
CLIENTS_DIR = Path("./clients")
DEFAULT_CALENDAR_URL = "https://cal.com/katem"

CRM_COLUMNS = [
    "id",
    "nombre",
    "direccion",
    "telefono",
    "website",
    "email",
    "linkedin",
    "rating",
    "status_places",
    "rubro",
    "ubicacion",
    "lead_score",
    "score_razon",
    "icebreaker",
    "website_summary",
    "dolores",
    "angulo_email",
    "scrape_status",
    "scrape_fuente",
    "pipeline_status",
    "calendario_url",
    "notas",
    "client_id",
    "enrichment_fuente",
    "fecha_creacion",
    "fecha_actualizacion",
]

VERTICAL_TEMPLATES: dict[str, dict[str, Any]] = {
    "directorios_locales": {
        "label": "Directorios / comercios locales",
        "default_nicho": "comercios y servicios locales",
        "default_ubicacion": "Pilar",
        "fuentes": [
            "Google Places",
            "SerpAPI Maps",
            "Outscraper Maps",
            "Directorios AR",
            "Instagram (Meta/local)",
            "Facebook Pages (Meta)",
            "Clay (webhook)",
        ],
        "scoring_hint": "Cliente ideal para directorio local y servicios de presencia digital.",
    },
    "b2b_servicios": {
        "label": "B2B servicios / agencias",
        "default_nicho": "agencias de marketing",
        "default_ubicacion": "Argentina",
        "fuentes": ["Apollo.io", "Clay (webhook)", "Google Places", "SerpAPI Maps", "Bright Data Maps"],
        "scoring_hint": "Cliente ideal para automatización SDR, outbound y growth B2B.",
    },
    "profesionales": {
        "label": "Profesionales / clínicas",
        "default_nicho": "clínicas y profesionales de la salud",
        "default_ubicacion": "Zona Norte GBA",
        "fuentes": [
            "Google Places",
            "Doctoralia",
            "Instagram (Meta/local)",
            "Facebook Pages (Meta)",
            "Clay (webhook)",
        ],
        "scoring_hint": "Cliente ideal para captación de pacientes y reputación online.",
    },
    "retail": {
        "label": "Retail / e-commerce",
        "default_nicho": "tiendas y e-commerce",
        "default_ubicacion": "Buenos Aires",
        "fuentes": [
            "Google Places",
            "SerpAPI Maps",
            "Mercado Libre Servicios",
            "PedidosYa Partners",
            "Instagram (Meta/local)",
            "Clay (webhook)",
        ],
        "scoring_hint": "Cliente ideal para performance ads, CRM y recuperación de carrito.",
    },
}

DEFAULT_CLIENTS: dict[str, dict[str, Any]] = {
    "katem-demo": {
        "id": "katem-demo",
        "name": "Katem Demo",
        "vertical": "b2b_servicios",
        "calendar_url": DEFAULT_CALENDAR_URL,
        "brand": "Katem",
        "notes": "Workspace demo de la vertical SDR Autónomo.",
    },
    "guia-pilar": {
        "id": "guia-pilar",
        "name": "Guía Pilar",
        "vertical": "directorios_locales",
        "calendar_url": "https://cal.com/katem",
        "brand": "Guía Pilar",
        "notes": "Cliente ejemplo — directorio local Pilar.",
    },
}

DISPATCH_COLUMNS = [
    "timestamp",
    "lead_id",
    "nombre",
    "destino",
    "http_status",
    "exito",
    "detalle",
]

PIPELINE_STATUSES = [
    "Nuevo",
    "Contactado",
    "Respuesta Recibida",
    "Agendado",
    "Descartado",
]


# ---------------------------------------------------------------------------
# Utilidades de estado y persistencia
# ---------------------------------------------------------------------------
def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _safe_str(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    return str(value).strip()


def ensure_clients_seeded() -> None:
    """Crea carpeta clients/ y workspaces demo si no existen."""
    CLIENTS_DIR.mkdir(parents=True, exist_ok=True)
    for cid, meta in DEFAULT_CLIENTS.items():
        cdir = CLIENTS_DIR / cid
        cdir.mkdir(parents=True, exist_ok=True)
        cfg_path = cdir / "client.json"
        if not cfg_path.exists():
            cfg_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


def list_clients() -> list[dict[str, Any]]:
    ensure_clients_seeded()
    clients: list[dict[str, Any]] = []
    for p in sorted(CLIENTS_DIR.glob("*/client.json")):
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            data.setdefault("id", p.parent.name)
            clients.append(data)
        except Exception:  # noqa: BLE001
            clients.append({"id": p.parent.name, "name": p.parent.name, "vertical": "b2b_servicios"})
    return clients


def get_client(client_id: str) -> dict[str, Any]:
    ensure_clients_seeded()
    cfg_path = CLIENTS_DIR / client_id / "client.json"
    if cfg_path.exists():
        try:
            data = json.loads(cfg_path.read_text(encoding="utf-8"))
            data.setdefault("id", client_id)
            return data
        except Exception:  # noqa: BLE001
            pass
    return DEFAULT_CLIENTS.get(client_id, {"id": client_id, "name": client_id, "vertical": "b2b_servicios"})


def save_client(meta: dict[str, Any]) -> None:
    cid = _safe_str(meta.get("id")) or "cliente"
    cdir = CLIENTS_DIR / cid
    cdir.mkdir(parents=True, exist_ok=True)
    meta = {**meta, "id": cid}
    (cdir / "client.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")


def get_crm_path(client_id: str | None = None) -> Path:
    cid = client_id or st.session_state.get("active_client_id", "katem-demo")
    cdir = CLIENTS_DIR / cid
    cdir.mkdir(parents=True, exist_ok=True)
    return cdir / "leads_crm.csv"


def get_dispatch_log_path(client_id: str | None = None) -> Path:
    cid = client_id or st.session_state.get("active_client_id", "katem-demo")
    cdir = CLIENTS_DIR / cid
    cdir.mkdir(parents=True, exist_ok=True)
    return cdir / "dispatch_log.csv"


def reset_pipeline_state_for_client() -> None:
    """Limpia estado de pipeline al cambiar de workspace."""
    st.session_state.sourced_leads = pd.DataFrame()
    st.session_state.selected_lead_ids = []
    st.session_state.scored_leads = pd.DataFrame()
    st.session_state.high_leads = pd.DataFrame()
    st.session_state.enriched_leads = pd.DataFrame()
    st.session_state.researched_leads = pd.DataFrame()
    st.session_state.last_search_meta = {}
    st.session_state.pipeline_step = 1
    st.session_state.step1_approved = False
    st.session_state.step2_approved = False
    st.session_state.step_enrich_approved = False
    st.session_state.step_scrape_approved = False
    st.session_state.scoring_queue_ids = []
    st.session_state.scoring_queue_idx = 0
    st.session_state.scoring_current_result = None
    st.session_state.scrape_queue_ids = []
    st.session_state.scrape_queue_idx = 0
    st.session_state.scrape_current_result = None
    st.session_state.dispatch_queue_ids = []
    st.session_state.dispatch_queue_idx = 0
    st.session_state.dispatch_approved_ids = []
    st.session_state.dispatch_log = load_dispatch_log()


def init_session_state() -> None:
    ensure_clients_seeded()
    defaults: dict[str, Any] = {
        "active_client_id": "katem-demo",
        "sourced_leads": pd.DataFrame(),
        "selected_lead_ids": [],
        "enriched_leads": pd.DataFrame(),
        "researched_leads": pd.DataFrame(),
        "scored_leads": pd.DataFrame(),
        "high_leads": pd.DataFrame(),
        "dispatch_log": pd.DataFrame(columns=DISPATCH_COLUMNS),
        "last_search_meta": {},
        "api_errors": [],
        # Pipeline: 1 sourcing → 2 enrich → 3 web/dolores → 4 scoring → 5 dispatch → 6 crm
        "pipeline_step": 1,
        "step1_approved": False,
        "step_enrich_approved": False,
        "step_scrape_approved": False,
        "step2_approved": False,
        "scoring_mode": "Uno a uno (supervisado)",
        "scoring_queue_ids": [],
        "scoring_queue_idx": 0,
        "scoring_current_result": None,
        "scrape_mode": "Uno a uno (supervisado)",
        "scrape_queue_ids": [],
        "scrape_queue_idx": 0,
        "scrape_current_result": None,
        "dispatch_mode": "Uno a uno (supervisado)",
        "dispatch_queue_ids": [],
        "dispatch_queue_idx": 0,
        "dispatch_approved_ids": [],
        "enrich_mode": "Uno a uno (supervisado)",
        "enrich_queue_ids": [],
        "enrich_queue_idx": 0,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value
    # Cargar log del cliente activo
    if st.session_state.dispatch_log.empty:
        st.session_state.dispatch_log = load_dispatch_log()


def load_crm() -> pd.DataFrame:
    crm_path = get_crm_path()
    if crm_path.exists():
        try:
            df = pd.read_csv(crm_path, dtype=str).fillna("")
            for col in CRM_COLUMNS:
                if col not in df.columns:
                    df[col] = ""
            return df[CRM_COLUMNS]
        except Exception as exc:  # noqa: BLE001
            st.warning(f"No se pudo leer el CRM local: {exc}. Se inicia vacío.")
    return pd.DataFrame(columns=CRM_COLUMNS)


def save_crm(df: pd.DataFrame) -> None:
    out = df.copy()
    for col in CRM_COLUMNS:
        if col not in out.columns:
            out[col] = ""
    cid = st.session_state.get("active_client_id", "")
    if "client_id" in out.columns:
        out["client_id"] = out["client_id"].replace("", cid)
        out.loc[out["client_id"].astype(str).str.len() == 0, "client_id"] = cid
    out[CRM_COLUMNS].to_csv(get_crm_path(), index=False)


def load_dispatch_log() -> pd.DataFrame:
    log_path = get_dispatch_log_path()
    if log_path.exists():
        try:
            df = pd.read_csv(log_path, dtype=str).fillna("")
            for col in DISPATCH_COLUMNS:
                if col not in df.columns:
                    df[col] = ""
            return df[DISPATCH_COLUMNS]
        except Exception:  # noqa: BLE001
            return pd.DataFrame(columns=DISPATCH_COLUMNS)
    return pd.DataFrame(columns=DISPATCH_COLUMNS)


def append_dispatch_log(rows: list[dict[str, Any]]) -> pd.DataFrame:
    new_df = pd.DataFrame(rows)
    current = st.session_state.dispatch_log
    if current.empty:
        combined = new_df
    else:
        combined = pd.concat([current, new_df], ignore_index=True)
    for col in DISPATCH_COLUMNS:
        if col not in combined.columns:
            combined[col] = ""
    combined = combined[DISPATCH_COLUMNS].fillna("")
    combined.to_csv(get_dispatch_log_path(), index=False)
    st.session_state.dispatch_log = combined
    return combined


def env_or_secret(key: str, default: str = "") -> str:
    """Prioriza variables de entorno (.env); permite override vía UI."""
    return os.getenv(key, default) or default


# ---------------------------------------------------------------------------
# Integración: Google Places (Text Search + Place Details)
# ---------------------------------------------------------------------------
def _mock_places_leads(nicho: str, ubicacion: str, cantidad: int, fuente: str = "simulado") -> pd.DataFrame:
    """Extracción estructurada simulada cuando no hay API key de la fuente elegida."""
    niches = nicho or "negocios locales"
    city = ubicacion or "Pilar"
    samples = [
        {
            "nombre": f"{niches.title()} del Norte — {city}",
            "direccion": f"Av. Caamaño 1250, {city}, Buenos Aires",
            "telefono": "+54 11 4000-1001",
            "website": "https://ejemplo-negocio-norte.com.ar",
            "rating": "4.6",
            "status_places": "OPERATIONAL",
        },
        {
            "nombre": f"Estudio {niches.title()} {city}",
            "direccion": f"Las Magnolias 340, {city}, Buenos Aires",
            "telefono": "+54 230 442-2200",
            "website": "https://estudio-local-pilar.com.ar",
            "rating": "4.3",
            "status_places": "OPERATIONAL",
        },
        {
            "nombre": f"{city} {niches.title()} Premium",
            "direccion": f"Panamericana Km 50, {city}",
            "telefono": "+54 11 5555-8899",
            "website": "https://premium-servicios.ar",
            "rating": "4.8",
            "status_places": "OPERATIONAL",
        },
        {
            "nombre": f"Grupo Comercial {niches.title()}",
            "direccion": f"Calle Derqui 890, {city}",
            "telefono": "+54 230 448-1100",
            "website": "",
            "rating": "3.9",
            "status_places": "OPERATIONAL",
        },
        {
            "nombre": f"Soluciones B2B {niches.title()} {city}",
            "direccion": f"Boulevard Tortugas 210, {city}",
            "telefono": "+54 11 4777-3322",
            "website": "https://solucionesb2b.com.ar",
            "rating": "4.5",
            "status_places": "OPERATIONAL",
        },
        {
            "nombre": f"Centro {niches.title()} Integral",
            "direccion": f"Ruta 8 Km 52.5, {city}",
            "telefono": "+54 230 450-0099",
            "website": "https://centro-integral.ar",
            "rating": "4.1",
            "status_places": "OPERATIONAL",
        },
        {
            "nombre": f"{niches.title()} Express {city}",
            "direccion": f"San Martín 1555, {city}",
            "telefono": "+54 11 4888-7766",
            "website": "https://express-local.com",
            "rating": "4.0",
            "status_places": "OPERATIONAL",
        },
        {
            "nombre": f"Alianza {niches.title()} Zona Norte",
            "direccion": f"Champagnat 780, {city}",
            "telefono": "",
            "website": "https://alianza-zn.com.ar",
            "rating": "4.7",
            "status_places": "OPERATIONAL",
        },
    ]

    rows = []
    for i in range(cantidad):
        base = samples[i % len(samples)].copy()
        if i >= len(samples):
            base["nombre"] = f"{base['nombre']} #{i + 1}"
            base["telefono"] = f"+54 11 4{100 + i:03d}-{2000 + i:04d}"
        rows.append(
            {
                "id": str(uuid.uuid4())[:8],
                "nombre": base["nombre"],
                "direccion": base["direccion"],
                "telefono": base["telefono"],
                "website": base["website"],
                "rating": base["rating"],
                "status_places": base["status_places"],
                "rubro": niches,
                "ubicacion": city,
                "seleccionado": False,
                "fuente": fuente,
            }
        )
    return pd.DataFrame(rows)


def fetch_place_details(place_id: str, api_key: str, timeout: float = 20.0) -> dict[str, Any]:
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "fields": "name,formatted_address,formatted_phone_number,website,rating,business_status",
        "key": api_key,
        "language": "es",
    }
    try:
        resp = httpx.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("status") not in {"OK", "ZERO_RESULTS"}:
            return {"error": payload.get("status", "UNKNOWN"), "raw": payload}
        return payload.get("result", {})
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


def search_google_places(
    nicho: str,
    ubicacion: str,
    cantidad: int,
    api_key: str,
) -> tuple[pd.DataFrame, str]:
    """
    Consulta Google Places Text Search + Details.
    Retorna (DataFrame, modo) donde modo es 'google_places' o 'simulado'.
    """
    if not api_key:
        return _mock_places_leads(nicho, ubicacion, cantidad), "simulado"

    query = f"{nicho} en {ubicacion}".strip()
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    collected: list[dict[str, Any]] = []
    next_page_token: str | None = None
    mode = "google_places"

    try:
        while len(collected) < cantidad:
            params: dict[str, Any] = {
                "query": query,
                "key": api_key,
                "language": "es",
            }
            if next_page_token:
                params["pagetoken"] = next_page_token
                time.sleep(2.1)  # Google exige delay entre páginas

            resp = httpx.get(url, params=params, timeout=30.0)
            resp.raise_for_status()
            payload = resp.json()
            status = payload.get("status", "")

            if status == "ZERO_RESULTS":
                break
            if status not in {"OK"}:
                # Fallback defensivo a simulación
                st.warning(
                    f"Google Places respondió '{status}'. "
                    "Se usará extracción simulada para no bloquear el flujo."
                )
                return _mock_places_leads(nicho, ubicacion, cantidad), "simulado_fallback"

            for item in payload.get("results", []):
                if len(collected) >= cantidad:
                    break
                place_id = item.get("place_id", "")
                details = fetch_place_details(place_id, api_key) if place_id else {}
                if details.get("error"):
                    details = {}

                collected.append(
                    {
                        "id": str(uuid.uuid4())[:8],
                        "nombre": _safe_str(
                            details.get("name") or item.get("name")
                        ),
                        "direccion": _safe_str(
                            details.get("formatted_address")
                            or item.get("formatted_address")
                        ),
                        "telefono": _safe_str(details.get("formatted_phone_number")),
                        "website": _safe_str(details.get("website")),
                        "rating": _safe_str(
                            details.get("rating")
                            if details.get("rating") is not None
                            else item.get("rating")
                        ),
                        "status_places": _safe_str(
                            details.get("business_status")
                            or item.get("business_status")
                            or "UNKNOWN"
                        ),
                        "rubro": nicho,
                        "ubicacion": ubicacion,
                        "seleccionado": False,
                        "fuente": "google_places",
                    }
                )

            next_page_token = payload.get("next_page_token")
            if not next_page_token:
                break

        if not collected:
            st.info("Sin resultados en Places. Generando muestra simulada.")
            return _mock_places_leads(nicho, ubicacion, cantidad), "simulado_vacio"

        return pd.DataFrame(collected[:cantidad]), mode

    except Exception as exc:  # noqa: BLE001
        st.error(f"Error consultando Google Places: {exc}")
        return _mock_places_leads(nicho, ubicacion, cantidad, "simulado_error"), "simulado_error"


# ---------------------------------------------------------------------------
# Integración: Apollo.io — Organization Search
# ---------------------------------------------------------------------------
def _normalize_lead_row(
    *,
    nombre: str,
    direccion: str = "",
    telefono: str = "",
    website: str = "",
    rating: str = "",
    status_places: str = "",
    rubro: str = "",
    ubicacion: str = "",
    email: str = "",
    linkedin: str = "",
    fuente: str = "",
) -> dict[str, Any]:
    return {
        "id": str(uuid.uuid4())[:8],
        "nombre": _safe_str(nombre),
        "direccion": _safe_str(direccion),
        "telefono": _safe_str(telefono),
        "website": _safe_str(website),
        "rating": _safe_str(rating),
        "status_places": _safe_str(status_places) or "UNKNOWN",
        "rubro": _safe_str(rubro),
        "ubicacion": _safe_str(ubicacion),
        "email": _safe_str(email),
        "linkedin": _safe_str(linkedin),
        "seleccionado": False,
        "fuente": _safe_str(fuente),
    }


def search_apollo_organizations(
    nicho: str,
    ubicacion: str,
    cantidad: int,
    api_key: str,
) -> tuple[pd.DataFrame, str]:
    """
    Busca organizaciones en Apollo.io (mixed_companies/search).
    Sin API key → muestra simulada estilo Apollo.
    """
    if not api_key:
        return (
            _mock_places_leads(nicho, ubicacion, cantidad, "apollo_simulado"),
            "apollo_simulado",
        )

    url = "https://api.apollo.io/api/v1/mixed_companies/search"
    headers = {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "X-Api-Key": api_key,
    }
    # Filtros: keyword + ubicación (Apollo acepta organization_locations[])
    params: dict[str, Any] = {
        "q_organization_keyword_tags": nicho,
        "organization_locations[]": ubicacion,
        "per_page": min(max(int(cantidad), 1), 100),
        "page": 1,
    }
    try:
        resp = httpx.post(url, headers=headers, params=params, json={}, timeout=45.0)
        # Algunos planes usan body en vez de query; reintento defensivo
        if resp.status_code >= 400:
            body = {
                "q_organization_keyword_tags": nicho,
                "organization_locations": [ubicacion],
                "per_page": min(max(int(cantidad), 1), 100),
                "page": 1,
            }
            resp = httpx.post(url, headers=headers, json=body, timeout=45.0)

        if resp.status_code >= 400:
            st.warning(
                f"Apollo respondió HTTP {resp.status_code}. "
                "Se usará extracción simulada Apollo para no bloquear el flujo."
            )
            return (
                _mock_places_leads(nicho, ubicacion, cantidad, "apollo_simulado_fallback"),
                "apollo_simulado_fallback",
            )

        payload = resp.json()
        orgs = payload.get("organizations") or payload.get("accounts") or []
        rows: list[dict[str, Any]] = []
        for org in orgs[:cantidad]:
            phone = ""
            primary_phone = org.get("primary_phone") or {}
            if isinstance(primary_phone, dict):
                phone = _safe_str(primary_phone.get("number") or primary_phone.get("sanitized_number"))
            elif primary_phone:
                phone = _safe_str(primary_phone)
            if not phone:
                phone = _safe_str(org.get("phone") or org.get("sanitized_phone"))

            address_parts = [
                _safe_str(org.get("raw_address")),
                _safe_str(org.get("city")),
                _safe_str(org.get("state")),
                _safe_str(org.get("country")),
            ]
            direccion = ", ".join([p for p in address_parts if p]) or _safe_str(org.get("street_address"))

            rows.append(
                _normalize_lead_row(
                    nombre=_safe_str(org.get("name") or org.get("organization_name")),
                    direccion=direccion,
                    telefono=phone,
                    website=_safe_str(org.get("website_url") or org.get("primary_domain")),
                    rating=_safe_str(org.get("estimated_num_employees") or org.get("industry")),
                    status_places="APOLLO",
                    rubro=nicho,
                    ubicacion=ubicacion,
                    email="",
                    linkedin=_safe_str(org.get("linkedin_url")),
                    fuente="apollo",
                )
            )

        if not rows:
            st.info("Apollo no devolvió organizaciones. Generando muestra simulada.")
            return (
                _mock_places_leads(nicho, ubicacion, cantidad, "apollo_simulado_vacio"),
                "apollo_simulado_vacio",
            )
        return pd.DataFrame(rows), "apollo"

    except Exception as exc:  # noqa: BLE001
        st.error(f"Error consultando Apollo: {exc}")
        return (
            _mock_places_leads(nicho, ubicacion, cantidad, "apollo_simulado_error"),
            "apollo_simulado_error",
        )


# ---------------------------------------------------------------------------
# Integración: Clay — Webhook / API de tabla
# ---------------------------------------------------------------------------
def _parse_clay_leads_payload(payload: Any, nicho: str, ubicacion: str, fuente: str) -> list[dict[str, Any]]:
    """Normaliza respuestas típicas de Clay (lista, {leads|rows|results|data})."""
    items: list[Any]
    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict):
        items = (
            payload.get("leads")
            or payload.get("rows")
            or payload.get("results")
            or payload.get("data")
            or payload.get("records")
            or []
        )
        if isinstance(items, dict):
            items = items.get("rows") or items.get("items") or []
    else:
        items = []

    rows: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        # Clay suele anidar campos en "fields" / "cells"
        fields = item.get("fields") or item.get("cells") or item
        if not isinstance(fields, dict):
            continue

        def pick(*keys: str) -> str:
            for k in keys:
                if k in fields and fields.get(k) not in (None, ""):
                    return _safe_str(fields.get(k))
                # case-insensitive
                for fk, fv in fields.items():
                    if str(fk).lower() == k.lower() and fv not in (None, ""):
                        return _safe_str(fv)
            return ""

        nombre = pick("nombre", "name", "company", "company_name", "Company Name", "Name")
        if not nombre:
            continue
        rows.append(
            _normalize_lead_row(
                nombre=nombre,
                direccion=pick("direccion", "address", "Address", "location"),
                telefono=pick("telefono", "phone", "Phone", "mobile"),
                website=pick("website", "domain", "Website", "url", "Company Domain"),
                rating=pick("rating", "score", "employee_count", "Employees"),
                status_places=pick("status", "Status") or "CLAY",
                rubro=nicho,
                ubicacion=ubicacion or pick("ubicacion", "city", "City"),
                email=pick("email", "Email", "work_email"),
                linkedin=pick("linkedin", "LinkedIn", "linkedin_url"),
                fuente=fuente,
            )
        )
    return rows


def search_clay_leads(
    nicho: str,
    ubicacion: str,
    cantidad: int,
    api_key: str,
    webhook_url: str,
) -> tuple[pd.DataFrame, str]:
    """
    Fuente Clay vía webhook de tabla/workbook.
    Contrato esperado (flexible):
      POST {webhook_url}
      body: {action, nicho, ubicacion, cantidad, source}
      response JSON: lista de leads o {leads|rows|results: [...]}

    Sin webhook → muestra simulada Clay.
    """
    if not webhook_url:
        return (
            _mock_places_leads(nicho, ubicacion, cantidad, "clay_simulado"),
            "clay_simulado",
        )

    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
        headers["x-clay-api-key"] = api_key

    body = {
        "action": "source_leads",
        "nicho": nicho,
        "ubicacion": ubicacion,
        "cantidad": int(cantidad),
        "source": "katem_sdr_autonomo",
        "timestamp": _utc_now_iso(),
    }

    try:
        resp = httpx.post(webhook_url, headers=headers, json=body, timeout=60.0)
        if resp.status_code >= 400:
            st.warning(
                f"Clay webhook respondió HTTP {resp.status_code}. "
                "Se usará extracción simulada Clay."
            )
            return (
                _mock_places_leads(nicho, ubicacion, cantidad, "clay_simulado_fallback"),
                "clay_simulado_fallback",
            )

        try:
            payload = resp.json()
        except Exception:
            st.warning("Clay devolvió una respuesta no-JSON. Usando simulación.")
            return (
                _mock_places_leads(nicho, ubicacion, cantidad, "clay_simulado_parse"),
                "clay_simulado_parse",
            )

        rows = _parse_clay_leads_payload(payload, nicho, ubicacion, "clay")
        if not rows:
            st.info("Clay no devolvió filas parseables. Generando muestra simulada.")
            return (
                _mock_places_leads(nicho, ubicacion, cantidad, "clay_simulado_vacio"),
                "clay_simulado_vacio",
            )
        return pd.DataFrame(rows[:cantidad]), "clay"

    except Exception as exc:  # noqa: BLE001
        st.error(f"Error consultando Clay: {exc}")
        return (
            _mock_places_leads(nicho, ubicacion, cantidad, "clay_simulado_error"),
            "clay_simulado_error",
        )



# ---------------------------------------------------------------------------
# Maps alternativos: SerpAPI / Outscraper / Bright Data
# ---------------------------------------------------------------------------
def search_serpapi_maps(
    nicho: str,
    ubicacion: str,
    cantidad: int,
    api_key: str,
) -> tuple[pd.DataFrame, str]:
    """Google Maps vía SerpAPI (engine=google_maps)."""
    if not api_key:
        return _mock_places_leads(nicho, ubicacion, cantidad, "serpapi_simulado"), "serpapi_simulado"

    query = f"{nicho} {ubicacion}".strip()
    url = "https://serpapi.com/search.json"
    params = {
        "engine": "google_maps",
        "q": query,
        "type": "search",
        "hl": "es",
        "api_key": api_key,
    }
    try:
        resp = httpx.get(url, params=params, timeout=45.0)
        if resp.status_code >= 400:
            st.warning(f"SerpAPI HTTP {resp.status_code}. Usando simulación.")
            return _mock_places_leads(nicho, ubicacion, cantidad, "serpapi_fallback"), "serpapi_fallback"
        payload = resp.json()
        results = payload.get("local_results") or payload.get("place_results") or []
        if isinstance(results, dict):
            results = [results]
        rows: list[dict[str, Any]] = []
        for item in results[:cantidad]:
            rows.append(
                _normalize_lead_row(
                    nombre=_safe_str(item.get("title") or item.get("name")),
                    direccion=_safe_str(item.get("address")),
                    telefono=_safe_str(item.get("phone")),
                    website=_safe_str(item.get("website") or item.get("link")),
                    rating=_safe_str(item.get("rating")),
                    status_places="SERPAPI",
                    rubro=nicho,
                    ubicacion=ubicacion,
                    email="",
                    linkedin="",
                    fuente="serpapi_maps",
                )
            )
        if not rows:
            return _mock_places_leads(nicho, ubicacion, cantidad, "serpapi_vacio"), "serpapi_vacio"
        return pd.DataFrame(rows), "serpapi_maps"
    except Exception as exc:  # noqa: BLE001
        st.error(f"Error SerpAPI: {exc}")
        return _mock_places_leads(nicho, ubicacion, cantidad, "serpapi_error"), "serpapi_error"


def search_outscraper_maps(
    nicho: str,
    ubicacion: str,
    cantidad: int,
    api_key: str,
) -> tuple[pd.DataFrame, str]:
    """Google Maps vía Outscraper Maps Search API."""
    if not api_key:
        return _mock_places_leads(nicho, ubicacion, cantidad, "outscraper_simulado"), "outscraper_simulado"

    query = f"{nicho} {ubicacion}".strip()
    url = "https://api.app.outscraper.com/maps/search-v3"
    headers = {"X-API-KEY": api_key}
    params = {"query": query, "limit": min(int(cantidad), 100), "language": "es", "region": "AR"}
    try:
        resp = httpx.get(url, headers=headers, params=params, timeout=60.0)
        if resp.status_code >= 400:
            st.warning(f"Outscraper HTTP {resp.status_code}. Usando simulación.")
            return _mock_places_leads(nicho, ubicacion, cantidad, "outscraper_fallback"), "outscraper_fallback"
        payload = resp.json()
        # Outscraper suele devolver [[...results]] o {"data": [[...]]}
        data = payload.get("data") if isinstance(payload, dict) else payload
        flat: list[Any] = []
        if isinstance(data, list):
            for block in data:
                if isinstance(block, list):
                    flat.extend(block)
                elif isinstance(block, dict):
                    flat.append(block)
        rows: list[dict[str, Any]] = []
        for item in flat[:cantidad]:
            if not isinstance(item, dict):
                continue
            rows.append(
                _normalize_lead_row(
                    nombre=_safe_str(item.get("name") or item.get("title")),
                    direccion=_safe_str(item.get("full_address") or item.get("address")),
                    telefono=_safe_str(item.get("phone")),
                    website=_safe_str(item.get("site") or item.get("website")),
                    rating=_safe_str(item.get("rating")),
                    status_places="OUTSCRAPER",
                    rubro=nicho,
                    ubicacion=ubicacion,
                    email=_safe_str(item.get("email")),
                    linkedin="",
                    fuente="outscraper_maps",
                )
            )
        if not rows:
            return _mock_places_leads(nicho, ubicacion, cantidad, "outscraper_vacio"), "outscraper_vacio"
        return pd.DataFrame(rows), "outscraper_maps"
    except Exception as exc:  # noqa: BLE001
        st.error(f"Error Outscraper: {exc}")
        return _mock_places_leads(nicho, ubicacion, cantidad, "outscraper_error"), "outscraper_error"


def search_brightdata_maps(
    nicho: str,
    ubicacion: str,
    cantidad: int,
    api_token: str,
    dataset_url: str,
) -> tuple[pd.DataFrame, str]:
    """
    Google Maps vía Bright Data (dataset/collector HTTP).
    Espera BRIGHTDATA_TOKEN + BRIGHTDATA_MAPS_URL (endpoint del collector).
    Body: {query, limit, country}.
    """
    if not api_token or not dataset_url:
        return _mock_places_leads(nicho, ubicacion, cantidad, "brightdata_simulado"), "brightdata_simulado"

    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }
    body = {
        "query": f"{nicho} {ubicacion}",
        "nicho": nicho,
        "ubicacion": ubicacion,
        "limit": int(cantidad),
        "country": "AR",
        "source": "katem_sdr",
    }
    try:
        resp = httpx.post(dataset_url, headers=headers, json=body, timeout=90.0)
        if resp.status_code >= 400:
            st.warning(f"Bright Data HTTP {resp.status_code}. Usando simulación.")
            return _mock_places_leads(nicho, ubicacion, cantidad, "brightdata_fallback"), "brightdata_fallback"
        payload = resp.json()
        items = payload if isinstance(payload, list) else (
            payload.get("results") or payload.get("data") or payload.get("leads") or []
        )
        rows: list[dict[str, Any]] = []
        for item in items[:cantidad]:
            if not isinstance(item, dict):
                continue
            rows.append(
                _normalize_lead_row(
                    nombre=_safe_str(item.get("name") or item.get("title") or item.get("nombre")),
                    direccion=_safe_str(item.get("address") or item.get("direccion")),
                    telefono=_safe_str(item.get("phone") or item.get("telefono")),
                    website=_safe_str(item.get("website") or item.get("url")),
                    rating=_safe_str(item.get("rating")),
                    status_places="BRIGHTDATA",
                    rubro=nicho,
                    ubicacion=ubicacion,
                    email=_safe_str(item.get("email")),
                    linkedin=_safe_str(item.get("linkedin")),
                    fuente="brightdata_maps",
                )
            )
        if not rows:
            return _mock_places_leads(nicho, ubicacion, cantidad, "brightdata_vacio"), "brightdata_vacio"
        return pd.DataFrame(rows), "brightdata_maps"
    except Exception as exc:  # noqa: BLE001
        st.error(f"Error Bright Data: {exc}")
        return _mock_places_leads(nicho, ubicacion, cantidad, "brightdata_error"), "brightdata_error"


# ---------------------------------------------------------------------------
# Social: Instagram / Facebook Graph (páginas locales)
# ---------------------------------------------------------------------------
def _mock_social_leads(nicho: str, ubicacion: str, cantidad: int, red: str) -> pd.DataFrame:
    city = ubicacion or "Pilar"
    niches = nicho or "negocios locales"
    slug = re.sub(r"[^a-z0-9]+", "", niches.lower())[:12] or "local"
    rows = []
    for i in range(cantidad):
        handle = f"{slug}{city.lower().replace(' ', '')}{i+1}"
        if red == "instagram":
            web = f"https://instagram.com/{handle}"
            fuente = "instagram_simulado"
        else:
            web = f"https://facebook.com/{handle}"
            fuente = "facebook_simulado"
        rows.append(
            _normalize_lead_row(
                nombre=f"{niches.title()} {city} {i+1}",
                direccion=f"{city}, Buenos Aires, Argentina",
                telefono=f"+54 11 4{200+i:03d}-{3000+i:04d}" if i % 2 == 0 else "",
                website=web,
                rating=str(round(4.0 + (i % 10) / 10, 1)),
                status_places=red.upper(),
                rubro=niches,
                ubicacion=city,
                email="",
                linkedin="",
                fuente=fuente,
            )
        )
    return pd.DataFrame(rows)


def search_meta_pages(
    nicho: str,
    ubicacion: str,
    cantidad: int,
    access_token: str,
    network: str = "facebook",
) -> tuple[pd.DataFrame, str]:
    """
    Páginas locales vía Facebook Graph API (pages/search).
    Para Instagram usa el mismo token (páginas vinculadas) o cae a simulación.
    """
    red = "instagram" if network.lower().startswith("insta") else "facebook"
    if not access_token:
        return _mock_social_leads(nicho, ubicacion, cantidad, red), f"{red}_simulado"

    if red == "instagram":
        # Graph no expone búsqueda pública IG Business fácil: usar pages y marcar fuente IG
        # o webhook; aquí intentamos pages/search y etiquetamos instagram si hay ig handle.
        pass

    url = "https://graph.facebook.com/v19.0/pages/search"
    params = {
        "q": f"{nicho} {ubicacion}",
        "type": "page",
        "fields": "id,name,location,phone,website,link,overall_star_rating,category",
        "limit": min(int(cantidad), 25),
        "access_token": access_token,
    }
    try:
        resp = httpx.get(url, params=params, timeout=40.0)
        if resp.status_code >= 400:
            st.warning(f"Meta Graph HTTP {resp.status_code}. Usando simulación {red}.")
            return _mock_social_leads(nicho, ubicacion, cantidad, red), f"{red}_fallback"
        payload = resp.json()
        data = payload.get("data") or []
        rows: list[dict[str, Any]] = []
        for item in data[:cantidad]:
            loc = item.get("location") or {}
            direccion = ", ".join(
                [
                    _safe_str(loc.get("street")),
                    _safe_str(loc.get("city")),
                    _safe_str(loc.get("state")),
                    _safe_str(loc.get("country")),
                ]
            ).strip(", ")
            website = _safe_str(item.get("website") or item.get("link"))
            if red == "instagram" and website and "instagram.com" not in website:
                # si no hay IG explícito, dejamos fanpage como website
                pass
            rows.append(
                _normalize_lead_row(
                    nombre=_safe_str(item.get("name")),
                    direccion=direccion or ubicacion,
                    telefono=_safe_str(item.get("phone")),
                    website=website,
                    rating=_safe_str(item.get("overall_star_rating")),
                    status_places=_safe_str(item.get("category")) or red.upper(),
                    rubro=nicho,
                    ubicacion=ubicacion,
                    email="",
                    linkedin="",
                    fuente="instagram_graph" if red == "instagram" else "facebook_graph",
                )
            )
        if not rows:
            return _mock_social_leads(nicho, ubicacion, cantidad, red), f"{red}_vacio"
        return pd.DataFrame(rows), f"{red}_graph"
    except Exception as exc:  # noqa: BLE001
        st.error(f"Error Meta Graph: {exc}")
        return _mock_social_leads(nicho, ubicacion, cantidad, red), f"{red}_error"


# ---------------------------------------------------------------------------
# Directorios Argentina (vía webhook scraper o simulación estructurada)
# ---------------------------------------------------------------------------
DIRECTORIOS_AR = [
    "Cuitonline",
    "GuiaBancos",
    "Páginas Amarillas",
    "Mercado Libre Servicios",
    "Doctoralia",
    "PedidosYa Partners",
]


def _mock_directorio_ar_leads(
    directorio: str,
    nicho: str,
    ubicacion: str,
    cantidad: int,
) -> pd.DataFrame:
    city = ubicacion or "Pilar"
    niches = nicho or "servicios"
    suffix = {
        "Cuitonline": "SAS",
        "GuiaBancos": "Sucursal",
        "Páginas Amarillas": "Avisos",
        "Mercado Libre Servicios": "Prestador",
        "Doctoralia": "Consultorio",
        "PedidosYa Partners": "Local",
    }.get(directorio, "AR")
    rows = []
    for i in range(cantidad):
        domain = f"{re.sub(r'[^a-z0-9]+', '', niches.lower())[:10]}{i+1}.com.ar"
        rows.append(
            _normalize_lead_row(
                nombre=f"{niches.title()} {suffix} {city} #{i+1}",
                direccion=f"{city}, Buenos Aires, Argentina",
                telefono=f"+54 11 5{100+i:03d}-{4000+i:04d}",
                website=f"https://{domain}",
                rating=str(round(3.8 + (i % 12) / 10, 1)),
                status_places=directorio.upper()[:12],
                rubro=niches,
                ubicacion=city,
                email=f"contacto@{domain}",
                linkedin="",
                fuente=f"directorio_ar:{directorio.lower().replace(' ', '_')}",
            )
        )
    return pd.DataFrame(rows)


def search_directorios_ar(
    directorio: str,
    nicho: str,
    ubicacion: str,
    cantidad: int,
    webhook_url: str,
    api_key: str = "",
) -> tuple[pd.DataFrame, str]:
    """
    Extrae leads de directorios AR.
    Sin API pública estable: usa webhook scraper (Make/n8n/Bright Data)
    o simulación estructurada por directorio.
    """
    dir_name = directorio.strip() or "Páginas Amarillas"
    if not webhook_url:
        return (
            _mock_directorio_ar_leads(dir_name, nicho, ubicacion, cantidad),
            f"directorio_ar_simulado:{dir_name}",
        )

    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    body = {
        "action": "source_directorio_ar",
        "directorio": dir_name,
        "nicho": nicho,
        "ubicacion": ubicacion,
        "cantidad": int(cantidad),
        "source": "katem_sdr",
        "timestamp": _utc_now_iso(),
    }
    try:
        resp = httpx.post(webhook_url, headers=headers, json=body, timeout=90.0)
        if resp.status_code >= 400:
            st.warning(f"Webhook directorio AR HTTP {resp.status_code}. Usando simulación.")
            return (
                _mock_directorio_ar_leads(dir_name, nicho, ubicacion, cantidad),
                f"directorio_ar_fallback:{dir_name}",
            )
        payload = resp.json()
        # Reusa parser flexible de Clay
        rows = _parse_clay_leads_payload(
            payload, nicho, ubicacion, f"directorio_ar:{dir_name.lower().replace(' ', '_')}"
        )
        if not rows:
            return (
                _mock_directorio_ar_leads(dir_name, nicho, ubicacion, cantidad),
                f"directorio_ar_vacio:{dir_name}",
            )
        return pd.DataFrame(rows[:cantidad]), f"directorio_ar:{dir_name}"
    except Exception as exc:  # noqa: BLE001
        st.error(f"Error directorio AR ({dir_name}): {exc}")
        return (
            _mock_directorio_ar_leads(dir_name, nicho, ubicacion, cantidad),
            f"directorio_ar_error:{dir_name}",
        )


def search_leads(
    source: str,
    nicho: str,
    ubicacion: str,
    cantidad: int,
    cfg: dict[str, str],
) -> tuple[pd.DataFrame, str]:
    """Dispatcher unificado de sourcing multi-fuente."""
    source_norm = (source or "").strip().lower()

    if source_norm.startswith("apollo"):
        return search_apollo_organizations(nicho, ubicacion, cantidad, cfg.get("apollo_key", ""))
    if source_norm.startswith("clay"):
        return search_clay_leads(
            nicho,
            ubicacion,
            cantidad,
            cfg.get("clay_key", ""),
            cfg.get("clay_webhook_url", ""),
        )
    if source_norm.startswith("serpapi"):
        return search_serpapi_maps(nicho, ubicacion, cantidad, cfg.get("serpapi_key", ""))
    if source_norm.startswith("outscraper"):
        return search_outscraper_maps(nicho, ubicacion, cantidad, cfg.get("outscraper_key", ""))
    if source_norm.startswith("bright"):
        return search_brightdata_maps(
            nicho,
            ubicacion,
            cantidad,
            cfg.get("brightdata_token", ""),
            cfg.get("brightdata_maps_url", ""),
        )
    if "instagram" in source_norm:
        return search_meta_pages(
            nicho, ubicacion, cantidad, cfg.get("meta_token", ""), network="instagram"
        )
    if "facebook" in source_norm or source_norm.startswith("meta "):
        return search_meta_pages(
            nicho, ubicacion, cantidad, cfg.get("meta_token", ""), network="facebook"
        )

    # Directorios AR individuales o grupo
    if source_norm.startswith("directorios ar"):
        directorio = cfg.get("directorio_ar") or "Páginas Amarillas"
        return search_directorios_ar(
            directorio,
            nicho,
            ubicacion,
            cantidad,
            cfg.get("directorios_ar_webhook", ""),
            cfg.get("directorios_ar_key", ""),
        )
    for d in DIRECTORIOS_AR:
        if d.lower() in source_norm:
            return search_directorios_ar(
                d,
                nicho,
                ubicacion,
                cantidad,
                cfg.get("directorios_ar_webhook", ""),
                cfg.get("directorios_ar_key", ""),
            )

    # Default: Google Places
    return search_google_places(nicho, ubicacion, cantidad, cfg.get("google_key", ""))



# ---------------------------------------------------------------------------
# Enrichment: Email (Hunter/Snov/heurística) + LinkedIn vía Clay
# ---------------------------------------------------------------------------
def _domain_from_website(website: str) -> str:
    w = _safe_str(website).lower()
    w = re.sub(r"^https?://", "", w)
    w = re.sub(r"^www\.", "", w)
    return w.split("/")[0].strip()


def _heuristic_emails(domain: str, company_name: str = "") -> list[str]:
    if not domain or "." not in domain:
        return []
    locals_ = ["info", "contacto", "hola", "admin", "ventas", "comercial"]
    # Si el nombre parece persona, no inventamos name@ — solo genéricos
    return [f"{local}@{domain}" for local in locals_[:3]]


def enrich_email_hunter(domain: str, api_key: str) -> dict[str, Any]:
    """Domain search en Hunter.io. Retorna email principal + confianza."""
    url = "https://api.hunter.io/v2/domain-search"
    params = {"domain": domain, "api_key": api_key, "limit": 5}
    resp = httpx.get(url, params=params, timeout=30.0)
    resp.raise_for_status()
    data = resp.json().get("data", {})
    emails = data.get("emails") or []
    if not emails:
        return {"email": "", "confidence": 0, "fuente": "hunter_empty", "raw_count": 0}
    # Prioriza type=generic o mayor confidence
    emails_sorted = sorted(emails, key=lambda e: int(e.get("confidence") or 0), reverse=True)
    top = emails_sorted[0]
    return {
        "email": _safe_str(top.get("value")),
        "confidence": int(top.get("confidence") or 0),
        "fuente": "hunter",
        "raw_count": len(emails),
    }


def enrich_email_snov(domain: str, api_key: str) -> dict[str, Any]:
    """
    Snov.io domain emails (API v1).
    Nota: Snov suele requerir OAuth; si falla, el caller hace fallback heurístico.
    """
    # Intento simple con token como query (algunos planes legacy)
    url = "https://api.snov.io/v1/get-domain-emails-with-info"
    params = {"domain": domain, "access_token": api_key, "type": "all", "limit": 5}
    resp = httpx.get(url, params=params, timeout=30.0)
    if resp.status_code >= 400:
        raise RuntimeError(f"Snov HTTP {resp.status_code}: {resp.text[:200]}")
    payload = resp.json()
    emails = payload.get("emails") or payload.get("data") or []
    if not emails:
        return {"email": "", "confidence": 0, "fuente": "snov_empty", "raw_count": 0}
    top = emails[0] if isinstance(emails[0], dict) else {"email": str(emails[0])}
    return {
        "email": _safe_str(top.get("email") or top.get("value")),
        "confidence": int(top.get("probability") or top.get("confidence") or 50),
        "fuente": "snov",
        "raw_count": len(emails),
    }


def enrich_lead_email(lead: dict[str, Any], provider: str, api_key: str) -> dict[str, Any]:
    """Enriquece un lead con email. Siempre retorna el lead mergeado."""
    out = dict(lead)
    domain = _domain_from_website(_safe_str(lead.get("website")))
    if not domain:
        out["email"] = _safe_str(out.get("email"))
        out["enrichment_fuente"] = _safe_str(out.get("enrichment_fuente")) or "email_sin_dominio"
        return out

    try:
        if provider.startswith("Hunter") and api_key:
            result = enrich_email_hunter(domain, api_key)
        elif provider.startswith("Snov") and api_key:
            result = enrich_email_snov(domain, api_key)
        else:
            guesses = _heuristic_emails(domain, _safe_str(lead.get("nombre")))
            result = {
                "email": guesses[0] if guesses else "",
                "confidence": 20 if guesses else 0,
                "fuente": "heuristica_email",
                "raw_count": len(guesses),
            }
    except Exception as exc:  # noqa: BLE001
        guesses = _heuristic_emails(domain, _safe_str(lead.get("nombre")))
        result = {
            "email": guesses[0] if guesses else "",
            "confidence": 15 if guesses else 0,
            "fuente": f"email_fallback:{exc}",
            "raw_count": len(guesses),
        }

    if result.get("email"):
        out["email"] = result["email"]
    fuentes = [x for x in [_safe_str(out.get("enrichment_fuente")), _safe_str(result.get("fuente"))] if x]
    out["enrichment_fuente"] = "+".join(dict.fromkeys(fuentes))
    out["email_confidence"] = str(result.get("confidence", ""))
    return out


def enrich_linkedin_via_clay(
    lead: dict[str, Any],
    webhook_url: str,
    api_key: str = "",
) -> dict[str, Any]:
    """
    Pide a Clay (webhook) enriquecer LinkedIn company/person.
    Contrato POST: {action: enrich_linkedin, lead: {...}}
    Response flexible: {linkedin|linkedin_url|company_linkedin: "..."}
    """
    out = dict(lead)
    if not webhook_url:
        # Fallback: URL de búsqueda LinkedIn company
        nombre = _safe_str(lead.get("nombre"))
        domain = _domain_from_website(_safe_str(lead.get("website")))
        if domain:
            out["linkedin"] = f"https://www.linkedin.com/company/{domain.split('.')[0]}"
            out["enrichment_fuente"] = (
                (_safe_str(out.get("enrichment_fuente")) + "+linkedin_heuristica").strip("+")
            )
        elif nombre:
            q = re.sub(r"\s+", "%20", nombre)
            out["linkedin"] = f"https://www.linkedin.com/search/results/companies/?keywords={q}"
            out["enrichment_fuente"] = (
                (_safe_str(out.get("enrichment_fuente")) + "+linkedin_search").strip("+")
            )
        return out

    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
        headers["x-clay-api-key"] = api_key
    body = {
        "action": "enrich_linkedin",
        "lead": {
            "id": _safe_str(lead.get("id")),
            "nombre": _safe_str(lead.get("nombre")),
            "website": _safe_str(lead.get("website")),
            "ubicacion": _safe_str(lead.get("ubicacion")),
            "rubro": _safe_str(lead.get("rubro")),
            "email": _safe_str(lead.get("email")),
        },
        "source": "katem_sdr_autonomo",
        "timestamp": _utc_now_iso(),
    }
    try:
        resp = httpx.post(webhook_url, headers=headers, json=body, timeout=60.0)
        if resp.status_code >= 400:
            raise RuntimeError(f"Clay LinkedIn HTTP {resp.status_code}")
        payload = resp.json()
        # Normalizar
        linkedin = ""
        if isinstance(payload, dict):
            linkedin = _safe_str(
                payload.get("linkedin")
                or payload.get("linkedin_url")
                or payload.get("company_linkedin")
                or (payload.get("lead") or {}).get("linkedin")
            )
            # rows[0]
            if not linkedin:
                rows = payload.get("rows") or payload.get("leads") or payload.get("results") or []
                if rows and isinstance(rows[0], dict):
                    r0 = rows[0].get("fields") or rows[0]
                    linkedin = _safe_str(
                        r0.get("linkedin") or r0.get("LinkedIn") or r0.get("linkedin_url")
                    )
        if linkedin:
            out["linkedin"] = linkedin
            out["enrichment_fuente"] = (
                (_safe_str(out.get("enrichment_fuente")) + "+clay_linkedin").strip("+")
            )
        else:
            # fallback heurístico si Clay no trajo dato
            return enrich_linkedin_via_clay(lead, "", "")
    except Exception as exc:  # noqa: BLE001
        out = enrich_linkedin_via_clay(lead, "", "")
        out["enrichment_fuente"] = (
            (_safe_str(out.get("enrichment_fuente")) + f"+clay_linkedin_error").strip("+")
        )
        out["notas"] = (_safe_str(out.get("notas")) + f" | Clay LI error: {exc}")[:300]
    return out


def enrich_one_lead(
    lead: dict[str, Any],
    email_provider: str,
    email_api_key: str,
    do_email: bool,
    do_linkedin: bool,
    clay_linkedin_webhook: str,
    clay_key: str,
) -> dict[str, Any]:
    out = dict(lead)
    if do_email:
        out = enrich_lead_email(out, email_provider, email_api_key)
    if do_linkedin:
        out = enrich_linkedin_via_clay(out, clay_linkedin_webhook, clay_key)
    return out


def enrich_leads_batch(
    leads_df: pd.DataFrame,
    email_provider: str,
    email_api_key: str,
    do_email: bool,
    do_linkedin: bool,
    clay_linkedin_webhook: str,
    clay_key: str,
) -> pd.DataFrame:
    rows = []
    total = max(len(leads_df), 1)
    progress = st.progress(0.0, text="Enriqueciendo leads…")
    for idx, (_, row) in enumerate(leads_df.iterrows()):
        enriched = enrich_one_lead(
            row.to_dict(),
            email_provider,
            email_api_key,
            do_email,
            do_linkedin,
            clay_linkedin_webhook,
            clay_key,
        )
        rows.append(enriched)
        progress.progress((idx + 1) / total, text=f"Enriquecidos {idx + 1}/{len(leads_df)}")
    progress.empty()
    return pd.DataFrame(rows)


def upsert_enriched_lead(lead: dict[str, Any]) -> None:
    """Actualiza sourced_leads / enriched_leads con el lead enriquecido."""
    lid = _safe_str(lead.get("id"))
    enriched = st.session_state.get("enriched_leads", pd.DataFrame())
    row = pd.DataFrame([lead])
    if not isinstance(enriched, pd.DataFrame) or enriched.empty:
        st.session_state.enriched_leads = row
    else:
        if lid and (enriched["id"].astype(str) == lid).any():
            enriched = enriched[enriched["id"].astype(str) != lid]
        st.session_state.enriched_leads = pd.concat([enriched, row], ignore_index=True)

    # Sync back into sourced_leads
    sourced = st.session_state.sourced_leads
    if isinstance(sourced, pd.DataFrame) and not sourced.empty and lid:
        for col, val in lead.items():
            if col == "seleccionado":
                continue
            if col not in sourced.columns:
                sourced[col] = ""
            sourced.loc[sourced["id"].astype(str) == lid, col] = val
        st.session_state.sourced_leads = sourced


# ---------------------------------------------------------------------------
# Research web: scrape del sitio + dolores / ángulo con Claude
# ---------------------------------------------------------------------------
class _HTMLTextExtractor(HTMLParser):
    """Extrae texto visible de HTML (stdlib, sin BeautifulSoup)."""

    _SKIP = {"script", "style", "noscript", "svg", "iframe", "head"}

    def __init__(self) -> None:
        super().__init__()
        self._skip_depth = 0
        self.parts: list[str] = []
        self.title = ""
        self._in_title = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        t = tag.lower()
        if t in self._SKIP:
            self._skip_depth += 1
        if t == "title":
            self._in_title = True

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()
        if t in self._SKIP and self._skip_depth:
            self._skip_depth -= 1
        if t == "title":
            self._in_title = False

    def handle_data(self, data: str) -> None:
        text = " ".join(data.split())
        if not text:
            return
        if self._in_title and not self.title:
            self.title = text
        if self._skip_depth:
            return
        self.parts.append(text)


def _normalize_website_url(raw: str) -> str:
    url = _safe_str(raw)
    if not url:
        return ""
    if url.startswith("http://") or url.startswith("https://"):
        return url
    return f"https://{url}"


def _candidate_paths(base_url: str) -> list[str]:
    """Homepage + páginas típicas AR/ES para señales comerciales."""
    base = _normalize_website_url(base_url).rstrip("/")
    if not base:
        return []
    paths = ["", "/servicios", "/nosotros", "/contacto", "/about", "/services", "/empresa"]
    seen: set[str] = set()
    out: list[str] = []
    for p in paths:
        u = urljoin(base + "/", p.lstrip("/")) if p else base
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def fetch_website_text(website: str, max_chars: int = 12000, timeout: float = 12.0) -> dict[str, Any]:
    """
    Descarga 1–N páginas del sitio y concatena texto visible.
    Retorna {ok, text, title, pages_ok, error, fuente_urls}.
    """
    urls = _candidate_paths(website)
    if not urls:
        return {
            "ok": False,
            "text": "",
            "title": "",
            "pages_ok": 0,
            "error": "sin_website",
            "fuente_urls": "",
        }

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (compatible; KatemSDR/1.0; +https://katem.com.ar; research-bot)"
        ),
        "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-AR,es;q=0.9,en;q=0.5",
    }
    chunks: list[str] = []
    titles: list[str] = []
    ok_urls: list[str] = []
    last_err = ""

    with httpx.Client(follow_redirects=True, timeout=timeout, headers=headers) as client:
        for url in urls:
            if sum(len(c) for c in chunks) >= max_chars:
                break
            try:
                resp = client.get(url)
                if resp.status_code >= 400:
                    last_err = f"HTTP {resp.status_code} @ {url}"
                    continue
                ctype = (resp.headers.get("content-type") or "").lower()
                if "html" not in ctype and "text" not in ctype and ctype:
                    continue
                parser = _HTMLTextExtractor()
                try:
                    parser.feed(resp.text[:250_000])
                except Exception as exc:  # noqa: BLE001
                    last_err = f"parse_error:{exc}"
                    continue
                page_text = " ".join(parser.parts)
                if len(page_text) < 40:
                    continue
                ok_urls.append(url)
                if parser.title:
                    titles.append(parser.title)
                host = urlparse(url).path or "/"
                chunks.append(f"[Página {host}]\n{page_text}")
            except Exception as exc:  # noqa: BLE001
                last_err = str(exc)
                continue

    text = "\n\n".join(chunks).strip()
    if len(text) > max_chars:
        text = text[:max_chars] + "…"
    if not text:
        return {
            "ok": False,
            "text": "",
            "title": titles[0] if titles else "",
            "pages_ok": 0,
            "error": last_err or "sin_contenido",
            "fuente_urls": "",
        }
    return {
        "ok": True,
        "text": text,
        "title": titles[0] if titles else "",
        "pages_ok": len(ok_urls),
        "error": "",
        "fuente_urls": " | ".join(ok_urls[:4]),
    }


def _client_offer_context() -> str:
    client = get_client(st.session_state.get("active_client_id", "katem-demo"))
    vertical = _safe_str(client.get("vertical"))
    tpl = VERTICAL_TEMPLATES.get(vertical, {})
    return (
        f"Cliente/workspace: {_safe_str(client.get('name')) or client.get('id')}\n"
        f"Marca: {_safe_str(client.get('brand'))}\n"
        f"Vertical: {tpl.get('label', vertical)}\n"
        f"Oferta / scoring hint: {_safe_str(tpl.get('scoring_hint'))}\n"
        f"Notas: {_safe_str(client.get('notes'))}"
    )


PAIN_EXTRACTION_SYSTEM = """Eres un estratega de outbound B2B en Argentina.
Analizás el texto de un sitio web de un lead y detectás dolores/oportunidades
reales para un email frío personalizado.

Reglas:
- Usá SOLO evidencia del texto del sitio. No inventes datos, métricas ni stack.
- Si el texto es pobre, decilo y proponé ángulos prudentes (presencia digital, captura de leads, reputación).
- Escribí en español rioplatense, tono consultivo, sin humo.
- El icebreaker debe sonar humano (1-2 oraciones), mencionando algo concreto del sitio.

Respondé ÚNICAMENTE JSON válido (sin markdown) con esta forma:
{
  "website_summary": "resumen del negocio en 1-2 oraciones",
  "dolores": ["dolor 1", "dolor 2", "dolor 3"],
  "angulo_email": "ángulo comercial concreto para el outreach",
  "icebreaker": "apertura de email personalizada 1-2 oraciones"
}
"""


def _heuristic_pain_from_text(lead: dict[str, Any], site: dict[str, Any]) -> dict[str, str]:
    """Fallback local si no hay Anthropic o falla la API."""
    nombre = _safe_str(lead.get("nombre")) or "tu equipo"
    rubro = _safe_str(lead.get("rubro")) or "tu rubro"
    ubicacion = _safe_str(lead.get("ubicacion")) or "la zona"
    text = (site.get("text") or "").lower()
    title = _safe_str(site.get("title"))

    signals: list[str] = []
    if "whatsapp" in text or "wa.me" in text:
        signals.append("Dependencia alta de WhatsApp (poca captura/automatización de leads).")
    if "instagram" in text and "formulario" not in text:
        signals.append("Presencia social visible, pero sin funnel claro en el sitio.")
    if any(k in text for k in ("turno", "reserva", "cita", "agenda")):
        signals.append("Agenda/turnos manuales: oportunidad de automatizar captación.")
    if any(k in text for k in ("delivery", "envíos", "pedidos ya", "rappi")):
        signals.append("Canal delivery activo: reputación y presencia local importan.")
    if not signals:
        signals = [
            f"Sitio de {rubro} con poca claridad de propuesta o CTA.",
            "Oportunidad de mejorar presencia local / captación de consultas.",
            "Email frío puede anclarse en diferenciación frente a competidores de la zona.",
        ]

    summary = (
        f"{title or nombre}: negocio de {rubro} en {ubicacion}. "
        f"Se extrajo texto de {site.get('pages_ok', 0)} página(s)."
        if site.get("ok")
        else f"{nombre}: sin contenido web usable ({site.get('error') or 'sin_website'})."
    )
    angulo = (
        f"Ofrecer ayuda concreta para captar más consultas de {rubro} en {ubicacion} "
        f"mejorando presencia digital y seguimiento."
    )
    if site.get("ok") and title:
        ice = (
            f"Vi el sitio de {nombre} ({title}) y me llamó la atención cómo presentan "
            f"{rubro} en {ubicacion}. ¿Les suma charlar una idea corta para traer más consultas?"
        )
    else:
        ice = (
            f"Hola equipo de {nombre}: estoy mapeando {rubro} en {ubicacion} y creo que "
            f"pueden destacar más frente a clientes locales. ¿Les sirve una idea breve?"
        )
    return {
        "website_summary": summary,
        "dolores": " | ".join(signals[:3]),
        "angulo_email": angulo,
        "icebreaker": ice,
        "scrape_status": "ok_heuristica" if site.get("ok") else f"fallback:{site.get('error') or 'sin_web'}",
        "scrape_fuente": _safe_str(site.get("fuente_urls")) or "heuristica",
    }


def extract_pains_with_claude(
    lead: dict[str, Any],
    site: dict[str, Any],
    api_key: str,
    model: str = "claude-3-5-haiku-20241022",
) -> dict[str, str]:
    """Usa Claude para resumir el sitio y proponer dolores + icebreaker."""
    if not api_key:
        return _heuristic_pain_from_text(lead, site)
    if not site.get("ok"):
        return _heuristic_pain_from_text(lead, site)

    user_content = (
        f"{_client_offer_context()}\n\n"
        f"Lead:\n{json.dumps({k: lead.get(k) for k in ('nombre','rubro','ubicacion','website','email','telefono')}, ensure_ascii=False, indent=2)}\n\n"
        f"Título detectado: {site.get('title')}\n"
        f"URLs leídas: {site.get('fuente_urls')}\n\n"
        f"Texto del sitio:\n{site.get('text')}"
    )
    url = "https://api.anthropic.com/v1/messages"
    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "Content-Type": "application/json",
    }
    body = {
        "model": model,
        "max_tokens": 900,
        "temperature": 0.3,
        "system": PAIN_EXTRACTION_SYSTEM,
        "messages": [{"role": "user", "content": user_content}],
    }
    try:
        resp = requests.post(url, headers=headers, json=body, timeout=90)
        resp.raise_for_status()
        data = resp.json()
        parts = data.get("content", [])
        text = "".join(p.get("text", "") for p in parts if p.get("type") == "text")
        parsed = _extract_json_object(text)
        if not parsed:
            out = _heuristic_pain_from_text(lead, site)
            out["scrape_status"] = "claude_parse_fallback"
            return out
        dolores = parsed.get("dolores") or []
        if isinstance(dolores, list):
            dolores_txt = " | ".join(_safe_str(x) for x in dolores if _safe_str(x))
        else:
            dolores_txt = _safe_str(dolores)
        return {
            "website_summary": _safe_str(parsed.get("website_summary")),
            "dolores": dolores_txt,
            "angulo_email": _safe_str(parsed.get("angulo_email")),
            "icebreaker": _safe_str(parsed.get("icebreaker")),
            "scrape_status": "ok_claude",
            "scrape_fuente": _safe_str(site.get("fuente_urls")),
        }
    except Exception as exc:  # noqa: BLE001
        out = _heuristic_pain_from_text(lead, site)
        out["scrape_status"] = f"claude_error_fallback:{exc}"
        return out


def research_one_lead(
    lead: dict[str, Any],
    anthropic_key: str,
    model: str = "claude-3-5-haiku-20241022",
    use_claude: bool = True,
) -> dict[str, Any]:
    """Scrape del website + extracción de dolores (Claude o heurística)."""
    out = dict(lead)
    website = _safe_str(lead.get("website"))
    site = fetch_website_text(website) if website else {
        "ok": False,
        "text": "",
        "title": "",
        "pages_ok": 0,
        "error": "sin_website",
        "fuente_urls": "",
    }
    if use_claude and anthropic_key and site.get("ok"):
        pains = extract_pains_with_claude(out, site, anthropic_key, model)
    else:
        pains = _heuristic_pain_from_text(out, site)
        if not anthropic_key and site.get("ok"):
            pains["scrape_status"] = "ok_heuristica_sin_claude"
    out.update(pains)
    return out


def research_leads_batch(
    leads_df: pd.DataFrame,
    anthropic_key: str,
    model: str,
    use_claude: bool,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    total = max(len(leads_df), 1)
    progress = st.progress(0.0, text="Scrapeando sitios y extrayendo dolores…")
    for idx, (_, row) in enumerate(leads_df.iterrows()):
        rows.append(
            research_one_lead(row.to_dict(), anthropic_key, model, use_claude=use_claude)
        )
        progress.progress(
            (idx + 1) / total,
            text=f"Research {idx + 1}/{len(leads_df)}",
        )
    progress.empty()
    return pd.DataFrame(rows)


def upsert_researched_lead(lead: dict[str, Any]) -> None:
    """Actualiza researched_leads (+ sync enriched/sourced)."""
    lid = _safe_str(lead.get("id"))
    researched = st.session_state.get("researched_leads", pd.DataFrame())
    row = pd.DataFrame([lead])
    if not isinstance(researched, pd.DataFrame) or researched.empty:
        st.session_state.researched_leads = row
    else:
        if lid and (researched["id"].astype(str) == lid).any():
            researched = researched[researched["id"].astype(str) != lid]
        st.session_state.researched_leads = pd.concat([researched, row], ignore_index=True)

    # Sync enriched + sourced so downstream steps see dolores/icebreaker
    for key in ("enriched_leads", "sourced_leads"):
        df = st.session_state.get(key, pd.DataFrame())
        if isinstance(df, pd.DataFrame) and not df.empty and lid:
            for col, val in lead.items():
                if col == "seleccionado":
                    continue
                if col not in df.columns:
                    df[col] = ""
                df.loc[df["id"].astype(str) == lid, col] = val
            st.session_state[key] = df


# ---------------------------------------------------------------------------
# Integración: OpenAI / Anthropic — Lead Scoring B2B
# ---------------------------------------------------------------------------
SCORING_SYSTEM_PROMPT = """Eres un analista senior de calificación de leads B2B para Katem
(estudio digital, katem.com.ar) y Guía Pilar (directorio/plataforma local, guia-pilar.com).

Evalúa si el negocio es un cliente ideal para:
1) Presencia en un directorio B2B/local de alta calidad (Guía Pilar), y/o
2) Servicios digitales / marketing / automatización del estudio Katem.

Si el lead trae website_summary / dolores / angulo_email (research del sitio),
PRIORIZÁ esa evidencia para score_razon e icebreaker (no inventes).

Responde ÚNICAMENTE con JSON válido (sin markdown) con esta forma exacta:
{
  "lead_score": "High" | "Medium" | "Low",
  "score_razon": "justificación breve en español (máx 2 oraciones)",
  "icebreaker": "línea de apertura personalizada en español, 1-2 oraciones, basada en rubro/web/ubicación"
}

Criterios High: negocio activo, con web o teléfono, rating >= 4 o presencia clara, encaje B2B/local.
Criterios Medium: potencial parcial, datos incompletos o encaje dudoso.
Criterios Low: poco encaje, datos muy pobres, o negocio cerrado/irrelevante.
"""


def _heuristic_score(lead: dict[str, Any]) -> dict[str, str]:
    """Scoring local defensivo si no hay API de IA disponible."""
    website = _safe_str(lead.get("website"))
    telefono = _safe_str(lead.get("telefono"))
    rating_raw = _safe_str(lead.get("rating"))
    nombre = _safe_str(lead.get("nombre")) or "tu negocio"
    rubro = _safe_str(lead.get("rubro")) or "tu rubro"
    ubicacion = _safe_str(lead.get("ubicacion")) or "la zona"

    try:
        rating = float(rating_raw) if rating_raw else 0.0
    except ValueError:
        rating = 0.0

    score = 0
    if website:
        score += 2
    if telefono:
        score += 1
    if rating >= 4.5:
        score += 2
    elif rating >= 4.0:
        score += 1
    if lead.get("status_places") == "OPERATIONAL":
        score += 1

    if score >= 5:
        lead_score = "High"
        razon = (
            f"Negocio operativo con buena presencia digital "
            f"({'web + ' if website else ''}rating {rating_raw or 'N/D'}) "
            f"y encaje claro para directorio/servicio B2B en {ubicacion}."
        )
    elif score >= 3:
        lead_score = "Medium"
        razon = (
            "Hay señales de potencial, pero faltan datos o el encaje "
            "con Guía Pilar / Katem es parcial."
        )
    else:
        lead_score = "Low"
        razon = (
            "Datos incompletos o señales débiles de encaje B2B; "
            "prioridad baja para outreach frío."
        )

    if website:
        icebreaker = (
            f"Vi el sitio de {nombre} y me llamó la atención cómo trabajan "
            f"{rubro} en {ubicacion}. En Guía Pilar estamos sumando referentes "
            f"del rubro — ¿te interesa que te cuente cómo apareceis frente a clientes locales?"
        )
    else:
        icebreaker = (
            f"Hola equipo de {nombre}: estoy armando el mapa de {rubro} en {ubicacion} "
            f"para Guía Pilar y Katem. Creo que pueden destacar mucho frente a clientes "
            f"que buscan proveedores confiables — ¿charlamos 10 minutos?"
        )

    prior_ice = _safe_str(lead.get("icebreaker"))
    if prior_ice:
        icebreaker = prior_ice
    elif _safe_str(lead.get("dolores")):
        first_pain = _safe_str(lead.get("dolores")).split("|")[0].strip()
        icebreaker = (
            f"Vi el sitio de {nombre} y me quedó resonando esto: {first_pain}. "
            f"¿Les suma charlar una idea corta para {ubicacion}?"
        )

    return {
        "lead_score": lead_score,
        "score_razon": razon,
        "icebreaker": icebreaker,
    }


def _extract_json_object(text: str) -> dict[str, Any] | None:
    text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{[\s\S]*\}", text)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                return None
    return None


def score_lead_openai(lead: dict[str, Any], api_key: str, model: str = "gpt-4o") -> dict[str, str]:
    user_content = (
        "Califica este lead B2B:\n"
        f"{json.dumps(lead, ensure_ascii=False, indent=2)}"
    )
    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    body = {
        "model": model,
        "temperature": 0.3,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": SCORING_SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
    }
    resp = requests.post(url, headers=headers, json=body, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    content = data["choices"][0]["message"]["content"]
    parsed = _extract_json_object(content)
    if not parsed:
        raise ValueError(f"Respuesta OpenAI no parseable: {content[:300]}")
    return {
        "lead_score": _safe_str(parsed.get("lead_score")) or "Medium",
        "score_razon": _safe_str(parsed.get("score_razon")),
        "icebreaker": _safe_str(parsed.get("icebreaker")),
    }


def score_lead_anthropic(
    lead: dict[str, Any],
    api_key: str,
    model: str = "claude-3-5-sonnet-20241022",
) -> dict[str, str]:
    user_content = (
        "Califica este lead B2B y responde solo JSON:\n"
        f"{json.dumps(lead, ensure_ascii=False, indent=2)}"
    )
    url = "https://api.anthropic.com/v1/messages"
    headers = {
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01",
        "Content-Type": "application/json",
    }
    body = {
        "model": model,
        "max_tokens": 800,
        "temperature": 0.3,
        "system": SCORING_SYSTEM_PROMPT,
        "messages": [{"role": "user", "content": user_content}],
    }
    resp = requests.post(url, headers=headers, json=body, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    parts = data.get("content", [])
    text = "".join(p.get("text", "") for p in parts if p.get("type") == "text")
    parsed = _extract_json_object(text)
    if not parsed:
        raise ValueError(f"Respuesta Anthropic no parseable: {text[:300]}")
    return {
        "lead_score": _safe_str(parsed.get("lead_score")) or "Medium",
        "score_razon": _safe_str(parsed.get("score_razon")),
        "icebreaker": _safe_str(parsed.get("icebreaker")),
    }


def score_one_lead(
    lead: dict[str, Any],
    provider: str,
    openai_key: str,
    anthropic_key: str,
    openai_model: str,
    anthropic_model: str,
) -> dict[str, Any]:
    """Califica un único lead (modo supervisado o lote)."""
    try:
        if provider == "OpenAI (GPT-4o)" and openai_key:
            scored = score_lead_openai(lead, openai_key, openai_model)
            scored["scoring_fuente"] = "openai"
        elif provider == "Anthropic (Claude)" and anthropic_key:
            scored = score_lead_anthropic(lead, anthropic_key, anthropic_model)
            scored["scoring_fuente"] = "anthropic"
        else:
            scored = _heuristic_score(lead)
            scored["scoring_fuente"] = "heuristica_local"
    except Exception as exc:  # noqa: BLE001
        scored = _heuristic_score(lead)
        scored["scoring_fuente"] = f"fallback_error:{exc}"
        scored["score_razon"] = (
            f"[Fallback local] {scored['score_razon']} "
            f"(API error: {exc})"
        )

    merged = {**lead, **scored}
    score_norm = _safe_str(merged.get("lead_score")).title()
    if score_norm not in {"High", "Medium", "Low"}:
        score_norm = "Medium"
    merged["lead_score"] = score_norm
    return merged


def score_leads_batch(
    leads_df: pd.DataFrame,
    provider: str,
    openai_key: str,
    anthropic_key: str,
    openai_model: str,
    anthropic_model: str,
) -> pd.DataFrame:
    results = []
    progress = st.progress(0.0, text="Calificando leads…")
    total = len(leads_df)

    for idx, (_, row) in enumerate(leads_df.iterrows()):
        merged = score_one_lead(
            row.to_dict(),
            provider,
            openai_key,
            anthropic_key,
            openai_model,
            anthropic_model,
        )
        results.append(merged)
        progress.progress((idx + 1) / total, text=f"Calificados {idx + 1}/{total}")

    progress.empty()
    return pd.DataFrame(results)


# ---------------------------------------------------------------------------
# Integración: Instantly / Webhook outbound
# ---------------------------------------------------------------------------
def build_outbound_payload(lead: dict[str, Any], campaign_id: str) -> dict[str, Any]:
    email_guess = ""
    website = _safe_str(lead.get("website"))
    if website:
        domain = re.sub(r"^https?://(www\.)?", "", website).split("/")[0]
        if domain:
            email_guess = f"contacto@{domain}"

    return {
        "email": email_guess or f"lead-{lead.get('id', 'x')}@placeholder.local",
        "first_name": _safe_str(lead.get("nombre"))[:80] or "Negocio",
        "company_name": _safe_str(lead.get("nombre")),
        "phone": _safe_str(lead.get("telefono")),
        "website": website,
        "custom_variables": {
            "icebreaker": _safe_str(lead.get("icebreaker")),
            "lead_score": _safe_str(lead.get("lead_score")),
            "score_razon": _safe_str(lead.get("score_razon")),
            "website_summary": _safe_str(lead.get("website_summary")),
            "dolores": _safe_str(lead.get("dolores")),
            "angulo_email": _safe_str(lead.get("angulo_email")),
            "direccion": _safe_str(lead.get("direccion")),
            "rubro": _safe_str(lead.get("rubro")),
            "ubicacion": _safe_str(lead.get("ubicacion")),
            "rating": _safe_str(lead.get("rating")),
            "calendario_url": _safe_str(lead.get("calendario_url")) or DEFAULT_CALENDAR_URL,
        },
        "campaign": campaign_id or None,
        "source": "katem_sdr_autonomo",
        "lead_id": _safe_str(lead.get("id")),
    }


def dispatch_to_instantly(
    lead: dict[str, Any],
    api_key: str,
    campaign_id: str,
) -> tuple[bool, int, str]:
    """
    Empuja lead a Instantly.ai (API leads).
    Documentación típica: POST https://api.instantly.ai/api/v1/lead/add
    o v2 /api/v2/leads — se intenta v2 y fallback v1.
    """
    payload = build_outbound_payload(lead, campaign_id)
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    # Intento API v2
    v2_body = {
        "email": payload["email"],
        "first_name": payload["first_name"],
        "company_name": payload["company_name"],
        "phone": payload["phone"],
        "website": payload["website"],
        "custom_variables": payload["custom_variables"],
    }
    if campaign_id:
        v2_body["campaign"] = campaign_id

    try:
        resp = httpx.post(
            "https://api.instantly.ai/api/v2/leads",
            headers=headers,
            json=v2_body,
            timeout=30.0,
        )
        if resp.status_code in {200, 201}:
            return True, resp.status_code, resp.text[:500]

        # Fallback v1 con api_key en query
        v1_params = {"api_key": api_key}
        v1_body = {
            "email": payload["email"],
            "first_name": payload["first_name"],
            "company_name": payload["company_name"],
            "personalization": payload["custom_variables"].get("icebreaker", ""),
            "campaign_id": campaign_id or "",
            "website": payload["website"],
            "phone": payload["phone"],
        }
        resp_v1 = httpx.post(
            "https://api.instantly.ai/api/v1/lead/add",
            params=v1_params,
            json=v1_body,
            timeout=30.0,
        )
        ok = resp_v1.status_code in {200, 201}
        detail = (
            f"v2={resp.status_code}:{resp.text[:200]} | "
            f"v1={resp_v1.status_code}:{resp_v1.text[:200]}"
        )
        return ok, resp_v1.status_code, detail
    except Exception as exc:  # noqa: BLE001
        return False, 0, str(exc)


def dispatch_to_webhook(
    lead: dict[str, Any],
    webhook_url: str,
    campaign_id: str,
) -> tuple[bool, int, str]:
    payload = build_outbound_payload(lead, campaign_id)
    payload["event"] = "cold_campaign_dispatch"
    payload["timestamp"] = _utc_now_iso()
    try:
        resp = httpx.post(webhook_url, json=payload, timeout=30.0)
        ok = 200 <= resp.status_code < 300
        return ok, resp.status_code, resp.text[:500]
    except Exception as exc:  # noqa: BLE001
        return False, 0, str(exc)


def dispatch_one_lead(
    lead: dict[str, Any],
    mode: str,
    instantly_key: str,
    webhook_url: str,
    campaign_id: str,
    calendar_url: str,
    crm: pd.DataFrame | None = None,
) -> tuple[dict[str, Any], pd.DataFrame]:
    """Despacha un único lead y actualiza CRM. Retorna (log_row, crm)."""
    if crm is None:
        crm = load_crm()

    lead = dict(lead)
    lead["calendario_url"] = calendar_url or DEFAULT_CALENDAR_URL

    if mode == "Instantly.ai" and instantly_key:
        ok, status, detail = dispatch_to_instantly(lead, instantly_key, campaign_id)
        destino = "instantly"
    elif mode == "Webhook (Make/n8n)" and webhook_url:
        ok, status, detail = dispatch_to_webhook(lead, webhook_url, campaign_id)
        destino = "webhook"
    else:
        ok, status, detail = False, 0, "Falta API key o URL de webhook"
        destino = mode.lower()

    log_row = {
        "timestamp": _utc_now_iso(),
        "lead_id": _safe_str(lead.get("id")),
        "nombre": _safe_str(lead.get("nombre")),
        "destino": destino,
        "http_status": str(status),
        "exito": "sí" if ok else "no",
        "detalle": detail,
    }

    now = _utc_now_iso()
    crm_row = {
        "id": _safe_str(lead.get("id")) or str(uuid.uuid4())[:8],
        "nombre": _safe_str(lead.get("nombre")),
        "direccion": _safe_str(lead.get("direccion")),
        "telefono": _safe_str(lead.get("telefono")),
        "website": _safe_str(lead.get("website")),
        "rating": _safe_str(lead.get("rating")),
        "status_places": _safe_str(lead.get("status_places")),
        "rubro": _safe_str(lead.get("rubro")),
        "ubicacion": _safe_str(lead.get("ubicacion")),
        "lead_score": _safe_str(lead.get("lead_score")),
        "score_razon": _safe_str(lead.get("score_razon")),
        "icebreaker": _safe_str(lead.get("icebreaker")),
        "pipeline_status": "Contactado" if ok else "Nuevo",
        "calendario_url": calendar_url or DEFAULT_CALENDAR_URL,
        "notas": f"Despacho {destino}: {'OK' if ok else 'FALLÓ'} — {detail[:120]}",
        "fecha_creacion": now,
        "fecha_actualizacion": now,
    }

    if not crm.empty and (crm["id"] == crm_row["id"]).any():
        mask = crm["id"] == crm_row["id"]
        for k, v in crm_row.items():
            if k == "fecha_creacion":
                continue
            crm.loc[mask, k] = v
    else:
        crm = pd.concat([crm, pd.DataFrame([crm_row])], ignore_index=True)

    return log_row, crm


def dispatch_high_leads(
    leads_df: pd.DataFrame,
    mode: str,
    instantly_key: str,
    webhook_url: str,
    campaign_id: str,
    calendar_url: str,
) -> list[dict[str, Any]]:
    log_rows: list[dict[str, Any]] = []
    crm = load_crm()
    progress = st.progress(0.0, text="Despachando leads…")
    total = max(len(leads_df), 1)

    for idx, (_, row) in enumerate(leads_df.iterrows()):
        log_row, crm = dispatch_one_lead(
            row.to_dict(),
            mode,
            instantly_key,
            webhook_url,
            campaign_id,
            calendar_url,
            crm=crm,
        )
        log_rows.append(log_row)
        progress.progress((idx + 1) / total, text=f"Enviados {idx + 1}/{len(leads_df)}")

    progress.empty()
    save_crm(crm)
    append_dispatch_log(log_rows)
    return log_rows


# ---------------------------------------------------------------------------
# Export helpers
# ---------------------------------------------------------------------------
def dataframe_to_excel_bytes(df: pd.DataFrame) -> bytes:
    buffer = BytesIO()
    try:
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="CRM")
        return buffer.getvalue()
    except Exception:
        # Fallback CSV bytes si openpyxl no está
        return df.to_csv(index=False).encode("utf-8")


# ---------------------------------------------------------------------------
# UI — Sidebar config
# ---------------------------------------------------------------------------

def render_sidebar() -> dict[str, str]:
    st.sidebar.markdown("### 🏢 Cliente / Workspace")
    clients = list_clients()
    labels = {c["id"]: f"{c.get('name', c['id'])} · {VERTICAL_TEMPLATES.get(c.get('vertical',''), {}).get('label', c.get('vertical',''))}" for c in clients}
    ids = list(labels.keys())
    current = st.session_state.get("active_client_id", ids[0] if ids else "katem-demo")
    if current not in ids and ids:
        current = ids[0]
    selected = st.sidebar.selectbox(
        "Cliente activo",
        options=ids,
        index=ids.index(current) if current in ids else 0,
        format_func=lambda i: labels.get(i, i),
        key="sidebar_client_select",
    )
    if selected != st.session_state.get("active_client_id"):
        st.session_state.active_client_id = selected
        reset_pipeline_state_for_client()
        st.rerun()

    client = get_client(selected)
    vertical = client.get("vertical", "b2b_servicios")
    st.sidebar.caption(f"Vertical: **{VERTICAL_TEMPLATES.get(vertical, {}).get('label', vertical)}**")
    st.sidebar.caption(client.get("notes", ""))

    with st.sidebar.expander("➕ Nuevo cliente"):
        new_id = st.text_input("ID (slug)", value="", placeholder="cliente-acme")
        new_name = st.text_input("Nombre comercial", value="")
        new_vertical = st.selectbox(
            "Template de vertical",
            options=list(VERTICAL_TEMPLATES.keys()),
            format_func=lambda k: VERTICAL_TEMPLATES[k]["label"],
        )
        new_cal = st.text_input("Calendario", value=DEFAULT_CALENDAR_URL)
        if st.button("Crear workspace", use_container_width=True):
            slug = re.sub(r"[^a-z0-9\-]+", "-", (new_id or new_name).lower()).strip("-")
            if not slug or not new_name.strip():
                st.error("Completá ID y nombre.")
            elif (CLIENTS_DIR / slug / "client.json").exists():
                st.error("Ese ID ya existe.")
            else:
                save_client(
                    {
                        "id": slug,
                        "name": new_name.strip(),
                        "vertical": new_vertical,
                        "calendar_url": new_cal.strip() or DEFAULT_CALENDAR_URL,
                        "brand": new_name.strip(),
                        "notes": f"Workspace creado desde panel Katem · vertical {new_vertical}",
                    }
                )
                st.session_state.active_client_id = slug
                reset_pipeline_state_for_client()
                st.success(f"Cliente `{slug}` creado.")
                st.rerun()

    st.sidebar.divider()
    st.sidebar.markdown("### ⚙️ APIs")
    st.sidebar.caption("Keys desde `.env` (override de sesión).")

    st.sidebar.markdown("#### Sourcing")
    google_key = st.sidebar.text_input(
        "Google Maps / Places Key",
        value=env_or_secret("GOOGLE_MAPS_KEY"),
        type="password",
    )
    apollo_key = st.sidebar.text_input(
        "Apollo.io API Key",
        value=env_or_secret("APOLLO_API_KEY"),
        type="password",
    )
    clay_key = st.sidebar.text_input(
        "Clay API Key",
        value=env_or_secret("CLAY_API_KEY"),
        type="password",
    )
    clay_webhook_url = st.sidebar.text_input(
        "Clay Webhook URL (sourcing)",
        value=env_or_secret("CLAY_WEBHOOK_URL"),
        help="Webhook Clay para buscar/traer leads.",
    )
    serpapi_key = st.sidebar.text_input(
        "SerpAPI Key (Google Maps)",
        value=env_or_secret("SERPAPI_KEY"),
        type="password",
    )
    outscraper_key = st.sidebar.text_input(
        "Outscraper API Key",
        value=env_or_secret("OUTSCRAPER_API_KEY"),
        type="password",
    )
    brightdata_token = st.sidebar.text_input(
        "Bright Data Token",
        value=env_or_secret("BRIGHTDATA_TOKEN"),
        type="password",
    )
    brightdata_maps_url = st.sidebar.text_input(
        "Bright Data Maps URL (collector)",
        value=env_or_secret("BRIGHTDATA_MAPS_URL"),
        help="Endpoint HTTP del collector/dataset de Maps.",
    )
    meta_token = st.sidebar.text_input(
        "Meta Graph Token (Facebook/Instagram pages)",
        value=env_or_secret("META_ACCESS_TOKEN"),
        type="password",
        help="Token para pages/search. Sin token → simulación social.",
    )
    directorios_ar_webhook = st.sidebar.text_input(
        "Webhook Directorios AR (Make/n8n/scraper)",
        value=env_or_secret("DIRECTORIOS_AR_WEBHOOK_URL"),
        help="POST {directorio, nicho, ubicacion, cantidad} → JSON leads.",
    )
    directorios_ar_key = st.sidebar.text_input(
        "Directorios AR webhook key (opcional)",
        value=env_or_secret("DIRECTORIOS_AR_KEY"),
        type="password",
    )

    st.sidebar.markdown("#### Enrichment")
    hunter_key = st.sidebar.text_input(
        "Hunter.io API Key",
        value=env_or_secret("HUNTER_API_KEY"),
        type="password",
        help="Domain search para emails. Sin key → heurística info@dominio.",
    )
    snov_key = st.sidebar.text_input(
        "Snov.io API Key / token",
        value=env_or_secret("SNOV_API_KEY"),
        type="password",
    )
    clay_linkedin_webhook = st.sidebar.text_input(
        "Clay Webhook URL (LinkedIn enrich)",
        value=env_or_secret("CLAY_LINKEDIN_WEBHOOK_URL") or env_or_secret("CLAY_WEBHOOK_URL"),
        help="Webhook Clay con action=enrich_linkedin. Sin URL → LinkedIn heurístico.",
    )

    st.sidebar.markdown("#### Scoring / Outbound")
    openai_key = st.sidebar.text_input("OpenAI API Key", value=env_or_secret("OPENAI_API_KEY"), type="password")
    anthropic_key = st.sidebar.text_input("Anthropic API Key", value=env_or_secret("ANTHROPIC_API_KEY"), type="password")
    instantly_key = st.sidebar.text_input(
        "Instantly / Smartlead API Key",
        value=env_or_secret("INSTANTLY_API_KEY") or env_or_secret("SMARTLEAD_API_KEY"),
        type="password",
    )
    webhook_url = st.sidebar.text_input("Webhook Make / n8n", value=env_or_secret("WEBHOOK_URL"))
    campaign_id = st.sidebar.text_input("Campaign ID", value=env_or_secret("INSTANTLY_CAMPAIGN_ID"))
    calendar_url = st.sidebar.text_input(
        "Cal.com / Calendly",
        value=client.get("calendar_url") or env_or_secret("CALENDAR_URL", DEFAULT_CALENDAR_URL),
    )

    st.sidebar.divider()
    st.sidebar.markdown("**Katem** · vertical SDR Autónomo")
    st.sidebar.caption("Multi-cliente · Guía Pilar es un workspace, no el producto.")

    return {
        "google_key": google_key.strip(),
        "apollo_key": apollo_key.strip(),
        "clay_key": clay_key.strip(),
        "clay_webhook_url": clay_webhook_url.strip(),
        "serpapi_key": serpapi_key.strip(),
        "outscraper_key": outscraper_key.strip(),
        "brightdata_token": brightdata_token.strip(),
        "brightdata_maps_url": brightdata_maps_url.strip(),
        "meta_token": meta_token.strip(),
        "directorios_ar_webhook": directorios_ar_webhook.strip(),
        "directorios_ar_key": directorios_ar_key.strip(),
        "directorio_ar": "",  # se setea en el form de sourcing
        "hunter_key": hunter_key.strip(),
        "snov_key": snov_key.strip(),
        "clay_linkedin_webhook": clay_linkedin_webhook.strip(),
        "openai_key": openai_key.strip(),
        "anthropic_key": anthropic_key.strip(),
        "instantly_key": instantly_key.strip(),
        "webhook_url": webhook_url.strip(),
        "campaign_id": campaign_id.strip(),
        "calendar_url": calendar_url.strip() or DEFAULT_CALENDAR_URL,
        "client_id": selected,
        "client_name": _safe_str(client.get("name")),
        "vertical": vertical,
    }



# ---------------------------------------------------------------------------
# UI — Tabs
# ---------------------------------------------------------------------------

def get_selected_sourced_leads() -> pd.DataFrame:
    sourced = st.session_state.sourced_leads
    if not isinstance(sourced, pd.DataFrame) or sourced.empty:
        return pd.DataFrame()
    if "seleccionado" in sourced.columns and sourced["seleccionado"].any():
        return sourced[sourced["seleccionado"] == True].copy()  # noqa: E712
    return sourced.copy()


def upsert_scored_lead(lead: dict[str, Any]) -> None:
    """Inserta o actualiza un lead calificado en sesión."""
    scored = st.session_state.scored_leads
    row = pd.DataFrame([lead])
    if not isinstance(scored, pd.DataFrame) or scored.empty:
        st.session_state.scored_leads = row
    else:
        lid = _safe_str(lead.get("id"))
        if lid and (scored["id"].astype(str) == lid).any():
            scored = scored[scored["id"].astype(str) != lid]
        st.session_state.scored_leads = pd.concat([scored, row], ignore_index=True)
    scored_all = st.session_state.scored_leads
    st.session_state.high_leads = scored_all[scored_all["lead_score"] == "High"].copy()



def render_pipeline_stepper() -> None:
    """Barra de progreso del pipeline supervisado multi-paso."""
    steps = [
        (1, "🔍 Sourcing"),
        (2, "✨ Enrich"),
        (3, "🌐 Web/Dolores"),
        (4, "🧠 Scoring"),
        (5, "🚀 Despacho"),
        (6, "📊 CRM"),
    ]
    current = int(st.session_state.get("pipeline_step", 1))
    cols = st.columns(6)
    for (num, label), col in zip(steps, cols):
        if num < current:
            col.success(f"✓ {label}")
        elif num == current:
            col.info(f"▶ {label}")
        else:
            col.caption(f"○ {label}")

    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        client = get_client(st.session_state.get("active_client_id", "katem-demo"))
        st.caption(
            f"Workspace **{client.get('name')}** · modo supervisado "
            "(cada etapa requiere aprobación explícita)."
        )
    with c2:
        if st.button("↺ Reiniciar pipeline", use_container_width=True):
            reset_pipeline_state_for_client()
            st.rerun()
    with c3:
        jump = st.selectbox(
            "Ir al paso",
            options=[1, 2, 3, 4, 5, 6],
            format_func=lambda n: steps[n - 1][1],
            index=min(max(current, 1), 6) - 1,
            label_visibility="collapsed",
        )
        if jump != current:
            st.session_state.pipeline_step = int(jump)
            st.rerun()
    st.divider()



def tab_sourcing(cfg: dict[str, str]) -> None:
    st.subheader("🔍 Paso 1 — Búsqueda de Leads (Sourcing)")
    st.write(
        "Elegí la fuente (Maps, Apollo, Clay, Instagram/Facebook o directorios AR), buscá por rubro/ubicación, "
        "revisá la tabla, seleccioná leads y **aprobá el paso** para avanzar."
    )

    with st.form("form_sourcing"):
        fuente = st.selectbox(
            "Fuente de extracción",
            [
                "Google Places",
                "SerpAPI Maps",
                "Outscraper Maps",
                "Bright Data Maps",
                "Apollo.io",
                "Clay (webhook)",
                "Instagram (Meta/local)",
                "Facebook Pages (Meta)",
                "Directorios AR",
                "Cuitonline",
                "GuiaBancos",
                "Páginas Amarillas",
                "Mercado Libre Servicios",
                "Doctoralia",
                "PedidosYa Partners",
            ],
            help=(
                "Maps: Places / SerpAPI / Outscraper / Bright Data. "
                "B2B: Apollo / Clay. Social: Instagram/Facebook. "
                "AR: Cuitonline, GuiaBancos, Páginas Amarillas, ML Servicios, Doctoralia, PedidosYa."
            ),
        )
        c1, c2, c3 = st.columns([2, 2, 1])
        with c1:
            nicho = st.text_input("Rubro / Nicho", value="estudios contables")
        with c2:
            ubicacion = st.text_input("Ubicación", value="Pilar")
        with c3:
            cantidad = st.number_input("Cantidad", min_value=1, max_value=60, value=8, step=1)

        directorio_ar = ""
        if fuente == "Directorios AR":
            directorio_ar = st.selectbox(
                "Directorio Argentina",
                DIRECTORIOS_AR,
                help="Sin webhook scraper se usa extracción simulada estructurada por directorio.",
            )

        if fuente.startswith("Clay"):
            st.caption("Clay webhook: `{nicho, ubicacion, cantidad}` → JSON leads.")
        elif fuente.startswith("Apollo"):
            st.caption("Apollo Organization Search. Sin key → simulación.")
        elif fuente.startswith("SerpAPI"):
            st.caption("Google Maps vía SerpAPI. Requiere `SERPAPI_KEY`.")
        elif fuente.startswith("Outscraper"):
            st.caption("Google Maps vía Outscraper. Requiere `OUTSCRAPER_API_KEY`.")
        elif fuente.startswith("Bright"):
            st.caption("Maps vía Bright Data collector URL + token.")
        elif "Instagram" in fuente or "Facebook" in fuente:
            st.caption("Meta Graph pages/search. Sin `META_ACCESS_TOKEN` → simulación social.")
        elif fuente == "Directorios AR" or fuente in DIRECTORIOS_AR:
            st.caption(
                "Directorios AR: webhook scraper opcional (`DIRECTORIOS_AR_WEBHOOK_URL`). "
                "Sin webhook → muestra estructurada por directorio."
            )
        else:
            st.caption("Google Places oficial. Sin key → simulación estructurada.")

        submitted = st.form_submit_button("Buscar leads", type="primary", use_container_width=True)

    if submitted:
        if not nicho.strip() or not ubicacion.strip():
            st.error("Completá rubro y ubicación.")
        else:
            cfg = dict(cfg)
            if directorio_ar:
                cfg["directorio_ar"] = directorio_ar
            with st.spinner(f"Consultando {fuente}…"):
                df, mode = search_leads(
                    fuente,
                    nicho.strip(),
                    ubicacion.strip(),
                    int(cantidad),
                    cfg,
                )
            # Asegurar columnas opcionales de Apollo/Clay
            for col in ("email", "linkedin", "fuente"):
                if col not in df.columns:
                    df[col] = ""
            st.session_state.sourced_leads = df
            st.session_state.step1_approved = False
            st.session_state.pipeline_step = 1
            st.session_state.last_search_meta = {
                "nicho": nicho,
                "ubicacion": ubicacion,
                "cantidad": int(cantidad),
                "source": fuente,
                "mode": mode,
                "at": _utc_now_iso(),
            }
            st.success(
                f"Se obtuvieron {len(df)} leads · fuente: `{fuente}` · modo: `{mode}` — "
                "revisalos antes de avanzar."
            )

    df = st.session_state.sourced_leads
    meta = st.session_state.last_search_meta

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Leads encontrados", len(df) if isinstance(df, pd.DataFrame) else 0)
    m2.metric("Con website", int(df["website"].astype(str).str.len().gt(0).sum()) if isinstance(df, pd.DataFrame) and not df.empty else 0)
    m3.metric("Con teléfono", int(df["telefono"].astype(str).str.len().gt(0).sum()) if isinstance(df, pd.DataFrame) and not df.empty else 0)
    m4.metric("Fuente", meta.get("mode", "—") if meta else "—")

    if isinstance(df, pd.DataFrame) and not df.empty:
        st.markdown("#### Resultados — seleccioná leads a procesar")
        editable = df.copy()
        if "seleccionado" not in editable.columns:
            editable["seleccionado"] = False
        editable["seleccionado"] = editable["seleccionado"].astype(bool)

        edited = st.data_editor(
            editable,
            use_container_width=True,
            hide_index=True,
            column_config={
                "seleccionado": st.column_config.CheckboxColumn("Seleccionar", default=False),
                "website": st.column_config.LinkColumn("Sitio Web"),
                "linkedin": st.column_config.LinkColumn("LinkedIn"),
                "rating": st.column_config.TextColumn("Rating / Señal"),
                "fuente": st.column_config.TextColumn("Fuente"),
                "email": st.column_config.TextColumn("Email"),
            },
            disabled=[c for c in editable.columns if c != "seleccionado"],
            key="editor_sourcing",
        )
        st.session_state.sourced_leads = edited

        csel1, csel2, csel3 = st.columns(3)
        with csel1:
            if st.button("Seleccionar todos", use_container_width=True):
                edited["seleccionado"] = True
                st.session_state.sourced_leads = edited
                st.rerun()
        with csel2:
            if st.button("Limpiar selección", use_container_width=True):
                edited["seleccionado"] = False
                st.session_state.sourced_leads = edited
                st.rerun()
        with csel3:
            selected_count = int(edited["seleccionado"].sum())
            st.info(f"{selected_count} lead(s) seleccionados")

        selected = edited[edited["seleccionado"] == True]  # noqa: E712
        st.session_state.selected_lead_ids = selected["id"].tolist() if not selected.empty else []

        st.download_button(
            "Descargar resultados CSV",
            data=edited.drop(columns=["seleccionado"], errors="ignore").to_csv(index=False),
            file_name=f"leads_{meta.get('ubicacion', 'zona')}_{meta.get('nicho', 'nicho')}.csv".replace(" ", "_"),
            mime="text/csv",
        )

        st.markdown("#### Puerta de aprobación — Paso 1 → Paso 2 (Enrichment)")
        n_sel = len(st.session_state.selected_lead_ids) or len(edited)
        confirm = st.checkbox(
            f"Revisé la lista y quiero calificar {n_sel} lead(s) en el siguiente paso",
            key="confirm_step1",
        )
        if st.button(
            "Aprobar selección y pasar a Enrichment →",
            type="primary",
            use_container_width=True,
            disabled=not confirm,
        ):
            ids = st.session_state.selected_lead_ids
            if not ids:
                # Si no marcó checkboxes, usa todos
                ids = edited["id"].astype(str).tolist()
                edited["seleccionado"] = True
                st.session_state.sourced_leads = edited
                st.session_state.selected_lead_ids = ids
            st.session_state.step1_approved = True
            st.session_state.step_enrich_approved = False
            st.session_state.pipeline_step = 2
            st.session_state.enrich_queue_ids = list(ids)
            st.session_state.enrich_queue_idx = 0
            st.session_state.scoring_queue_ids = list(ids)
            st.session_state.scoring_queue_idx = 0
            st.session_state.scoring_current_result = None
            # seed enriched with selected rows
            sourced = st.session_state.sourced_leads
            selected_df = sourced[sourced["id"].astype(str).isin([str(i) for i in ids])].copy()
            for col in ("email", "linkedin", "enrichment_fuente", "email_confidence"):
                if col not in selected_df.columns:
                    selected_df[col] = ""
            st.session_state.enriched_leads = selected_df.drop(columns=["seleccionado"], errors="ignore")
            st.success("Paso 1 aprobado. Continuá en Enrichment (email + LinkedIn).")
            st.rerun()

        if st.session_state.step1_approved:
            st.success("✓ Paso 1 aprobado — podés enriquecer en la pestaña Enrichment.")
    else:
        st.info("Todavía no hay leads. Completá el formulario y tocá **Buscar leads**.")



def tab_enrichment(cfg: dict[str, str]) -> None:
    st.subheader("✨ Paso 2 — Enrichment (Email + LinkedIn)")
    st.write(
        "Enriquecé los leads aprobados con **email** (Hunter/Snov/heurística) y "
        "**LinkedIn vía Clay**. Revisá uno a uno o en lote, y aprobá para pasar a scoring."
    )

    if not st.session_state.step1_approved:
        st.warning("Primero aprobá la selección en **Paso 1 (Sourcing)**.")
        if st.button("Ir a Sourcing", key="enrich_go_sourcing"):
            st.session_state.pipeline_step = 1
            st.rerun()
        return

    base = st.session_state.get("enriched_leads", pd.DataFrame())
    if not isinstance(base, pd.DataFrame) or base.empty:
        base = get_selected_sourced_leads().drop(columns=["seleccionado"], errors="ignore")
        for col in ("email", "linkedin", "enrichment_fuente", "email_confidence"):
            if col not in base.columns:
                base[col] = ""
        st.session_state.enriched_leads = base.copy()
        if not st.session_state.enrich_queue_ids and not base.empty:
            st.session_state.enrich_queue_ids = base["id"].astype(str).tolist()
            st.session_state.enrich_queue_idx = 0

    if base.empty:
        st.warning("No hay leads para enriquecer.")
        return

    c1, c2, c3 = st.columns(3)
    with c1:
        do_email = st.checkbox("Enriquecer email", value=True)
        email_provider = st.selectbox(
            "Proveedor email",
            ["Heurística local", "Hunter.io", "Snov.io"],
        )
    with c2:
        do_linkedin = st.checkbox("Enriquecer LinkedIn (Clay)", value=True)
        st.caption("Sin webhook Clay → URL company/search heurística.")
    with c3:
        mode = st.radio(
            "Modo",
            ["Uno a uno (supervisado)", "Lote completo (con confirmación)"],
            horizontal=False,
            key="enrich_mode_radio",
        )
    st.session_state.enrich_mode = mode
    st.metric("Leads en cola", len(base))

    email_key = cfg.get("hunter_key", "") if email_provider.startswith("Hunter") else cfg.get("snov_key", "")
    clay_li = cfg.get("clay_linkedin_webhook", "")
    clay_key = cfg.get("clay_key", "")

    # ---- Uno a uno ----
    if mode.startswith("Uno a uno"):
        queue = st.session_state.enrich_queue_ids or base["id"].astype(str).tolist()
        if not st.session_state.enrich_queue_ids:
            st.session_state.enrich_queue_ids = queue
            st.session_state.enrich_queue_idx = 0
        idx = int(st.session_state.enrich_queue_idx)
        total_q = len(st.session_state.enrich_queue_ids)
        if idx >= total_q:
            st.success(f"Cola de enrichment finalizada ({total_q}/{total_q}).")
        else:
            lead_id = str(st.session_state.enrich_queue_ids[idx])
            lead_row = base[base["id"].astype(str) == lead_id]
            if lead_row.empty:
                st.session_state.enrich_queue_idx = idx + 1
                st.rerun()
            lead = lead_row.iloc[0].to_dict()
            st.markdown(f"##### Lead {idx + 1} de {total_q}: **{_safe_str(lead.get('nombre'))}**")
            st.write(
                {
                    "website": lead.get("website"),
                    "email": lead.get("email"),
                    "linkedin": lead.get("linkedin"),
                    "telefono": lead.get("telefono"),
                }
            )
            b1, b2, b3 = st.columns(3)
            with b1:
                if st.button("Enriquecer este lead", type="primary", use_container_width=True, key="enrich_one_btn"):
                    enriched = enrich_one_lead(
                        lead,
                        email_provider,
                        email_key,
                        do_email,
                        do_linkedin,
                        clay_li,
                        clay_key,
                    )
                    upsert_enriched_lead(enriched)
                    st.success(
                        f"Email: `{enriched.get('email') or '—'}` · "
                        f"LinkedIn: `{enriched.get('linkedin') or '—'}`"
                    )
                    st.rerun()
            with b2:
                if st.button("Guardar y siguiente →", use_container_width=True, key="enrich_next_btn"):
                    st.session_state.enrich_queue_idx = idx + 1
                    st.rerun()
            with b3:
                if st.button("Omitir →", use_container_width=True, key="enrich_skip_btn"):
                    st.session_state.enrich_queue_idx = idx + 1
                    st.rerun()

            # editable fields
            edited_email = st.text_input("Email (editable)", value=_safe_str(lead.get("email")), key=f"enr_email_{lead_id}")
            edited_li = st.text_input("LinkedIn (editable)", value=_safe_str(lead.get("linkedin")), key=f"enr_li_{lead_id}")
            if st.button("Aplicar edición manual", use_container_width=True, key="enrich_manual_btn"):
                lead["email"] = edited_email.strip()
                lead["linkedin"] = edited_li.strip()
                upsert_enriched_lead(lead)
                st.success("Edición aplicada.")
                st.rerun()
    else:
        confirm = st.checkbox(
            f"Confirmo enriquecer en lote {len(base)} leads (email={do_email}, linkedin={do_linkedin})",
            key="confirm_enrich_batch",
        )
        if st.button("Enriquecer lote", type="primary", use_container_width=True, disabled=not confirm, key="enrich_batch_btn"):
            out = enrich_leads_batch(
                base,
                email_provider,
                email_key,
                do_email,
                do_linkedin,
                clay_li,
                clay_key,
            )
            st.session_state.enriched_leads = out
            # sync sourced
            for _, row in out.iterrows():
                upsert_enriched_lead(row.to_dict())
            st.session_state.enrich_queue_idx = len(base)
            st.success(f"Lote enriquecido: {len(out)} leads.")
            st.rerun()

    enriched = st.session_state.enriched_leads
    if isinstance(enriched, pd.DataFrame) and not enriched.empty:
        st.markdown("#### Resultado del enrichment")
        m1, m2, m3 = st.columns(3)
        m1.metric("Con email", int(enriched.get("email", pd.Series(dtype=str)).astype(str).str.len().gt(0).sum()) if "email" in enriched.columns else 0)
        m2.metric("Con LinkedIn", int(enriched.get("linkedin", pd.Series(dtype=str)).astype(str).str.len().gt(0).sum()) if "linkedin" in enriched.columns else 0)
        m3.metric("Total", len(enriched))
        st.dataframe(
            enriched[[c for c in ["nombre", "website", "email", "linkedin", "telefono", "enrichment_fuente", "fuente"] if c in enriched.columns]],
            use_container_width=True,
            hide_index=True,
        )

        st.markdown("#### Puerta de aprobación — Paso 2 → Paso 3 (Web / Dolores)")
        confirm2 = st.checkbox(
            "Revisé emails/LinkedIn y quiero scrapear sitios para personalizar emails",
            key="confirm_enrich_gate",
        )
        if st.button(
            "Aprobar enrichment y pasar a Web/Dolores →",
            type="primary",
            use_container_width=True,
            disabled=not confirm2,
        ):
            st.session_state.step_enrich_approved = True
            st.session_state.step_scrape_approved = False
            st.session_state.pipeline_step = 3
            st.session_state.scrape_queue_ids = enriched["id"].astype(str).tolist()
            st.session_state.scrape_queue_idx = 0
            st.session_state.scrape_current_result = None
            # seed researched with enriched rows
            st.session_state.researched_leads = enriched.copy()
            st.success("Enrichment aprobado. Continuá en Web / Dolores.")
            st.rerun()
        if st.session_state.step_enrich_approved:
            st.success("✓ Enrichment aprobado — podés investigir sitios en Web/Dolores.")
    else:
        st.info("Todavía no hay leads enriquecidos.")


def tab_research(cfg: dict[str, str]) -> None:
    st.subheader("🌐 Paso 3 — Scrape Web + Dolores (Claude)")
    st.write(
        "Scrapeá el sitio de cada lead, extráé dolores/ángulo con **Claude** "
        "(o heurística local) y aprobá antes de scoring. "
        "Claude no scrapea: lee el texto que bajamos del website."
    )

    if not st.session_state.step1_approved:
        st.warning("Primero aprobá la selección en **Paso 1 (Sourcing)**.")
        if st.button("Ir a Sourcing", key="research_go_sourcing"):
            st.session_state.pipeline_step = 1
            st.rerun()
        return
    if not st.session_state.step_enrich_approved:
        st.warning("Primero completá y aprobá **Paso 2 (Enrichment)**.")
        if st.button("Ir a Enrichment", key="research_go_enrich"):
            st.session_state.pipeline_step = 2
            st.rerun()
        return

    base = st.session_state.get("researched_leads", pd.DataFrame())
    if not isinstance(base, pd.DataFrame) or base.empty:
        enriched = st.session_state.get("enriched_leads", pd.DataFrame())
        if isinstance(enriched, pd.DataFrame) and not enriched.empty:
            base = enriched.copy()
            st.session_state.researched_leads = base
        else:
            base = get_selected_sourced_leads().drop(columns=["seleccionado"], errors="ignore")
            st.session_state.researched_leads = base.copy()
        if not st.session_state.scrape_queue_ids and not base.empty:
            st.session_state.scrape_queue_ids = base["id"].astype(str).tolist()
            st.session_state.scrape_queue_idx = 0

    if not isinstance(base, pd.DataFrame) or base.empty:
        st.warning("No hay leads para investigar.")
        return

    c1, c2, c3 = st.columns(3)
    with c1:
        use_claude = st.checkbox(
            "Usar Claude para dolores",
            value=bool(cfg.get("anthropic_key")),
            help="Sin ANTHROPIC_API_KEY → heurística local sobre el texto scrapeado.",
        )
    with c2:
        claude_model = st.text_input(
            "Modelo Claude (research)",
            value="claude-3-5-haiku-20241022",
            help="Haiku es barato para research; Sonnet si querés más calidad.",
        )
    with c3:
        mode = st.radio(
            "Modo",
            ["Uno a uno (supervisado)", "Lote completo (con confirmación)"],
            key="scrape_mode_radio",
        )
    st.session_state.scrape_mode = mode
    st.metric("Leads en cola", len(base))
    if use_claude and not cfg.get("anthropic_key"):
        st.info("Sin `ANTHROPIC_API_KEY`: se scrapeá el sitio y se usan dolores heurísticos.")

    # ---- Uno a uno ----
    if mode.startswith("Uno a uno"):
        queue = st.session_state.scrape_queue_ids or base["id"].astype(str).tolist()
        if not st.session_state.scrape_queue_ids:
            st.session_state.scrape_queue_ids = queue
            st.session_state.scrape_queue_idx = 0
        idx = int(st.session_state.scrape_queue_idx)
        total_q = len(st.session_state.scrape_queue_ids)
        if idx >= total_q:
            st.success(f"Cola de research finalizada ({total_q}/{total_q}).")
        else:
            lead_id = str(st.session_state.scrape_queue_ids[idx])
            lead_row = base[base["id"].astype(str) == lead_id]
            if lead_row.empty:
                st.session_state.scrape_queue_idx = idx + 1
                st.rerun()
            lead = lead_row.iloc[0].to_dict()
            st.markdown(f"##### Lead {idx + 1} de {total_q}: **{_safe_str(lead.get('nombre'))}**")
            st.write(
                {
                    "website": lead.get("website"),
                    "email": lead.get("email"),
                    "rubro": lead.get("rubro"),
                    "ubicacion": lead.get("ubicacion"),
                    "scrape_status": lead.get("scrape_status"),
                }
            )
            b1, b2, b3 = st.columns(3)
            with b1:
                if st.button(
                    "Scrape + dolores",
                    type="primary",
                    use_container_width=True,
                    key="research_one_btn",
                ):
                    result = research_one_lead(
                        lead,
                        cfg.get("anthropic_key", ""),
                        claude_model.strip() or "claude-3-5-haiku-20241022",
                        use_claude=use_claude,
                    )
                    st.session_state.scrape_current_result = result
                    upsert_researched_lead(result)
                    st.rerun()
            with b2:
                if st.button("Siguiente →", use_container_width=True, key="research_next_btn"):
                    st.session_state.scrape_current_result = None
                    st.session_state.scrape_queue_idx = idx + 1
                    st.rerun()
            with b3:
                if st.button("Omitir →", use_container_width=True, key="research_skip_btn"):
                    st.session_state.scrape_current_result = None
                    st.session_state.scrape_queue_idx = idx + 1
                    st.rerun()

            current = st.session_state.scrape_current_result
            if not current or _safe_str(current.get("id")) != lead_id:
                # show saved research if any
                current = lead if _safe_str(lead.get("scrape_status")) else None
            if current and _safe_str(current.get("id")) == lead_id and _safe_str(current.get("scrape_status")):
                st.markdown("###### Resultado — editá antes de seguir")
                st.caption(f"Status: `{_safe_str(current.get('scrape_status'))}` · Fuente: `{_safe_str(current.get('scrape_fuente'))}`")
                new_summary = st.text_area(
                    "Resumen del sitio",
                    value=_safe_str(current.get("website_summary")),
                    key=f"edit_sum_{lead_id}",
                )
                new_dolores = st.text_area(
                    "Dolores (separados por |)",
                    value=_safe_str(current.get("dolores")),
                    key=f"edit_dol_{lead_id}",
                )
                new_angulo = st.text_area(
                    "Ángulo email",
                    value=_safe_str(current.get("angulo_email")),
                    key=f"edit_ang_{lead_id}",
                )
                new_ice = st.text_area(
                    "Icebreaker",
                    value=_safe_str(current.get("icebreaker")),
                    key=f"edit_ice_res_{lead_id}",
                )
                if st.button("✓ Guardar edición y siguiente", type="primary", use_container_width=True):
                    current = dict(current)
                    current["website_summary"] = new_summary
                    current["dolores"] = new_dolores
                    current["angulo_email"] = new_angulo
                    current["icebreaker"] = new_ice
                    upsert_researched_lead(current)
                    st.session_state.scrape_current_result = None
                    st.session_state.scrape_queue_idx = idx + 1
                    st.rerun()
    else:
        confirm_batch = st.checkbox(
            f"Confirmo scrapear y analizar {len(base)} sitios (Claude={use_claude})",
            key="confirm_research_batch",
        )
        if st.button(
            "Research lote completo",
            type="primary",
            use_container_width=True,
            disabled=not confirm_batch,
            key="research_batch_btn",
        ):
            out = research_leads_batch(
                base,
                cfg.get("anthropic_key", ""),
                claude_model.strip() or "claude-3-5-haiku-20241022",
                use_claude,
            )
            st.session_state.researched_leads = out
            for _, row in out.iterrows():
                upsert_researched_lead(row.to_dict())
            st.session_state.scrape_queue_idx = len(base)
            st.success(f"Lote researched: {len(out)} leads.")
            st.rerun()

    researched = st.session_state.researched_leads
    if isinstance(researched, pd.DataFrame) and not researched.empty:
        st.markdown("#### Resultado del research")
        done = 0
        if "scrape_status" in researched.columns:
            done = int(researched["scrape_status"].astype(str).str.len().gt(0).sum())
        m1, m2, m3 = st.columns(3)
        m1.metric("Con research", done)
        m2.metric("Con website", int(researched.get("website", pd.Series(dtype=str)).astype(str).str.len().gt(0).sum()) if "website" in researched.columns else 0)
        m3.metric("Total", len(researched))
        st.dataframe(
            researched[
                [
                    c
                    for c in [
                        "nombre",
                        "website",
                        "website_summary",
                        "dolores",
                        "angulo_email",
                        "icebreaker",
                        "scrape_status",
                    ]
                    if c in researched.columns
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

        st.markdown("#### Puerta de aprobación — Paso 3 → Paso 4 (Scoring)")
        confirm_r = st.checkbox(
            "Revisé resumen/dolores/icebreakers y quiero pasar a scoring",
            key="confirm_research_gate",
        )
        if st.button(
            "Aprobar research y pasar a Scoring →",
            type="primary",
            use_container_width=True,
            disabled=not confirm_r,
        ):
            st.session_state.step_scrape_approved = True
            st.session_state.pipeline_step = 4
            st.session_state.scoring_queue_ids = researched["id"].astype(str).tolist()
            st.session_state.scoring_queue_idx = 0
            st.success("Research aprobado. Continuá en Scoring.")
            st.rerun()
        if st.session_state.step_scrape_approved:
            st.success("✓ Research aprobado — podés calificar en Scoring.")
    else:
        st.info("Todavía no hay research guardado.")


def tab_scoring(cfg: dict[str, str]) -> None:
    st.subheader("🧠 Paso 4 — Scoring e Inteligencia (supervisado)")
    st.write(
        "Calificá leads de a uno (recomendado) o en lote con confirmación. "
        "Revisá score, razón e icebreaker (prioriza dolores del scrape) antes de despacho."
    )

    if not st.session_state.step1_approved:
        st.warning("Primero aprobá la selección en **Paso 1 (Sourcing)**.")
        if st.button("Ir a Sourcing", key="scoring_go_sourcing"):
            st.session_state.pipeline_step = 1
            st.rerun()
        return
    if not st.session_state.step_enrich_approved:
        st.warning("Primero completá y aprobá **Paso 2 (Enrichment)**.")
        if st.button("Ir a Enrichment", key="scoring_go_enrich"):
            st.session_state.pipeline_step = 2
            st.rerun()
        return
    if not st.session_state.step_scrape_approved:
        st.warning("Primero completá y aprobá **Paso 3 (Web / Dolores)**.")
        if st.button("Ir a Web/Dolores", key="scoring_go_research"):
            st.session_state.pipeline_step = 3
            st.rerun()
        return

    researched = st.session_state.get("researched_leads", pd.DataFrame())
    enriched = st.session_state.get("enriched_leads", pd.DataFrame())
    if isinstance(researched, pd.DataFrame) and not researched.empty:
        to_score = researched.copy()
    elif isinstance(enriched, pd.DataFrame) and not enriched.empty:
        to_score = enriched.copy()
    else:
        to_score = get_selected_sourced_leads()

    if to_score.empty:
        st.warning("No hay leads seleccionados para calificar.")
        return

    c1, c2, c3 = st.columns(3)
    with c1:
        provider = st.selectbox(
            "Proveedor de IA",
            ["Heurística local (sin API)", "OpenAI (GPT-4o)", "Anthropic (Claude)"],
        )
    with c2:
        openai_model = st.text_input("Modelo OpenAI", value="gpt-4o")
    with c3:
        anthropic_model = st.text_input("Modelo Anthropic", value="claude-3-5-sonnet-20241022")

    mode = st.radio(
        "Modo de ejecución",
        ["Uno a uno (supervisado)", "Lote completo (con confirmación)"],
        horizontal=True,
        key="scoring_mode_radio",
    )
    st.session_state.scoring_mode = mode
    st.metric("Leads en cola", len(to_score))

    # Keep the rest of scoring body unchanged from here — replaced only the header/gates/to_score
    # -------- marker for next replace: scoring body continues below --------

    # -------- Modo uno a uno --------
    if mode.startswith("Uno a uno"):
        queue_ids = st.session_state.scoring_queue_ids or to_score["id"].astype(str).tolist()
        if not st.session_state.scoring_queue_ids:
            st.session_state.scoring_queue_ids = queue_ids
            st.session_state.scoring_queue_idx = 0
        idx = int(st.session_state.scoring_queue_idx)
        total_q = len(st.session_state.scoring_queue_ids)

        if idx >= total_q:
            st.success(f"Cola de scoring finalizada ({total_q}/{total_q}). Revisá resultados abajo.")
        else:
            lead_id = str(st.session_state.scoring_queue_ids[idx])
            lead_row = to_score[to_score["id"].astype(str) == lead_id]
            if lead_row.empty:
                st.session_state.scoring_queue_idx = idx + 1
                st.rerun()
            lead = lead_row.iloc[0].to_dict()
            st.markdown(f"##### Lead {idx + 1} de {total_q}")
            st.write(
                {
                    "nombre": lead.get("nombre"),
                    "rubro": lead.get("rubro"),
                    "ubicacion": lead.get("ubicacion"),
                    "website": lead.get("website"),
                    "telefono": lead.get("telefono"),
                    "rating": lead.get("rating"),
                }
            )

            b1, b2, b3 = st.columns(3)
            with b1:
                if st.button("Calificar este lead", type="primary", use_container_width=True):
                    result = score_one_lead(
                        {k: v for k, v in lead.items() if k != "seleccionado"},
                        provider=provider,
                        openai_key=cfg["openai_key"],
                        anthropic_key=cfg["anthropic_key"],
                        openai_model=openai_model.strip() or "gpt-4o",
                        anthropic_model=anthropic_model.strip() or "claude-3-5-sonnet-20241022",
                    )
                    st.session_state.scoring_current_result = result
            with b2:
                if st.button("Omitir lead →", use_container_width=True):
                    st.session_state.scoring_current_result = None
                    st.session_state.scoring_queue_idx = idx + 1
                    st.rerun()
            with b3:
                if st.button("Reiniciar cola scoring", use_container_width=True):
                    st.session_state.scoring_queue_ids = to_score["id"].astype(str).tolist()
                    st.session_state.scoring_queue_idx = 0
                    st.session_state.scoring_current_result = None
                    st.rerun()

            current = st.session_state.scoring_current_result
            if current and _safe_str(current.get("id")) == lead_id:
                st.markdown("###### Resultado — revisá antes de guardar")
                new_score = st.selectbox(
                    "Lead Score",
                    ["High", "Medium", "Low"],
                    index=["High", "Medium", "Low"].index(
                        _safe_str(current.get("lead_score")) if _safe_str(current.get("lead_score")) in {"High", "Medium", "Low"} else "Medium"
                    ),
                    key=f"edit_score_{lead_id}",
                )
                new_razon = st.text_area("Razón", value=_safe_str(current.get("score_razon")), key=f"edit_razon_{lead_id}")
                new_ice = st.text_area("Icebreaker", value=_safe_str(current.get("icebreaker")), key=f"edit_ice_{lead_id}")
                a1, a2 = st.columns(2)
                with a1:
                    if st.button("✓ Guardar y siguiente", type="primary", use_container_width=True):
                        current = dict(current)
                        current["lead_score"] = new_score
                        current["score_razon"] = new_razon
                        current["icebreaker"] = new_ice
                        upsert_scored_lead(current)
                        st.session_state.scoring_current_result = None
                        st.session_state.scoring_queue_idx = idx + 1
                        st.rerun()
                with a2:
                    if st.button("Descartar resultado", use_container_width=True):
                        st.session_state.scoring_current_result = None
                        st.rerun()

    # -------- Modo lote --------
    else:
        confirm_batch = st.checkbox(
            f"Confirmo calificar en lote los {len(to_score)} leads seleccionados",
            key="confirm_score_batch",
        )
        if st.button(
            "Calificar lote completo",
            type="primary",
            use_container_width=True,
            disabled=not confirm_batch,
        ):
            scored = score_leads_batch(
                to_score.drop(columns=["seleccionado"], errors="ignore"),
                provider=provider,
                openai_key=cfg["openai_key"],
                anthropic_key=cfg["anthropic_key"],
                openai_model=openai_model.strip() or "gpt-4o",
                anthropic_model=anthropic_model.strip() or "claude-3-5-sonnet-20241022",
            )
            st.session_state.scored_leads = scored
            st.session_state.high_leads = scored[scored["lead_score"] == "High"].copy()
            st.session_state.scoring_queue_idx = len(to_score)
            st.success(
                f"Lote calificado: {len(scored)} · "
                f"{(scored['lead_score']=='High').sum()} High · "
                f"{(scored['lead_score']=='Medium').sum()} Medium · "
                f"{(scored['lead_score']=='Low').sum()} Low"
            )

    scored = st.session_state.scored_leads
    if isinstance(scored, pd.DataFrame) and not scored.empty:
        st.markdown("#### Resultados calificados")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total", len(scored))
        m2.metric("High", int((scored["lead_score"] == "High").sum()))
        m3.metric("Medium", int((scored["lead_score"] == "Medium").sum()))
        m4.metric("Low", int((scored["lead_score"] == "Low").sum()))

        only_high = st.toggle("Mostrar solo High", value=True)
        view = scored[scored["lead_score"] == "High"] if only_high else scored
        edited_scores = st.data_editor(
            view.copy(),
            use_container_width=True,
            hide_index=True,
            column_config={
                "lead_score": st.column_config.SelectboxColumn("Score", options=["High", "Medium", "Low"]),
                "website": st.column_config.LinkColumn("Website"),
                "icebreaker": st.column_config.TextColumn("Icebreaker", width="large"),
            },
            key="editor_scored_review",
        )
        if st.button("Aplicar ediciones de score/icebreaker", use_container_width=True):
            full = scored.copy()
            for _, row in edited_scores.iterrows():
                mask = full["id"].astype(str) == str(row["id"])
                for col in ["lead_score", "score_razon", "icebreaker"]:
                    if col in full.columns and col in row:
                        full.loc[mask, col] = row[col]
            st.session_state.scored_leads = full
            st.session_state.high_leads = full[full["lead_score"] == "High"].copy()
            st.success("Ediciones aplicadas.")
            st.rerun()

        high = st.session_state.high_leads
        st.markdown("#### Puerta de aprobación — Paso 4 → Paso 5 (Despacho)")
        n_high = len(high) if isinstance(high, pd.DataFrame) else 0
        confirm2 = st.checkbox(
            f"Revisé los scores e icebreakers. Apruebo {n_high} lead(s) High para despacho",
            key="confirm_step2",
            disabled=n_high == 0,
        )
        if st.button(
            "Aprobar High y pasar a Despacho →",
            type="primary",
            use_container_width=True,
            disabled=not confirm2 or n_high == 0,
        ):
            st.session_state.step2_approved = True
            st.session_state.pipeline_step = 5
            st.session_state.dispatch_approved_ids = high["id"].astype(str).tolist()
            st.session_state.dispatch_queue_ids = high["id"].astype(str).tolist()
            st.session_state.dispatch_queue_idx = 0
            st.success("Paso 4 (Scoring) aprobado. Continuá en Despacho Outbound.")
            st.rerun()
        if st.session_state.step2_approved:
            st.success("✓ Paso 4 aprobado — podés despachar en Outbound.")
    else:
        st.info("Todavía no hay leads calificados. Usá el modo uno a uno o el lote.")


def tab_dispatch(cfg: dict[str, str]) -> None:
    st.subheader("🚀 Paso 5 — Despacho Outbound (supervisado)")
    st.write(
        "Enviá leads High de a uno o el lote aprobado. Cada envío requiere confirmación "
        "explícita para que puedas supervisar el proceso."
    )

    if not st.session_state.step2_approved:
        st.warning("Primero aprobá los leads High en **Paso 4 (Scoring)**.")
        if st.button("Ir a Scoring", key="dispatch_go_scoring"):
            st.session_state.pipeline_step = 4
            st.rerun()
        return

    high = st.session_state.high_leads
    scored = st.session_state.scored_leads
    if (not isinstance(high, pd.DataFrame) or high.empty) and isinstance(scored, pd.DataFrame) and not scored.empty:
        high = scored[scored["lead_score"] == "High"].copy()
        st.session_state.high_leads = high

    if not isinstance(high, pd.DataFrame) or high.empty:
        st.error("No hay leads High para despachar.")
        return

    # Checklist de aprobación individual
    st.markdown("#### Checklist de envío")
    approve_df = high.copy()
    approved_set = set(st.session_state.dispatch_approved_ids or [])
    approve_df["aprobar_envio"] = approve_df["id"].astype(str).isin(approved_set)
    edited = st.data_editor(
        approve_df[
            [c for c in ["aprobar_envio", "nombre", "website", "lead_score", "icebreaker", "telefono", "id"] if c in approve_df.columns]
        ],
        use_container_width=True,
        hide_index=True,
        column_config={
            "aprobar_envio": st.column_config.CheckboxColumn("Aprobar envío", default=True),
            "website": st.column_config.LinkColumn("Website"),
            "icebreaker": st.column_config.TextColumn("Icebreaker", width="large"),
        },
        disabled=[c for c in approve_df.columns if c not in {"aprobar_envio", "icebreaker"}],
        key="editor_dispatch_approve",
    )
    st.session_state.dispatch_approved_ids = (
        edited[edited["aprobar_envio"] == True]["id"].astype(str).tolist()  # noqa: E712
    )
    # Sync icebreaker edits back
    if "icebreaker" in edited.columns:
        full_high = high.copy()
        for _, row in edited.iterrows():
            mask = full_high["id"].astype(str) == str(row["id"])
            if mask.any() and "icebreaker" in full_high.columns:
                full_high.loc[mask, "icebreaker"] = row["icebreaker"]
        st.session_state.high_leads = full_high
        high = full_high

    payload = high[high["id"].astype(str).isin(st.session_state.dispatch_approved_ids)].copy()
    m1, m2, m3 = st.columns(3)
    m1.metric("Aprobados para envío", len(payload))
    m2.metric("Instantly key", "OK" if cfg["instantly_key"] else "No")
    m3.metric("Webhook", "OK" if cfg["webhook_url"] else "No")

    channel = st.selectbox("Canal de despacho", ["Instantly.ai", "Webhook (Make/n8n)"])
    exec_mode = st.radio(
        "Modo de ejecución",
        ["Uno a uno (supervisado)", "Lote aprobado (con confirmación)"],
        horizontal=True,
        key="dispatch_mode_radio",
    )
    st.session_state.dispatch_mode = exec_mode

    if payload.empty:
        st.warning("Marcá al menos un lead en **Aprobar envío**.")
        return

    # ---- Uno a uno ----
    if exec_mode.startswith("Uno a uno"):
        queue = st.session_state.dispatch_approved_ids
        if not st.session_state.dispatch_queue_ids:
            st.session_state.dispatch_queue_ids = list(queue)
            st.session_state.dispatch_queue_idx = 0
        # Refresh queue if approvals changed
        if set(st.session_state.dispatch_queue_ids) != set(queue):
            # keep progress on remaining
            remaining = [i for i in queue if i in set(queue)]
            st.session_state.dispatch_queue_ids = remaining
            st.session_state.dispatch_queue_idx = min(
                st.session_state.dispatch_queue_idx, max(len(remaining) - 1, 0)
            )

        idx = int(st.session_state.dispatch_queue_idx)
        total_q = len(st.session_state.dispatch_queue_ids)
        if total_q == 0:
            st.info("No hay leads en cola de despacho.")
            return
        if idx >= total_q:
            st.success("Cola de despacho finalizada.")
        else:
            lead_id = str(st.session_state.dispatch_queue_ids[idx])
            lead_row = payload[payload["id"].astype(str) == lead_id]
            if lead_row.empty:
                st.session_state.dispatch_queue_idx = idx + 1
                st.rerun()
            lead = lead_row.iloc[0].to_dict()
            st.markdown(f"##### Envío {idx + 1} de {total_q}: **{_safe_str(lead.get('nombre'))}**")
            st.write(
                {
                    "website": lead.get("website"),
                    "telefono": lead.get("telefono"),
                    "icebreaker": lead.get("icebreaker"),
                    "lead_score": lead.get("lead_score"),
                }
            )
            confirm_one = st.checkbox(
                f"Confirmo enviar este lead por {channel}",
                key=f"confirm_dispatch_{lead_id}",
            )
            d1, d2 = st.columns(2)
            with d1:
                if st.button(
                    "Enviar este lead ahora",
                    type="primary",
                    use_container_width=True,
                    disabled=not confirm_one,
                ):
                    if channel == "Instantly.ai" and not cfg["instantly_key"]:
                        st.error("Falta Instantly API Key.")
                    elif channel == "Webhook (Make/n8n)" and not cfg["webhook_url"]:
                        st.error("Falta URL de webhook.")
                    else:
                        log_row, crm = dispatch_one_lead(
                            lead,
                            channel,
                            cfg["instantly_key"],
                            cfg["webhook_url"],
                            cfg["campaign_id"],
                            cfg["calendar_url"],
                        )
                        save_crm(crm)
                        append_dispatch_log([log_row])
                        if log_row["exito"] == "sí":
                            st.success(f"Enviado OK — HTTP {log_row['http_status']}")
                        else:
                            st.error(f"Falló — {log_row['detalle'][:200]}")
                        st.session_state.dispatch_queue_idx = idx + 1
                        st.rerun()
            with d2:
                if st.button("Omitir este lead →", use_container_width=True):
                    st.session_state.dispatch_queue_idx = idx + 1
                    st.rerun()

    # ---- Lote ----
    else:
        confirm_batch = st.checkbox(
            f"Confirmo enviar el lote de {len(payload)} lead(s) aprobados por {channel}",
            key="confirm_dispatch_batch",
        )
        if st.button(
            "Enviar lote aprobado a Campaña Fría",
            type="primary",
            use_container_width=True,
            disabled=not confirm_batch,
        ):
            if channel == "Instantly.ai" and not cfg["instantly_key"]:
                st.error("Falta Instantly API Key.")
            elif channel == "Webhook (Make/n8n)" and not cfg["webhook_url"]:
                st.error("Falta URL de webhook.")
            else:
                logs = dispatch_high_leads(
                    payload,
                    mode=channel,
                    instantly_key=cfg["instantly_key"],
                    webhook_url=cfg["webhook_url"],
                    campaign_id=cfg["campaign_id"],
                    calendar_url=cfg["calendar_url"],
                )
                ok_n = sum(1 for r in logs if r["exito"] == "sí")
                fail_n = len(logs) - ok_n
                st.session_state.pipeline_step = 6
                if ok_n:
                    st.success(f"Despacho finalizado: {ok_n} OK · {fail_n} fallidos. Revisá el CRM.")
                else:
                    st.error(f"Ningún envío exitoso ({fail_n} fallidos).")

    if st.button("Marcar paso completado e ir al CRM →", use_container_width=True):
        st.session_state.pipeline_step = 6
        st.rerun()

    st.markdown("#### Historial / log de envíos")
    log_df = st.session_state.dispatch_log
    if isinstance(log_df, pd.DataFrame) and not log_df.empty:
        st.dataframe(log_df.sort_values("timestamp", ascending=False), use_container_width=True, hide_index=True)
        c1, c2 = st.columns(2)
        with c1:
            st.metric("Éxitos", int((log_df["exito"] == "sí").sum()))
        with c2:
            st.metric("Fallos", int((log_df["exito"] != "sí").sum()))
    else:
        st.caption("Todavía no hay envíos registrados.")



def tab_crm(cfg: dict[str, str]) -> None:
    st.subheader("📊 Paso 6 — CRM Local y Agendamiento")
    st.write(
        f"Pipeline persistente en `{get_crm_path().resolve()}`. "
        "Actualizá estados (Contactado → Respuesta → Agendado) y exportá a CSV/Excel."
    )

    crm = load_crm()

    # Métricas de pipeline
    def _count(status: str) -> int:
        if crm.empty:
            return 0
        return int((crm["pipeline_status"] == status).sum())

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Total CRM", len(crm))
    m2.metric("Contactados", _count("Contactado"))
    m3.metric("Respuestas", _count("Respuesta Recibida"))
    m4.metric("Agendados", _count("Agendado"))
    m5.metric("High score", int((crm["lead_score"] == "High").sum()) if not crm.empty else 0)

    st.markdown(f"**Enlace de agendamiento por defecto:** {cfg['calendar_url']}")

    if crm.empty:
        st.info("CRM vacío. Los leads High despachados o guardados desde Scoring aparecerán aquí.")
        # Formulario para alta manual
        with st.expander("Alta manual de lead"):
            with st.form("alta_manual"):
                n = st.text_input("Nombre")
                t = st.text_input("Teléfono")
                w = st.text_input("Website")
                r = st.text_input("Rubro")
                u = st.text_input("Ubicación", value="Pilar")
                if st.form_submit_button("Agregar"):
                    if not n.strip():
                        st.error("El nombre es obligatorio.")
                    else:
                        now = _utc_now_iso()
                        row = {col: "" for col in CRM_COLUMNS}
                        row.update(
                            {
                                "id": str(uuid.uuid4())[:8],
                                "nombre": n.strip(),
                                "telefono": t.strip(),
                                "website": w.strip(),
                                "rubro": r.strip(),
                                "ubicacion": u.strip(),
                                "lead_score": "Medium",
                                "pipeline_status": "Nuevo",
                                "calendario_url": cfg["calendar_url"],
                                "fecha_creacion": now,
                                "fecha_actualizacion": now,
                            }
                        )
                        save_crm(pd.DataFrame([row]))
                        st.success("Lead agregado.")
                        st.rerun()
        return

    filter_status = st.multiselect(
        "Filtrar por estado de pipeline",
        PIPELINE_STATUSES,
        default=PIPELINE_STATUSES,
    )
    filter_score = st.multiselect(
        "Filtrar por score",
        ["High", "Medium", "Low", ""],
        default=["High", "Medium", "Low", ""],
    )

    view = crm[
        crm["pipeline_status"].isin(filter_status)
        & crm["lead_score"].isin(filter_score)
    ].copy()

    st.markdown("#### Vista editable del pipeline")
    edited = st.data_editor(
        view,
        use_container_width=True,
        hide_index=True,
        column_config={
            "pipeline_status": st.column_config.SelectboxColumn(
                "Pipeline",
                options=PIPELINE_STATUSES,
                required=True,
            ),
            "lead_score": st.column_config.SelectboxColumn(
                "Score",
                options=["High", "Medium", "Low"],
            ),
            "website": st.column_config.LinkColumn("Website"),
            "calendario_url": st.column_config.LinkColumn("Agendamiento"),
            "icebreaker": st.column_config.TextColumn("Icebreaker", width="large"),
            "notas": st.column_config.TextColumn("Notas", width="medium"),
        },
        key="crm_editor",
    )

    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("Guardar cambios del CRM", type="primary", use_container_width=True):
            edited = edited.copy()
            edited["fecha_actualizacion"] = _utc_now_iso()
            # Merge back into full CRM by id
            full = crm.copy()
            for _, row in edited.iterrows():
                mask = full["id"] == row["id"]
                if mask.any():
                    for col in CRM_COLUMNS:
                        full.loc[mask, col] = row.get(col, full.loc[mask, col].values[0])
            save_crm(full)
            st.success("CRM actualizado.")
            st.rerun()
    with c2:
        st.download_button(
            "Exportar CSV limpio",
            data=edited.to_csv(index=False),
            file_name=f"katem_crm_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv",
            use_container_width=True,
        )
    with c3:
        xbytes = dataframe_to_excel_bytes(edited)
        is_xlsx = xbytes[:2] == b"PK"
        st.download_button(
            "Exportar Excel" if is_xlsx else "Exportar (CSV fallback)",
            data=xbytes,
            file_name=(
                f"katem_crm_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
                if is_xlsx
                else f"katem_crm_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
            ),
            mime=(
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                if is_xlsx
                else "text/csv"
            ),
            use_container_width=True,
        )

    with st.expander("Agregar lead manual al CRM"):
        with st.form("alta_manual_2"):
            n = st.text_input("Nombre", key="m2_n")
            t = st.text_input("Teléfono", key="m2_t")
            w = st.text_input("Website", key="m2_w")
            r = st.text_input("Rubro", key="m2_r")
            u = st.text_input("Ubicación", value="Pilar", key="m2_u")
            status = st.selectbox("Estado", PIPELINE_STATUSES, key="m2_s")
            if st.form_submit_button("Agregar al CRM"):
                if not n.strip():
                    st.error("El nombre es obligatorio.")
                else:
                    now = _utc_now_iso()
                    row = {col: "" for col in CRM_COLUMNS}
                    row.update(
                        {
                            "id": str(uuid.uuid4())[:8],
                            "nombre": n.strip(),
                            "telefono": t.strip(),
                            "website": w.strip(),
                            "rubro": r.strip(),
                            "ubicacion": u.strip(),
                            "lead_score": "Medium",
                            "pipeline_status": status,
                            "calendario_url": cfg["calendar_url"],
                            "fecha_creacion": now,
                            "fecha_actualizacion": now,
                        }
                    )
                    save_crm(pd.concat([crm, pd.DataFrame([row])], ignore_index=True))
                    st.success("Lead agregado.")
                    st.rerun()


def main() -> None:
    st.set_page_config(
        page_title=APP_TITLE,
        page_icon="🎯",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    init_session_state()

    client = get_client(st.session_state.get("active_client_id", "katem-demo"))
    st.title("🎯 SDR Autónomo — Katem")
    st.caption(
        f"Vertical productizada de Katem · Workspace: **{client.get('name')}** "
        f"({client.get('vertical')}) · "
        "Sourcing → Enrichment → Web/Dolores → Scoring → Outbound → CRM"
    )

    cfg = render_sidebar()
    render_pipeline_stepper()

    step = int(st.session_state.get("pipeline_step", 1))
    step_hints = {
        1: "Sourcing: buscá leads y aprobá la selección.",
        2: "Enrichment: completá email + LinkedIn (Clay) y aprobá.",
        3: "Web/Dolores: scrapeá sitios y extraé dolores con Claude (o heurística).",
        4: "Scoring: calificá de a uno o en lote confirmado.",
        5: "Despacho: enviá High con supervisión.",
        6: "CRM: actualizá pipeline y exportá.",
    }
    st.info(step_hints.get(step, ""))

    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(
        [
            "🔍 1. Sourcing",
            "✨ 2. Enrichment",
            "🌐 3. Web/Dolores",
            "🧠 4. Scoring",
            "🚀 5. Despacho",
            "📊 6. CRM",
        ]
    )
    with tab1:
        tab_sourcing(cfg)
    with tab2:
        tab_enrichment(cfg)
    with tab3:
        tab_research(cfg)
    with tab4:
        tab_scoring(cfg)
    with tab5:
        tab_dispatch(cfg)
    with tab6:
        tab_crm(cfg)



if __name__ == "__main__":
    main()

# =============================================================================
# Documentación embebida (asignada a variables para que Streamlit Magic
# NO las renderice en la UI; el contenido queda en el código fuente).
# =============================================================================

_SUGERENCIAS_MEJORA_Y_ARQUITECTURA_FUTURA = """
SUGERENCIAS DE MEJORA Y ARQUITECTURA FUTURA
-------------------------------------------
1) Cache y performance
   - Envolver search_leads (Places/Apollo/Clay) y lecturas de CRM con @st.cache_data(ttl=300)
     para evitar reconsultas costosas a Places en cada rerun de Streamlit.
   - Separar el estado mutable (selección, logs) del cache de datos de solo lectura.

2) Rate limits y resiliencia de APIs
   - Implementar retry con backoff exponencial (tenacity) para OpenAI, Anthropic,
     Google Places e Instantly (429/5xx).
   - Cola local de despacho (SQLite + worker) para no bloquear la UI cuando hay
     decenas de leads High y el proveedor outbound limita requests/minuto.

3) Persistencia escalable
   - Migrar leads_crm.csv / dispatch_log.csv a SQLite (SQLModel/SQLAlchemy) o
     Supabase/Postgres. Mantener IDs estables, índices por pipeline_status y
     lead_score, y auditoría de cambios (historial de estados).

4) Seguridad de API keys
   - No persistir keys en session_state ni logs. Preferir Streamlit Secrets /
     variables de entorno del servidor. Rotar keys y usar scopes mínimos.
   - En producción, proxy server-side (FastAPI) para que el browser nunca vea
     las claves de OpenAI/Instantly/Google.

5) Productización del SDR
   - Auth multi-usuario, billing por workspace, templates A/B de icebreaker, sync bidireccional
     con Instantly/Smartlead (replies → pipeline_status = Respuesta Recibida),
     y webhooks entrantes desde Cal.com para marcar "Agendado" automáticamente.
"""

_REQUIREMENTS_TXT = """
# ----- requirements.txt -----
streamlit>=1.32.0
pandas>=2.1.0
requests>=2.31.0
httpx>=0.27.0
python-dotenv>=1.0.0
openpyxl>=3.1.0
"""

_ENV_EXAMPLE = """
# ----- .env.example -----
OPENAI_API_KEY=sk-proj-xxxxxxxxxxxxxxxxxxxxxxxx
ANTHROPIC_API_KEY=sk-ant-xxxxxxxxxxxxxxxxxxxxxxxx
GOOGLE_MAPS_KEY=AIzaxxxxxxxxxxxxxxxxxxxxxxxx
APOLLO_API_KEY=apollo_xxxxxxxxxxxxxxxxxxxxxxxx
CLAY_API_KEY=clay_xxxxxxxxxxxxxxxxxxxxxxxx
CLAY_WEBHOOK_URL=https://api.clay.com/v1/webhooks/xxxxxxxx
CLAY_LINKEDIN_WEBHOOK_URL=https://api.clay.com/v1/webhooks/linkedin-xxxxxxxx
HUNTER_API_KEY=hunter_xxxxxxxxxxxxxxxxxxxxxxxx
SNOV_API_KEY=snov_xxxxxxxxxxxxxxxxxxxxxxxx
INSTANTLY_API_KEY=instantly_xxxxxxxxxxxxxxxxxx
SMARTLEAD_API_KEY=
INSTANTLY_CAMPAIGN_ID=campaign_xxxxxxxx
WEBHOOK_URL=https://hook.eu1.make.com/xxxxxxxx
CALENDAR_URL=https://cal.com/katem
"""
