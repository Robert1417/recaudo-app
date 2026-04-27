import ast
import json
import os
import re
import secrets
import sys
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import gspread
import numpy as np
import pandas as pd
import requests
import streamlit as st
from sklearn.base import BaseEstimator, TransformerMixin
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials
from google.oauth2.credentials import Credentials as UserCredentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from joblib import load

# Nota de entorno:
# Esta app puede instalarse de forma aislada con `requirements_independiente.txt`
# sin tocar el `requirements.txt` principal de la calculadora original.

MODEL_PATH = Path("mlp_recaudo_pipeline.joblib")
DATA_PARQUET = Path("data/cartera_asignada_filtrada.parquet")
DATA_CSV = Path("data/cartera_asignada_filtrada.csv")
GOOGLE_SHEET_ID = "1Aahltn7TSRf6ZpTpS-vPgpB89hO-r5KxpAhqKAPXziE"
GOOGLE_SHEET_TAB_RESPUESTAS = "Respuestas Estr"
GOOGLE_SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
DRIVE_FOLDER_CARTA_PAGARE_ID = "1nEo1iZWzFySJX_90crO9tjTTX1Cr_yVxs-xyn1C0TMu78Jt8rs2QYqVXs_wgzxEvn1AU0nMk"
GOOGLE_DRIVE_UPLOAD_SCOPES = ["https://www.googleapis.com/auth/drive.file"]

class LogAndDrop(BaseEstimator, TransformerMixin):
    def __init__(self, ca_col="C/A", amt_col="AMOUNT_TOTAL"):
        self.ca_col = ca_col
        self.amt_col = amt_col
        self.out_feature_names_ = ["PRI-ULT", "Ratio_PP", "C/A_log", "AMOUNT_TOTAL_log"]

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        X = X.copy()
        X["C/A_log"] = np.log1p(pd.to_numeric(X[self.ca_col], errors="coerce").astype(float))
        X["AMOUNT_TOTAL_log"] = np.log1p(pd.to_numeric(X[self.amt_col], errors="coerce").astype(float))
        X = X.drop(columns=[self.ca_col, self.amt_col])
        return X[["PRI-ULT", "Ratio_PP", "C/A_log", "AMOUNT_TOTAL_log"]]

    def get_feature_names_out(self, input_features=None):
        return np.array(self.out_feature_names_)


class Winsorizer(BaseEstimator, TransformerMixin):
    def __init__(self, columns=None, p_low=0.005, p_high=0.005):
        self.columns = columns or []
        self.p_low = p_low
        self.p_high = p_high
        self.lows_ = {}
        self.highs_ = {}

    def fit(self, X, y=None):
        X = pd.DataFrame(X, copy=True)
        for c in self.columns:
            s = pd.to_numeric(X[c], errors="coerce")
            self.lows_[c] = s.quantile(self.p_low)
            self.highs_[c] = s.quantile(1 - self.p_high)
        return self

    def transform(self, X):
        X = pd.DataFrame(X, copy=True)
        for c in self.columns:
            lo = self.lows_[c]
            hi = self.highs_[c]
            X[c] = pd.to_numeric(X[c], errors="coerce").clip(lower=lo, upper=hi)
        return X

# Compatibilidad joblib cuando el pipeline fue serializado desde __main__.
_main_module = sys.modules.get("__main__")
if _main_module is not None:
    setattr(_main_module, "LogAndDrop", LogAndDrop)
    setattr(_main_module, "Winsorizer", Winsorizer)


def _norm(text: str) -> str:
    text = str(text or "").strip().lower()
    text = text.translate(str.maketrans("áéíóúü", "aeiouu"))
    text = text.replace("_", " ").replace("-", " ")
    text = re.sub(r"[^a-z0-9 ]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _extract_mi_json_from_secrets_tree(value):
    try:
        if isinstance(value, str) and value.strip():
            return value
    except Exception:
        return None

    try:
        as_dict = dict(value)
    except Exception:
        return None

    if "private_key" in as_dict and "client_email" in as_dict:
        return as_dict

    for key, sub in as_dict.items():
        if str(key).strip().upper() == "MI_JSON":
            return sub
        nested = _extract_mi_json_from_secrets_tree(sub)
        if nested is not None:
            return nested
    return None


def _load_service_account_info() -> dict:
    source = None
    try:
        source = _extract_mi_json_from_secrets_tree(st.secrets)
    except Exception:
        source = None

    if source is None:
        source = os.environ.get("MI_JSON") or os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")

    if source is None:
        raise RuntimeError("No encontré credenciales. Configura MI_JSON en secrets o entorno.")

    if isinstance(source, str):
        source = source.strip()
        try:
            info = json.loads(source)
        except json.JSONDecodeError:
            info = ast.literal_eval(source)
    else:
        info = dict(source)

    if "private_key" in info and isinstance(info["private_key"], str):
        info["private_key"] = info["private_key"].replace("\\n", "\n")

    return info


def _load_google_oauth_client_config() -> dict:
    client_id = st.secrets.get("GOOGLE_OAUTH_CLIENT_ID")
    client_secret = st.secrets.get("GOOGLE_OAUTH_CLIENT_SECRET")
    if client_id and client_secret:
        return {
            "installed": {
                "client_id": str(client_id),
                "client_secret": str(client_secret),
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "redirect_uris": ["http://localhost"],
            }
        }
    raise RuntimeError("Faltan secretos OAuth: GOOGLE_OAUTH_CLIENT_ID y GOOGLE_OAUTH_CLIENT_SECRET.")


def _oauth_drive_configurado() -> bool:
    try:
        _load_google_oauth_client_config()
        return True
    except Exception:
        return False


def _extract_oauth_code(redirect_text: str) -> str:
    text = str(redirect_text or "").strip()
    if not text:
        return ""
    if text.startswith("http://") or text.startswith("https://"):
        parsed = urlparse(text)
        q = parse_qs(parsed.query)
        return str((q.get("code") or [""])[0]).strip()
    return text


def _start_drive_oauth_flow() -> str:
    cfg = _load_google_oauth_client_config()
    flow = Flow.from_client_config(cfg, scopes=GOOGLE_DRIVE_UPLOAD_SCOPES)
    flow.redirect_uri = "http://localhost"
    code_verifier = secrets.token_urlsafe(72)[:96]
    flow.code_verifier = code_verifier
    auth_url, _ = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
    )
    st.session_state.drive_oauth_cfg = cfg
    st.session_state.drive_oauth_code_verifier = code_verifier
    return auth_url


def _exchange_drive_oauth_code(code_or_url: str) -> None:
    cfg = st.session_state.get("drive_oauth_cfg") or _load_google_oauth_client_config()
    code_verifier = st.session_state.get("drive_oauth_code_verifier")
    code = _extract_oauth_code(code_or_url)
    if not code:
        raise ValueError("No encontré el code OAuth en la URL/código pegado.")
    flow = Flow.from_client_config(cfg, scopes=GOOGLE_DRIVE_UPLOAD_SCOPES)
    flow.redirect_uri = "http://localhost"
    if code_verifier:
        flow.code_verifier = code_verifier
    flow.fetch_token(code=code)
    creds = flow.credentials
    st.session_state.drive_user_token = json.loads(creds.to_json())


def _get_drive_user_credentials(*, refresh_if_needed: bool = True):
    token_data = st.session_state.get("drive_user_token")
    if not token_data:
        return None
    try:
        creds = UserCredentials.from_authorized_user_info(token_data, GOOGLE_DRIVE_UPLOAD_SCOPES)
    except Exception:
        return None
    should_refresh_proactively = False
    expiry = getattr(creds, "expiry", None)
    if expiry is not None:
        try:
            now_utc = datetime.now(timezone.utc)
            expiry_utc = expiry if expiry.tzinfo else expiry.replace(tzinfo=timezone.utc)
            should_refresh_proactively = (expiry_utc - now_utc) <= timedelta(minutes=5)
        except Exception:
            should_refresh_proactively = False
    if creds.valid and not should_refresh_proactively:
        return creds
    if refresh_if_needed and creds.refresh_token:
        try:
            creds.refresh(Request())
            st.session_state.drive_user_token = json.loads(creds.to_json())
            return creds
        except Exception:
            return None
    return None


def _build_drive_service_from_session():
    creds = _get_drive_user_credentials()
    if creds is None:
        return None
    return build("drive", "v3", credentials=creds)


@st.cache_resource(show_spinner=False)
def _google_clients():
    creds_info = _load_service_account_info()
    creds = Credentials.from_service_account_info(creds_info, scopes=GOOGLE_SHEETS_SCOPES)
    sheets_client = gspread.authorize(creds)
    drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)
    return sheets_client, drive_service


@st.cache_resource(show_spinner=False)
def _load_model():
    if not MODEL_PATH.exists():
        raise FileNotFoundError(f"No existe el modelo en {MODEL_PATH}")
    return load(MODEL_PATH)


def _col_index_to_letter(col_idx: int) -> str:
    letters = ""
    while col_idx > 0:
        col_idx, rem = divmod(col_idx - 1, 26)
        letters = chr(65 + rem) + letters
    return letters or "A"


def _upload_pdf_to_drive(drive_service, uploaded_file, folder_id: str) -> str:
    if uploaded_file is None:
        return ""
    name = str(uploaded_file.name or "archivo.pdf")
    if not name.lower().endswith(".pdf"):
        raise ValueError(f"{name}: solo se permite PDF.")

    file_metadata = {
        "name": f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{name}",
        "parents": [folder_id],
    }
    media = MediaIoBaseUpload(BytesIO(uploaded_file.getvalue()), mimetype="application/pdf", resumable=False)
    created = (
        drive_service.files()
        .create(body=file_metadata, media_body=media, fields="id,webViewLink")
        .execute()
    )
    return str(created.get("webViewLink", "")).strip()


def _append_respuesta(row_data: dict):
    sheets_client, _ = _google_clients()
    ws = sheets_client.open_by_key(GOOGLE_SHEET_ID).worksheet(GOOGLE_SHEET_TAB_RESPUESTAS)
    headers = ws.row_values(1)
    if not headers:
        raise RuntimeError("La pestaña Respuestas Estr no tiene encabezados en la fila 1.")

    normalized = [""] * len(headers)
    payload = dict(row_data)
    for idx, header in enumerate(headers):
        h = _norm(header)
        if "marca temporal" in h:
            normalized[idx] = payload.get("timestamp", "")
        elif "direccion de correo" in h or h == "correo":
            normalized[idx] = payload.get("correo_electronico", "")
        elif h == "referencia":
            normalized[idx] = payload.get("referencia", "")
        elif "id deuda" in h or "id de la deuda" in h or "id de deuda" in h:
            normalized[idx] = payload.get("ids", "")
        elif "banco" in h:
            normalized[idx] = payload.get("bancos", "")
        elif "carta" in h and "pagare" in h:
            normalized[idx] = payload.get("carta_pagare_link", "")
        elif "pantallazo" in h and "aceptacion" in h:
            normalized[idx] = payload.get("pantallazo_aceptacion_link", "")
        elif "condonacion" in h and "mensualidades" in h:
            normalized[idx] = payload.get("condonacion_mensualidades", "")
        elif "pantallazo" in h and "correo" in h and "condonacion" in h:
            normalized[idx] = payload.get("pantallazo_correo_condonacion_link", "")
        elif "total de comision" in h:
            normalized[idx] = payload.get("comision_exito_total", "")
        elif "primera comision" in h or "pago de la primera comision" in h:
            normalized[idx] = payload.get("ce_inicial", "")
        elif "aprobacion estructurados" in h:
            normalized[idx] = payload.get("es_aprobado_bool", "")
        elif h == "estado" or "comentario" in h:
            normalized[idx] = payload.get("estado_aprobacion", "")
        elif "calculadora" in h:
            normalized[idx] = payload.get("prediccion", "")

    normalized[-1] = payload.get("prediccion", "")
    target_row = len(ws.col_values(1)) + 1
    end_col = _col_index_to_letter(len(headers))
    ws.update(f"A{target_row}:{end_col}{target_row}", [normalized], value_input_option="USER_ENTERED")


def _parse_sheet_timestamp(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _ids_to_set(ids_value) -> set[str]:
    if isinstance(ids_value, (list, tuple, set)):
        raw = [str(x) for x in ids_value]
    else:
        raw = re.split(r"[^0-9A-Za-z]+", str(ids_value or ""))
    return {item.strip() for item in raw if str(item).strip()}


def _get_respuestas_duplicados_mes(referencia, ids) -> dict:
    try:
        sheets_client, _ = _google_clients()
        ws = sheets_client.open_by_key(GOOGLE_SHEET_ID).worksheet(GOOGLE_SHEET_TAB_RESPUESTAS)
        all_values = ws.get_all_values()
        if not all_values:
            return {"ok": True, "mode": "none", "exact_rows": [], "error": None}

        headers = all_values[0]
        rows = all_values[1:]
        header_idx = {_norm(h): i for i, h in enumerate(headers)}
        timestamp_idx = next((i for h, i in header_idx.items() if "marca temporal" in h), None)
        ref_idx = next((i for h, i in header_idx.items() if h == "referencia"), None)
        ids_idx = next((i for h, i in header_idx.items() if "id deuda" in h or "id de la deuda" in h or "id de deuda" in h), None)
        aprob_idx = next((i for h, i in header_idx.items() if "aprobacion estructurados" in h), None)
        comentario_idx = next((i for h, i in header_idx.items() if h == "estado" or "comentario" in h), None)

        if timestamp_idx is None or ref_idx is None or ids_idx is None:
            return {"ok": True, "mode": "none", "exact_rows": [], "error": None}

        now = datetime.now()
        ref_norm = _norm(str(referencia or ""))
        ids_target = _ids_to_set(ids)
        ref_found = False
        exact_rows = []
        for offset, row in enumerate(rows, start=2):
            ts_raw = row[timestamp_idx] if timestamp_idx < len(row) else ""
            ref_raw = row[ref_idx] if ref_idx < len(row) else ""
            ids_raw = row[ids_idx] if ids_idx < len(row) else ""
            row_dt = _parse_sheet_timestamp(ts_raw)
            if row_dt is None or row_dt.year != now.year or row_dt.month != now.month:
                continue
            if _norm(ref_raw) != ref_norm:
                continue
            ref_found = True
            if _ids_to_set(ids_raw) == ids_target:
                exact_rows.append(
                    {
                        "row_idx": offset,
                        "aprob_col_idx": aprob_idx + 1 if aprob_idx is not None else None,
                        "comentario_col_idx": comentario_idx + 1 if comentario_idx is not None else None,
                        "comentario_actual": row[comentario_idx] if comentario_idx is not None and comentario_idx < len(row) else "",
                    }
                )
        mode = "none"
        if exact_rows:
            mode = "exact_duplicate"
        elif ref_found:
            mode = "reference_duplicate"
        return {"ok": True, "mode": mode, "exact_rows": exact_rows, "error": None}
    except Exception as exc:
        return {"ok": False, "mode": "none", "exact_rows": [], "error": str(exc)}


def _marcar_anteriores_como_duplicado(exact_rows: list[dict]):
    if not exact_rows:
        return
    sheets_client, _ = _google_clients()
    ws = sheets_client.open_by_key(GOOGLE_SHEET_ID).worksheet(GOOGLE_SHEET_TAB_RESPUESTAS)
    for row_info in exact_rows:
        row_idx = row_info.get("row_idx")
        aprob_col_idx = row_info.get("aprob_col_idx")
        comentario_col_idx = row_info.get("comentario_col_idx")
        comentario_actual = _norm(row_info.get("comentario_actual", ""))
        if row_idx and aprob_col_idx:
            ws.update(f"{_col_index_to_letter(aprob_col_idx)}{row_idx}", [["FALSE"]], value_input_option="USER_ENTERED")
        if row_idx and comentario_col_idx and comentario_actual == "aprobado":
            ws.update(f"{_col_index_to_letter(comentario_col_idx)}{row_idx}", [["Duplicado"]], value_input_option="USER_ENTERED")


def _is_traditional_liquidation(tipo_liquidacion: str) -> bool:
    norm = _norm(tipo_liquidacion)
    return "tradicional" in norm


def _predict_recaudo(model, features: dict, pago_banco: float, primer_pago: float) -> float:
    x = pd.DataFrame([features], columns=["PRI-ULT", "Ratio_PP", "C/A", "AMOUNT_TOTAL"])
    yhat = float(model.predict(x)[0])

    if yhat == 0.98:
        yhat += 0.02
    elif yhat == 0.99:
        yhat += 0.01
    else:
        yhat += 0.03

    if float(features["AMOUNT_TOTAL"]) > 6_000_000:
        yhat += 0.05

    if pago_banco > 0 and (primer_pago / pago_banco) < 0.10:
        yhat = min(yhat, 0.74)

    return min(yhat, 0.99)


def _to_float(value, default=0.0) -> float:
    try:
        if value is None or (isinstance(value, str) and not value.strip()):
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _extract_case_data_from_record(record: dict) -> dict:
    rec = {str(k).strip().lower(): v for k, v in dict(record or {}).items()}

    def pick(*keys, default=""):
        for key in keys:
            if key in rec and rec[key] is not None:
                return rec[key]
        return default

    return {
        "referencia": str(pick("referencia", "reference", default="")).strip(),
        "ids": str(pick("ids", "id_deuda", "id deuda", "ids_deuda", default="")).strip(),
        "bancos": str(pick("bancos", "banco", default="")).strip(),
        "correo": str(pick("correo", "correo_electronico", "email", default="")).strip(),
        "tipo_liquidacion": str(pick("tipo_liquidacion", "tipo de liquidacion", default="")).strip(),
        "enviar": str(pick("enviar", "enviar_si_no", "send", default="No")).strip() or "No",
        "pri_ult": _to_float(pick("pri-ult", "pri_ult", "plazo"), 1.0),
        "ratio_pp": _to_float(pick("ratio_pp", "ratio pp"), 0.0),
        "c_a": _to_float(pick("c/a", "c_a", "cuota_apartado"), 1.0),
        "amount_total": _to_float(pick("amount_total", "comision_exito_total"), 0.0),
        "pago_banco": _to_float(pick("pago_banco", "pago banco"), 0.0),
        "primer_pago": _to_float(pick("primer_pago", "primer pago", "primer_pago_banco"), 0.0),
        "ce_inicial": _to_float(pick("ce_inicial", "ce inicial"), 0.0),
    }


def _resolver_tipo_liquidacion_desde_cartera(cartera_df: pd.DataFrame, referencia: str) -> str:
    if cartera_df is None or cartera_df.empty:
        return ""
    ref_cols = [c for c in cartera_df.columns if _norm(c) in {"referencia", "reference"}]
    tipo_cols = [
        c
        for c in cartera_df.columns
        if _norm(c) in {"tipo de liquidacion", "tipo liquidacion", "tipo de liquidación", "tipo_liquidacion"}
    ]
    if not ref_cols or not tipo_cols:
        return ""

    ref_col = ref_cols[0]
    tipo_col = tipo_cols[0]
    target_ref = str(referencia or "").strip()
    if not target_ref:
        return ""

    match = cartera_df[cartera_df[ref_col].astype(str).str.strip() == target_ref]
    if match.empty:
        return ""
    return str(match.iloc[0][tipo_col]).strip()


@st.cache_data(show_spinner=False)
def _load_repo_cartera() -> pd.DataFrame | None:
    try:
        if DATA_PARQUET.exists():
            return pd.read_parquet(DATA_PARQUET)
        if DATA_CSV.exists():
            return pd.read_csv(DATA_CSV)
        return None
    except Exception:
        try:
            if DATA_CSV.exists():
                return pd.read_csv(DATA_CSV)
        except Exception:
            return None
    return None


def main():
    st.set_page_config(page_title="Predicción independiente", page_icon="⚡", layout="centered")
    st.title("⚡ Calculadora independiente (predicción + envío)")
    st.caption(
        "Flujo independiente: solo ingresas features calculadas, subes carta y pagaré firmados, "
        "y se envía automáticamente a aprobación."
    )

    defaults = {
        "referencia": "",
        "ids": "",
        "bancos": "",
        "correo": "",
        "tipo_liquidacion": "",
        "enviar": "No",
        "pri_ult": 1.0,
        "ratio_pp": 0.0,
        "c_a": 1.0,
        "amount_total": 0.0,
        "pago_banco": 0.0,
        "primer_pago": 0.0,
        "ce_inicial": 0.0,
    }

    st.markdown("### Fuente de datos de entrada")
    source_mode = st.radio(
        "¿Cómo quieres cargar los inputs?",
        ["Manual", "Archivo (CSV/XLSX/JSON)", "Endpoint (JSON)"],
        horizontal=True,
    )

    up = None
    if source_mode == "Archivo (CSV/XLSX/JSON)":
        up = st.file_uploader("Sube un archivo con una fila", type=["csv", "xlsx", "json"], key="fuente_archivo")
        if up is not None:
            try:
                if up.name.lower().endswith(".csv"):
                    df_src = pd.read_csv(up)
                    record = df_src.iloc[0].to_dict()
                elif up.name.lower().endswith(".xlsx"):
                    df_src = pd.read_excel(up)
                    record = df_src.iloc[0].to_dict()
                else:
                    raw = json.loads(up.getvalue().decode("utf-8"))
                    if isinstance(raw, list):
                        record = dict(raw[0] if raw else {})
                    else:
                        record = dict(raw)
                defaults.update(_extract_case_data_from_record(record))
                st.success("Inputs cargados desde archivo. Puedes editarlos antes de enviar.")
            except Exception as exc:
                st.error(f"No se pudo leer el archivo: {exc}")
    elif source_mode == "Endpoint (JSON)":
        endpoint_url = st.text_input("URL endpoint (GET)")
        endpoint_token = st.text_input("Bearer token (opcional)", type="password")
        if st.button("Cargar desde endpoint", use_container_width=True):
            if not endpoint_url.strip():
                st.warning("Ingresa la URL del endpoint.")
            else:
                try:
                    headers = {}
                    if endpoint_token.strip():
                        headers["Authorization"] = f"Bearer {endpoint_token.strip()}"
                    resp = requests.get(endpoint_url.strip(), headers=headers, timeout=20)
                    resp.raise_for_status()
                    payload = resp.json()
                    if isinstance(payload, list):
                        record = dict(payload[0] if payload else {})
                    else:
                        record = dict(payload)
                    defaults.update(_extract_case_data_from_record(record))
                    st.success("Inputs cargados desde endpoint. Puedes editarlos antes de enviar.")
                except Exception as exc:
                    st.error(f"No se pudo cargar desde endpoint: {exc}")

    st.markdown("### Cartera (consulta automática desde el repositorio)")
    cartera_df = _load_repo_cartera()
    if cartera_df is not None and not cartera_df.empty:
        st.success("Cartera cargada desde `data/cartera_asignada_filtrada`.")
    else:
        st.error(
            "No encontré la cartera en el repositorio. "
            "Asegúrate de tener `data/cartera_asignada_filtrada.parquet` o `.csv`."
        )

    st.markdown("### Datos del caso")
    referencia = st.text_input("Referencia", value=str(defaults["referencia"]))
    ids = st.text_input("IDs deuda (separados por guion o coma)", value=str(defaults["ids"]))
    bancos = st.text_input("Banco(s)", value=str(defaults["bancos"]))
    correo = st.text_input("Correo corporativo", value=str(defaults["correo"]))
    st.caption("Tipo de liquidación se toma automáticamente desde el archivo de Cartera.")

    st.markdown("### Features ya calculadas")
    c1, c2 = st.columns(2)
    with c1:
        pri_ult = st.number_input("PRI-ULT (plazo)", min_value=1.0, step=1.0, value=float(defaults["pri_ult"]))
        c_a = st.number_input("C/A", min_value=0.01, step=0.01, value=float(defaults["c_a"]))
        amount_total = st.number_input("AMOUNT_TOTAL", min_value=0.0, step=1000.0, value=float(defaults["amount_total"]))
    with c2:
        ratio_pp = st.number_input("Ratio_PP", min_value=0.0, step=0.01, value=float(defaults["ratio_pp"]))
        pago_banco = st.number_input("PAGO BANCO", min_value=0.0, step=1000.0, value=float(defaults["pago_banco"]))
        primer_pago = st.number_input("Primer pago banco", min_value=0.0, step=1000.0, value=float(defaults["primer_pago"]))
        ce_inicial = st.number_input("CE inicial", min_value=0.0, step=1000.0, value=float(defaults["ce_inicial"]))

    st.markdown("### Soporte documental")
    carta_pagare_firmado = st.file_uploader("Carta + pagaré firmado (un solo PDF)", type=["pdf"])

    enviar_desde_archivo = str(defaults.get("enviar", "No")).strip()
    st.caption(f"Valor 'enviar' detectado en archivo/fuente: **{enviar_desde_archivo or 'No'}**")

    pred_col, send_col = st.columns(2)
    with pred_col:
        btn_predecir = st.button("🔮 Predecir", type="primary", use_container_width=True)
    with send_col:
        btn_enviar = st.button("📤 Enviar a aprobación", use_container_width=True)

    def _run_prediction(show_messages: bool = True):
        if not referencia.strip():
            if show_messages:
                st.warning("Para predecir debes ingresar la referencia.")
            return None
        tipo_liquidacion = _resolver_tipo_liquidacion_desde_cartera(cartera_df, referencia) if cartera_df is not None else ""
        if not tipo_liquidacion:
            tipo_liquidacion = "Tradicional"
            if show_messages:
                st.warning("No encontré la referencia en cartera; se asumirá Tipo de liquidación = Tradicional.")
        try:
            model = _load_model()
            features = {
                "PRI-ULT": float(pri_ult),
                "Ratio_PP": float(ratio_pp),
                "C/A": float(c_a),
                "AMOUNT_TOTAL": float(amount_total),
            }
            pred = _predict_recaudo(model, features, float(pago_banco), float(primer_pago))
            umbral = 0.8 if _is_traditional_liquidation(tipo_liquidacion) else 0.74
            st.session_state.ind_pred_value = float(pred)
            st.session_state.ind_tipo_liquidacion = str(tipo_liquidacion)
            st.session_state.ind_umbral = float(umbral)
            if show_messages:
                st.success(f"Predicción calculada: {pred:.4f}")
                st.caption(f"Criterio: umbral {umbral:.2f} → {'Aprobado' if pred >= umbral else 'No aprobado'}")
            return {"pred": float(pred), "tipo_liquidacion": str(tipo_liquidacion), "umbral": float(umbral)}
        except Exception as exc:
            if show_messages:
                st.error(f"No se pudo calcular la predicción: {exc}")
            return None

    def _run_send(pred_info: dict, show_messages: bool = True):
        pred = float(pred_info["pred"])
        tipo_liquidacion = str(pred_info["tipo_liquidacion"])
        umbral = float(pred_info["umbral"])
        aprobado = float(pred) >= umbral

        duplicate_check = _get_respuestas_duplicados_mes(referencia, ids)
        if not duplicate_check["ok"]:
            if show_messages:
                st.error(f"No fue posible validar duplicados: {duplicate_check['error']}")
            return
        duplicate_mode = duplicate_check["mode"]
        exact_rows_previas = duplicate_check.get("exact_rows", [])
        if duplicate_mode == "exact_duplicate":
            _marcar_anteriores_como_duplicado(exact_rows_previas)

        if duplicate_mode == "reference_duplicate":
            es_aprobado_bool = ""
            estado_aprobacion = ""
        else:
            es_aprobado_bool = "TRUE" if aprobado else "FALSE"
            estado_aprobacion = "Aprobado" if aprobado else "Rechazado"

        try:
            drive_service = _build_drive_service_from_session()
            if drive_service is None:
                if show_messages:
                    st.error("No hay sesión OAuth de Drive. Completa la autenticación antes de enviar.")
                return
            carta_pagare_link = _upload_pdf_to_drive(drive_service, carta_pagare_firmado, DRIVE_FOLDER_CARTA_PAGARE_ID)
            payload = {
                "timestamp": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
                "correo_electronico": correo.strip(),
                "referencia": referencia.strip(),
                "ids": re.sub(r"\s+", "", ids.strip()),
                "bancos": bancos.strip(),
                "carta_pagare_link": carta_pagare_link,
                "pantallazo_aceptacion_link": "No requerido (flujo independiente)",
                "condonacion_mensualidades": "No",
                "pantallazo_correo_condonacion_link": "",
                "comision_exito_total": float(amount_total),
                "ce_inicial": float(ce_inicial),
                "es_aprobado_bool": es_aprobado_bool,
                "estado_aprobacion": estado_aprobacion,
                "prediccion": round(float(pred), 4),
            }
            _append_respuesta(payload)
            if show_messages:
                st.success("Envío automático a aprobación realizado correctamente.")
                if duplicate_mode == "exact_duplicate":
                    st.info("Se detectó duplicado exacto: anterior marcado como Duplicado.")
                elif duplicate_mode == "reference_duplicate":
                    st.info("Referencia repetida con otro ID: se envió sin check de aprobación ni comentario.")
                st.caption(f"Carta + pagaré cargado: {carta_pagare_link}")
                st.caption(f"Tipo liquidación (cartera): {tipo_liquidacion}")
                st.caption(f"Criterio: umbral {umbral:.2f} → {'Aprobado' if aprobado else 'No aprobado'}")
        except Exception as exc:
            if show_messages:
                st.error(f"No se pudo completar el envío: {exc}")

    if btn_predecir:
        _run_prediction(show_messages=True)

    if btn_enviar:
        if not referencia.strip() or not ids.strip() or not bancos.strip() or not correo.strip():
            st.error("Completa referencia, IDs, bancos y correo.")
            return
        if not correo.strip().lower().endswith("@gobravo.com.co"):
            st.error("El correo debe terminar en @gobravo.com.co")
            return
        if carta_pagare_firmado is None:
            st.error("Debes adjuntar carta + pagaré firmado en un solo PDF.")
            return
        pred = st.session_state.get("ind_pred_value")
        if pred is None:
            st.warning("Primero debes presionar **Predecir**.")
            return
        if cartera_df is None or cartera_df.empty:
            st.error("No hay cartera disponible para obtener el Tipo de liquidación.")
            return
        pred_info = {
            "pred": float(pred),
            "tipo_liquidacion": str(st.session_state.get("ind_tipo_liquidacion", "Tradicional")),
            "umbral": float(st.session_state.get("ind_umbral", 0.8)),
        }
        _run_send(pred_info, show_messages=True)

    # Modo automático por archivo: predice siempre y envía solo si la columna enviar dice Sí.
    auto_flag = _norm(enviar_desde_archivo) in {"si", "sí", "yes", "true", "1"}
    if source_mode == "Archivo (CSV/XLSX/JSON)" and up is not None and referencia.strip():
        auto_sig = f"{up.name}|{getattr(up, 'size', 0)}|{referencia}|{ids}|{amount_total}|{auto_flag}"
        if st.session_state.get("ind_auto_sig") != auto_sig:
            pred_info = _run_prediction(show_messages=True)
            if pred_info and auto_flag:
                if not correo.strip().lower().endswith("@gobravo.com.co"):
                    st.error("Modo automático: correo inválido para enviar.")
                elif carta_pagare_firmado is None:
                    st.warning("Modo automático: faltó adjuntar carta + pagaré PDF, solo se calculó la predicción.")
                else:
                    _run_send(pred_info, show_messages=True)
            st.session_state.ind_auto_sig = auto_sig


if __name__ == "__main__":
    main()
