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

from recaudo_rules import LOW_RATIO_PP_WARNING, apply_low_ratio_pp_cap

# Nota de entorno:
# Esta app puede instalarse de forma aislada con `requirements_independiente.txt`
# sin tocar el `requirements.txt` principal de la calculadora original.
MODEL_PATH = Path(__file__).parent / "mlp_recaudo_pipeline.joblib"
DATA_PARQUET = Path(__file__).parent / "data/cartera_asignada_filtrada.parquet"
DATA_CSV = Path(__file__).parent / "data/cartera_asignada_filtrada.csv"
GOOGLE_SHEET_ID = "1Aahltn7TSRf6ZpTpS-vPgpB89hO-r5KxpAhqKAPXziE"
GOOGLE_SHEET_TAB = "Historico Calculadora"
GOOGLE_SHEET_TAB_RESPUESTAS = "Respuestas Estr"
GOOGLE_SHEET_HEADERS = [
    "fecha",
    "referencia",
    "ids_deuda",
    "plazo",
    "ratio_pp",
    "cuota_apartado",
    "amount_total",
    "prediccion",
]
GOOGLE_SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
DRIVE_FOLDER_CARTA_PAGARE_ID = "1nEo1iZWzFySJX_90crO9tjTTX1Cr_yVxs-xyn1C0TMu78Jt8rs2QYqVXs_wgzxEvn1AU0nMk"
GOOGLE_DRIVE_UPLOAD_SCOPES = ["https://www.googleapis.com/auth/drive.file"]
BATCH_TEMPLATE_COLUMNS = [
    "referencia",
    "ids",
    "bancos",
    "correo",
    "tipo_liquidacion",
    "url1",
    "url2",
    "pri_ult",
    "ratio_pp",
    "c_a",
    "amount_total",
    "pago_banco",
    "primer_pago",
    "ce_inicial",
]
BATCH_RESULT_COLUMNS = [
    "fila_origen",
    "referencia_usada",
    "ids_usados",
    "pri_ult_usado",
    "ratio_pp_usado",
    "c_a_usado",
    "amount_total_usado",
    "pago_banco_usado",
    "primer_pago_usado",
    "ce_inicial_usado",
    "tipo_liquidacion_resuelto",
    "prediccion",
    "umbral_aprobacion",
    "aprobado_estimado",
    "warning_ratio_pp",
    "error_prediccion",
]

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


def _append_historico_calculadora(row_data: dict):
    sheets_client, _ = _google_clients()
    ws = sheets_client.open_by_key(GOOGLE_SHEET_ID).worksheet(GOOGLE_SHEET_TAB)
    current_headers = ws.row_values(1)
    if current_headers[: len(GOOGLE_SHEET_HEADERS)] != GOOGLE_SHEET_HEADERS:
        end_col = _col_index_to_letter(len(GOOGLE_SHEET_HEADERS))
        ws.update(f"A1:{end_col}1", [GOOGLE_SHEET_HEADERS])

    ws.append_row(
        [row_data.get(header, "") for header in GOOGLE_SHEET_HEADERS],
        value_input_option="USER_ENTERED",
    )


def guardar_historico_calculadora(referencia, ids, features: dict, prediccion: float) -> dict:
    row_data = {
        "fecha": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "referencia": str(referencia),
        "ids_deuda": re.sub(r"\s+", "", str(ids or "")),
        "plazo": features.get("PRI-ULT"),
        "ratio_pp": features.get("Ratio_PP"),
        "cuota_apartado": features.get("C/A"),
        "amount_total": features.get("AMOUNT_TOTAL"),
        "prediccion": prediccion,
    }
    try:
        _append_historico_calculadora(row_data)
        return {
            "ok": True,
            "sheet_destination": f"Google Sheets > {GOOGLE_SHEET_TAB}",
            "error": None,
            "row": row_data,
        }
    except Exception as exc:
        return {
            "ok": False,
            "sheet_destination": f"Google Sheets > {GOOGLE_SHEET_TAB}",
            "error": str(exc),
            "row": row_data,
        }


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
            normalized[idx] = ""
        elif "pantallazo" in h and "correo" in h and "condonacion" in h:
            normalized[idx] = ""
        elif "total de comision" in h:
            normalized[idx] = payload.get("comision_exito_total", "")
        elif "1er pago comision" in h or "% 1er pago comision" in h:
            normalized[idx] = payload.get("ratio_pp", "")
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


def _predict_recaudo_result(model, features: dict) -> tuple[float, bool]:
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

    yhat = min(yhat, 0.99)
    return apply_low_ratio_pp_cap(yhat, features["Ratio_PP"])


def _predict_recaudo(model, features: dict, pago_banco: float, primer_pago: float) -> float:
    """Mantiene la interfaz anterior; la regla prioritaria usa Ratio_PP."""
    prediction, _ = _predict_recaudo_result(model, features)
    return prediction


def _to_float(value, default=0.0) -> float:
    try:
        if value is None or (isinstance(value, str) and not value.strip()):
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _is_blank_value(value) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except Exception:
        pass
    return isinstance(value, str) and not value.strip()


def _is_empty_record(record) -> bool:
    record = _safe_to_dict(record)
    return not any(not _is_blank_value(value) for value in record.values())


def _safe_to_dict(data):
    """Convierte CUALQUIER cosa a diccionario de forma segura"""
    if isinstance(data, dict):
        return data
    
    # Estrategias de conversión en orden
    strategies = [
        # Si es bytes, decodificar
        lambda d: d.decode('utf-8') if isinstance(d, bytes) else None,
        # Si es string, intentar JSON
        lambda d: json.loads(d) if isinstance(d, str) and d.strip() else None,
        # Si es string, intentar literal_eval
        lambda d: ast.literal_eval(d) if isinstance(d, str) else None,
        # Si es lista, convertir a dict
        lambda d: dict(d) if isinstance(d, list) else None,
        # Cualquier otro, convertir a string y luego JSON
        lambda d: json.loads(str(d)) if d is not None else None,
    ]
    
    for strategy in strategies:
        try:
            result = strategy(data)
            if isinstance(result, dict):
                return result
            elif isinstance(result, str) and result.strip():
                # Si dio string, intentar JSON nuevamente
                try:
                    return json.loads(result)
                except:
                    pass
        except:
            continue
    
    return {}


def _extract_case_data_from_record(record):
    # Convertir cualquier cosa a dict
    record = _safe_to_dict(record)
    
    # Resto de tu función original
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
        "url1": str(pick("url1", "url_1", "link1", "link_1", "carta_pagare_link", default="")).strip(),
        "url2": str(pick("url2", "url_2", "link2", "link_2", "pantallazo_aceptacion_link", default="")).strip(),
        "pri_ult": _to_float(pick("pri-ult", "pri_ult", "plazo"), 1.0),
        "ratio_pp": _to_float(pick("ratio_pp", "ratio pp"), 0.0),
        "c_a": _to_float(pick("c/a", "c_a", "cuota_apartado"), 1.0),
        "amount_total": _to_float(pick("amount_total", "comision_exito_total"), 0.0),
        "pago_banco": _to_float(pick("pago_banco", "pago banco"), 0.0),
        "primer_pago": _to_float(pick("primer_pago", "primer pago", "primer_pago_banco"), 0.0),
        "ce_inicial": _to_float(pick("ce_inicial", "ce inicial"), 0.0),
    }


def _read_tabular_upload(uploaded_file) -> pd.DataFrame:
    if uploaded_file is None:
        raise ValueError("Debes cargar un archivo.")

    try:
        uploaded_file.seek(0)
    except Exception:
        pass

    name = str(getattr(uploaded_file, "name", "") or "").lower()
    if name.endswith(".csv"):
        return pd.read_csv(uploaded_file, sep=None, engine="python")
    if name.endswith(".xlsx"):
        return pd.read_excel(uploaded_file)
    raise ValueError("Solo se permiten archivos CSV o XLSX para esta carga.")


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

def parse_response(response: dict, response_format: str) -> dict | str:
    if response_format == "string":
        return json.dumps(response)
    return response

def run_prediction(params: dict, cartera_df: pd.DataFrame | None = None, *, save_history: bool = True) -> dict:
    """Calcula la predicción desde parámetros externos (por ejemplo, JSON de Berex).

    Retorna un diccionario con `ok=True` y los datos de predicción, o `ok=False` con
    `error` si falta información o ocurre un problema al calcular.
    """
    record = _safe_to_dict(params)
    case = _extract_case_data_from_record(params or {})
    referencia = str(case.get("referencia", "")).strip()
    if not referencia:
        response = {"ok": False, "error": "Para predecir debes enviar la referencia."}
        return parse_response(response, record.get("format"))

    tipo_liquidacion = str(case.get("tipo_liquidacion", "")).strip()
    if not tipo_liquidacion:
        if cartera_df is None:
            cartera_df = _load_repo_cartera()
        tipo_liquidacion = _resolver_tipo_liquidacion_desde_cartera(cartera_df, referencia) if cartera_df is not None else ""
    tipo_liquidacion_encontrado = bool(tipo_liquidacion)
    if not tipo_liquidacion:
        tipo_liquidacion = "Tradicional"

    try:
        model = _load_model()
        features = {
            "PRI-ULT": float(case["pri_ult"]),
            "Ratio_PP": float(case["ratio_pp"]),
            "C/A": float(case["c_a"]),
            "AMOUNT_TOTAL": float(case["amount_total"]),
        }
        pred, low_ratio_cap_applied = _predict_recaudo_result(model, features)
        umbral = 0.8 if _is_traditional_liquidation(tipo_liquidacion) else 0.74
        if save_history:
            historico_result = guardar_historico_calculadora(
                referencia=referencia,
                ids=case.get("ids", ""),
                features=features,
                prediccion=float(pred),
            )
        else:
            historico_result = {
                "ok": True,
                "sheet_destination": "",
                "error": None,
                "row": None,
                "skipped": True,
            }
        response = {
            "ok": True,
            "pred": float(pred),
            "tipo_liquidacion": str(tipo_liquidacion),
            "tipo_liquidacion_encontrado": tipo_liquidacion_encontrado,
            "umbral": float(umbral),
            "aprobado": float(pred) >= float(umbral),
            "low_ratio_cap_applied": low_ratio_cap_applied,
            "features": features,
            "historico": historico_result,
        }

        return parse_response(response, record.get("format"))
    except Exception as exc:
        response = {"ok": False, "error": f"No se pudo calcular la predicción: {exc}"}
        return parse_response(response, record.get("format"))


def run_send(params: dict, pred_info: dict | None = None, cartera_df: pd.DataFrame | None = None) -> dict:
    """Calcula (si hace falta) y envía a aprobación usando parámetros externos.

    `params` puede venir directamente del JSON de Berex. Si `pred_info` no se envía,
    esta función ejecuta `run_prediction` antes de guardar el resultado en Sheets.
    """
    record = _safe_to_dict(params)
    case = _extract_case_data_from_record(params or {})
    referencia = str(case.get("referencia", "")).strip()
    ids = str(case.get("ids", "")).strip()
    bancos = str(case.get("bancos", "")).strip()
    correo = str(case.get("correo", "")).strip()
    url1 = str(case.get("url1", "")).strip()
    url2 = str(case.get("url2", "")).strip()

    missing = [
        label
        for label, value in {
            "referencia": referencia,
            "ids": ids,
            "bancos": bancos,
            "correo": correo,
        }.items()
        if not value
    ]
    if missing:
        response = {"ok": False, "sent": False, "error": f"Faltan campos requeridos: {', '.join(missing)}."}
        return parse_response(response, record.get("format"))
    if not correo.lower().endswith("@gobravo.com.co"):
        response = {"ok": False, "sent": False, "error": "El correo debe terminar en @gobravo.com.co."}
        return parse_response(response, record.get("format"))

    pred_info = _safe_to_dict(pred_info)

    if pred_info is None or len(pred_info) == 0:
        pred_info = run_prediction(params, cartera_df=cartera_df)
    if not pred_info or not pred_info.get("ok"):
        response = {
            "ok": False,
            "sent": False,
            "error": (pred_info or {}).get("error", "No fue posible calcular la predicción antes de enviar."),
        }
        return parse_response(response, record.get("format"))
    pred = float(pred_info["pred"])
    tipo_liquidacion = str(pred_info["tipo_liquidacion"])
    umbral = float(pred_info["umbral"])
    aprobado = float(pred) >= umbral
    if "historico" not in pred_info:
        features = pred_info.get("features") or {
            "PRI-ULT": float(case["pri_ult"]),
            "Ratio_PP": float(case["ratio_pp"]),
            "C/A": float(case["c_a"]),
            "AMOUNT_TOTAL": float(case["amount_total"]),
        }
        pred_info["historico"] = guardar_historico_calculadora(
            referencia=referencia,
            ids=ids,
            features=features,
            prediccion=float(pred),
        )

    duplicate_check = _get_respuestas_duplicados_mes(referencia, ids)
    if not duplicate_check["ok"]:
        response = {
            "ok": False,
            "sent": False,
            "prediction": pred_info,
            "error": f"No fue posible validar duplicados: {duplicate_check['error']}",
        }
        return parse_response(response, record.get("format"))
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
        payload = {
            "timestamp": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            "correo_electronico": correo,
            "referencia": referencia,
            "ids": re.sub(r"\s+", "", ids),
            "bancos": bancos,
            "carta_pagare_link": url1,
            "pantallazo_aceptacion_link": url2,
            "condonacion_mensualidades": "No",
            "pantallazo_correo_condonacion_link": "",
            "comision_exito_total": float(case["amount_total"]),
            "ce_inicial": float(case["ce_inicial"]),
            "ratio_pp": float(case["ratio_pp"]),
            "es_aprobado_bool": es_aprobado_bool,
            "estado_aprobacion": estado_aprobacion,
            "prediccion": round(float(pred), 4),
        }
        _append_respuesta(payload)
        response = {
            "ok": True,
            "sent": True,
            "prediction": pred_info,
            "duplicate_mode": duplicate_mode,
            "aprobado": aprobado,
            "payload": payload,
        }
        return parse_response(response, record.get("format"))
    except Exception as exc:
        response = {"ok": False, "sent": False, "prediction": pred_info, "error": f"No se pudo completar el envío: {exc}"}
        return parse_response(response, record.get("format"))


def process_prediction_request(params: dict, cartera_df: pd.DataFrame | None = None) -> dict:
    """Ejecuta el flujo completo para integraciones JSON.

    Si `enviar` viene como Sí/Yes/True/1, calcula y envía a aprobación. En cualquier
    otro caso solo calcula la predicción y retorna el resultado.
    """
    case = _extract_case_data_from_record(params or {})
    should_send = _norm(case.get("enviar", "No")) in {"si", "sí", "yes", "true", "1"}
    pred_info = run_prediction(params, cartera_df=cartera_df)
    if not pred_info.get("ok") or not should_send:
        return {"ok": pred_info.get("ok", False), "sent": False, "prediction": pred_info}
    return run_send(params, pred_info=pred_info, cartera_df=cartera_df)


def _build_batch_template_df() -> pd.DataFrame:
    return pd.DataFrame(columns=BATCH_TEMPLATE_COLUMNS)


def _build_batch_template_notes_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"campo": "referencia", "obligatorio": "Si", "descripcion": "Referencia del caso."},
            {"campo": "ids", "obligatorio": "Recomendado", "descripcion": "ID o IDs de deuda del caso."},
            {"campo": "bancos", "obligatorio": "No", "descripcion": "Se conserva por compatibilidad con el modo individual."},
            {"campo": "correo", "obligatorio": "No", "descripcion": "Se conserva por compatibilidad con el modo individual."},
            {"campo": "tipo_liquidacion", "obligatorio": "No", "descripcion": "Si va vacio, se intenta resolver desde la cartera."},
            {"campo": "url1", "obligatorio": "No", "descripcion": "No se usa en la prediccion masiva; queda por compatibilidad."},
            {"campo": "url2", "obligatorio": "No", "descripcion": "No se usa en la prediccion masiva; queda por compatibilidad."},
            {"campo": "pri_ult", "obligatorio": "Si", "descripcion": "Plazo usado por el modelo."},
            {"campo": "ratio_pp", "obligatorio": "Si", "descripcion": "Ratio del primer pago."},
            {"campo": "c_a", "obligatorio": "Si", "descripcion": "Valor C/A calculado para el caso."},
            {"campo": "amount_total", "obligatorio": "Si", "descripcion": "Comision exito total."},
            {"campo": "pago_banco", "obligatorio": "No", "descripcion": "Se conserva para mantener el mismo formato del modo individual."},
            {"campo": "primer_pago", "obligatorio": "No", "descripcion": "Se conserva para mantener el mismo formato del modo individual."},
            {"campo": "ce_inicial", "obligatorio": "No", "descripcion": "Se conserva para mantener el mismo formato del modo individual."},
        ]
    )


def _build_excel_bytes(sheets: dict[str, pd.DataFrame]) -> bytes:
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            safe_sheet_name = str(sheet_name or "Hoja")[:31]
            df.to_excel(writer, sheet_name=safe_sheet_name, index=False)
    buffer.seek(0)
    return buffer.getvalue()


def _build_batch_template_excel_bytes() -> bytes:
    return _build_excel_bytes(
        {
            "plantilla": _build_batch_template_df(),
            "instrucciones": _build_batch_template_notes_df(),
        }
    )


def _build_batch_template_csv_bytes() -> bytes:
    return _build_batch_template_df().to_csv(index=False).encode("utf-8-sig")


def _build_batch_results(records_df: pd.DataFrame, cartera_df: pd.DataFrame | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    if records_df is None or records_df.empty:
        raise ValueError("El archivo no contiene filas para procesar.")

    results = []
    skipped_blank_rows = 0

    for source_row_number, (_, row) in enumerate(records_df.iterrows(), start=2):
        raw_record = row.to_dict()
        if _is_empty_record(raw_record):
            skipped_blank_rows += 1
            continue

        case = _extract_case_data_from_record(raw_record)
        pred_info = run_prediction(raw_record, cartera_df=cartera_df, save_history=False)
        result_row = dict(raw_record)
        result_row.update(
            {
                "fila_origen": source_row_number,
                "referencia_usada": case.get("referencia", ""),
                "ids_usados": case.get("ids", ""),
                "pri_ult_usado": float(case.get("pri_ult", 0.0)),
                "ratio_pp_usado": float(case.get("ratio_pp", 0.0)),
                "c_a_usado": float(case.get("c_a", 0.0)),
                "amount_total_usado": float(case.get("amount_total", 0.0)),
                "pago_banco_usado": float(case.get("pago_banco", 0.0)),
                "primer_pago_usado": float(case.get("primer_pago", 0.0)),
                "ce_inicial_usado": float(case.get("ce_inicial", 0.0)),
                "tipo_liquidacion_resuelto": pred_info.get("tipo_liquidacion", "") if pred_info.get("ok") else "",
                "prediccion": round(float(pred_info["pred"]), 4) if pred_info.get("ok") else None,
                "umbral_aprobacion": float(pred_info["umbral"]) if pred_info.get("ok") else None,
                "aprobado_estimado": "Si" if pred_info.get("ok") and pred_info.get("aprobado") else ("No" if pred_info.get("ok") else ""),
                "warning_ratio_pp": LOW_RATIO_PP_WARNIN