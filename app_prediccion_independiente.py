import ast
import json
import os
import re
from datetime import datetime
from io import BytesIO
from pathlib import Path

import gspread
import pandas as pd
import requests
import streamlit as st
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from joblib import load

MODEL_PATH = Path("mlp_recaudo_pipeline.joblib")
GOOGLE_SHEET_ID = "1Aahltn7TSRf6ZpTpS-vPgpB89hO-r5KxpAhqKAPXziE"
GOOGLE_SHEET_TAB_RESPUESTAS = "Respuestas Estr"
GOOGLE_SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
DRIVE_FOLDER_CARTA_PAGARE_ID = "1nEo1iZWzFySJX_90crO9tjTTX1Cr_yVxs-xyn1C0TMu78Jt8rs2QYqVXs_wgzxEvn1AU0nMk"


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
        "tipo_liquidacion": str(pick("tipo_liquidacion", "tipo de liquidacion", default="No tradicional")).strip() or "No tradicional",
        "condonacion": str(pick("condonacion", "condonacion_mensualidades", default="No")).strip() or "No",
        "pri_ult": _to_float(pick("pri-ult", "pri_ult", "plazo"), 1.0),
        "ratio_pp": _to_float(pick("ratio_pp", "ratio pp"), 0.0),
        "c_a": _to_float(pick("c/a", "c_a", "cuota_apartado"), 1.0),
        "amount_total": _to_float(pick("amount_total", "comision_exito_total"), 0.0),
        "pago_banco": _to_float(pick("pago_banco", "pago banco"), 0.0),
        "primer_pago": _to_float(pick("primer_pago", "primer pago", "primer_pago_banco"), 0.0),
        "ce_inicial": _to_float(pick("ce_inicial", "ce inicial"), 0.0),
    }


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
        "tipo_liquidacion": "No tradicional",
        "condonacion": "No",
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

    with st.form("form_prediccion_independiente"):
        st.markdown("### Datos del caso")
        referencia = st.text_input("Referencia", value=str(defaults["referencia"]))
        ids = st.text_input("IDs deuda (separados por guion o coma)", value=str(defaults["ids"]))
        bancos = st.text_input("Banco(s)", value=str(defaults["bancos"]))
        correo = st.text_input("Correo corporativo", value=str(defaults["correo"]))
        tipo_index = 1 if _norm(str(defaults["tipo_liquidacion"])) == "tradicional" else 0
        tipo_liquidacion = st.selectbox("Tipo de liquidación", ["No tradicional", "Tradicional"], index=tipo_index)
        cond_index = 1 if _norm(str(defaults["condonacion"])) in {"si", "sí"} else 0
        condonacion = st.selectbox("Condonación de mensualidades", ["No", "Si"], index=cond_index)

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

        st.markdown("### Adjuntos requeridos")
        carta_firmada = st.file_uploader("Carta firmada (PDF)", type=["pdf"])
        pagare_firmado = st.file_uploader("Pagaré firmado (PDF)", type=["pdf"])

        submit = st.form_submit_button("🔮 Predecir y enviar a aprobación", use_container_width=True)

    if not submit:
        return

    if not referencia.strip() or not ids.strip() or not bancos.strip() or not correo.strip():
        st.error("Completa referencia, IDs, bancos y correo.")
        return
    if not correo.strip().lower().endswith("@gobravo.com.co"):
        st.error("El correo debe terminar en @gobravo.com.co")
        return
    if carta_firmada is None or pagare_firmado is None:
        st.error("Debes adjuntar carta y pagaré firmados (PDF).")
        return

    try:
        model = _load_model()
        features = {
            "PRI-ULT": float(pri_ult),
            "Ratio_PP": float(ratio_pp),
            "C/A": float(c_a),
            "AMOUNT_TOTAL": float(amount_total),
        }
        pred = _predict_recaudo(model, features, float(pago_banco), float(primer_pago))

        _, drive_service = _google_clients()
        carta_link = _upload_pdf_to_drive(drive_service, carta_firmada, DRIVE_FOLDER_CARTA_PAGARE_ID)
        pagare_link = _upload_pdf_to_drive(drive_service, pagare_firmado, DRIVE_FOLDER_CARTA_PAGARE_ID)
        carta_pagare_link = " | ".join([link for link in [carta_link, pagare_link] if link])

        umbral = 0.8 if _is_traditional_liquidation(tipo_liquidacion) else 0.74
        aprobado = pred >= umbral

        payload = {
            "timestamp": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
            "correo_electronico": correo.strip(),
            "referencia": referencia.strip(),
            "ids": re.sub(r"\s+", "", ids.strip()),
            "bancos": bancos.strip(),
            "carta_pagare_link": carta_pagare_link,
            "pantallazo_aceptacion_link": "No requerido (flujo independiente)",
            "condonacion_mensualidades": "Sí" if condonacion == "Si" else "No",
            "pantallazo_correo_condonacion_link": "",
            "comision_exito_total": float(amount_total),
            "ce_inicial": float(ce_inicial),
            "es_aprobado_bool": "TRUE" if aprobado else "FALSE",
            "estado_aprobacion": "Aprobado" if aprobado else "Rechazado",
            "prediccion": round(float(pred), 4),
        }

        _append_respuesta(payload)

        st.success(f"Predicción calculada: {pred:.4f}")
        st.success("Envío automático a aprobación realizado correctamente.")
        st.caption(f"Carta: {carta_link}")
        st.caption(f"Pagaré: {pagare_link}")
        st.caption(f"Criterio: umbral {umbral:.2f} → {'Aprobado' if aprobado else 'No aprobado'}")
    except Exception as exc:
        st.error(f"No se pudo completar el envío: {exc}")


if __name__ == "__main__":
    main()
