import ast
import json
import os
import re
from datetime import datetime
from io import BytesIO
from pathlib import Path

import gspread
import numpy as np
import pandas as pd
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


def main():
    st.set_page_config(page_title="Predicción independiente", page_icon="⚡", layout="centered")
    st.title("⚡ Calculadora independiente (predicción + envío)")
    st.caption(
        "Flujo independiente: solo ingresas features calculadas, subes carta y pagaré firmados, "
        "y se envía automáticamente a aprobación."
    )

    with st.form("form_prediccion_independiente"):
        st.markdown("### Datos del caso")
        referencia = st.text_input("Referencia")
        ids = st.text_input("IDs deuda (separados por guion o coma)")
        bancos = st.text_input("Banco(s)")
        correo = st.text_input("Correo corporativo")
        tipo_liquidacion = st.selectbox("Tipo de liquidación", ["No tradicional", "Tradicional"])
        condonacion = st.selectbox("Condonación de mensualidades", ["No", "Si"])

        st.markdown("### Features ya calculadas")
        c1, c2 = st.columns(2)
        with c1:
            pri_ult = st.number_input("PRI-ULT (plazo)", min_value=1.0, step=1.0, value=1.0)
            c_a = st.number_input("C/A", min_value=0.01, step=0.01, value=1.0)
            amount_total = st.number_input("AMOUNT_TOTAL", min_value=0.0, step=1000.0, value=0.0)
        with c2:
            ratio_pp = st.number_input("Ratio_PP", min_value=0.0, step=0.01, value=0.0)
            pago_banco = st.number_input("PAGO BANCO", min_value=0.0, step=1000.0, value=0.0)
            primer_pago = st.number_input("Primer pago banco", min_value=0.0, step=1000.0, value=0.0)
            ce_inicial = st.number_input("CE inicial", min_value=0.0, step=1000.0, value=0.0)

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
