import io
import re
from typing import Optional

import numpy as np
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Laboratorio de Flujo de Recaudo", page_icon="🧪", layout="wide")
st.title("🧪 Laboratorio de Flujo de Recaudo")
st.caption(
    "Entorno de prueba separado de `app.py` para diseñar y validar un flujo editable "
    "sin tocar la app que hoy está en uso."
)

EXPECTED_COLUMNS = ["Fecha", "Cantidad", "Concepto"]
CONCEPTO_MAP = {
    "pago 1 a entidad financiera": "Pago a Entidad Financiera",
    "pago 2 a entidad financiera": "Pago a Entidad Financiera",
    "pago 3 a entidad financiera": "Pago a Entidad Financiera",
    "comision resuelve": "Comisión Resuelve",
    "comisión resuelve": "Comisión Resuelve",
}


def normalize_text(value: object) -> str:
    text = str(value or "").strip().lower()
    text = (
        text.replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
    )
    text = re.sub(r"\s+", " ", text)
    return text


def parse_cop_amount(value: object) -> float:
    if pd.isna(value):
        return 0.0
    text = str(value).strip().upper()
    text = text.replace("COP", "").replace("$", "")
    text = text.replace(" ", "")
    if not text:
        return 0.0

    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    elif "," in text:
        text = text.replace(".", "").replace(",", ".")
    else:
        text = text.replace(",", "")

    return float(text)


def format_cop(value: float) -> str:
    return f"${value:,.2f} COP".replace(",", "_").replace(".", ",").replace("_", ".")


def suggest_concept(value: object) -> str:
    normalized = normalize_text(value)
    if normalized in CONCEPTO_MAP:
        return CONCEPTO_MAP[normalized]
    if "entidad financiera" in normalized:
        return "Pago a Entidad Financiera"
    if "comision" in normalized or "comisión" in normalized:
        return "Comisión Resuelve"
    return str(value or "").strip()


def detect_tipo(value: object) -> str:
    concept = normalize_text(value)
    if "pago" in concept:
        return "Pago"
    if "comision" in concept or "comisión" in concept:
        return "Comisión"
    return "Otro"


def coerce_columns(df: pd.DataFrame) -> pd.DataFrame:
    renamed = {col: col.strip() for col in df.columns}
    df = df.rename(columns=renamed).copy()
    missing = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas requeridas: {', '.join(missing)}")

    result = df[EXPECTED_COLUMNS].copy()
    result["Fecha"] = pd.to_datetime(result["Fecha"], errors="coerce", dayfirst=True)
    result["Cantidad"] = result["Cantidad"].apply(parse_cop_amount)
    result["Concepto"] = result["Concepto"].fillna("").astype(str).str.strip()
    result["Concepto sugerido"] = result["Concepto"].apply(suggest_concept)
    result["Tipo"] = result["Concepto sugerido"].apply(detect_tipo)
    result["Observación"] = np.where(
        result["Fecha"].isna(),
        "Revisar fecha",
        np.where(result["Cantidad"] <= 0, "Revisar cantidad", "OK"),
    )
    return result


def load_table_from_text(raw_text: str) -> pd.DataFrame:
    buffer = io.StringIO(raw_text.strip())
    return pd.read_csv(buffer, sep=None, engine="python")


with st.sidebar:
    st.markdown("### Cómo usar este laboratorio")
    st.write(
        "1. Sube un CSV/XLSX o pega una tabla.\n"
        "2. Edita filas en la grilla.\n"
        "3. Revisa cómo se recalculan conceptos, tipos y totales automáticamente."
    )
    reset = st.button("Reiniciar laboratorio")
    if reset:
        for key in ["preview_df", "source_mode"]:
            st.session_state.pop(key, None)
        st.rerun()

st.markdown("### 1) Cargar información")
source_mode = st.radio(
    "Origen de prueba",
    options=["Ejemplo", "Pegar tabla", "Subir archivo"],
    horizontal=True,
)

sample_df = pd.DataFrame(
    [
        ["14/03/2026", "$3.170.000,00 COP", "Pago 1 a Entidad Financiera"],
        ["14/03/2026", "$1.000.000,00 COP", "Comisión Resuelve"],
        ["15/04/2026", "$3.170.000,00 COP", "Pago 2 a Entidad Financiera"],
        ["15/05/2026", "$3.170.000,00 COP", "Pago 3 a Entidad Financiera"],
    ],
    columns=EXPECTED_COLUMNS,
)

loaded_df: Optional[pd.DataFrame] = None

if source_mode == "Ejemplo":
    loaded_df = sample_df
elif source_mode == "Pegar tabla":
    raw_text = st.text_area(
        "Pega una tabla con columnas Fecha, Cantidad y Concepto",
        height=180,
        placeholder="Fecha,Cantidad,Concepto\n14/03/2026,$3.170.000,00 COP,Pago 1 a Entidad Financiera",
    )
    if raw_text.strip():
        try:
            loaded_df = load_table_from_text(raw_text)
        except Exception as exc:
            st.error(f"No pude leer la tabla pegada: {exc}")
elif source_mode == "Subir archivo":
    uploaded = st.file_uploader("Sube CSV o XLSX", type=["csv", "xlsx"])
    if uploaded is not None:
        try:
            if uploaded.name.lower().endswith(".csv"):
                loaded_df = pd.read_csv(uploaded)
            else:
                loaded_df = pd.read_excel(uploaded)
        except Exception as exc:
            st.error(f"No pude leer el archivo: {exc}")

if loaded_df is not None:
    try:
        st.session_state.preview_df = coerce_columns(loaded_df)
    except Exception as exc:
        st.error(str(exc))

if "preview_df" not in st.session_state:
    st.info("Carga datos de ejemplo, pega una tabla o sube un archivo para iniciar el flujo.")
    st.stop()

st.markdown("### 2) Editar y normalizar")
edited_df = st.data_editor(
    st.session_state.preview_df,
    use_container_width=True,
    num_rows="dynamic",
    column_config={
        "Fecha": st.column_config.DateColumn("Fecha", format="DD/MM/YYYY"),
        "Cantidad": st.column_config.NumberColumn("Cantidad", format="%.2f", min_value=0.0),
        "Concepto": st.column_config.TextColumn("Concepto original"),
        "Concepto sugerido": st.column_config.TextColumn("Concepto estandarizado"),
        "Tipo": st.column_config.SelectboxColumn("Tipo", options=["Pago", "Comisión", "Otro"]),
        "Observación": st.column_config.TextColumn("Observación", disabled=True),
    },
    disabled=["Observación"],
    key="editor_preview",
)

working_df = edited_df.copy()
working_df["Cantidad"] = working_df["Cantidad"].apply(parse_cop_amount)
working_df["Concepto sugerido"] = working_df["Concepto sugerido"].apply(suggest_concept)
working_df["Tipo"] = working_df["Concepto sugerido"].apply(detect_tipo)
working_df["Observación"] = np.where(
    pd.to_datetime(working_df["Fecha"], errors="coerce", dayfirst=True).isna(),
    "Revisar fecha",
    np.where(working_df["Cantidad"] <= 0, "Revisar cantidad", "OK"),
)
st.session_state.preview_df = working_df

st.markdown("### 3) Resumen reactivo")
total_general = float(working_df["Cantidad"].sum())
resumen_tipo = (
    working_df.groupby("Tipo", dropna=False)["Cantidad"]
    .sum()
    .reset_index()
    .sort_values("Cantidad", ascending=False)
)
resumen_concepto = (
    working_df.groupby("Concepto sugerido", dropna=False)["Cantidad"]
    .agg(["count", "sum"])
    .reset_index()
    .rename(columns={"count": "Movimientos", "sum": "Total"})
    .sort_values("Total", ascending=False)
)

c1, c2, c3 = st.columns(3)
c1.metric("Total general", format_cop(total_general))
c2.metric("Pagos", format_cop(float(resumen_tipo.loc[resumen_tipo["Tipo"] == "Pago", "Cantidad"].sum())))
c3.metric("Comisiones", format_cop(float(resumen_tipo.loc[resumen_tipo["Tipo"] == "Comisión", "Cantidad"].sum())))

col_left, col_right = st.columns([1, 1])
with col_left:
    st.write("#### Totales por tipo")
    st.dataframe(resumen_tipo, use_container_width=True, hide_index=True)
with col_right:
    st.write("#### Totales por concepto")
    st.dataframe(resumen_concepto, use_container_width=True, hide_index=True)

st.markdown("### 4) Exportar resultado de prueba")
export_df = working_df.copy()
export_df["Fecha"] = pd.to_datetime(export_df["Fecha"], errors="coerce").dt.strftime("%d/%m/%Y")
export_df["Cantidad formateada"] = export_df["Cantidad"].apply(format_cop)
st.download_button(
    "Descargar CSV normalizado",
    data=export_df.to_csv(index=False).encode("utf-8-sig"),
    file_name="flujo_recaudo_preview.csv",
    mime="text/csv",
    use_container_width=True,
)

st.success(
    "Este laboratorio te deja validar primero el flujo de captura, corrección y resumen. "
    "Cuando el comportamiento quede estable, ahí sí conviene mover la lógica necesaria a `app.py`."
)
