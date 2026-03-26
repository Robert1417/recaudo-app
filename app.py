import streamlit as st
import pandas as pd
import numpy as np
from copy import deepcopy
from datetime import date
from calendar import monthrange
from pathlib import Path
from tempfile import gettempdir
import json
import ast
import csv
from joblib import load
import re  # ✅ NUEVO
from datetime import datetime
import os
import html as html_lib
import time
import ssl
from io import BytesIO
from zipfile import ZipFile
import xml.etree.ElementTree as ET
import zlib
import subprocess
import shutil
import tempfile
from urllib.parse import parse_qs, urlparse

import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from googleapiclient.errors import HttpError
from pypdf import PdfReader
import streamlit.components.v1 as components
from docx import Document
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Pt
from docx.text.paragraph import Paragraph

# ==== Transformadores CUSTOM (deben estar antes de load) ====
from sklearn.base import BaseEstimator, TransformerMixin

class LogAndDrop(BaseEstimator, TransformerMixin):
    """
    Crea columnas log1p de ['C/A', 'AMOUNT_TOTAL'] y elimina las originales.
    Mantiene ['PRI-ULT','Ratio_PP'] tal cual.
    Salida: ['PRI-ULT','Ratio_PP','C/A_log','AMOUNT_TOTAL_log']
    """
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
    """
    Winsoriza por cuantiles (p_low, p_high) columnas numéricas.
    Aprende límites en fit y los aplica en transform.
    """
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


# ------------------ Ajustes globales ------------------
pd.set_option("mode.copy_on_write", True)
def _configure_streamlit_page():
    try:
        st.set_page_config(page_title="Calculadora de Recaudo", page_icon="💸", layout="centered")
    except Exception:
        return


_configure_streamlit_page()
st.title("💸 Calculadora de Recaudo [SANDBOX]")

import sklearn, numpy, joblib
from sklearn.impute import SimpleImputer

# 🔧 PARCHE compatibilidad modelo viejo vs sklearn nuevo
if not hasattr(SimpleImputer, "_fill_dtype"):
    SimpleImputer._fill_dtype = None
st.sidebar.caption(
    f"🧩 NumPy: {numpy.__version__}\n"
    f"🧠 scikit-learn: {sklearn.__version__}\n"
    f"💼 joblib: {joblib.__version__}"
)

st.caption(
    "1) La app carga automáticamente la base generada por el workflow (`data/cartera_asignada_filtrada`) • "
    "2) Escribe la **Referencia** y selecciona **uno o varios Id deuda** • "
    "3) Ajusta valores editables (Deuda, Apartado, Comisión, Saldo) • "
    "4) Ingresa **PAGO BANCO** y **N PaB** → se calcula **DESCUENTO** y **Comisión de éxito** • "
    "6) Revisa KPIs (PLAZO lo ingresas tú)."
)

# =================== 🔄 Reinicio manual (limpiar cache) ===================
st.sidebar.markdown("### 🔄 Control")
if st.sidebar.button("Reiniciar calculadora (limpiar cache)"):
    st.cache_data.clear()
    st.cache_resource.clear()
    st.rerun()

# ==== Rutas de artefactos generados por el notebook/Action ====
DATA_PARQUET = Path("data/cartera_asignada_filtrada.parquet")
DATA_CSV     = Path("data/cartera_asignada_filtrada.csv")
MODEL_PATH   = Path("mlp_recaudo_pipeline.joblib")
META_PATH    = Path("mlp_recaudo_meta.json")
LOG_PATH     = Path("data/logs/logs_calculadora.csv")
DOCX_TEMPLATE_PATH = Path("data/Documento Estructurados en Blanco.docx")
CLIENTES_LOOKUP_PATH = Path("data/Consulta_F_Clientes_Parte_1.csv")
GOOGLE_SHEET_ID = "1Aahltn7TSRf6ZpTpS-vPgpB89hO-r5KxpAhqKAPXziE"
GOOGLE_SHEET_TAB = "Historico Calculadora"
GOOGLE_SHEET_TAB_RESPUESTAS = "Respuestas Estr"
GOOGLE_SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
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

GOOGLE_RESPUESTAS_COLS = [chr(i) for i in range(ord("A"), ord("V") + 1)]
DRIVE_FOLDER_CARTA_FIRMADA_PATH = ["Predicción fecha de liquidación", "Aprobaciones", "Carta y pagaré firmado"]
DRIVE_FOLDER_PANTALLAZO_PATH = ["Predicción fecha de liquidación", "Aprobaciones", "Pantallazos Confirmación"]
# ========= Helpers de "versión de archivo" para invalidar cache =========

def _file_version(path: Path) -> str:
    """
    Devuelve una 'firma' del archivo basada en mtime + tamaño.
    Si el archivo no existe, devuelve 'missing'.
    """
    try:
        stat = path.stat()
        return f"{stat.st_mtime_ns}-{stat.st_size}"
    except FileNotFoundError:
        return "missing"
##################################################################################################################################################################################        
def _sum_rounded_parts(values, digits=2):
    rounded = [round(float(v), digits) for v in values]
    if rounded:
        rounded[-1] = round(sum(values) - sum(rounded[:-1]), digits)
    return rounded


def _last_day_of_month(base_date: date, months_ahead: int) -> date:
    shifted = pd.Timestamp(base_date) + pd.DateOffset(months=months_ahead)
    return date(int(shifted.year), int(shifted.month), monthrange(int(shifted.year), int(shifted.month))[1])


def _day_of_month(base_date: date, months_ahead: int, day: int | None) -> date:
    if day is None:
        return _last_day_of_month(base_date, months_ahead)
    shifted = pd.Timestamp(base_date) + pd.DateOffset(months=months_ahead)
    year = int(shifted.year)
    month = int(shifted.month)
    safe_day = min(max(int(day), 1), monthrange(year, month)[1])
    return date(year, month, safe_day)


def _month_offset(base_date: date, target_date: date) -> int:
    return ((target_date.year - base_date.year) * 12) + (target_date.month - base_date.month)


def _rebalance_group_amounts(df_group: pd.DataFrame, total_objetivo: float) -> pd.DataFrame:
    df_group = df_group.sort_values("orden").copy()
    total_objetivo = max(float(total_objetivo or 0.0), 0.0)
    override_mask = df_group["cantidad_editada"].fillna(False).astype(bool)
    total_editado = min(max(float(df_group.loc[override_mask, "Cantidad"].sum()), 0.0), total_objetivo)
    restantes = df_group.index[~override_mask].tolist()
    restante_disponible = max(0.0, total_objetivo - total_editado)

    if restantes:
        partes = _sum_rounded_parts([restante_disponible / len(restantes)] * len(restantes))
        for idx, valor in zip(restantes, partes):
            df_group.at[idx, "Cantidad"] = valor
    elif override_mask.any():
        ultimo_idx = df_group.index[-1]
        otros = float(df_group.iloc[:-1]["Cantidad"].sum()) if len(df_group) > 1 else 0.0
        df_group.at[ultimo_idx, "Cantidad"] = round(max(total_objetivo - otros, 0.0), 2)

    return df_group


def _parse_amount_input(value) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float, np.integer, np.floating)):
        return float(value)
    text = str(value).strip()
    if not text:
        return 0.0
    cleaned = re.sub(r"[^\d-]", "", text)
    if cleaned in {"", "-"}:
        return 0.0
    return float(cleaned)


def _format_currency0(value) -> str:
    return f"$ {int(round(float(value or 0.0))):,}"


#############################################################################################################################################################################
#############################################################################################################################################################################
def _format_currency_cop(value) -> str:
    amount = float(value or 0.0)
    formatted = f"{amount:,.2f}".replace(",", "_").replace(".", ",").replace("_", ".")
    return f"${formatted} COP"


def _number_to_words_es(value: int) -> str:
    units = ["cero", "uno", "dos", "tres", "cuatro", "cinco", "seis", "siete", "ocho", "nueve"]
    teens = {
        10: "diez", 11: "once", 12: "doce", 13: "trece", 14: "catorce", 15: "quince",
        16: "dieciseis", 17: "diecisiete", 18: "dieciocho", 19: "diecinueve",
    }
    tens = {20: "veinte", 30: "treinta", 40: "cuarenta", 50: "cincuenta", 60: "sesenta", 70: "setenta", 80: "ochenta", 90: "noventa"}
    hundreds = {100: "cien", 200: "doscientos", 300: "trescientos", 400: "cuatrocientos", 500: "quinientos", 600: "seiscientos", 700: "setecientos", 800: "ochocientos", 900: "novecientos"}

    def convert(n: int) -> str:
        if n < 10:
            return units[n]
        if n < 20:
            return teens[n]
        if n < 30:
            return "veinte" if n == 20 else f"veinti{convert(n - 20)}"
        if n < 100:
            return tens[(n // 10) * 10] if n % 10 == 0 else f"{tens[(n // 10) * 10]} y {convert(n % 10)}"
        if n == 100:
            return "cien"
        if n < 1000:
            base = hundreds.get((n // 100) * 100, "ciento")
            if (n // 100) * 100 == 100:
                base = "ciento"
            return base if n % 100 == 0 else f"{base} {convert(n % 100)}"
        if n < 1_000_000:
            thousands = n // 1000
            remainder = n % 1000
            prefix = "mil" if thousands == 1 else f"{convert(thousands)} mil"
            return prefix if remainder == 0 else f"{prefix} {convert(remainder)}"
        if n < 1_000_000_000:
            millions = n // 1_000_000
            remainder = n % 1_000_000
            prefix = "un millon" if millions == 1 else f"{convert(millions)} millones"
            return prefix if remainder == 0 else f"{prefix} {convert(remainder)}"
        return str(n)

    return convert(max(int(value), 0))


def _format_currency_cop_words(value) -> str:
    return _number_to_words_es(int(round(float(value or 0.0)))).upper()


def _format_date_ddmmyyyy(value) -> str:
    if pd.isna(value):
        return ""
    return pd.to_datetime(value).strftime("%d/%m/%Y")


def _table_cell_text(value) -> str:
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, (int, float, np.integer, np.floating)):
        return _format_currency0(value)
    return str(value)


def _format_month_name_es(value) -> str:
    month_names = {
        1: "enero",
        2: "febrero",
        3: "marzo",
        4: "abril",
        5: "mayo",
        6: "junio",
        7: "julio",
        8: "agosto",
        9: "septiembre",
        10: "octubre",
        11: "noviembre",
        12: "diciembre",
    }
    dt_value = pd.to_datetime(value)
    return month_names.get(int(dt_value.month), "")


def _normalize_template_value(value) -> str:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return ""
    return str(value)



def _render_template_text(text: str, context: dict[str, str]) -> str:
    rendered = str(text)
    for key, value in context.items():
        rendered = rendered.replace(f"{{{key}}}", _normalize_template_value(value))
    return rendered


def _join_unique_values(values, separator=" - ") -> str:
    unique_values = []
    for value in values:
        text_value = str(value).strip()
        if not text_value or text_value.lower() == "nan":
            continue
        if text_value not in unique_values:
            unique_values.append(text_value)
    return separator.join(unique_values)


def _smart_title_case(value: str) -> str:
    words = []
    for word in str(value or "").split():
        parts = [part.capitalize() for part in word.split("-")]
        words.append("-".join(parts))
    return " ".join(words)


def _paragraph_has_column_break(paragraph) -> bool:
    return 'w:br w:type="column"' in paragraph._p.xml


def _build_document_context(
    referencia,
    bancos,
    pago_banco,
    comision_total,
    nombre_cliente="",
    numero_producto="",
    vehiculo="",
    cedula_cliente="",
    correo_cliente="",
    telefono_cliente="",
    ciudad_cliente="",
    direccion_cliente="",
    suma_comisiones_total=None,
) -> dict[str, str]:
    today = date.today()
    bancos_unicos = _join_unique_values(bancos)
    comision_total_text = _format_currency_cop(comision_total)
    suma_comisiones_value = float(suma_comisiones_total if suma_comisiones_total is not None else comision_total)
    suma_comisiones_text = _format_currency_cop(suma_comisiones_value)

    return {
        "referencia": str(referencia or ""),
        "dia_firma": str(today.day),
        "mes_firma": _format_month_name_es(today),
        "anio_firma": str(today.year),
        "entidad_financiera": bancos_unicos,
        "pago_banco": _format_currency_cop(pago_banco),
        "comision_total": comision_total_text,
        "nombre_cliente": _smart_title_case(nombre_cliente),
        "numero_producto": str(numero_producto or ""),
        "vehiculo": _smart_title_case(vehiculo),
        "cedula_cliente": str(cedula_cliente or ""),
        "correo_cliente": str(correo_cliente or ""),
        "telefono_cliente": str(telefono_cliente or ""),
        "ciudad_cliente": str(ciudad_cliente or ""),
        "direccion_cliente": str(direccion_cliente or ""),
        "suma_comisiones": suma_comisiones_text,
        "Suma_comisiones": suma_comisiones_text,
        "suma comisiones": suma_comisiones_text,
        "suma_comisiones_letras": _format_currency_cop_words(suma_comisiones_value),
    }


def _replace_paragraph_text_preserving_style(paragraph, new_text: str):
    if not paragraph.runs:
        paragraph.add_run(new_text)
        return

    template_run = next((run for run in paragraph.runs if run.text), paragraph.runs[0])

    if _paragraph_has_column_break(paragraph):
        for run in paragraph.runs:
            if run.text:
                run.text = ""
        new_run = paragraph.add_run(new_text)
        new_run.bold = template_run.bold
        new_run.italic = template_run.italic
        new_run.underline = template_run.underline
        if template_run.font is not None:
            new_run.font.name = template_run.font.name
            new_run.font.size = template_run.font.size
        return

    first_run = paragraph.runs[0]
    first_run.text = new_text
    for run in paragraph.runs[1:]:
        run.text = ""


def _apply_context_to_paragraph(paragraph, context: dict[str, str]):
    full_text = "".join(run.text for run in paragraph.runs) if paragraph.runs else paragraph.text
    rendered_text = _render_template_text(full_text, context)
    if rendered_text != full_text:
        _replace_paragraph_text_preserving_style(paragraph, rendered_text)


def _apply_context_to_table(table, context: dict[str, str]):
    for row in table.rows:
        for cell in row.cells:
            for paragraph in cell.paragraphs:
                _apply_context_to_paragraph(paragraph, context)
            for nested_table in cell.tables:
                _apply_context_to_table(nested_table, context)


def _apply_context_to_document(document, context: dict[str, str]):
    for paragraph in document.paragraphs:
        _apply_context_to_paragraph(paragraph, context)
    for table in document.tables:
        _apply_context_to_table(table, context)
    for section in document.sections:
        for paragraph in section.header.paragraphs:
            _apply_context_to_paragraph(paragraph, context)
        for table in section.header.tables:
            _apply_context_to_table(table, context)
        for paragraph in section.footer.paragraphs:
            _apply_context_to_paragraph(paragraph, context)
        for table in section.footer.tables:
            _apply_context_to_table(table, context)


def _set_table_cell_no_wrap(cell):
    tc_pr = cell._tc.get_or_add_tcPr()
    no_wrap = tc_pr.find(qn("w:noWrap"))
    if no_wrap is None:
        no_wrap = OxmlElement("w:noWrap")
        tc_pr.append(no_wrap)


def _set_cell_width(cell, width_inches: float):
    width_twips = int(width_inches * 1440)
    cell.width = width_twips
    tc_pr = cell._tc.get_or_add_tcPr()
    tc_w = tc_pr.find(qn("w:tcW"))
    if tc_w is None:
        tc_w = OxmlElement("w:tcW")
        tc_pr.append(tc_w)
    tc_w.set(qn("w:type"), "dxa")
    tc_w.set(qn("w:w"), str(width_twips))

def _set_table_grid_widths(table, column_widths: list[float]):
    """
    Ajusta el ancho de columnas a nivel de grid de tabla (w:tblGrid),
    que es lo que LibreOffice suele respetar al exportar a PDF.
    """
    tbl = table._tbl
    tbl_grid = tbl.tblGrid
    if tbl_grid is None:
        tbl_grid = OxmlElement("w:tblGrid")
        tbl.insert(0, tbl_grid)

    # Reemplazar definición previa del grid para evitar conflictos de ancho.
    for grid_col in list(tbl_grid):
        tbl_grid.remove(grid_col)

    for width in column_widths:
        width_twips = int(width * 1440)
        grid_col = OxmlElement("w:gridCol")
        grid_col.set(qn("w:w"), str(width_twips))
        tbl_grid.append(grid_col)

    # También aplicar a la API de python-docx por compatibilidad adicional.
    for idx, width in enumerate(column_widths):
        if idx < len(table.columns):
            table.columns[idx].width = int(width * 1440)


def _set_table_fixed_layout(table, column_widths: list[float]):
    """
    Fuerza layout fijo de tabla y ancho total explícito.
    Esto evita que LibreOffice "recalcule" columnas al exportar a PDF.
    """
    tbl_pr = table._tbl.tblPr
    if tbl_pr is None:
        tbl_pr = OxmlElement("w:tblPr")
        table._tbl.insert(0, tbl_pr)

    tbl_layout = tbl_pr.find(qn("w:tblLayout"))
    if tbl_layout is None:
        tbl_layout = OxmlElement("w:tblLayout")
        tbl_pr.append(tbl_layout)
    tbl_layout.set(qn("w:type"), "fixed")

    total_twips = str(int(sum(column_widths) * 1440))
    tbl_w = tbl_pr.find(qn("w:tblW"))
    if tbl_w is None:
        tbl_w = OxmlElement("w:tblW")
        tbl_pr.append(tbl_w)
    tbl_w.set(qn("w:type"), "dxa")
    tbl_w.set(qn("w:w"), total_twips)


def _apply_cronograma_table_layout(table):
    table.autofit = False
    column_widths = [0.23, 0.9, 1.2, 1.7]
    _set_table_fixed_layout(table, column_widths)
    _set_table_grid_widths(table, column_widths)
    for row in table.rows:
        for idx, width in enumerate(column_widths):
            if idx < len(row.cells):
                _set_cell_width(row.cells[idx], width)


def _apply_table_text_style(paragraph, run, *, bold=None):
    paragraph.paragraph_format.space_after = Pt(0)
    paragraph.paragraph_format.space_before = Pt(0)
    paragraph.paragraph_format.line_spacing = 1
    paragraph.alignment = paragraph.alignment

    run.font.name = "Times New Roman"
    r_pr = run._element.get_or_add_rPr()
    r_fonts = r_pr.rFonts
    if r_fonts is None:
        r_fonts = OxmlElement("w:rFonts")
        r_pr.append(r_fonts)
    r_fonts.set(qn("w:ascii"), "Times New Roman")
    r_fonts.set(qn("w:hAnsi"), "Times New Roman")
    r_fonts.set(qn("w:cs"), "Times New Roman")
    run.font.size = Pt(9.5)
    if bold is not None:
        run.bold = bold


def _replace_cell_text_preserving_style(cell, text: str):
    paragraph = cell.paragraphs[0] if cell.paragraphs else cell.add_paragraph()
    template_run = paragraph.runs[0] if paragraph.runs else None

    for extra_paragraph in cell.paragraphs[1:]:
        extra_paragraph._element.getparent().remove(extra_paragraph._element)

    for run in list(paragraph.runs):
        paragraph._element.remove(run._element)

    normalized_text = str(text).replace("\n", " ").strip()
    new_run = paragraph.add_run(normalized_text)
    _set_table_cell_no_wrap(cell)

    inherited_bold = template_run.bold if template_run is not None else None
    if template_run is not None:
        new_run.italic = template_run.italic
        new_run.underline = template_run.underline
    _apply_table_text_style(paragraph, new_run, bold=inherited_bold)


def _populate_docx_table(table, rows: list[list[str]]):
    if len(table.rows) < 2:
        raise ValueError("La plantilla Word debe tener encabezado + una fila de muestra por tabla.")

    template_row = table.rows[1]
    while len(table.rows) > 2:
        table._tbl.remove(table.rows[-1]._tr)

    first_data_row = table.rows[1]
    if not rows:
        rows = [[""] * len(first_data_row.cells)]

    for row_idx, values in enumerate(rows):
        target_row = first_data_row if row_idx == 0 else None
        if target_row is None:
            new_tr = deepcopy(template_row._tr)
            table._tbl.append(new_tr)
            target_row = table.rows[-1]

        for col_idx, value in enumerate(values):
            if col_idx < len(target_row.cells):
                _replace_cell_text_preserving_style(target_row.cells[col_idx], value)


def _remove_paragraph(paragraph: Paragraph):
    element = paragraph._element
    parent = element.getparent()
    if parent is not None:
        parent.remove(element)


def _remove_graduation_section(document):
    start_markers = [
        "mi estatus de Cliente Activo pasa a ser como Cliente Graduado",
        "Cliente Graduado",
    ]
    end_markers = [
        "Ref No",
        "C.C.",
        "Atentamente",
    ]

    paragraphs = list(document.paragraphs)
    start_idx = None
    end_idx = None

    for idx, paragraph in enumerate(paragraphs):
        text = str(paragraph.text or "").strip()
        if not text:
            continue
        if start_idx is None and any(marker in text for marker in start_markers):
            start_idx = idx
        if start_idx is not None and any(marker in text for marker in end_markers):
            end_idx = idx

    if start_idx is None:
        return

    if end_idx is None:
        end_idx = start_idx
        while end_idx + 1 < len(paragraphs) and not str(paragraphs[end_idx + 1].text or "").strip():
            end_idx += 1

    for paragraph in paragraphs[start_idx : end_idx + 1]:
        _remove_paragraph(paragraph)              


def build_recaudo_docx(
    template_path: Path,
    cronograma_df: pd.DataFrame,
    plan_df: pd.DataFrame,
    template_context: dict[str, str],
    include_graduation_section: bool = False,
) -> bytes:
    if not template_path.exists():
        raise FileNotFoundError(f"No encontré la plantilla Word: {template_path}")

    document = Document(str(template_path))
    _apply_context_to_document(document, template_context)
    if not include_graduation_section:
        _remove_graduation_section(document)
    if len(document.tables) < 2:
        raise ValueError("La plantilla Word debe tener al menos dos tablas para reemplazar.")

    cronograma_export = cronograma_df[cronograma_df["Cantidad"] > 0.005][["Fecha", "Cantidad", "Concepto"]].copy()
    cronograma_rows = []
    for idx, row in cronograma_export.reset_index(drop=True).iterrows():
        cronograma_rows.append([
            str(idx + 1),
            _format_date_ddmmyyyy(row["Fecha"]),
            _format_currency_cop(row["Cantidad"]),
            str(row["Concepto"]),
        ])

    plan_export = plan_df.copy()
    plan_rows = []
    for _, row in plan_export.iterrows():
        plan_rows.append([
            "",
            _format_date_ddmmyyyy(row["Fecha Límite de Pago"]),
            _table_cell_text(row["Pago a Banco"]),
            _table_cell_text(row["Comisión de Éxito"]),
            _table_cell_text(row["Comisión Mensual"]),
            _table_cell_text(row["Apartado Requerido"]),
        ])

    _populate_docx_table(document.tables[0], cronograma_rows)
    _apply_cronograma_table_layout(document.tables[0])
    _populate_docx_table(document.tables[1], plan_rows)

    output = BytesIO()
    document.save(output)
    output.seek(0)
    return output.getvalue()

def convert_docx_bytes_to_pdf_bytes(docx_bytes: bytes) -> bytes:
    """
    Convierte un DOCX a PDF usando LibreOffice en modo headless.
    Preserva al máximo el layout porque renderiza el mismo archivo Word,
    sin transcribir ni reconstruir contenido.
    """
    soffice_bin = shutil.which("soffice") or shutil.which("libreoffice")
    if not soffice_bin:
        raise RuntimeError(
            "No encontré LibreOffice (soffice) en el servidor. "
            "Instálalo para habilitar la exportación exacta a PDF."
        )

    with tempfile.TemporaryDirectory() as tmp_dir:
        input_path = Path(tmp_dir) / "documento.docx"
        output_dir = Path(tmp_dir) / "out"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "documento.pdf"
        input_path.write_bytes(docx_bytes)

        cmd = [
            soffice_bin,
            "--headless",
            "--convert-to",
            "pdf:writer_pdf_Export",
            str(input_path),
            "--outdir",
            str(output_dir),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(
                "La conversión a PDF falló en LibreOffice. "
                f"Detalle: {(result.stderr or result.stdout).strip()}"
            )
        if not output_path.exists():
            raise RuntimeError("LibreOffice no generó el PDF esperado.")
        return output_path.read_bytes()
#############################################################################################################################################################################
#############################################################################################################################################################################

def construir_plan_liquidacion(cronograma_df: pd.DataFrame, comision_mensual: float) -> pd.DataFrame:
    if cronograma_df.empty:
        return pd.DataFrame(columns=[
            "plan_key",
            "Fecha Límite de Pago",
            "Pago a Banco",
            "Comisión de Éxito",
            "Comisión Mensual",
            "Apartado Requerido",
        ])

    df = cronograma_df[cronograma_df["Cantidad"] > 0.005].copy()
    df["Fecha"] = pd.to_datetime(df["Fecha"])
    df["periodo"] = df["Fecha"].dt.to_period("M").astype(str)

    filas = []
    for periodo, group in df.groupby("periodo", sort=True):
        fecha_limite = group["Fecha"].min().date()
        pago_banco_mes = float(group.loc[group["Concepto"].str.contains("Entidad Financiera", na=False), "Cantidad"].sum())
        comision_exito_mes = float(group.loc[group["Concepto"].str.contains("Comisión Resuelve", na=False), "Cantidad"].sum())
        comision_mensual_mes = float(comision_mensual or 0.0)
        filas.append({
            "plan_key": periodo,
            "Fecha Límite de Pago": fecha_limite,
            "Pago a Banco": pago_banco_mes,
            "Comisión de Éxito": comision_exito_mes,
            "Comisión Mensual": comision_mensual_mes,
            "Apartado Requerido": pago_banco_mes + comision_exito_mes + comision_mensual_mes,
        })

    return pd.DataFrame(filas)


def construir_cronograma_pagos(
    fecha_inicial: date,
    plazo: int,
    n_pab: int,
    pago_banco_total: float,
    primer_pago_banco: float,
    comision_total: float,
    comision_inicial: float,
    dia_pago_banco: int | None = None,
    dia_pago_comision: int | None = None,
):
    plazo = max(int(plazo), 0)
    n_pab = max(int(n_pab), 1)
    primer_pago_banco = min(max(float(primer_pago_banco or 0.0), 0.0), max(float(pago_banco_total or 0.0), 0.0))
    pago_banco_total = max(float(pago_banco_total or 0.0), 0.0)
    comision_total = max(float(comision_total or 0.0), 0.0)
    comision_inicial = min(max(float(comision_inicial or 0.0), 0.0), comision_total)

    banco_restante = max(0.0, pago_banco_total - primer_pago_banco)
    meses_banco_restantes = max(0, n_pab - 1)
    meses_comision_restantes = max(0, plazo - meses_banco_restantes)
    comision_restante = max(0.0, comision_total - comision_inicial)

    pagos_banco = [primer_pago_banco]
    if meses_banco_restantes > 0:
        pagos_banco += _sum_rounded_parts([banco_restante / meses_banco_restantes] * meses_banco_restantes)

    pagos_comision = [comision_inicial]
    if meses_comision_restantes > 0:
        pagos_comision += _sum_rounded_parts([comision_restante / meses_comision_restantes] * meses_comision_restantes)

    filas = []
    if pagos_banco[0] > 0:
        filas.append({"Fecha": fecha_inicial, "Cantidad": pagos_banco[0], "Concepto": "Pago 1 a Entidad Financiera", "tipo": "banco", "orden": 0, "months_ahead": 0, "row_key": "banco_0"})
    if pagos_comision[0] > 0:
        filas.append({"Fecha": fecha_inicial, "Cantidad": pagos_comision[0], "Concepto": "Comisión Resuelve", "tipo": "comision", "orden": 1, "months_ahead": 0, "row_key": "comision_0"})

    for idx, valor in enumerate(pagos_banco[1:], start=1):
        if valor <= 0:
            continue
        filas.append({"Fecha": _day_of_month(fecha_inicial, idx, dia_pago_banco), "Cantidad": valor, "Concepto": f"Pago {idx + 1} a Entidad Financiera", "tipo": "banco", "orden": len(filas), "months_ahead": idx, "row_key": f"banco_{idx}"})

    for idx, valor in enumerate(pagos_comision[1:], start=1):
        if valor <= 0:
            continue
        offset = meses_banco_restantes + idx
        filas.append({"Fecha": _day_of_month(fecha_inicial, offset, dia_pago_comision), "Cantidad": valor, "Concepto": "Comisión Resuelve", "tipo": "comision", "orden": len(filas), "months_ahead": offset, "row_key": f"comision_{idx}"})

    cronograma = pd.DataFrame(filas)
    if cronograma.empty:
        cronograma = pd.DataFrame(columns=["Fecha", "Cantidad", "Concepto", "tipo", "orden", "months_ahead", "row_key"])
    return cronograma, {"meses_banco_restantes": meses_banco_restantes, "meses_comision_restantes": meses_comision_restantes}


def aplicar_overrides_cronograma(
    cronograma_df: pd.DataFrame,
    overrides_map: dict,
    totales_por_tipo: dict,
    fecha_inicial: date,
    dia_pago_banco: int | None,
    dia_pago_comision: int | None,
    primer_pago_banco_input: float,
    comision_inicial_input: float,
):
    if cronograma_df.empty:
        return cronograma_df.copy(), []

    df = cronograma_df.copy()
    df["cantidad_editada"] = False
    df["fecha_editada"] = False
    advertencias = []

    for row_key, cambios in (overrides_map or {}).items():
        matches = df.index[df["row_key"] == row_key].tolist()
        if not matches:
            continue
        idx = matches[0]
        if int(df.at[idx, "months_ahead"]) == 0:
            continue
        if "Fecha" in cambios and cambios["Fecha"]:
            try:
                df.at[idx, "Fecha"] = pd.to_datetime(cambios["Fecha"]).date()
                df.at[idx, "fecha_editada"] = True
            except Exception:
                advertencias.append(f"No pude interpretar la fecha editada de {row_key}.")
        if "Cantidad" in cambios:
            try:
                df.at[idx, "Cantidad"] = max(_parse_amount_input(cambios["Cantidad"]), 0.0)
                df.at[idx, "cantidad_editada"] = True
            except Exception:
                advertencias.append(f"No pude interpretar el monto editado de {row_key}.")

    for tipo, input_value in [("banco", primer_pago_banco_input), ("comision", comision_inicial_input)]:
        mask = (df["tipo"] == tipo) & (df["months_ahead"] == 0)
        if mask.any():
            idx = df.index[mask][0]
            df.at[idx, "Cantidad"] = max(float(input_value or 0.0), 0.0)
            df.at[idx, "cantidad_editada"] = True

    partes = []
    for tipo, group in df.groupby("tipo", sort=False):
        total_objetivo = totales_por_tipo.get(tipo, float(group["Cantidad"].sum()))
        partes.append(_rebalance_group_amounts(group, total_objetivo))
    df = pd.concat(partes).sort_values("orden").reset_index(drop=True)

    banco_mask = (df["tipo"] == "banco") & (df["months_ahead"] > 0)
    banco_ocupados = set()
    for idx in df.index[banco_mask]:
        if bool(df.at[idx, "fecha_editada"]):
            banco_ocupados.add(max(1, _month_offset(fecha_inicial, pd.to_datetime(df.at[idx, "Fecha"]).date())))
        else:
            offset = int(df.at[idx, "months_ahead"])
            banco_ocupados.add(offset)
            df.at[idx, "Fecha"] = _day_of_month(fecha_inicial, offset, dia_pago_banco)

    comision_mask = (df["tipo"] == "comision") & (df["months_ahead"] > 0)
    offsets_fijos = set()
    for idx in df.index[comision_mask]:
        if bool(df.at[idx, "fecha_editada"]):
            offsets_fijos.add(max(1, _month_offset(fecha_inicial, pd.to_datetime(df.at[idx, "Fecha"]).date())))

    siguiente_offset = 1
    for idx in df.index[comision_mask]:
        if bool(df.at[idx, "fecha_editada"]):
            continue
        while siguiente_offset in offsets_fijos:
            siguiente_offset += 1
        while siguiente_offset in banco_ocupados:
            siguiente_offset += 1
        df.at[idx, "months_ahead"] = siguiente_offset
        df.at[idx, "Fecha"] = _day_of_month(fecha_inicial, siguiente_offset, dia_pago_comision)
        offsets_fijos.add(siguiente_offset)
        siguiente_offset += 1

    df = df.sort_values(by=["Fecha", "orden"]).reset_index(drop=True)
    return df, advertencias
#####################################################################################################################################################################################    

def _model_version() -> str:
    return _file_version(MODEL_PATH)

def _meta_version() -> str:
    return _file_version(META_PATH)

def _data_version() -> str:
    # Si existe parquet, usamos ese; si no, miramos CSV.
    if DATA_PARQUET.exists():
        return _file_version(DATA_PARQUET)
    if DATA_CSV.exists():
        return _file_version(DATA_CSV)
    return "missing"

def _parse_relaxed_service_account_string(raw_value: str) -> dict:
    """
    Intenta reconstruir un JSON de service account aunque llegue como texto
    con saltos reales dentro de `private_key`, wrappers extra o texto copiado
    desde distintos gestores de secretos.
    """
    required_fields = ["private_key", "client_email"]
    known_fields = [
        "type",
        "project_id",
        "private_key_id",
        "private_key",
        "client_email",
        "client_id",
        "auth_uri",
        "token_uri",
        "auth_provider_x509_cert_url",
        "client_x509_cert_url",
        "universe_domain",
    ]

    text = raw_value.strip()

    if text.startswith("MI_JSON="):
        text = text.split("=", 1)[1].strip()

    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        text = text[start : end + 1]

    parsed = {}
    for i, field in enumerate(known_fields):
        field_match = re.search(rf'"{re.escape(field)}"\s*:\s*"', text)
        if not field_match:
            continue

        value_start = field_match.end()
        next_positions = []
        for next_field in known_fields[i + 1 :]:
            next_match = re.search(rf'",\s*"{re.escape(next_field)}"\s*:', text[value_start:])
            if next_match:
                next_positions.append(value_start + next_match.start())

        if next_positions:
            value_end = min(next_positions)
        else:
            tail_match = re.search(r'"\s*}$', text[value_start:])
            if not tail_match:
                continue
            value_end = value_start + tail_match.start()

        parsed[field] = text[value_start:value_end]

    if not all(field in parsed for field in required_fields):
        raise RuntimeError(
            "El secreto `MI_JSON` no se pudo interpretar. Revisa que tenga el JSON completo del service account."
        )

    return parsed
    
def _looks_like_service_account_mapping(value) -> bool:
    try:
        data = dict(value)
    except Exception:
        return False
    return "private_key" in data and "client_email" in data

def _extract_service_account_from_secrets_tree(value):
    """
    Busca credenciales en estructuras anidadas de st.secrets (dict/AttrDict).
    Soporta:
    - llave MI_JSON en cualquier nivel
    - objetos con private_key + client_email en cualquier nivel
    """
    try:
        if isinstance(value, str) and value.strip():
            return value
    except Exception:
        pass

    if _looks_like_service_account_mapping(value):
        return dict(value)

    try:
        items = dict(value).items()
    except Exception:
        return None

    for key, sub_value in items:
        if str(key).strip().upper() == "MI_JSON":
            return sub_value
        nested = _extract_service_account_from_secrets_tree(sub_value)
        if nested is not None:
            return nested
    return None



def _load_google_service_account_info() -> dict:
    """
    Carga el JSON del service account desde Streamlit Secrets o variable de entorno.
    Soporta MI_JSON como string JSON, tabla TOML, dict directo o variables separadas.
    """
    creds_source = None

    try:
        creds_source = _extract_service_account_from_secrets_tree(st.secrets)    
    except Exception:
        creds_source = None

    if creds_source is None:
        env_json = (
            os.environ.get("MI_JSON")
            or os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
            or os.environ.get("MI_JSON_SANDBOX")
        )
        if env_json:
            creds_source = env_json
        else:
            env_fields = {
                key: os.environ.get(key)
                for key in [
                    "type",
                    "project_id",
                    "private_key_id",
                    "private_key",
                    "client_email",
                    "client_id",
                    "auth_uri",
                    "token_uri",
                    "auth_provider_x509_cert_url",
                    "client_x509_cert_url",
                    "universe_domain",
                ]
                if os.environ.get(key) is not None
            }
            if _looks_like_service_account_mapping(env_fields):
                creds_source = env_fields

    if creds_source is None:
        raise RuntimeError(
            "No encontré credenciales de Google Sheets. "
            "Configura `MI_JSON` en Streamlit Secrets (de ESTE despliegue) "
            "o una variable de entorno (`MI_JSON` / `GOOGLE_SERVICE_ACCOUNT_JSON`)."
        )

    if isinstance(creds_source, str):
        creds_source = creds_source.strip()
        if not creds_source:
            raise RuntimeError("El secreto `MI_JSON` está vacío.")
        try:
            creds_info = json.loads(creds_source)
        except json.JSONDecodeError:
            try:
                creds_info = ast.literal_eval(creds_source)
            except Exception:
                creds_info = _parse_relaxed_service_account_string(creds_source)
    
    else:
        try:
            creds_info = dict(creds_source)
        except Exception as exc:
            raise RuntimeError("No pude interpretar el secreto `MI_JSON` como credenciales válidas.") from exc

    private_key = creds_info.get("private_key")
    if isinstance(private_key, str):
        normalized_key = private_key.strip().replace("\r\n", "\n").replace("\\n", "\n")
        if "-----BEGIN PRIVATE KEY-----" in normalized_key and not normalized_key.endswith("\n"):
            normalized_key += "\n"
        creds_info["private_key"] = normalized_key

    return creds_info


@st.cache_resource(show_spinner=False)
def get_google_sheet_worksheet(tab_name: str = GOOGLE_SHEET_TAB):
    """
    Devuelve la hoja de cálculo destino para histórico.
    Se cachea mientras no cambie el proceso.
    """
    creds_info = _load_google_service_account_info()
    credentials = Credentials.from_service_account_info(creds_info, scopes=GOOGLE_SHEETS_SCOPES)
    client = gspread.authorize(credentials)
    spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
    return spreadsheet.worksheet(tab_name)


def _append_row_to_google_sheet(row_data: dict):
    """
    Inserta una fila en Google Sheets y retorna (ok, destination, error_msg).
    """
    try:
        worksheet = get_google_sheet_worksheet()
        expected_headers = GOOGLE_SHEET_HEADERS
        current_headers = worksheet.row_values(1)
        if current_headers[: len(expected_headers)] != expected_headers:
            worksheet.update("A1:H1", [expected_headers])

        worksheet.append_row(
            [row_data.get(header, "") for header in expected_headers],
            value_input_option="USER_ENTERED",
        )
        return True, f"Google Sheets > {GOOGLE_SHEET_TAB}", None
    except Exception as e:
        return False, f"Google Sheets > {GOOGLE_SHEET_TAB}", str(e)

def _append_row_to_respuestas_estr(row_values: list):
    """
    Inserta una fila en la hoja "Respuestas Estr" respetando el orden A:V.
    """
    try:
        worksheet = get_google_sheet_worksheet(GOOGLE_SHEET_TAB_RESPUESTAS)
        normalized = list(row_values or [])
        if len(normalized) < len(GOOGLE_RESPUESTAS_COLS):
            normalized += [""] * (len(GOOGLE_RESPUESTAS_COLS) - len(normalized))
        elif len(normalized) > len(GOOGLE_RESPUESTAS_COLS):
            normalized = normalized[: len(GOOGLE_RESPUESTAS_COLS)]

        # Evita que el registro se vaya al final de filas "ocupadas" por fórmulas
        # (por ejemplo, columnas con FALSE/checkbox). Lo insertamos justo después
        # de la última fila donde A, B y C están diligenciadas.
        col_a = worksheet.col_values(1)
        col_b = worksheet.col_values(2)
        col_c = worksheet.col_values(3)
        max_len = max(len(col_a), len(col_b), len(col_c), 1)

        last_data_row = 1  # encabezados
        for row_idx in range(2, max_len + 1):
            a_val = col_a[row_idx - 1] if row_idx <= len(col_a) else ""
            b_val = col_b[row_idx - 1] if row_idx <= len(col_b) else ""
            c_val = col_c[row_idx - 1] if row_idx <= len(col_c) else ""
            if str(a_val).strip() and str(b_val).strip() and str(c_val).strip():
                last_data_row = row_idx

        target_row = last_data_row + 1
        worksheet.update(
            f"A{target_row}:V{target_row}",
            [normalized],
            value_input_option="USER_ENTERED",
        )
        return True, f"Google Sheets > {GOOGLE_SHEET_TAB_RESPUESTAS}", None
    except Exception as e:
        return False, f"Google Sheets > {GOOGLE_SHEET_TAB_RESPUESTAS}", str(e)

def _extract_drive_folder_id(folder_url: str) -> str:
    match = re.search(r"/folders/([a-zA-Z0-9_-]+)", str(folder_url))
    if match:
        return match.group(1)
    parsed = urlparse(str(folder_url))
    query = parse_qs(parsed.query)
    if "id" in query and query["id"]:
        return query["id"][0]
    raise ValueError(f"No pude obtener el folder id de la URL: {folder_url}")


def _normalize_drive_name(value: str) -> str:
    txt = unicodedata.normalize("NFKD", str(value or ""))
    txt = "".join(ch for ch in txt if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", txt).strip().lower()


def _find_drive_folder_by_name(drive_service, folder_name: str, parent_id: str | None = None):
    if parent_id:
        query = (
            f"'{parent_id}' in parents and "
            "mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        )
    else:
        query = "mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    response = drive_service.files().list(
        q=query,
        fields="files(id,name)",
        pageSize=200,
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
    ).execute()
    items = response.get("files", [])
    expected = _normalize_drive_name(folder_name)
    for item in items:
        if _normalize_drive_name(item.get("name")) == expected:
            return item
    return None


def _resolve_drive_folder_id_from_path(drive_service, folder_path: list[str]) -> str:
    parent_id = None
    for idx, level_name in enumerate(folder_path):
        found = _find_drive_folder_by_name(drive_service, level_name, parent_id=parent_id)
        if not found and idx == 0:
            # Fallback: buscar el primer nivel globalmente por nombre (útil si no cuelga de root SA).
            found = _find_drive_folder_by_name(drive_service, level_name, parent_id=None)
        if not found:
            raise RuntimeError(
                "No encontré la carpeta en Drive con la ruta: "
                + " / ".join(folder_path)
            )
        parent_id = found["id"]
    return parent_id


@st.cache_resource(show_spinner=False)
def get_google_drive_service():
    creds_info = _load_google_service_account_info()
    credentials = Credentials.from_service_account_info(creds_info, scopes=GOOGLE_SHEETS_SCOPES)
    return build("drive", "v3", credentials=credentials, cache_discovery=False)


def _sanitize_filename(name: str, fallback: str = "archivo") -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9._-]+", "_", str(name or "").strip())
    return cleaned[:150] if cleaned else fallback


def _find_drive_file_in_folder(drive_service, folder_id: str, filename: str):
    safe_name = str(filename).replace("'", "\\'")
    query = f"'{folder_id}' in parents and name = '{safe_name}' and trashed = false"
    response = drive_service.files().list(
        q=query,
        fields="files(id,name,webViewLink)",
        pageSize=5,
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
    ).execute()
    files = response.get("files", [])
    return files[0] if files else None


def _execute_resumable_request(request, max_attempts: int = 6):
    response = None
    attempts = 0
    while response is None:
        try:
            _, response = request.next_chunk(num_retries=5)
        except (HttpError, ssl.SSLEOFError, ConnectionError, OSError) as exc:
            attempts += 1
            if attempts >= max_attempts:
                raise RuntimeError(f"Falló la subida tras {attempts} intentos. Último error: {exc}") from exc
            time.sleep(2 ** attempts)
    return response


def upload_streamlit_file_to_drive(uploaded_file, folder_target, reference: str, prefix: str) -> str:
    drive_service = get_google_drive_service()
    if isinstance(folder_target, list):
        folder_id = _resolve_drive_folder_id_from_path(drive_service, folder_target)
    else:
        folder_id = _extract_drive_folder_id(str(folder_target))
    source_name = _sanitize_filename(getattr(uploaded_file, "name", "archivo"))
    reference_name = _sanitize_filename(str(reference), "sin_referencia")
    filename = f"{prefix}_{reference_name}_{source_name}"

    uploaded_file.seek(0)
    file_bytes = uploaded_file.read()
    media = MediaIoBaseUpload(
        BytesIO(file_bytes),
        mimetype=getattr(uploaded_file, "type", None) or "application/octet-stream",
        chunksize=2 * 1024 * 1024,
        resumable=True,
    )

    existing = _find_drive_file_in_folder(drive_service, folder_id, filename)
    if existing:
        request = drive_service.files().update(
            fileId=existing["id"],
            media_body=media,
            fields="id,webViewLink",
            supportsAllDrives=True,
        )
        updated = _execute_resumable_request(request)
        return updated.get("webViewLink") or f"https://drive.google.com/file/d/{updated['id']}/view"

    request = drive_service.files().create(
        body={"name": filename, "parents": [folder_id]},
        media_body=media,
        fields="id,webViewLink",
        supportsAllDrives=True,
    )
    created = _execute_resumable_request(request)
    return created.get("webViewLink") or f"https://drive.google.com/file/d/{created['id']}/view"


def _extract_pdf_pages_text(uploaded_pdf_file) -> list[str]:
    """
    Extrae texto por página con pypdf.
    """
    uploaded_pdf_file.seek(0)
    pdf_bytes = uploaded_pdf_file.read()
    uploaded_pdf_file.seek(0)
    reader = PdfReader(BytesIO(pdf_bytes))
    return [(page.extract_text() or "") for page in reader.pages]


def _validate_signed_pdf(uploaded_pdf_file, expected_reference: str) -> tuple[bool, str | None]:
    """
    Reglas:
    1) La referencia del caso debe aparecer en la primera hoja.
    2) La última hoja debe contener 'QR' (indicio de firma).
    """
    expected_ref = re.sub(r"[^0-9A-Za-z]", "", str(expected_reference or "")).lower()
    if not expected_ref:
        return False, "No hay referencia del caso para validar la carta firmada."

    uploaded_pdf_file.seek(0)
    raw = uploaded_pdf_file.read()
    uploaded_pdf_file.seek(0)
    if not raw:
        return False, "El PDF firmado está vacío."

    page_texts: list[str] = []
    try:
        page_texts = _extract_pdf_pages_text(uploaded_pdf_file)
    except Exception:
        page_texts = []

    if page_texts:
        first_page = re.sub(r"[^0-9A-Za-z]", "", page_texts[0]).lower()
        last_page_text = page_texts[-1].lower()
        if expected_ref in first_page and "qr" in last_page_text:
            return True, None

    # Fallback robusto: algunos PDFs no exponen bien streams por página.
    raw_text = raw.decode("latin-1", errors="ignore").lower()
    raw_compact = re.sub(r"[^0-9a-z]", "", raw_text)
    has_ref_anywhere = expected_ref in raw_compact
    has_qr_anywhere = "qr" in raw_text
    if has_ref_anywhere and has_qr_anywhere:
        return True, None

    if page_texts:
        return False, "No validó referencia en primera hoja y/o QR en última hoja del PDF firmado."
    return False, "No pude leer el PDF por páginas; tampoco encontré referencia+QR en el contenido del archivo."
    
def diagnosticar_google_sheets():
    """
    Valida que el secreto, el spreadsheet y la pestaña destino estén accesibles.
    No escribe datos; solo devuelve el estado para mostrarlo en la UI.
    """
    try:
        creds_info = _load_google_service_account_info()
        client_email = creds_info.get("client_email", "desconocido")

        credentials = Credentials.from_service_account_info(creds_info, scopes=GOOGLE_SHEETS_SCOPES)
        client = gspread.authorize(credentials)
        spreadsheet = client.open_by_key(GOOGLE_SHEET_ID)
        worksheet = spreadsheet.worksheet(GOOGLE_SHEET_TAB)

        return {
            "ok": True,
            "client_email": client_email,
            "spreadsheet_title": spreadsheet.title,
            "worksheet_title": worksheet.title,
            "error": None,
        }
    except Exception as e:
        client_email = "desconocido"
        try:
            creds_info = _load_google_service_account_info()
            client_email = creds_info.get("client_email", "desconocido")
        except Exception:
            pass

        return {
            "ok": False,
            "client_email": client_email,
            "spreadsheet_title": None,
            "worksheet_title": None,
            "error": str(e),
        }


def google_sheets_status():
    """
    Alias simple para evitar errores por diferencias de nombre al invocar el diagnóstico.
    """
    return diagnosticar_google_sheets()
        
# =================== Loaders cacheados ===================
@st.cache_resource(show_spinner=False)
def load_model(_version: str):
    """
    Carga el modelo MLP. _version es la firma del archivo y se usa
    solo para invalidar el cache cuando cambie el .joblib.
    """
    if not MODEL_PATH.exists():
        st.error("No encontré el archivo del modelo `mlp_recaudo_pipeline.joblib` en la raíz del repo.")
        st.stop()
    return load(MODEL_PATH)

@st.cache_data(show_spinner=False)
def load_meta(_version: str):
    """
    Carga el JSON de metadata. Se refresca cuando cambie el archivo.
    """
    meta = {}
    if META_PATH.exists():
        try:
            meta = json.loads(META_PATH.read_text(encoding="utf-8"))
        except Exception:
            meta = {}
    return meta

@st.cache_data(show_spinner=False)
def load_repo_base(_version: str) -> pd.DataFrame | None:
    """
    Carga la base que deja el workflow.
    Si existen Parquet y CSV, usa el CSV cuando trae más columnas que el Parquet
    (por ejemplo, campos enriquecidos para poblar el documento Word).
    _version se usa solo para invalidar cache cuando cambian los archivos.
    Devuelve None si no existe.
    """
    try:
        df_parquet = pd.read_parquet(DATA_PARQUET) if DATA_PARQUET.exists() else None
        df_csv = pd.read_csv(DATA_CSV) if DATA_CSV.exists() else None

        if df_parquet is not None and df_csv is not None:
            return df_csv if len(df_csv.columns) > len(df_parquet.columns) else df_parquet
        if df_parquet is not None:
            return df_parquet
        if df_csv is not None:
            return df_csv
        return None
    except Exception:
        # Si algo falla leyendo, permitimos fallback a subida manual
        return None

@st.cache_data(show_spinner=False)
def load_clientes_lookup() -> pd.DataFrame | None:
    if not CLIENTES_LOOKUP_PATH.exists():
        return None

    def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df.columns = [str(col).replace("﻿", "").strip() for col in df.columns]
        for col in df.columns:
            df[col] = df[col].map(lambda value: str(value).replace("﻿", "").strip() if value is not None else "")
        return df

    def _read_with_sep(sep, engine=None):
        kwargs = {"encoding": "latin-1", "dtype": str, "keep_default_na": False}
        if sep is not None:
            kwargs["sep"] = sep
        if engine is not None:
            kwargs["engine"] = engine
        return pd.read_csv(CLIENTES_LOOKUP_PATH, **kwargs)

    try:
        sample = CLIENTES_LOOKUP_PATH.read_text(encoding="latin-1", errors="ignore")[:4096]
        dialect = csv.Sniffer().sniff(sample, delimiters=",;")
        df = _read_with_sep(dialect.delimiter)
    except Exception:
        try:
            df = _read_with_sep(None, engine="python")
        except Exception:
            return None

    if len(df.columns) == 1:
        only_col = str(df.columns[0])
        fallback_sep = ";" if ";" in only_col else ","
        try:
            df = _read_with_sep(fallback_sep)
        except Exception:
            return _clean_df(df)

    return _clean_df(df)


def _normalize_lookup_key(value) -> str:
    text_value = str(value or "").strip()
    if text_value.endswith(".0"):
        text_value = text_value[:-2]
    digits_only = re.sub(r"\D", "", text_value)
    if digits_only and len(digits_only) >= max(6, len(text_value.replace(" ", "")) - 2):
        return digits_only
    return text_value


def _format_city_department(ciudad, departamento) -> str:
    ciudad_text = _smart_title_case(ciudad)
    departamento_text = _smart_title_case(departamento)
    normalized_city = _norm(ciudad_text)
    if normalized_city in {"bogota d c", "bogota dc", "bogota"}:
        return "Bogotá D.C."
    if ciudad_text and departamento_text:
        return f"{ciudad_text}, {departamento_text}"
    return ciudad_text or departamento_text


def _lookup_cliente_info(referencia, cedula_cliente) -> dict[str, str]:
    clientes_df = load_clientes_lookup()
    if clientes_df is None or clientes_df.empty:
        return {}

    col_ref = _find_col(clientes_df, ["Referencia"])
    col_doc = _find_col(clientes_df, ["Documento"])
    col_cel = _find_col(clientes_df, ["Celular"])
    col_ciu = _find_col(clientes_df, ["Ciudad"])
    col_dep = _find_col(clientes_df, ["Departamento"])
    col_dir = _find_col(clientes_df, ["Direccion", "Dirección"])

    match = pd.DataFrame()
    ref_text = _normalize_lookup_key(referencia)
    cedula_text = _normalize_lookup_key(cedula_cliente)

    if col_ref and ref_text:
        match = clientes_df[clientes_df[col_ref].map(_normalize_lookup_key) == ref_text]
    if match.empty and col_doc and cedula_text:
        match = clientes_df[clientes_df[col_doc].map(_normalize_lookup_key) == cedula_text]
    if match.empty:
        return {}

    row = match.iloc[0]
    return {
        "telefono_cliente": str(row[col_cel]).strip() if col_cel and pd.notna(row[col_cel]) else "",
        "ciudad_cliente": _format_city_department(row[col_ciu] if col_ciu else "", row[col_dep] if col_dep else ""),
        "direccion_cliente": str(row[col_dir]).strip() if col_dir and pd.notna(row[col_dir]) else "",
    }
# =================== LOG LOCAL ===================
def _get_writable_log_path() -> Path:
    """
    Devuelve la ruta donde se guardará el histórico.
    Si `data/` no es escribible (por despliegue), usa un fallback temporal.
    """
    candidates = [
        LOG_PATH,
        Path(gettempdir()) / "recaudo-app" / "logs_calculadora.csv",
    ]

    for path in candidates:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.parent.joinpath(".write_test").open("w", encoding="utf-8") as f:
                f.write("ok")
            path.parent.joinpath(".write_test").unlink(missing_ok=True)
            return path
        except Exception:
            continue

    return LOG_PATH


def guardar_log_calculo(
    referencia,
    ids,
    features,
    prediccion,
):
    """
    Guarda una fila del histórico en Google Sheets y, como respaldo,
    también en CSV local. Retorna un diccionario con el resultado.
    """
    
    fila = {
        "fecha": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "referencia": str(referencia),
        "ids_deuda": ",".join(map(str, ids)),
        "plazo": features.get("PRI-ULT"),
        "ratio_pp": features.get("Ratio_PP"),
        "cuota_apartado": features.get("C/A"),
        "amount_total": features.get("AMOUNT_TOTAL"),
        "prediccion": prediccion,
    }
    
    sheet_ok, sheet_dest, sheet_err = _append_row_to_google_sheet(fila)

    log_path = _get_writable_log_path()
    local_ok = False
    local_err = None
    try:
        df_log = pd.DataFrame([fila])
        file_exists = log_path.exists() and log_path.stat().st_size > 0
        df_log.to_csv(
            log_path,
            mode="a" if file_exists else "w",
            header=not file_exists,
            index=False,
            encoding="utf-8-sig",
        )
        local_ok = True
    except Exception as e:
        local_err = str(e)

    return {
        "sheet_ok": sheet_ok,
        "sheet_destination": sheet_dest,
        "sheet_error": sheet_err,
        "local_ok": local_ok,
        "local_path": log_path,
        "local_error": local_err,
    }

def enviar_aprobacion_estructurados(
    *,
    referencia,
    ids,
    bancos,
    correo_electronico,
    condonacion_mensualidades,
    comision_exito_total,
    ce_inicial,
    prediccion,
    link_carta_firmada,
    link_pantallazo,
    tipo_liquidacion="",
):
    tipo_liquidacion_norm = _norm(tipo_liquidacion)
    umbral_aprobacion = 0.8 if "tradicional" in tipo_liquidacion_norm else 0.74
    es_aprobado = float(prediccion or 0.0) >= float(umbral_aprobacion)

    respuestas_row = [
        datetime.now().strftime("%d/%m/%Y %H:%M:%S"),  # A Marca temporal
        str(correo_electronico or "").strip(),  # B Correo
        str(referencia),  # C Referencia
        "-".join(map(str, ids)),  # D ID deuda
        str(bancos),  # E Banco
        str(link_carta_firmada or "").strip(),  # F Carta pagaré firmado
        str(link_pantallazo or "").strip(),  # G Pantallazo aceptación
        "Sí" if str(condonacion_mensualidades).strip().lower() == "si" else "No",  # H Condonación
        str(link_pantallazo or "").strip(),  # I Adjuntar pantallazo de correo
        float(comision_exito_total or 0.0),  # J Comisión total
        float(ce_inicial or 0.0),  # K Primera comisión
        "",  # L
        "",  # M
        "",  # N
        "TRUE" if es_aprobado else "FALSE",  # O Aprobación Estructurados
        "Aprobado" if es_aprobado else "",  # P Estado
        "",  # Q
        "",  # R
        "",  # S
        "",  # T
        "",  # U
        float(prediccion or 0.0),  # V Calculadora
    ]
    estr_ok, estr_dest, estr_err = _append_row_to_respuestas_estr(respuestas_row)
    return {
        "estr_ok": estr_ok,
        "estr_destination": estr_dest,
        "estr_error": estr_err,
        "es_aprobado": es_aprobado,
        "umbral_aprobacion": umbral_aprobacion,
    }
        

# ------------------ utilidades ------------------
def _norm(s: str) -> str:
    # ✅ MEJORADO: soporta guiones, underscores, espacios raros, etc.
    s = str(s).strip().lower()
    rep = str.maketrans("áéíóúü", "aeiouu")
    s = s.translate(rep)
    s = s.replace("\xa0", " ")
    s = s.replace("_", " ").replace("-", " ")
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _find_col(df: pd.DataFrame, candidates):
    cols = {_norm(c): c for c in df.columns}
    for cand in candidates:
        if _norm(cand) in cols:
            return cols[_norm(cand)]
    return None

####################################################
###################################################

def _find_col_contains(df: pd.DataFrame, required_terms: list[str]):
    normalized_terms = [_norm(term) for term in required_terms]
    for col in df.columns:
        normalized_col = _norm(col)
        if all(term in normalized_col for term in normalized_terms):
            return col
    return None
#####################################################
####################################################

@st.cache_data(ttl=900, show_spinner=False)
def _read_file(file):
    if file.name.lower().endswith(".csv"):
        return pd.read_csv(file)
    return pd.read_excel(file, engine="openpyxl")

@st.cache_data(show_spinner=False)
def _normalize_numeric(df, cols):
    df2 = df.copy()
    for c in cols:
        df2[c] = pd.to_numeric(df2[c].astype(str).str.replace(",", ""), errors="coerce")
    return df2

@st.cache_data(show_spinner=False)
def _map_columns(columns_list: tuple[str, ...]):
    dummy_df = pd.DataFrame(columns=list(columns_list))
    col_ref   = _find_col(dummy_df, ["Referencia"])
    col_id    = _find_col(dummy_df, ["Id deuda","id deuda","id_deuda"])
    col_banco = _find_col(dummy_df, ["Banco"])

    # ✅ MEJORADO: acepta Deuda Resuelve o D_BRAVO (cualquier escritura)
    col_deu   = _find_col(dummy_df, [
        "Deuda Resuelve", "deuda resuelve", "deuda_resuelve", "deuda-resuelve",
        "D_BRAVO", "d_bravo", "d bravo", "d-bravo", "dbravo"
    ])

    col_apar  = _find_col(dummy_df, ["Apartado Mensual","apartado mensual"])
    col_com   = _find_col(dummy_df, ["Comisión Mensual","comision mensual","comisión mensual"])
    col_saldo = _find_col(dummy_df, ["Saldo","Ahorro"])
    col_ce    = _find_col(dummy_df, ["CE"])
    return col_ref, col_id, col_banco, col_deu, col_apar, col_com, col_saldo, col_ce

# ---- Helpers para inputs en pesos con separador de miles (solo otros campos) ----
def _format_pesos(value) -> str:
    try:
        v = float(value)
    except Exception:
        return ""
    if np.isnan(v):
        return ""
    # Formato colombiano: punto como separador de miles, sin decimales
    return f"{v:,.0f}".replace(",", ".")

def pesos_input(label: str, key: str, help: str | None = None, disabled: bool = False):
    """
    Input de texto para pesos colombianos (para Deuda, Apartado, etc.)
    NO se usa para Comisión de éxito para evitar conflictos.
    """
    raw_val = st.session_state.get(key, 0.0)
    try:
        base_val = float(raw_val or 0.0)
    except Exception:
        base_val = 0.0

    default_txt = _format_pesos(base_val)
    txt = st.text_input(
        label,
        value=default_txt,
        key=f"{key}_display",
        help=help,
        disabled=disabled
    )

    txt_clean = txt.strip().replace(".", "").replace(",", "")
    if txt_clean == "":
        new_val = 0.0
    else:
        try:
            new_val = float(txt_clean)
        except Exception:
            new_val = base_val  # si no se puede parsear, dejamos el valor anterior

    if new_val < 0:
        new_val = 0.0

    st.session_state[key] = new_val
    return new_val

# -------- helpers de CE --------
def _prefill_ce():
    """
    Comisión de éxito automática mientras no esté override:
    (Deuda Resuelve - PAGO BANCO) * 1.19 * CE base
    """
    if st.session_state.get("comision_exito_overridden", False):
        return
    deuda_res = float(st.session_state.get("deuda_res_edit", 0.0) or 0.0)
    pago_bco  = float(st.session_state.get("pago_banco", 0.0) or 0.0)
    ce_base   = float(st.session_state.get("ce_base", 0.0) or 0.0)
    ce = max(0.0, (deuda_res - pago_bco) * 1.19 * ce_base)
    st.session_state.comision_exito = ce
    st.session_state.comision_exito_auto = ce  # guardamos referencia del valor "auto"
    
# =================== 1) Cargar base ===================
st.markdown("### 1) Base `cartera_asignada_filtrada`")

DEBUG = False  # ← pon True solo cuando quieras ver debug

df_base = load_repo_base(_data_version())
src_badge = None

# 🔎 DEBUG 1: ¿qué archivo se está usando?
if DEBUG:
    if DATA_PARQUET.exists():
        st.info("📌 Leyendo PARQUET: data/cartera_asignada_filtrada.parquet")
    elif DATA_CSV.exists():
        st.info("📌 Leyendo CSV: data/cartera_asignada_filtrada.csv")
    else:
        st.warning("📌 No hay base en data/, usando subida manual")

# 🔎 DEBUG 2: ver columnas EXACTAS (repr)
if DEBUG and df_base is not None:
    st.write("🧾 Columnas detectadas (repr):")
    st.write([repr(c) for c in df_base.columns])

# 🧹 LIMPIEZA FUERTE de nombres de columnas (quita caracteres invisibles)
def _clean_colname(c):
    c = str(c)
    c = c.replace("\ufeff", "")   # BOM
    c = c.replace("\u200b", "")   # zero-width space
    c = c.replace("\xa0", " ")    # NBSP
    c = c.strip()
    c = re.sub(r"\s+", " ", c)    # colapsa espacios múltiples
    return c

if df_base is not None:
    df_base.columns = [_clean_colname(c) for c in df_base.columns]

# 🔎 DEBUG 3: columnas DESPUÉS de limpiar
if DEBUG and df_base is not None:
    st.write("🧾 Columnas limpiadas:")
    st.write(list(df_base.columns))

###########################################################################################################
if df_base is not None:
    src_badge = "📦 Fuente: data/ (workflow semanal)"
    st.success("✅ Cargada automáticamente desde el repo.")
else:
    src_badge = "📤 Fuente: subida manual"
    st.info("No encontré la base en `data/`. Sube un CSV/XLSX como respaldo.")
    up = st.file_uploader("📂 Subir `cartera_asignada_filtrada`", type=["csv", "xlsx"])
    if not up:
        st.stop()
    try:
        df_base = _read_file(up)
    except Exception as e:
        st.error(f"No pude leer el archivo: {e}")
        st.stop()

st.caption(src_badge)
# Mapear columnas obligatorias
colnames_tuple = tuple(map(str, df_base.columns))
col_ref, col_id, col_banco, col_deu, col_apar, col_com, col_saldo, col_ce = _map_columns(colnames_tuple)

needed = {"Referencia": col_ref, "Id deuda": col_id, "Banco": col_banco,
          "Deuda Resuelve": col_deu, "Apartado Mensual": col_apar,
          "Comisión Mensual": col_com, "Saldo/Ahorro": col_saldo, "CE": col_ce}
faltan = [k for k, v in needed.items() if v is None]
if faltan:
    st.error("Faltan columnas requeridas: " + ", ".join(faltan))
    st.stop()

df_base = _normalize_numeric(df_base, [col_deu, col_apar, col_com, col_saldo, col_ce])
st.success(f"✅ Base lista • filas: {len(df_base):,}")

# =================== 2) Referencia → seleccionar id(s) ===================
st.markdown("### 2) Referencia → seleccionar **Id deuda** (uno o varios)")
ref_input = st.text_input("🔎 Escribe la **Referencia** (exacta como aparece en la base)")
if not ref_input:
    st.stop()

df_ref = df_base[df_base[col_ref].astype(str) == str(ref_input)]
if df_ref.empty:
    st.warning("No encontramos esa referencia en la base.")
    st.stop()

st.subheader("Resultados (elige Id deuda)")
df_preview = df_ref[[col_id, col_banco]].head(500)
st.dataframe(df_preview.reset_index(drop=True), use_container_width=True)

ids_opciones = df_ref[col_id].astype(str).tolist()
ids_sel = st.multiselect("Seleccione **uno o varios** Id deuda", ids_opciones, default=ids_opciones[:1])
if not ids_sel:
    st.info("Selecciona al menos un Id deuda para continuar.")
    st.stop()

sel = df_ref[df_ref[col_id].astype(str).isin(ids_sel)].copy()
col_tipo_liquidacion = _find_col(sel, ["Tipo de Liquidacion", "Tipo Liquidacion", "Tipo de liquidación"]) or _find_col_contains(sel, ["tipo", "liquid"])
tipo_liquidacion_val = _join_unique_values(sel[col_tipo_liquidacion].tolist()) if col_tipo_liquidacion else ""
bancos_sel_text = _join_unique_values(sel[col_banco].astype(str).tolist())

# =================== 3) Valores base (reactivo) ===================
st.markdown("### 3) Valores base (puedes editarlos)")

fila_primera = sel.iloc[0]
deuda_res_total_def = float(sel[col_deu].sum(skipna=True))
apartado_base_def   = float(fila_primera[col_apar]) if pd.notna(fila_primera[col_apar]) else 0.0
comision_m_base_def = float(fila_primera[col_com]) if pd.notna(fila_primera[col_com]) else 0.0
saldo_base_def      = float(fila_primera[col_saldo]) if pd.notna(fila_primera[col_saldo]) else 0.0
ce_base_def         = float(fila_primera[col_ce]) if pd.notna(fila_primera[col_ce]) else 0.0

sig_sel = (str(ref_input), tuple(sorted(map(str, ids_sel))))
if st.session_state.get("sig_sel") != sig_sel:
    st.session_state.clear()
    st.session_state.sig_sel = sig_sel

    st.session_state.deuda_res_edit = deuda_res_total_def
    st.session_state.comision_m_edit = comision_m_base_def
    st.session_state.apartado_edit   = apartado_base_def
    st.session_state.saldo_edit      = saldo_base_def
    st.session_state.ce_base         = ce_base_def

    st.session_state.pago_banco      = 0.0
    st.session_state.n_pab           = 1
    st.session_state.primer_pago_banco = 0.0

    # Flags CE
    st.session_state.comision_exito_overridden = False

    # Comisión de éxito inicial (PAGO BANCO = 0)
    ce_ini = max(0.0, (deuda_res_total_def - 0.0) * 1.19 * ce_base_def)
    st.session_state.comision_exito = ce_ini
    st.session_state.comision_exito_auto = ce_ini

    st.session_state.ce_inicial_val  = 0.0

    st.session_state._last_pab = st.session_state.pago_banco
    st.session_state._last_n   = st.session_state.n_pab

    st.rerun()

col1, col2, col3, col4 = st.columns(4)
with col1:
    pesos_input("💰 Deuda Resuelve", key="deuda_res_edit")
with col2:
    pesos_input("🎯 Comisión Mensual", key="comision_m_edit")
with col3:
    pesos_input("📆 Apartado Mensual", key="apartado_edit")
with col4:
    pesos_input("💼 Saldo (Ahorro)", key="saldo_edit")

# 3.4 Saldo Neto y Depósito
saldo_neto = 0.0
if pd.notna(st.session_state.saldo_edit) and st.session_state.saldo_edit > 0:
    saldo_neto = float(st.session_state.saldo_edit) - (float(st.session_state.saldo_edit) - 25000.0) * 0.004
    saldo_neto = max(0.0, saldo_neto)
saldo_neto_disp = float(np.round(saldo_neto, 0))

col5, col6 = st.columns(2)
with col5:
    st.number_input(
        "🧾 Saldo Neto",
        value=saldo_neto_disp,
        step=1000.0,
        min_value=0.0,
        format="%.0f",
        disabled=True,
        help="Saldo − (Saldo − 25.000) × 0.004 (solo si Saldo > 0)"
    )
with col6:
    pesos_input("💵 Depósito", key="deposito_edit",
                help="Monto extra aportado al inicio; por defecto 0")

# =================== 4) PAGO BANCO y derivados ===================
st.markdown("### 4) PAGO BANCO y parámetros derivados")

c1, c2, c3 = st.columns([1,1,1])
with c1:
    pesos_input("🏦 PAGO BANCO", key="pago_banco")
with c2:
    descuento = None
    if st.session_state.deuda_res_edit and st.session_state.deuda_res_edit > 0:
        descuento = max(0.0, 1.0 - (st.session_state.pago_banco / st.session_state.deuda_res_edit)) * 100.0
    st.text_input(
        "📉 DESCUENTO (%)",
        value=(f"{descuento:.2f} %" if descuento is not None else ""),
        disabled=True
    )
with c3:
    st.number_input(
        "🧮 N PaB",
        min_value=1,
        step=1,
        value=int(st.session_state.n_pab),
        key="n_pab"
    )

# --- Lógica: si cambia N PaB, recalculamos Primer PAGO BANCO ---
pago_banco = float(st.session_state.get("pago_banco", 0.0) or 0.0)
n_pab = int(st.session_state.get("n_pab", 1) or 1)

prev_n_pab = st.session_state.get("_prev_n_pab_for_primer", n_pab)
if n_pab != prev_n_pab:
    # Cambió el N PaB → recalculamos primer pago
    if n_pab > 1:
        if pago_banco > 0:
            st.session_state.primer_pago_banco = pago_banco / n_pab
        else:
            st.session_state.primer_pago_banco = 0.0
    else:
        # Si vuelve a 1, todo el PAGO BANCO va al primer pago
        st.session_state.primer_pago_banco = pago_banco
st.session_state._prev_n_pab_for_primer = n_pab
# --------------------------------------------------------

# Campo adicional: Primer PAGO BANCO solo si N PaB > 1
if n_pab > 1:
    pago_banco_actual = float(st.session_state.pago_banco or 0.0)

    # Aseguramos que no supere el total ni sea negativo
    st.session_state.primer_pago_banco = min(
        max(float(st.session_state.get("primer_pago_banco", 0.0) or 0.0), 0.0),
        pago_banco_actual
    )

    pesos_input(
        "💳 Primer PAGO BANCO",
        key="primer_pago_banco",
        help="Monto del primer pago al banco (el resto se reparte en los siguientes PaB)."
    )
else:
    # Si solo hay un PaB, el primer pago es todo el PAGO BANCO
    st.session_state.primer_pago_banco = float(st.session_state.pago_banco or 0.0)

# Detectar cambios en PAGO BANCO / N PaB para recalcular CE
if (st.session_state._last_pab != st.session_state.pago_banco) or (st.session_state._last_n != st.session_state.n_pab):
    st.session_state._last_pab = st.session_state.pago_banco
    st.session_state._last_n   = st.session_state.n_pab
    _prefill_ce()

# Comisión de éxito (editable) y CE inicial
c4, c5 = st.columns(2)
with c4:
    # Valor automático de referencia
    auto_ce = float(st.session_state.get("comision_exito_auto", st.session_state.get("comision_exito", 0.0)) or 0.0)
    current_ce = float(st.session_state.get("comision_exito", 0.0) or 0.0)

    new_ce = st.number_input(
        "🏁 Comisión de éxito (editable)",
        key="comision_exito",
        value=current_ce,
        step=1000.0,
        format="%.0f",
        help="Por defecto se calcula con la fórmula, pero puedes ajustarla manualmente."
    )

    # Si el valor actual se separa del valor "auto", marcamos override
    if not st.session_state.get("comision_exito_overridden", False):
        if abs(new_ce - auto_ce) > 0.5:
            st.session_state.comision_exito_overridden = True

with c5:
    pesos_input("🧪 CE inicial", key="ce_inicial_val")

# Avance CE inicial vs Comisión de éxito
st.markdown("#### Avance de CE inicial sobre la Comisión de éxito")
ce_inicial = float(st.session_state.ce_inicial_val or 0.0)
base = float(st.session_state.comision_exito or 0.0)
if ce_inicial <= 0:
    st.info("Escribe un valor en **CE inicial** para ver el porcentaje.")
elif base <= 0:
    st.warning("La **Comisión de éxito** debe ser mayor a 0 para calcular el porcentaje.")
else:
    porcentaje = (ce_inicial / base) * 100.0
    porcentaje_capped = max(0.0, min(porcentaje, 100.0))
    st.progress(int(round(porcentaje_capped)))
    st.caption(f"CE inicial: {ce_inicial:,.0f}  |  Comisión de éxito: {base:,.0f}  →  **{porcentaje:,.2f}%**")

# =================== 6) Validación y KPIs (sin tabla) ===================
st.markdown("### 6) Validación y KPIs")

pago_banco        = float(st.session_state.get("pago_banco", 0.0) or 0.0)
n_pab             = int(st.session_state.get("n_pab", 1) or 1)
comision_mensual  = float(st.session_state.get("comision_m_edit", 0.0) or 0.0)
apartado_mensual  = float(st.session_state.get("apartado_edit", 0.0) or 0.0)
comision_exito    = float(st.session_state.get("comision_exito", 0.0) or 0.0)
ce_inicial        = float(st.session_state.get("ce_inicial_val", 0.0) or 0.0)

plazo = st.number_input("📅 PLAZO (meses) (lo ingresas tú)", min_value=1, step=1, value=1)

# Primer PAGO BANCO: si hay más de un PaB, usamos el input; si no, todo el pago
primer_pago_banco = float(
    st.session_state.get(
        "primer_pago_banco",
        pago_banco if n_pab == 1 else (pago_banco / n_pab if n_pab > 0 else 0.0)
    )
)
primer_pago_banco = min(max(primer_pago_banco, 0.0), pago_banco)
resto_pago_banco = max(0.0, pago_banco - primer_pago_banco)

pct_primer_pago = (ce_inicial / comision_exito) if comision_exito > 0 else np.nan

if (plazo - 1) > 0 and apartado_mensual > 0:
    numerador = (comision_exito + resto_pago_banco - ce_inicial + comision_mensual)
    cuota_apartado = (numerador / (plazo - 1)) / apartado_mensual
else:
    cuota_apartado = np.nan

cronograma_df, cronograma_meta = construir_cronograma_pagos(
    fecha_inicial=date.today(),
    plazo=int(plazo),
    n_pab=n_pab,
    pago_banco_total=pago_banco,
    primer_pago_banco=primer_pago_banco,
    comision_total=comision_exito,
    comision_inicial=ce_inicial,
)

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.number_input("🏁 Comisión de éxito total", value=float(comision_exito), step=0.0, format="%.0f", disabled=True)
with c2:
    st.number_input("📅 PLAZO (meses)", value=float(plazo), step=1.0, format="%.0f", disabled=True)
with c3:
    st.text_input(
        "% Primer Pago (CE inicial / CE)",
        value=("—" if np.isnan(pct_primer_pago) else f"{pct_primer_pago:.2f}"),
        disabled=True
    )
with c4:
    st.text_input(
        "Cuota/Apartado",
        value=("—" if np.isnan(cuota_apartado) else f"{cuota_apartado:.4f}"),
        disabled=True
    )

if st.session_state.get("comision_exito_overridden", False):
    st.caption("🔒 Comisión de éxito fijada manualmente: no se recalcula con cambios en PAGO BANCO/N PaB.")
############################################################################################################################################################################
st.markdown("### 6.1) Flujo sugerido de pagos")

fecha_cfg_1, fecha_cfg_2, fecha_cfg_3, fecha_cfg_4, fecha_cfg_5 = st.columns([1.2, 1, 1.2, 1, 1])
with fecha_cfg_1:
    modo_fecha_banco = st.radio("Fechas banco", options=["Fin de mes", "Día fijo"], horizontal=True, key="modo_fecha_banco")
with fecha_cfg_2:
    dia_pago_banco = None
    if modo_fecha_banco == "Día fijo":
        dia_pago_banco = int(st.number_input("Día banco", min_value=1, max_value=31, value=int(st.session_state.get("dia_pago_banco", 15)), step=1, key="dia_pago_banco"))
    else:
        st.caption("Banco: fin de mes.")
with fecha_cfg_3:
    modo_fecha_comision = st.radio("Fechas comisión", options=["Fin de mes", "Día fijo"], horizontal=True, key="modo_fecha_comision")
with fecha_cfg_4:
    dia_pago_comision = None
    if modo_fecha_comision == "Día fijo":
        dia_pago_comision = int(st.number_input("Día comisión", min_value=1, max_value=31, value=int(st.session_state.get("dia_pago_comision", 15)), step=1, key="dia_pago_comision"))
    else:
        st.caption("Comisión: fin de mes.")
with fecha_cfg_5:
    if st.button("Restablecer cronograma", use_container_width=True):
        st.session_state.pop("cronograma_editor", None)
        st.session_state.pop("cronograma_overrides", None)
        st.rerun()

cronograma_df, cronograma_meta = construir_cronograma_pagos(
    fecha_inicial=date.today(),
    plazo=int(plazo),
    n_pab=n_pab,
    pago_banco_total=pago_banco,
    primer_pago_banco=primer_pago_banco,
    comision_total=comision_exito,
    comision_inicial=ce_inicial,
    dia_pago_banco=dia_pago_banco,
    dia_pago_comision=dia_pago_comision,
)

totales_por_tipo = {"banco": float(pago_banco), "comision": float(comision_exito)}
cronograma_overrides = st.session_state.get("cronograma_overrides", {})
cronograma_editor_state = st.session_state.get("cronograma_editor", {})

cronograma_base_editado, _ = aplicar_overrides_cronograma(
    cronograma_df=cronograma_df,
    overrides_map=cronograma_overrides,
    totales_por_tipo=totales_por_tipo,
    fecha_inicial=date.today(),
    dia_pago_banco=dia_pago_banco,
    dia_pago_comision=dia_pago_comision,
    primer_pago_banco_input=primer_pago_banco,
    comision_inicial_input=ce_inicial,
)

cronograma_base_visible = cronograma_base_editado[cronograma_base_editado["Cantidad"] > 0.005].reset_index(drop=True)
cronograma_locked_rows_changed = False
for row_position_str, cambios in (cronograma_editor_state.get("edited_rows", {}) or {}).items():
    try:
        row_position = int(row_position_str)
    except (TypeError, ValueError):
        continue
    if row_position < 0 or row_position >= len(cronograma_base_visible):
        continue
    row = cronograma_base_visible.iloc[row_position]
    if int(row["months_ahead"]) == 0:
        cronograma_locked_rows_changed = True
        continue
    row_key = str(row["row_key"])
    existing = cronograma_overrides.get(row_key, {})
    existing.update(cambios)
    cronograma_overrides[row_key] = existing
st.session_state["cronograma_overrides"] = cronograma_overrides

if cronograma_locked_rows_changed:
    st.session_state.pop("cronograma_editor", None)
    st.rerun()

cronograma_editado, advertencias_cronograma = aplicar_overrides_cronograma(
    cronograma_df=cronograma_df,
    overrides_map=cronograma_overrides,
    totales_por_tipo=totales_por_tipo,
    fecha_inicial=date.today(),
    dia_pago_banco=dia_pago_banco,
    dia_pago_comision=dia_pago_comision,
    primer_pago_banco_input=primer_pago_banco,
    comision_inicial_input=ce_inicial,
)

for advertencia in advertencias_cronograma:
    st.warning(advertencia)

cronograma_view = cronograma_editado[cronograma_editado["Cantidad"] > 0.005][["Fecha", "Cantidad", "Concepto"]].copy()
if not cronograma_view.empty:
    cronograma_view["Fecha"] = pd.to_datetime(cronograma_view["Fecha"])
    cronograma_view["Cantidad"] = (
        pd.to_numeric(cronograma_view["Cantidad"], errors="coerce")
        .fillna(0.0)
        .round(0)
        .astype(int)
        .map(lambda x: f"$ {x:,}")
    )
    cronograma_view.index = range(1, len(cronograma_view) + 1)
    st.caption("Sugerencia: banco y comisión van en meses diferentes, pero si mueves una comisión al mismo mes del banco se respeta y las demás comisiones siguen ocupando los meses restantes sin dejar huecos.")
    st.data_editor(
        cronograma_view,
        key="cronograma_editor",
        use_container_width=True,
        num_rows="fixed",
        hide_index=False,
        column_config={
            "Fecha": st.column_config.DateColumn("Fecha", format="DD/MM/YYYY"),
            "Cantidad": st.column_config.TextColumn("Cantidad"),
            "Concepto": st.column_config.TextColumn("Concepto", disabled=True),
        },
        disabled=["Concepto"],
    )
else:
    st.info("Aún no hay valores suficientes para construir el cronograma.")

st.markdown("### PLAN DE LIQUIDACIÓN ESTRUCTURADA")

plan_df = construir_plan_liquidacion(cronograma_editado, comision_mensual)
plan_overrides = st.session_state.get("plan_liquidacion_overrides", {})
plan_editor_state = st.session_state.get("plan_liquidacion_editor", {})

for row_position_str, cambios in (plan_editor_state.get("edited_rows", {}) or {}).items():
    try:
        row_position = int(row_position_str)
    except (TypeError, ValueError):
        continue
    if row_position < 0 or row_position >= len(plan_df):
        continue
    plan_key = str(plan_df.iloc[row_position]["plan_key"])
    if "Comisión Mensual" in cambios:
        plan_overrides[plan_key] = _parse_amount_input(cambios["Comisión Mensual"])

st.session_state["plan_liquidacion_overrides"] = plan_overrides

if not plan_df.empty:
    plan_df["Comisión Mensual"] = plan_df.apply(
        lambda row: float(plan_overrides.get(str(row["plan_key"]), row["Comisión Mensual"])),
        axis=1,
    )
    plan_df["Apartado Requerido"] = plan_df["Pago a Banco"] + plan_df["Comisión de Éxito"] + plan_df["Comisión Mensual"]

    plan_view = plan_df.copy()
    plan_view["Fecha Límite de Pago"] = pd.to_datetime(plan_view["Fecha Límite de Pago"])
    plan_view["Pago a Banco"] = plan_view["Pago a Banco"].map(_format_currency0)
    plan_view["Comisión de Éxito"] = plan_view["Comisión de Éxito"].map(_format_currency0)
    plan_view["Comisión Mensual"] = plan_view["Comisión Mensual"].map(_format_currency0)
    plan_view["Apartado Requerido"] = plan_view["Apartado Requerido"].map(_format_currency0)
    plan_view = plan_view.drop(columns=["plan_key"])
    plan_view.index = range(1, len(plan_view) + 1)

    st.data_editor(
        plan_view,
        key="plan_liquidacion_editor",
        use_container_width=True,
        num_rows="fixed",
        hide_index=False,
        column_config={
            "Fecha Límite de Pago": st.column_config.DateColumn("Fecha Límite de Pago", format="DD/MM/YYYY"),
            "Pago a Banco": st.column_config.TextColumn("Pago a Banco", disabled=True),
            "Comisión de Éxito": st.column_config.TextColumn("Comisión de Éxito", disabled=True),
            "Comisión Mensual": st.column_config.TextColumn("Comisión Mensual"),
            "Apartado Requerido": st.column_config.TextColumn("Apartado Requerido", disabled=True),
        },
        disabled=["Pago a Banco", "Comisión de Éxito", "Apartado Requerido"],
    )
else:
    st.info("Aún no hay datos suficientes para construir el plan de liquidación.")

#############################################################################################################################################################################

def _build_document_context_inputs(default_context: dict[str, str]) -> dict[str, str]:
    with st.expander("Campos editables del documento", expanded=False):
        st.caption("Antes de descargar el Word, puedes revisar y corregir aquí los datos que se van a escribir en la plantilla.")

        col1, col2 = st.columns(2)
        with col1:
            referencia_doc = st.text_input("Referencia documento", value=str(default_context.get("referencia", "")), key="doc_referencia")
            dia_firma_doc = st.text_input("Día firma", value=str(default_context.get("dia_firma", "")), key="doc_dia_firma", disabled=True)
            mes_firma_doc = st.text_input("Mes firma", value=str(default_context.get("mes_firma", "")), key="doc_mes_firma", disabled=True)
            anio_firma_doc = st.text_input("Año firma", value=str(default_context.get("anio_firma", "")), key="doc_anio_firma", disabled=True)
            entidad_financiera_doc = st.text_input("Entidad financiera", value=str(default_context.get("entidad_financiera", "")), key="doc_entidad_financiera")
            nombre_cliente_doc = st.text_input("Nombre cliente", value=str(default_context.get("nombre_cliente", "")), key="doc_nombre_cliente")
            correo_cliente_doc = st.text_input("Correo cliente", value=str(default_context.get("correo_cliente", "")), key="doc_correo_cliente")
            telefono_cliente_doc = st.text_input("Teléfono cliente", value=str(default_context.get("telefono_cliente", "")), key="doc_telefono_cliente")
        with col2:
            numero_producto_doc = st.text_input("Número producto", value=str(default_context.get("numero_producto", "")), key="doc_numero_producto")
            vehiculo_doc = st.text_input("Vehículo", value=str(default_context.get("vehiculo", "")), key="doc_vehiculo")
            cedula_cliente_doc = st.text_input("Cédula cliente", value=str(default_context.get("cedula_cliente", "")), key="doc_cedula_cliente")
            ciudad_cliente_doc = st.text_input("Ciudad cliente", value=str(default_context.get("ciudad_cliente", "")), key="doc_ciudad_cliente")
            direccion_cliente_doc = st.text_input("Dirección cliente", value=str(default_context.get("direccion_cliente", "")), key="doc_direccion_cliente")
            pago_banco_doc = st.text_input("Pago banco documento", value=str(default_context.get("pago_banco", "")), key="doc_pago_banco", disabled=True)
            comision_total_doc = st.text_input("Comisión total documento", value=str(default_context.get("comision_total", "")), key="doc_comision_total", disabled=True)

    context = default_context.copy()
    context.update({
        "referencia": referencia_doc,
        "dia_firma": dia_firma_doc,
        "mes_firma": mes_firma_doc,
        "anio_firma": anio_firma_doc,
        "entidad_financiera": entidad_financiera_doc,
        "nombre_cliente": nombre_cliente_doc,
        "correo_cliente": correo_cliente_doc,
        "telefono_cliente": telefono_cliente_doc,
        "numero_producto": numero_producto_doc,
        "vehiculo": vehiculo_doc,
        "cedula_cliente": cedula_cliente_doc,
        "ciudad_cliente": ciudad_cliente_doc,
        "direccion_cliente": direccion_cliente_doc,
        "pago_banco": pago_banco_doc,
        "comision_total": comision_total_doc,
        "suma_comisiones": default_context.get("suma_comisiones", comision_total_doc),
        "Suma_comisiones": default_context.get("suma_comisiones", comision_total_doc),
        "suma comisiones": default_context.get("suma_comisiones", comision_total_doc),
        "suma_comisiones_letras": default_context.get("suma_comisiones_letras", ""),
    })
    return context


def _missing_document_fields(context: dict[str, str]) -> list[str]:
    required_labels = {
        "referencia": "Referencia documento",
        "dia_firma": "Día firma",
        "mes_firma": "Mes firma",
        "anio_firma": "Año firma",
        "entidad_financiera": "Entidad financiera",
        "nombre_cliente": "Nombre cliente",
        "correo_cliente": "Correo cliente",
        "telefono_cliente": "Teléfono cliente",
        "numero_producto": "Número producto",
        "vehiculo": "Vehículo",
        "cedula_cliente": "Cédula cliente",
        "ciudad_cliente": "Ciudad cliente",
        "direccion_cliente": "Dirección cliente",
        "pago_banco": "Pago banco documento",
        "comision_total": "Comisión total documento",
    }
    missing = []
    for key, label in required_labels.items():
        if not str(context.get(key, "")).strip():
            missing.append(label)
    return missing


st.markdown("### 6.2) Exportar documento estructurado")
st.caption(
    "La plantilla Word de `data/` se rellena con las dos tablas visibles en pantalla. "
    "Las celdas exportadas se normalizan en Times New Roman 9.5 para que el estilo interno coincida con el resto del documento."
)

if "doc_graduacion_confirmada" not in st.session_state:
    st.session_state.doc_graduacion_confirmada = False
if "doc_graduacion_pendiente" not in st.session_state:
    st.session_state.doc_graduacion_pendiente = False
if "doc_graduacion_check" not in st.session_state:
    st.session_state.doc_graduacion_check = False

st.checkbox(
    "Graduar cliente",
    key="doc_graduacion_check",
    help="Incluye el punto 6 de la primera página solo después de confirmarlo.",
)

if not st.session_state.doc_graduacion_check:
    st.session_state.doc_graduacion_confirmada = False
    st.session_state.doc_graduacion_pendiente = False
elif not st.session_state.doc_graduacion_confirmada:
    st.session_state.doc_graduacion_pendiente = True

if st.session_state.doc_graduacion_pendiente:
    st.caption("Confirma si el cliente sí se va a graduar.")
    col_confirmar_si, col_confirmar_no = st.columns([1, 1, 4])[:2]
    with col_confirmar_si:
        if st.button("Sí", key="confirmar_graduacion_si"):
            st.session_state.doc_graduacion_confirmada = True
            st.session_state.doc_graduacion_pendiente = False
            st.rerun()
    with col_confirmar_no:
        if st.button("No", key="confirmar_graduacion_no"):
            st.session_state.doc_graduacion_check = False
            st.session_state.doc_graduacion_confirmada = False
            st.session_state.doc_graduacion_pendiente = False
            st.rerun()
elif st.session_state.doc_graduacion_check and st.session_state.doc_graduacion_confirmada:
    st.caption("Graduación confirmada: el Word incluirá el punto 6 en la primera página.")

export_pdf_bytes = None
export_pdf_error = None
if not cronograma_editado.empty and not plan_df.empty:
    try:
        col_nombre_cliente = _find_col(sel, ["Nombre del cliente", "Nombre Cliente", "Nombre"]) or _find_col_contains(sel, ["nombre", "cliente"])
        col_numero_producto = _find_col(sel, ["Número de Crédito", "Numero de Credito", "Número Crédito", "Numero Producto"]) or _find_col_contains(sel, ["numero", "credito"])
        col_vehiculo = _find_col(sel, ["vehiculo", "Vehículo"]) or _find_col_contains(sel, ["vehiculo"])
        col_cedula_cliente = _find_col(sel, ["Cedula", "Cédula"]) or _find_col_contains(sel, ["cedula"])
        col_correo_cliente = _find_col(sel, ["correo", "Correo"]) or _find_col_contains(sel, ["correo"])

        cedula_cliente_value = _join_unique_values(sel[col_cedula_cliente].tolist()) if col_cedula_cliente else ""
        cliente_lookup = _lookup_cliente_info(ref_input, cedula_cliente_value)

        suma_comisiones_total = float(comision_exito) + float(plan_df["Comisión Mensual"].sum())

        template_context_default = _build_document_context(
            referencia=ref_input,
            bancos=sel[col_banco].astype(str).tolist(),
            pago_banco=pago_banco,
            comision_total=comision_exito,
            nombre_cliente=_join_unique_values(sel[col_nombre_cliente].tolist()) if col_nombre_cliente else "",
            numero_producto=_join_unique_values(sel[col_numero_producto].tolist()) if col_numero_producto else "",
            vehiculo=_join_unique_values(sel[col_vehiculo].tolist()) if col_vehiculo else "",
            cedula_cliente=cedula_cliente_value,
            correo_cliente=_join_unique_values(sel[col_correo_cliente].tolist()) if col_correo_cliente else "",
            telefono_cliente=cliente_lookup.get("telefono_cliente", ""),
            ciudad_cliente=cliente_lookup.get("ciudad_cliente", ""),
            direccion_cliente=cliente_lookup.get("direccion_cliente", ""),
            suma_comisiones_total=suma_comisiones_total,
        )
        template_context = _build_document_context_inputs(template_context_default)
        export_docx_bytes = build_recaudo_docx(
            template_path=DOCX_TEMPLATE_PATH,
            cronograma_df=cronograma_editado,
            plan_df=plan_df.drop(columns=["plan_key"], errors="ignore"),
            template_context=template_context,
            include_graduation_section=bool(st.session_state.get("doc_graduacion_check", False) and st.session_state.get("doc_graduacion_confirmada", False)),
        )
        export_pdf_bytes = convert_docx_bytes_to_pdf_bytes(export_docx_bytes)
    except Exception as export_exc:
        export_pdf_error = str(export_exc)
        st.error(f"No pude preparar el documento PDF: {export_exc}")

if export_pdf_bytes:
    missing_document_fields = _missing_document_fields(template_context)
    suma_comision_resuelve = float(
        cronograma_editado.loc[
            cronograma_editado["Concepto"].str.contains("Comisión Resuelve", na=False),
            "Cantidad",
        ].sum()
    )
    suma_pago_entidad = float(
        cronograma_editado.loc[
            cronograma_editado["Concepto"].str.contains("Entidad Financiera", na=False),
            "Cantidad",
        ].sum()
    )

    if suma_comision_resuelve > float(comision_exito) + 0.01:
        missing_document_fields.append("Ajustar cronograma: la suma de Comisión Resuelve no puede ser mayor a la Comisión de éxito total")
    if suma_pago_entidad > float(pago_banco) + 0.01:
        missing_document_fields.append("Ajustar cronograma: Pago a Entidad Financiera no puede ser mayor a PAGO BANCO")
    referencia_export = re.sub(r"[^A-Za-z0-9._-]+", " ", str(ref_input or "sin referencia")).strip() or "sin referencia"
    export_filename = f"{date.today().isoformat()} - ref {referencia_export}.pdf"
    if missing_document_fields:
        st.warning("Completa o corrige estos puntos antes de descargar el PDF: " + ", ".join(missing_document_fields))
    st.download_button(
        "⬇️ Descargar PDF con tablas",
        data=export_pdf_bytes,
        file_name=export_filename,
        mime="application/pdf",
        use_container_width=True,
        disabled=bool(missing_document_fields),
    )
else:
    if not export_pdf_error:
        st.info("Primero completa datos suficientes en el cronograma y en el plan para generar el PDF.")
#############################################################################################################################################################################
#############################################################################################################################################################################

#############################################################################################################################################################################    
# =================== 7) Predicción con el modelo ===================
st.markdown("### 7) Predicción de **recaudo_real** con MLP")

def _to_float_or_nan(x):
    try:
        return float(x)
    except Exception:
        return np.nan

feature_vals = {
    "PRI-ULT": _to_float_or_nan(plazo),
    "Ratio_PP": _to_float_or_nan(pct_primer_pago if not np.isnan(pct_primer_pago) else np.nan),
    "C/A": _to_float_or_nan(cuota_apartado if not np.isnan(cuota_apartado) else np.nan),
    "AMOUNT_TOTAL": _to_float_or_nan(comision_exito),
}

with st.expander("🔎 Ver features que se enviarán al modelo (crudas)", expanded=False):
    st.json(feature_vals)

issues = []
if np.isnan(feature_vals["AMOUNT_TOTAL"]) or feature_vals["AMOUNT_TOTAL"] < 0:
    issues.append("AMOUNT_TOTAL (Comisión de éxito total) no puede ser NaN ni negativa.")
if np.isnan(feature_vals["PRI-ULT"]) or feature_vals["PRI-ULT"] < 1:
    issues.append("PRI-ULT (PLAZO) debe ser un entero ≥ 1.")
if np.isnan(feature_vals["Ratio_PP"]) or feature_vals["Ratio_PP"] < 0:
    issues.append("Ratio_PP (% Primer Pago) no puede ser NaN ni negativo. Usa 0 si aplica.")
if np.isnan(feature_vals["C/A"]) or feature_vals["C/A"] <= 0:
    issues.append("C/A (Cuota/Apartado) debe ser > 0.")

if issues:
    st.warning("⚠️ Revisa antes de predecir:\n- " + "\n- ".join(issues))

with st.expander("📊 Diagnóstico de guardado en Google Sheets", expanded=False):
    gs_status = google_sheets_status()

    if gs_status["ok"]:
        st.success(
            "Conexión lista. "
            f"Cuenta de servicio: `{gs_status['client_email']}` • "
            f"Archivo: `{gs_status['spreadsheet_title']}` • "
            f"Hoja: `{gs_status['worksheet_title']}`"
        )
        st.caption(
            "No necesitas escribir los encabezados manualmente: la app intenta ponerlos en la fila 1 "
            "cuando guardas el primer registro."
        )
    else:
        st.error(
            "La app no pudo conectarse a Google Sheets. "
            f"Detalle: {gs_status['error']}"
        )
        st.markdown(
            f"""
**Revisa estos puntos:**
- El secreto debe existir en **Streamlit Secrets del despliegue actual** (sandbox/prod tienen secretos separados).
- Puedes usar `MI_JSON` (recomendado) o `GOOGLE_SERVICE_ACCOUNT_JSON`.
- Debes compartir el spreadsheet con este correo: `{gs_status['client_email']}`.
- La pestaña debe llamarse exactamente `{GOOGLE_SHEET_TAB}`.
- El guardado solo se ejecuta cuando presionas **Predecir recaudo**.
"""
        )

col_pred1, col_pred2 = st.columns([1,1])
with col_pred1:
    do_predict = st.button("🔮 Predecir recaudo", type="primary", use_container_width=True)
with col_pred2:
    meta = load_meta(_meta_version())
    if meta:
        st.caption(f"Modelo cargado • target: {meta.get('target','recaudo_real')}")

if do_predict:
    try:
        model = load_model(_model_version())
        FEATURES_RAW = ["PRI-ULT", "Ratio_PP", "C/A", "AMOUNT_TOTAL"]
        X_pred = pd.DataFrame([feature_vals], columns=FEATURES_RAW)
        yhat = float(model.predict(X_pred)[0])

# Ajustes existentes
        if yhat == 0.98:
            yhat_adj = yhat + 0.02
        elif yhat == 0.99:
            yhat_adj = yhat + 0.01
        else:
            yhat_adj = yhat + 0.03

# ✅ NUEVO AJUSTE
        if feature_vals["AMOUNT_TOTAL"] > 6_000_000:
            yhat_adj += 0.05

        st.success(f"✅ Predicción de recaudo: {yhat_adj:,.2f}")

        # ✅ Guardar registro del cálculo
        log_result = guardar_log_calculo(
            referencia=ref_input,
            ids=ids_sel,
            features=feature_vals,
            prediccion=yhat_adj,
        )
        st.session_state.last_prediction_value = float(yhat_adj)
        st.session_state.last_prediction_ready = True

        if log_result["sheet_ok"]:
            st.caption(f"📊 Histórico guardado en: `{log_result['sheet_destination']}`")
        else:
            st.warning(
                "No se pudo guardar el histórico en Google Sheets. "
                f"Detalle: {log_result['sheet_error']}"
            )

        if log_result["local_ok"]:
            st.caption(f"🗂️ Respaldo local guardado en: `{log_result['local_path']}`")
        elif not log_result["sheet_ok"]:
            st.error(
                "Tampoco se pudo guardar el respaldo local. "
                f"Detalle: {log_result['local_error']}"
            )

        st.caption("Entradas usadas por el pipeline (crudas):")
        st.dataframe(pd.DataFrame([feature_vals]), use_container_width=True)

    except Exception as e:
        st.error(f"Error al predecir: {e}")

st.markdown("---")
st.markdown("### 8) Envío a aprobación de estructurados")
st.caption("Obligatorio: subir carta/pagaré firmado (PDF) y pantallazo de aceptación (foto o PDF).")
st.caption("Validación carta firmada: referencia en primera hoja y texto 'QR' en la última hoja.")
correo_para_sheets = st.text_input(
    "📧 Dirección de correo electrónico (obligatorio para enviar)",
    key="correo_para_sheets",
).strip()
condonacion_mensualidades = st.selectbox(
    "¿El cliente cuenta con condonación de mensualidades? (obligatorio)",
    options=["", "Si", "No"],
    index=0,
    key="condonacion_mensualidades",
)
carta_firmada_file = st.file_uploader(
    "📎 Adjuntar carta con pagaré firmado (obligatorio, PDF)",
    type=["pdf"],
    key="carta_firmada_file",
)
pantallazo_file = st.file_uploader(
    "🖼️ Adjuntar pantallazo/foto/PDF de aceptación (obligatorio)",
    type=["pdf", "png", "jpg", "jpeg", "webp"],
    key="pantallazo_file",
)


enviar_aprobacion = st.button("Enviar AProbación estructurados", use_container_width=True)
if enviar_aprobacion:
    pred_value = st.session_state.get("last_prediction_value")
    if pred_value is None:
        st.warning("Primero debes presionar **Predecir recaudo**.")
    elif not correo_para_sheets:
        st.warning("Debes ingresar el correo electrónico antes de enviar.")
    elif condonacion_mensualidades not in {"Si", "No"}:
        st.warning("Debes seleccionar Si o No en condonación de mensualidades.")
    elif carta_firmada_file is None:
        st.warning("Debes adjuntar la carta/pagaré firmado en PDF.")
    elif pantallazo_file is None:
        st.warning("Debes adjuntar el pantallazo/foto/PDF de aceptación.")
     
    else:
        pdf_ok, pdf_error = _validate_signed_pdf(carta_firmada_file, ref_input)
        if not pdf_ok:
            envio_result = {
                "estr_ok": False,
                "estr_error": f"{pdf_error} Validación requerida: referencia en primera hoja + QR en última hoja.",
            }
            link_carta_firmada = None
            link_pantallazo = None
        else:
            try:
                link_carta_firmada = upload_streamlit_file_to_drive(
                    carta_firmada_file,
                    DRIVE_FOLDER_CARTA_FIRMADA_PATH,
                    reference=ref_input,
                    prefix="carta_firmada",
                )
                link_pantallazo = upload_streamlit_file_to_drive(
                    pantallazo_file,
                    DRIVE_FOLDER_PANTALLAZO_PATH,
                    reference=ref_input,
                    prefix="pantallazo",
                )
            except Exception as upload_exc:
                upload_msg = str(upload_exc)
                if "storageQuotaExceeded" in upload_msg or "Service Accounts do not have storage quota" in upload_msg:
                    st.error(
                        "No se pudieron subir los adjuntos a Drive por cuota/permisos de la cuenta de servicio. "
                        "El envío se detiene para evitar guardar solo links de carpeta."
                    )
                else:
                    st.error(f"No se pudieron subir los adjuntos a Drive. Detalle: {upload_exc}")
                link_carta_firmada = None
                link_pantallazo = None

            if not link_carta_firmada or not link_pantallazo:
                envio_result = {"estr_ok": False, "estr_error": "No se generaron links de Drive para ambos adjuntos."}
            else:
                envio_result = enviar_aprobacion_estructurados(
                    referencia=ref_input,
                    ids=ids_sel,
                    bancos=bancos_sel_text,
                    correo_electronico=correo_para_sheets,
                    condonacion_mensualidades=condonacion_mensualidades,
                    comision_exito_total=feature_vals.get("AMOUNT_TOTAL"),
                    ce_inicial=ce_inicial,
                    prediccion=pred_value,
                    link_carta_firmada=link_carta_firmada,
                    link_pantallazo=link_pantallazo,
                    tipo_liquidacion=tipo_liquidacion_val,
                )

        if envio_result.get("estr_ok"):
            estado_aprob = "✅ Aprobado" if envio_result["es_aprobado"] else "⛔ No aprobado"
            st.success(f"Envío exitoso a `{envio_result['estr_destination']}`.")
            st.caption(f"🔗 Carta firmada (columna F): {link_carta_firmada}")
            st.caption(f"🔗 Pantallazo/PDF (columnas G e I): {link_pantallazo}")
            st.caption(
                f"Criterio aplicado ({tipo_liquidacion_val or 'No Tradicional'}): "
                f"predicción {pred_value:.2f} vs umbral {envio_result['umbral_aprobacion']:.2f} → {estado_aprob}"
            )
        else:
            st.error(
                "No se pudo enviar a estructurados. "
                f"Detalle: {envio_result.get('estr_error')}"
            )
