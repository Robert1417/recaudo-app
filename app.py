import streamlit as st
import pandas as pd
import numpy as np
from datetime import date
# from joblib import load  # lo usaremos cuando activemos el modelo

st.set_page_config(page_title="Calculadora de Recaudo", page_icon="💸", layout="centered")
st.title("💸 Calculadora de Recaudo")

st.caption(
    "1) Carga tu base `cartera_asignada_filtrada` • "
    "2) Escribe la **Referencia** y selecciona **uno o varios Id deuda** • "
    "3) Ajusta valores editables (Deuda, Apartado, Comisión, Saldo) • "
    "4) Ingresa **PAGO BANCO** y **N PaB** → se calcula **DESCUENTO** y la **Comisión de éxito**.\n\n"
    "La tabla de pagos la agregamos en el siguiente paso."
)

# -------------------------------
# Utilidades
# -------------------------------
def _norm(s: str) -> str:
    s = str(s).strip().lower()
    rep = str.maketrans("áéíóúü", "aeiouu")
    return s.translate(rep).replace("  ", " ").replace("\xa0", " ")

def _find_col(df: pd.DataFrame, candidates):
    cols = { _norm(c): c for c in df.columns }
    for cand in candidates:
        if _norm(cand) in cols:
            return cols[_norm(cand)]
    return None

def _to_num(x):
    # convierte strings con separadores a float
    if isinstance(x, str):
        x = x.replace(",", "")
    return pd.to_numeric(x, errors="coerce")

# -------------------------------
# 1) Cargar base
# -------------------------------
st.markdown("### 1) Cargar base (CSV o Excel)")
up = st.file_uploader("📂 Sube `cartera_asignada_filtrada`", type=["csv", "xlsx"])

if not up:
    st.info("Sube un archivo para continuar.")
    st.stop()

try:
    if up.name.lower().endswith(".csv"):
        df_base = pd.read_csv(up)
    else:
        df_base = pd.read_excel(up, engine="openpyxl")
except Exception as e:
    st.error(f"No pude leer el archivo: {e}")
    st.stop()

st.success("✅ Base cargada")
st.dataframe(df_base.head(), use_container_width=True)

# Mapear nombres (tolerante a tildes/mayúsculas)
col_ref   = _find_col(df_base, ["Referencia"])
col_id    = _find_col(df_base, ["Id deuda","id deuda","id_deuda"])
col_banco = _find_col(df_base, ["Banco"])
col_deu   = _find_col(df_base, ["Deuda Resuelve","deuda resuelve"])
col_apar  = _find_col(df_base, ["Apartado Mensual","apartado mensual"])
col_com   = _find_col(df_base, ["Comisión Mensual","comision mensual","comisión mensual"])
col_saldo = _find_col(df_base, ["Saldo","Ahorro"])  # NUEVA
col_ce    = _find_col(df_base, ["CE"])              # para Comisión de éxito

needed = {"Referencia": col_ref, "Id deuda": col_id, "Banco": col_banco,
          "Deuda Resuelve": col_deu, "Apartado Mensual": col_apar,
          "Comisión Mensual": col_com, "Saldo": col_saldo, "CE": col_ce}
faltan = [k for k,v in needed.items() if v is None]
if faltan:
    st.error("Faltan columnas requeridas en tu base: " + ", ".join(faltan))
    st.stop()

# Normalizar numéricos
for c in [col_deu, col_apar, col_com, col_saldo, col_ce]:
    df_base[c] = df_base[c].apply(_to_num)

# -------------------------------
# 2) Buscar referencia y elegir uno o varios Id deuda
# -------------------------------
st.markdown("### 2) Referencia → seleccionar **Id deuda** (uno o varios)")
ref_input = st.text_input("🔎 Escribe la **Referencia** (exacta como aparece en la base)")

if not ref_input:
    st.stop()

df_ref = df_base[df_base[col_ref].astype(str) == str(ref_input)]
if df_ref.empty:
    st.warning("No encontramos esa referencia en la base.")
    st.stop()

st.subheader("Resultados de la referencia")
st.dataframe(
    df_ref[[col_ref, col_id, col_banco, col_deu, col_apar, col_com, col_saldo, col_ce]],
    use_container_width=True
)

ids_opciones = df_ref[col_id].astype(str).tolist()
ids_sel = st.multiselect("Seleccione **uno o varios** Id deuda", ids_opciones, default=ids_opciones[:1])
if not ids_sel:
    st.info("Selecciona al menos un Id deuda para continuar.")
    st.stop()

sel = df_ref[df_ref[col_id].astype(str).isin(ids_sel)].copy()

# -------------------------------
# 3) Cajas editables (prellenadas pero modificables)
#    - Deuda Resuelve (si hay varias → suma)
#    - Apartado Mensual (suma por defecto)
#    - Comisión Mensual (suma por defecto)
#    - Saldo (suma por defecto)
# -------------------------------
st.markdown("### 3) Valores base (puedes editarlos)")

deuda_res_total   = float(sel[col_deu].sum(skipna=True))
apartado_total    = float(sel[col_apar].sum(skipna=True))
comision_m_total  = float(sel[col_com].sum(skipna=True))
saldo_total       = float(sel[col_saldo].sum(skipna=True))

colA, colB = st.columns(2)
with colA:
    deuda_res_edit = st.number_input("💰 Deuda Resuelve (total seleccionado)", min_value=0.0, step=1000.0, value=deuda_res_total, format="%.0f")
    apartado_edit  = st.number_input("📆 Apartado Mensual (suma)", min_value=0.0, step=1000.0, value=apartado_total, format="%.0f")
with colB:
    comision_m_edit = st.number_input("🎯 Comisión Mensual (suma)", min_value=0.0, step=1000.0, value=comision_m_total, format="%.0f")
    saldo_edit      = st.number_input("💼 Saldo (Ahorro) (suma)", min_value=0.0, step=1000.0, value=saldo_total, format="%.0f")

# -------------------------------
# 4) PAGO BANCO → DESCUENTO, N PaB, Comisión de éxito y CE inicial
# -------------------------------
st.markdown("### 4) PAGO BANCO y parámetros derivados")

col1, col2, col3 = st.columns([1,1,1])
with col1:
    pago_banco = st.number_input("🏦 PAGO BANCO", min_value=0.0, step=1000.0, value=0.0, format="%.0f")
with col2:
    # DESCUENTO = 1 - (PAGO BANCO / Deuda Resuelve) * 100  (mostrado, no editable)
    descuento = None
    if deuda_res_edit and deuda_res_edit > 0:
        descuento = max(0.0, 1.0 - (pago_banco / deuda_res_edit)) * 100.0
    st.text_input("📉 DESCUENTO (%)", value=(f"{descuento:.2f} %" if descuento is not None else ""), disabled=True)
with col3:
    n_pab = st.number_input("🧮 N PaB", min_value=1, step=1, value=1)

# CE para la referencia (si hay varias filas, usamos promedio)
ce_ref = float(sel[col_ce].mean(skipna=True)) if sel[col_ce].notna().any() else 0.0
com_exito_default = max(0.0, (deuda_res_edit - pago_banco) * 1.19 * ce_ref)

col4, col5 = st.columns(2)
with col4:
    comision_exito = st.number_input("🏁 Comisión de éxito (editable)", min_value=0.0, step=1000.0, value=float(com_exito_default), format="%.0f",
                                     help=f"Se prellena con: (Deuda Resuelve − PAGO BANCO) × 1.19 × CE (CE promedio = {ce_ref:.4f})")
with col5:
    ce_inicial_txt = st.text_input("🧪 CE inicial (opcional)", value="", placeholder="Ej. 0.12")
    try:
        ce_inicial = float(ce_inicial_txt.replace(",", ".")) if ce_inicial_txt.strip() != "" else None
    except Exception:
        ce_inicial = None
        st.warning("CE inicial inválido; déjalo vacío o usa un número como 0.12")

# Resumen rápido
st.markdown("#### Resumen actual")
st.write({
    "Ids seleccionados": ids_sel,
    "Deuda Resuelve": deuda_res_edit,
    "Apartado Mensual": apartado_edit,
    "Comisión Mensual": comision_m_edit,
    "Saldo (Ahorro)": saldo_edit,
    "PAGO BANCO": pago_banco,
    "DESCUENTO (%)": None if descuento is None else round(descuento, 2),
    "N PaB": n_pab,
    "CE promedio (base)": ce_ref,
    "Comisión de éxito": comision_exito,
    "CE inicial (opcional)": ce_inicial
})

st.info("✅ Hasta aquí está listo. En el siguiente paso agregamos la **tabla de pagos** y los cálculos de % primer pago, PRI-ULT y C/A con este nuevo flujo.")
