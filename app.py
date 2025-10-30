import streamlit as st
import pandas as pd
import numpy as np
from datetime import date

st.set_page_config(page_title="Calculadora de Recaudo", page_icon="💸", layout="centered")
st.title("💸 Calculadora de Recaudo")

st.caption(
    "1) Carga tu base `cartera_asignada_filtrada` • "
    "2) Escribe la **Referencia** y selecciona **uno o varios Id deuda** • "
    "3) Ajusta valores editables (Deuda, Apartado, Comisión, Saldo) • "
    "4) Ingresa **PAGO BANCO** y **N PaB** → se calcula **DESCUENTO** y la **Comisión de éxito**."
)

# ---------- utilidades ----------
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
    if isinstance(x, str):
        x = x.replace(",", "")
    return pd.to_numeric(x, errors="coerce")

# ---------- 1) cargar base ----------
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

# mapear columnas
col_ref   = _find_col(df_base, ["Referencia"])
col_id    = _find_col(df_base, ["Id deuda","id deuda","id_deuda"])
col_banco = _find_col(df_base, ["Banco"])
col_deu   = _find_col(df_base, ["Deuda Resuelve","deuda resuelve"])
col_apar  = _find_col(df_base, ["Apartado Mensual","apartado mensual"])
col_com   = _find_col(df_base, ["Comisión Mensual","comision mensual","comisión mensual"])
col_saldo = _find_col(df_base, ["Saldo","Ahorro"])
col_ce    = _find_col(df_base, ["CE"])

needed = {"Referencia": col_ref, "Id deuda": col_id, "Banco": col_banco,
          "Deuda Resuelve": col_deu, "Apartado Mensual": col_apar,
          "Comisión Mensual": col_com, "Saldo/Ahorro": col_saldo, "CE": col_ce}
faltan = [k for k,v in needed.items() if v is None]
if faltan:
    st.error("Faltan columnas requeridas: " + ", ".join(faltan))
    st.stop()

# normalizar numéricos
for c in [col_deu, col_apar, col_com, col_saldo, col_ce]:
    df_base[c] = df_base[c].apply(_to_num)

st.success("✅ Base cargada")

# ---------- 2) referencia → seleccionar id(s) ----------
st.markdown("### 2) Referencia → seleccionar **Id deuda** (uno o varios)")
ref_input = st.text_input("🔎 Escribe la **Referencia** (exacta como aparece en la base)")
if not ref_input:
    st.stop()

df_ref = df_base[df_base[col_ref].astype(str) == str(ref_input)]
if df_ref.empty:
    st.warning("No encontramos esa referencia en la base.")
    st.stop()

# Mostrar SOLO Id deuda y Banco (ocultando otras columnas)
st.subheader("Resultados (elige Id deuda)")
st.dataframe(df_ref[[col_id, col_banco]].reset_index(drop=True), use_container_width=True)

ids_opciones = df_ref[col_id].astype(str).tolist()
ids_sel = st.multiselect("Seleccione **uno o varios** Id deuda", ids_opciones, default=ids_opciones[:1])
if not ids_sel:
    st.info("Selecciona al menos un Id deuda para continuar.")
    st.stop()

sel = df_ref[df_ref[col_id].astype(str).isin(ids_sel)].copy()

# ---------- 3) cajas editables ----------
st.markdown("### 3) Valores base (puedes editarlos)")

# Reglas que pediste:
# - Deuda Resuelve -> SUMA de las seleccionadas
# - Apartado Mensual, Comisión Mensual, Saldo -> del PRIMER registro (no sumar)
# - CE para Comisión de éxito -> del PRIMER registro
fila_primera = sel.iloc[0]

deuda_res_total   = float(sel[col_deu].sum(skipna=True))
apartado_base     = float(_to_num(fila_primera[col_apar])) if pd.notna(fila_primera[col_apar]) else 0.0
comision_m_base   = float(_to_num(fila_primera[col_com])) if pd.notna(fila_primera[col_com]) else 0.0
saldo_base        = float(_to_num(fila_primera[col_saldo])) if pd.notna(fila_primera[col_saldo]) else 0.0
ce_base           = float(_to_num(fila_primera[col_ce])) if pd.notna(fila_primera[col_ce]) else 0.0

colA, colB = st.columns(2)
with colA:
    deuda_res_edit = st.number_input("💰 Deuda Resuelve (suma de seleccionadas)", min_value=0.0, step=1000.0, value=deuda_res_total, format="%.0f")
    apartado_edit  = st.number_input("📆 Apartado Mensual (del 1er registro)", min_value=0.0, step=1000.0, value=apartado_base, format="%.0f")
with colB:
    comision_m_edit = st.number_input("🎯 Comisión Mensual (del 1er registro)", min_value=0.0, step=1000.0, value=comision_m_base, format="%.0f")
    saldo_edit      = st.number_input("💼 Saldo (Ahorro) (del 1er registro)", min_value=0.0, step=1000.0, value=saldo_base, format="%.0f")

# ---------- 4) Pago banco, descuento, N PaB, comisión éxito, CE inicial ----------
st.markdown("### 4) PAGO BANCO y parámetros derivados")

c1, c2, c3 = st.columns([1,1,1])
with c1:
    pago_banco = st.number_input("🏦 PAGO BANCO", min_value=0.0, step=1000.0, value=0.0, format="%.0f")
with c2:
    descuento = None
    if deuda_res_edit and deuda_res_edit > 0:
        descuento = max(0.0, 1.0 - (pago_banco / deuda_res_edit)) * 100.0
    st.text_input("📉 DESCUENTO (%)", value=(f"{descuento:.2f} %" if descuento is not None else ""), disabled=True)
with c3:
    n_pab = st.number_input("🧮 N PaB", min_value=1, step=1, value=1)

com_exito_default = max(0.0, (deuda_res_edit - pago_banco) * 1.19 * ce_base)

c4, c5 = st.columns(2)
with c4:
    comision_exito = st.number_input("🏁 Comisión de éxito (editable)", min_value=0.0, step=1000.0,
                                     value=float(com_exito_default), format="%.0f",
                                     help=f"Prefill: (Deuda Resuelve − PAGO BANCO) × 1.19 × CE (CE base del 1er registro = {ce_base:.4f})")
with c5:
    ce_inicial_txt = st.text_input("🧪 CE inicial (opcional)", value="", placeholder="Ej. 0.12")
    try:
        ce_inicial = float(ce_inicial_txt.replace(",", ".")) if ce_inicial_txt.strip() != "" else None
    except Exception:
        ce_inicial = None
        st.warning("CE inicial inválido; déjalo vacío o usa un número como 0.12")

# Resumen
st.markdown("#### Resumen actual")
st.write({
    "Ids seleccionados": ids_sel,
    "Deuda Resuelve": deuda_res_edit,
    "Apartado Mensual (1er registro)": apartado_edit,
    "Comisión Mensual (1er registro)": comision_m_edit,
    "Saldo (Ahorro) (1er registro)": saldo_edit,
    "PAGO BANCO": pago_banco,
    "DESCUENTO (%)": None if descuento is None else round(descuento, 2),
    "N PaB": n_pab,
    "CE (1er registro)": ce_base,
    "Comisión de éxito": comision_exito,
    "CE inicial (opcional)": ce_inicial
})

st.info("✅ Listo este bloque. Ahora podemos añadir la **tabla de pagos** y, con estos valores, calcular `% primer pago`, `PRI-ULT` y `C/A`.")
