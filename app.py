import streamlit as st
import pandas as pd
import numpy as np
from datetime import date, timedelta
from joblib import load

st.set_page_config(page_title="Calculadora de Recaudo", page_icon="💸", layout="centered")
st.title("💸 Calculadora de Recaudo")

st.caption(
    "1) Carga tu base `cartera_asignada_filtrada` • "
    "2) Busca la **Referencia** y elige el **Id deuda** • "
    "3) Escribe **COMISIÓN TOTAL** y arma la **tabla de pagos** • "
    "4) La app calcula **% primer pago (Ratio_PP)**, **PRI-ULT** y **C/A** • "
    "5) (Opcional) Carga el modelo `.pkl` para predecir `recaudo_real`."
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
    # convierte strings con comas/puntos a float
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

# Leer archivo
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
col_referencia = _find_col(df_base, ["Referencia"])
col_id_deuda   = _find_col(df_base, ["Id deuda","id_deuda","id deuda"])
col_banco      = _find_col(df_base, ["Banco"])
col_deuda_res  = _find_col(df_base, ["Deuda Resuelve","deuda resuelve"])
col_apartado   = _find_col(df_base, ["Apartado Mensual","apartado mensual"])
col_comision_m = _find_col(df_base, ["Comisión Mensual","comision mensual"])

needed = {
    "Referencia": col_referencia, "Id deuda": col_id_deuda, "Banco": col_banco,
    "Deuda Resuelve": col_deuda_res, "Apartado Mensual": col_apartado, "Comisión Mensual": col_comision_m
}
faltantes = [k for k,v in needed.items() if v is None]
if faltantes:
    st.error(f"En tu archivo faltan columnas requeridas: {', '.join(faltantes)}.")
    st.stop()

# Sanitizar numéricos de la base
for c in [col_deuda_res, col_apartado, col_comision_m]:
    df_base[c] = df_base[c].apply(_to_num)

# -------------------------------
# 2) Buscar referencia y elegir Id deuda
# -------------------------------
st.markdown("### 2) Buscar referencia y elegir **Id deuda**")

ref_input = st.text_input("🔎 Escribe la **Referencia** (exacta como aparece en la base)")

if not ref_input:
    st.stop()

df_ref = df_base[df_base[col_referencia].astype(str) == str(ref_input)]
if df_ref.empty:
    st.warning("No encontramos esa referencia en la base.")
    st.stop()

st.subheader("Resultados de la referencia")
st.dataframe(df_ref[[col_referencia, col_id_deuda, col_banco, col_deuda_res, col_apartado, col_comision_m]], use_container_width=True)

ids = df_ref[col_id_deuda].astype(str).tolist()
id_sel = st.selectbox("Seleccione el **Id deuda** a analizar", ids)

fila = df_ref[df_ref[col_id_deuda].astype(str) == id_sel].iloc[0]
banco = fila[col_banco]
deuda_resuelve = _to_num(fila[col_deuda_res])
apartado_mensual = _to_num(fila[col_apartado])
comision_mensual = _to_num(fila[col_comision_m])

st.markdown("#### Detalle de la deuda seleccionada")
colA, colB = st.columns(2)
with colA:
    st.write(f"**🏦 Banco:** {banco}")
    st.write(f"**💰 Deuda Resuelve:** {deuda_resuelve:,.2f}" if pd.notna(deuda_resuelve) else "—")
with colB:
    st.write(f"**📆 Apartado Mensual:** {apartado_mensual:,.2f}" if pd.notna(apartado_mensual) else "—")
    st.write(f"**🎯 Comisión Mensual:** {comision_mensual:,.2f}" if pd.notna(comision_mensual) else "—")

# -------------------------------
# 3) COMISIÓN TOTAL (AMOUNT_TOTAL)
# -------------------------------
st.markdown("### 3) Ingrese **COMISIÓN TOTAL** (AMOUNT_TOTAL)")
comision_total = st.number_input("COMISIÓN TOTAL", min_value=0.0, step=1000.0, value=0.0, format="%.0f")

# -------------------------------
# 4) Tabla de pagos editable (Excel-like)
# -------------------------------
st.markdown("### 4) Armar tabla de pagos")
st.caption("Agrega o borra filas. **N** es el orden (1,2,3,…). **FECHA DE PAGO** en formato fecha. **PAGO A BANCO** y **PAGO COMISION** en valores totales.")

# Plantilla inicial
plantilla = pd.DataFrame({
    "N": [1, 2, 3],
    "FECHA DE PAGO": [date.today(), None, None],
    "PAGO A BANCO": [apartado_mensual or 0.0, 0.0, 0.0],
    "PAGO COMISION": [comision_mensual or 0.0, 0.0, 0.0],
})

tabla = st.data_editor(
    plantilla,
    num_rows="dynamic",
    use_container_width=True,
    column_config={
        "N": st.column_config.NumberColumn(format="%d", step=1),
        "FECHA DE PAGO": st.column_config.DateColumn(format="YYYY-MM-DD"),
        "PAGO A BANCO": st.column_config.NumberColumn(format="%.0f", step=1000),
        "PAGO COMISION": st.column_config.NumberColumn(format="%.0f", step=1000),
    },
    key="tabla_pagos"
)

# Limpieza
df_pagos = tabla.copy()
df_pagos["N"] = pd.to_numeric(df_pagos["N"], errors="coerce").fillna(0).astype(int)
df_pagos = df_pagos[df_pagos["N"] > 0].sort_values("N")
df_pagos["FECHA DE PAGO"] = pd.to_datetime(df_pagos["FECHA DE PAGO"], errors="coerce")
for c in ["PAGO A BANCO","PAGO COMISION"]:
    df_pagos[c] = df_pagos[c].apply(_to_num).fillna(0.0)

st.markdown("**Tabla limpia:**")
st.dataframe(df_pagos, use_container_width=True)

# -------------------------------
# 5) Cálculos: % primer pago (Ratio_PP), PRI-ULT, C/A
# -------------------------------
st.markdown("### 5) Cálculos automáticos")

# % primer pago (Ratio_PP)
if comision_total and comision_total > 0 and (df_pagos["N"] == 1).any():
    pago_com_n1 = float(df_pagos.loc[df_pagos["N"] == 1, "PAGO COMISION"].sum())
    ratio_pp = float(np.clip(pago_com_n1 / comision_total, 0, 1))
else:
    ratio_pp = np.nan

# PRI-ULT (meses entre primera y última FECHA DE PAGO, mínimo 1)
if df_pagos["FECHA DE PAGO"].notna().sum() >= 2:
    fmin = df_pagos["FECHA DE PAGO"].min()
    fmax = df_pagos["FECHA DE PAGO"].max()
    meses = (fmax - fmin).days / 30.4375
    pri_ult = float(max(1.0, meses))
else:
    pri_ult = np.nan

# C/A = ((COMISIÓN TOTAL − Σ PAGO A BANCO − Σ PAGO COMISION + Comisión Mensual) / (max(N) − 1)) / Apartado Mensual
if not df_pagos.empty:
    sum_banco = float(df_pagos["PAGO A BANCO"].sum())
    sum_comis = float(df_pagos["PAGO COMISION"].sum())
    nmax = int(df_pagos["N"].max())
    den_n = max(1, nmax - 1)  # evita división por 0
    numerador = (comision_total or 0.0) - sum_banco - sum_comis + (comision_mensual or 0.0)
    c_a = np.nan if (apartado_mensual is None or apartado_mensual <= 0) else float((numerador / den_n) / apartado_mensual)
else:
    c_a = np.nan

res = pd.DataFrame({
    "Variable": ["% primer pago (Ratio_PP)", "PRI-ULT (meses)", "C/A"],
    "Valor": [ratio_pp, pri_ult, c_a]
})
st.dataframe(res, use_container_width=True)

# -------------------------------
# 6) (Opcional) Predicción con modelo .pkl
# -------------------------------
st.markdown("### 6) (Opcional) Cargar modelo `.pkl` para predecir `recaudo_real`")
mdl_file = st.file_uploader("Sube el pipeline entrenado (`.pkl` de joblib`)", type=["pkl"], key="mdl")

pipe = None
if mdl_file is not None:
    try:
        pipe = load(mdl_file)
        st.success("✅ Modelo cargado.")
    except Exception as e:
        st.error(f"No pude cargar el modelo: {e}")

if pipe is not None and st.button("Predecir `recaudo_real`"):
    # Si tu pipeline es el MLP robusto que armamos, espera columnas:
    # ["PRI-ULT","Ratio_PP","C/A_log","AMOUNT_TOTAL_log"]
    X_pred = pd.DataFrame([{
        "PRI-ULT": pri_ult,
        "Ratio_PP": ratio_pp,
        "C/A_log": np.log1p(c_a) if pd.notna(c_a) else np.nan,
        "AMOUNT_TOTAL_log": np.log1p(comision_total) if comision_total and comision_total > 0 else np.nan
    }])
    try:
        yhat = float(np.clip(pipe.predict(X_pred)[0], 0, 1))
        st.metric("🔮 `recaudo_real` (estimado)", f"{yhat:.3f}")
    except Exception as e:
        st.error(f"Error al predecir: {e}\nVerifica que tu pipeline espere estas 4 columnas transformadas.")
