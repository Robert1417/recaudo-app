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
    deuda_res_edit = st.number_input("💰 Deuda Resuelve", min_value=0.0, step=1000.0, value=deuda_res_total, format="%.0f")
    apartado_edit  = st.number_input("📆 Apartado Mensual", min_value=0.0, step=1000.0, value=apartado_base, format="%.0f")
with colB:
    comision_m_edit = st.number_input("🎯 Comisión Mensual", min_value=0.0, step=1000.0, value=comision_m_base, format="%.0f")
    saldo_edit      = st.number_input("💼 Saldo (Ahorro)", min_value=0.0, step=1000.0, value=saldo_base, format="%.0f")

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

# =========================
# 🧱 SECCIÓN: Tabla de pagos (PAGO BANCO)
# - Crea un DataFrame editable con columnas: N, FECHA, PAGO BANCO, PAGO COMISION
# - Reparte PAGO BANCO en N PaB
# - Si el usuario cambia el PAGO BANCO de la fila 1, las demás filas se reequilibran
# =========================

import pandas as pd

st.markdown("---")
st.header("📅 Tabla de pagos — PAGO BANCO")

# Utilidad: último día del mes para un timestamp dado
def end_of_month(ts: pd.Timestamp) -> pd.Timestamp:
    return (ts + pd.offsets.MonthEnd(0)).normalize()

# Construir fechas por defecto:
#  - Fila 1: hoy
#  - Filas 2..N: último día de los meses siguientes
today_ts = pd.Timestamp.today().normalize()

def default_fechas(n: int) -> list:
    if n <= 0:
        return []
    fechas = [today_ts.date()]
    for k in range(1, n):
        ts_k = (today_ts + pd.DateOffset(months=k))
        fechas.append(end_of_month(ts_k).date())
    return fechas

# Inicializar/Resetear tabla cuando cambian PAGO BANCO o N PaB
if "pab_table" not in st.session_state:
    st.session_state["pab_table"] = None
if "pab_last_total" not in st.session_state:
    st.session_state["pab_last_total"] = None
if "pab_last_n" not in st.session_state:
    st.session_state["pab_last_n"] = None

total_pab = float(pago_banco or 0.0)
n_rows = int(n_pab or 1)

def build_table(total: float, n: int) -> pd.DataFrame:
    if n < 1:
        n = 1
    base = 0.0 if n == 0 else (total / n)
    df0 = pd.DataFrame({
        "N": list(range(1, n + 1)),
        "FECHA": default_fechas(n),
        "PAGO BANCO": [base for _ in range(n)],
        "PAGO COMISION": [0.0 for _ in range(n)]  # la completaremos en otro paso
    })
    # Redondeo estético
    df0["PAGO BANCO"] = df0["PAGO BANCO"].round(0)
    df0["PAGO COMISION"] = df0["PAGO COMISION"].round(0)
    return df0

# Reset cuando cambian los parámetros
if (st.session_state["pab_table"] is None or
    st.session_state["pab_last_total"] != total_pab or
    st.session_state["pab_last_n"] != n_rows):
    st.session_state["pab_table"] = build_table(total_pab, n_rows)
    st.session_state["pab_last_total"] = total_pab
    st.session_state["pab_last_n"] = n_rows

# Editor
st.caption("✅ Puedes editar **FECHA** y el **PAGO BANCO de la primera fila**; el resto se reequilibrará automáticamente.")
edited = st.data_editor(
    st.session_state["pab_table"],
    num_rows="fixed",  # número de filas fijado por N PaB
    use_container_width=True,
    column_config={
        "N": st.column_config.NumberColumn(format="%d", step=1, disabled=True),
        "FECHA": st.column_config.DateColumn(format="YYYY-MM-DD"),
        # Permitimos editar PAGO BANCO libremente, pero reequilibramos según regla de negocio
        "PAGO BANCO": st.column_config.NumberColumn(format="%.0f", step=1000),
        "PAGO COMISION": st.column_config.NumberColumn(format="%.0f", step=1000),
    },
    key="editor_pab"
).copy()

# --- Reglas de reequilibrio:
# Si el usuario cambia el PAGO BANCO de la fila 1, las demás filas se ajustan iguales
# para mantener la suma total = total_pab. (No tocamos PAGO COMISION aún)
if not edited.empty:
    # Asegurar tipos
    edited["PAGO BANCO"] = pd.to_numeric(edited["PAGO BANCO"], errors="coerce").fillna(0.0)
    edited["PAGO COMISION"] = pd.to_numeric(edited["PAGO COMISION"], errors="coerce").fillna(0.0)

    # Total deseado
    T = total_pab

    # Valor de la fila 1 (editable por el usuario)
    v1 = float(edited.loc[edited["N"] == 1, "PAGO BANCO"].iloc[0] if 1 in edited["N"].values else 0.0)

    if n_rows == 1:
        # Caso trivial: una única fila debe llevar todo el total
        edited.loc[edited["N"] == 1, "PAGO BANCO"] = T
    else:
        # Reparto del remanente en filas 2..N
        rem = max(0.0, T - v1)
        per = rem / (n_rows - 1)

        # Aplicar a filas 2..N
        mask_rest = edited["N"] >= 2
        edited.loc[mask_rest, "PAGO BANCO"] = per

        # Corrección por redondeo para que la suma cierre exactamente en T
        suma = float(edited["PAGO BANCO"].sum())
        diff = T - suma
        if abs(diff) >= 0.5:  # si hay diferencia relevante, ajústala en la última fila
            edited.loc[edited["N"] == n_rows, "PAGO BANCO"] += diff

    # Guardar de nuevo en sesión
    st.session_state["pab_table"] = edited

# Mostrar tabla resultante
st.markdown("**Tabla resultante (balanceada):**")
st.dataframe(st.session_state["pab_table"], use_container_width=True)

# Resumen de control
sum_pab = float(st.session_state["pab_table"]["PAGO BANCO"].sum()) if not st.session_state["pab_table"].empty else 0.0
st.caption(f"🔎 Control: Total PAGO BANCO objetivo = {total_pab:,.0f} | Total en tabla = {sum_pab:,.0f}")

# Nota: Si cambias PAGO BANCO o N PaB arriba, la tabla se reinicia con el nuevo reparto.
