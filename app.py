import streamlit as st
import pandas as pd
import numpy as np
from datetime import date

# ------------------ Ajustes de rendimiento globales ------------------
pd.set_option("mode.copy_on_write", True)

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

# ------------------ Funciones cacheadas (perf) ------------------
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
    # evita correr _find_col en cada rerun
    dummy_df = pd.DataFrame(columns=list(columns_list))
    col_ref   = _find_col(dummy_df, ["Referencia"])
    col_id    = _find_col(dummy_df, ["Id deuda","id deuda","id_deuda"])
    col_banco = _find_col(dummy_df, ["Banco"])
    col_deu   = _find_col(dummy_df, ["Deuda Resuelve","deuda resuelve"])
    col_apar  = _find_col(dummy_df, ["Apartado Mensual","apartado mensual"])
    col_com   = _find_col(dummy_df, ["Comisión Mensual","comision mensual","comisión mensual"])
    col_saldo = _find_col(dummy_df, ["Saldo","Ahorro"])
    col_ce    = _find_col(dummy_df, ["CE"])
    return col_ref, col_id, col_banco, col_deu, col_apar, col_com, col_saldo, col_ce

# ---------- 1) cargar base ----------
st.markdown("### 1) Cargar base (CSV o Excel)")
up = st.file_uploader("📂 Sube `cartera_asignada_filtrada`", type=["csv", "xlsx"])
if not up:
    st.info("Sube un archivo para continuar.")
    st.stop()

try:
    df_base = _read_file(up)  # <-- cacheado
except Exception as e:
    st.error(f"No pude leer el archivo: {e}")
    st.stop()

# mapear columnas (cacheado) — pasa tuple hashable para evitar UnhashableParamError
colnames_tuple = tuple(map(str, df_base.columns))
col_ref, col_id, col_banco, col_deu, col_apar, col_com, col_saldo, col_ce = _map_columns(colnames_tuple)

needed = {"Referencia": col_ref, "Id deuda": col_id, "Banco": col_banco,
          "Deuda Resuelve": col_deu, "Apartado Mensual": col_apar,
          "Comisión Mensual": col_com, "Saldo/Ahorro": col_saldo, "CE": col_ce}
faltan = [k for k,v in needed.items() if v is None]
if faltan:
    st.error("Faltan columnas requeridas: " + ", ".join(faltan))
    st.stop()

# normalizar numéricos (cacheado)
df_base = _normalize_numeric(df_base, [col_deu, col_apar, col_com, col_saldo, col_ce])

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

# Mostrar SOLO Id deuda y Banco (ocultando otras columnas) — limitar filas pesadas
st.subheader("Resultados (elige Id deuda)")
df_preview = df_ref[[col_id, col_banco]].head(500)  # <- evita renderizar miles de filas
st.dataframe(df_preview.reset_index(drop=True), use_container_width=True)

ids_opciones = df_ref[col_id].astype(str).tolist()
ids_sel = st.multiselect("Seleccione **uno o varios** Id deuda", ids_opciones, default=ids_opciones[:1])
if not ids_sel:
    st.info("Selecciona al menos un Id deuda para continuar.")
    st.stop()

sel = df_ref[df_ref[col_id].astype(str).isin(ids_sel)].copy()

# ---------- 3) Valores base (reactivo, sin form ni botón) ----------
st.markdown("### 3) Valores base (puedes editarlos)")

# 3.1 Defaults desde la selección actual
fila_primera = sel.iloc[0]
deuda_res_total_def = float(sel[col_deu].sum(skipna=True))
apartado_base_def   = float(fila_primera[col_apar]) if pd.notna(fila_primera[col_apar]) else 0.0
comision_m_base_def = float(fila_primera[col_com]) if pd.notna(fila_primera[col_com]) else 0.0
saldo_base_def      = float(fila_primera[col_saldo]) if pd.notna(fila_primera[col_saldo]) else 0.0
ce_base_def         = float(fila_primera[col_ce]) if pd.notna(fila_primera[col_ce]) else 0.0

# 3.2 Inicializar/actualizar estado cuando cambie la selección
sig_sel = (str(ref_input), tuple(sorted(map(str, ids_sel))))
if st.session_state.get("sig_sel") != sig_sel:
    st.session_state.sig_sel = sig_sel
    st.session_state.deuda_res_edit = deuda_res_total_def
    st.session_state.comision_m_edit = comision_m_base_def
    st.session_state.apartado_edit   = apartado_base_def
    st.session_state.saldo_edit      = saldo_base_def
    st.session_state.ce_base         = ce_base_def
    # También reiniciamos campos derivados/editables
    st.session_state.pago_banco      = 0.0
    st.session_state.n_pab           = 1
    st.session_state.comision_exito  = max(0.0, (deuda_res_total_def - 0.0) * 1.19 * ce_base_def)
    st.session_state.ce_inicial_txt  = ""

# 3.3 Inputs principales (reactivos)
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.session_state.deuda_res_edit = st.number_input(
        "💰 Deuda Resuelve", min_value=0.0, step=1000.0,
        value=float(st.session_state.deuda_res_edit), format="%.0f", key="deuda_res_edit_input"
    )
    st.session_state.deuda_res_edit = st.session_state.deuda_res_edit_input

with col2:
    st.session_state.comision_m_edit = st.number_input(
        "🎯 Comisión Mensual", min_value=0.0, step=1000.0,
        value=float(st.session_state.comision_m_edit), format="%.0f", key="comision_m_edit_input"
    )
    st.session_state.comision_m_edit = st.session_state.comision_m_edit_input

with col3:
    st.session_state.apartado_edit = st.number_input(
        "📆 Apartado Mensual", min_value=0.0, step=1000.0,
        value=float(st.session_state.apartado_edit), format="%.0f", key="apartado_edit_input"
    )
    st.session_state.apartado_edit = st.session_state.apartado_edit_input

with col4:
    st.session_state.saldo_edit = st.number_input(
        "💼 Saldo (Ahorro)", min_value=0.0, step=1000.0,
        value=float(st.session_state.saldo_edit), format="%.0f", key="saldo_edit_input"
    )
    st.session_state.saldo_edit = st.session_state.saldo_edit_input

# 3.4 Saldo Neto y Depósito
saldo_neto = 0.0
if pd.notna(st.session_state.saldo_edit) and st.session_state.saldo_edit > 0:
    saldo_neto = float(st.session_state.saldo_edit) - (float(st.session_state.saldo_edit) - 25000.0) * 0.004
    saldo_neto = max(0.0, saldo_neto)
saldo_neto_disp = float(np.round(saldo_neto, 0))

col5, col6 = st.columns(2)
with col5:
    st.number_input(
        "🧾 Saldo Neto", value=saldo_neto_disp, step=1000.0, min_value=0.0,
        format="%.0f", disabled=True,
        help="Saldo − (Saldo − 25.000) × 0.004 (solo si Saldo > 0)"
    )
with col6:
    deposito_edit = st.number_input(
        "💵 Depósito", min_value=0.0, step=1000.0,
        value=0.0, format="%.0f", help="Monto extra aportado al inicio; por defecto 0"
    )

# ---------- 4) PAGO BANCO y parámetros derivados (reactivo) ----------
st.markdown("### 4) PAGO BANCO y parámetros derivados")

c1, c2, c3 = st.columns([1,1,1])
with c1:
    st.session_state.pago_banco = st.number_input(
        "🏦 PAGO BANCO", min_value=0.0, step=1000.0,
        value=float(st.session_state.pago_banco), format="%.0f", key="pago_banco_input"
    )
    st.session_state.pago_banco = st.session_state.pago_banco_input

with c2:
    descuento = None
    if st.session_state.deuda_res_edit and st.session_state.deuda_res_edit > 0:
        descuento = max(0.0, 1.0 - (st.session_state.pago_banco / st.session_state.deuda_res_edit)) * 100.0
    st.text_input("📉 DESCUENTO (%)", value=(f"{descuento:.2f} %" if descuento is not None else ""), disabled=True)

with c3:
    st.session_state.n_pab = st.number_input(
        "🧮 N PaB", min_value=1, step=1, value=int(st.session_state.n_pab), key="n_pab_input"
    )
    st.session_state.n_pab = st.session_state.n_pab_input

# Comisión de éxito editable (prefill dinámico si cambia deuda o pago)
com_exito_prefill = max(0.0, (st.session_state.deuda_res_edit - st.session_state.pago_banco) * 1.19 * st.session_state.ce_base)
c4, c5 = st.columns(2)
with c4:
    # Si el usuario no ha tocado manualmente, usamos el prefill
    if "comision_exito_overridden" not in st.session_state:
        st.session_state.comision_exito = com_exito_prefill
    com_input = st.number_input(
        "🏁 Comisión de éxito (editable)", min_value=0.0, step=1000.0,
        value=float(st.session_state.comision_exito), format="%.0f", key="comision_exito_input"
    )
    # Si difiere del prefill, marcamos que fue editado por el usuario
    st.session_state.comision_exito_overridden = (abs(com_input - com_exito_prefill) > 1e-6)
    st.session_state.comision_exito = com_input

with c5:
    st.session_state.ce_inicial_txt = st.text_input("🧪 CE inicial", value=st.session_state.ce_inicial_txt, placeholder="Ej. 150000")
    try:
        ce_inicial = float(st.session_state.ce_inicial_txt.replace(",", ".")) if st.session_state.ce_inicial_txt.strip() != "" else None
    except Exception:
        ce_inicial = None
        st.warning("CE inicial inválido; déjalo vacío o usa un número como 0.12")

# Barra de avance
st.markdown("#### Avance de CE inicial sobre la Comisión de éxito")
if (ce_inicial is None) or (ce_inicial <= 0):
    st.info("Escribe un valor en **CE inicial** para ver el porcentaje.")
else:
    base = float(st.session_state.comision_exito) if st.session_state.comision_exito and st.session_state.comision_exito > 0 else 0.0
    if base <= 0:
        st.warning("La **Comisión de éxito** debe ser mayor a 0 para calcular el porcentaje.")
    else:
        porcentaje = (float(ce_inicial) / base) * 100.0
        porcentaje_capped = max(0.0, min(porcentaje, 100.0))
        st.progress(int(round(porcentaje_capped)))
        st.caption(
            f"CE inicial: {ce_inicial:,.0f}  |  Comisión de éxito: {base:,.0f}  →  "
            f"**{porcentaje:,.2f}%** de la Comisión de éxito"
        )

# ---------- 5) Cronograma de pagos (tabla editable, sin formato de miles) ----------
st.markdown("### 5) Cronograma de pagos (tabla editable)")

# 1) Inicialización en sesión: 5 filas (N=0..4)
if "tabla_pagos" not in st.session_state:
    n_init = list(range(0, 5))  # 0..4
    fechas_init = [date.today()] + [pd.NaT] * (len(n_init) - 1)
    st.session_state.tabla_pagos = pd.DataFrame({
        "N": n_init,
        "FECHA": fechas_init,
        # usa 0.0 inicial para asegurar dtype float estable (evita perder ediciones)
        "Pago(s) a banco": [0.0] * len(n_init),
        "Pagos de CE": [0.0] * len(n_init),
    })

# 2) Tipos estables antes de editar
df_src = st.session_state.tabla_pagos.copy()
df_src["N"] = pd.to_numeric(df_src["N"], errors="coerce").fillna(0).astype(int)
df_src["Pago(s) a banco"] = pd.to_numeric(df_src["Pago(s) a banco"], errors="coerce")
df_src["Pagos de CE"] = pd.to_numeric(df_src["Pagos de CE"], errors="coerce")
df_src["FECHA"] = pd.to_datetime(df_src["FECHA"], errors="coerce")

# 3) Editor en vivo (sin botón de guardar, sin formato especial)
edited_df = st.data_editor(
    df_src,
    use_container_width=True,
    num_rows="dynamic",  # puedes agregar/eliminar filas
    column_config={
        "N": st.column_config.NumberColumn(
            "N", min_value=0, max_value=100, step=1,
            help="Consecutivo automático desde 0.",
            disabled=True
        ),
        "FECHA": st.column_config.DateColumn(
            "FECHA", format="YYYY-MM-DD",
            help="La primera fila queda en hoy si está vacía; las demás, elígelo."
        ),
        "Pago(s) a banco": st.column_config.NumberColumn(
            "Pago(s) a banco",
            step=1,  # sin miles, se escribe tal cual
            help="Escribe el valor en pesos (sin signo ni separador)."
        ),
        "Pagos de CE": st.column_config.NumberColumn(
            "Pagos de CE",
            step=1,  # sin miles, se escribe tal cual
            help="Escribe el valor en pesos (sin signo ni separador)."
        ),
    },
    key="editor_tabla_pagos",
)

# 4) Post-procesado en cada rerun para persistir lo editado y mantener consistencia
df_final = edited_df.copy()

# Completar primera fecha si quedó vacía
if len(df_final) > 0 and (pd.isna(df_final.loc[0, "FECHA"]) or str(df_final.loc[0, "FECHA"]).strip() == ""):
    df_final.loc[0, "FECHA"] = date.today()

# Recalcular N como 0..(n-1) para mantener el orden tras agregar/eliminar filas
df_final = df_final.reset_index(drop=True)
df_final["N"] = range(len(df_final))

# Asegurar tipo numérico (permitimos NaN si dejas celdas vacías)
for col_money in ["Pago(s) a banco", "Pagos de CE"]:
    df_final[col_money] = pd.to_numeric(df_final[col_money], errors="coerce")

# Guardar inmediatamente en sesión
st.session_state.tabla_pagos = df_final

st.caption("Escribe números tal cual (sin $ ni puntos). Puedes agregar filas; N se reenumera solo.")
