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
    cols = {_norm(c): c for c in df.columns}
    for cand in candidates:
        if _norm(cand) in cols:
            return cols[_norm(cand)]
    return None

def _distribuir_en_n_pagos(total: float, n: int) -> list[float]:
    """Reparte 'total' en 'n' cuotas enteras; la última ajusta para que la suma sea exacta."""
    n = max(1, int(n))
    total_int = int(round(float(total or 0.0)))
    base = total_int // n
    resto = total_int - base * n
    cuotas = [float(base)] * n
    cuotas[-1] = float(base + resto)
    return cuotas

def _ensure_table_exists():
    """Crea tabla_pagos mínima si no existe."""
    if "tabla_pagos" not in st.session_state or not isinstance(st.session_state.tabla_pagos, pd.DataFrame):
        st.session_state.tabla_pagos = pd.DataFrame({
            "N": [0, 1, 2, 3, 4],
            "FECHA": [date.today(), pd.NaT, pd.NaT, pd.NaT, pd.NaT],
            "Pago(s) a banco": [0.0]*5,
            "Pagos de CE": [0.0]*5,
        })

def _sync_ce_inicial_to_table():
    """Refleja CE inicial en la primera fila de 'Pagos de CE' inmediatamente."""
    _ensure_table_exists()
    df = st.session_state.tabla_pagos.copy(deep=True)
    ce = float(st.session_state.get("ce_inicial_val", 0.0) or 0.0)

    if len(df) == 0:
        df = pd.DataFrame({
            "N": [0],
            "FECHA": [date.today()],
            "Pago(s) a banco": [0.0],
            "Pagos de CE": [0.0],
        })

    # Asegurar FECHA 0
    if pd.isna(df.loc[0, "FECHA"]) or str(df.loc[0, "FECHA"]).strip() == "":
        df.loc[0, "FECHA"] = date.today()

    # Sincronizar primera celda de CE
    df.loc[0, "Pagos de CE"] = float(ce) if ce > 0 else 0.0

    # Recalcular N por si acaso
    df = df.reset_index(drop=True)
    df["N"] = range(len(df))
    st.session_state.tabla_pagos = df

def _repartir_pagos_banco():
    """Reparte PAGO BANCO según N PaB y actualiza tabla (sin tocar otras columnas)."""
    _ensure_table_exists()
    df = st.session_state.tabla_pagos.copy(deep=True)
    n = int(max(1, st.session_state.get("n_pab", 1)))
    total = float(st.session_state.get("pago_banco", 0.0) or 0.0)

    # Asegurar filas suficientes
    if len(df) < n:
        faltan = n - len(df)
        extra = pd.DataFrame({
            "N": list(range(len(df), len(df) + faltan)),
            "FECHA": [pd.NaT]*faltan,
            "Pago(s) a banco": [0.0]*faltan,
            "Pagos de CE": [0.0]*faltan,
        })
        df = pd.concat([df.reset_index(drop=True), extra], ignore_index=True)

    df = df.reset_index(drop=True)
    df["N"] = range(len(df))

    # Reparto: llenar 0..n-1, y desde n en adelante poner 0
    cuotas = _distribuir_en_n_pagos(total, n) if total > 0 else [0.0]*n
    df.loc[:, "Pago(s) a banco"] = 0.0
    for i in range(n):
        df.loc[i, "Pago(s) a banco"] = float(cuotas[i])

    st.session_state.tabla_pagos = df

# ------------------ cache lectura/normalización ------------------
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
    col_deu   = _find_col(dummy_df, ["Deuda Resuelve","deuda resuelve"])
    col_apar  = _find_col(dummy_df, ["Apartado Mensual","apartado mensual"])
    col_com   = _find_col(dummy_df, ["Comisión Mensual","comision mensual","comisión mensual"])
    col_saldo = _find_col(dummy_df, ["Saldo","Ahorro"])
    col_ce    = _find_col(dummy_df, ["CE"])
    return col_ref, col_id, col_banco, col_deu, col_apar, col_com, col_saldo, col_ce

# ------------------ 1) cargar base ------------------
st.markdown("### 1) Cargar base (CSV o Excel)")
up = st.file_uploader("📂 Sube `cartera_asignada_filtrada`", type=["csv", "xlsx"])
if not up:
    st.info("Sube un archivo para continuar.")
    st.stop()

try:
    df_base = _read_file(up)
except Exception as e:
    st.error(f"No pude leer el archivo: {e}")
    st.stop()

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
st.success("✅ Base cargada")

# ------------------ 2) referencia → seleccionar id(s) ------------------
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

# ------------------ 3) Valores base (reactivo, sin “doble tecleo”) ------------------
st.markdown("### 3) Valores base (puedes editarlos)")

fila_primera = sel.iloc[0]
deuda_res_total_def = float(sel[col_deu].sum(skipna=True))
apartado_base_def   = float(fila_primera[col_apar]) if pd.notna(fila_primera[col_apar]) else 0.0
comision_m_base_def = float(fila_primera[col_com]) if pd.notna(fila_primera[col_com]) else 0.0
saldo_base_def      = float(fila_primera[col_saldo]) if pd.notna(fila_primera[col_saldo]) else 0.0
ce_base_def         = float(fila_primera[col_ce]) if pd.notna(fila_primera[col_ce]) else 0.0

sig_sel = (str(ref_input), tuple(sorted(map(str, ids_sel))))
if st.session_state.get("sig_sel") != sig_sel:
    st.session_state.clear()  # limpiar estados previos
    st.session_state.sig_sel = sig_sel

    # Inits de inputs
    st.session_state.deuda_res_edit = deuda_res_total_def
    st.session_state.comision_m_edit = comision_m_base_def
    st.session_state.apartado_edit   = apartado_base_def
    st.session_state.saldo_edit      = saldo_base_def
    st.session_state.ce_base         = ce_base_def
    st.session_state.pago_banco      = 0.0
    st.session_state.n_pab           = 1

    # Comisión de éxito (no editada aún)
    st.session_state.comision_exito_overridden = False
    st.session_state.comision_exito  = max(0.0, (deuda_res_total_def - 0.0) * 1.19 * ce_base_def)

    # CE inicial numérico
    st.session_state.ce_inicial_val  = 0.0

    # Tabla base 0..4
    st.session_state.tabla_pagos = pd.DataFrame({
        "N": [0, 1, 2, 3, 4],
        "FECHA": [date.today(), pd.NaT, pd.NaT, pd.NaT, pd.NaT],
        "Pago(s) a banco": [0.0]*5,
        "Pagos de CE": [0.0]*5,
    })

    # memorias para detectar cambio y repartir
    st.session_state._last_pab = st.session_state.pago_banco
    st.session_state._last_n   = st.session_state.n_pab
    st.rerun()

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.number_input("💰 Deuda Resuelve", min_value=0.0, step=1000.0,
                    value=float(st.session_state.deuda_res_edit), format="%.0f", key="deuda_res_edit")
with col2:
    st.number_input("🎯 Comisión Mensual", min_value=0.0, step=1000.0,
                    value=float(st.session_state.comision_m_edit), format="%.0f", key="comision_m_edit")
with col3:
    st.number_input("📆 Apartado Mensual", min_value=0.0, step=1000.0,
                    value=float(st.session_state.apartado_edit), format="%.0f", key="apartado_edit")
with col4:
    st.number_input("💼 Saldo (Ahorro)", min_value=0.0, step=1000.0,
                    value=float(st.session_state.saldo_edit), format="%.0f", key="saldo_edit")

# 3.4 Saldo Neto y Depósito
saldo_neto = 0.0
if pd.notna(st.session_state.saldo_edit) and st.session_state.saldo_edit > 0:
    saldo_neto = float(st.session_state.saldo_edit) - (float(st.session_state.saldo_edit) - 25000.0) * 0.004
    saldo_neto = max(0.0, saldo_neto)
saldo_neto_disp = float(np.round(saldo_neto, 0))

col5, col6 = st.columns(2)
with col5:
    st.number_input("🧾 Saldo Neto", value=saldo_neto_disp, step=1000.0, min_value=0.0,
                    format="%.0f", disabled=True,
                    help="Saldo − (Saldo − 25.000) × 0.004 (solo si Saldo > 0)")
with col6:
    st.number_input("💵 Depósito", min_value=0.0, step=1000.0,
                    value=0.0, format="%.0f", key="deposito_edit",
                    help="Monto extra aportado al inicio; por defecto 0")

# ------------------ 4) PAGO BANCO y derivados ------------------
st.markdown("### 4) PAGO BANCO y parámetros derivados")

c1, c2, c3 = st.columns([1,1,1])
with c1:
    st.number_input("🏦 PAGO BANCO", min_value=0.0, step=1000.0,
                    value=float(st.session_state.pago_banco), format="%.0f", key="pago_banco")
with c2:
    descuento = None
    if st.session_state.deuda_res_edit and st.session_state.deuda_res_edit > 0:
        descuento = max(0.0, 1.0 - (st.session_state.pago_banco / st.session_state.deuda_res_edit)) * 100.0
    st.text_input("📉 DESCUENTO (%)", value=(f"{descuento:.2f} %" if descuento is not None else ""), disabled=True)
with c3:
    st.number_input("🧮 N PaB", min_value=1, step=1,
                    value=int(st.session_state.n_pab), key="n_pab")

# Si cambian PAGO BANCO o N PaB → repartir y rerun inmediato
if st.session_state._last_pab != st.session_state.pago_banco or st.session_state._last_n != st.session_state.n_pab:
    st.session_state._last_pab = st.session_state.pago_banco
    st.session_state._last_n   = st.session_state.n_pab

    _repartir_pagos_banco()

    # Prefill de Comisión de éxito si no está override
    com_exito_prefill = max(0.0, (st.session_state.deuda_res_edit - st.session_state.pago_banco) * 1.19 * st.session_state.ce_base)
    if not st.session_state.get("comision_exito_overridden", False):
        st.session_state.comision_exito = com_exito_prefill

    st.rerun()

# Comisión de éxito editable / override
c4, c5 = st.columns(2)
with c4:
    com_exito_prefill_now = max(0.0, (st.session_state.deuda_res_edit - st.session_state.pago_banco) * 1.19 * st.session_state.ce_base)
    if not st.session_state.get("comision_exito_overridden", False):
        st.session_state.comision_exito = com_exito_prefill_now
    val_prev = float(st.session_state.comision_exito)
    new_val = st.number_input("🏁 Comisión de éxito (editable)", min_value=0.0, step=1000.0,
                              value=val_prev, format="%.0f", key="comision_exito")
    st.session_state.comision_exito_overridden = (abs(new_val - com_exito_prefill_now) > 1e-6)

with c5:
    # CE inicial numérico y sincronización inmediata de la tabla al cambiar
    st.number_input(
        "🧪 CE inicial",
        min_value=0.0, step=1000.0,
        value=float(st.session_state.get("ce_inicial_val", 0.0)),
        format="%.0f",
        key="ce_inicial_val",
        on_change=_sync_ce_inicial_to_table,   # << sincroniza en el mismo ciclo
    )
    ce_inicial = float(st.session_state.ce_inicial_val or 0.0)

# Avance CE inicial vs Comisión de éxito
st.markdown("#### Avance de CE inicial sobre la Comisión de éxito")
if ce_inicial <= 0:
    st.info("Escribe un valor en **CE inicial** para ver el porcentaje.")
else:
    base = float(st.session_state.comision_exito) if st.session_state.comision_exito and st.session_state.comision_exito > 0 else 0.0
    if base <= 0:
        st.warning("La **Comisión de éxito** debe ser mayor a 0 para calcular el porcentaje.")
    else:
        porcentaje = (ce_inicial / base) * 100.0
        porcentaje_capped = max(0.0, min(porcentaje, 100.0))
        st.progress(int(round(porcentaje_capped)))
        st.caption(f"CE inicial: {ce_inicial:,.0f}  |  Comisión de éxito: {base:,.0f}  →  **{porcentaje:,.2f}%**")

# ------------------ 5) Cronograma de pagos (editable) ------------------
st.markdown("### 5) Cronograma de pagos (tabla editable)")

# Garantizar que la tabla ya refleje CE inicial vigente antes de pintar
_sync_ce_inicial_to_table()

df_view = st.session_state.tabla_pagos.copy(deep=True)
edited_df = st.data_editor(
    df_view,
    use_container_width=True,
    num_rows="dynamic",
    column_config={
        "N": st.column_config.NumberColumn(
            "N", min_value=0, max_value=100, step=1, disabled=True,
            help="Consecutivo automático desde 0."
        ),
        "FECHA": st.column_config.DateColumn(
            "FECHA", format="YYYY-MM-DD",
            help="La primera fila queda en hoy si está vacía."
        ),
        "Pago(s) a banco": st.column_config.NumberColumn(
            "Pago(s) a banco", step=1,
            help="Escribe el valor en pesos (sin signo ni separador)."
        ),
        "Pagos de CE": st.column_config.NumberColumn(
            "Pagos de CE", step=1,
            help="Escribe el valor en pesos (sin signo ni separador)."
        ),
    },
    key="editor_tabla_pagos",
)

# --- Persistir lo editado y aplicar mínimos (fecha/N) ---
df_final = edited_df.copy(deep=True)

if len(df_final) > 0:
    if pd.isna(df_final.loc[0, "FECHA"]) or str(df_final.loc[0, "FECHA"]).strip() == "":
        df_final.loc[0, "FECHA"] = date.today()

# Recalcular N si hiciera falta
n_expected = list(range(len(df_final)))
if "N" not in df_final.columns or df_final["N"].tolist() != n_expected:
    df_final.reset_index(drop=True, inplace=True)
    df_final["N"] = range(len(df_final))

st.session_state.tabla_pagos = df_final

st.caption(
    "Sin dobles tecleos: cualquier cambio en PAGO BANCO o N PaB reparte al instante (la última cuota ajusta) y el resto queda en 0. "
    "La primera fila de 'Pagos de CE' siempre refleja el CE inicial en el mismo instante. "
    "Al cambiar de Referencia/Id, todo se reinicia limpio."
)
