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

# ---------- 3) cajas editables ----------
st.markdown("### 3) Valores base (puedes editarlos)")

# Usamos un formulario para evitar rerun por cada tecla y acelerar la app
with st.form("parametros_base"):
    # Del primer registro tomamos Apartado/Comisión/Saldo/CE; la Deuda se SUMA si hay varias
    fila_primera = sel.iloc[0]
    deuda_res_total   = float(sel[col_deu].sum(skipna=True))
    apartado_base     = float(fila_primera[col_apar]) if pd.notna(fila_primera[col_apar]) else 0.0
    comision_m_base   = float(fila_primera[col_com]) if pd.notna(fila_primera[col_com]) else 0.0
    saldo_base        = float(fila_primera[col_saldo]) if pd.notna(fila_primera[col_saldo]) else 0.0
    ce_base           = float(fila_primera[col_ce]) if pd.notna(fila_primera[col_ce]) else 0.0

    # --- FILA 1: Deuda / Comisión / Apartado / Saldo ---
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        deuda_res_edit = st.number_input(
            "💰 Deuda Resuelve",
            min_value=0.0, step=1000.0,
            value=deuda_res_total, format="%.0f"
        )

    with col2:
        comision_m_edit = st.number_input(
            "🎯 Comisión Mensual",
            min_value=0.0, step=1000.0,
            value=comision_m_base, format="%.0f"
        )

    with col3:
        apartado_edit = st.number_input(
            "📆 Apartado Mensual",
            min_value=0.0, step=1000.0,
            value=apartado_base, format="%.0f"
        )

    with col4:
        saldo_edit = st.number_input(
            "💼 Saldo (Ahorro)",
            min_value=0.0, step=1000.0,
            value=saldo_base, format="%.0f"
        )

    # --- FILA 2: Saldo Neto / Depósito ---
    saldo_neto = 0.0
    if pd.notna(saldo_edit) and saldo_edit > 0:
        saldo_neto = float(saldo_edit) - (float(saldo_edit) - 25000.0) * 0.004
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
            help="Calculado automáticamente: Saldo − (Saldo − 25.000) × 0.004 (solo si Saldo > 0)"
        )

    with col6:
        deposito_edit = st.number_input(
            "💵 Depósito",
            min_value=0.0, step=1000.0,
            value=0.0, format="%.0f",
            help="Monto extra aportado al inicio; por defecto 0"
        )

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
        comision_exito = st.number_input(
            "🏁 Comisión de éxito (editable)",
            min_value=0.0, step=1000.0,
            value=float(com_exito_default), format="%.0f",
            help=f"Prefill: (Deuda Resuelve − PAGO BANCO) × 1.19 × CE (CE base del 1er registro = {ce_base:.4f})"
        )
    with c5:
        ce_inicial_txt = st.text_input("🧪 CE inicial", value="", placeholder="Ej. 150000")
        try:
            ce_inicial = float(ce_inicial_txt.replace(",", ".")) if ce_inicial_txt.strip() != "" else None
        except Exception:
            ce_inicial = None
            st.warning("CE inicial inválido; déjalo vacío o usa un número como 0.12")

    # --- Barra: CE inicial vs Comisión de éxito ---
    st.markdown("#### Avance de CE inicial sobre la Comisión de éxito")

    if (ce_inicial is None) or (ce_inicial <= 0):
        st.info("Escribe un valor en **CE inicial** para ver el porcentaje.")
    else:
        base = float(comision_exito) if comision_exito and comision_exito > 0 else 0.0
        if base <= 0:
            st.warning("La **Comisión de éxito** debe ser mayor a 0 para calcular el porcentaje.")
        else:
            porcentaje = (float(ce_inicial) / base) * 100.0
            porcentaje_capped = max(0.0, min(porcentaje, 100.0))  # limitar entre 0% y 100%

            # Barra de progreso
            st.progress(int(round(porcentaje_capped)))

            # Texto con detalle debajo
            st.caption(
                f"CE inicial: {ce_inicial:,.0f}  |  Comisión de éxito: {base:,.0f}  →  "
                f"**{porcentaje:,.2f}%** de la Comisión de éxito"
            )

    # Botón para aplicar (evita re-ejecutar todo en cada tecla)
    aplicar = st.form_submit_button("Aplicar cambios")

# Si no se han aplicado cambios, no seguimos (evitamos cálculos/render extra)
if not aplicar:
    st.stop()

# ===============================
# 5) 📅 Plan de pagos (editable)
# ===============================
import math
import numpy as np

# --- Recuperar parámetros congelados del formulario (bloque 4)
_snap = st.session_state.get("params_snapshot", {})
deuda_res_edit  = _snap.get("deuda_res_edit", 0.0)
apartado_edit   = _snap.get("apartado_edit",   0.0)
pago_banco      = _snap.get("pago_banco",      0.0)
n_pab           = _snap.get("n_pab",           1)
comision_exito  = _snap.get("comision_exito",  0.0)
ce_inicial      = _snap.get("ce_inicial",      0.0)

def end_of_month(ts: pd.Timestamp) -> pd.Timestamp:
    return (ts + pd.offsets.MonthEnd(0)).normalize()

def _build_base_plan(pago_total: float, n_cuotas: int):
    """Plan base con fechas y PB repartido en partes iguales (ajuste de redondeo al final)."""
    today = pd.Timestamp.today().normalize()
    n_cuotas = int(max(1, n_cuotas))
    fechas = [today] + [
        end_of_month(today + pd.DateOffset(months=k)) for k in range(1, n_cuotas)
    ]
    if n_cuotas == 1:
        pagos_banco = [int(round(pago_total))]
    else:
        base = pago_total / n_cuotas
        pagos_banco = [int(round(base)) for _ in range(n_cuotas)]
        diff = int(round(pago_total)) - sum(pagos_banco)
        if diff != 0:
            pagos_banco[-1] += diff
    df = pd.DataFrame({
        "N": list(range(1, n_cuotas + 1)),
        "FECHA": fechas,
        "PAGO BANCO": pagos_banco,
        "PAGO COMISION": [0] * n_cuotas,
    })
    return df

# --- Si se presionó "Aplicar cambios" o no hay plan en sesión, genera plan base
if st.session_state.get("params_ready", False) or "plan_before" not in st.session_state:
    df_plan = _build_base_plan(float(pago_banco or 0.0), int(n_pab or 1))
    # CE inicial fijo en N=1 (si hay fila)
    CE_INI = int(round(min(max(0.0, float(ce_inicial or 0.0)), max(0.0, float(comision_exito or 0.0)))))
    if len(df_plan) > 0:
        df_plan.at[0, "PAGO COMISION"] = CE_INI
    st.session_state["plan_before"] = df_plan.copy()
    st.session_state["params_ready"] = False  # consumir evento

# --- Usar el plan persistente
df_plan = st.session_state.get("plan_before", pd.DataFrame()).copy()
if df_plan.empty:
    st.warning("Aún no has generado un plan. Usa 'Aplicar cambios' arriba.")
    st.stop()

# --- Totales y CE inicial (acotado)
TOTAL_PB = int(round(float(pago_banco or 0.0)))
TOTAL_PC = int(round(float(comision_exito or 0.0)))
CE_INI   = int(round(float(ce_inicial or 0.0)))
CE_INI   = max(0, min(CE_INI, TOTAL_PC))  # acotar

# Garantizar tipos consistentes
df_plan["PAGO BANCO"] = pd.to_numeric(df_plan["PAGO BANCO"], errors="coerce").fillna(0).astype(int)
df_plan["PAGO COMISION"] = pd.to_numeric(df_plan["PAGO COMISION"], errors="coerce").fillna(0).astype(int)
if len(df_plan) > 0:
    df_plan.at[0, "PAGO COMISION"] = CE_INI

st.markdown("### 5) 📅 Plan de pagos sugerido (editable)")

edited = st.data_editor(
    df_plan[["N", "FECHA", "PAGO BANCO", "PAGO COMISION"]],
    use_container_width=True,
    hide_index=True,
    column_config={
        "N": st.column_config.NumberColumn(disabled=True, format="%d"),
        "FECHA": st.column_config.DatetimeColumn(disabled=True),
        "PAGO BANCO": st.column_config.NumberColumn(format="%.0f", step=1000),
        "PAGO COMISION": st.column_config.NumberColumn(format="%.0f", step=1000),
    },
    key="editor_plan_pag",
).copy()

# Normalizar tipos y re-fijar CE inicial en N=1 si existe
for c in ["PAGO BANCO", "PAGO COMISION"]:
    edited[c] = pd.to_numeric(edited[c], errors="coerce").fillna(0).astype(int)
if len(edited) > 0:
    edited.at[0, "PAGO COMISION"] = CE_INI

# ---------- utilidades de reequilibrio ----------
def _add_new_row(df):
    """Agrega una fila nueva (mes siguiente, PB=0, PC=0)."""
    if len(df) == 0:
        last_date = pd.Timestamp.today().normalize()
        new_N = 1
    else:
        last_date = pd.to_datetime(df["FECHA"].max()).normalize()
        new_N     = int(df["N"].max()) + 1
    new_date  = end_of_month(last_date + pd.DateOffset(months=1))
    return pd.concat([df, pd.DataFrame([{
        "N": new_N, "FECHA": new_date, "PAGO BANCO": 0, "PAGO COMISION": 0
    }])], ignore_index=True)

def _rebalance(values, total, changed_idx, lock_idxs=None):
    """
    Reequilibra 'values' (np.array int) para que sumen 'total'.
    - changed_idx: índice editado (no se toca).
    - lock_idxs: índices bloqueados (no se tocan).
    Devuelve np.array reequilibrado (≥0).
    """
    vals = values.copy()
    n = len(vals)
    if n == 0:
        return vals
    if lock_idxs is None:
        lock_idxs = set()
    lock_idxs = set(lock_idxs) | {changed_idx}

    diff = int(total - vals.sum())
    if diff == 0:
        return np.maximum(vals, 0)

    free = [i for i in range(n) if i not in lock_idxs]
    if len(free) == 0:
        return np.maximum(vals, 0)

    base = diff // len(free)
    rem  = diff - base * len(free)
    for i in free:
        vals[i] += base
    for i in reversed(free):
        if rem == 0:
            break
        step = 1 if rem > 0 else -1
        vals[i] += step
        rem -= step
    return np.maximum(vals, 0)

# ---------- detectar cambios respecto al estado previo ----------
prev = st.session_state["plan_before"].copy()

pb_prev = prev["PAGO BANCO"].astype(int).to_numpy()
pb_new  = edited["PAGO BANCO"].astype(int).to_numpy()

pc_prev = prev["PAGO COMISION"].astype(int).to_numpy()
pc_new  = edited["PAGO COMISION"].astype(int).to_numpy()

# Asegurar que tengan al menos una posición antes de fijar índice 0
if pc_prev.size > 0:
    pc_prev[0] = CE_INI
if pc_new.size > 0:
    pc_new[0] = CE_INI

changed_pb_idx = None
changed_pc_idx = None

if pb_prev.shape == pb_new.shape:
    diffs_pb = np.where(pb_prev != pb_new)[0]
    if diffs_pb.size > 0:
        changed_pb_idx = int(diffs_pb[0])
else:
    # Si cambió el largo, considera que el cambio está en la última fila editada
    changed_pb_idx = min(len(pb_new), len(pb_prev)) - 1 if min(len(pb_new), len(pb_prev)) > 0 else 0

if pc_prev.shape == pc_new.shape:
    diffs_pc = np.where(pc_prev != pc_new)[0]
    # ignorar N=1 fijo
    diffs_pc = diffs_pc[diffs_pc != 0]
    if diffs_pc.size > 0:
        changed_pc_idx = int(diffs_pc[0])
else:
    # cambio de largo
    changed_pc_idx = min(len(pc_new), len(pc_prev)) - 1 if min(len(pc_new), len(pc_prev)) > 0 else 0

# ---------- Reequilibrio PB ----------
if changed_pb_idx is not None:
    vals = _rebalance(
        edited["PAGO BANCO"].astype(int).to_numpy(),
        TOTAL_PB,
        changed_idx=changed_pb_idx,
        lock_idxs=None
    )
    # si aún no alcanza el total (p.ej. todas libres quedaron 0), crear fila y poner resto
    resto = int(TOTAL_PB - vals.sum())
    if resto != 0:
        edited = _add_new_row(edited)
        vals = np.append(vals, max(0, resto))
    edited["PAGO BANCO"] = vals

# ---------- Reequilibrio PC (con N=1 fijo si existe) ----------
if changed_pc_idx is not None:
    lock = {0} if len(edited) > 0 else set()
    vals = _rebalance(
        edited["PAGO COMISION"].astype(int).to_numpy(),
        TOTAL_PC,
        changed_idx=changed_pc_idx,
        lock_idxs=lock
    )
    # Fijar CE_INI en la primera fila si existe
    if vals.size > 0:
        vals[0] = CE_INI
    resto = int(TOTAL_PC - vals.sum())
    if resto != 0:
        edited = _add_new_row(edited)
        vals = np.append(vals, max(0, resto))
        # asegurar de nuevo el CE_INI
        if vals.size > 0:
            vals[0] = CE_INI
    edited["PAGO COMISION"] = vals

# ---------- Persistir y mostrar control ----------
st.session_state["plan_before"] = edited.copy()

st.caption(
    f"🔎 Control — Pago Banco: {edited['PAGO BANCO'].sum():,} | "
    f"Pago Comisión: {edited['PAGO COMISION'].sum():,} | "
    f"Cuotas totales: {len(edited):,}"
)

