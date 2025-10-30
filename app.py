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
# 5) 📅 Plan de pagos (N, FECHA, PAGO BANCO, PAGO COMISION)
# Regla: en N=1 SIEMPRE se paga CE inicial (sin tope).
# ===============================
import math

def end_of_month(ts: pd.Timestamp) -> pd.Timestamp:
    return (ts + pd.offsets.MonthEnd(0)).normalize()

# --- Fechas base
today = pd.Timestamp.today().normalize()
fechas = [today]
for k in range(1, int(n_pab)):
    fechas.append(end_of_month(today + pd.DateOffset(months=k)))

# --- PAGO BANCO dividido en N PaB
pago_total = float(pago_banco or 0.0)
n_cuotas_banco = int(max(1, n_pab))
if n_cuotas_banco == 1:
    pagos_banco = [pago_total]
else:
    base = pago_total / n_cuotas_banco
    pagos_banco = [round(base)] * n_cuotas_banco
    diff = round(pago_total) - sum(pagos_banco)
    if diff != 0:
        pagos_banco[-1] += diff

# --- DataFrame inicial
df_plan = pd.DataFrame({
    "N": list(range(1, n_cuotas_banco + 1)),
    "FECHA": fechas,
    "PAGO BANCO": pagos_banco,
    "PAGO COMISION": [0] * n_cuotas_banco,
})

# --- CE inicial (N=1 sin tope)
ce_ini = float(ce_inicial or 0.0)
com_exito = float(comision_exito or 0.0)
apartado = float(apartado_edit or 0.0)
ce_inicial_pagada = min(max(0.0, ce_ini), max(0.0, com_exito))
if len(df_plan) > 0 and ce_inicial_pagada > 0:
    df_plan.at[0, "PAGO COMISION"] = int(round(ce_inicial_pagada))

# --- Comisión restante con tope de Apartado y ajuste exacto
restante = int(round(max(0.0, com_exito - ce_inicial_pagada)))
apartado_i = int(round(apartado))

if restante > 0:
    # 1) capacidades desde N>=2
    def capacidades_actuales(df):
        caps, idxs = [], []
        for i in range(1, len(df)):
            pb = int(df.at[i, "PAGO BANCO"])
            pc = int(df.at[i, "PAGO COMISION"])
            cap = max(0, apartado_i - (pb + pc))
            if cap > 0:
                caps.append(cap); idxs.append(i)
        return caps, idxs

    caps, idxs = capacidades_actuales(df_plan)

    # 2) agrega meses si falta capacidad
    while apartado_i > 0 and sum(caps) < restante:
        last_date = df_plan["FECHA"].max() if len(df_plan) > 0 else today
        nueva_f = end_of_month(pd.Timestamp(last_date) + pd.DateOffset(months=1))
        df_plan.loc[len(df_plan)] = [len(df_plan) + 1, nueva_f, 0, 0]
        caps, idxs = capacidades_actuales(df_plan)

    # 3) distribuye "casi igual" dentro de capacidad
    if apartado_i > 0 and sum(caps) > 0:
        caps_with_idx = sorted(zip(caps, idxs), key=lambda x: x[0], reverse=True)
        # mínimo k tal que cuota <= cap_min_topk
        caps_sorted = [c for c, _ in caps_with_idx]
        k = None
        for m in range(1, len(caps_sorted) + 1):
            cap_min_topm = caps_sorted[m - 1]
            cuota_necesaria = math.ceil(restante / m)
            if cuota_necesaria <= cap_min_topm:
                k = m; break
        if k is None:
            k = len(caps_sorted)

        cuota_base = restante // k
        extras = restante - cuota_base * k
        cuotas = [cuota_base + 1] * extras + [cuota_base] * (k - extras)

        sel = sorted(caps_with_idx[:k], key=lambda x: x[1])  # cronológico
        for (cuota, (cap, i)) in zip(cuotas, sel):
            pb = int(df_plan.at[i, "PAGO BANCO"])
            pc = int(df_plan.at[i, "PAGO COMISION"])
            cap_mes = max(0, apartado_i - (pb + pc))
            df_plan.at[i, "PAGO COMISION"] = pc + min(int(cuota), cap_mes)

    # 4) ajuste exacto en última fila (puede superar Apartado)
    asignado_rest = int(df_plan["PAGO COMISION"].iloc[1:].sum()) if len(df_plan) > 1 else 0
    faltante = restante - asignado_rest
    if faltante > 0:
        if len(df_plan) == 1:
            last_date = df_plan["FECHA"].max()
            nueva_f = end_of_month(pd.Timestamp(last_date) + pd.DateOffset(months=1))
            df_plan.loc[len(df_plan)] = [2, nueva_f, 0, 0]
        idx_last = len(df_plan) - 1
        df_plan.at[idx_last, "PAGO COMISION"] = int(df_plan.at[idx_last, "PAGO COMISION"]) + int(faltante)

# ---------------------------
# 🔧 Editor Ver / Editar con estado persistente + validaciones
# ---------------------------
df_plan["PAGO BANCO"]    = pd.to_numeric(df_plan["PAGO BANCO"], errors="coerce").fillna(0).astype(int)
df_plan["PAGO COMISION"] = pd.to_numeric(df_plan["PAGO COMISION"], errors="coerce").fillna(0).astype(int)

TARGET_PB     = int(round(float(pago_banco or 0.0)))
CE_INI_INT    = int(round(ce_inicial_pagada))
TARGET_PC_REST = int(round(max(0.0, float(comision_exito or 0.0) - CE_INI_INT)))
EPS = 0.01  # 1%

# ===== estado de sesión (solo una vez o al pulsar "Aplicar cambios") =====
if "plan_saved" not in st.session_state or aplicar:
    st.session_state["plan_saved"] = df_plan.copy()
if "plan_edit" not in st.session_state or aplicar:
    st.session_state["plan_edit"] = st.session_state["plan_saved"].copy()
if "edit_mode" not in st.session_state:
    st.session_state["edit_mode"] = False

st.markdown("### 5) 📅 Plan de pagos sugerido (editable)")

# Controles
c1, c2, c3 = st.columns([1,1,2])
with c1:
    if not st.session_state["edit_mode"]:
        if st.button("✏️ Editar", use_container_width=True):
            st.session_state["plan_edit"] = st.session_state["plan_saved"].copy()
            st.session_state["edit_mode"] = True
            # opcional: st.rerun()
    else:
        if st.button("💾 Guardar", type="primary", use_container_width=True):
            # fijar CE inicial en primera fila al guardar
            if len(st.session_state["plan_edit"]) > 0:
                st.session_state["plan_edit"].iloc[0, st.session_state["plan_edit"].columns.get_loc("PAGO COMISION")] = CE_INI_INT
            st.session_state["plan_saved"] = st.session_state["plan_edit"].copy()
            st.session_state["edit_mode"] = False
            # opcional: st.rerun()

with c2:
    if st.session_state["edit_mode"]:
        if st.button("↩️ Cancelar", use_container_width=True):
            st.session_state["plan_edit"] = st.session_state["plan_saved"].copy()
            st.session_state["edit_mode"] = False
            # opcional: st.rerun()

with c3:
    if st.session_state["edit_mode"]:
        if st.button("➕ Agregar fila de pago", use_container_width=True):
            df_tmp = st.session_state["plan_edit"]
            last_date = pd.to_datetime(df_tmp["FECHA"].max()).normalize() if len(df_tmp) else pd.Timestamp.today().normalize()
            new_date  = end_of_month(last_date + pd.DateOffset(months=1))
            new_N     = int(df_tmp["N"].max()) + 1 if len(df_tmp) else 1
            df_tmp = pd.concat([df_tmp, pd.DataFrame([{
                "N": new_N, "FECHA": new_date, "PAGO BANCO": 0, "PAGO COMISION": 0
            }])], ignore_index=True)
            st.session_state["plan_edit"] = df_tmp

# Vista según modo
if not st.session_state["edit_mode"]:
    view_df = st.session_state["plan_saved"].copy()
    if len(view_df) > 0:
        view_df.iloc[0, view_df.columns.get_loc("PAGO COMISION")] = CE_INI_INT
    st.dataframe(view_df[["N", "FECHA", "PAGO BANCO", "PAGO COMISION"]], use_container_width=True)
    current = view_df
else:
    edited = st.data_editor(
        st.session_state["plan_edit"][["N", "FECHA", "PAGO BANCO", "PAGO COMISION"]],
        use_container_width=True,
        hide_index=True,
        column_config={
            "N": st.column_config.NumberColumn(disabled=True, format="%d"),
            "FECHA": st.column_config.DatetimeColumn(disabled=True),
            "PAGO BANCO": st.column_config.NumberColumn(format="%.0f", step=1000),
            "PAGO COMISION": st.column_config.NumberColumn(
                format="%.0f", step=1000,
                help="La primera fila de PAGO COMISION es fija (CE inicial) al guardar."
            ),
        },
        key="editor_plan_libre",
    ).copy()
    for c in ["PAGO BANCO", "PAGO COMISION"]:
        edited[c] = pd.to_numeric(edited[c], errors="coerce").fillna(0).astype(int)
    st.session_state["plan_edit"] = edited
    current = edited

# ===== Validaciones 99% =====
sum_pb = int(current["PAGO BANCO"].sum())
if TARGET_PB > 0:
    acc_pb = 1 - abs(sum_pb - TARGET_PB) / TARGET_PB
    if acc_pb >= (1 - EPS):
        st.success(f"✔ PAGO BANCO ok: {sum_pb:,} de {TARGET_PB:,} (exactitud {acc_pb*100:.2f}%).")
    else:
        dif = sum_pb - TARGET_PB
        st.error(f"⚠ PAGO BANCO {'exceden' if dif>0 else 'faltan'} {abs(dif):,}. Exactitud {acc_pb*100:.2f}% (<99%).")
else:
    st.info("ℹ PAGO BANCO objetivo es 0; no se valida exactitud.")

sum_pc_rest = int(current["PAGO COMISION"].iloc[1:].sum()) if len(current) > 1 else 0
if TARGET_PC_REST > 0:
    acc_pc = 1 - abs(sum_pc_rest - TARGET_PC_REST) / TARGET_PC_REST
    if acc_pc >= (1 - EPS):
        st.success(f"✔ PAGO COMISIÓN ok: {sum_pc_rest:,} de {TARGET_PC_REST:,} (exactitud {acc_pc*100:.2f}%).")
    else:
        dif = sum_pc_rest - TARGET_PC_REST
        st.error(f"⚠ PAGO COMISIÓN (sin CE inicial) {'exceden' if dif>0 else 'faltan'} {abs(dif):,}. Exactitud {acc_pc*100:.2f}% (<99%).")
else:
    st.info("ℹ Comisión restante objetivo es 0; no se valida exactitud.")

st.caption(
    f"🔎 Control — Pago Banco: {current['PAGO BANCO'].sum():,} | "
    f"Pago Comisión (total): {current['PAGO COMISION'].sum():,} | "
    f"Cuotas totales: {len(current):,}"
)

