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
# Regla nueva: en N=1 SIEMPRE se paga CE inicial (sin tope).
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
    pagos_banco = [base] * n_cuotas_banco
    pagos_banco = [round(x) for x in pagos_banco]
    diff = round(pago_total) - sum(pagos_banco)
    if diff != 0:
        pagos_banco[-1] += diff

# --- DataFrame inicial
df_plan = pd.DataFrame({
    "N": list(range(1, n_cuotas_banco + 1)),
    "FECHA": fechas,
    "PAGO BANCO": pagos_banco,
    "PAGO COMISION": [0.0] * n_cuotas_banco,
})

# --- CE inicial: SIEMPRE se paga en N=1 (sin tope de Apartado)
ce_ini = float(ce_inicial or 0.0)
com_exito = float(comision_exito or 0.0)
apartado = float(apartado_edit or 0.0)

ce_inicial_pagada = min(max(0.0, ce_ini), max(0.0, com_exito))  # por seguridad
if len(df_plan) > 0 and ce_inicial_pagada > 0:
    df_plan.at[0, "PAGO COMISION"] = ce_inicial_pagada

# --- Comisión restante en cuotas iguales, usando capacidad mensual (PB + PC ≤ Apartado)
restante = int(round(max(0.0, com_exito - ce_inicial_pagada)))
apartado_i = int(round(apartado))

if restante > 0:
    # 1) Construye capacidades por mes a partir de N=2 (N=1 ya tiene CE inicial)
    def capacidades_actuales(df):
        caps = []
        idxs = []
        for i in range(len(df)):
            if i == 0:
                # N=1: no usamos para cuotas iguales (solo CE inicial)
                continue
            pb = int(df.at[i, "PAGO BANCO"])
            pc = int(df.at[i, "PAGO COMISION"])
            cap = max(0, apartado_i - (pb + pc))
            if cap > 0:
                caps.append(cap)
                idxs.append(i)
        return caps, idxs

    caps, idxs = capacidades_actuales(df_plan)

    # 2) Asegura que haya capacidad suficiente (agrega meses PB=0 con capacidad = Apartado)
    while sum(caps) < restante:
        last_date = df_plan["FECHA"].max() if len(df_plan) > 0 else today
        nueva_f = end_of_month(pd.Timestamp(last_date) + pd.DateOffset(months=1))
        df_plan.loc[len(df_plan)] = [len(df_plan) + 1, nueva_f, 0, 0]
        # Recalcula capacidades
        caps, idxs = capacidades_actuales(df_plan)

    # 3) Elige el número mínimo de cuotas k para que cuota ≤ min(capacidad de los k mejores meses)
    #    (tomamos los k meses con mayor capacidad)
    caps_sorted = sorted(caps, reverse=True)
    k = None
    for m in range(1, len(caps_sorted) + 1):
        cap_min_topm = caps_sorted[m - 1]  # la menor capacidad dentro del top-m
        cuota_necesaria = math.ceil(restante / m)
        if cuota_necesaria <= cap_min_topm:
            k = m
            break
    if k is None:
        # fallback (no debería ocurrir porque sum(caps) ≥ restante): usar todos los meses
        k = len(caps_sorted)

    # 4) Calcula k cuotas casi iguales (±1) que caben en esos k meses
    cuota_base = restante // k
    extras = restante - cuota_base * k  # primeras 'extras' cuotas serán cuota_base+1
    cuotas = [cuota_base + 1] * extras + [cuota_base] * (k - extras)

    # 5) Selecciona los índices de los k meses con mayor capacidad, luego ordénalos cronológicamente
    #    Mapear capacidad→índice preservando originales
    caps_with_idx = list(zip(caps, idxs))
    caps_with_idx.sort(key=lambda x: x[0], reverse=True)  # por capacidad desc
    sel = caps_with_idx[:k]
    sel.sort(key=lambda x: x[1])  # cronológico

    # 6) Asigna cuotas respetando capacidad de cada mes seleccionado
    for (cuota, (cap, i)) in zip(cuotas, sel):
        # seguridad (no debería exceder):
        cuota_final = min(int(cuota), max(0, apartado_i - (int(df_plan.at[i, "PAGO BANCO"]) + int(df_plan.at[i, "PAGO COMISION"]))))
        df_plan.at[i, "PAGO COMISION"] = int(df_plan.at[i, "PAGO COMISION"]) + cuota_final

    # 7) Si por redondeos quedó algo sin colocar (muy raro), agrega un mes nuevo y cierra exacto
    faltante = restante - sum(cuotas)
    if faltante > 0:
        last_date = df_plan["FECHA"].max() if len(df_plan) > 0 else today
        nueva_f = end_of_month(pd.Timestamp(last_date) + pd.DateOffset(months=1))
        df_plan.loc[len(df_plan)] = [len(df_plan) + 1, nueva_f, 0, faltante]
        
# Formato final
df_plan["PAGO BANCO"] = df_plan["PAGO BANCO"].round(0).astype(int)
df_plan["PAGO COMISION"] = df_plan["PAGO COMISION"].round(0).astype(int)

st.markdown("### 5) 📅 Plan de pagos sugerido")
st.dataframe(df_plan, use_container_width=True)

st.caption(
    f"🔎 Control — Pago Banco: {df_plan['PAGO BANCO'].sum():,} | "
    f"Pago Comisión: {df_plan['PAGO COMISION'].sum():,} | "
    f"Cuotas totales: {len(df_plan):,}"
)



