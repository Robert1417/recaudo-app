import streamlit as st
import pandas as pd
import numpy as np
import calendar
from datetime import date

# ------------------ Ajustes globales ------------------
pd.set_option("mode.copy_on_write", True)
st.set_page_config(page_title="Calculadora de Recaudo", page_icon="💸", layout="centered")
st.title("💸 Calculadora de Recaudo")

st.caption(
    "1) Carga tu base `cartera_asignada_filtrada` • "
    "2) Escribe la **Referencia** y selecciona **uno o varios Id deuda** • "
    "3) Ajusta valores editables (Deuda, Apartado, Comisión, Saldo) • "
    "4) Ingresa **PAGO BANCO** y **N PaB** → se calcula **DESCUENTO**, **Comisión de éxito** y **Pagos de CE**."
)

# ------------------ utilidades ------------------
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
    """Reparte 'total' en 'n' cuotas enteras; la última ajusta para sumar exacto."""
    n = max(1, int(n))
    total_int = int(round(float(total or 0.0)))
    base = total_int // n
    resto = total_int - base * n
    cuotas = [float(base)] * n
    cuotas[-1] = float(base + resto)
    return cuotas

def _ensure_table_exists():
    if "tabla_pagos" not in st.session_state or not isinstance(st.session_state.tabla_pagos, pd.DataFrame):
        st.session_state.tabla_pagos = pd.DataFrame({
            "N": [0, 1, 2, 3, 4],
            "FECHA": [date.today(), pd.NaT, pd.NaT, pd.NaT, pd.NaT],
            "Pago(s) a banco": [0.0]*5,
            "Pagos de CE": [0.0]*5,
        })

def _ultimo_dia_mes_siguiente(fecha_base):
    if pd.isna(fecha_base) or str(fecha_base).strip() == "":
        fecha_base = date.today()
    y = fecha_base.year
    m = fecha_base.month + 1
    if m > 12:
        m = 1
        y += 1
    ultimo = calendar.monthrange(y, m)[1]
    return date(y, m, ultimo)

def _completar_fechas(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if len(df) > 0:
        if pd.isna(df.loc[0, "FECHA"]) or str(df.loc[0, "FECHA"]).strip() == "":
            df.loc[0, "FECHA"] = date.today()
        for i in range(1, len(df)):
            if pd.isna(df.loc[i, "FECHA"]) or str(df.loc[i, "FECHA"]).strip() == "":
                df.loc[i, "FECHA"] = _ultimo_dia_mes_siguiente(df.loc[i - 1, "FECHA"])
    return df

def _ensure_rows(df: pd.DataFrame, need_rows: int) -> pd.DataFrame:
    """Asegura al menos `need_rows` filas; rellena con NaT/0.0 y numera N; completa fechas."""
    if len(df) < need_rows:
        faltan = need_rows - len(df)
        extra = pd.DataFrame({
            "N": list(range(len(df), len(df) + faltan)),
            "FECHA": [pd.NaT] * faltan,
            "Pago(s) a banco": [0.0] * faltan,
            "Pagos de CE": [0.0] * faltan,
        })
        df = pd.concat([df.reset_index(drop=True), extra], ignore_index=True)
    df = df.reset_index(drop=True)
    df["N"] = range(len(df))
    return _completar_fechas(df)

def _sync_ce_inicial_to_table():
    """Forzar que la fila 0 de 'Pagos de CE' refleje CE inicial (sin tocar resto)."""
    _ensure_table_exists()
    df = st.session_state.tabla_pagos.copy(deep=True)
    ce = int(round(float(st.session_state.get("ce_inicial_val", 0.0) or 0.0)))
    if len(df) == 0:
        df = pd.DataFrame({"N":[0], "FECHA":[date.today()], "Pago(s) a banco":[0.0], "Pagos de CE":[0.0]})
    df = _completar_fechas(df)
    df.loc[0, "Pagos de CE"] = float(max(0, ce))
    df.reset_index(drop=True, inplace=True)
    df["N"] = range(len(df))
    st.session_state.tabla_pagos = df

def _repartir_pagos_banco():
    """Reparte PAGO BANCO en N PaB sobre la columna 'Pago(s) a banco'."""
    _ensure_table_exists()
    df = st.session_state.tabla_pagos.copy(deep=True)
    n = int(max(1, st.session_state.get("n_pab", 1)))
    total = float(st.session_state.get("pago_banco", 0.0) or 0.0)

    if len(df) < n:
        faltan = n - len(df)
        extra = pd.DataFrame({
            "N": list(range(len(df), len(df) + faltan)),
            "FECHA": [pd.NaT]*faltan,
            "Pago(s) a banco": [0.0]*faltan,
            "Pagos de CE": [0.0]*faltan,
        })
        df = pd.concat([df.reset_index(drop=True), extra], ignore_index=True)

    df.reset_index(drop=True, inplace=True)
    df["N"] = range(len(df))
    df = _completar_fechas(df)

    cuotas = _distribuir_en_n_pagos(total, n) if total > 0 else [0.0]*n
    df.loc[:, "Pago(s) a banco"] = 0.0
    for i in range(n):
        df.loc[i, "Pago(s) a banco"] = float(cuotas[i])

    st.session_state.tabla_pagos = df

def _distribuir_ce_restante_en_cuotas_iguales():
    """
    Define cuotas IGUALES para el CE restante (= comision_exito - ce_inicial), sin superar Apartado.
    No pisa la fila 0 (CE inicial). Guarda 'q' y 'n' en session_state para el balanceo posterior.
    """
    _ensure_table_exists()
    df = st.session_state.tabla_pagos.copy(deep=True)
    df = _completar_fechas(df)

    com_ex = int(round(float(st.session_state.get("comision_exito", 0.0) or 0.0)))
    ce_ini = int(round(float(st.session_state.get("ce_inicial_val", 0.0) or 0.0)))
    cap    = int(round(float(st.session_state.get("apartado_edit", 0.0) or 0.0)))

    if len(df) == 0:
        df = pd.DataFrame({"N":[0], "FECHA":[date.today()], "Pago(s) a banco":[0.0], "Pagos de CE":[0.0]})

    # Fila 0 = CE inicial (sin tope)
    df.loc[0, "Pagos de CE"] = float(max(0, ce_ini))
    restante = max(0, com_ex - ce_ini)

    # Si no hay restante, limpiar filas >0 y registrar q/n = 0
    if restante == 0:
        if len(df) > 1:
            df.loc[1:, "Pagos de CE"] = 0.0
        st.session_state._ce_cuota_val = 0
        st.session_state._ce_num_cuotas = 0
        st.session_state.tabla_pagos = df.reset_index(drop=True)
        st.session_state.tabla_pagos["N"] = range(len(st.session_state.tabla_pagos))
        return

    # Si no hay tope efectivo, toda una sola cuota en la fila 1
    if cap <= 0:
        df = _ensure_rows(df, 2)
        df.loc[1:, "Pagos de CE"] = 0.0
        df.loc[1, "Pagos de CE"] = float(restante)
        st.session_state._ce_cuota_val = restante
        st.session_state._ce_num_cuotas = 1
        st.session_state.tabla_pagos = df.reset_index(drop=True)
        st.session_state.tabla_pagos["N"] = range(len(st.session_state.tabla_pagos))
        return

    # Buscar n y cuota q iguales que EXACTAMENTE sumen 'restante' y q<=cap
    n_min = int(np.ceil(restante / cap))
    n_found, q_found = None, None

    for n in range(n_min, n_min + 241):  # margen amplio
        q = restante // n
        if q <= 0:
            q = 1
        if (q * n == restante) and (q <= cap):
            n_found, q_found = n, int(q)
            break

    if n_found is None:
        # fallback: incrementar n hasta que 'restante' sea múltiplo de n y q<=cap
        n = n_min
        while (restante % n != 0 or (restante // n) > cap) and n < n_min + 241:
            n += 1
        if (restante % n == 0) and (restante // n) <= cap:
            n_found, q_found = n, int(restante // n)
        else:
            # último recurso: forzar q <= cap y ajustar n para que q*n == restante
            q_found = int(min(cap, max(1, restante // n_min)))
            n_found = int(np.ceil(restante / q_found))
            while restante % n_found != 0 and n_found < n_min + 241:
                n_found += 1
            q_found = int(restante // n_found)

    # Registrar en sesión para el balanceo
    st.session_state._ce_cuota_val = int(q_found)
    st.session_state._ce_num_cuotas = int(n_found)

    # Prefill (opcional) 1..n con q, solo como sugerencia base antes del balanceo
    need = 1 + n_found
    df = _ensure_rows(df, need)
    df.loc[1:, "Pagos de CE"] = 0.0
    for i in range(n_found):
        df.loc[1 + i, "Pagos de CE"] = float(q_found)

    st.session_state.tabla_pagos = df.reset_index(drop=True)
    st.session_state.tabla_pagos["N"] = range(len(st.session_state.tabla_pagos))

def _balancear_ce_vs_apartado():
    """
    Coloca EXACTAMENTE n cuotas IGUALES de CE (valor q) después de la fila 0.
    No divide cuotas: si en una fila no cabe q (por PagoBanco alto), la salta y usa la siguiente.
    Crea filas nuevas (con fecha) si hace falta. La fila 0 conserva CE inicial intacta.
    """
    _ensure_table_exists()
    df = st.session_state.tabla_pagos.copy(deep=True)
    df = _completar_fechas(df)

    cap  = int(round(float(st.session_state.get("apartado_edit", 0.0) or 0.0)))
    q    = int(st.session_state.get("_ce_cuota_val", 0))
    n    = int(st.session_state.get("_ce_num_cuotas", 0))
    objetivo_ce = float(st.session_state.get("comision_exito", 0.0) or 0.0)

    if len(df) == 0:
        df = pd.DataFrame({"N":[0], "FECHA":[date.today()], "Pago(s) a banco":[0.0], "Pagos de CE":[0.0]})
    df = _ensure_rows(df, 1)  # al menos fila 0

    # limpiar CE de filas >=1; fila 0 queda como está (CE inicial)
    if len(df) > 1:
        df.loc[1:, "Pagos de CE"] = 0.0

    # Si no hay tope efectivo, poner todas las cuotas (que serían una sola en este caso) seguidas
    if cap <= 0:
        for k in range(n):
            idx = len(df)
            df = _ensure_rows(df, idx + 1)
            df.loc[idx, "Pagos de CE"] = float(q)
        st.session_state.tabla_pagos = df.reset_index(drop=True)
        st.session_state.tabla_pagos["N"] = range(len(st.session_state.tabla_pagos))
        return

    # Colocar n cuotas de valor q saltando filas que no tengan capacidad
    placed = 0
    i_row = 1
    while placed < n:
        if i_row >= len(df):
            df = _ensure_rows(df, i_row + 1)

        pago_i = float(df.loc[i_row, "Pago(s) a banco"] or 0.0)
        permitido = max(0, cap - int(round(pago_i)))

        if permitido >= q:
            # cabe la cuota completa aquí
            df.loc[i_row, "Pagos de CE"] = float(q)
            placed += 1
            i_row += 1
        else:
            # no cabe q → saltar
            i_row += 1

    # Garantizar suma total de CE = Comisión de éxito (por si fila 0 cambió)
    suma_ce = float(pd.to_numeric(df["Pagos de CE"], errors="coerce").fillna(0).sum())
    if suma_ce < objetivo_ce - 1e-6:
        # todavía falta (ej: fila 0 era menor); añadir cuotas extra completas si es posible
        faltante = int(round(objetivo_ce - suma_ce))
        extra_n = int(np.ceil(faltante / max(1, q)))
        for _ in range(extra_n):
            idx = len(df)
            df = _ensure_rows(df, idx + 1)
            pago = float(df.loc[idx, "Pago(s) a banco"] or 0.0)
            cap_idx = max(0, cap - int(round(pago)))
            if cap_idx >= q:
                poner = q
            else:
                # si ni siquiera cabe q aquí, saltamos y creamos otra fila
                continue
            df.loc[idx, "Pagos de CE"] = float(poner)

    df = df.reset_index(drop=True)
    df["N"] = range(len(df))
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

# ------------------ 3) Valores base (reactivo) ------------------
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

    # Inits
    st.session_state.deuda_res_edit = deuda_res_total_def
    st.session_state.comision_m_edit = comision_m_base_def
    st.session_state.apartado_edit   = apartado_base_def
    st.session_state.saldo_edit      = saldo_base_def
    st.session_state.ce_base         = ce_base_def

    st.session_state.pago_banco      = 0.0
    st.session_state.n_pab           = 1

    st.session_state.comision_exito_overridden = False
    st.session_state.comision_exito  = max(0.0, (deuda_res_total_def - 0.0) * 1.19 * ce_base_def)

    st.session_state.ce_inicial_val  = 0.0

    st.session_state.tabla_pagos = pd.DataFrame({
        "N": [0, 1, 2, 3, 4],
        "FECHA": [date.today(), pd.NaT, pd.NaT, pd.NaT, pd.NaT],
        "Pago(s) a banco": [0.0]*5,
        "Pagos de CE": [0.0]*5,
    })

    st.session_state._last_pab = st.session_state.pago_banco
    st.session_state._last_n   = st.session_state.n_pab

    st.session_state._last_ce_tuple = (int(round(st.session_state.comision_exito)),
                                       int(round(st.session_state.ce_inicial_val)),
                                       int(round(st.session_state.apartado_edit)),
                                       len(st.session_state.tabla_pagos))
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
                    value=float(st.session_state.apartado_edit), format="%.0f", key="apartado_edit",
                    on_change=lambda: (_distribuir_ce_restante_en_cuotas_iguales(), _balancear_ce_vs_apartado()))
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

if st.session_state._last_pab != st.session_state.pago_banco or st.session_state._last_n != st.session_state.n_pab:
    st.session_state._last_pab = st.session_state.pago_banco
    st.session_state._last_n   = st.session_state.n_pab
    _repartir_pagos_banco()
    com_ex_prefill = max(0.0, (st.session_state.deuda_res_edit - st.session_state.pago_banco) * 1.19 * st.session_state.ce_base)
    if not st.session_state.get("comision_exito_overridden", False):
        st.session_state.comision_exito = com_ex_prefill
    _balancear_ce_vs_apartado()
    st.rerun()

# Comisión de éxito (editable) y CE inicial
c4, c5 = st.columns(2)
with c4:
    com_ex_prefill_now = max(0.0, (st.session_state.deuda_res_edit - st.session_state.pago_banco) * 1.19 * st.session_state.ce_base)
    if not st.session_state.get("comision_exito_overridden", False):
        st.session_state.comision_exito = com_ex_prefill_now
    prev = float(st.session_state.comision_exito)
    new_val = st.number_input("🏁 Comisión de éxito (editable)", min_value=0.0, step=1000.0,
                              value=prev, format="%.0f", key="comision_exito",
                              on_change=lambda: (_distribuir_ce_restante_en_cuotas_iguales(), _balancear_ce_vs_apartado()))
    st.session_state.comision_exito_overridden = (abs(new_val - com_ex_prefill_now) > 1e-6)

with c5:
    st.number_input("🧪 CE inicial", min_value=0.0, step=1000.0,
                    value=float(st.session_state.get("ce_inicial_val", 0.0)),
                    format="%.0f", key="ce_inicial_val",
                    on_change=lambda: (_distribuir_ce_restante_en_cuotas_iguales(), _balancear_ce_vs_apartado()))
    _sync_ce_inicial_to_table()  # asegura que la fila 0 siempre coincida visualmente

# Avance CE inicial vs Comisión de éxito
st.markdown("#### Avance de CE inicial sobre la Comisión de éxito")
ce_inicial = float(st.session_state.ce_inicial_val or 0.0)
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

# ------------------ 5) Cronograma de pagos (tabla editable c/fechas) ------------------
st.markdown("### 5) Cronograma de pagos (tabla editable con fechas automáticas)")

# Si cambió algún driver, recalculamos y balanceamos antes de pintar (mantiene consistencia)
ce_tuple_now = (int(round(st.session_state.comision_exito)),
                int(round(st.session_state.ce_inicial_val)),
                int(round(st.session_state.apartado_edit)),
                len(st.session_state.tabla_pagos))
if ce_tuple_now != st.session_state.get("_last_ce_tuple"):
    _distribuir_ce_restante_en_cuotas_iguales()
    _balancear_ce_vs_apartado()
    st.session_state._last_ce_tuple = ce_tuple_now

df_view = _completar_fechas(st.session_state.tabla_pagos.copy(deep=True))

edited_df = st.data_editor(
    df_view,
    use_container_width=True,
    num_rows="dynamic",
    column_config={
        "N": st.column_config.NumberColumn("N", min_value=0, max_value=200, step=1, disabled=True,
                                           help="Consecutivo automático desde 0."),
        "FECHA": st.column_config.DateColumn("FECHA", format="YYYY-MM-DD",
                                             help="La primera es hoy; las demás, el último día del mes siguiente."),
        "Pago(s) a banco": st.column_config.NumberColumn("Pago(s) a banco", step=1,
                                                         help="Puedes editarlo; el reparto se rehace al cambiar PAGO BANCO/N PaB."),
        "Pagos de CE": st.column_config.NumberColumn("Pagos de CE", step=1,
                                                     help="Sugerido: CE inicial (fila 0) y resto balanceado ≤ Apartado."),
    },
    key="editor_tabla_pagos",
)

df_final = edited_df.copy(deep=True)
df_final.reset_index(drop=True, inplace=True)
df_final["N"] = range(len(df_final))
st.session_state.tabla_pagos = df_final

# ---------- 6) Validación y KPIs (no editables) ----------
st.markdown("### 6) Validación y KPIs")

# Totales objetivo
target_pab = float(st.session_state.get("pago_banco", 0.0) or 0.0)                # PAGO BANCO (input)
target_ce  = float(st.session_state.get("comision_exito", 0.0) or 0.0)            # Comisión de éxito (input)

# Totales de la tabla
df_calc   = st.session_state.tabla_pagos.copy(deep=True)
suma_pab  = pd.to_numeric(df_calc["Pago(s) a banco"], errors="coerce").fillna(0).sum()
suma_ce   = pd.to_numeric(df_calc["Pagos de CE"], errors="coerce").fillna(0).sum()

# Función tolerancia 98%
def _within_tol(x, y, tol=0.02):
    denom = max(1.0, abs(y))
    return abs(float(x) - float(y)) / denom <= tol

ok_pab = _within_tol(suma_pab, target_pab)
ok_ce  = _within_tol(suma_ce, target_ce)

if not (ok_pab and ok_ce):
    st.error("Las sumas no cuadran (≥98% requerido). Ajusta la tabla o los parámetros.")
    colA, colB = st.columns(2)
    with colA:
        st.write("**PAGO BANCO**")
        st.write(f"- Objetivo: {target_pab:,.0f}")
        st.write(f"- Suma tabla: {suma_pab:,.0f}")
        st.write(f"- Diferencia: {target_pab - suma_pab:,.0f}")
    with colB:
        st.write("**Comisión de éxito (CE)**")
        st.write(f"- Objetivo: {target_ce:,.0f}")
        st.write(f"- Suma tabla: {suma_ce:,.0f}")
        st.write(f"- Diferencia: {target_ce - suma_ce:,.0f}")
else:
    # -------- KPIs requeridos (bloqueados) --------
    # 1) % Primer Pago = CE inicial / Comisión de éxito
    ce_inicial = float(st.session_state.get("ce_inicial_val", 0.0) or 0.0)
    pct_primer_pago = (ce_inicial / target_ce) if target_ce > 0 else np.nan

    # 2) PLAZO = número de filas hasta la última donde (Pago banco != 0) o (Pagos CE != 0)
    mask_valid = (pd.to_numeric(df_calc["Pago(s) a banco"], errors="coerce").fillna(0) != 0) | \
                 (pd.to_numeric(df_calc["Pagos de CE"], errors="coerce").fillna(0) != 0)
    if mask_valid.any():
        last_idx = int(np.where(mask_valid.values)[0][-1])  # índice (0-based)
        plazo = last_idx + 1                                 # cantidad de filas válidas
    else:
        plazo = 0

    # 3) Comisión de éxito total = target_ce
    comision_total = target_ce

    # 4) Cuota/Apartado
    #    ((CE + PAGO BANCO - primera_fila_PagoBanco - primera_fila_PagosCE + Comisión Mensual) / (PLAZO - 1)) / Apartado Mensual
    first_pb = float(pd.to_numeric(df_calc.loc[0, "Pago(s) a banco"], errors="coerce") if len(df_calc) > 0 else 0.0)
    first_ce = float(pd.to_numeric(df_calc.loc[0, "Pagos de CE"], errors="coerce") if len(df_calc) > 0 else 0.0)
    comision_mensual  = float(st.session_state.get("comision_m_edit", 0.0) or 0.0)
    apartado_mensual  = float(st.session_state.get("apartado_edit", 0.0) or 0.0)

    numerador = (comision_total + target_pab - first_pb - first_ce + comision_mensual)
    if (plazo - 1) > 0 and apartado_mensual > 0:
        cuota_apartado = (numerador / (plazo - 1)) / apartado_mensual
    else:
        cuota_apartado = np.nan

    # ---- Mostrar en casillas no editables ----
    st.success("Listo: los totales cuadran (≥98%).")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.number_input("🏁 Comisión de éxito total", value=float(comision_total), step=0.0, format="%.0f", disabled=True)
    with c2:
        st.number_input("📅 PLAZO (meses)", value=float(plazo), step=1.0, format="%.0f", disabled=True)
    with c3:
        # Muestra estilo 0.50; si prefieres 50% usa: f"{pct_primer_pago*100:.2f}%"
        st.text_input("% Primer Pago (CE inicial / CE)", value=("—" if np.isnan(pct_primer_pago) else f"{pct_primer_pago:.2f}"), disabled=True)
    with c4:
        st.text_input("Cuota/Apartado", value=("—" if np.isnan(cuota_apartado) else f"{cuota_apartado:.4f}"), disabled=True)

st.caption(
    "🗓️ Fechas automáticas; 💼 PAGO BANCO se reparte en N PaB (última ajusta). "
    "🏁 Pagos de CE: fila 0 = CE inicial y el resto se balancea automáticamente para que cada fila cumpla el Apartado Mensual; "
    "si no alcanza, se crean filas nuevas. Todo sigue editable."
)
