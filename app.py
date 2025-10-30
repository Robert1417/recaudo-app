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

# Del primer registro tomamos Apartado/Comisión/Saldo/CE; la Deuda se SUMA si hay varias
fila_primera = sel.iloc[0]
deuda_res_total   = float(sel[col_deu].sum(skipna=True))
apartado_base     = float(_to_num(fila_primera[col_apar])) if pd.notna(fila_primera[col_apar]) else 0.0
comision_m_base   = float(_to_num(fila_primera[col_com])) if pd.notna(fila_primera[col_com]) else 0.0
saldo_base        = float(_to_num(fila_primera[col_saldo])) if pd.notna(fila_primera[col_saldo]) else 0.0
ce_base           = float(_to_num(fila_primera[col_ce])) if pd.notna(fila_primera[col_ce]) else 0.0

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
# =========================
# 🧮 Recomendación automática de PAGO COMISION
# =========================

st.markdown("### 💡 Recomendación de PAGO COMISIÓN")

# =========================
# 📅 Editor seguro de tabla de pagos (evita ArrowTypeError)
# =========================
if "pab_table" in st.session_state:
    df_tmp = st.session_state["pab_table"].copy()

    # 🔹 Normalizar columna FECHA — siempre string (para evitar ArrowTypeError)
    if "FECHA" in df_tmp.columns:
        df_tmp["FECHA"] = pd.to_datetime(df_tmp["FECHA"], errors="coerce")
        df_tmp["FECHA"] = df_tmp["FECHA"].dt.strftime("%Y-%m-%d").fillna("")
    else:
        df_tmp["FECHA"] = ""

    # 🔹 Asegurar tipos uniformes en todas las columnas
    for col in df_tmp.columns:
        if pd.api.types.is_numeric_dtype(df_tmp[col]):
            df_tmp[col] = pd.to_numeric(df_tmp[col], errors="coerce").fillna(0)
        else:
            df_tmp[col] = df_tmp[col].astype(str).replace("nan", "").replace("NaT", "")

    # 🔹 Evitar conflictos de clave con el otro editor
    st.markdown("📅 **Tabla de pagos — PAGO BANCO (revisión final)**")
    st.caption("✅ Ahora sí puedes editar sin errores — 100% compatible con PyArrow.")

    edited = st.data_editor(
        df_tmp,
        hide_index=True,
        use_container_width=True,
        key="editor_pab_final"  # clave distinta del editor anterior
    )

    # 🔹 Restaurar tipo fecha después de editar
    edited["FECHA"] = pd.to_datetime(edited["FECHA"], errors="coerce")
    st.session_state["pab_table"] = edited
# =========================
# 🧮 Cálculo de la recomendación
# =========================
if st.button("🧮 Calcular recomendación de PAGO COMISIÓN", type="primary"):
    import math

    apartado = float(apartado_edit or 0.0)
    ce_ini = float(ce_inicial or 0.0)
    com_exito = float(comision_exito or 0.0)

    if com_exito <= 0:
        st.warning("La **Comisión de éxito** debe ser mayor a 0 para calcular la recomendación.")
    else:
        df_tab = st.session_state["pab_table"].copy()

        # Asegurar tipos
        df_tab["PAGO BANCO"] = pd.to_numeric(df_tab["PAGO BANCO"], errors="coerce").fillna(0.0)
        df_tab["PAGO COMISION"] = pd.to_numeric(df_tab["PAGO COMISION"], errors="coerce").fillna(0.0)
        df_tab["FECHA"] = pd.to_datetime(df_tab["FECHA"], errors="coerce")

        # 1️⃣ Primera fila: CE inicial sí o sí
        if not df_tab.empty and 1 in df_tab["N"].values:
            df_tab.loc[df_tab["N"] == 1, "PAGO COMISION"] = ce_ini
        else:
            st.error("No hay fila N=1 en la tabla de pagos.")
            st.stop()

        # 2️⃣ Reparto durante meses con PAGO BANCO > 0
        restante_total = max(0.0, com_exito - ce_ini)

        for idx in df_tab.index:
            n_val = int(df_tab.at[idx, "N"])
            if n_val == 1:
                continue
            pago_bco = float(df_tab.at[idx, "PAGO BANCO"])
            if pago_bco > 0 and apartado > 0 and restante_total > 0:
                capacidad = max(0.0, apartado - pago_bco)
                usar = min(capacidad, restante_total)
                df_tab.at[idx, "PAGO COMISION"] = usar
                restante_total -= usar
            else:
                df_tab.at[idx, "PAGO COMISION"] = 0.0

        # 3️⃣ Repartir el resto (cuando ya no hay PAGO BANCO)
        if restante_total > 0:
            if apartado <= 0:
                tope = restante_total
            else:
                cuotas_min_con_tope_apartado = math.ceil(restante_total / apartado)
                tope = apartado if cuotas_min_con_tope_apartado <= 6 else 1.5 * apartado

            sin_banco_idx = [i for i in df_tab.index if float(df_tab.at[i, "PAGO BANCO"]) <= 0.0]
            cuotas_req = max(1, math.ceil(restante_total / (tope if tope > 0 else restante_total)))
            faltantes = max(0, cuotas_req - len(sin_banco_idx))

            # Si faltan filas, agregarlas automáticamente
            if faltantes > 0:
                if df_tab["FECHA"].notna().any():
                    last_date = pd.to_datetime(df_tab["FECHA"].max()).normalize()
                else:
                    last_date = pd.Timestamp.today().normalize()

                start_n = int(df_tab["N"].max()) + 1 if not df_tab.empty else 1
                nuevas_filas = []
                for k in range(faltantes):
                    ndx = start_n + k
                    nueva_fecha = end_of_month(last_date + pd.DateOffset(months=k + 1)).date()
                    nuevas_filas.append({
                        "N": ndx,
                        "FECHA": nueva_fecha,
                        "PAGO BANCO": 0.0,
                        "PAGO COMISION": 0.0
                    })
                df_tab = pd.concat([df_tab, pd.DataFrame(nuevas_filas)], ignore_index=True)
                sin_banco_idx = [i for i in df_tab.index if float(df_tab.at[i, "PAGO BANCO"]) <= 0.0]

            # Repartir restante en filas sin banco
            for i in sin_banco_idx:
                if restante_total <= 0:
                    break
                cuota = min(tope if tope > 0 else restante_total, restante_total)
                df_tab.at[i, "PAGO COMISION"] = cuota
                restante_total -= cuota

            # Ajuste final (cerrar exacto)
            suma_actual = float(df_tab["PAGO COMISION"].sum())
            diff_final = com_exito - suma_actual
            if abs(diff_final) >= 0.5:
                ult_idx = sin_banco_idx[-1] if len(sin_banco_idx) > 0 else df_tab.index[-1]
                df_tab.at[ult_idx, "PAGO COMISION"] += diff_final

        # ✅ Redondeo estético y guardado
        df_tab["PAGO COMISION"] = df_tab["PAGO COMISION"].round(0)
        st.session_state["pab_table"] = df_tab
        st.success("✅ Recomendación de PAGO COMISIÓN aplicada correctamente.")

        # =========================
        # 📊 Mostrar resultado sin errores
        # =========================
        df_show = df_tab.copy()
        df_show["FECHA"] = df_show["FECHA"].apply(
            lambda x: x.strftime("%Y-%m-%d") if pd.notna(x) else ""
        )

        st.dataframe(df_show, use_container_width=True)

        # 🎯 Control resumen
        total_com_rec = float(df_tab["PAGO COMISION"].sum())
        st.caption(
            f"🎯 Control comisión: CE inicial = {ce_ini:,.0f} | Comisión de éxito = {com_exito:,.0f} | "
            f"Total comisión en tabla = {total_com_rec:,.0f}"
        )
