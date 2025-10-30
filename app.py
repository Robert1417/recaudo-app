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
# 5) 📅 Plan de pagos sugerido (solo N, FECHA, PAGO BANCO, PAGO COMISION)
#    - Fila 1: PAGO COMISION = CE inicial (respetando Apartado)
#    - PAGO BANCO y PAGO COMISION pueden coexistir el mismo mes
#    - La suma mensual no puede exceder el Apartado (si Apartado > 0)
#    - El resto de la comisión se reparte en cuotas IGUALES y en el mínimo # de meses
# =========================
st.markdown("### 5) 📅 Plan de pagos sugerido")

# Validaciones básicas
if pago_banco < 0 or n_pab < 1 or comision_exito < 0:
    st.warning("Revisa: PAGO BANCO (≥0), N PaB (≥1) y Comisión de éxito (≥0).")
    st.stop()

# ---------- utilidades ----------
import math
from datetime import date
import pandas as pd

def end_of_month(ts: pd.Timestamp) -> pd.Timestamp:
    return (ts + pd.offsets.MonthEnd(0)).normalize()

def cap_mes(apartado, pago_banco_mes):
    """Capacidad disponible para comisión en un mes."""
    if apartado is None or apartado <= 0:
        # Sin tope efectivo si no se definió Apartado positivo
        return float("inf")
    return max(0.0, float(apartado) - float(pago_banco_mes))

# ---------- 1) Crear los meses base con PAGO BANCO dividido en N PaB ----------
hoy = pd.Timestamp.today().normalize()
fechas = []
pagos_banco = []

if n_pab == 1:
    pagos_banco = [float(pago_banco)]
    fechas = [hoy]
else:
    cuota_banco = float(pago_banco) / int(n_pab)
    pagos_banco = [cuota_banco] * int(n_pab)
    fechas = [hoy + pd.DateOffset(months=i) for i in range(int(n_pab))]

# Ajuste por redondeos del banco (a 0 decimales)
pagos_banco = [round(x, 0) for x in pagos_banco]
dif_bco = float(pago_banco) - sum(pagos_banco)
if abs(dif_bco) >= 0.5:
    pagos_banco[-1] += dif_bco  # corrige en la última cuota

# ---------- 2) Primera fila: CE inicial como PAGO COMISION (respetando tope mensual) ----------
# Aseguramos al menos una fila (si N PaB==0, igual generamos primera fecha hoy)
if len(fechas) == 0:
    fechas = [hoy]
    pagos_banco = [0.0]

cap_1 = cap_mes(apartado_edit, pagos_banco[0])
ce_inicial_val = float(ce_inicial or 0.0)
pago_comision_f1 = min(ce_inicial_val, cap_1)

# Comisión restante a repartir en cuotas iguales
restante = max(0.0, float(comision_exito) - pago_comision_f1)

# ---------- 3) Construir vector de capacidades mensuales (incluye meses sin banco) ----------
# capacidades[i] corresponde al mes i (0 = primera fila)
capacidades = [cap_1]  # mes 1
# meses siguientes ya existentes (con banco si i < n_pab; sin banco si i >= n_pab)
# iremos extendiendo dinámicamente según lo que se necesite

def capacidad_del_mes(idx):
    """Capacidad del mes idx (0-based). Extiende fechas/pagos si hace falta."""
    nonlocal fechas, pagos_banco
    while idx >= len(fechas):
        # Agregar mes al final (sin pago banco)
        next_date = fechas[-1] + pd.DateOffset(months=1)
        fechas.append(next_date)
        pagos_banco.append(0.0)
    return cap_mes(apartado_edit, pagos_banco[idx])

# ya tenemos mes 0; precargamos restantes existentes
for i in range(1, len(fechas)):
    capacidades.append(cap_mes(apartado_edit, pagos_banco[i]))

# ---------- 4) Encontrar el mínimo # de meses (k) con CUOTAS IGUALES ----------
# Queremos el menor k >= 0 (sobre meses 1,2,3,...) tal que:
#   cuota = ceil(restante / k)  (o redondeo a 0 decimales luego)
#   y cuota <= capacidad de cada uno de esos k meses.
# Nota: si restante == 0 -> k = 0

k = 0
cuota_igual = 0.0

if restante > 0:
    # Empezamos probando k = 1,2,3,... extendiendo meses si hace falta
    k = 1
    while True:
        # asegurar que tenemos capacidad para k meses (meses 1..k)
        while len(capacidades) < (k + 1):
            capacidades.append(capacidad_del_mes(len(capacidades)))

        # cuota tentativa (igual e entera a 0 decimales al final)
        cuota_tent = math.ceil(restante / k)  # entera hacia arriba para garantizar cubrir
        # Verificar que cabe en los k meses (1..k)
        cabe = True
        for m in range(1, k + 1):
            if cuota_tent > capacidades[m]:
                cabe = False
                break
        if cabe:
            cuota_igual = float(cuota_tent)
            break
        k += 1

# ---------- 5) Construir tabla resultante ----------
N_total = max(len(fechas), 1 + k)  # al menos meses para comisión igualitaria
# ampliar fechas/pagos si hace falta
while len(fechas) < N_total:
    fechas.append(fechas[-1] + pd.DateOffset(months=1))
    pagos_banco.append(0.0)
    capacidades.append(cap_mes(apartado_edit, pagos_banco[-1]))

pago_comision = [0.0] * N_total
# Fila 1: CE inicial (ajustada al tope)
pago_comision[0] = round(pago_comision_f1, 0)

# Colocar las k cuotas iguales (meses 1..k)
for m in range(1, 1 + k):
    pago_comision[m] = cuota_igual

# Ajuste por redondeos de comisión (que sume exacto a comision_exito)
dif_com = float(comision_exito) - sum(pago_comision)
if abs(dif_com) >= 0.5:
    # buscar el último mes con comisión > 0 y ajustar allí
    idxs = [i for i, v in enumerate(pago_comision) if v > 0]
    if idxs:
        pago_comision[idxs[-1]] += dif_com
    else:
        # Si no hubo comisiones >0 (caso raro), ajustar en la primera fila
        pago_comision[0] += dif_com

# Seguridad: no exceder tope mensual (si Apartado > 0). Si por ajuste lo excede,
# empujamos el excedente al mes siguiente (y extendemos si hace falta).
if apartado_edit and apartado_edit > 0:
    i = 0
    while i < len(pago_comision):
        total_mes = float(pagos_banco[i]) + float(pago_comision[i])
        if total_mes > float(apartado_edit) + 0.1:  # tolerancia mínima
            exced = total_mes - float(apartado_edit)
            # reducir comisión del mes i y pasar excedente al siguiente mes
            reducible = min(exced, pago_comision[i])
            pago_comision[i] -= reducible
            exced -= reducible
            # si aún queda excedente, no puede venir del banco (es fijo),
            # así que lo empujamos 100% como comisión al mes siguiente
            if exced > 0.1:
                if i + 1 >= len(pago_comision):
                    # agregar un mes nuevo
                    fechas.append(fechas[-1] + pd.DateOffset(months=1))
                    pagos_banco.append(0.0)
                    pago_comision.append(0.0)
                pago_comision[i + 1] += exced
                # volver a verificar siguiente mes (por si se pasó)
                # no incrementamos i para revalidar este mismo índice con el nuevo valor
                continue
        i += 1

# ---------- 6) DataFrame final (solo 4 columnas) ----------
df_plan = pd.DataFrame({
    "N": list(range(1, len(fechas) + 1)),
    "FECHA": [pd.to_datetime(f).strftime("%Y-%m-%d") for f in fechas],  # string para evitar errores Arrow
    "PAGO BANCO": [round(x, 0) for x in pagos_banco],
    "PAGO COMISION": [round(x, 0) for x in pago_comision],
})

# Mostrar
st.dataframe(
    df_plan.style.format({"PAGO BANCO": "{:,.0f}", "PAGO COMISION": "{:,.0f}"}),
    use_container_width=True
)

# Totales
st.markdown(
    f"**Totales:**  🏦 Banco = `{sum(df_plan['PAGO BANCO']):,.0f}`  •  💼 Comisión = `{sum(df_plan['PAGO COMISION']):,.0f}`  •  📊 Filas = `{len(df_plan):,}`"
)

# Descargar
csv = df_plan.to_csv(index=False, encoding="utf-8-sig")
st.download_button(
    "⬇️ Descargar tabla (CSV)",
    data=csv,
    file_name=f"plan_pagos_{ref_input}.csv",
    mime="text/csv"
)
