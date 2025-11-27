import streamlit as st
import pandas as pd
import numpy as np
from datetime import date
from pathlib import Path
import json
from joblib import load

# ==== Transformadores CUSTOM (deben estar antes de load) ====
from sklearn.base import BaseEstimator, TransformerMixin

class LogAndDrop(BaseEstimator, TransformerMixin):
    """
    Crea columnas log1p de ['C/A', 'AMOUNT_TOTAL'] y elimina las originales.
    Mantiene ['PRI-ULT','Ratio_PP'] tal cual.
    Salida: ['PRI-ULT','Ratio_PP','C/A_log','AMOUNT_TOTAL_log']
    """
    def __init__(self, ca_col="C/A", amt_col="AMOUNT_TOTAL"):
        self.ca_col = ca_col
        self.amt_col = amt_col
        self.out_feature_names_ = ["PRI-ULT", "Ratio_PP", "C/A_log", "AMOUNT_TOTAL_log"]

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        X = X.copy()
        X["C/A_log"] = np.log1p(pd.to_numeric(X[self.ca_col], errors="coerce").astype(float))
        X["AMOUNT_TOTAL_log"] = np.log1p(pd.to_numeric(X[self.amt_col], errors="coerce").astype(float))
        X = X.drop(columns=[self.ca_col, self.amt_col])
        return X[["PRI-ULT", "Ratio_PP", "C/A_log", "AMOUNT_TOTAL_log"]]

    def get_feature_names_out(self, input_features=None):
        return np.array(self.out_feature_names_)

class Winsorizer(BaseEstimator, TransformerMixin):
    """
    Winsoriza por cuantiles (p_low, p_high) columnas numéricas.
    Aprende límites en fit y los aplica en transform.
    """
    def __init__(self, columns=None, p_low=0.005, p_high=0.005):
        self.columns = columns or []
        self.p_low = p_low
        self.p_high = p_high
        self.lows_ = {}
        self.highs_ = {}

    def fit(self, X, y=None):
        X = pd.DataFrame(X, copy=True)
        for c in self.columns:
            s = pd.to_numeric(X[c], errors="coerce")
            self.lows_[c] = s.quantile(self.p_low)
            self.highs_[c] = s.quantile(1 - self.p_high)
        return self

    def transform(self, X):
        X = pd.DataFrame(X, copy=True)
        for c in self.columns:
            lo = self.lows_[c]
            hi = self.highs_[c]
            X[c] = pd.to_numeric(X[c], errors="coerce").clip(lower=lo, upper=hi)
        return X


# ------------------ Ajustes globales ------------------
pd.set_option("mode.copy_on_write", True)
st.set_page_config(page_title="Calculadora de Recaudo", page_icon="💸", layout="centered")
st.title("💸 Calculadora de Recaudo")

import sklearn, numpy, joblib
st.sidebar.caption(
    f"🧩 NumPy: {numpy.__version__}\n"
    f"🧠 scikit-learn: {sklearn.__version__}\n"
    f"💼 joblib: {joblib.__version__}"
)

st.caption(
    "1) La app carga automáticamente la base generada por el workflow (`data/cartera_asignada_filtrada`) • "
    "2) Escribe la **Referencia** y selecciona **uno o varios Id deuda** • "
    "3) Ajusta valores editables (Deuda, Apartado, Comisión, Saldo) • "
    "4) Ingresa **PAGO BANCO** y **N PaB** → se calcula **DESCUENTO** y **Comisión de éxito** • "
    "6) Revisa KPIs (PLAZO lo ingresas tú)."
)

# ==== Rutas de artefactos generados por el notebook/Action ====
DATA_PARQUET = Path("data/cartera_asignada_filtrada.parquet")
DATA_CSV     = Path("data/cartera_asignada_filtrada.csv")
MODEL_PATH   = Path("mlp_recaudo_pipeline.joblib")
META_PATH    = Path("mlp_recaudo_meta.json")


# ========= Helpers de "versión de archivo" para invalidar cache =========

def _file_version(path: Path) -> str:
    """
    Devuelve una 'firma' del archivo basada en mtime + tamaño.
    Si el archivo no existe, devuelve 'missing'.
    """
    try:
        stat = path.stat()
        return f"{stat.st_mtime_ns}-{stat.st_size}"
    except FileNotFoundError:
        return "missing"

def _model_version() -> str:
    return _file_version(MODEL_PATH)

def _meta_version() -> str:
    return _file_version(META_PATH)

def _data_version() -> str:
    # Si existe parquet, usamos ese; si no, miramos CSV.
    if DATA_PARQUET.exists():
        return _file_version(DATA_PARQUET)
    if DATA_CSV.exists():
        return _file_version(DATA_CSV)
    return "missing"


# =================== Loaders cacheados ===================
@st.cache_resource(show_spinner=False)
def load_model(_version: str):
    """
    Carga el modelo MLP. _version es la firma del archivo y se usa
    solo para invalidar el cache cuando cambie el .joblib.
    """
    if not MODEL_PATH.exists():
        st.error("No encontré el archivo del modelo `mlp_recaudo_pipeline.joblib` en la raíz del repo.")
        st.stop()
    return load(MODEL_PATH)

@st.cache_data(show_spinner=False)
def load_meta(_version: str):
    """
    Carga el JSON de metadata. Se refresca cuando cambie el archivo.
    """
    meta = {}
    if META_PATH.exists():
        try:
            meta = json.loads(META_PATH.read_text(encoding="utf-8"))
        except Exception:
            meta = {}
    return meta

@st.cache_data(show_spinner=False)
def load_repo_base(_version: str) -> pd.DataFrame | None:
    """
    Carga la base que deja el workflow (prefiere Parquet, luego CSV).
    _version se usa solo para invalidar cache cuando cambian los archivos.
    Devuelve None si no existe.
    """
    try:
        if DATA_PARQUET.exists():
            df = pd.read_parquet(DATA_PARQUET)
            return df
        if DATA_CSV.exists():
            return pd.read_csv(DATA_CSV)
        return None
    except Exception:
        # Si algo falla leyendo, permitimos fallback a subida manual
        return None

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

# -------- helpers de CE --------
def _prefill_ce():
    if not st.session_state.get("comision_exito_overridden", False):
        deuda_res = float(st.session_state.get("deuda_res_edit", 0.0) or 0.0)
        pago_bco  = float(st.session_state.get("pago_banco", 0.0) or 0.0)
        ce_base   = float(st.session_state.get("ce_base", 0.0) or 0.0)
        st.session_state.comision_exito = max(0.0, (deuda_res - pago_bco) * 1.19 * ce_base)

def _mark_override_ce():
    st.session_state.comision_exito_overridden = True

# =================== 1) Cargar base ===================
st.markdown("### 1) Base `cartera_asignada_filtrada`")

df_base = load_repo_base(_data_version())
src_badge = None

if df_base is not None:
    src_badge = "📦 Fuente: data/ (workflow semanal)"
    st.success("✅ Cargada automáticamente desde el repo.")
else:
    src_badge = "📤 Fuente: subida manual"
    st.info("No encontré la base en `data/`. Sube un CSV/XLSX como respaldo.")
    up = st.file_uploader("📂 Subir `cartera_asignada_filtrada`", type=["csv", "xlsx"])
    if not up:
        st.stop()
    try:
        df_base = _read_file(up)
    except Exception as e:
        st.error(f"No pude leer el archivo: {e}")
        st.stop()

st.caption(src_badge)

# Mapear columnas obligatorias
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
st.success(f"✅ Base lista • filas: {len(df_base):,}")

# =================== 2) Referencia → seleccionar id(s) ===================
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

# =================== 3) Valores base (reactivo) ===================
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

    st.session_state.deuda_res_edit = deuda_res_total_def
    st.session_state.comision_m_edit = comision_m_base_def
    st.session_state.apartado_edit   = apartado_base_def
    st.session_state.saldo_edit      = saldo_base_def
    st.session_state.ce_base         = ce_base_def

    st.session_state.pago_banco      = 0.0
    st.session_state.n_pab           = 1
    st.session_state.primer_pago_banco = 0.0

    st.session_state.comision_exito_overridden = False
    st.session_state.comision_exito  = max(0.0, (deuda_res_total_def - 0.0) * 1.19 * ce_base_def)

    st.session_state.ce_inicial_val  = 0.0

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

# =================== 4) PAGO BANCO y derivados ===================
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

# Campo adicional: Primer PAGO BANCO solo si N PaB > 1
if st.session_state.n_pab > 1:
    pago_banco_actual = float(st.session_state.pago_banco or 0.0)
    # default: lo que ya haya, o pago_banco / n_pab si no hay nada
    primer_default = st.session_state.get(
        "primer_pago_banco",
        (pago_banco_actual / st.session_state.n_pab) if (pago_banco_actual > 0 and st.session_state.n_pab > 0) else 0.0
    )
    # asegurar que el default no supere el total
    primer_default = min(max(primer_default, 0.0), pago_banco_actual)

    st.number_input(
        "💳 Primer PAGO BANCO",
        min_value=0.0,
        max_value=float(pago_banco_actual),
        step=1000.0,
        value=float(primer_default),
        format="%.0f",
        key="primer_pago_banco",
        help="Monto del primer pago al banco (el resto se reparte en los siguientes PaB)."
    )
else:
    # Si solo hay un PaB, el primer pago es todo el PAGO BANCO
    st.session_state.primer_pago_banco = float(st.session_state.pago_banco or 0.0)

# Detectar cambios en PAGO BANCO / N PaB para recalcular CE (si no está bloqueada)
if (st.session_state._last_pab != st.session_state.pago_banco) or (st.session_state._last_n != st.session_state.n_pab):
    st.session_state._last_pab = st.session_state.pago_banco
    st.session_state._last_n   = st.session_state.n_pab
    _prefill_ce()

# Comisión de éxito (editable) y CE inicial
c4, c5 = st.columns(2)
with c4:
    st.number_input("🏁 Comisión de éxito (editable)", min_value=0.0, step=1000.0,
                    value=float(st.session_state.get("comision_exito", 0.0)),
                    format="%.0f", key="comision_exito", on_change=_mark_override_ce)
with c5:
    st.number_input("🧪 CE inicial", min_value=0.0, step=1000.0,
                    value=float(st.session_state.get("ce_inicial_val", 0.0)),
                    format="%.0f", key="ce_inicial_val")

# Avance CE inicial vs Comisión de éxito
st.markdown("#### Avance de CE inicial sobre la Comisión de éxito")
ce_inicial = float(st.session_state.ce_inicial_val or 0.0)
base = float(st.session_state.comision_exito or 0.0)
if ce_inicial <= 0:
    st.info("Escribe un valor en **CE inicial** para ver el porcentaje.")
elif base <= 0:
    st.warning("La **Comisión de éxito** debe ser mayor a 0 para calcular el porcentaje.")
else:
    porcentaje = (ce_inicial / base) * 100.0
    porcentaje_capped = max(0.0, min(porcentaje, 100.0))
    st.progress(int(round(porcentaje_capped)))
    st.caption(f"CE inicial: {ce_inicial:,.0f}  |  Comisión de éxito: {base:,.0f}  →  **{porcentaje:,.2f}%**")

# =================== 6) Validación y KPIs (sin tabla) ===================
st.markdown("### 6) Validación y KPIs")

pago_banco        = float(st.session_state.get("pago_banco", 0.0) or 0.0)
n_pab             = int(st.session_state.get("n_pab", 1) or 1)
comision_mensual  = float(st.session_state.get("comision_m_edit", 0.0) or 0.0)
apartado_mensual  = float(st.session_state.get("apartado_edit", 0.0) or 0.0)
comision_exito    = float(st.session_state.get("comision_exito", 0.0) or 0.0)
ce_inicial        = float(st.session_state.get("ce_inicial_val", 0.0) or 0.0)

plazo = st.number_input("📅 PLAZO (meses) (lo ingresas tú)", min_value=1, step=1, value=1)

# Primer PAGO BANCO: si hay más de un PaB, usamos el input; si no, todo el pago
primer_pago_banco = float(
    st.session_state.get(
        "primer_pago_banco",
        pago_banco if n_pab == 1 else (pago_banco / n_pab if n_pab > 0 else 0.0)
    )
)
# Clamp por seguridad
primer_pago_banco = min(max(primer_pago_banco, 0.0), pago_banco)
resto_pago_banco = max(0.0, pago_banco - primer_pago_banco)

pct_primer_pago = (ce_inicial / comision_exito) if comision_exito > 0 else np.nan

if (plazo - 1) > 0 and apartado_mensual > 0:
    # Nota: ahora usamos explícitamente el resto del PAGO BANCO
    numerador = (comision_exito + resto_pago_banco - ce_inicial + comision_mensual)
    cuota_apartado = (numerador / (plazo - 1)) / apartado_mensual
else:
    cuota_apartado = np.nan

c1, c2, c3, c4 = st.columns(4)
with c1:
    st.number_input("🏁 Comisión de éxito total", value=float(comision_exito), step=0.0, format="%.0f", disabled=True)
with c2:
    st.number_input("📅 PLAZO (meses)", value=float(plazo), step=1.0, format="%.0f", disabled=True)
with c3:
    st.text_input("% Primer Pago (CE inicial / CE)", value=("—" if np.isnan(pct_primer_pago) else f"{pct_primer_pago:.2f}"), disabled=True)
with c4:
    st.text_input("Cuota/Apartado", value=("—" if np.isnan(cuota_apartado) else f"{cuota_apartado:.4f}"), disabled=True)

if st.session_state.get("comision_exito_overridden", False):
    st.caption("🔒 Comisión de éxito fijada manualmente: no se recalcula con cambios en PAGO BANCO/N PaB.")

# =================== 7) Predicción con el modelo ===================
st.markdown("### 7) Predicción de **recaudo_real** con MLP")

def _to_float_or_nan(x):
    try:
        return float(x)
    except Exception:
        return np.nan

feature_vals = {
    "PRI-ULT": _to_float_or_nan(plazo),
    "Ratio_PP": _to_float_or_nan(pct_primer_pago if not np.isnan(pct_primer_pago) else np.nan),
    "C/A": _to_float_or_nan(cuota_apartado if not np.isnan(cuota_apartado) else np.nan),
    "AMOUNT_TOTAL": _to_float_or_nan(comision_exito),
}

with st.expander("🔎 Ver features que se enviarán al modelo (crudas)", expanded=False):
    st.json(feature_vals)

issues = []
if np.isnan(feature_vals["AMOUNT_TOTAL"]) or feature_vals["AMOUNT_TOTAL"] < 0:
    issues.append("AMOUNT_TOTAL (Comisión de éxito total) no puede ser NaN ni negativa.")
if np.isnan(feature_vals["PRI-ULT"]) or feature_vals["PRI-ULT"] < 1:
    issues.append("PRI-ULT (PLAZO) debe ser un entero ≥ 1.")
if np.isnan(feature_vals["Ratio_PP"]) or feature_vals["Ratio_PP"] < 0:
    issues.append("Ratio_PP (% Primer Pago) no puede ser NaN ni negativo. Usa 0 si aplica.")
if np.isnan(feature_vals["C/A"]) or feature_vals["C/A"] <= 0:
    issues.append("C/A (Cuota/Apartado) debe ser > 0.")

if issues:
    st.warning("⚠️ Revisa antes de predecir:\n- " + "\n- ".join(issues))

col_pred1, col_pred2 = st.columns([1,1])
with col_pred1:
    do_predict = st.button("🔮 Predecir recaudo", type="primary", use_container_width=True)
with col_pred2:
    meta = load_meta(_meta_version())
    if meta:
        st.caption(f"Modelo cargado • target: {meta.get('target','recaudo_real')}")

if do_predict:
    try:
        model = load_model(_model_version())
        FEATURES_RAW = ["PRI-ULT", "Ratio_PP", "C/A", "AMOUNT_TOTAL"]
        X_pred = pd.DataFrame([feature_vals], columns=FEATURES_RAW)
        yhat = float(model.predict(X_pred)[0])

        if yhat == 0.98:
            yhat_adj = yhat + 0.02
        elif yhat == 0.99:
            yhat_adj = yhat + 0.01
        else:
            yhat_adj = yhat + 0.03

        st.success(f"✅ Predicción de recaudo: {yhat_adj:,.2f}")

        st.caption("Entradas usadas por el pipeline (crudas):")
        st.dataframe(pd.DataFrame([feature_vals]), use_container_width=True)

    except Exception as e:
        st.error(f"Error al predecir: {e}")
