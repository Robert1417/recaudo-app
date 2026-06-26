"""Módulo aislado para la carga masiva de predicciones independientes.

Este archivo mantiene separado el flujo masivo para no alterar la lógica manual,
de archivo unitario, endpoint ni envío a aprobación de `app_prediccion_independiente.py`.
"""
from datetime import datetime
from io import BytesIO
from typing import Callable

import pandas as pd
import streamlit as st

BULK_INPUT_COLUMNS = [
    "referencia",
    "ids",
    "bancos",
    "tipo_liquidacion",
    "pri_ult",
    "ratio_pp",
    "c_a",
    "amount_total",
    "pago_banco",
    "primer_pago",
    "ce_inicial",
]
BULK_OUTPUT_PREDICTION_COLUMN = "predicción"
BULK_OUTPUT_APPROVED_COLUMN = "aprobado"


def build_template_df() -> pd.DataFrame:
    """Devuelve la plantilla exacta que espera la carga masiva."""
    return pd.DataFrame(columns=BULK_INPUT_COLUMNS)


def dataframe_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "predicciones") -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return output.getvalue()


def read_prediction_file(uploaded_file) -> pd.DataFrame:
    name = str(getattr(uploaded_file, "name", "") or "").lower()
    if name.endswith(".csv"):
        return pd.read_csv(uploaded_file)
    if name.endswith(".xlsx"):
        return pd.read_excel(uploaded_file)
    raise ValueError("Formato no soportado. Sube un archivo CSV o Excel (.xlsx).")


def run_bulk_predictions(
    df_input: pd.DataFrame,
    *,
    cartera_df: pd.DataFrame | None,
    load_model: Callable,
    extract_case_data: Callable,
    predict_recaudo_result: Callable,
    is_traditional_liquidation: Callable,
    resolver_tipo_liquidacion: Callable,
) -> tuple[pd.DataFrame, dict]:
    """Calcula predicciones masivas sin enviar casos a aprobación ni guardar histórico."""
    if df_input is None or df_input.empty:
        raise ValueError("El archivo masivo no contiene filas para procesar.")

    df_result = df_input.copy()
    missing_cols = [col for col in BULK_INPUT_COLUMNS if col not in df_result.columns]
    if missing_cols:
        raise ValueError("Faltan columnas requeridas en el archivo: " + ", ".join(missing_cols))

    model = load_model()
    total_ok = 0
    errors: list[str] = []
    predictions: list[float | None] = []
    approved_flags: list[bool | None] = []
    error_values: list[str] = []

    for row_number, row in enumerate(df_result.to_dict(orient="records"), start=2):
        case = extract_case_data(row)
        try:
            referencia = str(case.get("referencia", "")).strip()
            if not referencia:
                raise ValueError("referencia vacía")

            tipo_liquidacion = str(case.get("tipo_liquidacion", "")).strip()
            if not tipo_liquidacion:
                tipo_liquidacion = resolver_tipo_liquidacion(cartera_df, referencia) if cartera_df is not None else ""
            if not tipo_liquidacion:
                tipo_liquidacion = "Tradicional"

            features = {
                "PRI-ULT": float(case["pri_ult"]),
                "Ratio_PP": float(case["ratio_pp"]),
                "C/A": float(case["c_a"]),
                "AMOUNT_TOTAL": float(case["amount_total"]),
            }
            pred, _low_ratio_cap_applied = predict_recaudo_result(model, features)
            umbral = 0.8 if is_traditional_liquidation(tipo_liquidacion) else 0.74
            aprobado = float(pred) >= float(umbral)

            predictions.append(round(float(pred), 4))
            approved_flags.append(bool(aprobado))
            error_values.append("")
            total_ok += 1
        except Exception as exc:
            msg = f"Fila {row_number}: {exc}"
            errors.append(msg)
            predictions.append(None)
            approved_flags.append(None)
            error_values.append(msg)

    df_result[BULK_OUTPUT_PREDICTION_COLUMN] = predictions
    df_result[BULK_OUTPUT_APPROVED_COLUMN] = approved_flags
    if errors:
        df_result["error_prediccion"] = error_values

    summary = {
        "total": int(len(df_result)),
        "ok": int(total_ok),
        "errors": int(len(errors)),
        "error_details": errors[:20],
    }
    return df_result, summary


def render_bulk_prediction_ui(
    *,
    cartera_df: pd.DataFrame | None,
    load_model: Callable,
    extract_case_data: Callable,
    predict_recaudo_result: Callable,
    is_traditional_liquidation: Callable,
    resolver_tipo_liquidacion: Callable,
) -> None:
    """Renderiza el módulo Streamlit de carga masiva."""
    st.markdown("### Carga masiva de predicciones")
    st.info(
        "Esta opción calcula la predicción para varias filas y **no envía** casos a aprobación. "
        "Descarga la plantilla, diligencia una fila por caso y vuelve a cargar el CSV o Excel."
    )
    template_df = build_template_df()
    c_template_xlsx, c_template_csv = st.columns(2)
    with c_template_xlsx:
        st.download_button(
            "⬇️ Descargar plantilla Excel",
            data=dataframe_to_excel_bytes(template_df, sheet_name="plantilla"),
            file_name="plantilla_prediccion_masiva.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
    with c_template_csv:
        st.download_button(
            "⬇️ Descargar plantilla CSV",
            data=template_df.to_csv(index=False).encode("utf-8-sig"),
            file_name="plantilla_prediccion_masiva.csv",
            mime="text/csv",
            use_container_width=True,
        )

    bulk_file = st.file_uploader(
        "Sube la base masiva diligenciada",
        type=["csv", "xlsx"],
        key="fuente_archivo_masivo",
    )
    if bulk_file is None:
        return

    try:
        df_bulk = read_prediction_file(bulk_file)
        st.caption(f"Filas cargadas: {len(df_bulk):,}")
        st.dataframe(df_bulk.head(20), use_container_width=True)
        if st.button("🔮 Calcular predicciones masivas", type="primary", use_container_width=True):
            df_pred, bulk_summary = run_bulk_predictions(
                df_bulk,
                cartera_df=cartera_df,
                load_model=load_model,
                extract_case_data=extract_case_data,
                predict_recaudo_result=predict_recaudo_result,
                is_traditional_liquidation=is_traditional_liquidation,
                resolver_tipo_liquidacion=resolver_tipo_liquidacion,
            )
            st.success(f"Predicciones calculadas: {bulk_summary['ok']:,} de {bulk_summary['total']:,} filas.")
            if bulk_summary["errors"]:
                st.warning(
                    f"Filas con error: {bulk_summary['errors']:,}. "
                    "El detalle también queda en la columna error_prediccion."
                )
                for detail in bulk_summary["error_details"]:
                    st.caption(detail)
            st.dataframe(df_pred.head(50), use_container_width=True)
            st.download_button(
                "⬇️ Descargar Excel con predicciones",
                data=dataframe_to_excel_bytes(df_pred, sheet_name="predicciones"),
                file_name=f"predicciones_masivas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
    except Exception as exc:
        st.error(f"No se pudo procesar la carga masiva: {exc}")
