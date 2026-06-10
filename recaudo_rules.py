"""Reglas de negocio compartidas para ajustar la predicción de recaudo."""

LOW_RATIO_PP_LIMIT = 0.10
LOW_RATIO_PP_PREDICTION_CAP = 0.75
LOW_RATIO_PP_WARNING = "El primer pago de comisión muy bajo, por favor pide más deposito."


def apply_low_ratio_pp_cap(prediction: float, ratio_pp: float) -> tuple[float, bool]:
    """Limita la predicción a 0.75 cuando Ratio_PP es <= 0.10.

    Devuelve la predicción ajustada y un indicador que permite mostrar la
    advertencia únicamente cuando fue necesario reducir la predicción.
    """
    prediction = float(prediction)
    ratio_pp = float(ratio_pp)
    cap_applied = ratio_pp <= LOW_RATIO_PP_LIMIT and prediction > LOW_RATIO_PP_PREDICTION_CAP
    return (LOW_RATIO_PP_PREDICTION_CAP, True) if cap_applied else (prediction, False)
