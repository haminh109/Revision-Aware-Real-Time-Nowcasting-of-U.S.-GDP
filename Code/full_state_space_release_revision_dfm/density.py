from __future__ import annotations

from dataclasses import dataclass
from math import erf, pi, sqrt

import numpy as np

from full_state_space_release_revision_dfm.kalman_em import LinearGaussianParams, _as_psd


@dataclass(slots=True)
class GaussianForecast:
    mean: np.ndarray
    variance: np.ndarray
    lower: np.ndarray
    upper: np.ndarray


def measurement_forecast_distribution(
    params: LinearGaussianParams,
    state_mean: np.ndarray,
    state_cov: np.ndarray,
    rows: list[int] | np.ndarray,
    *,
    z_value: float = 1.96,
) -> GaussianForecast:
    """Return Gaussian predictive distribution for selected measurement rows."""

    row_array = np.asarray(rows, dtype=int)
    design = params.design[row_array]
    obs_cov = params.obs_cov[np.ix_(row_array, row_array)]
    mean = design @ np.asarray(state_mean, dtype=float)
    cov = design @ _as_psd(state_cov) @ design.T + _as_psd(obs_cov)
    variance = np.clip(np.diag(cov), 1e-10, None)
    sd = np.sqrt(variance)
    return GaussianForecast(
        mean=mean,
        variance=variance,
        lower=mean - z_value * sd,
        upper=mean + z_value * sd,
    )


def gaussian_log_score(y_true: np.ndarray, mean: np.ndarray, variance: np.ndarray) -> np.ndarray:
    """Log predictive density under independent Gaussian marginals."""

    y = np.asarray(y_true, dtype=float)
    mu = np.asarray(mean, dtype=float)
    var = np.clip(np.asarray(variance, dtype=float), 1e-10, None)
    return -0.5 * (np.log(2.0 * pi * var) + ((y - mu) ** 2) / var)


def _standard_normal_pdf(x: np.ndarray) -> np.ndarray:
    return np.exp(-0.5 * x**2) / sqrt(2.0 * pi)


def _standard_normal_cdf(x: np.ndarray) -> np.ndarray:
    vectorized_erf = np.vectorize(erf)
    return 0.5 * (1.0 + vectorized_erf(x / sqrt(2.0)))


def gaussian_crps(y_true: np.ndarray, mean: np.ndarray, variance: np.ndarray) -> np.ndarray:
    """Closed-form CRPS for Gaussian forecasts."""

    y = np.asarray(y_true, dtype=float)
    mu = np.asarray(mean, dtype=float)
    sigma = np.sqrt(np.clip(np.asarray(variance, dtype=float), 1e-10, None))
    z = (y - mu) / sigma
    return sigma * (z * (2.0 * _standard_normal_cdf(z) - 1.0) + 2.0 * _standard_normal_pdf(z) - 1.0 / sqrt(pi))


def interval_coverage(y_true: np.ndarray, lower: np.ndarray, upper: np.ndarray) -> float:
    y = np.asarray(y_true, dtype=float)
    mask = np.isfinite(y) & np.isfinite(lower) & np.isfinite(upper)
    if not mask.any():
        return float("nan")
    return float(np.mean((y[mask] >= lower[mask]) & (y[mask] <= upper[mask])))
