from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from full_state_space_release_revision_dfm.kalman_em import (
    KalmanEMConfig,
    KalmanSmootherOutput,
    LinearGaussianParams,
    _as_psd,
    _expected_xx,
    _inverse_and_logdet,
    _stabilize_transition,
    rts_smoother,
)


RELEASE_NAMES = ("A", "S", "T", "M")


@dataclass(slots=True)
class ReleaseRevisionDFMConfig:
    n_factors: int = 1
    max_iter: int = 75
    tolerance: float = 1e-5
    min_variance: float = 1e-5
    enforce_release_order: bool = True
    initialization_seed: int | None = None
    initialization_jitter: float = 0.0
    verbose: bool = False


@dataclass(slots=True)
class ReleaseRevisionDFMResult:
    params: LinearGaussianParams
    smoother: KalmanSmootherOutput
    loglikelihood_history: list[float]
    converged: bool
    n_iter: int
    monthly_names: list[str]
    release_names: list[str]
    n_factors: int
    monthly_mean: np.ndarray
    monthly_scale: np.ndarray
    release_mean: float
    release_scale: float


def _fill_missing_with_column_means(values: np.ndarray) -> np.ndarray:
    filled = np.asarray(values, dtype=float).copy()
    filled = np.where(np.isfinite(filled), filled, np.nan)
    means = np.nanmean(filled, axis=0)
    means = np.where(np.isfinite(means), means, 0.0)
    rows, cols = np.where(~np.isfinite(filled))
    filled[rows, cols] = means[cols]
    filled = np.nan_to_num(filled, nan=0.0, posinf=0.0, neginf=0.0)
    return filled


def _standardize_observed(values: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    array = np.asarray(values, dtype=float)
    array = np.where(np.isfinite(array), array, np.nan)
    means = np.nanmean(array, axis=0)
    stds = np.nanstd(array, axis=0)
    means = np.where(np.isfinite(means), means, 0.0)
    stds = np.where(np.isfinite(stds) & (stds > 1e-8), stds, 1.0)
    with np.errstate(invalid="ignore", divide="ignore", over="ignore"):
        standardized = (array - means) / stds
    standardized = np.where(np.isfinite(standardized), standardized, np.nan)
    return standardized, means, stds


def _standardize_release_panel(values: np.ndarray) -> tuple[np.ndarray, float, float]:
    """Standardize all GDP releases on one common scale.

    A/S/T/M are measurements of the same economic unit. A common scale preserves
    the release ladder; column-wise standardization would let each release round
    have a separate artificial unit.
    """

    array = np.asarray(values, dtype=float)
    array = np.where(np.isfinite(array), array, np.nan)
    observed = array[np.isfinite(array)]
    if observed.size == 0:
        return array.copy(), 0.0, 1.0
    mean = float(observed.mean())
    scale = float(observed.std(ddof=0))
    if not np.isfinite(scale) or scale < 1e-8:
        scale = 1.0
    with np.errstate(invalid="ignore", divide="ignore", over="ignore"):
        standardized = (array - mean) / scale
    standardized = np.where(np.isfinite(standardized), standardized, np.nan)
    return standardized, mean, scale


def _initial_monthly_loadings(monthly: np.ndarray, n_factors: int) -> tuple[np.ndarray, np.ndarray]:
    standardized, _, _ = _standardize_observed(monthly)
    filled = _fill_missing_with_column_means(standardized)
    centered = filled - filled.mean(axis=0, keepdims=True)
    if centered.shape[0] < 3 or centered.shape[1] < 2:
        factors = np.zeros((centered.shape[0], n_factors), dtype=float)
        if centered.shape[1] > 0:
            factors[:, 0] = centered.mean(axis=1)
        loadings = np.zeros((centered.shape[1], n_factors), dtype=float)
        loadings[:, 0] = 1.0
        return factors, loadings
    centered = np.clip(np.nan_to_num(centered, nan=0.0, posinf=0.0, neginf=0.0), -25.0, 25.0)
    with np.errstate(invalid="ignore", divide="ignore", over="ignore"):
        _, singular_values, vh = np.linalg.svd(centered, full_matrices=False)
    k = min(n_factors, vh.shape[0])
    with np.errstate(invalid="ignore", divide="ignore", over="ignore"):
        factors = centered @ vh[:k].T
    factors = np.nan_to_num(factors, nan=0.0, posinf=0.0, neginf=0.0)
    loadings = np.nan_to_num(vh[:k].T, nan=0.0, posinf=0.0, neginf=0.0)
    if k < n_factors:
        factors = np.column_stack([factors, np.zeros((centered.shape[0], n_factors - k))])
        loadings = np.column_stack([loadings, np.zeros((centered.shape[1], n_factors - k))])
    scale = np.std(factors, axis=0)
    scale = np.where(scale > 1e-8, scale, 1.0)
    factors = factors / scale
    loadings = loadings * scale
    _ = singular_values
    return factors, loadings


def _initial_params(monthly: np.ndarray, releases: np.ndarray, config: ReleaseRevisionDFMConfig) -> LinearGaussianParams:
    n_obs, n_monthly = monthly.shape
    n_release = len(RELEASE_NAMES)
    n_state = config.n_factors + 2
    factors, monthly_loadings = _initial_monthly_loadings(monthly, config.n_factors)
    design = np.zeros((n_monthly + n_release, n_state), dtype=float)
    design[:n_monthly, : config.n_factors] = monthly_loadings
    g_idx = config.n_factors
    s_idx = config.n_factors + 1
    design[n_monthly + 0, g_idx] = 1.0
    design[n_monthly + 0, s_idx] = 1.0
    design[n_monthly + 1, g_idx] = 1.0
    design[n_monthly + 1, s_idx] = 0.55
    design[n_monthly + 2, g_idx] = 1.0
    design[n_monthly + 2, s_idx] = 0.25
    design[n_monthly + 3, g_idx] = 1.0
    design[n_monthly + 3, s_idx] = 0.0

    transition = np.eye(n_state, dtype=float) * 0.55
    if n_obs > 3 and config.n_factors > 0:
        for j in range(config.n_factors):
            x = factors[:-1, j]
            y = factors[1:, j]
            denom = float(x @ x)
            if denom > 1e-8:
                transition[j, j] = float(np.clip((x @ y) / denom, -0.95, 0.95))
    transition[g_idx, : config.n_factors] = 0.15 / max(config.n_factors, 1)
    transition[s_idx, : config.n_factors] = 0.05 / max(config.n_factors, 1)

    combined = np.column_stack([monthly, releases])
    obs_var = np.nanvar(combined, axis=0)
    obs_var = np.where(np.isfinite(obs_var) & (obs_var > config.min_variance), obs_var, 1.0)
    state_cov = np.eye(n_state, dtype=float) * 0.25
    initial_state = np.zeros(n_state, dtype=float)
    if factors.size:
        initial_state[: config.n_factors] = factors[0, : config.n_factors]
    params = LinearGaussianParams(
        transition=_stabilize_transition(transition),
        state_intercept=np.zeros(n_state, dtype=float),
        state_cov=state_cov,
        design=design,
        obs_cov=np.diag(obs_var),
        initial_state=initial_state,
        initial_cov=np.eye(n_state, dtype=float) * 5.0,
    )
    return _jitter_initial_params(params, config)


def _jitter_initial_params(params: LinearGaussianParams, config: ReleaseRevisionDFMConfig) -> LinearGaussianParams:
    if config.initialization_seed is None or config.initialization_jitter <= 0.0:
        return params
    rng = np.random.default_rng(config.initialization_seed)
    scale = float(config.initialization_jitter)
    out = params.copy()
    design_mask = np.abs(out.design) > 1e-12
    transition_mask = np.abs(out.transition) > 1e-12
    out.design = out.design + design_mask * rng.normal(0.0, scale, size=out.design.shape)
    out.transition = _stabilize_transition(out.transition + transition_mask * rng.normal(0.0, scale, size=out.transition.shape))
    out.initial_state = out.initial_state + rng.normal(0.0, scale, size=out.initial_state.shape)
    state_scale = np.exp(rng.normal(0.0, scale, size=out.state_cov.shape[0]))
    obs_scale = np.exp(rng.normal(0.0, scale, size=out.obs_cov.shape[0]))
    out.state_cov = _as_psd(out.state_cov * np.outer(state_scale, state_scale), ridge=config.min_variance)
    out.obs_cov = _as_psd(out.obs_cov * np.outer(obs_scale, obs_scale), ridge=config.min_variance)
    return out


def _transition_m_step(
    smoother: KalmanSmootherOutput,
    params: LinearGaussianParams,
    config: ReleaseRevisionDFMConfig,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    n_obs, n_state = smoother.smoothed_state.shape
    s_xx = np.zeros((n_state, n_state), dtype=float)
    s_xz = np.zeros((n_state, n_state + 1), dtype=float)
    s_zz = np.zeros((n_state + 1, n_state + 1), dtype=float)
    for t in range(1, n_obs):
        x_t = smoother.smoothed_state[t]
        x_prev = smoother.smoothed_state[t - 1]
        exx_t = _expected_xx(smoother, t)
        exx_prev = _expected_xx(smoother, t - 1)
        exx_cross = smoother.lag_one_cov[t] + np.outer(x_t, x_prev)
        ezz = np.zeros((n_state + 1, n_state + 1), dtype=float)
        ezz[0, 0] = 1.0
        ezz[0, 1:] = x_prev
        ezz[1:, 0] = x_prev
        ezz[1:, 1:] = exx_prev
        exz = np.column_stack([x_t, exx_cross])
        s_xx += exx_t
        s_xz += exz
        s_zz += ezz
    s_zz_inv, _ = _inverse_and_logdet(s_zz)
    block = s_xz @ s_zz_inv
    intercept = block[:, 0]
    transition = _stabilize_transition(block[:, 1:])
    state_cov = (s_xx - block @ s_xz.T) / max(n_obs - 1, 1)
    return transition, intercept, _as_psd(state_cov, ridge=config.min_variance)


def _restricted_design_m_step(
    y: np.ndarray,
    n_monthly: int,
    smoother: KalmanSmootherOutput,
    params: LinearGaussianParams,
    config: ReleaseRevisionDFMConfig,
) -> tuple[np.ndarray, np.ndarray]:
    observations = np.asarray(y, dtype=float)
    n_series = observations.shape[1]
    n_state = params.initial_state.shape[0]
    factor_slice = slice(0, config.n_factors)
    g_idx = config.n_factors
    s_idx = config.n_factors + 1
    design = np.zeros((n_series, n_state), dtype=float)
    obs_var = np.zeros((n_series, n_series), dtype=float)

    for i in range(n_monthly):
        observed = np.isfinite(observations[:, i])
        if observed.sum() == 0:
            design[i, factor_slice] = params.design[i, factor_slice]
            obs_var[i, i] = max(float(params.obs_cov[i, i]), config.min_variance)
            continue
        s_yx = np.zeros(config.n_factors, dtype=float)
        s_xx = np.zeros((config.n_factors, config.n_factors), dtype=float)
        for t in np.flatnonzero(observed):
            state = smoother.smoothed_state[t, factor_slice]
            cov = smoother.smoothed_cov[t, factor_slice, factor_slice]
            s_yx += observations[t, i] * state
            s_xx += cov + np.outer(state, state)
        inv, _ = _inverse_and_logdet(s_xx)
        design[i, factor_slice] = s_yx @ inv
        obs_var[i, i] = _series_residual_variance(observations[:, i], design[i], smoother, config.min_variance)

    release_rows = {
        "A": (n_monthly + 0, 1.0),
        "S": (n_monthly + 1, None),
        "T": (n_monthly + 2, None),
        "M": (n_monthly + 3, 0.0),
    }
    design[n_monthly + 0, [g_idx, s_idx]] = [1.0, 1.0]
    design[n_monthly + 3, [g_idx, s_idx]] = [1.0, 0.0]
    psi_s = _estimate_revision_loading(observations[:, n_monthly + 1], smoother, g_idx, s_idx, default=0.55)
    psi_t = _estimate_revision_loading(observations[:, n_monthly + 2], smoother, g_idx, s_idx, default=0.25)
    if config.enforce_release_order:
        psi_s = float(np.clip(psi_s, 0.05, 0.98))
        psi_t = float(np.clip(psi_t, 0.02, psi_s))
    design[n_monthly + 1, [g_idx, s_idx]] = [1.0, psi_s]
    design[n_monthly + 2, [g_idx, s_idx]] = [1.0, psi_t]

    for _, (row, _) in release_rows.items():
        obs_var[row, row] = _series_residual_variance(observations[:, row], design[row], smoother, config.min_variance)

    return design, obs_var


def _estimate_revision_loading(
    y: np.ndarray,
    smoother: KalmanSmootherOutput,
    g_idx: int,
    s_idx: int,
    *,
    default: float,
) -> float:
    observed = np.isfinite(y)
    if observed.sum() == 0:
        return default
    numerator = 0.0
    denominator = 0.0
    for t in np.flatnonzero(observed):
        g_mean = smoother.smoothed_state[t, g_idx]
        s_mean = smoother.smoothed_state[t, s_idx]
        s_var = smoother.smoothed_cov[t, s_idx, s_idx] + s_mean**2
        numerator += (y[t] - g_mean) * s_mean
        denominator += s_var
    if denominator <= 1e-10:
        return default
    return float(numerator / denominator)


def _series_residual_variance(
    y: np.ndarray,
    h: np.ndarray,
    smoother: KalmanSmootherOutput,
    min_variance: float,
) -> float:
    observed = np.isfinite(y)
    if observed.sum() == 0:
        return 1.0
    total = 0.0
    for t in np.flatnonzero(observed):
        exx = _expected_xx(smoother, t)
        total += y[t] ** 2 - 2.0 * y[t] * h @ smoother.smoothed_state[t] + h @ exx @ h.T
    return max(float(total / observed.sum()), min_variance)


def _release_revision_em_step(
    y: np.ndarray,
    n_monthly: int,
    smoother: KalmanSmootherOutput,
    params: LinearGaussianParams,
    config: ReleaseRevisionDFMConfig,
) -> LinearGaussianParams:
    transition, intercept, state_cov = _transition_m_step(smoother, params, config)
    design, obs_cov = _restricted_design_m_step(y, n_monthly, smoother, params, config)
    return LinearGaussianParams(
        transition=_stabilize_transition(transition),
        state_intercept=intercept,
        state_cov=state_cov,
        design=design,
        obs_cov=obs_cov,
        initial_state=smoother.smoothed_state[0],
        initial_cov=_as_psd(smoother.smoothed_cov[0], ridge=config.min_variance),
    )


def fit_release_revision_dfm(
    monthly_panel: pd.DataFrame | np.ndarray,
    release_panel: pd.DataFrame | np.ndarray,
    *,
    config: ReleaseRevisionDFMConfig | None = None,
    monthly_names: list[str] | None = None,
) -> ReleaseRevisionDFMResult:
    """Fit a joint release/revision DFM by Kalman filter, RTS smoother, and EM.

    Inputs must be aligned on the same time index. Monthly indicators occupy the
    first block of the measurement vector; GDP release observations A/S/T/M occupy
    the final four columns. Missing values must be np.nan.
    """

    config = config or ReleaseRevisionDFMConfig()
    monthly = np.asarray(monthly_panel, dtype=float)
    releases = np.asarray(release_panel, dtype=float)
    if releases.shape[1] != len(RELEASE_NAMES):
        raise ValueError("release_panel must have four columns ordered as A, S, T, M")
    if monthly.shape[0] != releases.shape[0]:
        raise ValueError("monthly_panel and release_panel must have the same number of rows")
    monthly_standardized, monthly_mean, monthly_scale = _standardize_observed(monthly)
    releases_standardized, release_mean, release_scale = _standardize_release_panel(releases)
    y = np.column_stack([monthly_standardized, releases_standardized])
    n_monthly = monthly.shape[1]
    params = _initial_params(monthly_standardized, releases_standardized, config)
    em_config = KalmanEMConfig(
        max_iter=config.max_iter,
        tolerance=config.tolerance,
        min_variance=config.min_variance,
        diagonal_obs_cov=True,
        verbose=config.verbose,
    )
    loglikelihood_history: list[float] = []
    converged = False
    iterations_run = 0

    for iteration in range(em_config.max_iter):
        iterations_run = iteration + 1
        smoother = rts_smoother(y, params)
        loglikelihood_history.append(float(smoother.loglikelihood))
        if config.verbose:
            print(f"release-revision EM iter {iteration + 1}: llf={smoother.loglikelihood:.6f}")
        if len(loglikelihood_history) >= 2:
            improvement = loglikelihood_history[-1] - loglikelihood_history[-2]
            if abs(improvement) <= config.tolerance * (1.0 + abs(loglikelihood_history[-2])):
                converged = True
                break
        params = _release_revision_em_step(y, n_monthly, smoother, params, config)

    final_smoother = rts_smoother(y, params)
    if not loglikelihood_history or final_smoother.loglikelihood != loglikelihood_history[-1]:
        loglikelihood_history.append(float(final_smoother.loglikelihood))
    if isinstance(monthly_panel, pd.DataFrame):
        inferred_monthly_names = list(monthly_panel.columns)
    else:
        inferred_monthly_names = monthly_names or [f"indicator_{i + 1}" for i in range(n_monthly)]
    return ReleaseRevisionDFMResult(
        params=params,
        smoother=final_smoother,
        loglikelihood_history=loglikelihood_history,
        converged=converged,
        n_iter=iterations_run,
        monthly_names=inferred_monthly_names,
        release_names=list(RELEASE_NAMES),
        n_factors=config.n_factors,
        monthly_mean=monthly_mean,
        monthly_scale=monthly_scale,
        release_mean=release_mean,
        release_scale=release_scale,
    )


def forecast_release_row(
    result: ReleaseRevisionDFMResult,
    state: np.ndarray | None = None,
    *,
    original_scale: bool = True,
) -> dict[str, float]:
    """Return A/S/T/M fitted values from the latest smoothed state by default."""

    state_vector = result.smoother.smoothed_state[-1] if state is None else np.asarray(state, dtype=float)
    n_monthly = len(result.monthly_names)
    release_design = result.params.design[n_monthly : n_monthly + len(RELEASE_NAMES)]
    forecasts = release_design @ state_vector
    if original_scale:
        forecasts = forecasts * result.release_scale + result.release_mean
    return {name: float(value) for name, value in zip(RELEASE_NAMES, forecasts)}


def forecast_release_moments(
    result: ReleaseRevisionDFMResult,
    state: np.ndarray | None = None,
    state_cov: np.ndarray | None = None,
    *,
    original_scale: bool = True,
) -> tuple[dict[str, float], pd.DataFrame]:
    """Return predictive means and covariance for GDP release measurements.

    The covariance includes uncertainty in the smoothed state at the latest
    forecast-origin row plus the estimated release measurement covariance. This
    is the model-implied Gaussian density for the GDP release block conditional
    on the estimated parameters.
    """

    state_vector = result.smoother.smoothed_state[-1] if state is None else np.asarray(state, dtype=float)
    cov = result.smoother.smoothed_cov[-1] if state_cov is None else np.asarray(state_cov, dtype=float)
    n_monthly = len(result.monthly_names)
    design = result.params.design[n_monthly : n_monthly + len(RELEASE_NAMES)]
    obs_cov = result.params.obs_cov[n_monthly : n_monthly + len(RELEASE_NAMES), n_monthly : n_monthly + len(RELEASE_NAMES)]
    means = design @ state_vector
    covariance = design @ _as_psd(cov) @ design.T + _as_psd(obs_cov)
    if original_scale:
        means = means * result.release_scale + result.release_mean
        covariance = covariance * np.outer(result.release_scale, result.release_scale)
    return (
        {name: float(value) for name, value in zip(RELEASE_NAMES, means)},
        pd.DataFrame(covariance, index=RELEASE_NAMES, columns=RELEASE_NAMES),
    )
