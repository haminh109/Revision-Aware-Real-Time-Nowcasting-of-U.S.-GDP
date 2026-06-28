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
from full_state_space_release_revision_dfm.release_revision_dfm import (
    RELEASE_NAMES,
    _fill_missing_with_column_means,
    _initial_monthly_loadings,
    _standardize_release_panel,
)


@dataclass(slots=True)
class JointIndicatorRevisionDFMConfig:
    n_factors: int = 1
    max_iter: int = 75
    tolerance: float = 1e-5
    min_variance: float = 1e-5
    enforce_release_order: bool = True
    gdp_revision_enabled: bool = True
    indicator_revision_enabled: bool = True
    initialization_seed: int | None = None
    initialization_jitter: float = 0.0
    verbose: bool = False


@dataclass(slots=True)
class JointIndicatorRevisionDFMResult:
    params: LinearGaussianParams
    smoother: KalmanSmootherOutput
    loglikelihood_history: list[float]
    converged: bool
    n_iter: int
    indicator_names: list[str]
    release_names: list[str]
    n_factors: int
    first_mean: np.ndarray
    first_scale: np.ndarray
    mature_mean: np.ndarray
    mature_scale: np.ndarray
    release_mean: float
    release_scale: float
    has_mature_indicators: bool


def _standardize_indicator_pair(
    first: np.ndarray,
    mature: np.ndarray | None,
) -> tuple[np.ndarray, np.ndarray | None, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    first_array = np.asarray(first, dtype=float)
    first_array = np.where(np.isfinite(first_array), first_array, np.nan)
    mature_array = None if mature is None else np.asarray(mature, dtype=float)
    if mature_array is not None:
        mature_array = np.where(np.isfinite(mature_array), mature_array, np.nan)
    if mature_array is not None and mature_array.shape != first_array.shape:
        raise ValueError("monthly_first_panel and monthly_mature_panel must have the same shape")

    if mature_array is None:
        means = np.nanmean(first_array, axis=0)
        scales = np.nanstd(first_array, axis=0)
    else:
        stacked = np.stack([first_array, mature_array], axis=0)
        means = np.nanmean(stacked, axis=(0, 1))
        scales = np.nanstd(stacked, axis=(0, 1))
    means = np.where(np.isfinite(means), means, 0.0)
    scales = np.where(np.isfinite(scales) & (scales > 1e-8), scales, 1.0)
    with np.errstate(invalid="ignore", divide="ignore", over="ignore"):
        first_std = (first_array - means) / scales
        mature_std = None if mature_array is None else (mature_array - means) / scales
    first_std = np.where(np.isfinite(first_std), first_std, np.nan)
    if mature_std is not None:
        mature_std = np.where(np.isfinite(mature_std), mature_std, np.nan)
    return first_std, mature_std, means, scales, means.copy(), scales.copy()


def _initial_joint_params(
    first: np.ndarray,
    mature: np.ndarray | None,
    releases: np.ndarray,
    config: JointIndicatorRevisionDFMConfig,
) -> LinearGaussianParams:
    n_obs, n_indicators = first.shape
    n_release = len(RELEASE_NAMES)
    n_state = config.n_factors + 3
    g_idx = config.n_factors
    gdp_revision_idx = config.n_factors + 1
    indicator_revision_idx = config.n_factors + 2

    factor_source = mature if mature is not None else first
    factors, loadings = _initial_monthly_loadings(factor_source, config.n_factors)
    n_measurements = n_indicators + (n_indicators if mature is not None else 0) + n_release
    design = np.zeros((n_measurements, n_state), dtype=float)
    design[:n_indicators, : config.n_factors] = loadings
    if config.indicator_revision_enabled:
        design[:n_indicators, indicator_revision_idx] = 0.30
    release_start = n_indicators
    if mature is not None:
        design[n_indicators : 2 * n_indicators, : config.n_factors] = loadings
        release_start = 2 * n_indicators

    if config.gdp_revision_enabled:
        design[release_start + 0, [g_idx, gdp_revision_idx]] = [1.0, 1.0]
        design[release_start + 1, [g_idx, gdp_revision_idx]] = [1.0, 0.55]
        design[release_start + 2, [g_idx, gdp_revision_idx]] = [1.0, 0.25]
        design[release_start + 3, [g_idx, gdp_revision_idx]] = [1.0, 0.0]
    else:
        design[release_start : release_start + n_release, g_idx] = 1.0

    transition = np.eye(n_state, dtype=float) * 0.55
    if n_obs > 3 and config.n_factors > 0:
        for j in range(config.n_factors):
            x = factors[:-1, j]
            y = factors[1:, j]
            denom = float(x @ x)
            if denom > 1e-8:
                transition[j, j] = float(np.clip((x @ y) / denom, -0.95, 0.95))
    transition[g_idx, : config.n_factors] = 0.12 / max(config.n_factors, 1)
    if config.gdp_revision_enabled:
        transition[gdp_revision_idx, : config.n_factors] = 0.04 / max(config.n_factors, 1)
    else:
        transition[gdp_revision_idx, :] = 0.0
        transition[gdp_revision_idx, gdp_revision_idx] = 0.10
    if config.indicator_revision_enabled:
        transition[indicator_revision_idx, : config.n_factors] = 0.04 / max(config.n_factors, 1)
    else:
        transition[indicator_revision_idx, :] = 0.0
        transition[indicator_revision_idx, indicator_revision_idx] = 0.10

    measurement = [first]
    if mature is not None:
        measurement.append(mature)
    measurement.append(releases)
    y = np.column_stack(measurement)
    obs_var = np.nanvar(y, axis=0)
    obs_var = np.where(np.isfinite(obs_var) & (obs_var > config.min_variance), obs_var, 1.0)
    initial_state = np.zeros(n_state, dtype=float)
    if factors.size:
        initial_state[: config.n_factors] = factors[0, : config.n_factors]
    params = LinearGaussianParams(
        transition=_stabilize_transition(transition),
        state_intercept=np.zeros(n_state, dtype=float),
        state_cov=np.eye(n_state, dtype=float) * 0.25,
        design=design,
        obs_cov=np.diag(obs_var),
        initial_state=initial_state,
        initial_cov=np.eye(n_state, dtype=float) * 5.0,
    )
    return _jitter_initial_params(params, config)


def _jitter_initial_params(params: LinearGaussianParams, config: JointIndicatorRevisionDFMConfig) -> LinearGaussianParams:
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
    config: JointIndicatorRevisionDFMConfig,
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
    inv, _ = _inverse_and_logdet(s_zz)
    block = s_xz @ inv
    transition = _stabilize_transition(block[:, 1:])
    intercept = block[:, 0]
    state_cov = (s_xx - block @ s_xz.T) / max(n_obs - 1, 1)
    return transition, intercept, _as_psd(state_cov, ridge=config.min_variance)


def _estimate_factor_loading(
    y: np.ndarray,
    smoother: KalmanSmootherOutput,
    factor_slice: slice,
    min_variance: float,
    default: np.ndarray,
) -> np.ndarray:
    observed = np.isfinite(y)
    n_factors = len(default)
    if observed.sum() == 0:
        return default
    s_yx = np.zeros(n_factors, dtype=float)
    s_xx = np.zeros((n_factors, n_factors), dtype=float)
    for t in np.flatnonzero(observed):
        state = smoother.smoothed_state[t, factor_slice]
        cov = smoother.smoothed_cov[t, factor_slice, factor_slice]
        s_yx += y[t] * state
        s_xx += cov + np.outer(state, state)
    inv, _ = _inverse_and_logdet(s_xx, ridge=min_variance)
    return s_yx @ inv


def _estimate_single_state_loading(
    residual: np.ndarray,
    smoother: KalmanSmootherOutput,
    state_idx: int,
    min_variance: float,
    default: float,
) -> float:
    observed = np.isfinite(residual)
    if observed.sum() == 0:
        return default
    numerator = 0.0
    denominator = 0.0
    for t in np.flatnonzero(observed):
        mean = smoother.smoothed_state[t, state_idx]
        second = smoother.smoothed_cov[t, state_idx, state_idx] + mean**2
        numerator += residual[t] * mean
        denominator += second
    if denominator <= min_variance:
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


def _estimate_gdp_revision_loading(
    y: np.ndarray,
    smoother: KalmanSmootherOutput,
    g_idx: int,
    rev_idx: int,
    *,
    default: float,
    min_variance: float,
) -> float:
    observed = np.isfinite(y)
    if observed.sum() == 0:
        return default
    residual = np.full_like(y, np.nan, dtype=float)
    residual[observed] = y[observed] - smoother.smoothed_state[observed, g_idx]
    return _estimate_single_state_loading(residual, smoother, rev_idx, min_variance, default)


def _joint_design_m_step(
    y: np.ndarray,
    n_indicators: int,
    has_mature: bool,
    smoother: KalmanSmootherOutput,
    params: LinearGaussianParams,
    config: JointIndicatorRevisionDFMConfig,
) -> tuple[np.ndarray, np.ndarray]:
    n_series = y.shape[1]
    n_state = params.initial_state.shape[0]
    factor_slice = slice(0, config.n_factors)
    g_idx = config.n_factors
    gdp_revision_idx = config.n_factors + 1
    indicator_revision_idx = config.n_factors + 2
    release_start = n_indicators * (2 if has_mature else 1)
    design = np.zeros((n_series, n_state), dtype=float)
    obs_cov = np.zeros((n_series, n_series), dtype=float)

    for i in range(n_indicators):
        first_row = i
        mature_row = n_indicators + i if has_mature else None
        default_loading = params.design[first_row, factor_slice]
        source_row = mature_row if mature_row is not None else first_row
        factor_loading = _estimate_factor_loading(
            y[:, source_row],
            smoother,
            factor_slice,
            config.min_variance,
            default_loading,
        )
        design[first_row, factor_slice] = factor_loading
        if mature_row is not None:
            design[mature_row, factor_slice] = factor_loading

        first_pred = smoother.smoothed_state[:, factor_slice] @ factor_loading
        first_residual = y[:, first_row] - first_pred
        if config.indicator_revision_enabled:
            gamma = _estimate_single_state_loading(
                first_residual,
                smoother,
                indicator_revision_idx,
                config.min_variance,
                float(params.design[first_row, indicator_revision_idx]),
            )
            design[first_row, indicator_revision_idx] = gamma
        obs_cov[first_row, first_row] = _series_residual_variance(y[:, first_row], design[first_row], smoother, config.min_variance)
        if mature_row is not None:
            obs_cov[mature_row, mature_row] = _series_residual_variance(
                y[:, mature_row],
                design[mature_row],
                smoother,
                config.min_variance,
            )

    if config.gdp_revision_enabled:
        design[release_start + 0, [g_idx, gdp_revision_idx]] = [1.0, 1.0]
        design[release_start + 3, [g_idx, gdp_revision_idx]] = [1.0, 0.0]
        psi_s = _estimate_gdp_revision_loading(
            y[:, release_start + 1],
            smoother,
            g_idx,
            gdp_revision_idx,
            default=float(params.design[release_start + 1, gdp_revision_idx]),
            min_variance=config.min_variance,
        )
        psi_t = _estimate_gdp_revision_loading(
            y[:, release_start + 2],
            smoother,
            g_idx,
            gdp_revision_idx,
            default=float(params.design[release_start + 2, gdp_revision_idx]),
            min_variance=config.min_variance,
        )
        if config.enforce_release_order:
            psi_s = float(np.clip(psi_s, 0.05, 0.98))
            psi_t = float(np.clip(psi_t, 0.02, psi_s))
        design[release_start + 1, [g_idx, gdp_revision_idx]] = [1.0, psi_s]
        design[release_start + 2, [g_idx, gdp_revision_idx]] = [1.0, psi_t]
    else:
        design[release_start : release_start + len(RELEASE_NAMES), g_idx] = 1.0
    for row in range(release_start, release_start + len(RELEASE_NAMES)):
        obs_cov[row, row] = _series_residual_variance(y[:, row], design[row], smoother, config.min_variance)
    return design, obs_cov


def _joint_em_step(
    y: np.ndarray,
    n_indicators: int,
    has_mature: bool,
    smoother: KalmanSmootherOutput,
    params: LinearGaussianParams,
    config: JointIndicatorRevisionDFMConfig,
) -> LinearGaussianParams:
    transition, intercept, state_cov = _transition_m_step(smoother, config)
    design, obs_cov = _joint_design_m_step(y, n_indicators, has_mature, smoother, params, config)
    return LinearGaussianParams(
        transition=transition,
        state_intercept=intercept,
        state_cov=state_cov,
        design=design,
        obs_cov=obs_cov,
        initial_state=smoother.smoothed_state[0],
        initial_cov=_as_psd(smoother.smoothed_cov[0], ridge=config.min_variance),
    )


def fit_joint_indicator_revision_dfm(
    monthly_first_panel: pd.DataFrame | np.ndarray,
    release_panel: pd.DataFrame | np.ndarray,
    *,
    monthly_mature_panel: pd.DataFrame | np.ndarray | None = None,
    config: JointIndicatorRevisionDFMConfig | None = None,
    indicator_names: list[str] | None = None,
) -> JointIndicatorRevisionDFMResult:
    """Fit a joint DFM with GDP-release and monthly-indicator revision states.

    `monthly_first_panel` should contain real-time/early monthly indicator
    observations. `monthly_mature_panel`, when available, should contain later
    vintage values aligned to the same observation periods. Missing observations
    are represented by np.nan.
    """

    config = config or JointIndicatorRevisionDFMConfig()
    first = np.asarray(monthly_first_panel, dtype=float)
    mature = None if monthly_mature_panel is None else np.asarray(monthly_mature_panel, dtype=float)
    releases = np.asarray(release_panel, dtype=float)
    if first.ndim != 2:
        raise ValueError("monthly_first_panel must be 2D")
    if releases.ndim != 2 or releases.shape[1] != len(RELEASE_NAMES):
        raise ValueError("release_panel must have four columns ordered as A, S, T, M")
    if first.shape[0] != releases.shape[0]:
        raise ValueError("monthly_first_panel and release_panel must have the same number of rows")
    if mature is not None and mature.shape != first.shape:
        raise ValueError("monthly_mature_panel must match monthly_first_panel shape")

    first_std, mature_std, first_mean, first_scale, mature_mean, mature_scale = _standardize_indicator_pair(first, mature)
    release_std, release_mean, release_scale = _standardize_release_panel(releases)
    measurement_parts = [first_std]
    if mature_std is not None:
        measurement_parts.append(mature_std)
    measurement_parts.append(release_std)
    y = np.column_stack(measurement_parts)
    params = _initial_joint_params(first_std, mature_std, release_std, config)
    em_config = KalmanEMConfig(
        max_iter=config.max_iter,
        tolerance=config.tolerance,
        min_variance=config.min_variance,
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
            print(f"joint-indicator-revision EM iter {iteration + 1}: llf={smoother.loglikelihood:.6f}")
        if len(loglikelihood_history) >= 2:
            improvement = loglikelihood_history[-1] - loglikelihood_history[-2]
            if abs(improvement) <= config.tolerance * (1.0 + abs(loglikelihood_history[-2])):
                converged = True
                break
        params = _joint_em_step(y, first.shape[1], mature is not None, smoother, params, config)

    final_smoother = rts_smoother(y, params)
    if not loglikelihood_history or final_smoother.loglikelihood != loglikelihood_history[-1]:
        loglikelihood_history.append(float(final_smoother.loglikelihood))

    if isinstance(monthly_first_panel, pd.DataFrame):
        inferred_names = list(monthly_first_panel.columns)
    else:
        inferred_names = indicator_names or [f"indicator_{i + 1}" for i in range(first.shape[1])]

    return JointIndicatorRevisionDFMResult(
        params=params,
        smoother=final_smoother,
        loglikelihood_history=loglikelihood_history,
        converged=converged,
        n_iter=iterations_run,
        indicator_names=inferred_names,
        release_names=list(RELEASE_NAMES),
        n_factors=config.n_factors,
        first_mean=first_mean,
        first_scale=first_scale,
        mature_mean=mature_mean,
        mature_scale=mature_scale,
        release_mean=release_mean,
        release_scale=release_scale,
        has_mature_indicators=mature is not None,
    )


def forecast_gdp_releases(
    result: JointIndicatorRevisionDFMResult,
    state: np.ndarray | None = None,
    *,
    original_scale: bool = True,
) -> dict[str, float]:
    state_vector = result.smoother.smoothed_state[-1] if state is None else np.asarray(state, dtype=float)
    release_start = len(result.indicator_names) * (2 if result.has_mature_indicators else 1)
    release_design = result.params.design[release_start : release_start + len(RELEASE_NAMES)]
    forecasts = release_design @ state_vector
    if original_scale:
        forecasts = forecasts * result.release_scale + result.release_mean
    return {name: float(value) for name, value in zip(RELEASE_NAMES, forecasts)}


def forecast_gdp_release_moments(
    result: JointIndicatorRevisionDFMResult,
    state: np.ndarray | None = None,
    state_cov: np.ndarray | None = None,
    *,
    original_scale: bool = True,
) -> tuple[dict[str, float], pd.DataFrame]:
    """Return predictive means and covariance for the GDP release block."""

    state_vector = result.smoother.smoothed_state[-1] if state is None else np.asarray(state, dtype=float)
    cov = result.smoother.smoothed_cov[-1] if state_cov is None else np.asarray(state_cov, dtype=float)
    release_start = len(result.indicator_names) * (2 if result.has_mature_indicators else 1)
    design = result.params.design[release_start : release_start + len(RELEASE_NAMES)]
    obs_cov = result.params.obs_cov[release_start : release_start + len(RELEASE_NAMES), release_start : release_start + len(RELEASE_NAMES)]
    means = design @ state_vector
    covariance = design @ _as_psd(cov) @ design.T + _as_psd(obs_cov)
    if original_scale:
        means = means * result.release_scale + result.release_mean
        covariance = covariance * np.outer(result.release_scale, result.release_scale)
    return (
        {name: float(value) for name, value in zip(RELEASE_NAMES, means)},
        pd.DataFrame(covariance, index=RELEASE_NAMES, columns=RELEASE_NAMES),
    )


def forecast_indicator_revisions(
    result: JointIndicatorRevisionDFMResult,
    state: np.ndarray | None = None,
    *,
    original_scale: bool = True,
) -> pd.DataFrame:
    """Forecast mature-minus-first indicator revisions at the latest state."""

    state_vector = result.smoother.smoothed_state[-1] if state is None else np.asarray(state, dtype=float)
    n = len(result.indicator_names)
    first_design = result.params.design[:n]
    if result.has_mature_indicators:
        mature_design = result.params.design[n : 2 * n]
    else:
        mature_design = first_design.copy()
        mature_design[:, result.n_factors + 2] = 0.0
    first_forecast = first_design @ state_vector
    mature_forecast = mature_design @ state_vector
    if original_scale:
        first_forecast = first_forecast * result.first_scale + result.first_mean
        mature_forecast = mature_forecast * result.mature_scale + result.mature_mean
    return pd.DataFrame(
        {
            "series_id": result.indicator_names,
            "first_release_forecast": first_forecast,
            "mature_forecast": mature_forecast,
            "revision_forecast": mature_forecast - first_forecast,
        }
    )
