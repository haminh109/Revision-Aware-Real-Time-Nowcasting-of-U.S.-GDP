from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from full_state_space_release_revision_dfm.kalman_em import (
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
    _initial_monthly_loadings,
    _standardize_release_panel,
)


RATE_OR_LEVEL_SERIES = {"UNRATE", "TCU", "FEDFUNDS", "GS10", "TB3MS", "T10Y3MM", "UMCSENT"}
MAX_SAFE_VALUE = 1e8
MAX_MOMENT_VALUE = 1e10
MIN_DENOMINATOR = 1e-10


@dataclass(slots=True)
class MixedFrequencyReleaseKalmanConfig:
    """Monthly mixed-frequency Kalman prototype for the GDP release ladder.

    The measurement equation observes monthly indicators every month and GDP
    release targets only at target-quarter end months. The GDP release rows load
    on a three-month latent activity average plus a release-revision state.
    """

    n_factors: int = 1
    max_iter: int = 75
    tolerance: float = 1e-5
    min_variance: float = 1e-5
    verbose: bool = False


@dataclass(slots=True)
class MixedFrequencyReleaseKalmanResult:
    params: LinearGaussianParams
    smoother: KalmanSmootherOutput
    loglikelihood_history: list[float]
    converged: bool
    n_iter: int
    monthly_names: list[str]
    release_names: list[str]
    monthly_mean: np.ndarray
    monthly_scale: np.ndarray
    release_mean: float
    release_scale: float
    measurement_index: pd.DatetimeIndex
    numerical_guard_events: dict[str, int]


def _record_guard_event(events: dict[str, int], name: str, count: int = 1) -> None:
    events[name] = int(events.get(name, 0)) + int(count)


def _finite_clipped(values: np.ndarray, events: dict[str, int], name: str, *, clip: float = MAX_SAFE_VALUE) -> np.ndarray:
    array = np.asarray(values, dtype=float)
    bad = ~np.isfinite(array)
    too_large = np.isfinite(array) & (np.abs(array) > clip)
    count = int(bad.sum() + too_large.sum())
    if count:
        _record_guard_event(events, name, count)
    array = np.nan_to_num(array, nan=0.0, posinf=clip, neginf=-clip)
    return np.clip(array, -clip, clip)


def _safe_dot(left: np.ndarray, right: np.ndarray, events: dict[str, int], name: str) -> float:
    l_clean = _finite_clipped(left, events, f"{name}_left_cleaned", clip=MAX_MOMENT_VALUE)
    r_clean = _finite_clipped(right, events, f"{name}_right_cleaned", clip=MAX_MOMENT_VALUE)
    with np.errstate(over="ignore", divide="ignore", invalid="ignore", under="ignore"):
        value = float(l_clean @ r_clean)
    if not np.isfinite(value):
        _record_guard_event(events, f"{name}_nonfinite_dot")
        return 0.0
    return float(np.clip(value, -MAX_MOMENT_VALUE, MAX_MOMENT_VALUE))


def _safe_moment_matrix(matrix: np.ndarray, events: dict[str, int], name: str) -> np.ndarray:
    return _finite_clipped(matrix, events, name, clip=MAX_MOMENT_VALUE)


def _quarter_to_period(label: str) -> pd.Period:
    year, quarter = str(label).split(":Q")
    return pd.Period(year=int(year), quarter=int(quarter), freq="Q")


def _quarter_end_month(label: str) -> pd.Timestamp:
    return _quarter_to_period(label).asfreq("M", how="end").to_timestamp()


def _month_start_index(index: pd.Index) -> pd.DatetimeIndex:
    dates = pd.to_datetime(index)
    return pd.DatetimeIndex(dates.to_period("M").to_timestamp())


def _transform_monthly_panel(panel: pd.DataFrame) -> pd.DataFrame:
    transformed = pd.DataFrame(index=_month_start_index(panel.index))
    work = panel.copy()
    work.index = transformed.index
    for col in work.columns:
        series = pd.to_numeric(work[col], errors="coerce").astype(float)
        if col in RATE_OR_LEVEL_SERIES:
            transformed[col] = series.diff()
        elif (series.dropna() > 0).all():
            with np.errstate(invalid="ignore", divide="ignore"):
                transformed[col] = 1200.0 * np.log(series).diff()
        else:
            transformed[col] = series.diff()
    return transformed.replace([np.inf, -np.inf], np.nan).sort_index()


def _standardize_monthly(values: pd.DataFrame) -> tuple[pd.DataFrame, np.ndarray, np.ndarray]:
    array = values.to_numpy(dtype=float)
    array = np.where(np.isfinite(array), array, np.nan)
    means = np.nanmean(array, axis=0)
    scales = np.nanstd(array, axis=0)
    means = np.where(np.isfinite(means), means, 0.0)
    scales = np.where(np.isfinite(scales) & (scales > 1e-8), scales, 1.0)
    with np.errstate(invalid="ignore", divide="ignore", over="ignore"):
        standardized = (array - means) / scales
    standardized = np.where(np.isfinite(standardized), standardized, np.nan)
    return pd.DataFrame(standardized, index=values.index, columns=values.columns), means, scales


def _monthly_grid(monthly_panel: pd.DataFrame, release_train: pd.DataFrame) -> pd.DatetimeIndex:
    monthly_index = _month_start_index(monthly_panel.index) if len(monthly_panel.index) else pd.DatetimeIndex([])
    release_months = pd.DatetimeIndex([_quarter_end_month(label) for label in release_train.index])
    starts = [idx.min() for idx in (monthly_index, release_months) if len(idx)]
    ends = [idx.max() for idx in (monthly_index, release_months) if len(idx)]
    if not starts or not ends:
        raise ValueError("mixed-frequency Kalman requires non-empty monthly and release panels")
    return pd.date_range(min(starts), max(ends), freq="MS")


def _build_measurement_panel(
    monthly_panel: pd.DataFrame,
    release_train: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, np.ndarray, np.ndarray, float, float]:
    monthly = _transform_monthly_panel(monthly_panel)
    monthly = monthly.groupby(monthly.index).last()
    month_index = _monthly_grid(monthly, release_train)
    monthly = monthly.reindex(month_index)
    monthly_std, monthly_mean, monthly_scale = _standardize_monthly(monthly)

    release_obs = pd.DataFrame(np.nan, index=month_index, columns=list(RELEASE_NAMES), dtype=float)
    for quarter_label, row in release_train.iterrows():
        month = _quarter_end_month(str(quarter_label))
        if month in release_obs.index:
            for release_name in RELEASE_NAMES:
                value = pd.to_numeric(row.get(release_name, np.nan), errors="coerce")
                release_obs.loc[month, release_name] = float(value) if np.isfinite(value) else np.nan
    release_std_array, release_mean, release_scale = _standardize_release_panel(release_obs.to_numpy(dtype=float))
    release_std = pd.DataFrame(release_std_array, index=month_index, columns=list(RELEASE_NAMES))
    return monthly_std, release_std, monthly_mean, monthly_scale, release_mean, release_scale


def _initial_params(
    monthly_std: pd.DataFrame,
    release_std: pd.DataFrame,
    config: MixedFrequencyReleaseKalmanConfig,
    guard_events: dict[str, int],
) -> LinearGaussianParams:
    monthly_array = monthly_std.to_numpy(dtype=float)
    release_array = release_std.to_numpy(dtype=float)
    n_monthly = monthly_array.shape[1]
    n_state = config.n_factors + 4
    g_idx = config.n_factors
    g_lag1_idx = config.n_factors + 1
    g_lag2_idx = config.n_factors + 2
    revision_idx = config.n_factors + 3

    factors, loadings = _initial_monthly_loadings(monthly_array, config.n_factors)
    design = np.zeros((n_monthly + len(RELEASE_NAMES), n_state), dtype=float)
    design[:n_monthly, : config.n_factors] = loadings
    release_loadings = {"A": 1.0, "S": 0.55, "T": 0.25, "M": 0.0}
    for offset, release_name in enumerate(RELEASE_NAMES):
        row = n_monthly + offset
        design[row, [g_idx, g_lag1_idx, g_lag2_idx]] = 1.0 / 3.0
        design[row, revision_idx] = release_loadings[release_name]

    transition = np.eye(n_state, dtype=float) * 0.35
    if monthly_array.shape[0] > 3 and config.n_factors > 0:
        for j in range(config.n_factors):
            x = factors[:-1, j]
            y = factors[1:, j]
            denom = _safe_dot(x, x, guard_events, "initial_factor_ar_denominator")
            if denom > 1e-8:
                numerator = _safe_dot(x, y, guard_events, "initial_factor_ar_numerator")
                transition[j, j] = float(np.clip(numerator / max(denom, MIN_DENOMINATOR), -0.95, 0.95))
            else:
                _record_guard_event(guard_events, "initial_factor_ar_small_denominator")
    transition[g_idx, : config.n_factors] = 0.12 / max(config.n_factors, 1)
    transition[g_idx, g_idx] = 0.45
    transition[g_idx, g_lag1_idx] = 0.10
    transition[g_idx, g_lag2_idx] = 0.05
    transition[g_lag1_idx, :] = 0.0
    transition[g_lag1_idx, g_idx] = 1.0
    transition[g_lag2_idx, :] = 0.0
    transition[g_lag2_idx, g_lag1_idx] = 1.0
    transition[revision_idx, : config.n_factors] = 0.03 / max(config.n_factors, 1)
    transition[revision_idx, revision_idx] = 0.50

    y = np.column_stack([monthly_array, release_array])
    obs_var = np.nanvar(y, axis=0)
    obs_var = np.where(np.isfinite(obs_var) & (obs_var > config.min_variance), obs_var, 1.0)
    state_cov = np.eye(n_state, dtype=float) * 0.20
    state_cov[g_lag1_idx, g_lag1_idx] = config.min_variance
    state_cov[g_lag2_idx, g_lag2_idx] = config.min_variance
    initial_state = np.zeros(n_state, dtype=float)
    if factors.size:
        initial_state[: config.n_factors] = factors[0, : config.n_factors]
    return LinearGaussianParams(
        transition=_stabilize_transition(transition),
        state_intercept=np.zeros(n_state, dtype=float),
        state_cov=_as_psd(state_cov, ridge=config.min_variance),
        design=design,
        obs_cov=np.diag(obs_var),
        initial_state=initial_state,
        initial_cov=np.eye(n_state, dtype=float) * 5.0,
    )


def _transition_m_step(
    smoother: KalmanSmootherOutput,
    params: LinearGaussianParams,
    config: MixedFrequencyReleaseKalmanConfig,
    guard_events: dict[str, int],
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
        s_xx += exx_t
        s_xz += np.column_stack([x_t, exx_cross])
        s_zz += ezz
    s_xx = _safe_moment_matrix(s_xx, guard_events, "transition_s_xx_cleaned")
    s_xz = _safe_moment_matrix(s_xz, guard_events, "transition_s_xz_cleaned")
    s_zz = _safe_moment_matrix(s_zz, guard_events, "transition_s_zz_cleaned")
    inv, _ = _inverse_and_logdet(s_zz)
    with np.errstate(over="ignore", divide="ignore", invalid="ignore", under="ignore"):
        block = s_xz @ inv
    block = _safe_moment_matrix(block, guard_events, "transition_block_cleaned")
    transition = _stabilize_transition(block[:, 1:])
    intercept = _finite_clipped(block[:, 0], guard_events, "transition_intercept_cleaned")

    g_idx = config.n_factors
    g_lag1_idx = config.n_factors + 1
    g_lag2_idx = config.n_factors + 2
    transition[g_lag1_idx, :] = 0.0
    transition[g_lag1_idx, g_idx] = 1.0
    transition[g_lag2_idx, :] = 0.0
    transition[g_lag2_idx, g_lag1_idx] = 1.0
    intercept[g_lag1_idx] = 0.0
    intercept[g_lag2_idx] = 0.0

    with np.errstate(over="ignore", divide="ignore", invalid="ignore", under="ignore"):
        state_cov = (s_xx - block @ s_xz.T) / max(n_obs - 1, 1)
    state_cov = _safe_moment_matrix(state_cov, guard_events, "transition_state_cov_cleaned")
    state_cov[g_lag1_idx, :] = 0.0
    state_cov[:, g_lag1_idx] = 0.0
    state_cov[g_lag2_idx, :] = 0.0
    state_cov[:, g_lag2_idx] = 0.0
    state_cov[g_lag1_idx, g_lag1_idx] = config.min_variance
    state_cov[g_lag2_idx, g_lag2_idx] = config.min_variance
    return transition, intercept, _as_psd(state_cov, ridge=config.min_variance)


def _fixed_design_obs_cov(
    y: np.ndarray,
    design: np.ndarray,
    smoother: KalmanSmootherOutput,
    min_variance: float,
    guard_events: dict[str, int],
) -> np.ndarray:
    n_series = y.shape[1]
    obs_cov = np.zeros((n_series, n_series), dtype=float)
    for i in range(n_series):
        observed = np.isfinite(y[:, i])
        if observed.sum() == 0:
            obs_cov[i, i] = 1.0
            continue
        h = design[i]
        residual_var = 0.0
        for t in np.flatnonzero(observed):
            with np.errstate(over="ignore", divide="ignore", invalid="ignore", under="ignore"):
                term = y[t, i] ** 2 - 2.0 * y[t, i] * h @ smoother.smoothed_state[t] + h @ _expected_xx(smoother, t) @ h.T
            if np.isfinite(term):
                residual_var += float(np.clip(term, -MAX_MOMENT_VALUE, MAX_MOMENT_VALUE))
            else:
                _record_guard_event(guard_events, "obs_cov_nonfinite_residual_term")
        if not np.isfinite(residual_var):
            _record_guard_event(guard_events, "obs_cov_nonfinite_residual_var")
            residual_var = min_variance * observed.sum()
        obs_cov[i, i] = max(float(residual_var / observed.sum()), min_variance)
    return obs_cov


def fit_mixed_frequency_release_kalman(
    monthly_panel: pd.DataFrame,
    release_train: pd.DataFrame,
    config: MixedFrequencyReleaseKalmanConfig | None = None,
) -> MixedFrequencyReleaseKalmanResult:
    config = config or MixedFrequencyReleaseKalmanConfig()
    monthly_std, release_std, monthly_mean, monthly_scale, release_mean, release_scale = _build_measurement_panel(
        monthly_panel,
        release_train,
    )
    guard_events: dict[str, int] = {}
    params = _initial_params(monthly_std, release_std, config, guard_events)
    y = np.column_stack([monthly_std.to_numpy(dtype=float), release_std.to_numpy(dtype=float)])
    loglikelihood_history: list[float] = []
    converged = False
    iterations_run = 0
    for iteration in range(config.max_iter):
        iterations_run = iteration + 1
        smoother = rts_smoother(y, params)
        loglikelihood_history.append(float(smoother.loglikelihood))
        if config.verbose:
            print(f"mixed-frequency EM iter {iteration + 1}: llf={smoother.loglikelihood:.6f}")
        if len(loglikelihood_history) >= 2:
            improvement = loglikelihood_history[-1] - loglikelihood_history[-2]
            if abs(improvement) <= config.tolerance * (1.0 + abs(loglikelihood_history[-2])):
                converged = True
                break
        transition, intercept, state_cov = _transition_m_step(smoother, params, config, guard_events)
        obs_cov = _fixed_design_obs_cov(y, params.design, smoother, config.min_variance, guard_events)
        params = LinearGaussianParams(
            transition=transition,
            state_intercept=intercept,
            state_cov=state_cov,
            design=params.design.copy(),
            obs_cov=obs_cov,
            initial_state=smoother.smoothed_state[0],
            initial_cov=_as_psd(smoother.smoothed_cov[0], ridge=config.min_variance),
        )
    final_smoother = rts_smoother(y, params)
    if not loglikelihood_history or final_smoother.loglikelihood != loglikelihood_history[-1]:
        loglikelihood_history.append(float(final_smoother.loglikelihood))
    return MixedFrequencyReleaseKalmanResult(
        params=params,
        smoother=final_smoother,
        loglikelihood_history=loglikelihood_history,
        converged=converged,
        n_iter=iterations_run,
        monthly_names=list(monthly_std.columns),
        release_names=list(RELEASE_NAMES),
        monthly_mean=monthly_mean,
        monthly_scale=monthly_scale,
        release_mean=float(release_mean),
        release_scale=float(release_scale),
        measurement_index=monthly_std.index,
        numerical_guard_events=guard_events,
    )


def forecast_mixed_frequency_release_kalman(
    monthly_panel: pd.DataFrame,
    release_train: pd.DataFrame,
    target_quarter: str,
    config: MixedFrequencyReleaseKalmanConfig | None = None,
) -> tuple[dict[str, float], dict[str, float], pd.DataFrame, dict[str, object]]:
    result = fit_mixed_frequency_release_kalman(monthly_panel, release_train, config)
    target_month = _quarter_end_month(target_quarter)
    if target_month not in result.measurement_index:
        target_pos = len(result.measurement_index) - 1
    else:
        target_pos = int(result.measurement_index.get_loc(target_month))
    n_monthly = len(result.monthly_names)
    release_design = result.params.design[n_monthly : n_monthly + len(RELEASE_NAMES)]
    state = result.smoother.smoothed_state[target_pos]
    cov = result.smoother.smoothed_cov[target_pos]
    forecast_std = release_design @ state
    with np.errstate(over="ignore", divide="ignore", invalid="ignore", under="ignore"):
        forecast_cov_std = release_design @ cov @ release_design.T + result.params.obs_cov[
            n_monthly : n_monthly + len(RELEASE_NAMES),
            n_monthly : n_monthly + len(RELEASE_NAMES),
        ]
    forecast_cov_std = _as_psd(forecast_cov_std)
    forecast_values = result.release_mean + result.release_scale * forecast_std
    forecast_cov = (result.release_scale**2) * forecast_cov_std
    forecasts = {name: float(value) for name, value in zip(RELEASE_NAMES, forecast_values, strict=True)}
    variances = {name: float(forecast_cov[i, i]) for i, name in enumerate(RELEASE_NAMES)}
    forecast_cov_frame = pd.DataFrame(forecast_cov, index=list(RELEASE_NAMES), columns=list(RELEASE_NAMES))
    diagnostics = {
        "converged": bool(result.converged),
        "n_iter": int(result.n_iter),
        "loglikelihood": float(result.loglikelihood_history[-1]) if result.loglikelihood_history else np.nan,
        "llf_initial": float(result.loglikelihood_history[0]) if result.loglikelihood_history else np.nan,
        "llf_final": float(result.loglikelihood_history[-1]) if result.loglikelihood_history else np.nan,
        "llf_last_improvement": (
            float(result.loglikelihood_history[-1] - result.loglikelihood_history[-2])
            if len(result.loglikelihood_history) >= 2
            else np.nan
        ),
        "llf_relative_last_improvement": (
            abs(float(result.loglikelihood_history[-1] - result.loglikelihood_history[-2]))
            / (1.0 + abs(float(result.loglikelihood_history[-2])))
            if len(result.loglikelihood_history) >= 2
            else np.nan
        ),
        "llf_history_length": int(len(result.loglikelihood_history)),
        "numerical_guard_event_count": int(sum(result.numerical_guard_events.values())),
        "numerical_guard_events": dict(result.numerical_guard_events),
    }
    return forecasts, variances, forecast_cov_frame, diagnostics
