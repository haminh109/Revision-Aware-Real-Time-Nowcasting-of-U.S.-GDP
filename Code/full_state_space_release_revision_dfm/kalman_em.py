from __future__ import annotations

from dataclasses import dataclass

import numpy as np


MIN_VARIANCE = 1e-6
MAX_ABS_VALUE = 1e8
MAX_LOADING_VALUE = 1e4
MAX_SPECTRAL_RADIUS = 0.985
PSD_JITTER_MULTIPLIERS = (0.0, 1.0, 10.0, 100.0, 1000.0)


def _symmetrize(matrix: np.ndarray) -> np.ndarray:
    return 0.5 * (matrix + matrix.T)


def _clean_array(values: np.ndarray, *, clip: float = MAX_ABS_VALUE) -> np.ndarray:
    array = np.nan_to_num(np.asarray(values, dtype=float), nan=0.0, posinf=clip, neginf=-clip)
    return np.clip(array, -clip, clip)


def _clean_symmetric_matrix(matrix: np.ndarray) -> np.ndarray:
    matrix = np.asarray(matrix, dtype=float)
    matrix = np.nan_to_num(matrix, nan=0.0, posinf=MAX_ABS_VALUE, neginf=-MAX_ABS_VALUE)
    matrix = np.clip(matrix, -MAX_ABS_VALUE, MAX_ABS_VALUE)
    if matrix.ndim != 2 or matrix.shape[0] != matrix.shape[1]:
        raise ValueError("matrix must be a square 2D array")
    return _symmetrize(matrix)


def _is_effectively_diagonal(matrix: np.ndarray, diag: np.ndarray) -> bool:
    offdiag = matrix - np.diag(diag)
    max_offdiag = float(np.max(np.abs(offdiag))) if offdiag.size else 0.0
    return max_offdiag <= 1e-12


def _is_strictly_diagonally_dominant(matrix: np.ndarray, diag: np.ndarray, ridge: float) -> bool:
    offdiag = matrix - np.diag(diag)
    row_margin = diag - np.sum(np.abs(offdiag), axis=1)
    return bool(np.all(np.isfinite(row_margin)) and np.min(row_margin) >= ridge)


def _cholesky_stabilized(matrix: np.ndarray, ridge: float = MIN_VARIANCE) -> tuple[np.ndarray, np.ndarray] | None:
    if matrix.shape[0] == 0:
        return matrix, matrix
    eye = np.eye(matrix.shape[0], dtype=float)
    base = _symmetrize(matrix)
    jitters = [ridge * multiplier for multiplier in PSD_JITTER_MULTIPLIERS]
    jitters.extend([1e-3, 1e-2])
    for jitter in jitters:
        candidate = base if jitter == 0.0 else base + eye * jitter
        candidate = _symmetrize(np.clip(candidate, -MAX_ABS_VALUE, MAX_ABS_VALUE))
        try:
            chol = np.linalg.cholesky(candidate)
        except np.linalg.LinAlgError:
            continue
        if np.isfinite(chol).all():
            return candidate, chol
    return None


def _diagonal_inverse_and_logdet(diag: np.ndarray, ridge: float) -> tuple[np.ndarray, float]:
    diag = np.clip(np.nan_to_num(diag, nan=ridge, posinf=MAX_ABS_VALUE, neginf=ridge), ridge, MAX_ABS_VALUE)
    return np.diag(1.0 / diag), float(np.sum(np.log(diag)))


def _as_psd(matrix: np.ndarray, ridge: float = MIN_VARIANCE) -> np.ndarray:
    matrix = _clean_symmetric_matrix(matrix)
    diag = np.diag(matrix).copy()
    if matrix.shape[0] > 0:
        if _is_effectively_diagonal(matrix, diag):
            return np.diag(np.clip(diag, ridge, MAX_ABS_VALUE))
        if _is_strictly_diagonally_dominant(matrix, diag, ridge):
            return matrix
        cholesky = _cholesky_stabilized(matrix, ridge=ridge)
        if cholesky is not None:
            return cholesky[0]
    try:
        eigenvalues, eigenvectors = np.linalg.eigh(matrix)
    except np.linalg.LinAlgError:
        return np.eye(matrix.shape[0], dtype=float) * max(ridge, 1.0)
    eigenvalues = np.nan_to_num(eigenvalues, nan=ridge, posinf=MAX_ABS_VALUE, neginf=ridge)
    eigenvalues = np.clip(eigenvalues, ridge, MAX_ABS_VALUE)
    eigenvectors = _clean_array(eigenvectors, clip=MAX_LOADING_VALUE)
    with np.errstate(over="ignore", divide="ignore", invalid="ignore", under="ignore"):
        projected = (eigenvectors * eigenvalues) @ eigenvectors.T
    projected = np.nan_to_num(projected, nan=0.0, posinf=MAX_ABS_VALUE, neginf=-MAX_ABS_VALUE)
    return _symmetrize(np.clip(projected, -MAX_ABS_VALUE, MAX_ABS_VALUE))


def _inverse_and_logdet(matrix: np.ndarray, ridge: float = MIN_VARIANCE) -> tuple[np.ndarray, float]:
    matrix = _clean_symmetric_matrix(matrix)
    diag = np.diag(matrix).copy()
    if matrix.shape[0] > 0:
        if _is_effectively_diagonal(matrix, diag):
            return _diagonal_inverse_and_logdet(diag, ridge)
        cholesky = _cholesky_stabilized(matrix, ridge=ridge)
        if cholesky is not None:
            stabilized, chol = cholesky
            eye = np.eye(stabilized.shape[0], dtype=float)
            try:
                inverse = np.linalg.solve(chol.T, np.linalg.solve(chol, eye))
            except np.linalg.LinAlgError:
                inverse = None
            if inverse is not None and np.isfinite(inverse).all():
                diag_chol = np.clip(np.diag(chol), ridge, MAX_ABS_VALUE)
                logdet = float(2.0 * np.sum(np.log(diag_chol)))
                inverse = np.nan_to_num(inverse, nan=0.0, posinf=MAX_ABS_VALUE, neginf=-MAX_ABS_VALUE)
                inverse = np.clip(inverse, -MAX_ABS_VALUE, MAX_ABS_VALUE)
                return _symmetrize(inverse), logdet
    try:
        eigenvalues, eigenvectors = np.linalg.eigh(_as_psd(matrix, ridge=ridge))
    except np.linalg.LinAlgError:
        matrix = np.eye(matrix.shape[0], dtype=float) * max(ridge, 1.0)
        eigenvalues, eigenvectors = np.linalg.eigh(matrix)
    eigenvalues = np.nan_to_num(eigenvalues, nan=ridge, posinf=MAX_ABS_VALUE, neginf=ridge)
    eigenvalues = np.clip(eigenvalues, ridge, MAX_ABS_VALUE)
    eigenvectors = _clean_array(eigenvectors, clip=MAX_LOADING_VALUE)
    with np.errstate(over="ignore", divide="ignore", invalid="ignore", under="ignore"):
        inverse = (eigenvectors * (1.0 / eigenvalues)) @ eigenvectors.T
    inverse = np.nan_to_num(inverse, nan=0.0, posinf=MAX_ABS_VALUE, neginf=-MAX_ABS_VALUE)
    inverse = np.clip(inverse, -MAX_ABS_VALUE, MAX_ABS_VALUE)
    logdet = float(np.sum(np.log(eigenvalues)))
    return _symmetrize(inverse), logdet


def _stabilize_transition(matrix: np.ndarray, max_radius: float = MAX_SPECTRAL_RADIUS) -> np.ndarray:
    transition = np.nan_to_num(np.asarray(matrix, dtype=float), nan=0.0, posinf=0.0, neginf=0.0)
    transition = np.clip(transition, -100.0, 100.0)
    try:
        radius = float(np.max(np.abs(np.linalg.eigvals(transition))))
    except np.linalg.LinAlgError:
        radius = np.inf
    if np.isfinite(radius) and radius > max_radius:
        transition *= max_radius / max(radius, 1e-12)
    elif not np.isfinite(radius):
        transition = np.eye(transition.shape[0], dtype=float) * 0.5
    return transition


@dataclass(slots=True)
class LinearGaussianParams:
    """Time-invariant linear-Gaussian state-space parameters.

    Model:
        x_t = transition @ x_{t-1} + state_intercept + eta_t
        y_t = design @ x_t + eps_t

    Missing observations in y are represented by np.nan and are omitted from
    each Kalman update.
    """

    transition: np.ndarray
    state_intercept: np.ndarray
    state_cov: np.ndarray
    design: np.ndarray
    obs_cov: np.ndarray
    initial_state: np.ndarray
    initial_cov: np.ndarray

    def copy(self) -> "LinearGaussianParams":
        return LinearGaussianParams(
            transition=self.transition.copy(),
            state_intercept=self.state_intercept.copy(),
            state_cov=self.state_cov.copy(),
            design=self.design.copy(),
            obs_cov=self.obs_cov.copy(),
            initial_state=self.initial_state.copy(),
            initial_cov=self.initial_cov.copy(),
        )


@dataclass(slots=True)
class KalmanFilterOutput:
    predicted_state: np.ndarray
    predicted_cov: np.ndarray
    filtered_state: np.ndarray
    filtered_cov: np.ndarray
    loglikelihood: float


@dataclass(slots=True)
class KalmanSmootherOutput:
    predicted_state: np.ndarray
    predicted_cov: np.ndarray
    filtered_state: np.ndarray
    filtered_cov: np.ndarray
    smoothed_state: np.ndarray
    smoothed_cov: np.ndarray
    lag_one_cov: np.ndarray
    loglikelihood: float


@dataclass(slots=True)
class KalmanEMConfig:
    max_iter: int = 100
    tolerance: float = 1e-5
    min_variance: float = 1e-5
    diagonal_obs_cov: bool = True
    verbose: bool = False


@dataclass(slots=True)
class KalmanEMResult:
    params: LinearGaussianParams
    smoother: KalmanSmootherOutput
    loglikelihood_history: list[float]
    converged: bool
    n_iter: int


def kalman_filter(y: np.ndarray, params: LinearGaussianParams) -> KalmanFilterOutput:
    """Run a Kalman filter with arbitrary missing observations."""

    observations = np.asarray(y, dtype=float)
    if observations.ndim != 2:
        raise ValueError("y must be a 2D array with shape (n_obs, n_series)")

    n_obs = observations.shape[0]
    n_state = params.initial_state.shape[0]
    predicted_state = np.zeros((n_obs, n_state), dtype=float)
    predicted_cov = np.zeros((n_obs, n_state, n_state), dtype=float)
    filtered_state = np.zeros((n_obs, n_state), dtype=float)
    filtered_cov = np.zeros((n_obs, n_state, n_state), dtype=float)
    loglikelihood = 0.0

    current_state = _clean_array(params.initial_state)
    current_cov = _as_psd(params.initial_cov)
    transition = _stabilize_transition(params.transition)
    intercept = _clean_array(params.state_intercept)
    state_cov = _as_psd(params.state_cov)
    design = _clean_array(params.design, clip=MAX_LOADING_VALUE)
    obs_cov = _as_psd(params.obs_cov)
    obs_cov_is_diagonal = _is_effectively_diagonal(obs_cov, np.diag(obs_cov).copy())

    for t in range(n_obs):
        if t == 0:
            state_pred = current_state
            cov_pred = current_cov
        else:
            with np.errstate(over="ignore", divide="ignore", invalid="ignore", under="ignore"):
                state_pred = transition @ filtered_state[t - 1] + intercept
                cov_pred = transition @ filtered_cov[t - 1] @ transition.T + state_cov
            state_pred = _clean_array(state_pred)
            cov_pred = _clean_array(cov_pred)
            cov_pred = _as_psd(cov_pred)

        predicted_state[t] = state_pred
        predicted_cov[t] = cov_pred

        observed = np.isfinite(observations[t])
        if observed.any():
            if obs_cov_is_diagonal:
                y_t = observations[t, observed]
                h_t = design[observed]
                r_diag = np.diag(obs_cov)[observed]
                r_diag = np.clip(np.nan_to_num(r_diag, nan=MIN_VARIANCE, posinf=MAX_ABS_VALUE, neginf=MIN_VARIANCE), MIN_VARIANCE, MAX_ABS_VALUE)
                with np.errstate(over="ignore", divide="ignore", invalid="ignore", under="ignore"):
                    innovation = y_t - h_t @ state_pred
                innovation = _clean_array(innovation)
                cov_pred_inv, cov_pred_logdet = _inverse_and_logdet(cov_pred)
                r_inv = 1.0 / r_diag
                weighted_h = h_t * r_inv[:, None]
                with np.errstate(over="ignore", divide="ignore", invalid="ignore", under="ignore"):
                    posterior_precision = cov_pred_inv + h_t.T @ weighted_h
                    weighted_innovation = h_t.T @ (r_inv * innovation)
                posterior_precision = _clean_array(posterior_precision)
                cov_filt, precision_logdet = _inverse_and_logdet(posterior_precision)
                with np.errstate(over="ignore", divide="ignore", invalid="ignore", under="ignore"):
                    state_filt = state_pred + cov_filt @ weighted_innovation
                    prior_quadratic = float(innovation @ (r_inv * innovation))
                    posterior_quadratic = float(weighted_innovation @ cov_filt @ weighted_innovation)
                state_filt = _clean_array(state_filt)
                cov_filt = _as_psd(cov_filt)
                quadratic = prior_quadratic - posterior_quadratic
                if not np.isfinite(quadratic):
                    quadratic = MAX_ABS_VALUE
                quadratic = float(np.clip(quadratic, 0.0, MAX_ABS_VALUE))
                innovation_logdet = float(np.sum(np.log(r_diag)) + cov_pred_logdet + precision_logdet)
                if not np.isfinite(innovation_logdet):
                    innovation_logdet = float(np.log(MAX_ABS_VALUE))
                loglikelihood += float(
                    -0.5
                    * (
                        observed.sum() * np.log(2.0 * np.pi)
                        + innovation_logdet
                        + quadratic
                    )
                )
            else:
                y_t = observations[t, observed]
                h_t = design[observed]
                r_t = obs_cov[np.ix_(observed, observed)]
                with np.errstate(over="ignore", divide="ignore", invalid="ignore", under="ignore"):
                    innovation = y_t - h_t @ state_pred
                    innovation_cov = h_t @ cov_pred @ h_t.T + r_t
                innovation = _clean_array(innovation)
                innovation_cov = _clean_array(innovation_cov)
                innovation_inv, innovation_logdet = _inverse_and_logdet(innovation_cov)
                with np.errstate(over="ignore", divide="ignore", invalid="ignore", under="ignore"):
                    gain = cov_pred @ h_t.T @ innovation_inv
                    state_filt = state_pred + gain @ innovation
                    cov_filt = cov_pred - gain @ h_t @ cov_pred
                state_filt = _clean_array(state_filt)
                cov_filt = _clean_array(cov_filt)
                cov_filt = _as_psd(cov_filt)
                with np.errstate(over="ignore", divide="ignore", invalid="ignore", under="ignore"):
                    quadratic = float(innovation.T @ innovation_inv @ innovation)
                if not np.isfinite(quadratic):
                    quadratic = MAX_ABS_VALUE
                quadratic = float(np.clip(quadratic, 0.0, MAX_ABS_VALUE))
                if not np.isfinite(innovation_logdet):
                    innovation_logdet = float(np.log(MAX_ABS_VALUE))
                loglikelihood += float(
                    -0.5
                    * (
                        observed.sum() * np.log(2.0 * np.pi)
                        + innovation_logdet
                        + quadratic
                    )
                )
        else:
            state_filt = state_pred
            cov_filt = cov_pred

        filtered_state[t] = state_filt
        filtered_cov[t] = cov_filt

    return KalmanFilterOutput(
        predicted_state=predicted_state,
        predicted_cov=predicted_cov,
        filtered_state=filtered_state,
        filtered_cov=filtered_cov,
        loglikelihood=loglikelihood,
    )


def rts_smoother(y: np.ndarray, params: LinearGaussianParams) -> KalmanSmootherOutput:
    """Run Kalman filter plus Rauch--Tung--Striebel smoother."""

    filt = kalman_filter(y, params)
    n_obs, n_state = filt.filtered_state.shape
    transition = _stabilize_transition(params.transition)
    smoothed_state = filt.filtered_state.copy()
    smoothed_cov = filt.filtered_cov.copy()
    lag_one_cov = np.zeros((n_obs, n_state, n_state), dtype=float)

    smoother_gain = np.zeros((max(n_obs - 1, 1), n_state, n_state), dtype=float)
    for t in range(n_obs - 2, -1, -1):
        pred_cov_next = _clean_symmetric_matrix(filt.predicted_cov[t + 1])
        pred_inv, _ = _inverse_and_logdet(pred_cov_next)
        with np.errstate(over="ignore", divide="ignore", invalid="ignore", under="ignore"):
            gain = filt.filtered_cov[t] @ transition.T @ pred_inv
        gain = _clean_array(gain)
        smoother_gain[t] = gain
        state_gap = smoothed_state[t + 1] - filt.predicted_state[t + 1]
        with np.errstate(over="ignore", divide="ignore", invalid="ignore", under="ignore"):
            smoothed_state[t] = filt.filtered_state[t] + gain @ state_gap
            smoothed_cov[t] = filt.filtered_cov[t] + gain @ (smoothed_cov[t + 1] - pred_cov_next) @ gain.T
        smoothed_state[t] = _clean_array(smoothed_state[t])
        smoothed_cov[t] = _clean_array(smoothed_cov[t])
        smoothed_cov[t] = _as_psd(smoothed_cov[t])

    for t in range(1, n_obs):
        lag_one_cov[t] = smoothed_cov[t] @ smoother_gain[t - 1].T

    return KalmanSmootherOutput(
        predicted_state=filt.predicted_state,
        predicted_cov=filt.predicted_cov,
        filtered_state=filt.filtered_state,
        filtered_cov=filt.filtered_cov,
        smoothed_state=smoothed_state,
        smoothed_cov=smoothed_cov,
        lag_one_cov=lag_one_cov,
        loglikelihood=filt.loglikelihood,
    )


def _expected_xx(smoother: KalmanSmootherOutput, t: int) -> np.ndarray:
    x_t = smoother.smoothed_state[t]
    return smoother.smoothed_cov[t] + np.outer(x_t, x_t)


def _generic_em_step(
    y: np.ndarray,
    smoother: KalmanSmootherOutput,
    params: LinearGaussianParams,
    config: KalmanEMConfig,
) -> LinearGaussianParams:
    """Generic unconstrained M-step for a linear-Gaussian state-space model."""

    observations = np.asarray(y, dtype=float)
    n_obs, n_series = observations.shape
    n_state = params.initial_state.shape[0]

    if n_obs < 2:
        raise ValueError("EM requires at least two observations")

    s_xx = np.zeros((n_state, n_state), dtype=float)
    s_xz = np.zeros((n_state, n_state + 1), dtype=float)
    s_zz = np.zeros((n_state + 1, n_state + 1), dtype=float)

    for t in range(1, n_obs):
        x_t = smoother.smoothed_state[t]
        x_prev = smoother.smoothed_state[t - 1]
        exx_t = _expected_xx(smoother, t)
        exx_prev = _expected_xx(smoother, t - 1)
        exx_cross = smoother.lag_one_cov[t] + np.outer(x_t, x_prev)
        z_mean = np.r_[1.0, x_prev]
        ezz = np.zeros((n_state + 1, n_state + 1), dtype=float)
        ezz[0, 0] = 1.0
        ezz[0, 1:] = x_prev
        ezz[1:, 0] = x_prev
        ezz[1:, 1:] = exx_prev
        exz = np.column_stack([x_t, exx_cross])
        s_xx += exx_t
        s_xz += exz
        s_zz += ezz
        _ = z_mean  # keeps the augmented moment structure explicit.

    s_zz_inv, _ = _inverse_and_logdet(s_zz)
    transition_block = s_xz @ s_zz_inv
    state_intercept = transition_block[:, 0]
    transition = _stabilize_transition(transition_block[:, 1:])
    state_cov = (s_xx - transition_block @ s_xz.T) / max(n_obs - 1, 1)
    state_cov = _as_psd(state_cov, ridge=config.min_variance)

    design = np.zeros((n_series, n_state), dtype=float)
    obs_cov = np.zeros((n_series, n_series), dtype=float)
    for i in range(n_series):
        observed = np.isfinite(observations[:, i])
        if observed.sum() == 0:
            design[i] = params.design[i]
            obs_cov[i, i] = max(float(params.obs_cov[i, i]), config.min_variance)
            continue
        s_yx = np.zeros(n_state, dtype=float)
        s_xx_i = np.zeros((n_state, n_state), dtype=float)
        for t in np.flatnonzero(observed):
            s_yx += observations[t, i] * smoother.smoothed_state[t]
            s_xx_i += _expected_xx(smoother, t)
        s_xx_inv, _ = _inverse_and_logdet(s_xx_i)
        design[i] = s_yx @ s_xx_inv
        residual_var = 0.0
        for t in np.flatnonzero(observed):
            h_i = design[i]
            residual_var += (
                observations[t, i] ** 2
                - 2.0 * observations[t, i] * h_i @ smoother.smoothed_state[t]
                + h_i @ _expected_xx(smoother, t) @ h_i.T
            )
        obs_cov[i, i] = max(residual_var / observed.sum(), config.min_variance)

    if not config.diagonal_obs_cov:
        obs_cov = np.diag(np.diag(obs_cov))

    return LinearGaussianParams(
        transition=transition,
        state_intercept=state_intercept,
        state_cov=state_cov,
        design=design,
        obs_cov=obs_cov,
        initial_state=smoother.smoothed_state[0],
        initial_cov=_as_psd(smoother.smoothed_cov[0], ridge=config.min_variance),
    )


def fit_em(
    y: np.ndarray,
    initial_params: LinearGaussianParams,
    config: KalmanEMConfig | None = None,
) -> KalmanEMResult:
    """Fit a generic linear-Gaussian state-space model by Kalman/EM."""

    config = config or KalmanEMConfig()
    params = initial_params.copy()
    loglikelihood_history: list[float] = []
    converged = False
    iterations_run = 0

    for iteration in range(config.max_iter):
        iterations_run = iteration + 1
        smoother = rts_smoother(y, params)
        loglikelihood_history.append(float(smoother.loglikelihood))
        if config.verbose:
            print(f"EM iter {iteration + 1}: llf={smoother.loglikelihood:.6f}")
        if len(loglikelihood_history) >= 2:
            improvement = loglikelihood_history[-1] - loglikelihood_history[-2]
            if abs(improvement) <= config.tolerance * (1.0 + abs(loglikelihood_history[-2])):
                converged = True
                break
        params = _generic_em_step(y, smoother, params, config)

    final_smoother = rts_smoother(y, params)
    if not loglikelihood_history or final_smoother.loglikelihood != loglikelihood_history[-1]:
        loglikelihood_history.append(float(final_smoother.loglikelihood))

    return KalmanEMResult(
        params=params,
        smoother=final_smoother,
        loglikelihood_history=loglikelihood_history,
        converged=converged,
        n_iter=iterations_run,
    )
