from __future__ import annotations

import numpy as np
import pandas as pd

from full_state_space_release_revision_dfm import (
    JointIndicatorRevisionDFMConfig,
    ReleaseRevisionDFMConfig,
    fit_joint_indicator_revision_dfm,
    fit_release_revision_dfm,
    forecast_gdp_releases,
    forecast_indicator_revisions,
    forecast_release_row,
)


def make_synthetic_data(
    n_obs: int = 96,
    n_monthly: int = 8,
    seed: int = 42,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    rng = np.random.default_rng(seed)
    factor = np.zeros(n_obs)
    gdp = np.zeros(n_obs)
    revision = np.zeros(n_obs)
    indicator_revision = np.zeros(n_obs)
    for t in range(1, n_obs):
        factor[t] = 0.65 * factor[t - 1] + rng.normal(scale=0.8)
        gdp[t] = 0.55 * gdp[t - 1] + 0.45 * factor[t] + rng.normal(scale=0.25)
        revision[t] = 0.35 * revision[t - 1] + 0.15 * factor[t] + rng.normal(scale=0.15)
        indicator_revision[t] = (
            0.45 * indicator_revision[t - 1] + 0.10 * factor[t] + rng.normal(scale=0.20)
        )

    loadings = rng.normal(loc=0.8, scale=0.25, size=n_monthly)
    revision_loadings = rng.normal(loc=0.35, scale=0.10, size=n_monthly)
    monthly_mature = factor[:, None] * loadings[None, :] + rng.normal(scale=0.35, size=(n_obs, n_monthly))
    monthly_first = (
        monthly_mature
        + indicator_revision[:, None] * revision_loadings[None, :]
        + rng.normal(scale=0.20, size=(n_obs, n_monthly))
    )
    releases = np.column_stack(
        [
            gdp + revision + rng.normal(scale=0.25, size=n_obs),
            gdp + 0.55 * revision + rng.normal(scale=0.18, size=n_obs),
            gdp + 0.20 * revision + rng.normal(scale=0.14, size=n_obs),
            gdp + rng.normal(scale=0.10, size=n_obs),
        ]
    )

    monthly_first[rng.random(monthly_first.shape) < 0.10] = np.nan
    monthly_mature[rng.random(monthly_mature.shape) < 0.18] = np.nan
    releases[rng.random(releases.shape) < 0.35] = np.nan
    monthly_first_df = pd.DataFrame(monthly_first, columns=[f"x{i + 1}" for i in range(n_monthly)])
    monthly_mature_df = pd.DataFrame(monthly_mature, columns=[f"x{i + 1}" for i in range(n_monthly)])
    release_df = pd.DataFrame(releases, columns=["A", "S", "T", "M"])
    return monthly_first_df, monthly_mature_df, release_df


def main() -> None:
    monthly_first, monthly_mature, releases = make_synthetic_data()
    release_result = fit_release_revision_dfm(
        monthly_first,
        releases,
        config=ReleaseRevisionDFMConfig(max_iter=20, tolerance=1e-4, verbose=False),
    )
    print("release/revision model converged:", release_result.converged)
    print("release/revision iterations:", release_result.n_iter)
    print("release/revision llf tail:", [round(value, 3) for value in release_result.loglikelihood_history[-5:]])
    print("latest GDP fitted values:", forecast_release_row(release_result))

    joint_result = fit_joint_indicator_revision_dfm(
        monthly_first,
        releases,
        monthly_mature_panel=monthly_mature,
        config=JointIndicatorRevisionDFMConfig(max_iter=50, tolerance=3e-4, verbose=False),
    )
    print("joint indicator-revision model converged:", joint_result.converged)
    print("joint indicator-revision iterations:", joint_result.n_iter)
    print("joint indicator-revision llf tail:", [round(value, 3) for value in joint_result.loglikelihood_history[-5:]])
    print("latest joint GDP fitted values:", forecast_gdp_releases(joint_result))
    print("latest indicator revision forecasts:")
    print(forecast_indicator_revisions(joint_result).head().to_string(index=False))


if __name__ == "__main__":
    main()
