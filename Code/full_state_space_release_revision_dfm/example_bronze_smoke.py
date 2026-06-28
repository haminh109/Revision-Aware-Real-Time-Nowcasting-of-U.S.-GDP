from __future__ import annotations

from pathlib import Path

from full_state_space_release_revision_dfm import (
    JointIndicatorRevisionDFMConfig,
    align_quarterly_model_panels,
    fit_joint_indicator_revision_dfm,
    forecast_gdp_releases,
    forecast_indicator_revisions,
    load_alfred_first_mature_monthly_panels,
    load_gdp_release_panel,
)


DEFAULT_SERIES = ["PAYEMS", "UNRATE", "INDPRO", "TCU", "W875RX1", "HOUST"]


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    first, mature = load_alfred_first_mature_monthly_panels(
        repo_root / "data/bronze/indicators/alfred_monthly_long.csv",
        series_ids=DEFAULT_SERIES,
        start="1992-01-01",
    )
    releases = load_gdp_release_panel(repo_root / "data/bronze/targets/gdp_release_targets.csv")
    first_q, mature_q, release_q = align_quarterly_model_panels(first, mature, releases)

    # Smoke mode deliberately uses the latest 80 quarters and few EM iterations.
    first_q = first_q.tail(80)
    mature_q = mature_q.tail(80)
    release_q = release_q.tail(80)

    result = fit_joint_indicator_revision_dfm(
        first_q,
        release_q,
        monthly_mature_panel=mature_q,
        config=JointIndicatorRevisionDFMConfig(max_iter=5, tolerance=1e-4),
    )
    print("quarterly panel shape:", first_q.shape, release_q.shape)
    print("iterations:", result.n_iter)
    print("loglikelihood tail:", [round(value, 3) for value in result.loglikelihood_history[-5:]])
    print("latest GDP fitted values:", forecast_gdp_releases(result))
    print("indicator revision forecast head:")
    print(forecast_indicator_revisions(result).head().to_string(index=False))


if __name__ == "__main__":
    main()
