from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.request import urlretrieve
import re
import zipfile

import numpy as np
import pandas as pd


RELEASE_ORDER = ("A", "S", "T", "M")
RATE_OR_LEVEL_SERIES = {"UNRATE", "TCU", "FEDFUNDS", "GS10", "TB3MS", "T10Y3MM", "UMCSENT"}
PHILLY_FED_SPF_RGDP_MEAN_GROWTH_URL = (
    "https://www.philadelphiafed.org/-/media/FRBP/Assets/Surveys-And-Data/"
    "survey-of-professional-forecasters/data-files/files/Mean_RGDP_Growth.xlsx"
)


def _sanitize_xlsx_core_properties(path: Path) -> None:
    """Normalize loose XLSX core-property datetimes before openpyxl reads them.

    Some public Philadelphia Fed SPF workbooks are valid enough for Excel but
    contain W3CDTF metadata such as ``T 2:31:52`` instead of ``T02:31:52``.
    Recent openpyxl versions reject that metadata before reading worksheet data.
    The worksheet contents are unchanged; only docProps/core.xml is normalized.
    """

    with zipfile.ZipFile(path, "r") as source_zip:
        names = source_zip.namelist()
        if "docProps/core.xml" not in names:
            return
        core_xml = source_zip.read("docProps/core.xml").decode("utf-8", errors="replace")

    fixed_xml = re.sub(r"T\s+(\d):", r"T0\1:", core_xml)
    if fixed_xml == core_xml:
        return

    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with zipfile.ZipFile(path, "r") as source_zip, zipfile.ZipFile(tmp_path, "w", compression=zipfile.ZIP_DEFLATED) as target_zip:
        for item in source_zip.infolist():
            data = source_zip.read(item.filename)
            if item.filename == "docProps/core.xml":
                data = fixed_xml.encode("utf-8")
            target_zip.writestr(item, data)
    tmp_path.replace(path)


@dataclass(slots=True)
class SPFBenchmark:
    """Optional external benchmark forecasts aligned by forecast origin."""

    forecasts: pd.DataFrame

    @classmethod
    def from_csv(cls, path: str | Path | None) -> "SPFBenchmark | None":
        if path is None:
            return None
        file_path = Path(path)
        if not file_path.exists():
            return None
        frame = pd.read_csv(file_path, parse_dates=["forecast_origin_date"])
        required = {"forecast_origin_date", "target_quarter", "target_id", "forecast_value"}
        missing = required.difference(frame.columns)
        if missing:
            raise ValueError(f"SPF benchmark file is missing columns: {sorted(missing)}")
        frame = frame.copy()
        frame["target_quarter"] = frame["target_quarter"].astype(str)
        frame["target_id"] = frame["target_id"].astype(str)
        frame["forecast_value"] = pd.to_numeric(frame["forecast_value"], errors="coerce")
        frame = frame.dropna(subset=["forecast_origin_date", "forecast_value"])
        return cls(frame.sort_values(["target_quarter", "target_id", "forecast_origin_date"]))

    def forecast_for_origin(self, target_quarter: str, origin_date: pd.Timestamp) -> dict[str, float]:
        rows = self.forecasts[
            self.forecasts["target_quarter"].eq(str(target_quarter))
            & (self.forecasts["forecast_origin_date"] <= pd.Timestamp(origin_date))
        ]
        if rows.empty:
            return {}
        latest = rows.sort_values("forecast_origin_date").groupby("target_id", as_index=False).tail(1)
        return {
            str(row.target_id): float(row.forecast_value)
            for row in latest.itertuples(index=False)
            if str(row.target_id) in RELEASE_ORDER and np.isfinite(float(row.forecast_value))
        }


def _period_label(period: pd.Period) -> str:
    return f"{period.year}:Q{period.quarter}"


def _spf_conservative_availability_date(survey_period: pd.Period) -> pd.Timestamp:
    """Availability proxy for public SPF files that omit exact survey release dates.

    Philadelphia Fed SPF quarterly files identify the survey quarter but not a
    row-level public release date. The second month-end of the survey quarter is
    conservative for this project: it is later than the usual SPF publication
    window, yet still before the same-quarter GDP advance-release checkpoint.
    """

    second_month = survey_period.asfreq("M", how="start") + 1
    return second_month.to_timestamp(how="end").normalize()


def normalize_spf_rgdp_growth(
    raw: pd.DataFrame,
    *,
    statistic: str = "mean",
    source_url: str = PHILLY_FED_SPF_RGDP_MEAN_GROWTH_URL,
) -> pd.DataFrame:
    """Normalize Philadelphia Fed SPF RGDP growth files to benchmark schema.

    The public `DRGDP2` forecast is the current survey-quarter annualized RGDP
    growth forecast; `DRGDP3` is one quarter ahead, and so on. SPF forecasts do
    not target BEA release rounds, so the same professional forecast is exposed
    as A/S/T/M point forecasts. Revision forecasts implied from SPF are therefore
    a zero-revision external benchmark.
    """

    frame = raw.copy()
    if not {"YEAR", "QUARTER"}.issubset(frame.columns):
        raise ValueError("SPF RGDP growth file must contain YEAR and QUARTER columns")
    rows: list[dict[str, object]] = []
    horizon_cols = [col for col in frame.columns if str(col).upper().startswith("DRGDP")]
    for row in frame.itertuples(index=False):
        year = int(getattr(row, "YEAR"))
        quarter = int(getattr(row, "QUARTER"))
        survey_period = pd.Period(year=year, quarter=quarter, freq="Q")
        forecast_origin_date = _spf_conservative_availability_date(survey_period)
        for col in horizon_cols:
            horizon_number = int(str(col).upper().replace("DRGDP", ""))
            horizon_quarters = horizon_number - 2
            target_period = survey_period + horizon_quarters
            value = pd.to_numeric(getattr(row, col), errors="coerce")
            if not np.isfinite(value):
                continue
            for target_id in RELEASE_ORDER:
                rows.append(
                    {
                        "forecast_origin_date": forecast_origin_date,
                        "target_quarter": _period_label(target_period),
                        "target_id": target_id,
                        "forecast_value": float(value),
                        "spf_survey_quarter": _period_label(survey_period),
                        "spf_horizon_quarters": int(horizon_quarters),
                        "spf_column": str(col),
                        "source_statistic": statistic,
                        "source_url": source_url,
                        "availability_rule": "survey_quarter_second_month_end_conservative_proxy",
                    }
                )
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(["target_quarter", "target_id", "forecast_origin_date", "spf_horizon_quarters"]).reset_index(drop=True)


def build_public_spf_rgdp_growth_csv(
    output_path: str | Path,
    *,
    source_url: str = PHILLY_FED_SPF_RGDP_MEAN_GROWTH_URL,
    raw_cache_path: str | Path | None = None,
    statistic: str = "mean",
) -> pd.DataFrame:
    """Download and normalize the public Philadelphia Fed SPF RGDP benchmark."""

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    if raw_cache_path is None:
        raw_cache = output.parent / "Mean_RGDP_Growth.xlsx"
    else:
        raw_cache = Path(raw_cache_path)
    raw_cache.parent.mkdir(parents=True, exist_ok=True)
    urlretrieve(source_url, raw_cache)
    _sanitize_xlsx_core_properties(raw_cache)
    raw = pd.read_excel(raw_cache)
    normalized = normalize_spf_rgdp_growth(raw, statistic=statistic, source_url=source_url)
    normalized.to_csv(output, index=False)
    return normalized


def _ols_predict(X_train: np.ndarray, y_train: np.ndarray, x_current: np.ndarray, ridge: float = 1e-4) -> float:
    X = np.asarray(X_train, dtype=float)
    y = np.asarray(y_train, dtype=float)
    x = np.asarray(x_current, dtype=float)
    if X.ndim == 1:
        X = X.reshape(-1, 1)
    mask = np.isfinite(y) & np.isfinite(X).all(axis=1)
    if mask.sum() < 8:
        clean = y[np.isfinite(y)]
        return float(clean[-1]) if clean.size else np.nan
    X = np.nan_to_num(X[mask], nan=0.0, posinf=0.0, neginf=0.0)
    y = np.nan_to_num(y[mask], nan=0.0, posinf=0.0, neginf=0.0)
    X = np.column_stack([np.ones(len(X)), X])
    x = np.r_[1.0, np.nan_to_num(x, nan=0.0, posinf=0.0, neginf=0.0)]
    if ridge > 0:
        penalty = np.sqrt(ridge) * np.eye(X.shape[1])
        penalty[0, 0] = 0.0
        X = np.vstack([X, penalty])
        y = np.r_[y, np.zeros(X.shape[1])]
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    return float(x @ beta)


def _ar_one_step(series: pd.Series, max_lag: int = 4) -> float:
    values = pd.to_numeric(series, errors="coerce").dropna().to_numpy(dtype=float)
    if values.size == 0:
        return np.nan
    if values.size < 12:
        return float(values[-1])
    lag = min(max_lag, max(1, values.size // 8))
    y = values[lag:]
    X = np.column_stack([values[lag - j - 1 : values.size - j - 1] for j in range(lag)])
    x_current = np.array([values[-j - 1] for j in range(lag)])
    return _ols_predict(X, y, x_current, ridge=1e-5)


def forecast_no_revision(release_train: pd.DataFrame, checkpoint_id: str) -> dict[str, float]:
    """Naive benchmark that sets adjacent unreleased GDP revisions to zero.

    Before the advance release, no same-quarter GDP value is known, so the base
    level is a one-step AR forecast of A. Before the second release, the base is
    the known A value. Before the third release, the base is the known S value.
    """

    current = release_train.iloc[-1]
    history = release_train.iloc[:-1]
    if checkpoint_id == "pre_advance" or not np.isfinite(current.get("A", np.nan)):
        base = _ar_one_step(history["A"])
        return {target_id: base for target_id in RELEASE_ORDER}
    if checkpoint_id == "pre_second" or not np.isfinite(current.get("S", np.nan)):
        base = float(current["A"])
        return {"A": base, "S": base, "T": base, "M": base}
    base = float(current["S"])
    return {"A": float(current["A"]), "S": base, "T": base, "M": base}


def _transform_monthly_panel(panel: pd.DataFrame) -> pd.DataFrame:
    transformed = pd.DataFrame(index=panel.index)
    for col in panel.columns:
        series = pd.to_numeric(panel[col], errors="coerce").astype(float)
        if col in RATE_OR_LEVEL_SERIES:
            transformed[col] = series.diff()
        elif (series.dropna() > 0).all():
            with np.errstate(invalid="ignore", divide="ignore"):
                transformed[col] = 1200.0 * np.log(series).diff()
        else:
            transformed[col] = series.diff()
    return transformed.replace([np.inf, -np.inf], np.nan)


def _quarter_monthly_lag_features(monthly_panel: pd.DataFrame, quarter_index: pd.Index, n_lags: int) -> pd.DataFrame:
    monthly = _transform_monthly_panel(monthly_panel)
    if not isinstance(monthly.index, pd.DatetimeIndex):
        monthly = monthly.copy()
        monthly.index = pd.to_datetime(monthly.index)
    rows: list[dict[str, float | str]] = []
    for quarter_label in quarter_index:
        year, quarter = str(quarter_label).split(":Q")
        period = pd.Period(year=int(year), quarter=int(quarter), freq="Q")
        end_month = period.asfreq("M", how="end").to_timestamp(how="end").normalize()
        row: dict[str, float | str] = {"target_quarter": str(quarter_label)}
        for lag in range(n_lags):
            month = (end_month.to_period("M") - lag).to_timestamp()
            if month not in monthly.index:
                for series_id in monthly.columns:
                    row[f"{series_id}_m{lag}"] = np.nan
                continue
            values = monthly.loc[month]
            for series_id, value in values.items():
                row[f"{series_id}_m{lag}"] = float(value) if np.isfinite(value) else np.nan
        rows.append(row)
    features = pd.DataFrame(rows).set_index("target_quarter")
    return features.reindex(quarter_index.astype(str))


def _standardize_and_fill(features: pd.DataFrame, train_index: pd.Index) -> pd.DataFrame:
    train = features.loc[features.index.intersection(train_index.astype(str))]
    means = train.mean(skipna=True)
    scales = train.std(skipna=True).replace(0.0, np.nan).fillna(1.0)
    standardized = (features - means) / scales
    return standardized.replace([np.inf, -np.inf], np.nan).fillna(standardized.loc[train.index].mean()).fillna(0.0)


def _known_release_columns(release_train: pd.DataFrame, checkpoint_id: str) -> list[str]:
    if checkpoint_id == "pre_second":
        return ["A"]
    if checkpoint_id == "pre_third":
        return ["A", "S"]
    return []


def forecast_midas_umidas(
    monthly_panel: pd.DataFrame,
    release_train: pd.DataFrame,
    checkpoint_id: str,
    *,
    n_lags: int = 6,
    ridge: float = 1e-3,
) -> dict[str, float]:
    """Unrestricted MIDAS benchmark using separate monthly lag coefficients."""

    quarter_index = release_train.index.astype(str)
    train_index = quarter_index[:-1]
    current_index = quarter_index[-1]
    lag_features = _quarter_monthly_lag_features(monthly_panel, quarter_index, n_lags=n_lags)
    lag_features = _standardize_and_fill(lag_features, pd.Index(train_index))
    design = lag_features.copy()
    for col in _known_release_columns(release_train, checkpoint_id):
        design[f"known_{col}"] = release_train[col].to_numpy(dtype=float)
    design = design.fillna(design.loc[train_index].mean()).fillna(0.0)
    forecasts: dict[str, float] = {}
    for target in RELEASE_ORDER:
        y = pd.to_numeric(release_train[target], errors="coerce")
        forecasts[target] = _ols_predict(
            design.loc[train_index].to_numpy(dtype=float),
            y.iloc[:-1].to_numpy(dtype=float),
            design.loc[current_index].to_numpy(dtype=float),
            ridge=ridge,
        )
    return forecasts
