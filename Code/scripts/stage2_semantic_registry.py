from copy import deepcopy

CANONICAL_TARGET_REGISTRY = {
    "routput_first_second_third": {
        "canonical_target_id": "target_gdp_release_stage_growth_rtdsm",
        "target_family": "gdp",
        "target_object_type": "release_stage_growth_target",
        "source_family": "RTDSM",
        "source_dataset": "routput_first_second_third",
        "target_variable_id": "ROUTPUT",
        "measurement_semantics": "quarter_over_quarter_annualized_growth_rate",
        "unit_semantics": "annualized_percent_change",
        "release_structure_type": "first_second_third_and_raw_most_recent_release_stages",
        "comparability_group": "gdp_release_stage_growth_targets_only",
        "is_real_time_release_target": True,
        "is_revision_history_target": False,
        "source_artifact": "data/bronze/targets/gdp_release_targets.csv",
        "notes": (
            "This canonical target represents the RTDSM release-stage GDP growth object. "
            "It preserves first, second, third, and raw most_recent release stages from the source workbook. "
            "The rows do not carry exact row-level release dates in the raw source, so blank release_date values must remain blank downstream."
        ),
    },
    "ROUTPUTQvQd": {
        "canonical_target_id": "target_gdp_complete_vintage_history_rtdsm",
        "target_family": "gdp",
        "target_object_type": "complete_vintage_numeric_history",
        "source_family": "RTDSM",
        "source_dataset": "ROUTPUTQvQd",
        "target_variable_id": "ROUTPUT",
        "measurement_semantics": "quarter_coded_numeric_vintage_history_not_harmonized",
        "unit_semantics": "raw_numeric_unit_not_explicit_in_source_workbook",
        "release_structure_type": "quarter_coded_vintage_history_matrix",
        "comparability_group": "gdp_complete_vintage_numeric_history_only",
        "is_real_time_release_target": False,
        "is_revision_history_target": True,
        "source_artifact": "data/bronze/targets/gdp_complete_vintages_long.csv",
        "notes": (
            "This canonical target represents the RTDSM complete-vintage GDP history object. "
            "It is a separate raw numeric vintage-history object and must not be silently collapsed into the release-stage GDP growth target. "
            "The source exposes quarter-coded vintage labels rather than exact daily vintage dates."
        ),
    },
}

CURRENTLY_UNMAPPED_BLOCK = "currently_unmapped"

DEFAULT_UNMAPPED_NOTE = (
    "No defensible release-calendar mapping is encoded in the current repo for this indicator. "
    "Stage 3 must not force a release-event join for this series without expanding the timing-source layer first."
)


def _indicator_entry(
    *,
    release_block,
    release_block_mapping_status,
    calendar_support_type,
    indicator_family,
    source_family_for_timing,
    mapping_confidence,
    mapping_basis,
    source_type,
    notes,
):
    return {
        "release_block": release_block,
        "release_block_mapping_status": release_block_mapping_status,
        "calendar_support_type": calendar_support_type,
        "indicator_family": indicator_family,
        "source_family_for_timing": source_family_for_timing,
        "mapping_confidence": mapping_confidence,
        "mapping_basis": mapping_basis,
        "source_type": source_type,
        "notes": notes,
    }


INDICATOR_SEMANTIC_REGISTRY = {
    "GDPC1": _indicator_entry(
        release_block="gdp",
        release_block_mapping_status="mapped_official_with_coverage_limits",
        calendar_support_type="official_calendar_supported_with_coverage_limits",
        indicator_family="gdp_reference",
        source_family_for_timing="BEA",
        mapping_confidence="high",
        mapping_basis="Quarterly GDP level series is conceptually tied to the official BEA GDP release block present in the repo calendar layer.",
        source_type="official",
        notes="Official timing support exists through BEA calendar artifacts, but the current repo stores snapshot-style coverage rather than a completed historical archive.",
    ),
    "A191RL1Q225SBEA": _indicator_entry(
        release_block="gdp",
        release_block_mapping_status="mapped_official_with_coverage_limits",
        calendar_support_type="official_calendar_supported_with_coverage_limits",
        indicator_family="gdp_reference",
        source_family_for_timing="BEA",
        mapping_confidence="high",
        mapping_basis="Quarterly real GDP growth series is conceptually tied to the official BEA GDP release block present in the repo calendar layer.",
        source_type="official",
        notes="Official timing support exists through BEA calendar artifacts, but the current repo stores snapshot-style coverage rather than a completed historical archive.",
    ),
    "PAYEMS": _indicator_entry(
        release_block="employment_situation",
        release_block_mapping_status="mapped_official_with_coverage_limits",
        calendar_support_type="official_calendar_supported_with_coverage_limits",
        indicator_family="labor_market",
        source_family_for_timing="BLS",
        mapping_confidence="high",
        mapping_basis="Nonfarm payroll employment is part of the BLS Employment Situation release package represented in the repo calendar layer.",
        source_type="official",
        notes="Official BLS timing support exists, but the current repo calendar coverage is still snapshot/scheduled-page based rather than a completed historical archive.",
    ),
    "UNRATE": _indicator_entry(
        release_block="employment_situation",
        release_block_mapping_status="mapped_official_with_coverage_limits",
        calendar_support_type="official_calendar_supported_with_coverage_limits",
        indicator_family="labor_market",
        source_family_for_timing="BLS",
        mapping_confidence="high",
        mapping_basis="The unemployment rate is part of the BLS Employment Situation release package represented in the repo calendar layer.",
        source_type="official",
        notes="Official BLS timing support exists, but the current repo calendar coverage is still snapshot/scheduled-page based rather than a completed historical archive.",
    ),
    "AWHMAN": _indicator_entry(
        release_block="employment_situation",
        release_block_mapping_status="mapped_official_with_coverage_limits",
        calendar_support_type="official_calendar_supported_with_coverage_limits",
        indicator_family="labor_market",
        source_family_for_timing="BLS",
        mapping_confidence="medium",
        mapping_basis="Average weekly hours in manufacturing is released with the BLS establishment survey package associated with Employment Situation timing.",
        source_type="official",
        notes="Mapped to the Employment Situation block conservatively; official BLS timing support exists in the repo, but historical archive coverage is not yet complete.",
    ),
    "INDPRO": _indicator_entry(
        release_block="industrial_production_capacity_utilization",
        release_block_mapping_status="mapped_official_date_only",
        calendar_support_type="official_calendar_supported_date_only",
        indicator_family="industrial_activity",
        source_family_for_timing="FED_G17",
        mapping_confidence="high",
        mapping_basis="Industrial Production is the headline object of the Federal Reserve G.17 release dates page stored in the repo.",
        source_type="official",
        notes="Official G.17 release dates are available historically, but the source does not publish official intraday release times in the current repo artifacts.",
    ),
    "TCU": _indicator_entry(
        release_block="industrial_production_capacity_utilization",
        release_block_mapping_status="mapped_official_date_only",
        calendar_support_type="official_calendar_supported_date_only",
        indicator_family="industrial_activity",
        source_family_for_timing="FED_G17",
        mapping_confidence="high",
        mapping_basis="Capacity utilization is released in the Federal Reserve G.17 package stored in the repo calendar layer.",
        source_type="official",
        notes="Official G.17 release dates are available historically, but the source does not publish official intraday release times in the current repo artifacts.",
    ),
    "W875RX1": _indicator_entry(
        release_block="personal_income_outlays",
        release_block_mapping_status="mapped_official_with_coverage_limits",
        calendar_support_type="official_calendar_supported_with_coverage_limits",
        indicator_family="income_and_consumption",
        source_family_for_timing="BEA",
        mapping_confidence="medium",
        mapping_basis="Real personal income less transfers is released through the BEA personal income and outlays flow represented in the repo calendar layer.",
        source_type="official",
        notes="Mapped conservatively to the BEA personal income and outlays block; official timing support exists, but current coverage is snapshot-style.",
    ),
    "DSPIC96": _indicator_entry(
        release_block="personal_income_outlays",
        release_block_mapping_status="mapped_official_with_coverage_limits",
        calendar_support_type="official_calendar_supported_with_coverage_limits",
        indicator_family="income_and_consumption",
        source_family_for_timing="BEA",
        mapping_confidence="medium",
        mapping_basis="Real disposable personal income is released through the BEA personal income and outlays flow represented in the repo calendar layer.",
        source_type="official",
        notes="Mapped conservatively to the BEA personal income and outlays block; official timing support exists, but current coverage is snapshot-style.",
    ),
    "PCECC96": _indicator_entry(
        release_block="personal_income_outlays",
        release_block_mapping_status="mapped_official_with_coverage_limits",
        calendar_support_type="official_calendar_supported_with_coverage_limits",
        indicator_family="income_and_consumption",
        source_family_for_timing="BEA",
        mapping_confidence="medium",
        mapping_basis="Real personal consumption expenditures are released through the BEA personal income and outlays flow represented in the repo calendar layer.",
        source_type="official",
        notes="Mapped conservatively to the BEA personal income and outlays block; official timing support exists, but current coverage is snapshot-style.",
    ),
    "RSAFS": _indicator_entry(
        release_block="retail_sales",
        release_block_mapping_status="mapped_proxy",
        calendar_support_type="proxy_supported",
        indicator_family="retail_sales",
        source_family_for_timing="CENSUS_PROXY",
        mapping_confidence="high",
        mapping_basis="Series is part of the explicit Census proxy retail_sales block built from ALFRED realtime availability dates in Stage 0.",
        source_type="proxy",
        notes="Timing support is day-level proxy availability only and must never be treated as official Census release timestamp truth.",
    ),
    "RSXFS": _indicator_entry(
        release_block="retail_sales",
        release_block_mapping_status="mapped_proxy",
        calendar_support_type="proxy_supported",
        indicator_family="retail_sales",
        source_family_for_timing="CENSUS_PROXY",
        mapping_confidence="high",
        mapping_basis="Series is part of the explicit Census proxy retail_sales block built from ALFRED realtime availability dates in Stage 0.",
        source_type="proxy",
        notes="Timing support is day-level proxy availability only and must never be treated as official Census release timestamp truth.",
    ),
    "HOUST": _indicator_entry(
        release_block="housing",
        release_block_mapping_status="mapped_proxy",
        calendar_support_type="proxy_supported",
        indicator_family="housing",
        source_family_for_timing="CENSUS_PROXY",
        mapping_confidence="high",
        mapping_basis="Series is part of the explicit Census proxy housing block built from ALFRED realtime availability dates in Stage 0.",
        source_type="proxy",
        notes="Timing support is day-level proxy availability only and must never be treated as official Census release timestamp truth.",
    ),
    "PERMIT": _indicator_entry(
        release_block="housing",
        release_block_mapping_status="mapped_proxy",
        calendar_support_type="proxy_supported",
        indicator_family="housing",
        source_family_for_timing="CENSUS_PROXY",
        mapping_confidence="high",
        mapping_basis="Series is part of the explicit Census proxy housing block built from ALFRED realtime availability dates in Stage 0.",
        source_type="proxy",
        notes="Timing support is day-level proxy availability only and must never be treated as official Census release timestamp truth.",
    ),
    "DGORDER": _indicator_entry(
        release_block="durable_goods",
        release_block_mapping_status="mapped_proxy",
        calendar_support_type="proxy_supported",
        indicator_family="orders",
        source_family_for_timing="CENSUS_PROXY",
        mapping_confidence="high",
        mapping_basis="Series is part of the explicit Census proxy durable_goods block built from ALFRED realtime availability dates in Stage 0.",
        source_type="proxy",
        notes="Timing support is day-level proxy availability only and must never be treated as official Census release timestamp truth.",
    ),
    "NEWORDER": _indicator_entry(
        release_block="durable_goods",
        release_block_mapping_status="mapped_proxy",
        calendar_support_type="proxy_supported",
        indicator_family="orders",
        source_family_for_timing="CENSUS_PROXY",
        mapping_confidence="high",
        mapping_basis="Series is part of the explicit Census proxy durable_goods block built from ALFRED realtime availability dates in Stage 0.",
        source_type="proxy",
        notes="Timing support is day-level proxy availability only and must never be treated as official Census release timestamp truth.",
    ),
    "BUSINV": _indicator_entry(
        release_block="inventories",
        release_block_mapping_status="mapped_proxy",
        calendar_support_type="proxy_supported",
        indicator_family="inventories",
        source_family_for_timing="CENSUS_PROXY",
        mapping_confidence="high",
        mapping_basis="Series is part of the explicit Census proxy inventories block built from ALFRED realtime availability dates in Stage 0.",
        source_type="proxy",
        notes="Timing support is day-level proxy availability only and must never be treated as official Census release timestamp truth.",
    ),
    "ISRATIO": _indicator_entry(
        release_block="inventories",
        release_block_mapping_status="mapped_proxy",
        calendar_support_type="proxy_supported",
        indicator_family="inventories",
        source_family_for_timing="CENSUS_PROXY",
        mapping_confidence="high",
        mapping_basis="Series is part of the explicit Census proxy inventories block built from ALFRED realtime availability dates in Stage 0.",
        source_type="proxy",
        notes="Timing support is day-level proxy availability only and must never be treated as official Census release timestamp truth.",
    ),
    "BOPGSTB": _indicator_entry(
        release_block="trade",
        release_block_mapping_status="mapped_proxy",
        calendar_support_type="proxy_supported",
        indicator_family="trade",
        source_family_for_timing="CENSUS_PROXY",
        mapping_confidence="high",
        mapping_basis="Series is part of the explicit Census proxy trade block built from ALFRED realtime availability dates in Stage 0.",
        source_type="proxy",
        notes="Timing support is day-level proxy availability only and must never be treated as official Census release timestamp truth.",
    ),
    "BOPTEXP": _indicator_entry(
        release_block="trade",
        release_block_mapping_status="mapped_proxy",
        calendar_support_type="proxy_supported",
        indicator_family="trade",
        source_family_for_timing="CENSUS_PROXY",
        mapping_confidence="high",
        mapping_basis="Series is part of the explicit Census proxy trade block built from ALFRED realtime availability dates in Stage 0.",
        source_type="proxy",
        notes="Timing support is day-level proxy availability only and must never be treated as official Census release timestamp truth.",
    ),
    "BOPTIMP": _indicator_entry(
        release_block="trade",
        release_block_mapping_status="mapped_proxy",
        calendar_support_type="proxy_supported",
        indicator_family="trade",
        source_family_for_timing="CENSUS_PROXY",
        mapping_confidence="high",
        mapping_basis="Series is part of the explicit Census proxy trade block built from ALFRED realtime availability dates in Stage 0.",
        source_type="proxy",
        notes="Timing support is day-level proxy availability only and must never be treated as official Census release timestamp truth.",
    ),
    "UMCSENT": _indicator_entry(
        release_block=CURRENTLY_UNMAPPED_BLOCK,
        release_block_mapping_status="currently_unmapped",
        calendar_support_type="currently_unmapped",
        indicator_family="survey_sentiment",
        source_family_for_timing="UNMAPPED",
        mapping_confidence="none",
        mapping_basis="No official University of Michigan release-calendar source is currently stored in the repo calendar layer.",
        source_type="unmapped",
        notes=DEFAULT_UNMAPPED_NOTE,
    ),
    "FEDFUNDS": _indicator_entry(
        release_block=CURRENTLY_UNMAPPED_BLOCK,
        release_block_mapping_status="currently_unmapped",
        calendar_support_type="currently_unmapped",
        indicator_family="financial_rates",
        source_family_for_timing="UNMAPPED",
        mapping_confidence="none",
        mapping_basis="No defensible event-style release calendar mapping is encoded in the current repo for this policy-rate series.",
        source_type="unmapped",
        notes=DEFAULT_UNMAPPED_NOTE,
    ),
    "TB3MS": _indicator_entry(
        release_block=CURRENTLY_UNMAPPED_BLOCK,
        release_block_mapping_status="currently_unmapped",
        calendar_support_type="currently_unmapped",
        indicator_family="financial_rates",
        source_family_for_timing="UNMAPPED",
        mapping_confidence="none",
        mapping_basis="No defensible event-style release calendar mapping is encoded in the current repo for this rate series.",
        source_type="unmapped",
        notes=DEFAULT_UNMAPPED_NOTE,
    ),
    "GS10": _indicator_entry(
        release_block=CURRENTLY_UNMAPPED_BLOCK,
        release_block_mapping_status="currently_unmapped",
        calendar_support_type="currently_unmapped",
        indicator_family="financial_rates",
        source_family_for_timing="UNMAPPED",
        mapping_confidence="none",
        mapping_basis="No defensible event-style release calendar mapping is encoded in the current repo for this rate series.",
        source_type="unmapped",
        notes=DEFAULT_UNMAPPED_NOTE,
    ),
    "T10Y3MM": _indicator_entry(
        release_block=CURRENTLY_UNMAPPED_BLOCK,
        release_block_mapping_status="currently_unmapped",
        calendar_support_type="currently_unmapped",
        indicator_family="financial_rates",
        source_family_for_timing="UNMAPPED",
        mapping_confidence="none",
        mapping_basis="No defensible event-style release calendar mapping is encoded in the current repo for this yield-spread series.",
        source_type="unmapped",
        notes=DEFAULT_UNMAPPED_NOTE,
    ),
    "SP500": _indicator_entry(
        release_block=CURRENTLY_UNMAPPED_BLOCK,
        release_block_mapping_status="currently_unmapped",
        calendar_support_type="currently_unmapped",
        indicator_family="financial_rates",
        source_family_for_timing="UNMAPPED",
        mapping_confidence="none",
        mapping_basis="Optional series placeholder; no release-calendar mapping is encoded in the current repo.",
        source_type="unmapped",
        notes=DEFAULT_UNMAPPED_NOTE,
    ),
    "NAPM": _indicator_entry(
        release_block=CURRENTLY_UNMAPPED_BLOCK,
        release_block_mapping_status="currently_unmapped",
        calendar_support_type="currently_unmapped",
        indicator_family="survey_sentiment",
        source_family_for_timing="UNMAPPED",
        mapping_confidence="none",
        mapping_basis="Optional series placeholder; no release-calendar mapping is encoded in the current repo.",
        source_type="unmapped",
        notes=DEFAULT_UNMAPPED_NOTE,
    ),
}


CALENDAR_COVERAGE_REGISTRY = {
    ("BEA", "full_release_schedule"): {
        "source_family": "BEA",
        "source_subsource": "full_release_schedule",
        "source_type": "official",
        "observed_coverage_scope": "current_year_snapshot",
        "coverage_status": "official_snapshot_only",
        "intended_research_role": "official_release_timing_for_bea_targets_and_bea_indicator_blocks",
        "is_historical_archive_complete": False,
        "is_current_snapshot_only": True,
        "has_official_release_time": True,
        "has_only_release_date": False,
        "is_proxy": False,
        "downstream_usage_constraint": (
            "Do not treat the current BEA calendar artifact as a completed historical archive. "
            "Stage 3 must extend raw BEA calendar coverage or restrict any exact-vintage exercise to the covered window."
        ),
        "notes": "Official BEA schedule rows carry release dates and official Eastern Times, but the repo currently stores a current-year snapshot rather than a historical archive.",
    },
    ("BLS", "current_year"): {
        "source_family": "BLS",
        "source_subsource": "current_year",
        "source_type": "official",
        "observed_coverage_scope": "current_year_snapshot",
        "coverage_status": "official_snapshot_only",
        "intended_research_role": "official_release_timing_for_general_bls_monthly_release_blocks",
        "is_historical_archive_complete": False,
        "is_current_snapshot_only": True,
        "has_official_release_time": True,
        "has_only_release_date": False,
        "is_proxy": False,
        "downstream_usage_constraint": (
            "Use only with explicit recognition that the current repo stores a current-year BLS snapshot, not a completed historical archive."
        ),
        "notes": "Official BLS current-year release calendar with release dates and official Eastern Times, but not a historical archive.",
    },
    ("BLS", "employment_situation"): {
        "source_family": "BLS",
        "source_subsource": "employment_situation",
        "source_type": "official",
        "observed_coverage_scope": "scheduled_release_specific_page",
        "coverage_status": "official_scheduled_page_snapshot",
        "intended_research_role": "official_release_timing_for_employment_situation_block",
        "is_historical_archive_complete": False,
        "is_current_snapshot_only": True,
        "has_official_release_time": True,
        "has_only_release_date": False,
        "is_proxy": False,
        "downstream_usage_constraint": (
            "Employment Situation timing is official, but the current repo stores a scheduled-page snapshot rather than a completed historical archive."
        ),
        "notes": "Official BLS Employment Situation schedule page with release dates and official Eastern Times for recent and scheduled releases.",
    },
    ("FED_G17", "release_dates"): {
        "source_family": "FED_G17",
        "source_subsource": "release_dates",
        "source_type": "official",
        "observed_coverage_scope": "historical_and_scheduled",
        "coverage_status": "official_historical_dates_only",
        "intended_research_role": "official_release_dates_for_industrial_production_and_capacity_utilization",
        "is_historical_archive_complete": True,
        "is_current_snapshot_only": False,
        "has_official_release_time": False,
        "has_only_release_date": True,
        "is_proxy": False,
        "downstream_usage_constraint": (
            "Safe for date-level joins to the G.17 release, but the repo must keep release_time_et blank because the source page does not provide official intraday times."
        ),
        "notes": "Official Federal Reserve G.17 release dates page appears to provide historical and scheduled release dates, but not official release times.",
    },
    ("CENSUS_PROXY", "alfred_proxy_calendar"): {
        "source_family": "CENSUS_PROXY",
        "source_subsource": "alfred_proxy_calendar",
        "source_type": "proxy",
        "observed_coverage_scope": "proxy_from_available_vintages",
        "coverage_status": "proxy_availability_calendar",
        "intended_research_role": "day_level_proxy_availability_logic_for_census_related_indicator_blocks",
        "is_historical_archive_complete": False,
        "is_current_snapshot_only": False,
        "has_official_release_time": False,
        "has_only_release_date": True,
        "is_proxy": True,
        "downstream_usage_constraint": (
            "Use only as a proxy day-level availability calendar. "
            "Do not label these rows as official Census release timestamps and do not fabricate release_time_et values."
        ),
        "notes": "ALFRED-based proxy calendar built from realtime_start availability dates for Census-related indicator blocks already used in the repo.",
    },
}


def get_target_registry():
    return deepcopy(CANONICAL_TARGET_REGISTRY)


def get_target_definition(source_dataset: str):
    return deepcopy(CANONICAL_TARGET_REGISTRY[source_dataset])


def build_default_indicator_entry(series_id: str):
    return {
        "release_block": CURRENTLY_UNMAPPED_BLOCK,
        "release_block_mapping_status": "currently_unmapped",
        "calendar_support_type": "currently_unmapped",
        "indicator_family": "unclassified",
        "source_family_for_timing": "UNMAPPED",
        "mapping_confidence": "none",
        "mapping_basis": "No curated Stage 2 semantic registry entry exists for this series yet.",
        "source_type": "unmapped",
        "notes": DEFAULT_UNMAPPED_NOTE,
    }


def get_indicator_definition(series_id: str):
    entry = INDICATOR_SEMANTIC_REGISTRY.get(series_id)
    if entry is None:
        entry = build_default_indicator_entry(series_id)
    output = deepcopy(entry)
    output["canonical_indicator_id"] = f"indicator_{series_id.lower()}"
    return output


def get_indicator_registry():
    return {series_id: get_indicator_definition(series_id) for series_id in INDICATOR_SEMANTIC_REGISTRY}


def get_calendar_coverage_definition(source_family: str, source_subsource: str):
    key = (source_family, source_subsource)
    if key not in CALENDAR_COVERAGE_REGISTRY:
        raise KeyError(f"No Stage 2 calendar coverage registry entry for {(source_family, source_subsource)}")
    return deepcopy(CALENDAR_COVERAGE_REGISTRY[key])


def get_calendar_coverage_registry():
    return {key: deepcopy(value) for key, value in CALENDAR_COVERAGE_REGISTRY.items()}
