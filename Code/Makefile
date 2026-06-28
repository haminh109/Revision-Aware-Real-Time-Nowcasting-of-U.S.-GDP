PYTHON ?= python

MAX_ITERS ?= 50 100
MAX_ORIGINS ?= 0
MCS_REPS ?= 5000
MATURE_MAX_ITER ?= 50
MATURE_VARIANTS ?= mature_1y mature_3y mature_latest
FREEZE_NAME ?= radfm_journal_candidate

.PHONY: help install fetch-data stage0 stage1 stage2 timing-inputs validate data-build pipeline-smoke pipeline-full pdf pdf-anonymous pdf-all test clean-pycache

help:
	@echo "RADFM Makefile targets"
	@echo ""
	@echo "Setup and data:"
	@echo "  make install          Install Python requirements"
	@echo "  make fetch-data       Download ALFRED/calendars/BEA data and build Census proxy"
	@echo "  make stage0           Validate raw-data foundation"
	@echo "  make stage1           Build bronze normalized inputs"
	@echo "  make stage2           Build curated silver layer"
	@echo "  make timing-inputs    Build GDP release calendar, mature targets, and SPF benchmark"
	@echo "  make validate         Run Stage 0/1/2 validation checks"
	@echo "  make data-build       Run fetch-data, stage1, stage2, timing-inputs, validate"
	@echo ""
	@echo "Modeling and outputs:"
	@echo "  make pipeline-smoke   Fast smoke run of the journal pipeline"
	@echo "  make pipeline-full    Full journal-candidate pipeline; can take a long time"
	@echo ""
	@echo "Manuscript:"
	@echo "  make pdf              Build main manuscript PDF"
	@echo "  make pdf-anonymous    Build anonymous manuscript PDF"
	@echo "  make pdf-all          Build both manuscript PDFs"
	@echo ""
	@echo "Quality:"
	@echo "  make test             Run pytest"
	@echo "  make clean-pycache    Remove Python cache folders"

install:
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements.txt

fetch-data:
	$(PYTHON) scripts/download_alfred_and_calendars.py
	$(PYTHON) scripts/build_census_proxy_calendar.py
	$(PYTHON) scripts/download_bea.py

stage0:
	$(PYTHON) scripts/validate_stage0.py

stage1:
	$(PYTHON) scripts/01_parse_rtdsm_release_specific.py
	$(PYTHON) scripts/02_parse_rtdsm_complete_vintages.py
	$(PYTHON) scripts/03_parse_alfred_monthly.py
	$(PYTHON) scripts/04_build_release_calendar_master.py

stage2:
	$(PYTHON) scripts/05_build_target_definition_tables.py
	$(PYTHON) scripts/06_build_indicator_metadata.py
	$(PYTHON) scripts/07_build_release_taxonomy_and_mappings.py
	$(PYTHON) scripts/08_build_calendar_coverage_metadata.py
	$(PYTHON) scripts/09_build_silver_curated_layer.py

timing-inputs:
	$(PYTHON) scripts/10_build_gdp_release_calendar_from_alfred.py
	$(PYTHON) scripts/11_build_mature_target_robustness_panels.py
	$(PYTHON) scripts/13_build_public_spf_benchmark.py

validate:
	$(PYTHON) scripts/validate_stage0.py
	$(PYTHON) scripts/validate_stage1.py
	$(PYTHON) scripts/validate_stage2.py

data-build: fetch-data stage1 stage2 timing-inputs validate

pipeline-smoke:
	$(PYTHON) scripts/12_run_journal_candidate_pipeline.py \
		--max-iters 3 \
		--max-origins 1 \
		--mcs-bootstrap-reps 99 \
		--mature-max-iter 2 \
		--mature-variants mature_1y \
		--run-initialization-audit \
		--initialization-seeds 1 2 \
		--initialization-max-iter 3 \
		--initialization-max-origins 1 \
		--freeze-name smoke_journal_candidate_freeze

pipeline-full:
	$(PYTHON) scripts/12_run_journal_candidate_pipeline.py \
		--max-iters $(MAX_ITERS) \
		--max-origins $(MAX_ORIGINS) \
		--mcs-bootstrap-reps $(MCS_REPS) \
		--mature-max-iter $(MATURE_MAX_ITER) \
		--mature-variants $(MATURE_VARIANTS) \
		--run-initialization-audit \
		--initialization-seeds 1 2 3 4 5 \
		--initialization-max-iter $(MATURE_MAX_ITER) \
		--initialization-max-origins $(MAX_ORIGINS) \
		--freeze-name $(FREEZE_NAME)

pdf:
	bash scripts/build_manuscript_pdf.sh main

pdf-anonymous:
	bash scripts/build_manuscript_pdf.sh anonymous

pdf-all: pdf pdf-anonymous

test:
	$(PYTHON) -m pytest

clean-pycache:
	find . -type d -name '__pycache__' -prune -exec rm -rf {} +
	find . -type d -name '.pytest_cache' -prune -exec rm -rf {} +
