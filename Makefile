PYTHON ?= python
ORDERS ?= 500000
CUSTOMERS ?= 50000
PRODUCTS ?= 5000

setup:
	$(PYTHON) -m pip install -r requirements.txt

generate:
	$(PYTHON) -m src.generate_data --orders $(ORDERS) --customers $(CUSTOMERS) --products $(PRODUCTS)

bronze:
	$(PYTHON) -m src.bronze_ingest

silver:
	$(PYTHON) -m src.silver_transform

quality:
	$(PYTHON) -m src.quality_gate

gold:
	$(PYTHON) -m src.gold_models

export:
	$(PYTHON) -m src.export_silver

dbt:
	cd dbt && dbt build --profiles-dir .

benchmark:
	$(PYTHON) -m src.benchmark --runs 3

pipeline: generate bronze silver quality gold export dbt benchmark

test:
	pytest -q
