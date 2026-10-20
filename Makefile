
.PHONY: data

data:
	python src/verify_manifest.py
	python src/data_pipeline.py
	python src/data_quality_agent.py
	python src/build_warehouse.py

