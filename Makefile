
.PHONY: data

data:
	python src/data_pipeline.py
	python src/build_warehouse.py
