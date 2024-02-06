.PHONY: check
check:
	black .
	pytest
	ruff . --fix
	mypy

.PHONY: lint
lint:
	black .
	ruff . --fix
	mypy

.PHONY: serve
serve:
	textual run --dev -c python -m textual_fastdatatable

.PHONY: profile
profile:
	pyinstrument -r html -o profile.html "src/scripts/run_arrow_wide.py"

.PHONY: benchmark
benchmark:
	python src/scripts/benchmark.py > /dev/null
