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
