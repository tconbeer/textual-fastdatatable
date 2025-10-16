.PHONY: check
check:
	uv sync --group test --group static
	uv run ruff format .
	uv run pytest
	uv run ruff check . --fix
	uv run mypy

.PHONY: lint
lint:
	uv sync --group test --group static
	uv run ruff format .
	uv run ruff check . --fix
	uv run mypy

.PHONY: serve
serve:
	uv sync --group dev
	uv run textual run --dev -c python -m textual_fastdatatable

.PHONY: profile
profile:
	uv sync --group dev
	uv run pyinstrument -r html -o profile.html "src/scripts/run_arrow_wide.py"

.PHONY: benchmark
benchmark:
	uv sync --group dev
	uv run scripts/benchmark.py > /dev/null
