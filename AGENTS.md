# AGENTS.md

This file provides guidance to coding agents (including Claude Code) when working with
code in this repository.

## Project

`textual-fastdatatable` is a performance-focused reimplementation of Textual's built-in
`DataTable` widget, with a pluggable data storage backend. It is a library (published to
PyPI, consumed by [harlequin](https://github.com/tconbeer/harlequin)), not an application.
The performance win comes from never materializing the whole dataset as Python objects:
the data stays in a columnar store (Arrow or Polars) and only visible cells are converted
and rendered.

## Commands

Dependency management is `uv`; every command runs through `uv run`.

```bash
make check        # sync deps, ruff format, pytest, ruff check --fix, mypy
make lint         # same, without the tests
make serve        # run the demo app (src/textual_fastdatatable/__main__.py) in textual dev mode
make benchmark    # scripts/benchmark.py: this widget vs. Textual's built-in, over tests/data/*.parquet
make profile      # pyinstrument HTML profile of a wide-table render

uv run pytest tests/unit_tests/test_backends.py                    # one file
uv run pytest tests/unit_tests/test_backends.py::test_get_cell_at  # one test
uv run pytest -k "sort"                                            # by name
uv run pytest --snapshot-update                                    # accept new snapshot SVGs
```

CI (`.github/workflows/test.yml`) runs `pytest` on Python 3.10–3.14 × Linux/macOS/Windows;
`static.yml` runs `ruff format --diff`, `ruff check`, and `mypy --no-incremental`.

## Architecture

### Two layers: widget and backend

`data_table.py` (~2800 lines) is a fork of Textual's `DataTable`, rewritten so that it
holds no row/cell objects of its own. Instead it asks a `DataTableBackend` for values by
integer index. There are no `RowKey`/`ColumnKey` identity objects as in upstream Textual —
rows and columns are addressed positionally, which is why sorting invalidates caches via
`_update_count` rather than remapping keys.

`backend.py` defines the ABC `DataTableBackend[_TableTypeT]` plus two implementations:

- `ArrowBackend` (pyarrow, always available) — also handles pandas DataFrames via
  `pa.Table.from_pandas`, and parquet files via `pq.read_table`.
- `PolarsBackend` — defined only when polars imports (`if _HAS_POLARS:` guard at module
  level). It is the backend for CSV/JSON/IPC file paths, so those formats require the
  `polars` extra.

`create_backend()` is the dispatch point for `DataTable(data=...)`: it type-tests the input
and picks a backend. Note `_is_pandas_dataframe` deliberately checks `sys.modules` instead
of importing pandas — pandas is not a dependency of this package.

Its optional `column_names` argument carries labels the caller has but the data doesn't
(a cursor description, say). It is threaded down to each backend's `__init__` and applied
*before* the duplicate-name de-duplication, so `ArrowBackend.source_data` keeps duplicates
verbatim while `.data` gets `a`, `a0`. `_relabel` holds the rules: no columns at all means
build an empty table from the names; one name per column means rename; any other count
means the data's own names win.

The contract a backend must satisfy is narrow and index-based: `row_count`,
`column_count`, `columns`, `column_content_widths`, `get_row_at`/`get_column_at`/
`get_cell_at`, and the mutation methods (`append_rows`, `append_column`, `drop_row`,
`update_cell`, `sort`). Mutations are implemented by rebuilding the immutable table
(`pa.Table.from_batches`, `pl.concat`) and are expected to be slow; the design optimizes
for large immutable data.

`max_rows` truncates the displayed data while `source_data`/`source_row_count` keep
reporting the full input — the widget shows both counts.

### Column widths

`column_content_widths` is the hot path for first paint. Each backend computes it with
vectorized column operations rather than per-cell Python (`_measure`): booleans and nulls
are constants, numerics measure only min/max, temporals measure one non-null value, and
everything else casts the whole column to string and takes the widest result of
`_measure_strings`. That runs `_cell_widths` as an Arrow scalar UDF, which measures an
array in cells rather than characters: an array with as many bytes as characters is all
ASCII, so Arrow's own character count is its width, and anything else is measured with
`wcwidth`, once per distinct non-ASCII value, and mapped back over the rows by Arrow.
The per-value fallback for non-strings is `backend._measure_width`, a lazy wrapper
around `format.measure_width` (see Conventions). The result
is cached on the backend and cleared by `_reset_content_widths()` on mutation. The Arrow
path registers another scalar UDF as a fallback for types Arrow can't cast to string.

Both UDFs are registered through `_register_udf`, which registers a name at most once:
`pc.register_scalar_function` raises for a name that is taken **and drops a reference to
the function already registered under it**, so re-registering segfaults pyarrow a couple
of calls later. `tests/unit_tests/test_backends.py::test_column_content_widths_are_repeatable`
guards this.

`format.measure_width` is the one place a width is measured: it formats the value with
`cell_formatter` and measures the result in cells against a console. The widget passes
the app's, via `DataTable._measure`, so a column is never wider than the screen; callers
with no app (the backend, and the widget before it mounts) get the console this module
builds lazily, which is `MAX_MEASURE_WIDTH` wide, so nothing caps those measurements.
Widths are never counted with `len()` — a character can occupy two cells or none. The
widget measures column labels this way too and folds the result into
`Column.content_width` in `ordered_columns` and `add_column`, so `Column` itself only
adds padding.

`column.Column` turns those content widths into render widths (`+ CELL_X_PADDING`,
clamped by `max_column_content_width` when set). It also detects ID-ish column names by
regex so `format.cell_formatter` omits thousands separators for those integers.

### Rendering and caches

`format.cell_formatter` converts a raw Python value to a Rich renderable — right-aligning
numbers/dates, locale-formatting via `{obj:n}` (callers should `locale.setlocale()` first),
escaping or parsing markup depending on `render_markup`, and rendering `datetime.max`/
`date.max` (produced by `_handle_overflow` when Arrow values overflow Python types) as ∞.

The render path is `render_line` → `_render_line_in_row` → `_render_cell`, each backed by
an `LRUCache` (`_line_cache`, `_row_render_cache`, `_cell_render_cache`, `_tooltip_cache`).
Cache keys include `_update_count` and `_pseudo_class_state`, so **any state change that
affects appearance must either be part of a cache key or call `_clear_caches()`** — a
stale cell is the classic bug here. Dimension recalculation is deferred to `_on_idle` via
`_require_update_dimensions`.

### Divergences from upstream Textual DataTable

Row heights are always 1 line; row labels are not supported (the related snapshot test is
skipped). `cursor_type="range"` adds shift-based range selection and a `SelectionCopied`
message on `ctrl+c`/`super+c` — this requires the host app to be built with
`inherit_bindings=False`. Widget-emitted messages are nested classes on `DataTable`
(`CellHighlighted`, `SelectionCopied`, `DataLoadError`, …); `DataLoadError` is posted from
`__init__` when `create_backend` raises, leaving `self.backend is None`.

## Conventions

- mypy runs in strict mode over `src/` and `tests/unit_tests/`. Local pyarrow stubs live
  in `stubs/` (`mypy_path = "stubs,src"`); if you touch a pyarrow API mypy doesn't know
  about, add it to the stub rather than adding `# type: ignore`.
- Backend tests use the `backend` fixture in `tests/conftest.py`, which is parametrized
  over `ArrowBackend` and `PolarsBackend` — behavior changes should hold for both.
- Snapshot tests (`tests/snapshot_tests/`) each mount a tiny app from `snapshot_apps/` and
  compare rendered SVGs. Review diffs before running `--snapshot-update`.
- `backend.py` must stay importable without Textual or rich, so that downstream consumers
  (harlequin's headless `hsql` CLI) can use `create_backend()` as a normalizer without
  paying for display. `format.py` and `column.py` may use rich, but must stay free of
  Textual. Four deliberate deferrals keep this true:
  - `__init__.py` imports `DataTable` lazily via a module-level `__getattr__` (PEP 562),
    with a `TYPE_CHECKING` import so mypy still resolves it for downstream users. A
    convenience import at the top of `__init__.py` silently undoes this.
  - `pyarrow.parquet` is imported inside `ArrowBackend.from_parquet`, its only use site.
    `pyarrow.compute`/`types`/`lib` stay at module scope; they're used in the hot path.
    Any module-scope `pq.` use undoes this.
  - `backend._measure_width` imports `format.measure_width` (and rich with it, plus the
    `Console` that module builds on its first measurement) on first call. Backends must
    not import rich or `format` at module scope, or construct a `Console` in `__init__`.
  - `backend._cell_widths` imports `wcwidth` only when a column is not all ASCII, so an
    ASCII table never pays for its tables.

  `tests/unit_tests/test_lazy_imports.py` asserts all four, in a subprocess. To check by
  hand (all `False`; the import costs ~225 modules on 3.10 against the required deps, 450
  with the `polars` extra, which `uv sync` installs):

  ```bash
  uv run python -c "import sys; from textual_fastdatatable.backend import create_backend; print('textual' in sys.modules, 'rich' in sys.modules, 'pyarrow.parquet' in sys.modules, len(sys.modules))"
  ```
- `tests/unit_tests/test_wheels.py` resolves the dependency floors in `pyproject.toml` for
  every supported Python/platform with wheels only. It shells out to `uv` and hits PyPI, so
  it skips offline. Changing a dependency pin means checking this test.
- The Textual version is pinned in three places that must move together: the `test`
  dependency group, `.pre-commit-config.yaml`'s mypy `additional_dependencies`, and the
  `textual>=` floor in `[project] dependencies`.
- `CHANGELOG.md` follows keep-a-changelog; add user-facing changes under `## [Unreleased]`.
  Releases are cut by the `release.yml` workflow (version bump on a `release/vX.Y.Z`
  branch), which publishes on merge — do not bump the version by hand.
