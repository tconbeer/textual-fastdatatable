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
everything else becomes the text a cell shows for it and takes the widest result of
`_measure_strings`. That runs `_measure_cells` as an Arrow scalar UDF, which measures an
array in cells rather than characters. Arrow measures the values it can by itself, and
hands the rest to `backend._measure_width` — a lazy wrapper around
`format.measure_width` (see Conventions), which is also the per-value path for
non-strings — once per distinct value, mapped back over the rows by Arrow. Arrow can
measure a value when:

- it has as many bytes as characters, so it is all ASCII and one cell per character; and
- with `render_markup`, it does not match `_MARKUP_PATTERN`, so rich will render it
  unchanged. **That pattern must stay a superset of what rich treats as markup**
  (`rich.markup.RE_TAGS`, plus the `\[` escape `render` unescapes) — matching too much
  only costs a measurement, matching too little measures a value wrong. Markup is never
  stripped before measuring: rich decides what it means, by rendering it. A cheap "is
  there a `[` at all" test runs first, so a column with no brackets never pays for the
  regex.

A row is one line tall, so a multi-line value renders — and is measured — as its first
line plus `format.MULTILINE_MARKER`; `cell_formatter` clips every value that way, the
text it gets from `display_text` included, since only a string's own breaks are visible
to the branch that handles strings. `_measure_cells` gets that width from the first
break's position (`pc.find_substring`, which for an all-ASCII value *is* a width), but
runs the kernel only for a column `_line_breaks_present` found a break in: the byte scan
costs ~4ms per million values against the kernel's ~30ms, and almost no column has a
break. `backend.LINE_BREAKS` and `_MARKER_WIDTH` restate `format`, which `backend`
cannot import; `test_backends.test_line_breaks_match_the_formatters` holds them in step.

When a cell renders, `_render_cell` passes the room it has as `cell_formatter`'s
`max_width`, and a clipped value is cropped to leave the marker its cells. Without that
reservation the marker — being the tail of the line — is the first thing rich's overflow
drops in a column capped by `max_column_content_width`. Measuring passes no `max_width`.

Because `measure_width` renders the value, it has to be told whether the widget renders
markup; `_measure_cells` is registered as two UDFs per type, `_cell_widths` and
`_cell_widths_no_markup`, since a UDF is registered under its name for the life of the
process. The result
is cached on the backend and cleared by `_reset_content_widths()` on mutation.

Only a column already stored as the characters it displays can be turned into that text
by Arrow itself — `_arrow_casts_to_display_text`: the string types, and a dictionary of
them. **`arr.cast(pa.string())` is not a test of that**, because it succeeds for types it
reinterprets rather than renders: a binary type, and an extension type over one, come
back as their storage bytes, so an `arrow.uuid`'s 16 bytes became 16 bytes of would-be
text (rarely valid UTF-8, which is how #176 crashed) rather than the 36 characters the
widget draws. So every other type — binary, extension, nested, a dictionary of anything
but strings, whatever a driver invents next — is converted value by value with
`format.display_text`, which is what `cell_formatter` renders those values as; polars is
the same, and cannot cast a binary or nested column at all. `display_text` is told what
the table renders strings as — the one value type that setting changes — and escapes a
string it will render literally, so what comes back is markup either way, and is
measured with `render_markup=True`. `_measure_display_text`
walks the column `_VALUE_BLOCK_SIZE` values at a time and keeps the widest, so a large
column never holds a Python object per row — the bound the scalar UDF this replaced got
from Arrow's chunking. This path costs 1.5–5s per million values, against ~20ms for a
column Arrow can cast, so what belongs on the fast side of
`_arrow_casts_to_display_text` is a performance question as much as a correctness one.

An extension type gets a look before any of that, because it is a storage type with a
meaning attached and only the type says which of the two a cell shows. `arrow.json`,
`arrow.opaque` and every type this pyarrow has no class for leave the value as the
storage's — pyarrow's own `ExtensionScalar.as_py` — so `_measure` recurses into
`_extension_storage` and the column is measured on whichever path its storage belongs
to (a json column is measured as the strings it is). `arrow.uuid` and `arrow.bool8`
override `as_py`, and their values render at a width their Python type fixes
(`format.FIXED_WIDTH_TYPES`), so one value measures the column, as for a temporal type.
**The test is the scalar class, never whether the value equals its storage**:
`arrow.bool8`'s `True` equals its storage's `1` and renders `✓ True` against `1`. A
pyarrow that gives one of these a class of its own only costs a measurement; it cannot
mismeasure, which is what makes the rule safe for extension types nobody has seen yet.

Every UDF is registered through `_register_udf`, which registers a name at most once:
`pc.register_scalar_function` raises for a name that is taken **and drops a reference to
the function already registered under it**, so re-registering segfaults pyarrow a couple
of calls later. `tests/unit_tests/test_backends.py::test_column_content_widths_are_repeatable`
guards this.

`format.measure_width` is the one place a width is measured: it formats the value with
`cell_formatter` and measures the result in cells against a console. The widget passes
the app's, via `DataTable._measure`, so a column is never wider than the screen; callers
with no app (the backend, and the widget before it mounts) get the console this module
builds lazily, which is `MAX_MEASURE_WIDTH` wide, so nothing caps those measurements
(and whose `ConsoleOptions` are cached with it: `Console.options` rebuilds them on every
access, about half the cost of measuring a short value). `render_markup` must match the
widget's, or `[dim]a[/]` measures one cell where eleven render.
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
Every value it does not hand to rich as a string, a `Text` or a renderable of its own
gets its text from `format.display_text` — bytes as a bounded escaped preview, anything
else (a uuid, a list, a struct's dict) as an escaped `str(obj)`: the brackets in a repr
are the repr's, so a tag rich finds in one was never markup, and rendering it eats the
structure around it (or raises, for an unbalanced tag). A string is the only value
markup is parsed in, and `render_markup` says whether it is. That is the one place a value
becomes text, so that the backends measure what the widget draws;
`test_format.test_display_text_measures_as_the_cell_it_describes` holds the two in step.

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
  Textual. Three deliberate deferrals keep this true:
  - `__init__.py` imports `DataTable` lazily via a module-level `__getattr__` (PEP 562),
    with a `TYPE_CHECKING` import so mypy still resolves it for downstream users. A
    convenience import at the top of `__init__.py` silently undoes this.
  - `pyarrow.parquet` is imported inside `ArrowBackend.from_parquet`, its only use site.
    `pyarrow.compute`/`types`/`lib` stay at module scope; they're used in the hot path.
    Any module-scope `pq.` use undoes this.
  - `backend._measure_width` imports `format.measure_width` (and rich with it, plus the
    `Console` that module builds on its first measurement) on first call. Backends must
    not import rich or `format` at module scope, or construct a `Console` in `__init__`.
    A string column that is all ASCII never calls it, so an ASCII table never pays for
    rich at all.

  `tests/unit_tests/test_lazy_imports.py` asserts all three, in a subprocess. To check by
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
