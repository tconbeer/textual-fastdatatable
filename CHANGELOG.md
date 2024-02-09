# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

-   Adds a `backend.source_data` property to exposue the underlying Arrow table, before slicing.

## [0.7.0] - 2024-02-07

### Breaking Changes

-   Removes the NumpyBackend ([#78](https://github.com/tconbeer/textual-fastdatatable/issues/78)).

### Features

-   Values are now formatted based on their type. Numbers have separators based on the locale, and numbers, dates/times/etc., and bools are right-aligned ([#70](https://github.com/tconbeer/textual-fastdatatable/issues/70)).

### Bug Fixes

-   Fixes bug that caused either a crash or an empty table from initializing a table `from_records` or `from_pydict` with mixed (widening or narrowing) types in one column.

## [0.6.3] - 2024-01-09

### Bug Fixes

-   Widens acceptable types for create_backend to accept a sequence of any iterable, not just iterables that are instances of typing.Iterable.

## [0.6.2] - 2024-01-08

### Bug Fixes

-   Adds the tzdata package as a dependency for Windows installs, since Windows does not ship with a built-in tzdata database.

## [0.6.1] - 2024-01-05

### Bug Fixes

-   Fixes the behavior of <kbd>tab</kbd> and <kbd>shift+tab</kbd> to cycle to the next/prev row if at the end/start of a row or table.
-   Fixes a crash from pressing <kbd>ctrl+c</kbd> when the cursor type is column.

## [0.6.0] - 2024-01-05

### Features

-   Adds keybindings for navigating the cursor in the data table. <kbd>ctrl+right/left/up/down/home/end</kbd> (with <kbd>shift</kbd> variants), <kbd>tab</kbd>, <kbd>shift+tab</kbd>, <kbd>ctrl+a</kbd> now all do roughly what they do in Excel (if the cursor type is `range`).

## [0.5.1] - 2024-01-05

### Bug Fixes

-   Adds a dependency on pytz for Python &lt;3.9 for timezone support.
-   Fixes a bug where Arrow crashes while casting timestamptz to string ([tconbeer/harlequin#382](https://github.com/tconbeer/harlequin/issues/382)).

### Performance

-   Vectorizes fallback string casting for datatypes unsupported by `pc.cast` ([#8](https://github.com/tconbeer/textual-fastdatatable/issues/8))

## [0.5.0] - 2023-12-21

### Features

-   Adds a `range` cursor type that will highlight a range of selected cells, like Excel.
-   <kbd>ctrl+c</kbd> now posts a `SelectionCopied` message, with a values attribute that conttains a list of tuples of values from the data table.
-   Adds a `max_column_content_width` parameter to DataTable. If set, DataTable will truncate values longer than the width, but show the full value in a tooltip on hover.

## [0.4.1] - 2023-12-14

-   Fixes a crash caused by calling `create_backend` with an empty sequence.

## [0.4.0] - 2023-11-14

### Breaking API Changes

-   When calling `create_backend` with a sequence of iterables, the default behavior now assumes the data does not contain headers. You can restore the old behavior with `create_backend(has_headers=True)`.
-   When calling `DataTable(data=...)` with a sequence of iterables, the first row is treated as a header only if `column_labels` is not provided.

## [0.3.0] - 2023-11-11

### Features

-   The DataTable now accepts a `max_rows` kwarg; if provided, backends will only store the first `max_rows` and the DataTable will only present `max_rows`. The original row count of the data source is available as DataTable().source_row_count ([tconbeer/harlequin#281](https://github.com/tconbeer/harlequin/issues/281)).

### API Changes

-   Backends must now accept a `max_rows` kwarg on initialization.

## [0.2.1] - 2023-11-10

### Bug Fixes

-   Tables with the ArrowBackend no longer display incorrect output when column labels are duplicated ([#26](https://github.com/tconbeer/textual-fastdatatable/issues/26)).

## [0.2.0] - 2023-11-08

### Features

-   Adds a `null_rep: str` argument when initializing the data table; this string will be used to replace missing data.
-   Adds a `NumpyBackend` that uses Numpy Record Arrays; this backend is marginally slower than the `ArrowBackend` in most scenarios ([#23](https://github.com/tconbeer/textual-fastdatatable/issues/23)).

### Bug Fixes

-   Fixes a crash when using `ArrowBackend.from_records(has_header=False)`.

### Performance

-   Drastically improves performance for tables that are much wider than the viewport ([#12](https://github.com/tconbeer/textual-fastdatatable/issues/12)). 

### Benchmarks

-   Improves benchmarks to exclude data load times, disable garbage collection, and include more information about first paint and scroll performance.

## [0.1.4] - 2023-11-06

-   Fixes a crash when computing the widths of columns with no rows ([#19](https://github.com/tconbeer/textual-fastdatatable/issues/19)).

## [0.1.3] - 2023-10-09

-   Fixes a crash when creating a column from a null or complex type.

## [0.1.2] - 2023-10-02

## [0.1.1] - 2023-09-29

-   Fixes a crash when rows were added to an empty table.

## [0.1.0] - 2023-09-29

-   Initial release. Adds DataTable and ArrowBackend, which is 1000x faster for datasets of 500k records or more.

[Unreleased]: https://github.com/tconbeer/textual-fastdatatable/compare/0.7.0...HEAD

[0.7.0]: https://github.com/tconbeer/textual-fastdatatable/compare/0.6.3...0.7.0

[0.6.3]: https://github.com/tconbeer/textual-fastdatatable/compare/0.6.2...0.6.3

[0.6.2]: https://github.com/tconbeer/textual-fastdatatable/compare/0.6.1...0.6.2

[0.6.1]: https://github.com/tconbeer/textual-fastdatatable/compare/0.6.0...0.6.1

[0.6.0]: https://github.com/tconbeer/textual-fastdatatable/compare/0.5.1...0.6.0

[0.5.1]: https://github.com/tconbeer/textual-fastdatatable/compare/0.5.0...0.5.1

[0.5.0]: https://github.com/tconbeer/textual-fastdatatable/compare/0.4.1...0.5.0

[0.4.1]: https://github.com/tconbeer/textual-fastdatatable/compare/0.4.0...0.4.1

[0.4.0]: https://github.com/tconbeer/textual-fastdatatable/compare/0.3.0...0.4.0

[0.3.0]: https://github.com/tconbeer/textual-fastdatatable/compare/0.2.1...0.3.0

[0.2.1]: https://github.com/tconbeer/textual-fastdatatable/compare/0.2.0...0.2.1

[0.2.0]: https://github.com/tconbeer/textual-fastdatatable/compare/0.1.4...0.2.0

[0.1.4]: https://github.com/tconbeer/textual-fastdatatable/compare/0.1.3...0.1.4

[0.1.3]: https://github.com/tconbeer/textual-fastdatatable/compare/0.1.2...0.1.3

[0.1.2]: https://github.com/tconbeer/textual-fastdatatable/compare/0.1.1...0.1.2

[0.1.1]: https://github.com/tconbeer/textual-fastdatatable/compare/0.1.0...0.1.1

[0.1.0]: https://github.com/tconbeer/textual-fastdatatable/compare/4b9f99175d34f693dd0d4198c39d72f89caf6479...0.1.0
