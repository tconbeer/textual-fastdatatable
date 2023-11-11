# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

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

[Unreleased]: https://github.com/tconbeer/textual-fastdatatable/compare/0.3.0...HEAD

[0.3.0]: https://github.com/tconbeer/textual-fastdatatable/compare/0.2.1...0.3.0

[0.2.1]: https://github.com/tconbeer/textual-fastdatatable/compare/0.2.0...0.2.1

[0.2.0]: https://github.com/tconbeer/textual-fastdatatable/compare/0.1.4...0.2.0

[0.1.4]: https://github.com/tconbeer/textual-fastdatatable/compare/0.1.3...0.1.4

[0.1.3]: https://github.com/tconbeer/textual-fastdatatable/compare/0.1.2...0.1.3

[0.1.2]: https://github.com/tconbeer/textual-fastdatatable/compare/0.1.1...0.1.2

[0.1.1]: https://github.com/tconbeer/textual-fastdatatable/compare/0.1.0...0.1.1

[0.1.0]: https://github.com/tconbeer/textual-fastdatatable/compare/4b9f99175d34f693dd0d4198c39d72f89caf6479...0.1.0
