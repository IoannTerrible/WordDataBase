# Changelog

All notable changes to this package are documented here.
Format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.10.0] — 2026-05-11

Major refactor. Surface-compatible at the call-site level for existing sync API; namespaces unified and new async / DI surfaces added.

### Added
- **`net10.0` target** alongside `net8.0` and `net9.0`.
- **Async API.** Every `IDatabase` method has a `*Async` sibling using `File.ReadAllLinesAsync` / `WriteAllLinesAsync` and accepting `CancellationToken`.
- **DI integration.** `services.AddSimpleTextDatabase(opts => ...)` and `services.AddSimpleTextDatabase("path")` register `IDatabase` as a singleton (depends on `Microsoft.Extensions.DependencyInjection.Abstractions`).
- **`TextDatabaseOptions`** for binding from configuration.
- **`TextDatabase` implements `IDisposable`** — releases the internal semaphore and cleans up any orphan transaction temp file.
- **`WordDataBase.Tests`** xUnit project (70 tests: sync + DI + async/cancellation/concurrency).
- **CI publish workflow** via the shared `TechTeaStudio/.github/.github/workflows/nuget-publish-reusable.yml`.
- **`Directory.Build.props`** with shared lang/nullable/author/copyright properties.
- **`CHANGELOG.md`** and rich README.

### Changed
- **Unified namespace** to `WordDataBase`. `Column` and `TextDatabase` moved out of `SimpleTextDatabase` namespace (the package id `SimpleTextDatabase` is unchanged).
- **Source layout**: code moved to `src/WordDataBase/`, tests to `tests/WordDataBase.Tests/`.
- **Single class per file**, Microsoft-style: `IDatabase.cs`, `Column.cs`, `DataType.cs`, `TextDatabase.cs`, `TextDatabaseOptions.cs`, `ServiceCollectionExtensions.cs`.
- **Thread safety**: all operations serialize through a single `SemaphoreSlim(1,1)` shared by sync and async paths. (SpinLock was considered and rejected — file I/O is millisecond-scale; spinning would burn CPU.)
- **`Select` returns `IReadOnlyList<string[]>`** (was `IEnumerable<string[]>`). Filter and ordering are applied to full rows before projection, so projection no longer corrupts ordering.
- **`File` I/O is UTF-8 explicit** and **read-once per operation** — `GetLine` per-line file reopen is gone (`Select` was O(N²), now O(N)).
- **Invariant culture** for `Int`/`Bool` parse and serialize (`ToLowerInvariant`, `CultureInfo.InvariantCulture`).
- **Company metadata** fixed: `Teach Tea Studio` → `Tech Tea Studio`.
- **License** switched to `<PackageLicenseExpression>MIT</PackageLicenseExpression>` (was file reference).
- **Copyright year** 2025 → 2026.

### Fixed
- **`ConvertValue` dead `reverse` parameter** — both branches of the ternary were identical. Parameter removed.
- **`DropDatabase` during active transaction** — previously left a dangling temp file and an inconsistent transaction pointer; now aborts the transaction cleanly.
- **Temp-file leaks** — `BeginTransaction` failure paths and `Commit` now delete the temp file in `finally`.
- **Constructor input validation** — empty/whitespace path, path pointing to a directory, or missing parent directory now throw descriptive exceptions instead of failing later in a confusing way.
- **`Select` projection + ordering bug** — ordering on a column not in the projection now works (full rows are sorted, then projected).
- **`Select` returning `null` values** for short rows — now returns `string.Empty`.

### Removed
- **Unused `Table` record.**
- **`reverse` parameter** from `ProcessData` / `ConvertValue` (dead code, no caller passed `true`).
- **Public `EnsureCreated()`** — internal-only.

### Migration notes (from 1.9.1)

- `using SimpleTextDatabase;` → `using WordDataBase;`. `Column` and `TextDatabase` now live there.
- `IEnumerable<string[]> Select(...)` → `IReadOnlyList<string[]> Select(...)`. Callers using `.ToList()` are unaffected; LINQ chains still work.

---

## [1.9.1] — 2025-01-26

- Added `net9.0` target.

## [1.9.0] — 2025-01-26

- License update; csproj packaging cleanup.

## [1.0.2] — 2024-08-17

- Fixed csproj packaging metadata.

## [1.0.0] — 2024-08-17

- Initial release: line-based text database with `CreateTable` / `InsertData` / `Select` / `DropTable` / `DropDatabase` and transaction support.

[1.10.0]: https://github.com/IoannTerrible/WordDataBase/releases/tag/v1.10.0
[1.9.1]: https://github.com/IoannTerrible/WordDataBase/releases/tag/v1.9.1
[1.9.0]: https://github.com/IoannTerrible/WordDataBase/releases/tag/v1.9.0
[1.0.2]: https://github.com/IoannTerrible/WordDataBase/releases/tag/v1.0.2
[1.0.0]: https://github.com/IoannTerrible/WordDataBase/releases/tag/v1.0.0
