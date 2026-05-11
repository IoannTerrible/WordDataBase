# SimpleTextDatabase

A simple text-based database with basic ORM functionality for .NET 8 / 9 / 10. One file on disk, tables and rows are line-based, with sync and async APIs, transactions, and DI integration.

[![NuGet](https://img.shields.io/nuget/v/SimpleTextDatabase.svg)](https://www.nuget.org/packages/SimpleTextDatabase)

## Install

```bash
dotnet add package SimpleTextDatabase
```

## Quick start

```csharp
using WordDataBase;

using var db = new TextDatabase("data.txt");

db.CreateTable("users", new[]
{
    new Column("Id",     DataType.Int),
    new Column("Name",   DataType.String),
    new Column("Active", DataType.Bool),
});

db.InsertData("users", new[] { "1", "Alice", "true" });
db.InsertData("users", new[] { "2", "Bob",   "false" });

var rows = db.Select("users",
    columns: new[] { "Name" },
    filter:  r => r[0] != "Bob",
    orderByColumn: "Name");
// => [ ["Alice"] ]
```

## Async API

Every operation has an async sibling that uses `File.ReadAllLinesAsync` / `WriteAllLinesAsync`:

```csharp
await db.CreateTableAsync("users", schema, cancellationToken);
await db.InsertDataAsync("users", row, cancellationToken);
var rows = await db.SelectAsync("users", cancellationToken: cancellationToken);
```

Sync and async paths share a single `SemaphoreSlim` gate, so mixing them is safe.

## Dependency injection

```csharp
using Microsoft.Extensions.DependencyInjection;
using WordDataBase;

var services = new ServiceCollection();
services.AddSimpleTextDatabase(opts => opts.FilePath = "data.txt");
// or
services.AddSimpleTextDatabase("data.txt");

using var sp = services.BuildServiceProvider();
var db = sp.GetRequiredService<IDatabase>();
```

`IDatabase` is registered as a singleton.

## Transactions

```csharp
db.BeginTransaction();
try
{
    db.InsertData("users", new[] { "3", "Carol", "true" });
    db.CommitTransaction();
}
catch
{
    db.RollbackTransaction();
    throw;
}
```

- Only one transaction is active at a time.
- `Commit` and `Rollback` clean up the temp file in `finally`, so a crash mid-commit doesn't leak it permanently.
- `DropDatabase` aborts any active transaction and recreates an empty file.

## Supported column types

| `DataType` | Stored as              |
|------------|------------------------|
| `Int`      | invariant decimal int  |
| `String`   | UTF-8 text             |
| `Bool`     | invariant `true`/`false` |

Values are parsed and re-serialized with `CultureInfo.InvariantCulture` so files round-trip across locales.

## Build & test

```bash
dotnet build WordDataBase.sln
dotnet test  WordDataBase.sln
```

## Release

Bump `<Version>` in [`src/WordDataBase/WordDataBase.csproj`](src/WordDataBase/WordDataBase.csproj), commit, push to `main`. CI builds, packs, and publishes to nuget.org automatically (`--skip-duplicate`).

## License

MIT — see [`LICENSE.txt`](LICENSE.txt).
