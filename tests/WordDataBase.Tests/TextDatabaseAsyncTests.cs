namespace WordDataBase.Tests;

public class TextDatabaseAsyncTests
{
    private static Column[] Schema() => new[]
    {
        new Column("Id", DataType.Int),
        new Column("Name", DataType.String),
        new Column("Active", DataType.Bool),
    };

    // ---------- CreateTableAsync ----------

    [Fact]
    public async Task CreateTableAsync_CreatesTable()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await db.CreateTableAsync("users", Schema());
        var content = await File.ReadAllTextAsync(tmp.Path);
        Assert.Contains("#users", content);
    }

    [Fact]
    public async Task CreateTableAsync_AlreadyExists_Throws()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await db.CreateTableAsync("users", Schema());
        await Assert.ThrowsAsync<InvalidOperationException>(() => db.CreateTableAsync("users", Schema()));
    }

    [Theory]
    [InlineData("")]
    [InlineData("bad#name")]
    [InlineData("bad|name")]
    public async Task CreateTableAsync_BadName_Throws(string name)
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await Assert.ThrowsAsync<ArgumentException>(() => db.CreateTableAsync(name, Schema()));
    }

    [Fact]
    public async Task CreateTableAsync_NullColumns_Throws()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await Assert.ThrowsAsync<ArgumentException>(() => db.CreateTableAsync("t", null!));
    }

    [Fact]
    public async Task CreateTableAsync_BigSchema_Roundtrips()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        var cols = Enumerable.Range(0, 20)
            .Select(i => new Column($"C{i}", DataType.String)).ToArray();
        await db.CreateTableAsync("wide", cols);
        await db.InsertDataAsync("wide", Enumerable.Range(0, 20).Select(i => $"v{i}").ToArray());
        var rows = await db.SelectAsync("wide");
        Assert.Single(rows);
        Assert.Equal(20, rows[0].Length);
    }

    // ---------- InsertDataAsync ----------

    [Fact]
    public async Task InsertDataAsync_TableMissing_Throws()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            db.InsertDataAsync("users", new[] { "1", "Bob", "true" }));
    }

    [Fact]
    public async Task InsertDataAsync_WrongArity_Throws()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await db.CreateTableAsync("users", Schema());
        await Assert.ThrowsAsync<ArgumentException>(() =>
            db.InsertDataAsync("users", new[] { "1", "Bob" }));
    }

    [Fact]
    public async Task InsertDataAsync_PipeInValue_Throws()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await db.CreateTableAsync("users", Schema());
        await Assert.ThrowsAsync<ArgumentException>(() =>
            db.InsertDataAsync("users", new[] { "1", "Bob|x", "true" }));
    }

    [Fact]
    public async Task InsertDataAsync_NullData_Throws()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await db.CreateTableAsync("users", Schema());
        await Assert.ThrowsAsync<ArgumentNullException>(() => db.InsertDataAsync("users", null!));
    }

    [Fact]
    public async Task InsertDataAsync_PersistsToDisk()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await db.CreateTableAsync("t", Schema());
        await db.InsertDataAsync("t", new[] { "1", "Bob", "true" });

        var content = await File.ReadAllTextAsync(tmp.Path);
        Assert.Contains("1|Bob|true", content);
    }

    // ---------- SelectAsync ----------

    [Fact]
    public async Task SelectAsync_Roundtrips()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await db.CreateTableAsync("users", Schema());
        await db.InsertDataAsync("users", new[] { "1", "Alice", "true" });
        await db.InsertDataAsync("users", new[] { "2", "Bob", "false" });

        var rows = await db.SelectAsync("users");
        Assert.Equal(2, rows.Count);
    }

    [Fact]
    public async Task SelectAsync_TableMissing_Throws()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await Assert.ThrowsAsync<InvalidOperationException>(() => db.SelectAsync("nope"));
    }

    [Fact]
    public async Task SelectAsync_EmptyTable_ReturnsEmpty()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await db.CreateTableAsync("t", Schema());
        var rows = await db.SelectAsync("t");
        Assert.Empty(rows);
    }

    [Fact]
    public async Task SelectAsync_WithProjection_OnlyReturnsRequestedColumns()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await db.CreateTableAsync("u", Schema());
        await db.InsertDataAsync("u", new[] { "1", "Alice", "true" });

        var rows = await db.SelectAsync("u", columns: new[] { "Name" });
        Assert.Equal(new[] { "Alice" }, rows[0]);
    }

    [Fact]
    public async Task SelectAsync_WithFilter_AppliesPredicate()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await db.CreateTableAsync("u", Schema());
        await db.InsertDataAsync("u", new[] { "1", "Alice", "true" });
        await db.InsertDataAsync("u", new[] { "2", "Bob", "false" });

        var rows = await db.SelectAsync("u", filter: r => r[2] == "true");
        Assert.Single(rows);
        Assert.Equal("Alice", rows[0][1]);
    }

    [Fact]
    public async Task SelectAsync_WithOrdering_SortsResults()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await db.CreateTableAsync("u", Schema());
        await db.InsertDataAsync("u", new[] { "2", "Bob", "true" });
        await db.InsertDataAsync("u", new[] { "1", "Alice", "true" });

        var rows = await db.SelectAsync("u", orderByColumn: "Name");
        Assert.Equal("Alice", rows[0][1]);
        Assert.Equal("Bob", rows[1][1]);
    }

    [Fact]
    public async Task SelectAsync_InvalidOrderColumn_Throws()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await db.CreateTableAsync("u", Schema());
        await db.InsertDataAsync("u", new[] { "1", "Alice", "true" });
        await Assert.ThrowsAsync<ArgumentException>(() => db.SelectAsync("u", orderByColumn: "Nope"));
    }

    // ---------- Transaction (async) ----------

    [Fact]
    public async Task BeginTransactionAsync_Works()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await db.BeginTransactionAsync();
        await db.RollbackTransactionAsync();
    }

    [Fact]
    public async Task BeginTransactionAsync_DoubleBegin_Throws()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await db.BeginTransactionAsync();
        await Assert.ThrowsAsync<InvalidOperationException>(() => db.BeginTransactionAsync());
    }

    [Fact]
    public async Task CommitTransactionAsync_Persists()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await db.CreateTableAsync("t", Schema());
        await db.BeginTransactionAsync();
        await db.InsertDataAsync("t", new[] { "1", "x", "true" });
        await db.CommitTransactionAsync();

        Assert.Single(await db.SelectAsync("t"));
    }

    [Fact]
    public async Task CommitTransactionAsync_WithoutBegin_Throws()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await Assert.ThrowsAsync<InvalidOperationException>(() => db.CommitTransactionAsync());
    }

    [Fact]
    public async Task RollbackTransactionAsync_Discards()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await db.CreateTableAsync("t", Schema());
        await db.BeginTransactionAsync();
        await db.InsertDataAsync("t", new[] { "1", "x", "true" });
        await db.RollbackTransactionAsync();

        Assert.Empty(await db.SelectAsync("t"));
    }

    [Fact]
    public async Task RollbackTransactionAsync_WithoutBegin_Throws()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await Assert.ThrowsAsync<InvalidOperationException>(() => db.RollbackTransactionAsync());
    }

    // ---------- Drop (async) ----------

    [Fact]
    public async Task DropTableAsync_Removes()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await db.CreateTableAsync("t", Schema());
        await db.DropTableAsync("t");
        await Assert.ThrowsAsync<InvalidOperationException>(() => db.SelectAsync("t"));
    }

    [Fact]
    public async Task DropTableAsync_Missing_Throws()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await Assert.ThrowsAsync<InvalidOperationException>(() => db.DropTableAsync("nope"));
    }

    [Fact]
    public async Task DropDatabaseAsync_EmptiesFile()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await db.CreateTableAsync("t", Schema());
        await db.DropDatabaseAsync();
        Assert.Equal(0, new FileInfo(tmp.Path).Length);
    }

    [Fact]
    public async Task DropDatabaseAsync_DuringTransaction_CleansTempFile()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await db.CreateTableAsync("t", Schema());
        await db.BeginTransactionAsync();
        await db.InsertDataAsync("t", new[] { "1", "x", "true" });
        await db.DropDatabaseAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(() => db.CommitTransactionAsync());
    }

    // ---------- Cancellation ----------

    [Fact]
    public async Task BeginTransactionAsync_AlreadyCanceled_Throws()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => db.BeginTransactionAsync(cts.Token));
    }

    [Fact]
    public async Task CreateTableAsync_AlreadyCanceled_Throws()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => db.CreateTableAsync("t", Schema(), cts.Token));
    }

    [Fact]
    public async Task InsertDataAsync_AlreadyCanceled_Throws()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await db.CreateTableAsync("t", Schema());
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => db.InsertDataAsync("t", new[] { "1", "x", "true" }, cts.Token));
    }

    [Fact]
    public async Task SelectAsync_AlreadyCanceled_Throws()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await db.CreateTableAsync("t", Schema());
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => db.SelectAsync("t", cancellationToken: cts.Token));
    }

    [Fact]
    public async Task DropTableAsync_AlreadyCanceled_Throws()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await db.CreateTableAsync("t", Schema());
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => db.DropTableAsync("t", cts.Token));
    }

    [Fact]
    public async Task DropDatabaseAsync_AlreadyCanceled_Throws()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => db.DropDatabaseAsync(cts.Token));
    }

    // ---------- Concurrency / interop ----------

    [Fact]
    public async Task ConcurrentInsertsAsync_AllPersist()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await db.CreateTableAsync("t", Schema());

        var tasks = Enumerable.Range(0, 50)
            .Select(i => db.InsertDataAsync("t",
                new[] { i.ToString(System.Globalization.CultureInfo.InvariantCulture), $"u{i}", "true" }))
            .ToArray();
        await Task.WhenAll(tasks);

        var rows = await db.SelectAsync("t");
        Assert.Equal(50, rows.Count);
    }

    [Fact]
    public async Task ConcurrentSelectsAsync_DoNotCorrupt()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await db.CreateTableAsync("t", Schema());
        await db.InsertDataAsync("t", new[] { "1", "x", "true" });

        var tasks = Enumerable.Range(0, 20).Select(_ => db.SelectAsync("t")).ToArray();
        var results = await Task.WhenAll(tasks);
        Assert.All(results, r => Assert.Single(r));
    }

    [Fact]
    public async Task AsyncTransactionCommit_SyncSelect_SeesData()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await db.CreateTableAsync("t", Schema());
        await db.BeginTransactionAsync();
        await db.InsertDataAsync("t", new[] { "1", "x", "true" });
        await db.CommitTransactionAsync();

        Assert.Single(db.Select("t"));
    }

    [Fact]
    public async Task AsyncRollback_SyncSelect_SeesNothing()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await db.CreateTableAsync("t", Schema());
        await db.BeginTransactionAsync();
        await db.InsertDataAsync("t", new[] { "1", "x", "true" });
        await db.RollbackTransactionAsync();

        Assert.Empty(db.Select("t"));
    }

    [Fact]
    public async Task SyncCreate_AsyncSelect_Interop()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        db.CreateTable("t", Schema());
        db.InsertData("t", new[] { "1", "x", "true" });
        var rows = await db.SelectAsync("t");
        Assert.Single(rows);
    }

    [Fact]
    public async Task AsyncCreate_SyncSelect_Interop()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await db.CreateTableAsync("t", Schema());
        await db.InsertDataAsync("t", new[] { "1", "x", "true" });
        var rows = db.Select("t");
        Assert.Single(rows);
    }

    [Fact]
    public async Task AsyncFile_Encoding_IsUtf8()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await db.CreateTableAsync("t", new[] { new Column("Name", DataType.String) });
        await db.InsertDataAsync("t", new[] { "Привет" });

        var rows = await db.SelectAsync("t");
        Assert.Equal("Привет", rows[0][0]);
    }

    [Fact]
    public async Task AsyncBool_LowercaseInvariant()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await db.CreateTableAsync("b", new[] { new Column("V", DataType.Bool) });
        await db.InsertDataAsync("b", new[] { "True" });
        var rows = await db.SelectAsync("b");
        Assert.Equal("true", rows[0][0]);
    }

    [Fact]
    public async Task AsyncInt_RoundtripsInvariant()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        await db.CreateTableAsync("n", new[] { new Column("V", DataType.Int) });
        await db.InsertDataAsync("n", new[] { "12345" });
        var rows = await db.SelectAsync("n");
        Assert.Equal("12345", rows[0][0]);
    }

    [Fact]
    public async Task AsyncTransaction_LeavesNoTempFiles()
    {
        using var tmp = new TempFile();
        var tempBefore = Directory.GetFiles(Path.GetTempPath(), "tmp*").Length;

        using (var db = new TextDatabase(tmp.Path))
        {
            await db.CreateTableAsync("t", Schema());
            await db.BeginTransactionAsync();
            await db.InsertDataAsync("t", new[] { "1", "x", "true" });
            await db.CommitTransactionAsync();
        }

        var tempAfter = Directory.GetFiles(Path.GetTempPath(), "tmp*").Length;
        Assert.True(tempAfter <= tempBefore + 1, $"Temp leaked: before={tempBefore} after={tempAfter}");
    }
}
