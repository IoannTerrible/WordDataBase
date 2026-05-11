namespace WordDataBase.Tests;

public class TextDatabaseTests
{
    private static Column[] Schema() => new[]
    {
        new Column("Id", DataType.Int),
        new Column("Name", DataType.String),
        new Column("Active", DataType.Bool),
    };

    [Fact]
    public void Ctor_NullOrEmptyPath_Throws()
    {
        Assert.Throws<ArgumentException>(() => new TextDatabase(""));
        Assert.Throws<ArgumentException>(() => new TextDatabase("   "));
    }

    [Fact]
    public void Ctor_PathIsDirectory_Throws()
    {
        using var dir = new TempFile();
        Directory.CreateDirectory(dir.Path);
        try
        {
            Assert.Throws<ArgumentException>(() => new TextDatabase(dir.Path));
        }
        finally { Directory.Delete(dir.Path, recursive: true); }
    }

    [Fact]
    public void Ctor_DirectoryMissing_Throws()
    {
        var path = Path.Combine(Path.GetTempPath(), $"missing-{Guid.NewGuid():N}", "db.txt");
        Assert.Throws<DirectoryNotFoundException>(() => new TextDatabase(path));
    }

    [Fact]
    public void Ctor_ValidPath_CreatesFile()
    {
        using var tmp = new TempFile();
        _ = new TextDatabase(tmp.Path);
        Assert.True(File.Exists(tmp.Path));
    }

    [Fact]
    public void CreateTable_AlreadyExists_Throws()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        db.CreateTable("users", Schema());
        Assert.Throws<InvalidOperationException>(() => db.CreateTable("users", Schema()));
    }

    [Theory]
    [InlineData("")]
    [InlineData("bad#name")]
    [InlineData("bad|name")]
    public void CreateTable_BadName_Throws(string name)
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        Assert.Throws<ArgumentException>(() => db.CreateTable(name, Schema()));
    }

    [Fact]
    public void InsertData_TableMissing_Throws()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        Assert.Throws<InvalidOperationException>(() =>
            db.InsertData("users", new[] { "1", "Bob", "true" }));
    }

    [Fact]
    public void InsertData_WrongArity_Throws()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        db.CreateTable("users", Schema());
        Assert.Throws<ArgumentException>(() => db.InsertData("users", new[] { "1", "Bob" }));
    }

    [Fact]
    public void InsertData_PipeInValue_Throws()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        db.CreateTable("users", Schema());
        Assert.Throws<ArgumentException>(() => db.InsertData("users", new[] { "1", "Bob|Bad", "true" }));
    }

    [Fact]
    public void Select_RoundtripsRows()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        db.CreateTable("users", Schema());
        db.InsertData("users", new[] { "1", "Alice", "true" });
        db.InsertData("users", new[] { "2", "Bob", "false" });

        var rows = db.Select("users").ToList();
        Assert.Equal(2, rows.Count);
        Assert.Equal(new[] { "1", "Alice", "true" }, rows[0]);
        Assert.Equal(new[] { "2", "Bob", "false" }, rows[1]);
    }

    [Fact]
    public void Select_WithFilterAndOrder_Works()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        db.CreateTable("users", Schema());
        db.InsertData("users", new[] { "2", "Bob", "true" });
        db.InsertData("users", new[] { "1", "Alice", "true" });
        db.InsertData("users", new[] { "3", "Carol", "false" });

        var rows = db.Select("users",
            columns: new[] { "Name" },
            filter: r => r[0] != "Bob",
            orderByColumn: "Name").ToList();

        Assert.Equal(new[] { "Alice", "Carol" }, rows.Select(r => r[0]).ToArray());
    }

    [Fact]
    public void Transaction_Commit_Persists()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        db.CreateTable("t", Schema());
        db.BeginTransaction();
        db.InsertData("t", new[] { "1", "x", "true" });
        db.CommitTransaction();

        Assert.Single(db.Select("t"));
    }

    [Fact]
    public void Transaction_Rollback_Discards()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        db.CreateTable("t", Schema());
        db.BeginTransaction();
        db.InsertData("t", new[] { "1", "x", "true" });
        db.RollbackTransaction();

        Assert.Empty(db.Select("t"));
    }

    [Fact]
    public void Transaction_DoubleBegin_Throws()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        db.BeginTransaction();
        Assert.Throws<InvalidOperationException>(() => db.BeginTransaction());
    }

    [Fact]
    public void Transaction_CommitWithoutBegin_Throws()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        Assert.Throws<InvalidOperationException>(() => db.CommitTransaction());
    }

    [Fact]
    public void DropTable_Removes()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        db.CreateTable("t", Schema());
        db.InsertData("t", new[] { "1", "x", "true" });
        db.DropTable("t");
        Assert.Throws<InvalidOperationException>(() => db.Select("t"));
    }

    [Fact]
    public void DropDatabase_DuringTransaction_AbortsCleanly()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        db.CreateTable("t", Schema());
        db.BeginTransaction();
        db.InsertData("t", new[] { "1", "x", "true" });
        db.DropDatabase();

        Assert.Throws<InvalidOperationException>(() => db.CommitTransaction());
        Assert.True(File.Exists(tmp.Path));
        Assert.Equal(0, new FileInfo(tmp.Path).Length);
    }

    [Fact]
    public void ConvertValue_Int_RoundtripsInvariant()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        db.CreateTable("n", new[] { new Column("Value", DataType.Int) });
        db.InsertData("n", new[] { "42" });
        Assert.Equal("42", db.Select("n").Single()[0]);
    }

    [Fact]
    public void ConvertValue_Bool_LowercasedInvariant()
    {
        using var tmp = new TempFile();
        using var db = new TextDatabase(tmp.Path);
        db.CreateTable("b", new[] { new Column("V", DataType.Bool) });
        db.InsertData("b", new[] { "True" });
        Assert.Equal("true", db.Select("b").Single()[0]);
    }
}
