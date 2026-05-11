namespace WordDataBase;

/// <summary>Abstraction for a simple table-oriented text database with sync and async APIs.</summary>
public interface IDatabase
{
    /// <summary>Begins a new transaction; only one transaction can be active at a time.</summary>
    void BeginTransaction();

    /// <summary>Commits the active transaction, persisting all staged changes.</summary>
    void CommitTransaction();

    /// <summary>Rolls back the active transaction, discarding all staged changes.</summary>
    void RollbackTransaction();

    /// <summary>Creates a new table with the given name and columns.</summary>
    void CreateTable(string tableName, Column[] columns);

    /// <summary>Inserts a row of values into the specified table.</summary>
    void InsertData(string tableName, string[] data);

    /// <summary>Selects rows from a table with optional column projection, filter, and ordering.</summary>
    IReadOnlyList<string[]> Select(
        string tableName,
        string[]? columns = null,
        Func<string[], bool>? filter = null,
        string? orderByColumn = null);

    /// <summary>Drops the named table and its data.</summary>
    void DropTable(string tableName);

    /// <summary>Deletes the entire database file and recreates an empty one, aborting any active transaction.</summary>
    void DropDatabase();

    /// <summary>Asynchronously begins a new transaction.</summary>
    Task BeginTransactionAsync(CancellationToken cancellationToken = default);

    /// <summary>Asynchronously commits the active transaction.</summary>
    Task CommitTransactionAsync(CancellationToken cancellationToken = default);

    /// <summary>Asynchronously rolls back the active transaction.</summary>
    Task RollbackTransactionAsync(CancellationToken cancellationToken = default);

    /// <summary>Asynchronously creates a new table.</summary>
    Task CreateTableAsync(string tableName, Column[] columns, CancellationToken cancellationToken = default);

    /// <summary>Asynchronously inserts a row of values into the specified table.</summary>
    Task InsertDataAsync(string tableName, string[] data, CancellationToken cancellationToken = default);

    /// <summary>Asynchronously selects rows from a table.</summary>
    Task<IReadOnlyList<string[]>> SelectAsync(
        string tableName,
        string[]? columns = null,
        Func<string[], bool>? filter = null,
        string? orderByColumn = null,
        CancellationToken cancellationToken = default);

    /// <summary>Asynchronously drops the named table.</summary>
    Task DropTableAsync(string tableName, CancellationToken cancellationToken = default);

    /// <summary>Asynchronously deletes the entire database file and recreates an empty one.</summary>
    Task DropDatabaseAsync(CancellationToken cancellationToken = default);
}
