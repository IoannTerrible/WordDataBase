using System.Globalization;
using System.Text;

namespace WordDataBase;

/// <summary>Simple line-based text database storing tables and rows in a single file.</summary>
/// <remarks>
/// Thread safety: all public operations are serialized through a single <see cref="SemaphoreSlim"/>.
/// A spin-based primitive would be a poor fit here because each operation performs file I/O on the
/// order of milliseconds — spinning would just burn CPU. <see cref="SemaphoreSlim"/> also lets sync
/// and async paths share the same gate.
/// </remarks>
public sealed class TextDatabase : IDatabase, IDisposable
{
    private readonly string filePath;
    private readonly SemaphoreSlim gate = new(1, 1);
    private string? transactionFilePath;
    private bool disposed;

    /// <summary>Initializes the database at the given file path, creating the file if missing.</summary>
    public TextDatabase(string filePath)
    {
        if (string.IsNullOrWhiteSpace(filePath))
            throw new ArgumentException("File path cannot be null or empty.", nameof(filePath));
        if (Directory.Exists(filePath))
            throw new ArgumentException("File path points to an existing directory.", nameof(filePath));

        var directory = Path.GetDirectoryName(filePath);
        if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
            throw new DirectoryNotFoundException($"Directory '{directory}' does not exist.");

        this.filePath = filePath;
        EnsureCreated();
    }

    /// <summary>Ensures the database file exists.</summary>
    private void EnsureCreated()
    {
        if (!File.Exists(filePath))
            using (File.Create(filePath)) { }
    }

    /// <inheritdoc />
    public void BeginTransaction()
    {
        gate.Wait();
        try { BeginTransactionCore(); }
        finally { gate.Release(); }
    }

    /// <inheritdoc />
    public async Task BeginTransactionAsync(CancellationToken cancellationToken = default)
    {
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try { BeginTransactionCore(); }
        finally { gate.Release(); }
    }

    private void BeginTransactionCore()
    {
        if (transactionFilePath != null)
            throw new InvalidOperationException("A transaction is already in progress.");

        var temp = Path.GetTempFileName();
        try
        {
            File.Copy(filePath, temp, overwrite: true);
            transactionFilePath = temp;
        }
        catch
        {
            TryDelete(temp);
            throw;
        }
    }

    /// <inheritdoc />
    public void CommitTransaction()
    {
        gate.Wait();
        try { CommitTransactionCore(); }
        finally { gate.Release(); }
    }

    /// <inheritdoc />
    public async Task CommitTransactionAsync(CancellationToken cancellationToken = default)
    {
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try { CommitTransactionCore(); }
        finally { gate.Release(); }
    }

    private void CommitTransactionCore()
    {
        if (transactionFilePath == null)
            throw new InvalidOperationException("No transaction is in progress.");

        var temp = transactionFilePath;
        try
        {
            File.Copy(temp, filePath, overwrite: true);
        }
        finally
        {
            TryDelete(temp);
            transactionFilePath = null;
        }
    }

    /// <inheritdoc />
    public void RollbackTransaction()
    {
        gate.Wait();
        try { RollbackTransactionCore(); }
        finally { gate.Release(); }
    }

    /// <inheritdoc />
    public async Task RollbackTransactionAsync(CancellationToken cancellationToken = default)
    {
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try { RollbackTransactionCore(); }
        finally { gate.Release(); }
    }

    private void RollbackTransactionCore()
    {
        if (transactionFilePath == null)
            throw new InvalidOperationException("No transaction is in progress.");

        TryDelete(transactionFilePath);
        transactionFilePath = null;
    }

    /// <inheritdoc />
    public void CreateTable(string tableName, Column[] columns)
    {
        ValidateTableName(tableName);
        ValidateColumns(columns);

        gate.Wait();
        try
        {
            var lines = ReadAllLines();
            if (TableExists(lines, tableName, out _, out _))
                throw new InvalidOperationException("Table already exists.");

            AppendTable(lines, tableName, columns);
            WriteAllLines(lines);
        }
        finally { gate.Release(); }
    }

    /// <inheritdoc />
    public async Task CreateTableAsync(string tableName, Column[] columns, CancellationToken cancellationToken = default)
    {
        ValidateTableName(tableName);
        ValidateColumns(columns);

        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var lines = await ReadAllLinesAsync(cancellationToken).ConfigureAwait(false);
            if (TableExists(lines, tableName, out _, out _))
                throw new InvalidOperationException("Table already exists.");

            AppendTable(lines, tableName, columns);
            await WriteAllLinesAsync(lines, cancellationToken).ConfigureAwait(false);
        }
        finally { gate.Release(); }
    }

    private static void AppendTable(List<string> lines, string tableName, Column[] columns)
    {
        var columnDefinitions = string.Join("|", columns.Select(c => $"{c.Name}:{c.Type}"));
        lines.Add($"#{tableName}");
        lines.Add(columnDefinitions);
        lines.Add(string.Empty);
    }

    /// <inheritdoc />
    public void InsertData(string tableName, string[] data)
    {
        ArgumentNullException.ThrowIfNull(data);

        gate.Wait();
        try
        {
            var lines = ReadAllLines();
            InsertDataCore(lines, tableName, data);
            WriteAllLines(lines);
        }
        finally { gate.Release(); }
    }

    /// <inheritdoc />
    public async Task InsertDataAsync(string tableName, string[] data, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(data);

        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var lines = await ReadAllLinesAsync(cancellationToken).ConfigureAwait(false);
            InsertDataCore(lines, tableName, data);
            await WriteAllLinesAsync(lines, cancellationToken).ConfigureAwait(false);
        }
        finally { gate.Release(); }
    }

    private static void InsertDataCore(List<string> lines, string tableName, string[] data)
    {
        if (!TableExists(lines, tableName, out var startLine, out var endLine))
            throw new InvalidOperationException("Table not found.");

        var columns = ParseColumns(lines[startLine + 1]);
        if (data.Length != columns.Length)
            throw new ArgumentException("Data length does not match the number of columns.", nameof(data));

        foreach (var value in data)
            if (value != null && value.Contains('|'))
                throw new ArgumentException("Data values cannot contain the '|' character.", nameof(data));

        var processed = ProcessData(columns, data);
        lines.Insert(endLine, string.Join("|", processed));
    }

    /// <inheritdoc />
    public IReadOnlyList<string[]> Select(
        string tableName,
        string[]? columns = null,
        Func<string[], bool>? filter = null,
        string? orderByColumn = null)
    {
        gate.Wait();
        try
        {
            var lines = ReadAllLines();
            return SelectCore(lines, tableName, columns, filter, orderByColumn);
        }
        finally { gate.Release(); }
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<string[]>> SelectAsync(
        string tableName,
        string[]? columns = null,
        Func<string[], bool>? filter = null,
        string? orderByColumn = null,
        CancellationToken cancellationToken = default)
    {
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var lines = await ReadAllLinesAsync(cancellationToken).ConfigureAwait(false);
            return SelectCore(lines, tableName, columns, filter, orderByColumn);
        }
        finally { gate.Release(); }
    }

    private static IReadOnlyList<string[]> SelectCore(
        List<string> lines,
        string tableName,
        string[]? columns,
        Func<string[], bool>? filter,
        string? orderByColumn)
    {
        if (!TableExists(lines, tableName, out var startLine, out var endLine))
            throw new InvalidOperationException("Table not found.");

        var allColumns = ParseColumns(lines[startLine + 1]);
        var projection = GetColumnIndices(allColumns, columns);

        IEnumerable<string[]> pipeline = ReadFullRows(lines, startLine, endLine, allColumns.Length);

        if (!string.IsNullOrEmpty(orderByColumn))
        {
            var orderIndex = Array.FindIndex(
                allColumns,
                col => col.Name.Equals(orderByColumn, StringComparison.OrdinalIgnoreCase));
            if (orderIndex == -1)
                throw new ArgumentException("Invalid column name for ordering.", nameof(orderByColumn));

            pipeline = pipeline.OrderBy(r => r[orderIndex], StringComparer.Ordinal);
        }

        var projected = pipeline.Select(r => Project(r, projection));
        if (filter != null)
            projected = projected.Where(filter);

        return projected.ToList();
    }

    private static int[] GetColumnIndices(Column[] allColumns, string[]? columns)
    {
        if (columns == null)
            return Enumerable.Range(0, allColumns.Length).ToArray();

        var result = new int[columns.Length];
        for (var i = 0; i < columns.Length; i++)
        {
            result[i] = Array.FindIndex(
                allColumns,
                col => col.Name.Equals(columns[i], StringComparison.OrdinalIgnoreCase));
        }
        return result;
    }

    private static List<string[]> ReadFullRows(List<string> lines, int startLine, int endLine, int columnCount)
    {
        var rows = new List<string[]>();
        for (var i = startLine + 2; i < endLine; i++)
        {
            var line = lines[i];
            if (string.IsNullOrWhiteSpace(line))
                continue;

            var row = line.Split('|');
            if (row.Length < columnCount)
            {
                var padded = new string[columnCount];
                Array.Copy(row, padded, row.Length);
                for (var j = row.Length; j < columnCount; j++)
                    padded[j] = string.Empty;
                row = padded;
            }
            rows.Add(row);
        }
        return rows;
    }

    private static string[] Project(string[] row, int[] indices)
    {
        var result = new string[indices.Length];
        for (var i = 0; i < indices.Length; i++)
        {
            var idx = indices[i];
            result[i] = idx >= 0 && idx < row.Length ? row[idx] : string.Empty;
        }
        return result;
    }

    /// <inheritdoc />
    public void DropTable(string tableName)
    {
        gate.Wait();
        try
        {
            var lines = ReadAllLines();
            DropTableCore(lines, tableName);
            WriteAllLines(lines);
        }
        finally { gate.Release(); }
    }

    /// <inheritdoc />
    public async Task DropTableAsync(string tableName, CancellationToken cancellationToken = default)
    {
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var lines = await ReadAllLinesAsync(cancellationToken).ConfigureAwait(false);
            DropTableCore(lines, tableName);
            await WriteAllLinesAsync(lines, cancellationToken).ConfigureAwait(false);
        }
        finally { gate.Release(); }
    }

    private static void DropTableCore(List<string> lines, string tableName)
    {
        if (!TableExists(lines, tableName, out var startLine, out var endLine))
            throw new InvalidOperationException("Table not found.");

        lines.RemoveRange(startLine, endLine - startLine);
    }

    /// <inheritdoc />
    public void DropDatabase()
    {
        gate.Wait();
        try { DropDatabaseCore(); }
        finally { gate.Release(); }
    }

    /// <inheritdoc />
    public async Task DropDatabaseAsync(CancellationToken cancellationToken = default)
    {
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try { DropDatabaseCore(); }
        finally { gate.Release(); }
    }

    private void DropDatabaseCore()
    {
        if (transactionFilePath != null)
        {
            TryDelete(transactionFilePath);
            transactionFilePath = null;
        }

        if (File.Exists(filePath))
            File.Delete(filePath);

        EnsureCreated();
    }

    private string GetCurrentFilePath() => transactionFilePath ?? filePath;

    private List<string> ReadAllLines() => File.ReadAllLines(GetCurrentFilePath(), Encoding.UTF8).ToList();

    private async Task<List<string>> ReadAllLinesAsync(CancellationToken cancellationToken)
        => (await File.ReadAllLinesAsync(GetCurrentFilePath(), Encoding.UTF8, cancellationToken).ConfigureAwait(false)).ToList();

    private void WriteAllLines(List<string> lines) => File.WriteAllLines(GetCurrentFilePath(), lines, Encoding.UTF8);

    private Task WriteAllLinesAsync(List<string> lines, CancellationToken cancellationToken)
        => File.WriteAllLinesAsync(GetCurrentFilePath(), lines, Encoding.UTF8, cancellationToken);

    private static bool TableExists(List<string> lines, string tableName, out int startLine, out int endLine)
    {
        startLine = -1;
        endLine = -1;

        for (var i = 0; i < lines.Count; i++)
        {
            var trimmed = lines[i].Trim();
            if (trimmed.StartsWith('#') && trimmed[1..].Equals(tableName, StringComparison.OrdinalIgnoreCase))
            {
                startLine = i;
                for (var j = i + 1; j < lines.Count; j++)
                {
                    if (lines[j].Trim().StartsWith('#'))
                    {
                        endLine = j;
                        break;
                    }
                }
                if (endLine == -1)
                    endLine = lines.Count;
                return true;
            }
        }
        return false;
    }

    private static Column[] ParseColumns(string columnDefinitions)
    {
        var columns = columnDefinitions
            .Split('|')
            .Select(cd =>
            {
                var parts = cd.Split(':');
                if (parts.Length != 2)
                    throw new InvalidOperationException("Invalid column definition.");

                var name = parts[0];
                if (string.IsNullOrWhiteSpace(name) || name.Contains('|') || name.Contains(':'))
                    throw new InvalidOperationException("Invalid column name.");

                if (!Enum.TryParse<DataType>(parts[1], ignoreCase: true, out var type))
                    throw new InvalidOperationException("Invalid column type.");

                return new Column(name, type);
            })
            .ToArray();

        if (columns.Length == 0)
            throw new InvalidOperationException("No columns defined.");

        return columns;
    }

    private static string[] ProcessData(Column[] columns, string[] data)
    {
        var result = new string[data.Length];
        for (var i = 0; i < data.Length; i++)
            result[i] = ConvertValue(columns[i].Type, data[i]);
        return result;
    }

    private static string ConvertValue(DataType type, string value) => type switch
    {
        DataType.Int => int.Parse(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture),
        DataType.Bool => bool.Parse(value).ToString().ToLowerInvariant(),
        DataType.String => value,
        _ => value
    };

    private static void ValidateTableName(string tableName)
    {
        if (string.IsNullOrWhiteSpace(tableName) || tableName.Contains('#') || tableName.Contains('|'))
            throw new ArgumentException("Invalid table name.", nameof(tableName));
    }

    private static void ValidateColumns(Column[] columns)
    {
        if (columns == null || columns.Length == 0)
            throw new ArgumentException("Columns cannot be null or empty.", nameof(columns));

        foreach (var column in columns)
            if (string.IsNullOrWhiteSpace(column.Name) || column.Name.Contains('|') || column.Name.Contains(':'))
                throw new ArgumentException("Invalid column name.", nameof(columns));
    }

    private static void TryDelete(string path)
    {
        try { if (File.Exists(path)) File.Delete(path); }
        catch { /* best-effort cleanup */ }
    }

    /// <summary>Disposes the database, releasing the internal semaphore and cleaning up any orphan transaction file.</summary>
    public void Dispose()
    {
        if (disposed) return;
        disposed = true;

        if (transactionFilePath != null)
        {
            TryDelete(transactionFilePath);
            transactionFilePath = null;
        }
        gate.Dispose();
    }
}
