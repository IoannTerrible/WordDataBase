using WordDataBase;

namespace SimpleTextDatabase
{
    public record Column(string Name, DataType Type);

    public record Table(string Name, Column[] Columns);

    public class TextDatabase : IDatabase
    {
        private readonly string filePath;
        private string transactionFilePath;

        public TextDatabase(string filePath)
        {
            this.filePath = filePath ?? throw new ArgumentNullException(nameof(filePath));
            EnsureCreated();
        }

        /// <summary>Ensures the database file is created if it does not exist.</summary>
        public void EnsureCreated()
        {
            if (!File.Exists(filePath))
            {
                using (File.Create(filePath)) { }
            }
        }

        /// <summary>Begins a transaction by creating a temporary file for changes.</summary>
        /// <exception cref="InvalidOperationException">Thrown if a transaction is already in progress.</exception>
        public void BeginTransaction()
        {
            if (transactionFilePath != null)
            {
                throw new InvalidOperationException("A transaction is already in progress.");
            }

            transactionFilePath = Path.GetTempFileName();
            File.Copy(filePath, transactionFilePath, overwrite: true);
        }

        /// <summary>Commits the current transaction by applying changes to the main file.</summary>
        /// <exception cref="InvalidOperationException">Thrown if no transaction is in progress.</exception>
        public void CommitTransaction()
        {
            if (transactionFilePath == null)
            {
                throw new InvalidOperationException("No transaction is in progress.");
            }

            File.Copy(transactionFilePath, filePath, overwrite: true);
            File.Delete(transactionFilePath);
            transactionFilePath = null;
        }

        /// <summary>Rolls back the current transaction by discarding changes.</summary>
        /// <exception cref="InvalidOperationException">Thrown if no transaction is in progress.</exception>
        public void RollbackTransaction()
        {
            if (transactionFilePath == null)
            {
                throw new InvalidOperationException("No transaction is in progress.");
            }

            File.Delete(transactionFilePath);
            transactionFilePath = null;
        }

        /// <summary>Creates a new table with the specified name and columns.</summary>
        /// <param name="tableName">The name of the table to be created.</param>
        /// <param name="columns">An array of columns defining the table schema.</param>
        /// <exception cref="ArgumentException">Thrown if the table name or column names are invalid.</exception>
        /// <exception cref="InvalidOperationException">Thrown if the table already exists.</exception>
        public void CreateTable(string tableName, Column[] columns)
        {
            if (
                string.IsNullOrWhiteSpace(tableName)
                || tableName.Contains("#")
                || tableName.Contains("|")
            )
            {
                throw new ArgumentException("Invalid table name.", nameof(tableName));
            }

            if (columns == null || columns.Length == 0)
            {
                throw new ArgumentException("Columns cannot be null or empty.", nameof(columns));
            }

            foreach (var column in columns)
            {
                if (
                    string.IsNullOrWhiteSpace(column.Name)
                    || column.Name.Contains("|")
                    || column.Name.Contains(":")
                )
                {
                    throw new ArgumentException("Invalid column name.", nameof(column.Name));
                }
            }

            if (TableExists(tableName, out int startLine, out int endLine))
            {
                throw new InvalidOperationException("Table already exists.");
            }

            var path = GetCurrentFilePath();
            using (var writer = new StreamWriter(path, append: true))
            {
                var columnDefinitions = string.Join("|", columns.Select(c => $"{c.Name}:{c.Type}"));
                writer.WriteLine($"#{tableName}");
                writer.WriteLine(columnDefinitions);
                writer.WriteLine();
            }
        }

        /// <summary>Inserts data into the specified table.</summary>
        /// <param name="tableName">The name of the table to insert data into.</param>
        /// <param name="data">An array of values to insert into the table.</param>
        /// <exception cref="InvalidOperationException">Thrown if the table is not found.</exception>
        /// <exception cref="ArgumentException">Thrown if the data length does not match the number of columns.</exception>
        public void InsertData(string tableName, string[] data)
        {
            if (!TableExists(tableName, out int startLine, out int endLine))
            {
                throw new InvalidOperationException("Table not found.");
            }

            var columns = ParseColumns(GetLine(startLine + 1));
            if (data.Length != columns.Length)
            {
                throw new ArgumentException("Data length does not match the number of columns.");
            }

            foreach (var value in data)
            {
                if (value.Contains("|"))
                {
                    throw new ArgumentException("Data values cannot contain the '|' character.");
                }
            }

            var processedData = ProcessData(columns, data);

            var path = GetCurrentFilePath();
            var lines = File.ReadAllLines(path).ToList();
            lines.Insert(endLine, string.Join("|", processedData));
            File.WriteAllLines(path, lines);
        }

        /// <summary>Selects rows from the specified table with optional filtering and sorting.</summary>
        /// <param name="tableName">The name of the table to select data from.</param>
        /// <param name="columns">An array of column names to select, or null to select all columns.</param>
        /// <param name="filter">A predicate function to filter rows, or null to include all rows.</param>
        /// <param name="orderByColumn">The name of the column to order the results by, or null for no ordering.</param>
        /// <returns>A collection of selected rows as arrays of strings.</returns>
        /// <exception cref="InvalidOperationException">Thrown if the table is not found.</exception>
        /// <exception cref="ArgumentException">Thrown if the column name for ordering is invalid.</exception>
        public IEnumerable<string[]> Select(
            string tableName,
            string[] columns = null,
            Func<string[], bool> filter = null,
            string orderByColumn = null
        )
        {
            if (!TableExists(tableName, out int startLine, out int endLine))
            {
                throw new InvalidOperationException("Table not found.");
            }

            var allColumns = ParseColumns(GetLine(startLine + 1));
            var columnIndices = GetColumnIndices(allColumns, columns);
            var rows = FilterRows(startLine, endLine, columnIndices, filter);

            if (!string.IsNullOrEmpty(orderByColumn))
            {
                rows = SortRows(rows, allColumns, orderByColumn);
            }

            return rows;
        }

        private int[] GetColumnIndices(Column[] allColumns, string[] columns)
        {
            return columns
                    ?.Select(c =>
                        Array.FindIndex(
                            allColumns,
                            col => col.Name.Equals(c, StringComparison.OrdinalIgnoreCase)
                        )
                    )
                    .ToArray() ?? Enumerable.Range(0, allColumns.Length).ToArray();
        }

        private List<string[]> FilterRows(
            int startLine,
            int endLine,
            int[] columnIndices,
            Func<string[], bool> filter
        )
        {
            var rows = new List<string[]>();

            for (int i = startLine + 2; i < endLine; i++)
            {
                var line = GetLine(i);
                if (string.IsNullOrWhiteSpace(line))
                {
                    continue;
                }

                var row = line.Split('|');

                var selectedRow = new string[columnIndices.Length];

                for (int j = 0; j < columnIndices.Length; j++)
                {
                    var columnIndex = columnIndices[j];
                    if (columnIndex < row.Length)
                    {
                        selectedRow[j] = row[columnIndex];
                    }
                    else
                    {
                        selectedRow[j] = null;
                    }
                }

                if (filter == null || filter(selectedRow))
                {
                    rows.Add(selectedRow);
                }
            }
            return rows;
        }

        private List<string[]> SortRows(
            List<string[]> rows,
            Column[] allColumns,
            string orderByColumn
        )
        {
            var orderByIndex = Array.FindIndex(
                allColumns,
                col => col.Name.Equals(orderByColumn, StringComparison.OrdinalIgnoreCase)
            );

            if (orderByIndex == -1)
            {
                throw new ArgumentException(
                    "Invalid column name for ordering.",
                    nameof(orderByColumn)
                );
            }

            return rows.OrderBy(r => r[orderByIndex]).ToList();
        }

        /// <summary>Drops (deletes) the specified table from the database.</summary>
        /// <param name="tableName">The name of the table to drop.</param>
        /// <exception cref="InvalidOperationException">Thrown if the table is not found.</exception>
        public void DropTable(string tableName)
        {
            if (!TableExists(tableName, out int startLine, out int endLine))
            {
                throw new InvalidOperationException("Table not found.");
            }

            var path = GetCurrentFilePath();
            var lines = File.ReadAllLines(path).ToList();
            lines.RemoveRange(startLine, endLine - startLine);
            File.WriteAllLines(path, lines);
        }

        /// <summary>Deletes the database file and recreates it.</summary>
        public void DropDatabase()
        {
            File.Delete(filePath);
            EnsureCreated();
        }

        private string GetCurrentFilePath()
        {
            return transactionFilePath ?? filePath;
        }

        private bool TableExists(string tableName, out int startLine, out int endLine)
        {
            startLine = -1;
            endLine = -1;

            var lines = File.ReadAllLines(GetCurrentFilePath());
            for (int i = 0; i < lines.Length; i++)
            {
                if (
                    lines[i].Trim().StartsWith("#")
                    && lines[i]
                        .Trim()
                        .Substring(1)
                        .Equals(tableName, StringComparison.OrdinalIgnoreCase)
                )
                {
                    startLine = i;
                    for (int j = i + 1; j < lines.Length; j++)
                    {
                        if (lines[j].Trim().StartsWith("#"))
                        {
                            endLine = j;
                            break;
                        }
                    }
                    if (endLine == -1)
                    {
                        endLine = lines.Length;
                    }
                    break;
                }
            }

            return startLine != -1 && endLine != -1;
        }

        private string GetLine(int lineNumber)
        {
            using (var reader = new StreamReader(GetCurrentFilePath()))
            {
                for (int i = 0; i < lineNumber; i++)
                {
                    if (reader.ReadLine() == null)
                    {
                        throw new InvalidOperationException("Line number out of range.");
                    }
                }

                return reader.ReadLine();
            }
        }

        private Column[] ParseColumns(string columnDefinitions)
        {
            var columns = columnDefinitions
                .Split('|')
                .Select(cd =>
                {
                    var parts = cd.Split(':');
                    if (parts.Length != 2)
                    {
                        throw new InvalidOperationException("Invalid column definition.");
                    }

                    var name = parts[0];
                    if (string.IsNullOrWhiteSpace(name) || name.Contains("|") || name.Contains(":"))
                    {
                        throw new InvalidOperationException("Invalid column name.");
                    }

                    if (!Enum.TryParse<DataType>(parts[1], ignoreCase: true, out var type))
                    {
                        throw new InvalidOperationException("Invalid column type.");
                    }

                    return new Column(name, type);
                })
                .ToArray();

            if (columns.Length == 0)
            {
                throw new InvalidOperationException("No columns defined.");
            }

            return columns;
        }

        private string[] ProcessData(Column[] columns, string[] data, bool reverse = false)
        {
            return data.Select(
                    (value, index) =>
                    {
                        var columnType = columns[index].Type;
                        return ConvertValue(columnType, value, reverse);
                    }
                )
                .ToArray();
        }

        private string ConvertValue(DataType type, string value, bool reverse = false)
        {
            return type switch
            {
                DataType.Int => reverse ? int.Parse(value).ToString() : int.Parse(value).ToString(),
                DataType.Bool
                    => reverse
                        ? bool.Parse(value).ToString().ToLower()
                        : bool.Parse(value).ToString().ToLower(),
                DataType.String => value,
                _ => value
            };
        }
    }
}
