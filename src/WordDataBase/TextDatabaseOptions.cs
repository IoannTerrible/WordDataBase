namespace WordDataBase;

/// <summary>Options for configuring a <see cref="TextDatabase"/> instance.</summary>
public sealed class TextDatabaseOptions
{
    /// <summary>Path to the database file. Must be non-empty and point to a file (not a directory).</summary>
    public string FilePath { get; set; } = string.Empty;
}
