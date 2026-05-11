namespace WordDataBase;

/// <summary>Defines a table column by name and value type.</summary>
/// <param name="Name">Column name; must not contain <c>|</c> or <c>:</c>.</param>
/// <param name="Type">Data type stored in the column.</param>
public sealed record Column(string Name, DataType Type);
