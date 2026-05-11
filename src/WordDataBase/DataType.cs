namespace WordDataBase;

/// <summary>Supported column value types for <see cref="TextDatabase"/>.</summary>
public enum DataType
{
    /// <summary>32-bit signed integer.</summary>
    Int,

    /// <summary>Arbitrary text without the pipe (<c>|</c>) character.</summary>
    String,

    /// <summary>Boolean value (<c>true</c>/<c>false</c>).</summary>
    Bool
}
