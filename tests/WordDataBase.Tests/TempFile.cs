namespace WordDataBase.Tests;

/// <summary>Disposable temp file path holder for tests.</summary>
internal sealed class TempFile : IDisposable
{
    public string Path { get; }

    public TempFile()
    {
        Path = System.IO.Path.Combine(System.IO.Path.GetTempPath(),
            $"wdb-test-{Guid.NewGuid():N}.txt");
    }

    public void Dispose()
    {
        try { if (File.Exists(Path)) File.Delete(Path); }
        catch { /* ignore */ }
    }
}
