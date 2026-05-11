using Microsoft.Extensions.DependencyInjection;

namespace WordDataBase.Tests;

public class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddSimpleTextDatabase_Action_RegistersSingleton()
    {
        using var tmp = new TempFile();
        var services = new ServiceCollection();
        services.AddSimpleTextDatabase(o => o.FilePath = tmp.Path);

        using var sp = services.BuildServiceProvider();
        var a = sp.GetRequiredService<IDatabase>();
        var b = sp.GetRequiredService<IDatabase>();

        Assert.Same(a, b);
    }

    [Fact]
    public void AddSimpleTextDatabase_Path_RegistersAndWorks()
    {
        using var tmp = new TempFile();
        var services = new ServiceCollection();
        services.AddSimpleTextDatabase(tmp.Path);

        using var sp = services.BuildServiceProvider();
        var db = sp.GetRequiredService<IDatabase>();
        db.CreateTable("t", new[] { new Column("Id", DataType.Int) });
        db.InsertData("t", new[] { "1" });

        Assert.Single(db.Select("t"));
    }

    [Fact]
    public void AddSimpleTextDatabase_NullServices_Throws()
    {
        IServiceCollection? services = null;
        Assert.Throws<ArgumentNullException>(() => services!.AddSimpleTextDatabase(_ => { }));
    }

    [Fact]
    public void AddSimpleTextDatabase_NullConfigure_Throws()
    {
        var services = new ServiceCollection();
        Assert.Throws<ArgumentNullException>(() => services.AddSimpleTextDatabase((Action<TextDatabaseOptions>)null!));
    }
}
