using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace WordDataBase;

/// <summary>Dependency-injection registration helpers for <see cref="TextDatabase"/>.</summary>
public static class ServiceCollectionExtensions
{
    /// <summary>Registers <see cref="IDatabase"/> backed by <see cref="TextDatabase"/> as a singleton, configured via options callback.</summary>
    public static IServiceCollection AddSimpleTextDatabase(
        this IServiceCollection services,
        Action<TextDatabaseOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.Configure(configure);
        services.AddSingleton<IDatabase>(sp =>
        {
            var opts = sp.GetRequiredService<IOptions<TextDatabaseOptions>>().Value;
            return new TextDatabase(opts.FilePath);
        });
        return services;
    }

    /// <summary>Registers <see cref="IDatabase"/> backed by <see cref="TextDatabase"/> at the given file path.</summary>
    public static IServiceCollection AddSimpleTextDatabase(this IServiceCollection services, string filePath)
        => services.AddSimpleTextDatabase(opts => opts.FilePath = filePath);
}
