using Microsoft.Extensions.Logging;
using Momento.Sdk.Config;

namespace Momento.Sdk.Incubating;

/// <summary>
/// Factory class used to instantiate the incubating Simple Cache Client.
///
/// Use this to enable preview features in the cache client.
/// </summary>
public class SimpleCacheClientFactory
{
    /// <summary>
    /// Instantiate an instance of the incubating Simple Cache Client.
    /// </summary>
    /// <param name="config">Configuration to use for the transport, retries, middlewares. See <see href="https://github.com/momentohq/client-sdk-dotnet/blob/main/src/Momento.Sdk/Config/Configurations.cs"/> for out-of-the-box configuration choices, eg <see href="https://github.com/momentohq/client-sdk-dotnet/blob/main/src/Momento.Sdk/Config/Configurations.cs#L22"/></param>
    /// <param name="authToken">Momento JWT.</param>
    /// <param name="defaultTtlSeconds">Default time to live for the item in cache.</param>
    /// <param name="loggerFactory">Logger factory to create loggers for contained instances.</param>
    /// <returns>An instance of the incubating Simple Cache Client.</returns>
    public static SimpleCacheClient CreateClient(IConfiguration config, string authToken, uint defaultTtlSeconds, ILoggerFactory? loggerFactory = null)
    {
        var simpleCacheClient = new Momento.Sdk.SimpleCacheClient(config, authToken, defaultTtlSeconds, loggerFactory);
        return new SimpleCacheClient(simpleCacheClient, config, authToken, defaultTtlSeconds, loggerFactory);
    }
}
