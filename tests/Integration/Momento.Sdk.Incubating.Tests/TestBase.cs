using Momento.Sdk.Auth;

namespace Momento.Sdk.Incubating.Tests;

public class TestBase
{
    protected readonly ISimpleCacheClient client;
    protected readonly string cacheName;
    protected readonly ICredentialProvider authProvider;
    protected readonly TimeSpan defaultTtl = SimpleCacheClientFixture.DefaultTtl;

    public TestBase(SimpleCacheClientFixture fixture)
    {
        this.client = fixture.Client;
        this.cacheName = fixture.CacheName;
        this.authProvider = fixture.AuthProvider;
    }
}
