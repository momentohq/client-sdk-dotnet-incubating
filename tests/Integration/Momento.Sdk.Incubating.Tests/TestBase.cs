namespace Momento.Sdk.Incubating.Tests;

public class TestBase
{
    protected readonly SimpleCacheClient client;
    protected readonly string cacheName;
    protected readonly string authToken;
    protected const uint defaultTtlSeconds = SimpleCacheClientFixture.DefaultTtlSeconds;

    public TestBase(SimpleCacheClientFixture fixture)
    {
        this.client = fixture.Client;
        this.cacheName = fixture.CacheName;
        this.authToken = fixture.AuthToken;
    }
}
