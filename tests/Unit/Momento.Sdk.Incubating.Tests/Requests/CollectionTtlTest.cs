using System;
using Momento.Sdk.Incubating.Requests;

namespace Momento.Sdk.Incubating.Tests.Requests;

public class CollectionTtlTest
{
    [Fact]
    public void FromCacheTtl_NoArgs_HappyPath()
    {
        var collectionTtl = CollectionTtl.FromCacheTtl();
        Assert.True(collectionTtl.RefreshTtl);
        Assert.Null(collectionTtl.Ttl);
    }

    [Fact]
    public void Of_1DayTtl_HappyPath()
    {
        var collectionTtl = CollectionTtl.Of(TimeSpan.FromDays(1));
        Assert.True(collectionTtl.RefreshTtl, "RefreshTtl should be true but wasn't");
        Assert.Equal(TimeSpan.FromDays(1), collectionTtl.Ttl);
    }

    [Fact]
    public void WithRefreshTtlOnUpdates_OverrideRefresh_HappyPath()
    {
        var collectionTtl = new CollectionTtl(null, false);
        Assert.Null(collectionTtl.Ttl);
        Assert.False(collectionTtl.RefreshTtl, "RefreshTtl should be false but wasn't");

        var newCollectionTtl = collectionTtl.WithRefreshTtlOnUpdates();
        Assert.Equal(collectionTtl.Ttl, newCollectionTtl.Ttl);
        Assert.True(newCollectionTtl.RefreshTtl, "RefreshTtl should be true but wasn't");

        // Test propgates ttl
        collectionTtl = new CollectionTtl(TimeSpan.FromDays(1), false);
        Assert.Equal(TimeSpan.FromDays(1), collectionTtl.Ttl);
        Assert.False(collectionTtl.RefreshTtl, "RefreshTtl should be false but wasn't");

        newCollectionTtl = collectionTtl.WithRefreshTtlOnUpdates();
        Assert.Equal(collectionTtl.Ttl, newCollectionTtl.Ttl);
        Assert.True(newCollectionTtl.RefreshTtl, "RefreshTtl should be true but wasn't");

        // Test doesn't change refresh ttl
        collectionTtl = new CollectionTtl(null, true);
        Assert.Null(collectionTtl.Ttl);
        Assert.True(collectionTtl.RefreshTtl, "RefreshTtl should be true but wasn't");

        newCollectionTtl = collectionTtl.WithRefreshTtlOnUpdates();
        Assert.Null(newCollectionTtl.Ttl);
        Assert.True(newCollectionTtl.RefreshTtl);
    }

    [Fact]
    public void WithNoRefreshTtlOnUpdates_OverrideRefresh_HappyPath()
    {
        var collectionTtl = new CollectionTtl(null, true);
        Assert.Null(collectionTtl.Ttl);
        Assert.True(collectionTtl.RefreshTtl, "RefreshTtl should be true but wasn't");

        var newCollectionTtl = collectionTtl.WithNoRefreshTtlOnUpdates();
        Assert.Equal(collectionTtl.Ttl, newCollectionTtl.Ttl);
        Assert.False(newCollectionTtl.RefreshTtl, "RefreshTtl should be false but wasn't");

        // Test propgates ttl
        collectionTtl = new CollectionTtl(TimeSpan.FromDays(1), true);
        Assert.Equal(TimeSpan.FromDays(1), collectionTtl.Ttl);
        Assert.True(collectionTtl.RefreshTtl, "RefreshTtl should be true but wasn't");

        newCollectionTtl = collectionTtl.WithNoRefreshTtlOnUpdates();
        Assert.Equal(collectionTtl.Ttl, newCollectionTtl.Ttl);
        Assert.False(newCollectionTtl.RefreshTtl, "RefreshTtl should be false but wasn't");

        // Test doesn't change refresh ttl
        collectionTtl = new CollectionTtl(null, false);
        Assert.Null(collectionTtl.Ttl);
        Assert.False(collectionTtl.RefreshTtl, "RefreshTtl should be false but wasn't");

        newCollectionTtl = collectionTtl.WithNoRefreshTtlOnUpdates();
        Assert.Null(newCollectionTtl.Ttl);
        Assert.False(newCollectionTtl.RefreshTtl);
    }
}
