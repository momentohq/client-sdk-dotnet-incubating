using Momento.Sdk.Internal.ExtensionMethods;
using Momento.Sdk.Responses;
using Momento.Sdk.Incubating.Responses;

namespace Momento.Sdk.Incubating.Tests;

[Collection("SimpleCacheClient")]
public class ListTest : TestBase
{
    public ListTest(SimpleCacheClientFixture fixture) : base(fixture)
    {
    }

    [Theory]
    [InlineData(null, "my-list", new byte[] { 0x00 })]
    [InlineData("cache", null, new byte[] { 0x00 })]
    [InlineData("cache", "my-list", null)]
    public async Task ListPushFrontAsync_NullChecksByteArray_ThrowsException(string cacheName, string listName, byte[] value)
    {
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.ListPushFrontAsync(cacheName, listName, value, false));
    }

    [Fact]
    public async Task ListPushFrontFetch_ValueIsByteArray_HappyPath()
    {
        var listName = Utils.NewGuidString();
        var value1 = Utils.NewGuidByteArray();

        CacheListPushFrontResponse pushResponse = await client.ListPushFrontAsync(cacheName, listName, value1, false);
        var successResponse = (CacheListPushFrontResponse.Success)pushResponse;
        Assert.Equal(1, successResponse.ListLength);

        CacheListFetchResponse fetchResponse = await client.ListFetchAsync(cacheName, listName);
        var hitResponse = (CacheListFetchResponse.Hit)fetchResponse;

        var list = hitResponse.ByteArrayList;
        Assert.Single(list);
        Assert.Contains(value1, list);

        // Test push semantics
        var value2 = Utils.NewGuidByteArray();
        pushResponse = await client.ListPushFrontAsync(cacheName, listName, value2, false);
        successResponse = (CacheListPushFrontResponse.Success)pushResponse;
        Assert.Equal(2, successResponse.ListLength);

        fetchResponse = await client.ListFetchAsync(cacheName, listName);
        hitResponse = (CacheListFetchResponse.Hit)fetchResponse;
        list = hitResponse.ByteArrayList!;
        Assert.Equal(value2, list[0]);
        Assert.Equal(value1, list[1]);
    }

    [Fact]
    public async Task ListPushFrontFetch_ValueIsByteArray_NoRefreshTtl()
    {
        var listName = Utils.NewGuidString();
        var value = Utils.NewGuidByteArray();

        await client.ListPushFrontAsync(cacheName, listName, value, false, ttlSeconds: 5);
        await Task.Delay(100);

        await client.ListPushFrontAsync(cacheName, listName, value, false, ttlSeconds: 10);
        await Task.Delay(4900);

        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True((CacheListFetchResponse.Miss)response is CacheListFetchResponse.Miss);
    }

    [Fact]
    public async Task ListPushFrontFetch_ValueIsByteArray_RefreshTtl()
    {
        var listName = Utils.NewGuidString();
        var value = Utils.NewGuidByteArray();

        await client.ListPushFrontAsync(cacheName, listName, value, false, ttlSeconds: 2);
        await client.ListPushFrontAsync(cacheName, listName, value, true, ttlSeconds: 10);
        await Task.Delay(2000);

        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        var hitResponse = (CacheListFetchResponse.Hit)response;
        Assert.Equal(2, hitResponse.ByteArrayList!.Count);
    }

    [Fact]
    public async Task ListPushFrontAsync_ValueIsByteArrayTruncateBackToSizeIsZero_ThrowsException()
    {
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await client.ListPushFrontAsync("myCache", "listName", new byte[] { 0x00 }, false, truncateBackToSize: 0));
    }

    [Theory]
    [InlineData(null, "my-list", "my-value")]
    [InlineData("cache", null, "my-value")]
    [InlineData("cache", "my-list", null)]
    public async Task ListPushFrontAsync_NullChecksString_ThrowsException(string cacheName, string listName, string value)
    {
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.ListPushFrontAsync(cacheName, listName, value, false));
    }

    [Fact]
    public async Task ListPushFrontFetch_ValueIsString_HappyPath()
    {
        var listName = Utils.NewGuidString();
        var value1 = Utils.NewGuidString();

        CacheListPushFrontResponse pushResponse = await client.ListPushFrontAsync(cacheName, listName, value1, false);
        var successResponse = (CacheListPushFrontResponse.Success)pushResponse;
        Assert.Equal(1, successResponse.ListLength);

        CacheListFetchResponse fetchResponse = await client.ListFetchAsync(cacheName, listName);
        var hitResponse = (CacheListFetchResponse.Hit)fetchResponse;

        var list = hitResponse.StringList();
        Assert.Single(list);
        Assert.Contains(value1, list);

        // Test push semantics
        var value2 = Utils.NewGuidString();
        pushResponse = await client.ListPushFrontAsync(cacheName, listName, value2, false);
        successResponse = (CacheListPushFrontResponse.Success)pushResponse;
        Assert.Equal(2, successResponse.ListLength);

        fetchResponse = await client.ListFetchAsync(cacheName, listName);
        hitResponse = (CacheListFetchResponse.Hit)fetchResponse;
        list = hitResponse.StringList()!;
        Assert.Equal(value2, list[0]);
        Assert.Equal(value1, list[1]);
    }

    [Fact]
    public async Task ListPushFrontFetch_ValueIsString_NoRefreshTtl()
    {
        var listName = Utils.NewGuidString();
        var value = Utils.NewGuidString();

        await client.ListPushFrontAsync(cacheName, listName, value, false, ttlSeconds: 5);
        await Task.Delay(100);

        await client.ListPushFrontAsync(cacheName, listName, value, false, ttlSeconds: 10);
        await Task.Delay(4900);

        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True((CacheListFetchResponse.Miss)response is CacheListFetchResponse.Miss);
    }

    [Fact]
    public async Task ListPushFrontFetch_ValueIsString_RefreshTtl()
    {
        var listName = Utils.NewGuidString();
        var value = Utils.NewGuidString();

        await client.ListPushFrontAsync(cacheName, listName, value, false, ttlSeconds: 2);
        await client.ListPushFrontAsync(cacheName, listName, value, true, ttlSeconds: 10);
        await Task.Delay(2000);

        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        var hitResponse = (CacheListFetchResponse.Hit)response;
        Assert.Equal(2, hitResponse.StringList()!.Count);
    }

    [Fact]
    public async Task ListPushFrontAsync_ValueIsStringTruncateBackToSizeIsZero_ThrowsException()
    {
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await client.ListPushFrontAsync("myCache", "listName", "value", false, truncateBackToSize: 0));
    }

    [Theory]
    [InlineData(null, "my-list", new byte[] { 0x00 })]
    [InlineData("cache", null, new byte[] { 0x00 })]
    [InlineData("cache", "my-list", null)]
    public async Task ListPushBackAsync_NullChecksByteArray_ThrowsException(string cacheName, string listName, byte[] value)
    {
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.ListPushBackAsync(cacheName, listName, value, false));
    }

    [Fact]
    public async Task ListPushBackFetch_ValueIsByteArray_HappyPath()
    {
        var listName = Utils.NewGuidString();
        var value1 = Utils.NewGuidByteArray();

        CacheListPushBackResponse pushResponse = await client.ListPushBackAsync(cacheName, listName, value1, false);
        var successResponse = (CacheListPushBackResponse.Success)pushResponse;
        Assert.Equal(1, successResponse.ListLength);

        CacheListFetchResponse fetchResponse = await client.ListFetchAsync(cacheName, listName);
        var hitResponse = (CacheListFetchResponse.Hit)fetchResponse;

        var list = hitResponse.ByteArrayList;
        Assert.Single(list);
        Assert.Contains(value1, list);

        // Test push semantics
        var value2 = Utils.NewGuidByteArray();
        pushResponse = await client.ListPushBackAsync(cacheName, listName, value2, false);
        successResponse = (CacheListPushBackResponse.Success)pushResponse;
        Assert.Equal(2, successResponse.ListLength);

        fetchResponse = await client.ListFetchAsync(cacheName, listName);
        hitResponse = (CacheListFetchResponse.Hit)fetchResponse;
        list = hitResponse.ByteArrayList!;
        Assert.Equal(value1, list[0]);
        Assert.Equal(value2, list[1]);
    }

    [Fact]
    public async Task ListPushBackFetch_ValueIsByteArray_NoRefreshTtl()
    {
        var listName = Utils.NewGuidString();
        var value = Utils.NewGuidByteArray();

        await client.ListPushBackAsync(cacheName, listName, value, false, ttlSeconds: 5);
        await Task.Delay(100);

        await client.ListPushBackAsync(cacheName, listName, value, false, ttlSeconds: 10);
        await Task.Delay(4900);

        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True((CacheListFetchResponse.Miss)response is CacheListFetchResponse.Miss);
    }

    [Fact]
    public async Task ListPushBackFetch_ValueIsByteArray_RefreshTtl()
    {
        var listName = Utils.NewGuidString();
        var value = Utils.NewGuidByteArray();

        await client.ListPushBackAsync(cacheName, listName, value, false, ttlSeconds: 2);
        await client.ListPushBackAsync(cacheName, listName, value, true, ttlSeconds: 10);
        await Task.Delay(2000);

        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        var hitResponse = (CacheListFetchResponse.Hit)response;
        Assert.Equal(2, hitResponse.ByteArrayList!.Count);
    }

    [Fact]
    public async Task ListPushBackAsync_ValueIsByteArrayTruncateFrontToSizeIsZero_ThrowsException()
    {
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await client.ListPushBackAsync("myCache", "listName", new byte[] { 0x00 }, false, truncateFrontToSize: 0));
    }

    [Theory]
    [InlineData(null, "my-list", "my-value")]
    [InlineData("cache", null, "my-value")]
    [InlineData("cache", "my-list", null)]
    public async Task ListPushBackAsync_NullChecksString_ThrowsException(string cacheName, string listName, string value)
    {
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.ListPushBackAsync(cacheName, listName, value, false));
    }

    [Fact]
    public async Task ListPushBackTruncate_TruncatesList_String()
    {
        var listName = Utils.NewGuidString();
        var value1 = Utils.NewGuidString();
        var value2 = Utils.NewGuidString();
        var value3 = Utils.NewGuidString();
        await client.ListPushBackAsync(cacheName, listName, value1, false);
        await client.ListPushBackAsync(cacheName, listName, value2, false);
        await client.ListPushBackAsync(cacheName, listName, value3, false, null, 2);
        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        var hitResponse = (CacheListFetchResponse.Hit)response;
        Assert.Equal(2, hitResponse.StringList()!.Count);
        Assert.Equal(value2, hitResponse.StringList()![0]);
        Assert.Equal(value3, hitResponse.StringList()![1]);
    }

    [Fact]
    public async Task ListPushBackTruncate_TruncatesList_Bytes()
    {
        var listName = Utils.NewGuidString();
        var value1 = Utils.NewGuidByteArray();
        var value2 = Utils.NewGuidByteArray();
        var value3 = Utils.NewGuidByteArray();
        await client.ListPushBackAsync(cacheName, listName, value1, false);
        await client.ListPushBackAsync(cacheName, listName, value2, false);
        await client.ListPushBackAsync(cacheName, listName, value3, false, null, 2);
        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        var hitResponse = (CacheListFetchResponse.Hit)response;
        Assert.Equal(2, hitResponse.ByteArrayList!.Count);
        Assert.Equal(value2, hitResponse.ByteArrayList![0]);
        Assert.Equal(value3, hitResponse.ByteArrayList![1]);
    }

    [Fact]
    public async Task ListPushFrontTruncate_TruncatesList_String()
    {
        var listName = Utils.NewGuidString();
        var value1 = Utils.NewGuidString();
        var value2 = Utils.NewGuidString();
        var value3 = Utils.NewGuidString();
        await client.ListPushFrontAsync(cacheName, listName, value1, false);
        await client.ListPushFrontAsync(cacheName, listName, value2, false);
        await client.ListPushFrontAsync(cacheName, listName, value3, false, null, 2);
        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        var hitResponse = (CacheListFetchResponse.Hit)response;
        Assert.Equal(2, hitResponse.StringList()!.Count);
        Assert.Equal(value2, hitResponse.StringList()![1]);
        Assert.Equal(value3, hitResponse.StringList()![0]);
    }

    [Fact]
    public async Task ListPushFrontTruncate_TruncatesList_Bytes()
    {
        var listName = Utils.NewGuidString();
        var value1 = Utils.NewGuidByteArray();
        var value2 = Utils.NewGuidByteArray();
        var value3 = Utils.NewGuidByteArray();
        await client.ListPushFrontAsync(cacheName, listName, value1, false);
        await client.ListPushFrontAsync(cacheName, listName, value2, false);
        await client.ListPushFrontAsync(cacheName, listName, value3, false, null, 2);
        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        var hitResponse = (CacheListFetchResponse.Hit)response;
        Assert.Equal(2, hitResponse.ByteArrayList!.Count);
        Assert.Equal(value2, hitResponse.ByteArrayList![1]);
        Assert.Equal(value3, hitResponse.ByteArrayList![0]);
    }

    [Fact]
    public async Task ListPushBackFetch_ValueIsString_HappyPath()
    {
        var listName = Utils.NewGuidString();
        var value1 = Utils.NewGuidString();

        CacheListPushBackResponse pushResponse = await client.ListPushBackAsync(cacheName, listName, value1, false);
        var successResponse = (CacheListPushBackResponse.Success)pushResponse;
        Assert.Equal(1, successResponse.ListLength);

        CacheListFetchResponse fetchResponse = await client.ListFetchAsync(cacheName, listName);
        var hitResponse = (CacheListFetchResponse.Hit)fetchResponse;

        var list = hitResponse.StringList();
        Assert.Single(list);
        Assert.Contains(value1, list);

        // Test push semantics
        var value2 = Utils.NewGuidString();
        pushResponse = await client.ListPushBackAsync(cacheName, listName, value2, false);
        successResponse = (CacheListPushBackResponse.Success)pushResponse;
        Assert.Equal(2, successResponse.ListLength);

        fetchResponse = await client.ListFetchAsync(cacheName, listName);
        hitResponse = (CacheListFetchResponse.Hit)fetchResponse;
        list = hitResponse.StringList()!;
        Assert.Equal(value1, list[0]);
        Assert.Equal(value2, list[1]);
    }

    [Fact]
    public async Task ListPushBackFetch_ValueIsString_NoRefreshTtl()
    {
        var listName = Utils.NewGuidString();
        var value = Utils.NewGuidString();

        await client.ListPushBackAsync(cacheName, listName, value, false, ttlSeconds: 5);
        await Task.Delay(100);

        await client.ListPushBackAsync(cacheName, listName, value, false, ttlSeconds: 10);
        await Task.Delay(4900);

        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True((CacheListFetchResponse.Miss)response is CacheListFetchResponse.Miss);
    }

    [Fact]
    public async Task ListPushBackFetch_ValueIsString_RefreshTtl()
    {
        var listName = Utils.NewGuidString();
        var value = Utils.NewGuidString();

        await client.ListPushBackAsync(cacheName, listName, value, false, ttlSeconds: 2);
        await client.ListPushBackAsync(cacheName, listName, value, true, ttlSeconds: 10);
        await Task.Delay(2000);

        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        var hitResponse = (CacheListFetchResponse.Hit)response;
        Assert.Equal(2, hitResponse.StringList()!.Count);
    }

    [Fact]
    public async Task ListPushBackAsync_ValueIsStringTruncateFrontToSizeIsZero_ThrowsException()
    {
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await client.ListPushBackAsync("myCache", "listName", "value", false, truncateFrontToSize: 0));
    }

    [Theory]
    [InlineData(null, "my-list")]
    [InlineData("cache", null)]
    public async Task ListPopFrontAsync_NullChecks_ThrowsException(string cacheName, string listName)
    {
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.ListPopFrontAsync(cacheName, listName));
    }

    [Fact]
    public async Task ListPopFrontAsync_ListIsMissing_HappyPath()
    {
        var listName = Utils.NewGuidString();
        CacheListPopFrontResponse response = await client.ListPopFrontAsync(cacheName, listName);
        var missResponse = (CacheListPopFrontResponse.Miss)response;
        Assert.Null(missResponse.ByteArray);
        Assert.Null(missResponse.String());
    }

    [Fact]
    public async Task ListPopFrontAsync_ValueIsByteArray_HappyPath()
    {
        var listName = Utils.NewGuidString();
        var value1 = Utils.NewGuidByteArray();
        var value2 = Utils.NewGuidByteArray();

        await client.ListPushFrontAsync(cacheName, listName, value1, false);
        await client.ListPushFrontAsync(cacheName, listName, value2, false);
        CacheListPopFrontResponse response = await client.ListPopFrontAsync(cacheName, listName);
        var hitResponse = (CacheListPopFrontResponse.Hit)response;

        Assert.Equal(value2, hitResponse.ByteArray);
    }

    [Fact]
    public async Task ListPopFrontAsync_ValueIsString_HappyPath()
    {
        var listName = Utils.NewGuidString();
        var value1 = Utils.NewGuidString();
        var value2 = Utils.NewGuidString();

        await client.ListPushFrontAsync(cacheName, listName, value1, false);
        await client.ListPushFrontAsync(cacheName, listName, value2, false);
        CacheListPopFrontResponse response = await client.ListPopFrontAsync(cacheName, listName);
        var hitResponse = (CacheListPopFrontResponse.Hit)response;

        Assert.Equal(value2, hitResponse.String());
    }

    [Theory]
    [InlineData(null, "my-list")]
    [InlineData("cache", null)]
    public async Task ListPopBackAsync_NullChecks_ThrowsException(string cacheName, string listName)
    {
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.ListPopBackAsync(cacheName, listName));
    }

    [Fact]
    public async Task ListPopBackAsync_ListIsMissing_HappyPath()
    {
        var listName = Utils.NewGuidString();
        CacheListPopBackResponse response = await client.ListPopBackAsync(cacheName, listName);
        var missResponse = (CacheListPopBackResponse.Miss)response;
        Assert.Null(missResponse.ByteArray);
        Assert.Null(missResponse.String());
    }

    [Fact]
    public async Task ListPopBackAsync_ValueIsByteArray_HappyPath()
    {
        var listName = Utils.NewGuidString();
        var value1 = Utils.NewGuidByteArray();
        var value2 = Utils.NewGuidByteArray();

        await client.ListPushBackAsync(cacheName, listName, value1, false);
        await client.ListPushBackAsync(cacheName, listName, value2, false);
        CacheListPopBackResponse response = await client.ListPopBackAsync(cacheName, listName);
        var hitResponse = (CacheListPopBackResponse.Hit)response;

        Assert.Equal(value2, hitResponse.ByteArray);
    }

    [Fact]
    public async Task ListPopBackAsync_ValueIsString_HappyPath()
    {
        var listName = Utils.NewGuidString();
        var value1 = Utils.NewGuidString();
        var value2 = Utils.NewGuidString();

        await client.ListPushBackAsync(cacheName, listName, value1, false);
        await client.ListPushBackAsync(cacheName, listName, value2, false);
        CacheListPopBackResponse response = await client.ListPopBackAsync(cacheName, listName);
        var hitResponse = (CacheListPopBackResponse.Hit)response;

        Assert.Equal(value2, hitResponse.String());
    }

    [Theory]
    [InlineData(null, "my-list")]
    [InlineData("cache", null)]
    public async Task ListFetchAsync_NullChecks_ThrowsException(string cacheName, string listName)
    {
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.ListFetchAsync(cacheName, listName));
    }

    [Fact]
    public async Task ListFetchAsync_Missing_HappyPath()
    {
        var listName = Utils.NewGuidString();
        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        var missResponse = (CacheListFetchResponse.Miss)response;
        Assert.Null(missResponse.ByteArrayList);
        Assert.Null(missResponse.StringList());
    }

    [Fact]
    public async Task ListFetchAsync_HasContentString_HappyPath()
    {
        var listName = Utils.NewGuidString();
        var field1 = Utils.NewGuidString();
        var field2 = Utils.NewGuidString();
        var contentList = new List<string>() { field1, field2 };

        await client.ListPushFrontAsync(cacheName, listName, field2, true, ttlSeconds: 10);
        await client.ListPushFrontAsync(cacheName, listName, field1, true, ttlSeconds: 10);

        CacheListFetchResponse fetchResponse = await client.ListFetchAsync(cacheName, listName);
        var hitResponse = (CacheListFetchResponse.Hit)fetchResponse;

        Assert.Equal(hitResponse.StringList(), contentList);
    }

    [Fact]
    public async Task ListFetchAsync_HasContentByteArray_HappyPath()
    {
        var listName = Utils.NewGuidString();
        var field1 = Utils.NewGuidByteArray();
        var field2 = Utils.NewGuidByteArray();
        var contentList = new List<byte[]> { field1, field2 };

        await client.ListPushFrontAsync(cacheName, listName, field2, true, ttlSeconds: 10);
        await client.ListPushFrontAsync(cacheName, listName, field1, true, ttlSeconds: 10);

        CacheListFetchResponse fetchResponse = await client.ListFetchAsync(cacheName, listName);
        var hitResponse = (CacheListFetchResponse.Hit)fetchResponse;

        Assert.Contains(field1, hitResponse.ByteArrayList!);
        Assert.Contains(field2, hitResponse.ByteArrayList!);
        Assert.Equal(2, hitResponse.ByteArrayList!.Count);
    }

    [Theory]
    [InlineData(null, "my-list", new byte[] { 0x00 })]
    [InlineData("cache", null, new byte[] { 0x00 })]
    [InlineData("cache", "my-list", null)]
    public async Task ListRemoveValueAsync_NullChecksByteArray_ThrowsException(string cacheName, string listName, byte[] value)
    {
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.ListRemoveValueAsync(cacheName, listName, value));
    }

    [Fact]
    public async Task ListRemoveValueAsync_ValueIsByteArray_HappyPath()
    {
        var listName = Utils.NewGuidString();
        var list = new List<byte[]>() { Utils.NewGuidByteArray(), Utils.NewGuidByteArray(), Utils.NewGuidByteArray() };
        var valueOfInterest = Utils.NewGuidByteArray();

        // Add elements to the list
        foreach (var value in list)
        {
            await client.ListPushBackAsync(cacheName, listName, value, true, ttlSeconds: 60);
        }

        await client.ListPushBackAsync(cacheName, listName, valueOfInterest, false);
        await client.ListPushBackAsync(cacheName, listName, valueOfInterest, false);

        // Remove value of interest
        await client.ListRemoveValueAsync(cacheName, listName, valueOfInterest);

        // Test not there
        var cachedList = ((CacheListFetchResponse.Hit)await client.ListFetchAsync(cacheName, listName)).ByteArrayList!;
        Assert.True(list.ListEquals(cachedList));
    }

    [Fact]
    public async Task ListRemoveValueAsync_ValueIsByteArray_ValueNotPresentNoop()
    {
        var listName = Utils.NewGuidString();
        var list = new List<byte[]>() { Utils.NewGuidByteArray(), Utils.NewGuidByteArray(), Utils.NewGuidByteArray() };

        foreach (var value in list)
        {
            await client.ListPushBackAsync(cacheName, listName, value, false);
        }

        await client.ListRemoveValueAsync(cacheName, listName, Utils.NewGuidByteArray());

        var cachedList = ((CacheListFetchResponse.Hit)await client.ListFetchAsync(cacheName, listName)).ByteArrayList!;
        Assert.True(list.ListEquals(cachedList));
    }

    [Fact]
    public async Task ListRemoveValueAsync_ValueIsByteArray_ListNotThereNoop()
    {
        var listName = Utils.NewGuidString();
        Assert.True((CacheListFetchResponse.Miss)(await client.ListFetchAsync(cacheName, listName)) is CacheListFetchResponse.Miss);
        await client.ListRemoveValueAsync(cacheName, listName, Utils.NewGuidByteArray());
        Assert.True((CacheListFetchResponse.Miss)(await client.ListFetchAsync(cacheName, listName)) is CacheListFetchResponse.Miss);
    }

    [Theory]
    [InlineData(null, "my-list", "")]
    [InlineData("cache", null, "")]
    [InlineData("cache", "my-list", null)]
    public async Task ListRemoveValueAsync_NullChecksString_ThrowsException(string cacheName, string listName, string value)
    {
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.ListRemoveValueAsync(cacheName, listName, value));
    }

    [Fact]
    public async Task ListRemoveValueAsync_ValueIsString_HappyPath()
    {
        var listName = Utils.NewGuidString();
        var list = new List<string>() { Utils.NewGuidString(), Utils.NewGuidString(), Utils.NewGuidString() };
        var valueOfInterest = Utils.NewGuidString();

        // Add elements to the list
        foreach (var value in list)
        {
            await client.ListPushBackAsync(cacheName, listName, value, true, ttlSeconds: 60);
        }

        await client.ListPushBackAsync(cacheName, listName, valueOfInterest, false);
        await client.ListPushBackAsync(cacheName, listName, valueOfInterest, false);

        // Remove value of interest
        await client.ListRemoveValueAsync(cacheName, listName, valueOfInterest);

        // Test not there
        var cachedList = ((CacheListFetchResponse.Hit)await client.ListFetchAsync(cacheName, listName)).StringList()!;
        Assert.True(list.SequenceEqual(cachedList));
    }

    [Fact]
    public async Task ListRemoveValueAsync_ValueIsByteString_ValueNotPresentNoop()
    {
        var listName = Utils.NewGuidString();
        var list = new List<string>() { Utils.NewGuidString(), Utils.NewGuidString(), Utils.NewGuidString() };

        foreach (var value in list)
        {
            await client.ListPushBackAsync(cacheName, listName, value, false);
        }

        await client.ListRemoveValueAsync(cacheName, listName, Utils.NewGuidString());

        var cachedList = ((CacheListFetchResponse.Hit)await client.ListFetchAsync(cacheName, listName)).StringList()!;
        Assert.True(list.SequenceEqual(cachedList));
    }

    [Fact]
    public async Task ListRemoveValueAsync_ValueIsString_ListNotThereNoop()
    {
        var listName = Utils.NewGuidString();
        Assert.True((CacheListFetchResponse.Miss)(await client.ListFetchAsync(cacheName, listName)) is CacheListFetchResponse.Miss);
        await client.ListRemoveValueAsync(cacheName, listName, Utils.NewGuidString());
        Assert.True((CacheListFetchResponse.Miss)(await client.ListFetchAsync(cacheName, listName)) is CacheListFetchResponse.Miss);
    }

    [Theory]
    [InlineData(null, "my-list")]
    [InlineData("cache", null)]
    public async Task ListLengthAsync_NullChecks_ThrowsException(string cacheName, string listName)
    {
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.ListLengthAsync(cacheName, listName));
    }

    [Fact]
    public async Task ListLengthAsync_ListIsMissing_HappyPath()
    {
        CacheListLengthResponse lengthResponse = await client.ListLengthAsync(cacheName, Utils.NewGuidString());
        var successResponse = (CacheListLengthResponse.Success)lengthResponse;
        Assert.Equal(0, successResponse.Length);
    }

    [Fact]
    public async Task ListLengthAsync_ListIsFound_HappyPath()
    {
        var listName = Utils.NewGuidString();
        foreach (var i in Enumerable.Range(0, 10))
        {
            await client.ListPushBackAsync(cacheName, listName, Utils.NewGuidByteArray(), false);
        }

        CacheListLengthResponse lengthResponse = await client.ListLengthAsync(cacheName, listName);
        var successResponse = (CacheListLengthResponse.Success)lengthResponse;
        Assert.Equal(10, successResponse.Length);
    }
}
