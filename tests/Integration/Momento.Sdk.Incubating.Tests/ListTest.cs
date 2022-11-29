using Momento.Sdk.Incubating.Requests;
using Momento.Sdk.Incubating.Responses;
using Momento.Sdk.Internal.ExtensionMethods;

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
    public async Task ListPushFrontAsync_NullChecksByteArray_IsError(string cacheName, string listName, byte[] value)
    {
        CacheListPushFrontResponse response = await client.ListPushFrontAsync(cacheName, listName, value);
        Assert.True(response is CacheListPushFrontResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheListPushFrontResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task ListPushFrontFetch_ValueIsByteArray_HappyPath()
    {
        var listName = Utils.NewGuidString();
        var value1 = Utils.NewGuidByteArray();

        CacheListPushFrontResponse pushResponse = await client.ListPushFrontAsync(cacheName, listName, value1);
        Assert.True(pushResponse is CacheListPushFrontResponse.Success, $"Unexpected response: {pushResponse}");
        Assert.Equal(1, ((CacheListPushFrontResponse.Success)pushResponse).ListLength);

        CacheListFetchResponse fetchResponse = await client.ListFetchAsync(cacheName, listName);
        Assert.True(fetchResponse is CacheListFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
        var hitResponse = (CacheListFetchResponse.Hit)fetchResponse;

        var list = hitResponse.ValueListByteArray;
        Assert.Single(list);
        Assert.Contains(value1, list);

        // Test push semantics
        var value2 = Utils.NewGuidByteArray();
        pushResponse = await client.ListPushFrontAsync(cacheName, listName, value2);
        Assert.True(pushResponse is CacheListPushFrontResponse.Success, $"Unexpected response: {pushResponse}");
        var successResponse = (CacheListPushFrontResponse.Success)pushResponse;
        Assert.Equal(2, successResponse.ListLength);

        fetchResponse = await client.ListFetchAsync(cacheName, listName);
        Assert.True(fetchResponse is CacheListFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
        hitResponse = (CacheListFetchResponse.Hit)fetchResponse;
        list = hitResponse.ValueListByteArray!;
        Assert.Equal(value2, list[0]);
        Assert.Equal(value1, list[1]);
    }

    [Fact]
    public async Task ListPushFrontFetch_ValueIsByteArray_NoRefreshTtl()
    {
        var listName = Utils.NewGuidString();
        var value = Utils.NewGuidByteArray();

        await client.ListPushFrontAsync(cacheName, listName, value, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(5)).WithNoRefreshTtlOnUpdates());
        await Task.Delay(100);

        await client.ListPushFrontAsync(cacheName, listName, value, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(10)).WithNoRefreshTtlOnUpdates());
        await Task.Delay(4900);

        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Miss, $"Unexpected response: {response}");
    }

    [Fact]
    public async Task ListPushFrontFetch_ValueIsByteArray_RefreshTtl()
    {
        var listName = Utils.NewGuidString();
        var value = Utils.NewGuidByteArray();

        await client.ListPushFrontAsync(cacheName, listName, value, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(2)).WithNoRefreshTtlOnUpdates());
        await client.ListPushFrontAsync(cacheName, listName, value, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(10)));
        await Task.Delay(2000);

        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Hit, $"Unexpected response: {response}");
        var hitResponse = (CacheListFetchResponse.Hit)response;
        Assert.Equal(2, hitResponse.ValueListByteArray!.Count);
    }

    [Fact]
    public async Task ListPushFrontAsync_ValueIsByteArrayTruncateBackToSizeIsZero_IsError()
    {
        var response = await client.ListPushFrontAsync("myCache", "listName", new byte[] { 0x00 }, truncateBackToSize: 0);
        Assert.True(response is CacheListPushFrontResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheListPushFrontResponse.Error)response).ErrorCode);
    }

    [Theory]
    [InlineData(null, "my-list", "my-value")]
    [InlineData("cache", null, "my-value")]
    [InlineData("cache", "my-list", null)]
    public async Task ListPushFrontAsync_NullChecksString_IsError(string cacheName, string listName, string value)
    {
        CacheListPushFrontResponse response = await client.ListPushFrontAsync(cacheName, listName, value);
        Assert.True(response is CacheListPushFrontResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheListPushFrontResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task ListPushFrontFetch_ValueIsString_HappyPath()
    {
        var listName = Utils.NewGuidString();
        var value1 = Utils.NewGuidString();

        CacheListPushFrontResponse pushResponse = await client.ListPushFrontAsync(cacheName, listName, value1);
        Assert.True(pushResponse is CacheListPushFrontResponse.Success, $"Unexpected response: {pushResponse}");
        var successResponse = (CacheListPushFrontResponse.Success)pushResponse;
        Assert.Equal(1, successResponse.ListLength);

        CacheListFetchResponse fetchResponse = await client.ListFetchAsync(cacheName, listName);
        Assert.True(fetchResponse is CacheListFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
        var hitResponse = (CacheListFetchResponse.Hit)fetchResponse;

        var list = hitResponse.ValueListString;
        Assert.Single(list);
        Assert.Contains(value1, list);

        // Test push semantics
        var value2 = Utils.NewGuidString();
        pushResponse = await client.ListPushFrontAsync(cacheName, listName, value2);
        Assert.True(pushResponse is CacheListPushFrontResponse.Success, $"Unexpected response: {pushResponse}");
        successResponse = (CacheListPushFrontResponse.Success)pushResponse;
        Assert.Equal(2, successResponse.ListLength);

        fetchResponse = await client.ListFetchAsync(cacheName, listName);
        Assert.True(fetchResponse is CacheListFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
        hitResponse = (CacheListFetchResponse.Hit)fetchResponse;
        list = hitResponse.ValueListString!;
        Assert.Equal(value2, list[0]);
        Assert.Equal(value1, list[1]);
    }

    [Fact]
    public async Task ListPushFrontFetch_ValueIsString_NoRefreshTtl()
    {
        var listName = Utils.NewGuidString();
        var value = Utils.NewGuidString();

        await client.ListPushFrontAsync(cacheName, listName, value, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(5)).WithNoRefreshTtlOnUpdates());
        await Task.Delay(100);

        await client.ListPushFrontAsync(cacheName, listName, value, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(10)).WithNoRefreshTtlOnUpdates());
        await Task.Delay(4900);

        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Miss, $"Unexpected response: {response}");
    }

    [Fact]
    public async Task ListPushFrontFetch_ValueIsString_RefreshTtl()
    {
        var listName = Utils.NewGuidString();
        var value = Utils.NewGuidString();

        await client.ListPushFrontAsync(cacheName, listName, value, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(2)).WithNoRefreshTtlOnUpdates());
        await client.ListPushFrontAsync(cacheName, listName, value, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(10)));
        await Task.Delay(2000);

        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Hit, $"Unexpected response: {response}");
        var hitResponse = (CacheListFetchResponse.Hit)response;
        Assert.Equal(2, hitResponse.ValueListString!.Count);
    }

    [Fact]
    public async Task ListPushFrontAsync_ValueIsStringTruncateBackToSizeIsZero_IsError()
    {
        var response = await client.ListPushFrontAsync("myCache", "listName", "value", truncateBackToSize: 0);
        Assert.True(response is CacheListPushFrontResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheListPushFrontResponse.Error)response).ErrorCode);
    }

    [Theory]
    [InlineData(null, "my-list", new byte[] { 0x00 })]
    [InlineData("cache", null, new byte[] { 0x00 })]
    [InlineData("cache", "my-list", null)]
    public async Task ListPushBackAsync_NullChecksByteArray_IsError(string cacheName, string listName, byte[] value)
    {
        CacheListPushBackResponse response = await client.ListPushBackAsync(cacheName, listName, value);
        Assert.True(response is CacheListPushBackResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheListPushBackResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task ListPushBackFetch_ValueIsByteArray_HappyPath()
    {
        var listName = Utils.NewGuidString();
        var value1 = Utils.NewGuidByteArray();

        CacheListPushBackResponse pushResponse = await client.ListPushBackAsync(cacheName, listName, value1);
        Assert.True(pushResponse is CacheListPushBackResponse.Success, $"Unexpected response: {pushResponse}");
        var successResponse = (CacheListPushBackResponse.Success)pushResponse;
        Assert.Equal(1, successResponse.ListLength);

        CacheListFetchResponse fetchResponse = await client.ListFetchAsync(cacheName, listName);
        Assert.True(fetchResponse is CacheListFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
        var hitResponse = (CacheListFetchResponse.Hit)fetchResponse;

        var list = hitResponse.ValueListByteArray;
        Assert.Single(list);
        Assert.Contains(value1, list);

        // Test push semantics
        var value2 = Utils.NewGuidByteArray();
        pushResponse = await client.ListPushBackAsync(cacheName, listName, value2);
        Assert.True(pushResponse is CacheListPushBackResponse.Success, $"Unexpected response: {pushResponse}");
        successResponse = (CacheListPushBackResponse.Success)pushResponse;
        Assert.Equal(2, successResponse.ListLength);

        fetchResponse = await client.ListFetchAsync(cacheName, listName);
        Assert.True(fetchResponse is CacheListFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
        hitResponse = (CacheListFetchResponse.Hit)fetchResponse;
        list = hitResponse.ValueListByteArray!;
        Assert.Equal(value1, list[0]);
        Assert.Equal(value2, list[1]);
    }

    [Fact]
    public async Task ListPushBackFetch_ValueIsByteArray_NoRefreshTtl()
    {
        var listName = Utils.NewGuidString();
        var value = Utils.NewGuidByteArray();

        await client.ListPushBackAsync(cacheName, listName, value, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(5)).WithNoRefreshTtlOnUpdates());
        await Task.Delay(100);

        await client.ListPushBackAsync(cacheName, listName, value, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(10)).WithNoRefreshTtlOnUpdates());
        await Task.Delay(4900);

        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Miss, $"Unexpected response: {response}");
    }

    [Fact]
    public async Task ListPushBackFetch_ValueIsByteArray_RefreshTtl()
    {
        var listName = Utils.NewGuidString();
        var value = Utils.NewGuidByteArray();

        await client.ListPushBackAsync(cacheName, listName, value, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(2)).WithNoRefreshTtlOnUpdates());
        await client.ListPushBackAsync(cacheName, listName, value, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(10)));
        await Task.Delay(2000);

        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Hit, $"Unexpected response: {response}");
        var hitResponse = (CacheListFetchResponse.Hit)response;
        Assert.Equal(2, hitResponse.ValueListByteArray!.Count);
    }

    [Fact]
    public async Task ListPushBackAsync_ValueIsByteArrayTruncateFrontToSizeIsZero_IsError()
    {
        var response = await client.ListPushBackAsync("myCache", "listName", new byte[] { 0x00 }, truncateFrontToSize: 0);
        Assert.True(response is CacheListPushBackResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheListPushBackResponse.Error)response).ErrorCode);
    }

    [Theory]
    [InlineData(null, "my-list", "my-value")]
    [InlineData("cache", null, "my-value")]
    [InlineData("cache", "my-list", null)]
    public async Task ListPushBackAsync_NullChecksString_IsError(string cacheName, string listName, string value)
    {
        CacheListPushBackResponse response = await client.ListPushBackAsync(cacheName, listName, value);
        Assert.True(response is CacheListPushBackResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheListPushBackResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task ListPushBackTruncate_TruncatesList_String()
    {
        var listName = Utils.NewGuidString();
        var value1 = Utils.NewGuidString();
        var value2 = Utils.NewGuidString();
        var value3 = Utils.NewGuidString();
        await client.ListPushBackAsync(cacheName, listName, value1);
        await client.ListPushBackAsync(cacheName, listName, value2);
        await client.ListPushBackAsync(cacheName, listName, value3, 2);
        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Hit, $"Unexpected response: {response}");
        var hitResponse = (CacheListFetchResponse.Hit)response;
        Assert.Equal(2, hitResponse.ValueListString!.Count);
        Assert.Equal(value2, hitResponse.ValueListString![0]);
        Assert.Equal(value3, hitResponse.ValueListString![1]);
    }

    [Fact]
    public async Task ListPushBackTruncate_TruncatesList_Bytes()
    {
        var listName = Utils.NewGuidString();
        var value1 = Utils.NewGuidByteArray();
        var value2 = Utils.NewGuidByteArray();
        var value3 = Utils.NewGuidByteArray();
        await client.ListPushBackAsync(cacheName, listName, value1);
        await client.ListPushBackAsync(cacheName, listName, value2);
        await client.ListPushBackAsync(cacheName, listName, value3, 2);
        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Hit, $"Unexpected response: {response}");
        var hitResponse = (CacheListFetchResponse.Hit)response;
        Assert.Equal(2, hitResponse.ValueListByteArray!.Count);
        Assert.Equal(value2, hitResponse.ValueListByteArray![0]);
        Assert.Equal(value3, hitResponse.ValueListByteArray![1]);
    }

    [Fact]
    public async Task ListPushFrontTruncate_TruncatesList_String()
    {
        var listName = Utils.NewGuidString();
        var value1 = Utils.NewGuidString();
        var value2 = Utils.NewGuidString();
        var value3 = Utils.NewGuidString();
        await client.ListPushFrontAsync(cacheName, listName, value1);
        await client.ListPushFrontAsync(cacheName, listName, value2);
        await client.ListPushFrontAsync(cacheName, listName, value3, 2);
        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Hit, $"Unexpected response: {response}");
        var hitResponse = (CacheListFetchResponse.Hit)response;
        Assert.Equal(2, hitResponse.ValueListString!.Count);
        Assert.Equal(value2, hitResponse.ValueListString![1]);
        Assert.Equal(value3, hitResponse.ValueListString![0]);
    }

    [Fact]
    public async Task ListPushFrontTruncate_TruncatesList_Bytes()
    {
        var listName = Utils.NewGuidString();
        var value1 = Utils.NewGuidByteArray();
        var value2 = Utils.NewGuidByteArray();
        var value3 = Utils.NewGuidByteArray();
        await client.ListPushFrontAsync(cacheName, listName, value1);
        await client.ListPushFrontAsync(cacheName, listName, value2);
        await client.ListPushFrontAsync(cacheName, listName, value3, 2);
        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Hit, $"Unexpected response: {response}");
        var hitResponse = (CacheListFetchResponse.Hit)response;
        Assert.Equal(2, hitResponse.ValueListByteArray!.Count);
        Assert.Equal(value2, hitResponse.ValueListByteArray![1]);
        Assert.Equal(value3, hitResponse.ValueListByteArray![0]);
    }

    [Fact]
    public async Task ListPushBackFetch_ValueIsString_HappyPath()
    {
        var listName = Utils.NewGuidString();
        var value1 = Utils.NewGuidString();

        CacheListPushBackResponse pushResponse = await client.ListPushBackAsync(cacheName, listName, value1);
        Assert.True(pushResponse is CacheListPushBackResponse.Success, $"Unexpected response: {pushResponse}");
        var successResponse = (CacheListPushBackResponse.Success)pushResponse;
        Assert.Equal(1, successResponse.ListLength);

        CacheListFetchResponse fetchResponse = await client.ListFetchAsync(cacheName, listName);
        Assert.True(fetchResponse is CacheListFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
        var hitResponse = (CacheListFetchResponse.Hit)fetchResponse;

        var list = hitResponse.ValueListString;
        Assert.Single(list);
        Assert.Contains(value1, list);

        // Test push semantics
        var value2 = Utils.NewGuidString();
        pushResponse = await client.ListPushBackAsync(cacheName, listName, value2);
        successResponse = (CacheListPushBackResponse.Success)pushResponse;
        successResponse = (CacheListPushBackResponse.Success)pushResponse;
        Assert.Equal(2, successResponse.ListLength);

        fetchResponse = await client.ListFetchAsync(cacheName, listName);
        Assert.True(fetchResponse is CacheListFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
        hitResponse = (CacheListFetchResponse.Hit)fetchResponse;
        list = hitResponse.ValueListString!;
        Assert.Equal(value1, list[0]);
        Assert.Equal(value2, list[1]);
    }

    [Fact]
    public async Task ListPushBackFetch_ValueIsString_NoRefreshTtl()
    {
        var listName = Utils.NewGuidString();
        var value = Utils.NewGuidString();

        await client.ListPushBackAsync(cacheName, listName, value, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(2)).WithNoRefreshTtlOnUpdates());
        await Task.Delay(100);

        await client.ListPushBackAsync(cacheName, listName, value, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(10)).WithNoRefreshTtlOnUpdates());
        await Task.Delay(4900);

        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Miss, $"Unexpected response: {response}");
    }

    [Fact]
    public async Task ListPushBackFetch_ValueIsString_RefreshTtl()
    {
        var listName = Utils.NewGuidString();
        var value = Utils.NewGuidString();

        await client.ListPushBackAsync(cacheName, listName, value, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(2)));
        await client.ListPushBackAsync(cacheName, listName, value, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(10)));
        await Task.Delay(2000);

        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Hit, $"Unexpected response: {response}");
        var hitResponse = (CacheListFetchResponse.Hit)response;
        Assert.Equal(2, hitResponse.ValueListString!.Count);
    }

    [Fact]
    public async Task ListPushBackAsync_ValueIsStringTruncateFrontToSizeIsZero_IsError()
    {
        var response = await client.ListPushBackAsync("myCache", "listName", "value", truncateFrontToSize: 0);
        Assert.True(response is CacheListPushBackResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheListPushBackResponse.Error)response).ErrorCode);
    }

    [Theory]
    [InlineData(null, "my-list")]
    [InlineData("cache", null)]
    public async Task ListPopFrontAsync_NullChecks_IsError(string cacheName, string listName)
    {
        CacheListPopFrontResponse response = await client.ListPopFrontAsync(cacheName, listName);
        Assert.True(response is CacheListPopFrontResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheListPopFrontResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task ListPopFrontAsync_ListIsMissing_HappyPath()
    {
        var listName = Utils.NewGuidString();
        CacheListPopFrontResponse response = await client.ListPopFrontAsync(cacheName, listName);
        Assert.True(response is CacheListPopFrontResponse.Miss, $"Unexpected response: {response}");
    }

    [Fact]
    public async Task ListPopFrontAsync_ValueIsByteArray_HappyPath()
    {
        var listName = Utils.NewGuidString();
        var value1 = Utils.NewGuidByteArray();
        var value2 = Utils.NewGuidByteArray();

        await client.ListPushFrontAsync(cacheName, listName, value1);
        await client.ListPushFrontAsync(cacheName, listName, value2);
        CacheListPopFrontResponse response = await client.ListPopFrontAsync(cacheName, listName);
        Assert.True(response is CacheListPopFrontResponse.Hit, $"Unexpected response: {response}");
        var hitResponse = (CacheListPopFrontResponse.Hit)response;

        Assert.Equal(value2, hitResponse.ValueByteArray);
    }

    [Fact]
    public async Task ListPopFrontAsync_ValueIsString_HappyPath()
    {
        var listName = Utils.NewGuidString();
        var value1 = Utils.NewGuidString();
        var value2 = Utils.NewGuidString();

        await client.ListPushFrontAsync(cacheName, listName, value1);
        await client.ListPushFrontAsync(cacheName, listName, value2);
        CacheListPopFrontResponse response = await client.ListPopFrontAsync(cacheName, listName);
        Assert.True(response is CacheListPopFrontResponse.Hit, $"Unexpected response: {response}");
        var hitResponse = (CacheListPopFrontResponse.Hit)response;

        Assert.Equal(value2, hitResponse.ValueString);
    }

    [Theory]
    [InlineData(null, "my-list")]
    [InlineData("cache", null)]
    public async Task ListPopBackAsync_NullChecks_IsError(string cacheName, string listName)
    {
        CacheListPopBackResponse response = await client.ListPopBackAsync(cacheName, listName);
        Assert.True(response is CacheListPopBackResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheListPopBackResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task ListPopBackAsync_ListIsMissing_HappyPath()
    {
        var listName = Utils.NewGuidString();
        CacheListPopBackResponse response = await client.ListPopBackAsync(cacheName, listName);
        Assert.True(response is CacheListPopBackResponse.Miss, $"Unexpected response: {response}");
    }

    [Fact]
    public async Task ListPopBackAsync_ValueIsByteArray_HappyPath()
    {
        var listName = Utils.NewGuidString();
        var value1 = Utils.NewGuidByteArray();
        var value2 = Utils.NewGuidByteArray();

        await client.ListPushBackAsync(cacheName, listName, value1);
        await client.ListPushBackAsync(cacheName, listName, value2);
        CacheListPopBackResponse response = await client.ListPopBackAsync(cacheName, listName);
        Assert.True(response is CacheListPopBackResponse.Hit, $"Unexpected response: {response}");
        var hitResponse = (CacheListPopBackResponse.Hit)response;

        Assert.Equal(value2, hitResponse.ValueByteArray);
    }

    [Fact]
    public async Task ListPopBackAsync_ValueIsString_HappyPath()
    {
        var listName = Utils.NewGuidString();
        var value1 = Utils.NewGuidString();
        var value2 = Utils.NewGuidString();

        await client.ListPushBackAsync(cacheName, listName, value1);
        await client.ListPushBackAsync(cacheName, listName, value2);
        CacheListPopBackResponse response = await client.ListPopBackAsync(cacheName, listName);
        Assert.True(response is CacheListPopBackResponse.Hit, $"Unexpected response: {response}");
        var hitResponse = (CacheListPopBackResponse.Hit)response;

        Assert.Equal(value2, hitResponse.ValueString);
    }

    [Theory]
    [InlineData(null, "my-list")]
    [InlineData("cache", null)]
    public async Task ListFetchAsync_NullChecks_IsError(string cacheName, string listName)
    {
        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheListFetchResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task ListFetchAsync_Missing_HappyPath()
    {
        var listName = Utils.NewGuidString();
        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Miss, $"Unexpected response: {response}");
    }

    [Fact]
    public async Task ListFetchAsync_HasContentString_HappyPath()
    {
        var listName = Utils.NewGuidString();
        var field1 = Utils.NewGuidString();
        var field2 = Utils.NewGuidString();
        var contentList = new List<string>() { field1, field2 };

        await client.ListPushFrontAsync(cacheName, listName, field2);
        await client.ListPushFrontAsync(cacheName, listName, field1);

        CacheListFetchResponse fetchResponse = await client.ListFetchAsync(cacheName, listName);
        Assert.True(fetchResponse is CacheListFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
        var hitResponse = (CacheListFetchResponse.Hit)fetchResponse;

        Assert.Equal(hitResponse.ValueListString, contentList);
    }

    [Fact]
    public async Task ListFetchAsync_HasContentByteArray_HappyPath()
    {
        var listName = Utils.NewGuidString();
        var field1 = Utils.NewGuidByteArray();
        var field2 = Utils.NewGuidByteArray();
        var contentList = new List<byte[]> { field1, field2 };

        await client.ListPushFrontAsync(cacheName, listName, field2);
        await client.ListPushFrontAsync(cacheName, listName, field1);

        CacheListFetchResponse fetchResponse = await client.ListFetchAsync(cacheName, listName);
        Assert.True(fetchResponse is CacheListFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
        var hitResponse = (CacheListFetchResponse.Hit)fetchResponse;

        Assert.Contains(field1, hitResponse.ValueListByteArray!);
        Assert.Contains(field2, hitResponse.ValueListByteArray!);
        Assert.Equal(2, hitResponse.ValueListByteArray!.Count);
    }

    [Theory]
    [InlineData(null, "my-list", new byte[] { 0x00 })]
    [InlineData("cache", null, new byte[] { 0x00 })]
    [InlineData("cache", "my-list", null)]
    public async Task ListRemoveValueAsync_NullChecksByteArray_IsError(string cacheName, string listName, byte[] value)
    {
        CacheListRemoveValueResponse response = await client.ListRemoveValueAsync(cacheName, listName, value);
        Assert.True(response is CacheListRemoveValueResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheListRemoveValueResponse.Error)response).ErrorCode);
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
            await client.ListPushBackAsync(cacheName, listName, value);
        }

        await client.ListPushBackAsync(cacheName, listName, valueOfInterest);
        await client.ListPushBackAsync(cacheName, listName, valueOfInterest);

        // Remove value of interest
        await client.ListRemoveValueAsync(cacheName, listName, valueOfInterest);

        // Test not there
        var response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Hit, $"Unexpected response: {response}");
        var cachedList = ((CacheListFetchResponse.Hit)response).ValueListByteArray!;
        Assert.True(list.ListEquals(cachedList));
    }

    [Fact]
    public async Task ListRemoveValueAsync_ValueIsByteArray_ValueNotPresentNoop()
    {
        var listName = Utils.NewGuidString();
        var list = new List<byte[]>() { Utils.NewGuidByteArray(), Utils.NewGuidByteArray(), Utils.NewGuidByteArray() };

        foreach (var value in list)
        {
            await client.ListPushBackAsync(cacheName, listName, value);
        }

        await client.ListRemoveValueAsync(cacheName, listName, Utils.NewGuidByteArray());

        var response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Hit, $"Unexpected response: {response}");
        var cachedList = ((CacheListFetchResponse.Hit)response).ValueListByteArray!;
        Assert.True(list.ListEquals(cachedList));
    }

    [Fact]
    public async Task ListRemoveValueAsync_ValueIsByteArray_ListNotThereNoop()
    {
        var listName = Utils.NewGuidString();
        Assert.True(await client.ListFetchAsync(cacheName, listName) is CacheListFetchResponse.Miss);
        await client.ListRemoveValueAsync(cacheName, listName, Utils.NewGuidByteArray());
        Assert.True(await client.ListFetchAsync(cacheName, listName) is CacheListFetchResponse.Miss);
    }

    [Theory]
    [InlineData(null, "my-list", "")]
    [InlineData("cache", null, "")]
    [InlineData("cache", "my-list", null)]
    public async Task ListRemoveValueAsync_NullChecksString_IsError(string cacheName, string listName, string value)
    {
        CacheListRemoveValueResponse response = await client.ListRemoveValueAsync(cacheName, listName, value);
        Assert.True(response is CacheListRemoveValueResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheListRemoveValueResponse.Error)response).ErrorCode);
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
            await client.ListPushBackAsync(cacheName, listName, value);
        }

        await client.ListPushBackAsync(cacheName, listName, valueOfInterest);
        await client.ListPushBackAsync(cacheName, listName, valueOfInterest);

        // Remove value of interest
        await client.ListRemoveValueAsync(cacheName, listName, valueOfInterest);

        // Test not there
        var response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Hit, $"Unexpected response: {response}");
        var cachedList = ((CacheListFetchResponse.Hit)response).ValueListString!;
        Assert.True(list.SequenceEqual(cachedList));
    }

    [Fact]
    public async Task ListRemoveValueAsync_ValueIsByteString_ValueNotPresentNoop()
    {
        var listName = Utils.NewGuidString();
        var list = new List<string>() { Utils.NewGuidString(), Utils.NewGuidString(), Utils.NewGuidString() };

        foreach (var value in list)
        {
            await client.ListPushBackAsync(cacheName, listName, value);
        }

        await client.ListRemoveValueAsync(cacheName, listName, Utils.NewGuidString());

        var response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Hit, $"Unexpected response: {response}");
        var cachedList = ((CacheListFetchResponse.Hit)response).ValueListString!;
        Assert.True(list.SequenceEqual(cachedList));
    }

    [Fact]
    public async Task ListRemoveValueAsync_ValueIsString_ListNotThereNoop()
    {
        var listName = Utils.NewGuidString();
        Assert.True(await client.ListFetchAsync(cacheName, listName) is CacheListFetchResponse.Miss);
        await client.ListRemoveValueAsync(cacheName, listName, Utils.NewGuidString());
        Assert.True(await client.ListFetchAsync(cacheName, listName) is CacheListFetchResponse.Miss);
    }

    [Theory]
    [InlineData(null, "my-list")]
    [InlineData("cache", null)]
    public async Task ListLengthAsync_NullChecks_IsError(string cacheName, string listName)
    {
        CacheListLengthResponse response = await client.ListLengthAsync(cacheName, listName);
        Assert.True(response is CacheListLengthResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheListLengthResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task ListLengthAsync_ListIsMissing_HappyPath()
    {
        CacheListLengthResponse lengthResponse = await client.ListLengthAsync(cacheName, Utils.NewGuidString());
        Assert.True(lengthResponse is CacheListLengthResponse.Success, $"Unexpected response: {lengthResponse}");
        var successResponse = (CacheListLengthResponse.Success)lengthResponse;
        Assert.Equal(0, successResponse.Length);
    }

    [Fact]
    public async Task ListLengthAsync_ListIsFound_HappyPath()
    {
        var listName = Utils.NewGuidString();
        foreach (var i in Enumerable.Range(0, 10))
        {
            await client.ListPushBackAsync(cacheName, listName, Utils.NewGuidByteArray());
        }

        CacheListLengthResponse lengthResponse = await client.ListLengthAsync(cacheName, listName);
        Assert.True(lengthResponse is CacheListLengthResponse.Success, $"Unexpected response: {lengthResponse}");
        var successResponse = (CacheListLengthResponse.Success)lengthResponse;
        Assert.Equal(10, successResponse.Length);
    }

    [Theory]
    [InlineData(null, "my-list")]
    [InlineData("my-cache", null)]
    public async Task ListDeleteAsync_NullChecks_IsError(string cacheName, string listName)
    {
        var response = await client.ListDeleteAsync(cacheName, listName);
        Assert.True(response is CacheListDeleteResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheListDeleteResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task ListDeleteAsync_ListDoesNotExist_Noop()
    {
        var listName = Utils.NewGuidString();
        Assert.True((await client.ListFetchAsync(cacheName, listName)) is CacheListFetchResponse.Miss);
        var deleteResponse = await client.ListDeleteAsync(cacheName, listName);
        Assert.True(deleteResponse is CacheListDeleteResponse.Success, $"Unexpected response: {deleteResponse}");
        Assert.True((await client.ListFetchAsync(cacheName, listName)) is CacheListFetchResponse.Miss);
    }

    [Fact]
    public async Task ListDeleteAsync_ListExists_HappyPath()
    {
        var listName = Utils.NewGuidString();
        var pushResponse = await client.ListPushFrontAsync(cacheName, listName, Utils.NewGuidString());
        Assert.True(pushResponse is CacheListPushFrontResponse.Success, $"Unexpected response: {pushResponse}");
        pushResponse = await client.ListPushFrontAsync(cacheName, listName, Utils.NewGuidString());
        Assert.True(pushResponse is CacheListPushFrontResponse.Success, $"Unexpected response: {pushResponse}");
        pushResponse = await client.ListPushFrontAsync(cacheName, listName, Utils.NewGuidString());
        Assert.True(pushResponse is CacheListPushFrontResponse.Success, $"Unexpected response: {pushResponse}");

        Assert.True((await client.ListFetchAsync(cacheName, listName)) is CacheListFetchResponse.Hit);
        var deleteResponse = await client.ListDeleteAsync(cacheName, listName);
        Assert.True(deleteResponse is CacheListDeleteResponse.Success, $"Unexpected response: {deleteResponse}");

        var fetchResponse = await client.ListFetchAsync(cacheName, listName);
        Assert.True(fetchResponse is CacheListFetchResponse.Miss, $"Unexpected response: {fetchResponse}");
    }
}
