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
    [InlineData(null, "my-list", new string[] {"value"})]
    [InlineData("cache", null,  new string[] {"value"})]
    [InlineData("cache", "my-list", null)]
    public async Task ListConcatenateFrontAsync_NullChecksStringArray_IsError(string cacheName, string listName, IEnumerable<string> values)
    {
        CacheListConcatenateFrontResponse response = await client.ListConcatenateFrontAsync(cacheName, listName, values, false);
        Assert.True(response is CacheListConcatenateFrontResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheListConcatenateFrontResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task ListConcatenateFrontFetch_ValueIsByteArray_HappyPath()
    {
        var listName = Utils.NewGuidString();
        byte[][] values1 = new byte[][] { Utils.NewGuidByteArray(), Utils.NewGuidByteArray() };

        CacheListConcatenateFrontResponse concatenateResponse = await client.ListConcatenateFrontAsync(cacheName, listName, values1, false);
        Assert.True(concatenateResponse is CacheListConcatenateFrontResponse.Success, $"Unexpected response: {concatenateResponse}");
        Assert.Equal(1, ((CacheListConcatenateFrontResponse.Success)concatenateResponse).ListLength);

        CacheListFetchResponse fetchResponse = await client.ListFetchAsync(cacheName, listName);
        Assert.True(fetchResponse is CacheListFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
        var hitResponse = (CacheListFetchResponse.Hit)fetchResponse;

        var list = hitResponse.ByteArrayList;
        Assert.NotEmpty(list);
        foreach (byte[] value in values1) {
            Assert.Contains(value, list);
        }

        // Test adding at the front semantics
        byte[][] values2 = new byte[][] { Utils.NewGuidByteArray(), Utils.NewGuidByteArray() };
        concatenateResponse = await client.ListConcatenateFrontAsync(cacheName, listName, values2, false);
        Assert.True(concatenateResponse is CacheListConcatenateFrontResponse.Success, $"Unexpected response: {concatenateResponse}");
        var successResponse = (CacheListConcatenateFrontResponse.Success)concatenateResponse;
        Assert.Equal(4, successResponse.ListLength);

        fetchResponse = await client.ListFetchAsync(cacheName, listName);
        Assert.True(fetchResponse is CacheListFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
        hitResponse = (CacheListFetchResponse.Hit)fetchResponse;
        list = hitResponse.ByteArrayList!;
        for (int i = 0; i < values2.Length; i++) {
            Assert.Equal(values2[i], list[i]);
        }
        foreach (byte[] value in values1) {
            Assert.Contains(value, list);
        }
    }

    [Fact]
    public async Task ListConcatenateFrontFetch_ValueIsByteArray_NoRefreshTtl()
    {
        var listName = Utils.NewGuidString();
        byte[][] values = new byte[][] { Utils.NewGuidByteArray() };

        await client.ListConcatenateFrontAsync(cacheName, listName, values, false, ttl: TimeSpan.FromSeconds(5));
        await Task.Delay(100);

        await client.ListConcatenateFrontAsync(cacheName, listName, values, false, ttl: TimeSpan.FromSeconds(5));
        await Task.Delay(4900);

        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Miss, $"Unexpected response: {response}");
    }

    [Fact]
    public async Task ListConcatenateFrontFetch_ValueIsByteArray_RefreshTtl()
    {
        var listName = Utils.NewGuidString();
        byte[][] values = new byte[][] { Utils.NewGuidByteArray() };

        await client.ListConcatenateFrontAsync(cacheName, listName, values, false, ttl: TimeSpan.FromSeconds(2));
        await client.ListConcatenateFrontAsync(cacheName, listName, values, true, ttl: TimeSpan.FromSeconds(10));
        await Task.Delay(2000);

        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Hit, $"Unexpected response: {response}");
        var hitResponse = (CacheListFetchResponse.Hit)response;
        Assert.Equal(2, hitResponse.ByteArrayList!.Count);
    }

    [Fact]
    public async Task ListConcatenateFrontAsync_ValueIsByteArrayTruncateBackToSizeIsZero_IsError()
    {
        byte[][] values = new byte[][] { };
        var response = await client.ListConcatenateFrontAsync("myCache", "listName", values, false, truncateBackToSize: 0);
        Assert.True(response is CacheListConcatenateFrontResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheListConcatenateFrontResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task ListConcatenateFrontFetch_ValueIsStringArray_HappyPath()
    {
        var listName = Utils.NewGuidString();
        string[] values1 = new string[] { Utils.NewGuidString(), Utils.NewGuidString() };

        CacheListConcatenateFrontResponse concatenateResponse = await client.ListConcatenateFrontAsync(cacheName, listName, values1, false);
        Assert.True(concatenateResponse is CacheListConcatenateFrontResponse.Success, $"Unexpected response: {concatenateResponse}");
        Assert.Equal(1, ((CacheListConcatenateFrontResponse.Success)concatenateResponse).ListLength);

        CacheListFetchResponse fetchResponse = await client.ListFetchAsync(cacheName, listName);
        Assert.True(fetchResponse is CacheListFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
        var hitResponse = (CacheListFetchResponse.Hit)fetchResponse;

        var list = hitResponse.StringList()!;
        Assert.NotEmpty(list);
        foreach (string value in values1) {
            Assert.Contains(value, list);
        }

        // Test adding at the front semantics
        string[] values2 = new string[] { Utils.NewGuidString(), Utils.NewGuidString() };
        concatenateResponse = await client.ListConcatenateFrontAsync(cacheName, listName, values2, false);
        Assert.True(concatenateResponse is CacheListConcatenateFrontResponse.Success, $"Unexpected response: {concatenateResponse}");
        var successResponse = (CacheListConcatenateFrontResponse.Success)concatenateResponse;
        Assert.Equal(4, successResponse.ListLength);

        fetchResponse = await client.ListFetchAsync(cacheName, listName);
        Assert.True(fetchResponse is CacheListFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
        hitResponse = (CacheListFetchResponse.Hit)fetchResponse;
        list = hitResponse.StringList()!;
        var values3 = new List<String>(values2);
        values3.AddRange(values1);
        Assert.Equal(values3, list);
    }

    [Fact]
    public async Task ListConcatenateFrontFetch_ValueIsStringArray_NoRefreshTtl()
    {
        var listName = Utils.NewGuidString();
        string[] values = new string[] { Utils.NewGuidString() };

        await client.ListConcatenateFrontAsync(cacheName, listName, values, false, ttl: TimeSpan.FromSeconds(5));
        await Task.Delay(100);

        await client.ListConcatenateFrontAsync(cacheName, listName, values, false, ttl: TimeSpan.FromSeconds(5));
        await Task.Delay(4900);

        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Miss, $"Unexpected response: {response}");
    }

    [Fact]
    public async Task ListConcatenateFrontFetch_ValueIsStringArray_RefreshTtl()
    {
        var listName = Utils.NewGuidString();
        string[] values = new string[] { Utils.NewGuidString() };

        await client.ListConcatenateFrontAsync(cacheName, listName, values, false, ttl: TimeSpan.FromSeconds(2));
        await client.ListConcatenateFrontAsync(cacheName, listName, values, true, ttl: TimeSpan.FromSeconds(10));
        await Task.Delay(2000);

        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Hit, $"Unexpected response: {response}");
        var hitResponse = (CacheListFetchResponse.Hit)response;
        Assert.Equal(2, hitResponse.ByteArrayList!.Count);
    }

    [Fact]
    public async Task ListConcatenateFrontAsync_ValueIsStringArrayTruncateBackToSizeIsZero_IsError()
    {
        string[] values = new string[] { };
        var response = await client.ListConcatenateFrontAsync("myCache", "listName", values, false, truncateBackToSize: 0);
        Assert.True(response is CacheListConcatenateFrontResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheListConcatenateFrontResponse.Error)response).ErrorCode);
    }

[Theory]
    [InlineData(null, "my-list", new string[] {"value"})]
    [InlineData("cache", null,  new string[] {"value"})]
    [InlineData("cache", "my-list", null)]
    public async Task ListConcatenateBackAsync_NullChecksStringArray_IsError(string cacheName, string listName, IEnumerable<string> values)
    {
        CacheListConcatenateBackResponse response = await client.ListConcatenateBackAsync(cacheName, listName, values, false);
        Assert.True(response is CacheListConcatenateBackResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheListConcatenateBackResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task ListConcatenateBackFetch_ValueIsByteArray_HappyPath()
    {
        var listName = Utils.NewGuidString();
        byte[][] values1 = new byte[][] { Utils.NewGuidByteArray(), Utils.NewGuidByteArray() };

        CacheListConcatenateBackResponse concatenateResponse = await client.ListConcatenateBackAsync(cacheName, listName, values1, false);
        Assert.True(concatenateResponse is CacheListConcatenateBackResponse.Success, $"Unexpected response: {concatenateResponse}");
        Assert.Equal(1, ((CacheListConcatenateBackResponse.Success)concatenateResponse).ListLength);

        CacheListFetchResponse fetchResponse = await client.ListFetchAsync(cacheName, listName);
        Assert.True(fetchResponse is CacheListFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
        var hitResponse = (CacheListFetchResponse.Hit)fetchResponse;

        var list = hitResponse.ByteArrayList;
        Assert.NotEmpty(list);
        foreach (byte[] value in values1) {
            Assert.Contains(value, list);
        }

        // Test adding at the front semantics
        byte[][] values2 = new byte[][] { Utils.NewGuidByteArray(), Utils.NewGuidByteArray() };
        concatenateResponse = await client.ListConcatenateBackAsync(cacheName, listName, values2, false);
        Assert.True(concatenateResponse is CacheListConcatenateBackResponse.Success, $"Unexpected response: {concatenateResponse}");
        var successResponse = (CacheListConcatenateBackResponse.Success)concatenateResponse;
        Assert.Equal(4, successResponse.ListLength);

        fetchResponse = await client.ListFetchAsync(cacheName, listName);
        Assert.True(fetchResponse is CacheListFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
        hitResponse = (CacheListFetchResponse.Hit)fetchResponse;
        list = hitResponse.ByteArrayList!;
        for (int i = 0; i < values2.Length; i++) {
            Assert.Equal(values1[i], list[i]);
        }
        foreach (byte[] value in values2) {
            Assert.Contains(value, list);
        }
    }

    [Fact]
    public async Task ListConcatenateBackFetch_ValueIsByteArray_NoRefreshTtl()
    {
        var listName = Utils.NewGuidString();
        byte[][] values = new byte[][] { Utils.NewGuidByteArray() };

        await client.ListConcatenateBackAsync(cacheName, listName, values, false, ttl: TimeSpan.FromSeconds(5));
        await Task.Delay(100);

        await client.ListConcatenateBackAsync(cacheName, listName, values, false, ttl: TimeSpan.FromSeconds(5));
        await Task.Delay(4900);

        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Miss, $"Unexpected response: {response}");
    }

    [Fact]
    public async Task ListConcatenateBackFetch_ValueIsByteArray_RefreshTtl()
    {
        var listName = Utils.NewGuidString();
        byte[][] values = new byte[][] { Utils.NewGuidByteArray() };

        await client.ListConcatenateBackAsync(cacheName, listName, values, false, ttl: TimeSpan.FromSeconds(2));
        await client.ListConcatenateBackAsync(cacheName, listName, values, true, ttl: TimeSpan.FromSeconds(10));
        await Task.Delay(2000);

        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Hit, $"Unexpected response: {response}");
        var hitResponse = (CacheListFetchResponse.Hit)response;
        Assert.Equal(2, hitResponse.ByteArrayList!.Count);
    }

    [Fact]
    public async Task ListConcatenateBackAsync_ValueIsByteArrayTruncateFrontoSizeIsZero_IsError()
    {
        byte[][] values = new byte[][] { };
        var response = await client.ListConcatenateBackAsync("myCache", "listName", values, false, truncateFrontToSize: 0);
        Assert.True(response is CacheListConcatenateBackResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheListConcatenateBackResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task ListConcatenateBackFetch_ValueIsStringArray_HappyPath()
    {
        var listName = Utils.NewGuidString();
        string[] values1 = new string[] { Utils.NewGuidString(), Utils.NewGuidString() };

        CacheListConcatenateBackResponse concatenateResponse = await client.ListConcatenateBackAsync(cacheName, listName, values1, false);
        Assert.True(concatenateResponse is CacheListConcatenateBackResponse.Success, $"Unexpected response: {concatenateResponse}");
        Assert.Equal(1, ((CacheListConcatenateBackResponse.Success)concatenateResponse).ListLength);

        CacheListFetchResponse fetchResponse = await client.ListFetchAsync(cacheName, listName);
        Assert.True(fetchResponse is CacheListFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
        var hitResponse = (CacheListFetchResponse.Hit)fetchResponse;

        var list = hitResponse.StringList()!;
        Assert.NotEmpty(list);
        foreach (string value in values1) {
            Assert.Contains(value, list);
        }

        // Test adding at the front semantics
        string[] values2 = new string[] { Utils.NewGuidString(), Utils.NewGuidString() };
        concatenateResponse = await client.ListConcatenateBackAsync(cacheName, listName, values2, false);
        Assert.True(concatenateResponse is CacheListConcatenateBackResponse.Success, $"Unexpected response: {concatenateResponse}");
        var successResponse = (CacheListConcatenateBackResponse.Success)concatenateResponse;
        Assert.Equal(4, successResponse.ListLength);

        fetchResponse = await client.ListFetchAsync(cacheName, listName);
        Assert.True(fetchResponse is CacheListFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
        hitResponse = (CacheListFetchResponse.Hit)fetchResponse;
        list = hitResponse.StringList()!;
        var values3 = new List<String>(values1);
        values3.AddRange(values2);
        Assert.Equal(values3, list);
    }

    [Fact]
    public async Task ListConcatenateBackFetch_ValueIsStringArray_NoRefreshTtl()
    {
        var listName = Utils.NewGuidString();
        string[] values = new string[] { Utils.NewGuidString() };

        await client.ListConcatenateBackAsync(cacheName, listName, values, false, ttl: TimeSpan.FromSeconds(5));
        await Task.Delay(100);

        await client.ListConcatenateBackAsync(cacheName, listName, values, false, ttl: TimeSpan.FromSeconds(5));
        await Task.Delay(4900);

        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Miss, $"Unexpected response: {response}");
    }

    [Fact]
    public async Task ListConcatenateBackFetch_ValueIsStringArray_RefreshTtl()
    {
        var listName = Utils.NewGuidString();
        string[] values = new string[] { Utils.NewGuidString() };

        await client.ListConcatenateBackAsync(cacheName, listName, values, false, ttl: TimeSpan.FromSeconds(2));
        await client.ListConcatenateBackAsync(cacheName, listName, values, true, ttl: TimeSpan.FromSeconds(10));
        await Task.Delay(2000);

        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Hit, $"Unexpected response: {response}");
        var hitResponse = (CacheListFetchResponse.Hit)response;
        Assert.Equal(2, hitResponse.ByteArrayList!.Count);
    }

    [Fact]
    public async Task ListConcatenateBackAsync_ValueIsStringArrayTruncateBackToSizeIsZero_IsError()
    {
        string[] values = new string[] { };
        var response = await client.ListConcatenateBackAsync("myCache", "listName", values, false, truncateFrontToSize: 0);
        Assert.True(response is CacheListConcatenateBackResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheListConcatenateBackResponse.Error)response).ErrorCode);
    }

    [Theory]
    [InlineData(null, "my-list", new byte[] { 0x00 })]
    [InlineData("cache", null, new byte[] { 0x00 })]
    [InlineData("cache", "my-list", null)]
    public async Task ListPushFrontAsync_NullChecksByteArray_IsError(string cacheName, string listName, byte[] value)
    {
        CacheListPushFrontResponse response = await client.ListPushFrontAsync(cacheName, listName, value, false);
        Assert.True(response is CacheListPushFrontResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheListPushFrontResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task ListPushFrontFetch_ValueIsByteArray_HappyPath()
    {
        var listName = Utils.NewGuidString();
        var value1 = Utils.NewGuidByteArray();

        CacheListPushFrontResponse pushResponse = await client.ListPushFrontAsync(cacheName, listName, value1, false);
        Assert.True(pushResponse is CacheListPushFrontResponse.Success, $"Unexpected response: {pushResponse}");
        Assert.Equal(1, ((CacheListPushFrontResponse.Success)pushResponse).ListLength);

        CacheListFetchResponse fetchResponse = await client.ListFetchAsync(cacheName, listName);
        Assert.True(fetchResponse is CacheListFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
        var hitResponse = (CacheListFetchResponse.Hit)fetchResponse;

        var list = hitResponse.ByteArrayList;
        Assert.Single(list);
        Assert.Contains(value1, list);

        // Test push semantics
        var value2 = Utils.NewGuidByteArray();
        pushResponse = await client.ListPushFrontAsync(cacheName, listName, value2, false);
        Assert.True(pushResponse is CacheListPushFrontResponse.Success, $"Unexpected response: {pushResponse}");
        var successResponse = (CacheListPushFrontResponse.Success)pushResponse;
        Assert.Equal(2, successResponse.ListLength);

        fetchResponse = await client.ListFetchAsync(cacheName, listName);
        Assert.True(fetchResponse is CacheListFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
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

        await client.ListPushFrontAsync(cacheName, listName, value, false, ttl: TimeSpan.FromSeconds(5));
        await Task.Delay(100);

        await client.ListPushFrontAsync(cacheName, listName, value, false, ttl: TimeSpan.FromSeconds(10));
        await Task.Delay(4900);

        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Miss, $"Unexpected response: {response}");
    }

    [Fact]
    public async Task ListPushFrontFetch_ValueIsByteArray_RefreshTtl()
    {
        var listName = Utils.NewGuidString();
        var value = Utils.NewGuidByteArray();

        await client.ListPushFrontAsync(cacheName, listName, value, false, ttl: TimeSpan.FromSeconds(2));
        await client.ListPushFrontAsync(cacheName, listName, value, true, ttl: TimeSpan.FromSeconds(10));
        await Task.Delay(2000);

        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Hit, $"Unexpected response: {response}");
        var hitResponse = (CacheListFetchResponse.Hit)response;
        Assert.Equal(2, hitResponse.ByteArrayList!.Count);
    }

    [Fact]
    public async Task ListPushFrontAsync_ValueIsByteArrayTruncateBackToSizeIsZero_IsError()
    {
        var response = await client.ListPushFrontAsync("myCache", "listName", new byte[] { 0x00 }, false, truncateBackToSize: 0);
        Assert.True(response is CacheListPushFrontResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheListPushFrontResponse.Error)response).ErrorCode);
    }

    [Theory]
    [InlineData(null, "my-list", "my-value")]
    [InlineData("cache", null, "my-value")]
    [InlineData("cache", "my-list", null)]
    public async Task ListPushFrontAsync_NullChecksString_IsError(string cacheName, string listName, string value)
    {
        CacheListPushFrontResponse response = await client.ListPushFrontAsync(cacheName, listName, value, false);
        Assert.True(response is CacheListPushFrontResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheListPushFrontResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task ListPushFrontFetch_ValueIsString_HappyPath()
    {
        var listName = Utils.NewGuidString();
        var value1 = Utils.NewGuidString();

        CacheListPushFrontResponse pushResponse = await client.ListPushFrontAsync(cacheName, listName, value1, false);
        Assert.True(pushResponse is CacheListPushFrontResponse.Success, $"Unexpected response: {pushResponse}");
        var successResponse = (CacheListPushFrontResponse.Success)pushResponse;
        Assert.Equal(1, successResponse.ListLength);

        CacheListFetchResponse fetchResponse = await client.ListFetchAsync(cacheName, listName);
        Assert.True(fetchResponse is CacheListFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
        var hitResponse = (CacheListFetchResponse.Hit)fetchResponse;

        var list = hitResponse.StringList();
        Assert.Single(list);
        Assert.Contains(value1, list);

        // Test push semantics
        var value2 = Utils.NewGuidString();
        pushResponse = await client.ListPushFrontAsync(cacheName, listName, value2, false);
        Assert.True(pushResponse is CacheListPushFrontResponse.Success, $"Unexpected response: {pushResponse}");
        successResponse = (CacheListPushFrontResponse.Success)pushResponse;
        Assert.Equal(2, successResponse.ListLength);

        fetchResponse = await client.ListFetchAsync(cacheName, listName);
        Assert.True(fetchResponse is CacheListFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
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

        await client.ListPushFrontAsync(cacheName, listName, value, false, ttl: TimeSpan.FromSeconds(5));
        await Task.Delay(100);

        await client.ListPushFrontAsync(cacheName, listName, value, false, ttl: TimeSpan.FromSeconds(10));
        await Task.Delay(4900);

        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Miss, $"Unexpected response: {response}");
    }

    [Fact]
    public async Task ListPushFrontFetch_ValueIsString_RefreshTtl()
    {
        var listName = Utils.NewGuidString();
        var value = Utils.NewGuidString();

        await client.ListPushFrontAsync(cacheName, listName, value, false, ttl: TimeSpan.FromSeconds(2));
        await client.ListPushFrontAsync(cacheName, listName, value, true, ttl: TimeSpan.FromSeconds(10));
        await Task.Delay(2000);

        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Hit, $"Unexpected response: {response}");
        var hitResponse = (CacheListFetchResponse.Hit)response;
        Assert.Equal(2, hitResponse.StringList()!.Count);
    }

    [Fact]
    public async Task ListPushFrontAsync_ValueIsStringTruncateBackToSizeIsZero_IsError()
    {
        var response = await client.ListPushFrontAsync("myCache", "listName", "value", false, truncateBackToSize: 0);
        Assert.True(response is CacheListPushFrontResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheListPushFrontResponse.Error)response).ErrorCode);
    }

    [Theory]
    [InlineData(null, "my-list", new byte[] { 0x00 })]
    [InlineData("cache", null, new byte[] { 0x00 })]
    [InlineData("cache", "my-list", null)]
    public async Task ListPushBackAsync_NullChecksByteArray_IsError(string cacheName, string listName, byte[] value)
    {
        CacheListPushBackResponse response = await client.ListPushBackAsync(cacheName, listName, value, false);
        Assert.True(response is CacheListPushBackResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheListPushBackResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task ListPushBackFetch_ValueIsByteArray_HappyPath()
    {
        var listName = Utils.NewGuidString();
        var value1 = Utils.NewGuidByteArray();

        CacheListPushBackResponse pushResponse = await client.ListPushBackAsync(cacheName, listName, value1, false);
        Assert.True(pushResponse is CacheListPushBackResponse.Success, $"Unexpected response: {pushResponse}");
        var successResponse = (CacheListPushBackResponse.Success)pushResponse;
        Assert.Equal(1, successResponse.ListLength);

        CacheListFetchResponse fetchResponse = await client.ListFetchAsync(cacheName, listName);
        Assert.True(fetchResponse is CacheListFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
        var hitResponse = (CacheListFetchResponse.Hit)fetchResponse;

        var list = hitResponse.ByteArrayList;
        Assert.Single(list);
        Assert.Contains(value1, list);

        // Test push semantics
        var value2 = Utils.NewGuidByteArray();
        pushResponse = await client.ListPushBackAsync(cacheName, listName, value2, false);
        Assert.True(pushResponse is CacheListPushBackResponse.Success, $"Unexpected response: {pushResponse}");
        successResponse = (CacheListPushBackResponse.Success)pushResponse;
        Assert.Equal(2, successResponse.ListLength);

        fetchResponse = await client.ListFetchAsync(cacheName, listName);
        Assert.True(fetchResponse is CacheListFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
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

        await client.ListPushBackAsync(cacheName, listName, value, false, ttl: TimeSpan.FromSeconds(5));
        await Task.Delay(100);

        await client.ListPushBackAsync(cacheName, listName, value, false, ttl: TimeSpan.FromSeconds(10));
        await Task.Delay(4900);

        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Miss, $"Unexpected response: {response}");
    }

    [Fact]
    public async Task ListPushBackFetch_ValueIsByteArray_RefreshTtl()
    {
        var listName = Utils.NewGuidString();
        var value = Utils.NewGuidByteArray();

        await client.ListPushBackAsync(cacheName, listName, value, false, ttl: TimeSpan.FromSeconds(2));
        await client.ListPushBackAsync(cacheName, listName, value, true, ttl: TimeSpan.FromSeconds(10));
        await Task.Delay(2000);

        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Hit, $"Unexpected response: {response}");
        var hitResponse = (CacheListFetchResponse.Hit)response;
        Assert.Equal(2, hitResponse.ByteArrayList!.Count);
    }

    [Fact]
    public async Task ListPushBackAsync_ValueIsByteArrayTruncateFrontToSizeIsZero_IsError()
    {
        var response = await client.ListPushBackAsync("myCache", "listName", new byte[] { 0x00 }, false, truncateFrontToSize: 0);
        Assert.True(response is CacheListPushBackResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheListPushBackResponse.Error)response).ErrorCode);
    }

    [Theory]
    [InlineData(null, "my-list", "my-value")]
    [InlineData("cache", null, "my-value")]
    [InlineData("cache", "my-list", null)]
    public async Task ListPushBackAsync_NullChecksString_IsError(string cacheName, string listName, string value)
    {
        CacheListPushBackResponse response = await client.ListPushBackAsync(cacheName, listName, value, false);
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
        await client.ListPushBackAsync(cacheName, listName, value1, false);
        await client.ListPushBackAsync(cacheName, listName, value2, false);
        await client.ListPushBackAsync(cacheName, listName, value3, false, null, 2);
        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Hit, $"Unexpected response: {response}");
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
        Assert.True(response is CacheListFetchResponse.Hit, $"Unexpected response: {response}");
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
        Assert.True(response is CacheListFetchResponse.Hit, $"Unexpected response: {response}");
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
        Assert.True(response is CacheListFetchResponse.Hit, $"Unexpected response: {response}");
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
        Assert.True(pushResponse is CacheListPushBackResponse.Success, $"Unexpected response: {pushResponse}");
        var successResponse = (CacheListPushBackResponse.Success)pushResponse;
        Assert.Equal(1, successResponse.ListLength);

        CacheListFetchResponse fetchResponse = await client.ListFetchAsync(cacheName, listName);
        Assert.True(fetchResponse is CacheListFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
        var hitResponse = (CacheListFetchResponse.Hit)fetchResponse;

        var list = hitResponse.StringList();
        Assert.Single(list);
        Assert.Contains(value1, list);

        // Test push semantics
        var value2 = Utils.NewGuidString();
        pushResponse = await client.ListPushBackAsync(cacheName, listName, value2, false);
        successResponse = (CacheListPushBackResponse.Success)pushResponse;
        successResponse = (CacheListPushBackResponse.Success)pushResponse;
        Assert.Equal(2, successResponse.ListLength);

        fetchResponse = await client.ListFetchAsync(cacheName, listName);
        Assert.True(fetchResponse is CacheListFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
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

        await client.ListPushBackAsync(cacheName, listName, value, false, ttl: TimeSpan.FromSeconds(5));
        await Task.Delay(100);

        await client.ListPushBackAsync(cacheName, listName, value, false, ttl: TimeSpan.FromSeconds(10));
        await Task.Delay(4900);

        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Miss, $"Unexpected response: {response}");
    }

    [Fact]
    public async Task ListPushBackFetch_ValueIsString_RefreshTtl()
    {
        var listName = Utils.NewGuidString();
        var value = Utils.NewGuidString();

        await client.ListPushBackAsync(cacheName, listName, value, false, ttl: TimeSpan.FromSeconds(2));
        await client.ListPushBackAsync(cacheName, listName, value, true, ttl: TimeSpan.FromSeconds(10));
        await Task.Delay(2000);

        CacheListFetchResponse response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Hit, $"Unexpected response: {response}");
        var hitResponse = (CacheListFetchResponse.Hit)response;
        Assert.Equal(2, hitResponse.StringList()!.Count);
    }

    [Fact]
    public async Task ListPushBackAsync_ValueIsStringTruncateFrontToSizeIsZero_IsError()
    {
        var response = await client.ListPushBackAsync("myCache", "listName", "value", false, truncateFrontToSize: 0);
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

        await client.ListPushFrontAsync(cacheName, listName, value1, false);
        await client.ListPushFrontAsync(cacheName, listName, value2, false);
        CacheListPopFrontResponse response = await client.ListPopFrontAsync(cacheName, listName);
        Assert.True(response is CacheListPopFrontResponse.Hit, $"Unexpected response: {response}");
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
        Assert.True(response is CacheListPopFrontResponse.Hit, $"Unexpected response: {response}");
        var hitResponse = (CacheListPopFrontResponse.Hit)response;

        Assert.Equal(value2, hitResponse.String());
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

        await client.ListPushBackAsync(cacheName, listName, value1, false);
        await client.ListPushBackAsync(cacheName, listName, value2, false);
        CacheListPopBackResponse response = await client.ListPopBackAsync(cacheName, listName);
        Assert.True(response is CacheListPopBackResponse.Hit, $"Unexpected response: {response}");
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
        Assert.True(response is CacheListPopBackResponse.Hit, $"Unexpected response: {response}");
        var hitResponse = (CacheListPopBackResponse.Hit)response;

        Assert.Equal(value2, hitResponse.String());
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

        await client.ListPushFrontAsync(cacheName, listName, field2, true, ttl: TimeSpan.FromSeconds(10));
        await client.ListPushFrontAsync(cacheName, listName, field1, true, ttl: TimeSpan.FromSeconds(10));

        CacheListFetchResponse fetchResponse = await client.ListFetchAsync(cacheName, listName);
        Assert.True(fetchResponse is CacheListFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
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

        await client.ListPushFrontAsync(cacheName, listName, field2, true, ttl: TimeSpan.FromSeconds(10));
        await client.ListPushFrontAsync(cacheName, listName, field1, true, ttl: TimeSpan.FromSeconds(10));

        CacheListFetchResponse fetchResponse = await client.ListFetchAsync(cacheName, listName);
        Assert.True(fetchResponse is CacheListFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
        var hitResponse = (CacheListFetchResponse.Hit)fetchResponse;

        Assert.Contains(field1, hitResponse.ByteArrayList!);
        Assert.Contains(field2, hitResponse.ByteArrayList!);
        Assert.Equal(2, hitResponse.ByteArrayList!.Count);
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
            await client.ListPushBackAsync(cacheName, listName, value, true, ttl: TimeSpan.FromSeconds(60));
        }

        await client.ListPushBackAsync(cacheName, listName, valueOfInterest, false);
        await client.ListPushBackAsync(cacheName, listName, valueOfInterest, false);

        // Remove value of interest
        await client.ListRemoveValueAsync(cacheName, listName, valueOfInterest);

        // Test not there
        var response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Hit, $"Unexpected response: {response}");
        var cachedList = ((CacheListFetchResponse.Hit)response).ByteArrayList!;
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

        var response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Hit, $"Unexpected response: {response}");
        var cachedList = ((CacheListFetchResponse.Hit)response).ByteArrayList!;
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
            await client.ListPushBackAsync(cacheName, listName, value, true, ttl: TimeSpan.FromSeconds(60));
        }

        await client.ListPushBackAsync(cacheName, listName, valueOfInterest, false);
        await client.ListPushBackAsync(cacheName, listName, valueOfInterest, false);

        // Remove value of interest
        await client.ListRemoveValueAsync(cacheName, listName, valueOfInterest);

        // Test not there
        var response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Hit, $"Unexpected response: {response}");
        var cachedList = ((CacheListFetchResponse.Hit)response).StringList()!;
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

        var response = await client.ListFetchAsync(cacheName, listName);
        Assert.True(response is CacheListFetchResponse.Hit, $"Unexpected response: {response}");
        var cachedList = ((CacheListFetchResponse.Hit)response).StringList()!;
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
            await client.ListPushBackAsync(cacheName, listName, Utils.NewGuidByteArray(), false);
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
        var pushResponse = await client.ListPushFrontAsync(cacheName, listName, Utils.NewGuidString(), false);
        Assert.True(pushResponse is CacheListPushFrontResponse.Success, $"Unexpected response: {pushResponse}");
        pushResponse = await client.ListPushFrontAsync(cacheName, listName, Utils.NewGuidString(), false);
        Assert.True(pushResponse is CacheListPushFrontResponse.Success, $"Unexpected response: {pushResponse}");
        pushResponse = await client.ListPushFrontAsync(cacheName, listName, Utils.NewGuidString(), false);
        Assert.True(pushResponse is CacheListPushFrontResponse.Success, $"Unexpected response: {pushResponse}");

        Assert.True((await client.ListFetchAsync(cacheName, listName)) is CacheListFetchResponse.Hit);
        var deleteResponse = await client.ListDeleteAsync(cacheName, listName);
        Assert.True(deleteResponse is CacheListDeleteResponse.Success, $"Unexpected response: {deleteResponse}");

        var fetchResponse = await client.ListFetchAsync(cacheName, listName);
        Assert.True(fetchResponse is CacheListFetchResponse.Miss, $"Unexpected response: {fetchResponse}");
    }
}
