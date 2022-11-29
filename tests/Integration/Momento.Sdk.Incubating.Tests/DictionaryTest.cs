using System;
using Momento.Sdk.Incubating.Requests;
using Momento.Sdk.Incubating.Responses;
using Momento.Sdk.Internal.ExtensionMethods;

namespace Momento.Sdk.Incubating.Tests;

[Collection("SimpleCacheClient")]
public class DictionaryTest : TestBase
{
    public DictionaryTest(SimpleCacheClientFixture fixture) : base(fixture)
    {
    }

    [Theory]
    [InlineData(null, "my-dictionary", new byte[] { 0x00 })]
    [InlineData("cache", null, new byte[] { 0x00 })]
    [InlineData("cache", "my-dictionary", null)]
    public async Task DictionaryGetAsync_NullChecksFieldIsByteArray_IsError(string cacheName, string dictionaryName, byte[] field)
    {
        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetResponse.Error)response).ErrorCode);
    }

    [Theory]
    [InlineData(null, "my-dictionary", new byte[] { 0x00 }, new byte[] { 0x00 })]
    [InlineData("cache", null, new byte[] { 0x00 }, new byte[] { 0x00 })]
    [InlineData("cache", "my-dictionary", null, new byte[] { 0x00 })]
    [InlineData("cache", "my-dictionary", new byte[] { 0x00 }, null)]
    public async Task DictionarySetAsync_NullChecksFieldIsByteArrayValueIsByteArray_IsError(string cacheName, string dictionaryName, byte[] field, byte[] value)
    {
        CacheDictionarySetResponse response = await client.DictionarySetAsync(cacheName, dictionaryName, field, value);
        Assert.True(response is CacheDictionarySetResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionarySetResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task DictionaryGetAsync_FieldIsByteArray_DictionaryIsMissing()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidByteArray();
        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Miss, $"Unexpected response: {response}");
    }

    [Fact]
    public async Task DictionarySetGetAsync_FieldIsByteArrayValueIsByteArray_HappyPath()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidByteArray();
        var value = Utils.NewGuidByteArray();

        await client.DictionarySetAsync(cacheName, dictionaryName, field, value);

        CacheDictionaryGetResponse getResponse = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(getResponse is CacheDictionaryGetResponse.Hit, $"Unexpected response: {getResponse}");
    }

    [Fact]
    public async Task DictionarySetGetAsync_FieldIsByteArrayDictionaryIsPresent_FieldIsMissing()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidByteArray();
        var value = Utils.NewGuidByteArray();

        CacheDictionarySetResponse setResponse = await client.DictionarySetAsync(cacheName, dictionaryName, field, value);
        Assert.True(setResponse is CacheDictionarySetResponse.Success, $"Unexpected response: {setResponse}");

        var otherField = Utils.NewGuidByteArray();
        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, otherField);
        Assert.True(response is CacheDictionaryGetResponse.Miss, $"Unexpected response: {response}");
    }

    [Fact]
    public async Task DictionarySetGetAsync_FieldIsByteArrayValueIsByteArray_NoRefreshTtl()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidByteArray();
        var value = Utils.NewGuidByteArray();

        CacheDictionarySetResponse setResponse = await client.DictionarySetAsync(cacheName, dictionaryName, field, value, CollectionTtl.Of(TimeSpan.FromSeconds(5)).WithNoRefreshTtlOnUpdates());
        Assert.True(setResponse is CacheDictionarySetResponse.Success, $"Unexpected response: {setResponse}");
        await Task.Delay(100);

        setResponse = await client.DictionarySetAsync(cacheName, dictionaryName, field, value, CollectionTtl.Of(TimeSpan.FromSeconds(10)).WithNoRefreshTtlOnUpdates());
        Assert.True(setResponse is CacheDictionarySetResponse.Success, $"Unexpected response: {setResponse}");
        await Task.Delay(4900);

        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Miss, $"Unexpected response: {response}");
    }

    [Fact]
    public async Task DictionarySetGetAsync_FieldIsByteArrayValueIsByteArray_RefreshTtl()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidByteArray();
        var value = Utils.NewGuidByteArray();

        CacheDictionarySetResponse setResponse = await client.DictionarySetAsync(cacheName, dictionaryName, field, value, CollectionTtl.Of(TimeSpan.FromSeconds(2)));
        Assert.True(setResponse is CacheDictionarySetResponse.Success, $"Unexpected response: {setResponse}");
        setResponse = await client.DictionarySetAsync(cacheName, dictionaryName, field, value, CollectionTtl.Of(TimeSpan.FromSeconds(10)));
        Assert.True(setResponse is CacheDictionarySetResponse.Success, $"Unexpected response: {setResponse}");
        await Task.Delay(2000);

        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Hit, $"Unexpected response: {response}");
        Assert.Equal(value, ((CacheDictionaryGetResponse.Hit)response).ByteArray);
    }

    [Theory]
    [InlineData(null, "my-dictionary", "field")]
    [InlineData("cache", null, "field")]
    [InlineData("cache", "my-dictionary", null)]
    public async Task DictionaryIncrementAsync_NullChecksFieldIsString_IsError(string cacheName, string dictionaryName, string field)
    {
        CacheDictionaryIncrementResponse response = await client.DictionaryIncrementAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryIncrementResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryIncrementResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task DictionaryIncrementAsync_IncrementFromZero_HappyPath()
    {
        var dictionaryName = Utils.NewGuidString();
        var fieldName = Utils.NewGuidString();

        CacheDictionaryIncrementResponse incrementResponse = await client.DictionaryIncrementAsync(cacheName, dictionaryName, fieldName, 1);
        Assert.True(incrementResponse is CacheDictionaryIncrementResponse.Success, $"Unexpected response: {incrementResponse}");
        var successResponse = (CacheDictionaryIncrementResponse.Success)incrementResponse;
        Assert.Equal(1, successResponse.Value);

        incrementResponse = await client.DictionaryIncrementAsync(cacheName, dictionaryName, fieldName, 41);
        Assert.True(incrementResponse is CacheDictionaryIncrementResponse.Success, $"Unexpected response: {incrementResponse}");
        successResponse = (CacheDictionaryIncrementResponse.Success)incrementResponse;
        Assert.Equal(42, successResponse.Value);

        incrementResponse = await client.DictionaryIncrementAsync(cacheName, dictionaryName, fieldName, -1042);
        Assert.True(incrementResponse is CacheDictionaryIncrementResponse.Success, $"Unexpected response: {incrementResponse}");
        successResponse = (CacheDictionaryIncrementResponse.Success)incrementResponse;
        Assert.Equal(-1000, successResponse.Value);

        CacheDictionaryGetResponse getResponse = await client.DictionaryGetAsync(cacheName, dictionaryName, fieldName);
        Assert.True(getResponse is CacheDictionaryGetResponse.Hit, $"Unexpected response: {getResponse}");
        var hitResponse = (CacheDictionaryGetResponse.Hit)getResponse;
        Assert.Equal("-1000", hitResponse.String());
    }

    [Fact]
    public async Task DictionaryIncrementAsync_IncrementFromZero_RefreshTtl()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidString();

        CacheDictionaryIncrementResponse resp = await client.DictionaryIncrementAsync(cacheName, dictionaryName, field, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(2)));
        Assert.True(resp is CacheDictionaryIncrementResponse.Success, $"Unexpected response: {resp}");
        resp = await client.DictionaryIncrementAsync(cacheName, dictionaryName, field, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(10)));
        Assert.True(resp is CacheDictionaryIncrementResponse.Success, $"Unexpected response: {resp}");
        await Task.Delay(2000);

        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Hit, $"Unexpected response: {response}");
        Assert.Equal("2", ((CacheDictionaryGetResponse.Hit)response).String());
    }

    [Fact]
    public async Task DictionaryIncrementAsync_IncrementFromZero_NoRefreshTtl()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidString();

        CacheDictionaryIncrementResponse resp = await client.DictionaryIncrementAsync(cacheName, dictionaryName, field, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(5)));
        Assert.True(resp is CacheDictionaryIncrementResponse.Success, $"Unexpected response: {resp}");
        await Task.Delay(101);

        resp = await client.DictionaryIncrementAsync(cacheName, dictionaryName, field, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(10)).WithNoRefreshTtlOnUpdates());
        Assert.True(resp is CacheDictionaryIncrementResponse.Success, $"Unexpected response: {resp}");
        await Task.Delay(4900);

        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Miss, $"Unexpected response: {response}");
    }

    [Fact]
    public async Task DictionaryIncrementAsync_SetAndReset_HappyPath()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidString();

        // Set field
        await client.DictionarySetAsync(cacheName, dictionaryName, field, "10");
        CacheDictionaryIncrementResponse incrementResponse = await client.DictionaryIncrementAsync(cacheName, dictionaryName, field, amount: 0);
        Assert.True(incrementResponse is CacheDictionaryIncrementResponse.Success, $"Unexpected response: {incrementResponse}");
        var successResponse = (CacheDictionaryIncrementResponse.Success)incrementResponse;
        Assert.Equal(10, successResponse.Value);

        incrementResponse = await client.DictionaryIncrementAsync(cacheName, dictionaryName, field, amount: 90);
        Assert.True(incrementResponse is CacheDictionaryIncrementResponse.Success, $"Unexpected response: {incrementResponse}");
        successResponse = (CacheDictionaryIncrementResponse.Success)incrementResponse;
        Assert.Equal(100, successResponse.Value);

        // Reset field
        await client.DictionarySetAsync(cacheName, dictionaryName, field, "0");
        incrementResponse = await client.DictionaryIncrementAsync(cacheName, dictionaryName, field, amount: 0);
        Assert.True(incrementResponse is CacheDictionaryIncrementResponse.Success, $"Unexpected response: {incrementResponse}");
        successResponse = (CacheDictionaryIncrementResponse.Success)incrementResponse;
        Assert.Equal(0, successResponse.Value);
    }

    [Fact]
    public async Task DictionaryIncrementAsync_FailedPrecondition_IsError()
    {
        var dictionaryName = Utils.NewGuidString();
        var fieldName = Utils.NewGuidString();

        var setResponse = await client.DictionarySetAsync(cacheName, dictionaryName, fieldName, "abcxyz");
        Assert.True(setResponse is CacheDictionarySetResponse.Success, $"Unexpected response: {setResponse}");

        var dictionaryIncrementResponse = await client.DictionaryIncrementAsync(cacheName, dictionaryName, fieldName);
        Assert.True(dictionaryIncrementResponse is CacheDictionaryIncrementResponse.Error, $"Unexpected response: {dictionaryIncrementResponse}");
        Assert.Equal(MomentoErrorCode.FAILED_PRECONDITION_ERROR, ((CacheDictionaryIncrementResponse.Error)dictionaryIncrementResponse).ErrorCode);
    }

    [Theory]
    [InlineData(null, "my-dictionary", "my-field")]
    [InlineData("cache", null, "my-field")]
    [InlineData("cache", "my-dictionary", null)]
    public async Task DictionaryGetAsync_NullChecksFieldIsString_IsError(string cacheName, string dictionaryName, string field)
    {
        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetResponse.Error)response).ErrorCode);
    }

    [Theory]
    [InlineData(null, "my-dictionary", "my-field", "my-value")]
    [InlineData("cache", null, "my-field", "my-value")]
    [InlineData("cache", "my-dictionary", null, "my-value")]
    [InlineData("cache", "my-dictionary", "my-field", null)]
    public async Task DictionarySetAsync_NullChecksFieldIsStringValueIsString_IsError(string cacheName, string dictionaryName, string field, string value)
    {
        CacheDictionarySetResponse response = await client.DictionarySetAsync(cacheName, dictionaryName, field, value);
        Assert.True(response is CacheDictionarySetResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionarySetResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task DictionaryGetAsync_FieldIsString_DictionaryIsMissing()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidString();
        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Miss, $"Unexpected response: {response}");
    }

    [Fact]
    public async Task DictionarySetGetAsync_FieldIsStringValueIsString_HappyPath()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidString();
        var value = Utils.NewGuidString();

        CacheDictionarySetResponse response = await client.DictionarySetAsync(cacheName, dictionaryName, field, value);
        Assert.True(response is CacheDictionarySetResponse.Success, $"Unexpected response: {response}");

        CacheDictionaryGetResponse getResponse = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(getResponse is CacheDictionaryGetResponse.Hit, $"Unexpected response: {getResponse}");
    }

    [Fact]
    public async Task DictionarySetGetAsync_DictionaryIsPresent_FieldIsMissing()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidString();
        var value = Utils.NewGuidString();

        CacheDictionarySetResponse setResponse = await client.DictionarySetAsync(cacheName, dictionaryName, field, value);
        Assert.True(setResponse is CacheDictionarySetResponse.Success, $"Unexpected response: {setResponse}");

        var otherField = Utils.NewGuidString();
        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, otherField);
        Assert.True(response is CacheDictionaryGetResponse.Miss, $"Unexpected response: {response}");
    }

    [Fact]
    public async Task DictionarySetGetAsync_FieldIsStringValueIsString_NoRefreshTtl()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidString();
        var value = Utils.NewGuidString();

        CacheDictionarySetResponse setResponse = await client.DictionarySetAsync(cacheName, dictionaryName, field, value, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(5)));
        Assert.True(setResponse is CacheDictionarySetResponse.Success, $"Unexpected response: {setResponse}");
        await Task.Delay(100);

        setResponse = await client.DictionarySetAsync(cacheName, dictionaryName, field, value, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(10)).WithNoRefreshTtlOnUpdates());
        Assert.True(setResponse is CacheDictionarySetResponse.Success, $"Unexpected response: {setResponse}");
        await Task.Delay(4900);

        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Miss, $"Unexpected response: {response}");
    }

    [Fact]
    public async Task DictionarySetGetAsync_FieldIsStringValueIsString_RefreshTtl()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidString();
        var value = Utils.NewGuidString();

        CacheDictionarySetResponse setResponse = await client.DictionarySetAsync(cacheName, dictionaryName, field, value, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(2)));
        Assert.True(setResponse is CacheDictionarySetResponse.Success, $"Unexpected response: {setResponse}");
        setResponse = await client.DictionarySetAsync(cacheName, dictionaryName, field, value, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(10)));
        Assert.True(setResponse is CacheDictionarySetResponse.Success, $"Unexpected response: {setResponse}");
        await Task.Delay(2000);

        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Hit, $"Unexpected response: {response}");
        Assert.Equal(value, ((CacheDictionaryGetResponse.Hit)response).String());
    }

    [Theory]
    [InlineData(null, "my-dictionary", "my-field", new byte[] { 0x00 })]
    [InlineData("cache", null, "my-field", new byte[] { 0x00 })]
    [InlineData("cache", "my-dictionary", null, new byte[] { 0x00 })]
    [InlineData("cache", "my-dictionary", "my-field", null)]
    public async Task DictionarySetAsync_NullChecksFieldIsStringValueIsByteArray_IsError(string cacheName, string dictionaryName, string field, byte[] value)
    {
        CacheDictionarySetResponse response = await client.DictionarySetAsync(cacheName, dictionaryName, field, value);
        Assert.True(response is CacheDictionarySetResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionarySetResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task DictionarySetGetAsync_FieldIsStringValueIsByteArray_HappyPath()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidString();
        var value = Utils.NewGuidByteArray();

        CacheDictionarySetResponse setResponse = await client.DictionarySetAsync(cacheName, dictionaryName, field, value);
        Assert.True(setResponse is CacheDictionarySetResponse.Success, $"Unexpected response: {setResponse}");

        CacheDictionaryGetResponse getResponse = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(getResponse is CacheDictionaryGetResponse.Hit, $"Unexpected response: {getResponse}");
    }

    [Fact]
    public async Task DictionarySetGetAsync_FieldIsStringValueIsByteArray_NoRefreshTtl()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidString();
        var value = Utils.NewGuidByteArray();

        CacheDictionarySetResponse setResponse = await client.DictionarySetAsync(cacheName, dictionaryName, field, value, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(5)));
        Assert.True(setResponse is CacheDictionarySetResponse.Success, $"Unexpected response: {setResponse}");
        await Task.Delay(100);

        setResponse = await client.DictionarySetAsync(cacheName, dictionaryName, field, value, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(10)).WithNoRefreshTtlOnUpdates());
        Assert.True(setResponse is CacheDictionarySetResponse.Success, $"Unexpected response: {setResponse}");
        await Task.Delay(4900);

        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Miss, $"Unexpected response: {response}");
    }

    [Fact]
    public async Task DictionarySetGetAsync_FieldIsStringValueIsByteArray_RefreshTtl()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidString();
        var value = Utils.NewGuidByteArray();

        CacheDictionarySetResponse setResponse = await client.DictionarySetAsync(cacheName, dictionaryName, field, value, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(2)));
        Assert.True(setResponse is CacheDictionarySetResponse.Success, $"Unexpected response: {setResponse}");
        setResponse = await client.DictionarySetAsync(cacheName, dictionaryName, field, value, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(10)));
        Assert.True(setResponse is CacheDictionarySetResponse.Success, $"Unexpected response: {setResponse}");
        await Task.Delay(2000);

        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Hit, $"Unexpected response: {response}");
        Assert.Equal(value, ((CacheDictionaryGetResponse.Hit)response).ByteArray);
    }

    [Fact]
    public async Task DictionarySetBatchAsync_NullChecksFieldIsByteArrayValueIsByteArray_IsError()
    {
        var dictionaryName = Utils.NewGuidString();
        var dictionary = new Dictionary<byte[], byte[]>();
        CacheDictionarySetBatchResponse response = await client.DictionarySetBatchAsync(null!, dictionaryName, dictionary);
        Assert.True(response is CacheDictionarySetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionarySetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionarySetBatchAsync(cacheName, null!, dictionary);
        Assert.True(response is CacheDictionarySetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionarySetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionarySetBatchAsync(cacheName, dictionaryName, (IEnumerable<KeyValuePair<byte[], byte[]>>)null!);
        Assert.True(response is CacheDictionarySetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionarySetBatchResponse.Error)response).ErrorCode);

        dictionary[Utils.NewGuidByteArray()] = null!;
        response = await client.DictionarySetBatchAsync(cacheName, dictionaryName, dictionary);
        Assert.True(response is CacheDictionarySetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionarySetBatchResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task DictionarySetBatchAsync_FieldsAreByteArrayValuesAreByteArray_HappyPath()
    {
        var dictionaryName = Utils.NewGuidString();
        var field1 = Utils.NewGuidByteArray();
        var value1 = Utils.NewGuidByteArray();
        var field2 = Utils.NewGuidByteArray();
        var value2 = Utils.NewGuidByteArray();

        var items = new Dictionary<byte[], byte[]>() { { field1, value1 }, { field2, value2 } };

        await client.DictionarySetBatchAsync(cacheName, dictionaryName, items, CollectionTtl.Of(TimeSpan.FromSeconds(10)));

        CacheDictionaryGetResponse getResponse = await client.DictionaryGetAsync(cacheName, dictionaryName, field1);
        Assert.True(getResponse is CacheDictionaryGetResponse.Hit, $"Unexpected response: {getResponse}");
        Assert.Equal(value1, ((CacheDictionaryGetResponse.Hit)getResponse).ByteArray);

        getResponse = await client.DictionaryGetAsync(cacheName, dictionaryName, field2);
        Assert.True(getResponse is CacheDictionaryGetResponse.Hit, $"Unexpected response: {getResponse}");
        Assert.Equal(value2, ((CacheDictionaryGetResponse.Hit)getResponse).ByteArray);
    }

    [Fact]
    public async Task DictionarySetBatchAsync_FieldsAreByteArrayValuesAreByteArray_NoRefreshTtl()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidByteArray();
        var value = Utils.NewGuidByteArray();
        var content = new Dictionary<byte[], byte[]>() { { field, value } };

        await client.DictionarySetBatchAsync(cacheName, dictionaryName, content, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(5)));
        await Task.Delay(100);

        await client.DictionarySetBatchAsync(cacheName, dictionaryName, content, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(10)).WithNoRefreshTtlOnUpdates());
        await Task.Delay(4900);

        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Miss, $"Unexpected response: {response}");
    }

    [Fact]
    public async Task DictionarySetBatchAsync_FieldsAreByteArrayValuesAreByteArray_RefreshTtl()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidByteArray();
        var value = Utils.NewGuidByteArray();
        var content = new Dictionary<byte[], byte[]>() { { field, value } };

        await client.DictionarySetBatchAsync(cacheName, dictionaryName, content, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(2)));
        await client.DictionarySetBatchAsync(cacheName, dictionaryName, content, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(10)));
        await Task.Delay(2000);

        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Hit, $"Unexpected response: {response}");
        Assert.Equal(value, ((CacheDictionaryGetResponse.Hit)response).ByteArray);
    }

    [Fact]
    public async Task DictionarySetBatchAsync_NullChecksFieldsAreStringValuesAreString_IsError()
    {
        var dictionaryName = Utils.NewGuidString();
        var dictionary = new Dictionary<string, string>();
        CacheDictionarySetBatchResponse response = await client.DictionarySetBatchAsync(null!, dictionaryName, dictionary);
        Assert.True(response is CacheDictionarySetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionarySetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionarySetBatchAsync(cacheName, null!, dictionary);
        Assert.True(response is CacheDictionarySetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionarySetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionarySetBatchAsync(cacheName, dictionaryName, (IEnumerable<KeyValuePair<string, string>>)null!);
        Assert.True(response is CacheDictionarySetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionarySetBatchResponse.Error)response).ErrorCode);

        dictionary[Utils.NewGuidString()] = null!;
        response = await client.DictionarySetBatchAsync(cacheName, dictionaryName, dictionary);
        Assert.True(response is CacheDictionarySetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionarySetBatchResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task DictionarySetBatchAsync_FieldsAreStringValuesAreString_HappyPath()
    {
        var dictionaryName = Utils.NewGuidString();
        var field1 = Utils.NewGuidString();
        var value1 = Utils.NewGuidString();
        var field2 = Utils.NewGuidString();
        var value2 = Utils.NewGuidString();

        var items = new Dictionary<string, string>() { { field1, value1 }, { field2, value2 } };

        await client.DictionarySetBatchAsync(cacheName, dictionaryName, items);

        CacheDictionaryGetResponse getResponse = await client.DictionaryGetAsync(cacheName, dictionaryName, field1);
        Assert.True(getResponse is CacheDictionaryGetResponse.Hit, $"Unexpected response: {getResponse}");
        Assert.Equal(value1, ((CacheDictionaryGetResponse.Hit)getResponse).String());

        getResponse = await client.DictionaryGetAsync(cacheName, dictionaryName, field2);
        Assert.True(getResponse is CacheDictionaryGetResponse.Hit, $"Unexpected response: {getResponse}");
        Assert.Equal(value2, ((CacheDictionaryGetResponse.Hit)getResponse).String());
    }

    [Fact]
    public async Task DictionarySetBatchAsync_FieldsAreStringValuesAreString_NoRefreshTtl()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidString();
        var value = Utils.NewGuidString();
        var content = new Dictionary<string, string>() { { field, value } };

        await client.DictionarySetBatchAsync(cacheName, dictionaryName, content, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(5)));
        await Task.Delay(100);

        await client.DictionarySetBatchAsync(cacheName, dictionaryName, content, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(10)).WithNoRefreshTtlOnUpdates());
        await Task.Delay(4900);

        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Miss, $"Unexpected response: {response}");
    }

    [Fact]
    public async Task DictionarySetBatchAsync_FieldsAreStringValuesAreString_RefreshTtl()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidString();
        var value = Utils.NewGuidString();
        var content = new Dictionary<string, string>() { { field, value } };

        await client.DictionarySetBatchAsync(cacheName, dictionaryName, content, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(2)));
        await client.DictionarySetBatchAsync(cacheName, dictionaryName, content, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(10)));
        await Task.Delay(2000);

        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Hit, $"Unexpected response: {response}");
        Assert.Equal(value, ((CacheDictionaryGetResponse.Hit)response).String());
    }

    [Fact]
    public async Task DictionarySetBatchAsync_NullChecksFieldsAreStringValuesAreByteArray_IsError()
    {
        var dictionaryName = Utils.NewGuidString();
        var dictionary = new Dictionary<string, string>();
        CacheDictionarySetBatchResponse response = await client.DictionarySetBatchAsync(null!, dictionaryName, dictionary);
        Assert.True(response is CacheDictionarySetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionarySetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionarySetBatchAsync(cacheName, null!, dictionary);
        Assert.True(response is CacheDictionarySetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionarySetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionarySetBatchAsync(cacheName, dictionaryName, (IEnumerable<KeyValuePair<string, byte[]>>)null!);
        Assert.True(response is CacheDictionarySetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionarySetBatchResponse.Error)response).ErrorCode);

        dictionary[Utils.NewGuidString()] = null!;
        response = await client.DictionarySetBatchAsync(cacheName, dictionaryName, dictionary);
        Assert.True(response is CacheDictionarySetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionarySetBatchResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task DictionarySetBatchAsync_FieldsAreStringValuesAreByteArray_HappyPath()
    {
        var dictionaryName = Utils.NewGuidString();
        var field1 = Utils.NewGuidString();
        var value1 = Utils.NewGuidByteArray();
        var field2 = Utils.NewGuidString();
        var value2 = Utils.NewGuidByteArray();

        var items = new Dictionary<string, byte[]>() { { field1, value1 }, { field2, value2 } };

        await client.DictionarySetBatchAsync(cacheName, dictionaryName, items);

        CacheDictionaryGetResponse getResponse = await client.DictionaryGetAsync(cacheName, dictionaryName, field1);
        Assert.True(getResponse is CacheDictionaryGetResponse.Hit, $"Unexpected response: {getResponse}");
        Assert.Equal(value1, ((CacheDictionaryGetResponse.Hit)getResponse).ByteArray);

        getResponse = await client.DictionaryGetAsync(cacheName, dictionaryName, field2);
        Assert.True(getResponse is CacheDictionaryGetResponse.Hit, $"Unexpected response: {getResponse}");
        Assert.Equal(value2, ((CacheDictionaryGetResponse.Hit)getResponse).ByteArray);
    }

    [Fact]
    public async Task DictionarySetBatchAsync_FieldsAreStringValuesAreByteArray_NoRefreshTtl()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidString();
        var value = Utils.NewGuidByteArray();
        var content = new Dictionary<string, byte[]>() { { field, value } };

        await client.DictionarySetBatchAsync(cacheName, dictionaryName, content, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(5)).WithNoRefreshTtlOnUpdates());
        await Task.Delay(100);

        await client.DictionarySetBatchAsync(cacheName, dictionaryName, content, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(10)).WithNoRefreshTtlOnUpdates());
        await Task.Delay(4900);

        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Miss, $"Unexpected response: {response}");
    }

    [Fact]
    public async Task DictionarySetBatchAsync_FieldsAreStringValuesAreByteArray_RefreshTtl()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidString();
        var value = Utils.NewGuidByteArray();
        var content = new Dictionary<string, byte[]>() { { field, value } };

        await client.DictionarySetBatchAsync(cacheName, dictionaryName, content, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(2)));
        await client.DictionarySetBatchAsync(cacheName, dictionaryName, content, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(10)));
        await Task.Delay(2000);

        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Hit, $"Unexpected response: {response}");
        Assert.Equal(value, ((CacheDictionaryGetResponse.Hit)response).ByteArray);
    }

    [Fact]
    public async Task DictionaryGetBatchAsync_NullChecksFieldsAreByteArray_IsError()
    {
        var dictionaryName = Utils.NewGuidString();
        var testData = new byte[][][] { new byte[][] { Utils.NewGuidByteArray(), Utils.NewGuidByteArray() }, new byte[][] { Utils.NewGuidByteArray(), null! } };

        CacheDictionaryGetBatchResponse response = await client.DictionaryGetBatchAsync(null!, dictionaryName, testData[0]);
        Assert.True(response is CacheDictionaryGetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionaryGetBatchAsync(cacheName, null!, testData[0]);
        Assert.True(response is CacheDictionaryGetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionaryGetBatchAsync(cacheName, dictionaryName, (byte[][])null!);
        Assert.True(response is CacheDictionaryGetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionaryGetBatchAsync(cacheName, dictionaryName, testData[1]);
        Assert.True(response is CacheDictionaryGetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetBatchResponse.Error)response).ErrorCode);

        var fieldsList = new List<byte[]>(testData[0]);
        response = await client.DictionaryGetBatchAsync(null!, dictionaryName, fieldsList);
        Assert.True(response is CacheDictionaryGetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionaryGetBatchAsync(cacheName, null!, fieldsList);
        Assert.True(response is CacheDictionaryGetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionaryGetBatchAsync(cacheName, dictionaryName, (List<byte[]>)null!);
        Assert.True(response is CacheDictionaryGetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionaryGetBatchAsync(cacheName, dictionaryName, new List<byte[]>(testData[1]));
        Assert.True(response is CacheDictionaryGetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetBatchResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task DictionaryGetBatchAsync_FieldsAreByteArrayValuesAreByteArray_HappyPath()
    {
        var dictionaryName = Utils.NewGuidString();
        var field1 = Utils.NewGuidByteArray();
        var value1 = Utils.NewGuidByteArray();
        var field2 = Utils.NewGuidByteArray();
        var value2 = Utils.NewGuidByteArray();
        var field3 = Utils.NewGuidByteArray();

        await client.DictionarySetAsync(cacheName, dictionaryName, field1, value1);
        await client.DictionarySetAsync(cacheName, dictionaryName, field2, value2);

        CacheDictionaryGetBatchResponse response = await client.DictionaryGetBatchAsync(cacheName, dictionaryName, new byte[][] { field1, field2, field3 });
        Assert.True(response is CacheDictionaryGetBatchResponse.Success, $"Unexpected response: {response}");

        var success = (CacheDictionaryGetBatchResponse.Success)response;
        Assert.Equal(3, success.Responses.Count);
        Assert.True(success.Responses[0] is CacheDictionaryGetResponse.Hit);
        Assert.True(success.Responses[1] is CacheDictionaryGetResponse.Hit);
        Assert.True(success.Responses[2] is CacheDictionaryGetResponse.Miss);
        var values = new byte[]?[] { value1, value2, null };
        Assert.Equal(values, success.ByteArrays);
    }

    [Fact]
    public async Task DictionaryGetBatchAsync_DictionaryMissing_HappyPath()
    {
        var dictionaryName = Utils.NewGuidString();
        var field1 = Utils.NewGuidByteArray();
        var field2 = Utils.NewGuidByteArray();
        var field3 = Utils.NewGuidByteArray();

        CacheDictionaryGetBatchResponse response = await client.DictionaryGetBatchAsync(cacheName, dictionaryName, new byte[][] { field1, field2, field3 });
        Assert.True(response is CacheDictionaryGetBatchResponse.Success, $"Unexpected response: {response}");
        var nullResponse = (CacheDictionaryGetBatchResponse.Success)response;
        Assert.True(nullResponse.Responses[0] is CacheDictionaryGetResponse.Miss);
        Assert.True(nullResponse.Responses[1] is CacheDictionaryGetResponse.Miss);
        Assert.True(nullResponse.Responses[2] is CacheDictionaryGetResponse.Miss);
        var byteArrays = new byte[]?[] { null, null, null };
        var strings = new string?[] { null, null, null };

        Assert.Equal(byteArrays, nullResponse.ByteArrays);
        Assert.Equal(strings, nullResponse.Strings()!);
    }

    [Fact]
    public async Task DictionaryGetBatchAsync_NullChecksFieldsAreString_IsError()
    {
        var dictionaryName = Utils.NewGuidString();
        var testData = new string[][] { new string[] { Utils.NewGuidString(), Utils.NewGuidString() }, new string[] { Utils.NewGuidString(), null! } };
        CacheDictionaryGetBatchResponse response = await client.DictionaryGetBatchAsync(null!, dictionaryName, testData[0]);
        Assert.True(response is CacheDictionaryGetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionaryGetBatchAsync(cacheName, null!, testData[0]);
        Assert.True(response is CacheDictionaryGetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionaryGetBatchAsync(cacheName, dictionaryName, (string[])null!);
        Assert.True(response is CacheDictionaryGetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionaryGetBatchAsync(cacheName, dictionaryName, testData[1]);
        Assert.True(response is CacheDictionaryGetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetBatchResponse.Error)response).ErrorCode);

        var fieldsList = new List<string>(testData[0]);
        response = await client.DictionaryGetBatchAsync(null!, dictionaryName, fieldsList);
        Assert.True(response is CacheDictionaryGetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionaryGetBatchAsync(cacheName, null!, fieldsList);
        Assert.True(response is CacheDictionaryGetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionaryGetBatchAsync(cacheName, dictionaryName, (List<string>)null!);
        Assert.True(response is CacheDictionaryGetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionaryGetBatchAsync(cacheName, dictionaryName, new List<string>(testData[1]));
        Assert.True(response is CacheDictionaryGetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetBatchResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task DictionaryGetBatchAsync_FieldsAreString_HappyPath()
    {
        var dictionaryName = Utils.NewGuidString();
        var field1 = Utils.NewGuidString();
        var value1 = Utils.NewGuidString();
        var field2 = Utils.NewGuidString();
        var value2 = Utils.NewGuidString();
        var field3 = Utils.NewGuidString();

        await client.DictionarySetAsync(cacheName, dictionaryName, field1, value1);
        await client.DictionarySetAsync(cacheName, dictionaryName, field2, value2);

        CacheDictionaryGetBatchResponse response = await client.DictionaryGetBatchAsync(cacheName, dictionaryName, new string[] { field1, field2, field3 });
        Assert.True(response is CacheDictionaryGetBatchResponse.Success, $"Unexpected response: {response}");
        var success = (CacheDictionaryGetBatchResponse.Success)response;

        Assert.Equal(3, success.Responses.Count);
        Assert.True(success.Responses[0] is CacheDictionaryGetResponse.Hit);
        Assert.True(success.Responses[1] is CacheDictionaryGetResponse.Hit);
        Assert.True(success.Responses[2] is CacheDictionaryGetResponse.Miss);
        var values = new string?[] { value1, value2, null };
        Assert.Equal(values, success.Strings());
    }

    [Theory]
    [InlineData(null, "my-dictionary")]
    [InlineData("cache", null)]
    public async Task DictionaryFetchAsync_NullChecks_IsError(string cacheName, string dictionaryName)
    {
        CacheDictionaryFetchResponse response = await client.DictionaryFetchAsync(cacheName, dictionaryName);
        Assert.True(response is CacheDictionaryFetchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryFetchResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task DictionaryFetchAsync_Missing_HappyPath()
    {
        var dictionaryName = Utils.NewGuidString();
        CacheDictionaryFetchResponse response = await client.DictionaryFetchAsync(cacheName, dictionaryName);
        Assert.True(response is CacheDictionaryFetchResponse.Miss, $"Unexpected response: {response}");
    }

    [Fact]
    public async Task DictionaryFetchAsync_HasContentStringString_HappyPath()
    {
        var dictionaryName = Utils.NewGuidString();
        var field1 = Utils.NewGuidString();
        var value1 = Utils.NewGuidString();
        var field2 = Utils.NewGuidString();
        var value2 = Utils.NewGuidString();
        var contentDictionary = new Dictionary<string, string>() {
            {field1, value1},
            {field2, value2}
        };

        await client.DictionarySetAsync(cacheName, dictionaryName, field1, value1);
        await client.DictionarySetAsync(cacheName, dictionaryName, field2, value2);

        CacheDictionaryFetchResponse fetchResponse = await client.DictionaryFetchAsync(cacheName, dictionaryName);

        Assert.True(fetchResponse is CacheDictionaryFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
        var hitResponse = (CacheDictionaryFetchResponse.Hit)fetchResponse;
        Assert.Equal(hitResponse.StringStringDictionary(), contentDictionary);

        // Test field caching behavior
        Assert.Same(hitResponse.StringStringDictionary(), hitResponse.StringStringDictionary());
    }

    [Fact]
    public async Task DictionaryFetchAsync_HasContentStringByteArray_HappyPath()
    {
        var dictionaryName = Utils.NewGuidString();
        var field1 = Utils.NewGuidString();
        var value1 = Utils.NewGuidByteArray();
        var field2 = Utils.NewGuidString();
        var value2 = Utils.NewGuidByteArray();
        var contentDictionary = new Dictionary<string, byte[]>() {
            {field1, value1},
            {field2, value2}
        };

        await client.DictionarySetAsync(cacheName, dictionaryName, field1, value1);
        await client.DictionarySetAsync(cacheName, dictionaryName, field2, value2);

        CacheDictionaryFetchResponse fetchResponse = await client.DictionaryFetchAsync(cacheName, dictionaryName);

        Assert.True(fetchResponse is CacheDictionaryFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
        var hitResponse = (CacheDictionaryFetchResponse.Hit)fetchResponse;
        Assert.Equal(hitResponse.StringByteArrayDictionary(), contentDictionary);

        // Test field caching behavior
        Assert.Same(hitResponse.StringByteArrayDictionary(), hitResponse.StringByteArrayDictionary());
    }

    [Fact]
    public async Task DictionaryFetchAsync_HasContentByteArrayByteArray_HappyPath()
    {
        var dictionaryName = Utils.NewGuidString();
        var field1 = Utils.NewGuidByteArray();
        var value1 = Utils.NewGuidByteArray();
        var field2 = Utils.NewGuidByteArray();
        var value2 = Utils.NewGuidByteArray();
        var contentDictionary = new Dictionary<byte[], byte[]>() {
            {field1, value1},
            {field2, value2}
        };

        await client.DictionarySetAsync(cacheName, dictionaryName, field1, value1);
        await client.DictionarySetAsync(cacheName, dictionaryName, field2, value2);

        CacheDictionaryFetchResponse fetchResponse = await client.DictionaryFetchAsync(cacheName, dictionaryName);

        Assert.True(fetchResponse is CacheDictionaryFetchResponse.Hit, $"Unexpected response: {fetchResponse}");

        var hitResponse = (CacheDictionaryFetchResponse.Hit)fetchResponse;
        // Exercise byte array dictionary structural equality comparer
        Assert.True(hitResponse.ByteArrayByteArrayDictionary!.ContainsKey(field1));
        Assert.True(hitResponse.ByteArrayByteArrayDictionary!.ContainsKey(field2));
        Assert.Equal(2, hitResponse.ByteArrayByteArrayDictionary!.Count);

        // Exercise DictionaryEquals extension
        Assert.True(hitResponse.ByteArrayByteArrayDictionary!.DictionaryEquals(contentDictionary));

        // Test field caching behavior
        Assert.Same(hitResponse.ByteArrayByteArrayDictionary, hitResponse.ByteArrayByteArrayDictionary);
    }

    [Theory]
    [InlineData(null, "my-dictionary")]
    [InlineData("my-cache", null)]
    public async Task DictionaryDeleteAsync_NullChecks_IsError(string cacheName, string dictionaryName)
    {
        CacheDictionaryDeleteResponse response = await client.DictionaryDeleteAsync(cacheName, dictionaryName);
        Assert.True(response is CacheDictionaryDeleteResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryDeleteResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task DictionaryDeleteAsync_DictionaryDoesNotExist_Noop()
    {
        var dictionaryName = Utils.NewGuidString();
        Assert.True((await client.DictionaryFetchAsync(cacheName, dictionaryName)) is CacheDictionaryFetchResponse.Miss);
        await client.DictionaryDeleteAsync(cacheName, dictionaryName);
        Assert.True((await client.DictionaryFetchAsync(cacheName, dictionaryName)) is CacheDictionaryFetchResponse.Miss);
    }

    [Fact]
    public async Task DictionaryDeleteAsync_DictionaryExists_HappyPath()
    {
        var dictionaryName = Utils.NewGuidString();
        await client.DictionarySetAsync(cacheName, dictionaryName, Utils.NewGuidString(), Utils.NewGuidString());
        await client.DictionarySetAsync(cacheName, dictionaryName, Utils.NewGuidString(), Utils.NewGuidString());
        await client.DictionarySetAsync(cacheName, dictionaryName, Utils.NewGuidString(), Utils.NewGuidString());

        Assert.True((await client.DictionaryFetchAsync(cacheName, dictionaryName)) is CacheDictionaryFetchResponse.Hit);
        await client.DictionaryDeleteAsync(cacheName, dictionaryName);
        Assert.True((await client.DictionaryFetchAsync(cacheName, dictionaryName)) is CacheDictionaryFetchResponse.Miss);
    }

    [Theory]
    [InlineData(null, "my-dictionary", new byte[] { 0x00 })]
    [InlineData("my-cache", null, new byte[] { 0x00 })]
    [InlineData("my-cache", "my-dictionary", null)]
    public async Task DictionaryRemoveFieldAsync_NullChecksFieldIsByteArray_IsError(string cacheName, string dictionaryName, byte[] field)
    {
        CacheDictionaryRemoveFieldResponse response = await client.DictionaryRemoveFieldAsync(cacheName, dictionaryName, field);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldResponse.Error)response).ErrorCode);
    }

    [Theory]
    [InlineData(null, "my-dictionary", "my-field")]
    [InlineData("my-cache", null, "my-field")]
    [InlineData("my-cache", "my-dictionary", null)]
    public async Task DictionaryRemoveFieldAsync_NullChecksFieldIsString_IsError(string cacheName, string dictionaryName, string field)
    {
        CacheDictionaryRemoveFieldResponse response = await client.DictionaryRemoveFieldAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryRemoveFieldResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task DictionaryRemoveFieldAsync_FieldIsByteArray_HappyPath()
    {
        var dictionaryName = Utils.NewGuidString();
        var field1 = Utils.NewGuidByteArray();
        var value1 = Utils.NewGuidByteArray();
        var field2 = Utils.NewGuidByteArray();

        // Add a field then delete it
        Assert.True((await client.DictionaryGetAsync(cacheName, dictionaryName, field1)) is CacheDictionaryGetResponse.Miss);
        await client.DictionarySetAsync(cacheName, dictionaryName, field1, value1);
        Assert.True((await client.DictionaryGetAsync(cacheName, dictionaryName, field1)) is CacheDictionaryGetResponse.Hit);

        await client.DictionaryRemoveFieldAsync(cacheName, dictionaryName, field1);
        Assert.True((await client.DictionaryGetAsync(cacheName, dictionaryName, field1)) is CacheDictionaryGetResponse.Miss);

        // Test no-op
        Assert.True((await client.DictionaryGetAsync(cacheName, dictionaryName, field2)) is CacheDictionaryGetResponse.Miss);
        await client.DictionaryRemoveFieldAsync(cacheName, dictionaryName, field2);
        Assert.True((await client.DictionaryGetAsync(cacheName, dictionaryName, field2)) is CacheDictionaryGetResponse.Miss);
    }

    [Fact]
    public async Task DictionaryRemoveFieldAsync_FieldIsString_HappyPath()
    {
        var dictionaryName = Utils.NewGuidString();
        var field1 = Utils.NewGuidString();
        var value1 = Utils.NewGuidString();
        var field2 = Utils.NewGuidString();

        // Add a field then delete it
        Assert.True((await client.DictionaryGetAsync(cacheName, dictionaryName, field1)) is CacheDictionaryGetResponse.Miss);
        await client.DictionarySetAsync(cacheName, dictionaryName, field1, value1);
        Assert.True((await client.DictionaryGetAsync(cacheName, dictionaryName, field1)) is CacheDictionaryGetResponse.Hit);

        await client.DictionaryRemoveFieldAsync(cacheName, dictionaryName, field1);
        Assert.True((await client.DictionaryGetAsync(cacheName, dictionaryName, field1)) is CacheDictionaryGetResponse.Miss);

        // Test no-op
        Assert.True((await client.DictionaryGetAsync(cacheName, dictionaryName, field2)) is CacheDictionaryGetResponse.Miss);
        await client.DictionaryRemoveFieldAsync(cacheName, dictionaryName, field2);
        Assert.True((await client.DictionaryGetAsync(cacheName, dictionaryName, field2)) is CacheDictionaryGetResponse.Miss);
    }

    [Fact]
    public async Task DictionaryRemoveFieldsAsync_NullChecksFieldsAreByteArray_IsError()
    {
        var dictionaryName = Utils.NewGuidString();
        var testData = new byte[][][] { new byte[][] { Utils.NewGuidByteArray(), Utils.NewGuidByteArray() }, new byte[][] { Utils.NewGuidByteArray(), null! } };

        CacheDictionaryRemoveFieldsResponse response = await client.DictionaryRemoveFieldsAsync(null!, dictionaryName, testData[0]);
        Assert.True(response is CacheDictionaryRemoveFieldsResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldsResponse.Error)response).ErrorCode);
        response = await client.DictionaryRemoveFieldsAsync(cacheName, null!, testData[0]);
        Assert.True(response is CacheDictionaryRemoveFieldsResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldsResponse.Error)response).ErrorCode);
        response = await client.DictionaryRemoveFieldsAsync(cacheName, dictionaryName, (byte[][])null!);
        Assert.True(response is CacheDictionaryRemoveFieldsResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldsResponse.Error)response).ErrorCode);
        response = await client.DictionaryRemoveFieldsAsync(cacheName, dictionaryName, testData[1]);
        Assert.True(response is CacheDictionaryRemoveFieldsResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldsResponse.Error)response).ErrorCode);

        var fieldsList = new List<byte[]>(testData[0]);
        response = await client.DictionaryRemoveFieldsAsync(null!, dictionaryName, fieldsList);
        Assert.True(response is CacheDictionaryRemoveFieldsResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldsResponse.Error)response).ErrorCode);
        response = await client.DictionaryRemoveFieldsAsync(cacheName, null!, fieldsList);
        Assert.True(response is CacheDictionaryRemoveFieldsResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldsResponse.Error)response).ErrorCode);
        response = await client.DictionaryRemoveFieldsAsync(cacheName, dictionaryName, (List<byte[]>)null!);
        Assert.True(response is CacheDictionaryRemoveFieldsResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldsResponse.Error)response).ErrorCode);
        response = await client.DictionaryRemoveFieldsAsync(cacheName, dictionaryName, new List<byte[]>(testData[1]));
        Assert.True(response is CacheDictionaryRemoveFieldsResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldsResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task DictionaryRemoveFieldsAsync_FieldsAreByteArray_HappyPath()
    {
        var dictionaryName = Utils.NewGuidString();
        var fields = new byte[][] { Utils.NewGuidByteArray(), Utils.NewGuidByteArray() };
        var otherField = Utils.NewGuidByteArray();

        // Test enumerable
        await client.DictionarySetAsync(cacheName, dictionaryName, fields[0], Utils.NewGuidByteArray());
        await client.DictionarySetAsync(cacheName, dictionaryName, fields[1], Utils.NewGuidByteArray());
        await client.DictionarySetAsync(cacheName, dictionaryName, otherField, Utils.NewGuidByteArray());

        var fieldsList = new List<byte[]>(fields);
        await client.DictionaryRemoveFieldsAsync(cacheName, dictionaryName, fieldsList);
        Assert.True((await client.DictionaryGetAsync(cacheName, dictionaryName, fields[0])) is CacheDictionaryGetResponse.Miss);
        Assert.True((await client.DictionaryGetAsync(cacheName, dictionaryName, fields[1])) is CacheDictionaryGetResponse.Miss);
        Assert.True((await client.DictionaryGetAsync(cacheName, dictionaryName, otherField)) is CacheDictionaryGetResponse.Hit);
    }

    [Fact]
    public async Task DictionaryRemoveFieldsAsync_NullChecksFieldsAreString_IsError()
    {
        var dictionaryName = Utils.NewGuidString();
        var testData = new string[][] { new string[] { Utils.NewGuidString(), Utils.NewGuidString() }, new string[] { Utils.NewGuidString(), null! } };
        CacheDictionaryRemoveFieldsResponse response = await client.DictionaryRemoveFieldsAsync(null!, dictionaryName, testData[0]);
        Assert.True(response is CacheDictionaryRemoveFieldsResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldsResponse.Error)response).ErrorCode);
        response = await client.DictionaryRemoveFieldsAsync(cacheName, null!, testData[0]);
        Assert.True(response is CacheDictionaryRemoveFieldsResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldsResponse.Error)response).ErrorCode);
        response = await client.DictionaryRemoveFieldsAsync(cacheName, dictionaryName, (string[])null!);
        Assert.True(response is CacheDictionaryRemoveFieldsResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldsResponse.Error)response).ErrorCode);
        response = await client.DictionaryRemoveFieldsAsync(cacheName, dictionaryName, testData[1]);
        Assert.True(response is CacheDictionaryRemoveFieldsResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldsResponse.Error)response).ErrorCode);

        var fieldsList = new List<string>(testData[0]);
        response = await client.DictionaryRemoveFieldsAsync(null!, dictionaryName, fieldsList);
        Assert.True(response is CacheDictionaryRemoveFieldsResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldsResponse.Error)response).ErrorCode);
        response = await client.DictionaryRemoveFieldsAsync(cacheName, null!, fieldsList);
        Assert.True(response is CacheDictionaryRemoveFieldsResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldsResponse.Error)response).ErrorCode);
        response = await client.DictionaryRemoveFieldsAsync(cacheName, dictionaryName, (List<string>)null!);
        Assert.True(response is CacheDictionaryRemoveFieldsResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldsResponse.Error)response).ErrorCode);
        response = await client.DictionaryRemoveFieldsAsync(cacheName, dictionaryName, new List<string>(testData[1]));
        Assert.True(response is CacheDictionaryRemoveFieldsResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldsResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task DictionaryRemoveFieldsAsync_FieldsAreString_HappyPath()
    {
        var dictionaryName = Utils.NewGuidString();
        var fields = new string[] { Utils.NewGuidString(), Utils.NewGuidString() };
        var otherField = Utils.NewGuidString();

        // Test enumerable
        await client.DictionarySetAsync(cacheName, dictionaryName, fields[0], Utils.NewGuidString());
        await client.DictionarySetAsync(cacheName, dictionaryName, fields[1], Utils.NewGuidString());
        await client.DictionarySetAsync(cacheName, dictionaryName, otherField, Utils.NewGuidString());

        var fieldsList = new List<string>(fields);
        await client.DictionaryRemoveFieldsAsync(cacheName, dictionaryName, fieldsList);
        Assert.True((await client.DictionaryGetAsync(cacheName, dictionaryName, fields[0])) is CacheDictionaryGetResponse.Miss);
        Assert.True((await client.DictionaryGetAsync(cacheName, dictionaryName, fields[1])) is CacheDictionaryGetResponse.Miss);
        Assert.True((await client.DictionaryGetAsync(cacheName, dictionaryName, otherField)) is CacheDictionaryGetResponse.Hit);
    }
}
