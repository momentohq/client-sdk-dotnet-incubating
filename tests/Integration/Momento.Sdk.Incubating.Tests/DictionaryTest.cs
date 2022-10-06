using Momento.Sdk.Incubating.Responses;
using Momento.Sdk.Internal.ExtensionMethods;
using Momento.Sdk.Responses;

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
    public async Task DictionaryGetAsync_NullChecksFieldIsByteArray_ThrowsException(string cacheName, string dictionaryName, byte[] field)
    {
        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetResponse.Error)response).ErrorCode);
    }

    [Theory]
    [InlineData(null, "my-dictionary", new byte[] { 0x00 }, new byte[] { 0x00 })]
    [InlineData("cache", null, new byte[] { 0x00 }, new byte[] { 0x00 })]
    [InlineData("cache", "my-dictionary", null, new byte[] { 0x00 })]
    [InlineData("cache", "my-dictionary", new byte[] { 0x00 }, null)]
    public async Task DictionarySetAsync_NullChecksFieldIsByteArrayValueIsByteArray_ThrowsException(string cacheName, string dictionaryName, byte[] field, byte[] value)
    {
        CacheDictionarySetResponse response = await client.DictionarySetAsync(cacheName, dictionaryName, field, value, false);
        Assert.True(response is CacheDictionarySetResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionarySetResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task DictionaryGetAsync_FieldIsByteArray_DictionaryIsMissing()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidByteArray();
        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Miss);
    }

    [Fact]
    public async Task DictionarySetGetAsync_FieldIsByteArrayValueIsByteArray_HappyPath()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidByteArray();
        var value = Utils.NewGuidByteArray();

        await client.DictionarySetAsync(cacheName, dictionaryName, field, value, false);

        CacheDictionaryGetResponse getResponse = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(getResponse is CacheDictionaryGetResponse.Hit);
    }

    [Fact]
    public async Task DictionarySetGetAsync_FieldIsByteArrayDictionaryIsPresent_FieldIsMissing()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidByteArray();
        var value = Utils.NewGuidByteArray();

        CacheDictionarySetResponse setResponse = await client.DictionarySetAsync(cacheName, dictionaryName, field, value, false);
        Assert.True(setResponse is CacheDictionarySetResponse.Success);

        var otherField = Utils.NewGuidByteArray();
        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, otherField);
        Assert.True(response is CacheDictionaryGetResponse.Miss);
    }

    [Fact]
    public async Task DictionarySetGetAsync_FieldIsByteArrayValueIsByteArray_NoRefreshTtl()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidByteArray();
        var value = Utils.NewGuidByteArray();

        CacheDictionarySetResponse setResponse = await client.DictionarySetAsync(cacheName, dictionaryName, field, value, false, ttlSeconds: 5);
        Assert.True(setResponse is CacheDictionarySetResponse.Success);
        await Task.Delay(100);

        setResponse = await client.DictionarySetAsync(cacheName, dictionaryName, field, value, false, ttlSeconds: 10);
        Assert.True(setResponse is CacheDictionarySetResponse.Success);
        await Task.Delay(4900);

        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Miss);
    }

    [Fact]
    public async Task DictionarySetGetAsync_FieldIsByteArrayValueIsByteArray_RefreshTtl()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidByteArray();
        var value = Utils.NewGuidByteArray();

        CacheDictionarySetResponse setResponse = await client.DictionarySetAsync(cacheName, dictionaryName, field, value, false, ttlSeconds: 2);
        Assert.True(setResponse is CacheDictionarySetResponse.Success);
        setResponse = await client.DictionarySetAsync(cacheName, dictionaryName, field, value, true, ttlSeconds: 10);
        Assert.True(setResponse is CacheDictionarySetResponse.Success);
        await Task.Delay(2000);

        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Hit);
        Assert.Equal(value, ((CacheDictionaryGetResponse.Hit)response).ByteArray);
    }

    [Theory]
    [InlineData(null, "my-dictionary", "field")]
    [InlineData("cache", null, "field")]
    [InlineData("cache", "my-dictionary", null)]
    public async Task DictionaryIncrementAsync_NullChecksFieldIsString_ThrowsException(string cacheName, string dictionaryName, string field)
    {
        CacheDictionaryIncrementResponse response = await client.DictionaryIncrementAsync(cacheName, dictionaryName, field, refreshTtl: true);
        Assert.True(response is CacheDictionaryIncrementResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryIncrementResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task DictionaryIncrementAsync_IncrementFromZero_HappyPath()
    {
        var dictionaryName = Utils.NewGuidString();
        var fieldName = Utils.NewGuidString();

        CacheDictionaryIncrementResponse incrementResponse = await client.DictionaryIncrementAsync(cacheName, dictionaryName, fieldName, false, 1);
        Assert.True(incrementResponse is CacheDictionaryIncrementResponse.Success);
        var successResponse = (CacheDictionaryIncrementResponse.Success)incrementResponse;
        Assert.Equal(1, successResponse.Value);

        incrementResponse = await client.DictionaryIncrementAsync(cacheName, dictionaryName, fieldName, false, 41);
        Assert.True(incrementResponse is CacheDictionaryIncrementResponse.Success);
        successResponse = (CacheDictionaryIncrementResponse.Success)incrementResponse;
        Assert.Equal(42, successResponse.Value);

        incrementResponse = await client.DictionaryIncrementAsync(cacheName, dictionaryName, fieldName, false, -1042);
        Assert.True(incrementResponse is CacheDictionaryIncrementResponse.Success);
        successResponse = (CacheDictionaryIncrementResponse.Success)incrementResponse;
        Assert.Equal(-1000, successResponse.Value);

        CacheDictionaryGetResponse getResponse = await client.DictionaryGetAsync(cacheName, dictionaryName, fieldName);
        Assert.True(getResponse is CacheDictionaryGetResponse.Hit);
        var hitResponse = (CacheDictionaryGetResponse.Hit)getResponse;
        Assert.Equal("-1000", hitResponse.String());
    }

    [Fact]
    public async Task DictionaryIncrementAsync_IncrementFromZero_RefreshTtl()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidString();

        CacheDictionaryIncrementResponse resp = await client.DictionaryIncrementAsync(cacheName, dictionaryName, field, refreshTtl: false, ttlSeconds: 2);
        Assert.True(resp is CacheDictionaryIncrementResponse.Success);
        resp = await client.DictionaryIncrementAsync(cacheName, dictionaryName, field, refreshTtl: true, ttlSeconds: 10);
        Assert.True(resp is CacheDictionaryIncrementResponse.Success);
        await Task.Delay(2000);

        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Hit);
        Assert.Equal("2", ((CacheDictionaryGetResponse.Hit)response).String());
    }

    [Fact]
    public async Task DictionaryIncrementAsync_IncrementFromZero_NoRefreshTtl()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidString();

        CacheDictionaryIncrementResponse resp = await client.DictionaryIncrementAsync(cacheName, dictionaryName, field, refreshTtl: false, ttlSeconds: 5);
        Assert.True(resp is CacheDictionaryIncrementResponse.Success);
        await Task.Delay(100);

        resp = await client.DictionaryIncrementAsync(cacheName, dictionaryName, field, refreshTtl: false, ttlSeconds: 10);
        Assert.True(resp is CacheDictionaryIncrementResponse.Success);
        await Task.Delay(4900);

        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Miss);
    }

    [Fact]
    public async Task DictionaryIncrementAsync_SetAndReset_HappyPath()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidString();

        // Set field
        await client.DictionarySetAsync(cacheName, dictionaryName, field, "10", false);
        CacheDictionaryIncrementResponse incrementResponse = await client.DictionaryIncrementAsync(cacheName, dictionaryName, field, amount: 0, refreshTtl: false);
        Assert.True(incrementResponse is CacheDictionaryIncrementResponse.Success);
        var successResponse = (CacheDictionaryIncrementResponse.Success)incrementResponse;
        Assert.Equal(10, successResponse.Value);

        incrementResponse = await client.DictionaryIncrementAsync(cacheName, dictionaryName, field, amount: 90, refreshTtl: false);
        Assert.True(incrementResponse is CacheDictionaryIncrementResponse.Success);
        successResponse = (CacheDictionaryIncrementResponse.Success)incrementResponse;
        Assert.Equal(100, successResponse.Value);

        // Reset field
        await client.DictionarySetAsync(cacheName, dictionaryName, field, "0", false);
        incrementResponse = await client.DictionaryIncrementAsync(cacheName, dictionaryName, field, amount: 0, refreshTtl: false);
        Assert.True(incrementResponse is CacheDictionaryIncrementResponse.Success);
        successResponse = (CacheDictionaryIncrementResponse.Success)incrementResponse;
        Assert.Equal(0, successResponse.Value);
    }

    [Fact]
    public async Task DictionaryIncrementAsync_FailedPrecondition_ThrowsException()
    {
        var dictionaryName = Utils.NewGuidString();
        var fieldName = Utils.NewGuidString();

        await client.DictionarySetAsync(cacheName, dictionaryName, fieldName, "abcxyz", false);
        await Assert.ThrowsAsync<FailedPreconditionException>(async () => await client.DictionaryIncrementAsync(cacheName, dictionaryName, fieldName, amount: 1, refreshTtl: true));
    }

    [Theory]
    [InlineData(null, "my-dictionary", "my-field")]
    [InlineData("cache", null, "my-field")]
    [InlineData("cache", "my-dictionary", null)]
    public async Task DictionaryGetAsync_NullChecksFieldIsString_ThrowsException(string cacheName, string dictionaryName, string field)
    {
        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetResponse.Error)response).ErrorCode);
    }

    [Theory]
    [InlineData(null, "my-dictionary", "my-field", "my-value")]
    [InlineData("cache", null, "my-field", "my-value")]
    [InlineData("cache", "my-dictionary", null, "my-value")]
    [InlineData("cache", "my-dictionary", "my-field", null)]
    public async Task DictionarySetAsync_NullChecksFieldIsStringValueIsString_ThrowsException(string cacheName, string dictionaryName, string field, string value)
    {
        CacheDictionarySetResponse response = await client.DictionarySetAsync(cacheName, dictionaryName, field, value, false);
        Assert.True(response is CacheDictionarySetResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionarySetResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task DictionaryGetAsync_FieldIsString_DictionaryIsMissing()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidString();
        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Miss);
    }

    [Fact]
    public async Task DictionarySetGetAsync_FieldIsStringValueIsString_HappyPath()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidString();
        var value = Utils.NewGuidString();

        CacheDictionarySetResponse response = await client.DictionarySetAsync(cacheName, dictionaryName, field, value, false);
        Assert.True(response is CacheDictionarySetResponse.Success);

        CacheDictionaryGetResponse getResponse = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(getResponse is CacheDictionaryGetResponse.Hit);
    }

    [Fact]
    public async Task DictionarySetGetAsync_DictionaryIsPresent_FieldIsMissing()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidString();
        var value = Utils.NewGuidString();

        CacheDictionarySetResponse setResponse = await client.DictionarySetAsync(cacheName, dictionaryName, field, value, false);
        Assert.True(setResponse is CacheDictionarySetResponse.Success);

        var otherField = Utils.NewGuidString();
        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, otherField);
        Assert.True(response is CacheDictionaryGetResponse.Miss);
    }

    [Fact]
    public async Task DictionarySetGetAsync_FieldIsStringValueIsString_NoRefreshTtl()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidString();
        var value = Utils.NewGuidString();

        CacheDictionarySetResponse setResponse = await client.DictionarySetAsync(cacheName, dictionaryName, field, value, false, ttlSeconds: 5);
        Assert.True(setResponse is CacheDictionarySetResponse.Success);
        await Task.Delay(100);

        setResponse = await client.DictionarySetAsync(cacheName, dictionaryName, field, value, false, ttlSeconds: 10);
        Assert.True(setResponse is CacheDictionarySetResponse.Success);
        await Task.Delay(4900);

        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Miss);
    }

    [Fact]
    public async Task DictionarySetGetAsync_FieldIsStringValueIsString_RefreshTtl()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidString();
        var value = Utils.NewGuidString();

        CacheDictionarySetResponse setResponse = await client.DictionarySetAsync(cacheName, dictionaryName, field, value, false, ttlSeconds: 2);
        Assert.True(setResponse is CacheDictionarySetResponse.Success);
        setResponse = await client.DictionarySetAsync(cacheName, dictionaryName, field, value, true, ttlSeconds: 10);
        Assert.True(setResponse is CacheDictionarySetResponse.Success);
        await Task.Delay(2000);

        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Hit);
        Assert.Equal(value, ((CacheDictionaryGetResponse.Hit)response).String());
    }

    [Theory]
    [InlineData(null, "my-dictionary", "my-field", new byte[] { 0x00 })]
    [InlineData("cache", null, "my-field", new byte[] { 0x00 })]
    [InlineData("cache", "my-dictionary", null, new byte[] { 0x00 })]
    [InlineData("cache", "my-dictionary", "my-field", null)]
    public async Task DictionarySetAsync_NullChecksFieldIsStringValueIsByteArray_ThrowsException(string cacheName, string dictionaryName, string field, byte[] value)
    {
        CacheDictionarySetResponse response = await client.DictionarySetAsync(cacheName, dictionaryName, field, value, false);
        Assert.True(response is CacheDictionarySetResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionarySetResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task DictionarySetGetAsync_FieldIsStringValueIsByteArray_HappyPath()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidString();
        var value = Utils.NewGuidByteArray();

        CacheDictionarySetResponse setResponse = await client.DictionarySetAsync(cacheName, dictionaryName, field, value, false);
        Assert.True(setResponse is CacheDictionarySetResponse.Success);

        CacheDictionaryGetResponse getResponse = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(getResponse is CacheDictionaryGetResponse.Hit);
    }

    [Fact]
    public async Task DictionarySetGetAsync_FieldIsStringValueIsByteArray_NoRefreshTtl()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidString();
        var value = Utils.NewGuidByteArray();

        CacheDictionarySetResponse setResponse = await client.DictionarySetAsync(cacheName, dictionaryName, field, value, false, ttlSeconds: 5);
        Assert.True(setResponse is CacheDictionarySetResponse.Success);
        await Task.Delay(100);

        setResponse = await client.DictionarySetAsync(cacheName, dictionaryName, field, value, false, ttlSeconds: 10);
        Assert.True(setResponse is CacheDictionarySetResponse.Success);
        await Task.Delay(4900);

        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Miss);
    }

    [Fact]
    public async Task DictionarySetGetAsync_FieldIsStringValueIsByteArray_RefreshTtl()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidString();
        var value = Utils.NewGuidByteArray();

        CacheDictionarySetResponse setResponse = await client.DictionarySetAsync(cacheName, dictionaryName, field, value, false, ttlSeconds: 2);
        Assert.True(setResponse is CacheDictionarySetResponse.Success);
        setResponse = await client.DictionarySetAsync(cacheName, dictionaryName, field, value, true, ttlSeconds: 10);
        Assert.True(setResponse is CacheDictionarySetResponse.Success);
        await Task.Delay(2000);

        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Hit);
        Assert.Equal(value, ((CacheDictionaryGetResponse.Hit)response).ByteArray);
    }

    [Fact]
    public async Task DictionarySetBatchAsync_NullChecksFieldIsByteArrayValueIsByteArray_ThrowsException()
    {
        var dictionaryName = Utils.NewGuidString();
        var dictionary = new Dictionary<byte[], byte[]>();
        CacheDictionarySetBatchResponse response = await client.DictionarySetBatchAsync(null!, dictionaryName, dictionary, false);
        Assert.True(response is CacheDictionarySetBatchResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionarySetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionarySetBatchAsync(cacheName, null!, dictionary, false);
        Assert.True(response is CacheDictionarySetBatchResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionarySetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionarySetBatchAsync(cacheName, dictionaryName, (IEnumerable<KeyValuePair<byte[], byte[]>>)null!, false);
        Assert.True(response is CacheDictionarySetBatchResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionarySetBatchResponse.Error)response).ErrorCode);

        dictionary[Utils.NewGuidByteArray()] = null!;
        response = await client.DictionarySetBatchAsync(cacheName, dictionaryName, dictionary, false);
        Assert.True(response is CacheDictionarySetBatchResponse.Error);
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

        await client.DictionarySetBatchAsync(cacheName, dictionaryName, items, false, 10);

        CacheDictionaryGetResponse getResponse = await client.DictionaryGetAsync(cacheName, dictionaryName, field1);
        Assert.True(getResponse is CacheDictionaryGetResponse.Hit);
        Assert.Equal(value1, ((CacheDictionaryGetResponse.Hit)getResponse).ByteArray);

        getResponse = await client.DictionaryGetAsync(cacheName, dictionaryName, field2);
        Assert.True(getResponse is CacheDictionaryGetResponse.Hit);
        Assert.Equal(value2, ((CacheDictionaryGetResponse.Hit)getResponse).ByteArray);
    }

    [Fact]
    public async Task DictionarySetBatchAsync_FieldsAreByteArrayValuesAreByteArray_NoRefreshTtl()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidByteArray();
        var value = Utils.NewGuidByteArray();
        var content = new Dictionary<byte[], byte[]>() { { field, value } };

        await client.DictionarySetBatchAsync(cacheName, dictionaryName, content, false, ttlSeconds: 5);
        await Task.Delay(100);

        await client.DictionarySetBatchAsync(cacheName, dictionaryName, content, false, ttlSeconds: 10);
        await Task.Delay(4900);

        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Miss);
    }

    [Fact]
    public async Task DictionarySetBatchAsync_FieldsAreByteArrayValuesAreByteArray_RefreshTtl()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidByteArray();
        var value = Utils.NewGuidByteArray();
        var content = new Dictionary<byte[], byte[]>() { { field, value } };

        await client.DictionarySetBatchAsync(cacheName, dictionaryName, content, false, ttlSeconds: 2);
        await client.DictionarySetBatchAsync(cacheName, dictionaryName, content, true, ttlSeconds: 10);
        await Task.Delay(2000);

        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Hit);
        Assert.Equal(value, ((CacheDictionaryGetResponse.Hit)response).ByteArray);
    }

    [Fact]
    public async Task DictionarySetBatchAsync_NullChecksFieldsAreStringValuesAreString_ThrowsException()
    {
        var dictionaryName = Utils.NewGuidString();
        var dictionary = new Dictionary<string, string>();
        CacheDictionarySetBatchResponse response = await client.DictionarySetBatchAsync(null!, dictionaryName, dictionary, false);
        Assert.True(response is CacheDictionarySetBatchResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionarySetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionarySetBatchAsync(cacheName, null!, dictionary, false);
        Assert.True(response is CacheDictionarySetBatchResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionarySetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionarySetBatchAsync(cacheName, dictionaryName, (IEnumerable<KeyValuePair<string, string>>)null!, false);
        Assert.True(response is CacheDictionarySetBatchResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionarySetBatchResponse.Error)response).ErrorCode);

        dictionary[Utils.NewGuidString()] = null!;
        response = await client.DictionarySetBatchAsync(cacheName, dictionaryName, dictionary, false);
        Assert.True(response is CacheDictionarySetBatchResponse.Error);
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

        await client.DictionarySetBatchAsync(cacheName, dictionaryName, items, false, 10);

        CacheDictionaryGetResponse getResponse = await client.DictionaryGetAsync(cacheName, dictionaryName, field1);
        Assert.True(getResponse is CacheDictionaryGetResponse.Hit);
        Assert.Equal(value1, ((CacheDictionaryGetResponse.Hit)getResponse).String());

        getResponse = await client.DictionaryGetAsync(cacheName, dictionaryName, field2);
        Assert.True(getResponse is CacheDictionaryGetResponse.Hit);
        Assert.Equal(value2, ((CacheDictionaryGetResponse.Hit)getResponse).String());
    }

    [Fact]
    public async Task DictionarySetBatchAsync_FieldsAreStringValuesAreString_NoRefreshTtl()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidString();
        var value = Utils.NewGuidString();
        var content = new Dictionary<string, string>() { { field, value } };

        await client.DictionarySetBatchAsync(cacheName, dictionaryName, content, false, ttlSeconds: 5);
        await Task.Delay(100);

        await client.DictionarySetBatchAsync(cacheName, dictionaryName, content, false, ttlSeconds: 10);
        await Task.Delay(4900);

        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Miss);
    }

    [Fact]
    public async Task DictionarySetBatchAsync_FieldsAreStringValuesAreString_RefreshTtl()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidString();
        var value = Utils.NewGuidString();
        var content = new Dictionary<string, string>() { { field, value } };

        await client.DictionarySetBatchAsync(cacheName, dictionaryName, content, false, ttlSeconds: 2);
        await client.DictionarySetBatchAsync(cacheName, dictionaryName, content, true, ttlSeconds: 10);
        await Task.Delay(2000);

        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Hit);
        Assert.Equal(value, ((CacheDictionaryGetResponse.Hit)response).String());
    }

    [Fact]
    public async Task DictionarySetBatchAsync_NullChecksFieldsAreStringValuesAreByteArray_ThrowsException()
    {
        var dictionaryName = Utils.NewGuidString();
        var dictionary = new Dictionary<string, string>();
        CacheDictionarySetBatchResponse response = await client.DictionarySetBatchAsync(null!, dictionaryName, dictionary, false);
        Assert.True(response is CacheDictionarySetBatchResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionarySetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionarySetBatchAsync(cacheName, null!, dictionary, false);
        Assert.True(response is CacheDictionarySetBatchResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionarySetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionarySetBatchAsync(cacheName, dictionaryName, (IEnumerable<KeyValuePair<string, byte[]>>)null!, false);
        Assert.True(response is CacheDictionarySetBatchResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionarySetBatchResponse.Error)response).ErrorCode);

        dictionary[Utils.NewGuidString()] = null!;
        response = await client.DictionarySetBatchAsync(cacheName, dictionaryName, dictionary, false);
        Assert.True(response is CacheDictionarySetBatchResponse.Error);
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

        await client.DictionarySetBatchAsync(cacheName, dictionaryName, items, false, 10);

        CacheDictionaryGetResponse getResponse = await client.DictionaryGetAsync(cacheName, dictionaryName, field1);
        Assert.True(getResponse is CacheDictionaryGetResponse.Hit);
        Assert.Equal(value1, ((CacheDictionaryGetResponse.Hit)getResponse).ByteArray);

        getResponse = await client.DictionaryGetAsync(cacheName, dictionaryName, field2);
        Assert.True(getResponse is CacheDictionaryGetResponse.Hit);
        Assert.Equal(value2, ((CacheDictionaryGetResponse.Hit)getResponse).ByteArray);
    }

    [Fact]
    public async Task DictionarySetBatchAsync_FieldsAreStringValuesAreByteArray_NoRefreshTtl()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidString();
        var value = Utils.NewGuidByteArray();
        var content = new Dictionary<string, byte[]>() { { field, value } };

        await client.DictionarySetBatchAsync(cacheName, dictionaryName, content, false, ttlSeconds: 5);
        await Task.Delay(100);

        await client.DictionarySetBatchAsync(cacheName, dictionaryName, content, false, ttlSeconds: 10);
        await Task.Delay(4900);

        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Miss);
    }

    [Fact]
    public async Task DictionarySetBatchAsync_FieldsAreStringValuesAreByteArray_RefreshTtl()
    {
        var dictionaryName = Utils.NewGuidString();
        var field = Utils.NewGuidString();
        var value = Utils.NewGuidByteArray();
        var content = new Dictionary<string, byte[]>() { { field, value } };

        await client.DictionarySetBatchAsync(cacheName, dictionaryName, content, false, ttlSeconds: 2);
        await client.DictionarySetBatchAsync(cacheName, dictionaryName, content, true, ttlSeconds: 10);
        await Task.Delay(2000);

        CacheDictionaryGetResponse response = await client.DictionaryGetAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryGetResponse.Hit);
        Assert.Equal(value, ((CacheDictionaryGetResponse.Hit)response).ByteArray);
    }

    [Fact]
    public async Task DictionaryGetBatchAsync_NullChecksFieldsAreByteArray_ThrowsException()
    {
        var dictionaryName = Utils.NewGuidString();
        var testData = new byte[][][] { new byte[][] { Utils.NewGuidByteArray(), Utils.NewGuidByteArray() }, new byte[][] { Utils.NewGuidByteArray(), null! } };

        CacheDictionaryGetBatchResponse response = await client.DictionaryGetBatchAsync(null!, dictionaryName, testData[0]);
        Assert.True(response is CacheDictionaryGetBatchResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionaryGetBatchAsync(cacheName, null!, testData[0]);
        Assert.True(response is CacheDictionaryGetBatchResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionaryGetBatchAsync(cacheName, dictionaryName, (byte[][])null!);
        Assert.True(response is CacheDictionaryGetBatchResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionaryGetBatchAsync(cacheName, dictionaryName, testData[1]);
        Assert.True(response is CacheDictionaryGetBatchResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetBatchResponse.Error)response).ErrorCode);

        var fieldsList = new List<byte[]>(testData[0]);
        response = await client.DictionaryGetBatchAsync(null!, dictionaryName, fieldsList);
        Assert.True(response is CacheDictionaryGetBatchResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionaryGetBatchAsync(cacheName, null!, fieldsList);
        Assert.True(response is CacheDictionaryGetBatchResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionaryGetBatchAsync(cacheName, dictionaryName, (List<byte[]>)null!);
        Assert.True(response is CacheDictionaryGetBatchResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionaryGetBatchAsync(cacheName, dictionaryName, new List<byte[]>(testData[1]));
        Assert.True(response is CacheDictionaryGetBatchResponse.Error);
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

        await client.DictionarySetAsync(cacheName, dictionaryName, field1, value1, false, 10);
        await client.DictionarySetAsync(cacheName, dictionaryName, field2, value2, false, 10);

        CacheDictionaryGetBatchResponse response = await client.DictionaryGetBatchAsync(cacheName, dictionaryName, new byte[][] { field1, field2, field3 });
        Assert.True(response is CacheDictionaryGetBatchResponse.Success);

        var values = new byte[]?[] { value1, value2, null };
        Assert.Equal(values, ((CacheDictionaryGetBatchResponse.Success)response).ByteArrays);
    }

    [Fact]
    public async Task DictionaryGetBatchAsync_DictionaryMissing_HappyPath()
    {
        var dictionaryName = Utils.NewGuidString();
        var field1 = Utils.NewGuidByteArray();
        var field2 = Utils.NewGuidByteArray();
        var field3 = Utils.NewGuidByteArray();

        CacheDictionaryGetBatchResponse response = await client.DictionaryGetBatchAsync(cacheName, dictionaryName, new byte[][] { field1, field2, field3 });
        Assert.True(response is CacheDictionaryGetBatchResponse.Success);
        var nullResponse = (CacheDictionaryGetBatchResponse.Success)response;
        var byteArrays = new byte[]?[] { null, null, null };
        var strings = new string?[] { null, null, null };

        Assert.Equal(byteArrays, nullResponse.ByteArrays);
        Assert.Equal(strings, nullResponse.Strings()!);
    }

    [Fact]
    public async Task DictionaryGetBatchAsync_NullChecksFieldsAreString_ThrowsException()
    {
        var dictionaryName = Utils.NewGuidString();
        var testData = new string[][] { new string[] { Utils.NewGuidString(), Utils.NewGuidString() }, new string[] { Utils.NewGuidString(), null! } };
        CacheDictionaryGetBatchResponse response = await client.DictionaryGetBatchAsync(null!, dictionaryName, testData[0]);
        Assert.True(response is CacheDictionaryGetBatchResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionaryGetBatchAsync(cacheName, null!, testData[0]);
        Assert.True(response is CacheDictionaryGetBatchResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionaryGetBatchAsync(cacheName, dictionaryName, (string[])null!);
        Assert.True(response is CacheDictionaryGetBatchResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionaryGetBatchAsync(cacheName, dictionaryName, testData[1]);
        Assert.True(response is CacheDictionaryGetBatchResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetBatchResponse.Error)response).ErrorCode);

        var fieldsList = new List<string>(testData[0]);
        response = await client.DictionaryGetBatchAsync(null!, dictionaryName, fieldsList);
        Assert.True(response is CacheDictionaryGetBatchResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionaryGetBatchAsync(cacheName, null!, fieldsList);
        Assert.True(response is CacheDictionaryGetBatchResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionaryGetBatchAsync(cacheName, dictionaryName, (List<string>)null!);
        Assert.True(response is CacheDictionaryGetBatchResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryGetBatchResponse.Error)response).ErrorCode);
        response = await client.DictionaryGetBatchAsync(cacheName, dictionaryName, new List<string>(testData[1]));
        Assert.True(response is CacheDictionaryGetBatchResponse.Error);
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

        await client.DictionarySetAsync(cacheName, dictionaryName, field1, value1, false, 10);
        await client.DictionarySetAsync(cacheName, dictionaryName, field2, value2, false, 10);

        CacheDictionaryGetBatchResponse response = await client.DictionaryGetBatchAsync(cacheName, dictionaryName, new string[] { field1, field2, field3 });
        Assert.True(response is CacheDictionaryGetBatchResponse.Success);
        var values = new string?[] { value1, value2, null };
        Assert.Equal(values, ((CacheDictionaryGetBatchResponse.Success)response).Strings());
    }

    [Theory]
    [InlineData(null, "my-dictionary")]
    [InlineData("cache", null)]
    public async Task DictionaryFetchAsync_NullChecks_ThrowsException(string cacheName, string dictionaryName)
    {
        CacheDictionaryFetchResponse response = await client.DictionaryFetchAsync(cacheName, dictionaryName);
        Assert.True(response is CacheDictionaryFetchResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryFetchResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task DictionaryFetchAsync_Missing_HappyPath()
    {
        var dictionaryName = Utils.NewGuidString();
        CacheDictionaryFetchResponse response = await client.DictionaryFetchAsync(cacheName, dictionaryName);
        Assert.True(response is CacheDictionaryFetchResponse.Miss);
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

        await client.DictionarySetAsync(cacheName, dictionaryName, field1, value1, true, ttlSeconds: 10);
        await client.DictionarySetAsync(cacheName, dictionaryName, field2, value2, true, ttlSeconds: 10);

        CacheDictionaryFetchResponse fetchResponse = await client.DictionaryFetchAsync(cacheName, dictionaryName);

        Assert.True(fetchResponse is CacheDictionaryFetchResponse.Hit);
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

        await client.DictionarySetAsync(cacheName, dictionaryName, field1, value1, true, ttlSeconds: 10);
        await client.DictionarySetAsync(cacheName, dictionaryName, field2, value2, true, ttlSeconds: 10);

        CacheDictionaryFetchResponse fetchResponse = await client.DictionaryFetchAsync(cacheName, dictionaryName);

        Assert.True(fetchResponse is CacheDictionaryFetchResponse.Hit);
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

        await client.DictionarySetAsync(cacheName, dictionaryName, field1, value1, true, ttlSeconds: 10);
        await client.DictionarySetAsync(cacheName, dictionaryName, field2, value2, true, ttlSeconds: 10);

        CacheDictionaryFetchResponse fetchResponse = await client.DictionaryFetchAsync(cacheName, dictionaryName);

        Assert.True(fetchResponse is CacheDictionaryFetchResponse.Hit);

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
    public async Task DictionaryDeleteAsync_NullChecks_ThrowsException(string cacheName, string dictionaryName)
    {
        CacheDictionaryDeleteResponse response = await client.DictionaryDeleteAsync(cacheName, dictionaryName);
        Assert.True(response is CacheDictionaryDeleteResponse.Error);
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
        await client.DictionarySetAsync(cacheName, dictionaryName, Utils.NewGuidString(), Utils.NewGuidString(), false);
        await client.DictionarySetAsync(cacheName, dictionaryName, Utils.NewGuidString(), Utils.NewGuidString(), false);
        await client.DictionarySetAsync(cacheName, dictionaryName, Utils.NewGuidString(), Utils.NewGuidString(), false);

        Assert.True((await client.DictionaryFetchAsync(cacheName, dictionaryName)) is CacheDictionaryFetchResponse.Hit);
        await client.DictionaryDeleteAsync(cacheName, dictionaryName);
        Assert.True((await client.DictionaryFetchAsync(cacheName, dictionaryName)) is CacheDictionaryFetchResponse.Miss);
    }

    [Theory]
    [InlineData(null, "my-dictionary", new byte[] { 0x00 })]
    [InlineData("my-cache", null, new byte[] { 0x00 })]
    [InlineData("my-cache", "my-dictionary", null)]
    public async Task DictionaryRemoveFieldAsync_NullChecksFieldIsByteArray_ThrowsException(string cacheName, string dictionaryName, byte[] field)
    {
        CacheDictionaryRemoveFieldResponse response = await client.DictionaryRemoveFieldAsync(cacheName, dictionaryName, field);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldResponse.Error)response).ErrorCode);
    }

    [Theory]
    [InlineData(null, "my-dictionary", "my-field")]
    [InlineData("my-cache", null, "my-field")]
    [InlineData("my-cache", "my-dictionary", null)]
    public async Task DictionaryRemoveFieldAsync_NullChecksFieldIsString_ThrowsException(string cacheName, string dictionaryName, string field)
    {
        CacheDictionaryRemoveFieldResponse response = await client.DictionaryRemoveFieldAsync(cacheName, dictionaryName, field);
        Assert.True(response is CacheDictionaryRemoveFieldResponse.Error);
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
        await client.DictionarySetAsync(cacheName, dictionaryName, field1, value1, false);
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
        await client.DictionarySetAsync(cacheName, dictionaryName, field1, value1, false);
        Assert.True((await client.DictionaryGetAsync(cacheName, dictionaryName, field1)) is CacheDictionaryGetResponse.Hit);

        await client.DictionaryRemoveFieldAsync(cacheName, dictionaryName, field1);
        Assert.True((await client.DictionaryGetAsync(cacheName, dictionaryName, field1)) is CacheDictionaryGetResponse.Miss);

        // Test no-op
        Assert.True((await client.DictionaryGetAsync(cacheName, dictionaryName, field2)) is CacheDictionaryGetResponse.Miss);
        await client.DictionaryRemoveFieldAsync(cacheName, dictionaryName, field2);
        Assert.True((await client.DictionaryGetAsync(cacheName, dictionaryName, field2)) is CacheDictionaryGetResponse.Miss);
    }

    [Fact]
    public async Task DictionaryRemoveFieldsAsync_NullChecksFieldsAreByteArray_ThrowsException()
    {
        var dictionaryName = Utils.NewGuidString();
        var testData = new byte[][][] { new byte[][] { Utils.NewGuidByteArray(), Utils.NewGuidByteArray() }, new byte[][] { Utils.NewGuidByteArray(), null! } };

        CacheDictionaryRemoveFieldsResponse response = await client.DictionaryRemoveFieldsAsync(null!, dictionaryName, testData[0]);
        Assert.True(response is CacheDictionaryRemoveFieldsResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldsResponse.Error)response).ErrorCode);
        response = await client.DictionaryRemoveFieldsAsync(cacheName, null!, testData[0]);
        Assert.True(response is CacheDictionaryRemoveFieldsResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldsResponse.Error)response).ErrorCode);
        response = await client.DictionaryRemoveFieldsAsync(cacheName, dictionaryName, (byte[][])null!);
        Assert.True(response is CacheDictionaryRemoveFieldsResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldsResponse.Error)response).ErrorCode);
        response = await client.DictionaryRemoveFieldsAsync(cacheName, dictionaryName, testData[1]);
        Assert.True(response is CacheDictionaryRemoveFieldsResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldsResponse.Error)response).ErrorCode);

        var fieldsList = new List<byte[]>(testData[0]);
        response = await client.DictionaryRemoveFieldsAsync(null!, dictionaryName, fieldsList);
        Assert.True(response is CacheDictionaryRemoveFieldsResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldsResponse.Error)response).ErrorCode);
        response = await client.DictionaryRemoveFieldsAsync(cacheName, null!, fieldsList);
        Assert.True(response is CacheDictionaryRemoveFieldsResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldsResponse.Error)response).ErrorCode);
        response = await client.DictionaryRemoveFieldsAsync(cacheName, dictionaryName, (List<byte[]>)null!);
        Assert.True(response is CacheDictionaryRemoveFieldsResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldsResponse.Error)response).ErrorCode);
        response = await client.DictionaryRemoveFieldsAsync(cacheName, dictionaryName, new List<byte[]>(testData[1]));
        Assert.True(response is CacheDictionaryRemoveFieldsResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldsResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task DictionaryRemoveFieldsAsync_FieldsAreByteArray_HappyPath()
    {
        var dictionaryName = Utils.NewGuidString();
        var fields = new byte[][] { Utils.NewGuidByteArray(), Utils.NewGuidByteArray() };
        var otherField = Utils.NewGuidByteArray();

        // Test enumerable
        await client.DictionarySetAsync(cacheName, dictionaryName, fields[0], Utils.NewGuidByteArray(), false);
        await client.DictionarySetAsync(cacheName, dictionaryName, fields[1], Utils.NewGuidByteArray(), false);
        await client.DictionarySetAsync(cacheName, dictionaryName, otherField, Utils.NewGuidByteArray(), false);

        var fieldsList = new List<byte[]>(fields);
        await client.DictionaryRemoveFieldsAsync(cacheName, dictionaryName, fieldsList);
        Assert.True((await client.DictionaryGetAsync(cacheName, dictionaryName, fields[0])) is CacheDictionaryGetResponse.Miss);
        Assert.True((await client.DictionaryGetAsync(cacheName, dictionaryName, fields[1])) is CacheDictionaryGetResponse.Miss);
        Assert.True((await client.DictionaryGetAsync(cacheName, dictionaryName, otherField)) is CacheDictionaryGetResponse.Hit);
    }

    [Fact]
    public async Task DictionaryRemoveFieldsAsync_NullChecksFieldsAreString_ThrowsException()
    {
        var dictionaryName = Utils.NewGuidString();
        var testData = new string[][] { new string[] { Utils.NewGuidString(), Utils.NewGuidString() }, new string[] { Utils.NewGuidString(), null! } };
        CacheDictionaryRemoveFieldsResponse response = await client.DictionaryRemoveFieldsAsync(null!, dictionaryName, testData[0]);
        Assert.True(response is CacheDictionaryRemoveFieldsResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldsResponse.Error)response).ErrorCode);
        response = await client.DictionaryRemoveFieldsAsync(cacheName, null!, testData[0]);
        Assert.True(response is CacheDictionaryRemoveFieldsResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldsResponse.Error)response).ErrorCode);
        response = await client.DictionaryRemoveFieldsAsync(cacheName, dictionaryName, (string[])null!);
        Assert.True(response is CacheDictionaryRemoveFieldsResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldsResponse.Error)response).ErrorCode);
        response = await client.DictionaryRemoveFieldsAsync(cacheName, dictionaryName, testData[1]);
        Assert.True(response is CacheDictionaryRemoveFieldsResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldsResponse.Error)response).ErrorCode);

        var fieldsList = new List<string>(testData[0]);
        response = await client.DictionaryRemoveFieldsAsync(null!, dictionaryName, fieldsList);
        Assert.True(response is CacheDictionaryRemoveFieldsResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldsResponse.Error)response).ErrorCode);
        response = await client.DictionaryRemoveFieldsAsync(cacheName, null!, fieldsList);
        Assert.True(response is CacheDictionaryRemoveFieldsResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldsResponse.Error)response).ErrorCode);
        response = await client.DictionaryRemoveFieldsAsync(cacheName, dictionaryName, (List<string>)null!);
        Assert.True(response is CacheDictionaryRemoveFieldsResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldsResponse.Error)response).ErrorCode);
        response = await client.DictionaryRemoveFieldsAsync(cacheName, dictionaryName, new List<string>(testData[1]));
        Assert.True(response is CacheDictionaryRemoveFieldsResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheDictionaryRemoveFieldsResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task DictionaryRemoveFieldsAsync_FieldsAreString_HappyPath()
    {
        var dictionaryName = Utils.NewGuidString();
        var fields = new string[] { Utils.NewGuidString(), Utils.NewGuidString() };
        var otherField = Utils.NewGuidString();

        // Test enumerable
        await client.DictionarySetAsync(cacheName, dictionaryName, fields[0], Utils.NewGuidString(), false);
        await client.DictionarySetAsync(cacheName, dictionaryName, fields[1], Utils.NewGuidString(), false);
        await client.DictionarySetAsync(cacheName, dictionaryName, otherField, Utils.NewGuidString(), false);

        var fieldsList = new List<string>(fields);
        await client.DictionaryRemoveFieldsAsync(cacheName, dictionaryName, fieldsList);
        Assert.True((await client.DictionaryGetAsync(cacheName, dictionaryName, fields[0])) is CacheDictionaryGetResponse.Miss);
        Assert.True((await client.DictionaryGetAsync(cacheName, dictionaryName, fields[1])) is CacheDictionaryGetResponse.Miss);
        Assert.True((await client.DictionaryGetAsync(cacheName, dictionaryName, otherField)) is CacheDictionaryGetResponse.Hit);
    }
}
