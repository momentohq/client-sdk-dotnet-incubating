namespace Momento.Sdk.Incubating.Tests;

using System;
using Momento.Sdk.Config;
using Momento.Sdk.Incubating.Responses;
using Momento.Sdk.Responses;

[Collection("SimpleCacheClient")]
public class BatchTests : TestBase
{
    public BatchTests(SimpleCacheClientFixture fixture) : base(fixture)
    {
    }

    [Fact]
    public async Task GetBatchAsync_NullCheckByteArray_IsError()
    {
        CacheGetBatchResponse response = await client.GetBatchAsync(null!, new List<byte[]>());
        Assert.True(response is CacheGetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheGetBatchResponse.Error)response).ErrorCode);
        response = await client.GetBatchAsync("cache", (List<byte[]>)null!);
        Assert.True(response is CacheGetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheGetBatchResponse.Error)response).ErrorCode);
        var badList = new List<byte[]>(new byte[][] { Utils.NewGuidByteArray(), null! });
        response = await client.GetBatchAsync("cache", badList);
        Assert.True(response is CacheGetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheGetBatchResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task GetBatchAsync_KeysAreByteArray_HappyPath()
    {
        string key1 = Utils.NewGuidString();
        string value1 = Utils.NewGuidString();
        string key2 = Utils.NewGuidString();
        string value2 = Utils.NewGuidString();
        var setResponse = await client.SetAsync(cacheName, key1, value1);
        Assert.True(setResponse is CacheSetResponse.Success, $"Unexpected response: {setResponse}");
        setResponse = await client.SetAsync(cacheName, key2, value2);
        Assert.True(setResponse is CacheSetResponse.Success, $"Unexpected response: {setResponse}");

        List<byte[]> keys = new() { Utils.Utf8ToByteArray(key1), Utils.Utf8ToByteArray(key2) };

        CacheGetBatchResponse result = await client.GetBatchAsync(cacheName, keys);
        Assert.True(result is CacheGetBatchResponse.Success, $"Unexpected response: {result}");
        var goodResult = (CacheGetBatchResponse.Success)result;
        string? stringResult1 = goodResult.ValueStrings.ToList()[0];
        string? stringResult2 = goodResult.ValueStrings.ToList()[1];
        Assert.Equal(value1, stringResult1);
        Assert.Equal(value2, stringResult2);
    }

    [Fact]
    public async Task GetBatchAsync_NullCheckString_IsError()
    {
        CacheGetBatchResponse response = await client.GetBatchAsync(null!, new List<string>());
        Assert.True(response is CacheGetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheGetBatchResponse.Error)response).ErrorCode);
        response = await client.GetBatchAsync("cache", (List<string>)null!);
        Assert.True(response is CacheGetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheGetBatchResponse.Error)response).ErrorCode);

        List<string> strings = new(new string[] { "key1", "key2", null! });
        response = await client.GetBatchAsync("cache", strings);
        Assert.True(response is CacheGetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheGetBatchResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task GetBatchAsync_KeysAreString_HappyPath()
    {
        string key1 = Utils.NewGuidString();
        string value1 = Utils.NewGuidString();
        string key2 = Utils.NewGuidString();
        string value2 = Utils.NewGuidString();
        var setResponse = await client.SetAsync(cacheName, key1, value1);
        Assert.True(setResponse is CacheSetResponse.Success, $"Unexpected response: {setResponse}");
        setResponse = await client.SetAsync(cacheName, key2, value2);
        Assert.True(setResponse is CacheSetResponse.Success, $"Unexpected response: {setResponse}");

        List<string> keys = new() { key1, key2, "key123123" };
        CacheGetBatchResponse result = await client.GetBatchAsync(cacheName, keys);
        Assert.True(result is CacheGetBatchResponse.Success, $"Unexpected response: {result}");
        var goodResult = (CacheGetBatchResponse.Success)result;
        Assert.Equal(goodResult.ValueStrings, new string[] { value1, value2, null! });
        Assert.True(goodResult.Responses[0] is CacheGetResponse.Hit);
        Assert.True(goodResult.Responses[1] is CacheGetResponse.Hit);
        Assert.True(goodResult.Responses[2] is CacheGetResponse.Miss);
    }

    [Fact]
    public async Task GetBatchAsync_Failure()
    {
        // Set very small timeout for dataClientOperationTimeout
        IConfiguration config = Configurations.Laptop.Latest();
        config = config.WithTransportStrategy(
            config.TransportStrategy.WithGrpcConfig(
                config.TransportStrategy.GrpcConfig.WithDeadline(TimeSpan.FromMilliseconds(1))));

        using SimpleCacheClient simpleCacheClient = SimpleCacheClientFactory.CreateClient(config, this.authProvider, defaultTtl);
        List<string> keys = new() { Utils.NewGuidString(), Utils.NewGuidString(), Utils.NewGuidString(), Utils.NewGuidString() };
        CacheGetBatchResponse response = await simpleCacheClient.GetBatchAsync(cacheName, keys);
        Assert.True(response is CacheGetBatchResponse.Error, $"Unexpected response: {response}");
        var badResponse = (CacheGetBatchResponse.Error)response;
        Assert.Equal(MomentoErrorCode.TIMEOUT_ERROR, badResponse.ErrorCode);
    }

    [Fact]
    public async Task SetBatchAsync_NullCheckByteArray_IsError()
    {
        CacheSetBatchResponse response = await client.SetBatchAsync(null!, new Dictionary<byte[], byte[]>());
        Assert.True(response is CacheSetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetBatchResponse.Error)response).ErrorCode);
        response = await client.SetBatchAsync("cache", (Dictionary<byte[], byte[]>)null!);
        Assert.True(response is CacheSetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetBatchResponse.Error)response).ErrorCode);

        var badDictionary = new Dictionary<byte[], byte[]>() { { Utils.Utf8ToByteArray("asdf"), null! } };
        response = await client.SetBatchAsync("cache", badDictionary);
        Assert.True(response is CacheSetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetBatchResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task SetBatchAsync_ItemsAreByteArray_HappyPath()
    {
        var key1 = Utils.NewGuidByteArray();
        var key2 = Utils.NewGuidByteArray();
        var value1 = Utils.NewGuidByteArray();
        var value2 = Utils.NewGuidByteArray();

        var dictionary = new Dictionary<byte[], byte[]>() {
                { key1, value1 },
                { key2, value2 }
            };
        await client.SetBatchAsync(cacheName, dictionary);

        var getResponse = await client.GetAsync(cacheName, key1);
        Assert.True(getResponse is CacheGetResponse.Hit, $"Unexpected response: {getResponse}");
        var goodGetResponse = (CacheGetResponse.Hit)getResponse;
        Assert.Equal(value1, goodGetResponse.ValueByteArray);

        getResponse = await client.GetAsync(cacheName, key2);
        Assert.True(getResponse is CacheGetResponse.Hit, $"Unexpected response: {getResponse}");
        goodGetResponse = (CacheGetResponse.Hit)getResponse;
        Assert.Equal(value2, goodGetResponse.ValueByteArray);
    }

    [Fact]
    public async Task SetBatchAsync_NullCheckStrings_IsError()
    {
        CacheSetBatchResponse response = await client.SetBatchAsync(null!, new Dictionary<string, string>());
        Assert.True(response is CacheSetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetBatchResponse.Error)response).ErrorCode);
        response = await client.SetBatchAsync("cache", (Dictionary<string, string>)null!);
        Assert.True(response is CacheSetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetBatchResponse.Error)response).ErrorCode);

        var badDictionary = new Dictionary<string, string>() { { "asdf", null! } };
        response = await client.SetBatchAsync("cache", badDictionary);
        Assert.True(response is CacheSetBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetBatchResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task SetBatchAsync_KeysAreString_HappyPath()
    {
        var key1 = Utils.NewGuidString();
        var key2 = Utils.NewGuidString();
        var value1 = Utils.NewGuidString();
        var value2 = Utils.NewGuidString();

        var dictionary = new Dictionary<string, string>() {
                { key1, value1 },
                { key2, value2 }
            };
        await client.SetBatchAsync(cacheName, dictionary);

        var getResponse = await client.GetAsync(cacheName, key1);
        Assert.True(getResponse is CacheGetResponse.Hit, $"Unexpected response: {getResponse}");
        var goodGetResponse = (CacheGetResponse.Hit)getResponse;
        Assert.Equal(value1, goodGetResponse.ValueString);

        getResponse = await client.GetAsync(cacheName, key2);
        Assert.True(getResponse is CacheGetResponse.Hit, $"Unexpected response: {getResponse}");
        goodGetResponse = (CacheGetResponse.Hit)getResponse;
        Assert.Equal(value2, goodGetResponse.ValueString);
    }
}
