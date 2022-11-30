using System.Xml.Linq;
using Momento.Sdk.Incubating.Requests;
using Momento.Sdk.Incubating.Responses;
using Momento.Sdk.Responses;

namespace Momento.Sdk.Incubating.Tests;

[Collection("SimpleCacheClient")]
public class SetTest : TestBase
{
    public SetTest(SimpleCacheClientFixture fixture) : base(fixture)
    {
    }

    [Theory]
    [InlineData(null, "my-set", new byte[] { 0x00 })]
    [InlineData("cache", null, new byte[] { 0x00 })]
    [InlineData("cache", "my-set", null)]
    public async Task SetAddAsync_NullChecksByteArray_IsError(string cacheName, string setName, byte[] element)
    {
        CacheSetAddResponse response = await client.SetAddAsync(cacheName, setName, element);
        Assert.True(response is CacheSetAddResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetAddResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task SetAddFetch_ElementIsByteArray_HappyPath()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidByteArray();

        CacheSetAddResponse response = await client.SetAddAsync(cacheName, setName, element);
        Assert.True(response is CacheSetAddResponse.Success, $"Unexpected response: {response}");

        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Hit, $"Unexpected response: {fetchResponse}");

        var set = ((CacheSetFetchResponse.Hit)fetchResponse).ValueSetByteArray;
        Assert.Single(set);
        Assert.Contains(element, set);
    }

    [Fact]
    public async Task SetAddFetch_ElementIsByteArray_noRefreshTtlOnUpdates()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidByteArray();

        CacheSetAddResponse response = await client.SetAddAsync(cacheName, setName, element, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(5)).WithNoRefreshTtlOnUpdates());
        Assert.True(response is CacheSetAddResponse.Success, $"Unexpected response: {response}");
        await Task.Delay(100);

        response = await client.SetAddAsync(cacheName, setName, element, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(10)).WithNoRefreshTtlOnUpdates());
        Assert.True(response is CacheSetAddResponse.Success, $"Unexpected response: {response}");
        await Task.Delay(4900);
        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Miss, $"Unexpected response: {fetchResponse}");
    }

    [Fact]
    public async Task SetAddFetch_ElementIsByteArray_RefreshTtl()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidByteArray();

        CacheSetAddResponse response = await client.SetAddAsync(cacheName, setName, element, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(2)).WithNoRefreshTtlOnUpdates());
        Assert.True(response is CacheSetAddResponse.Success, $"Unexpected response: {response}");
        await client.SetAddAsync(cacheName, setName, element, CollectionTtl.Of(TimeSpan.FromSeconds(10)));
        await Task.Delay(2000);

        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
        Assert.Single(((CacheSetFetchResponse.Hit)fetchResponse).ValueSetByteArray);
    }

    [Theory]
    [InlineData(null, "my-set", "my-element")]
    [InlineData("cache", null, "my-element")]
    [InlineData("cache", "my-set", null)]
    public async Task SetAddAsync_NullChecksString_IsError(string cacheName, string setName, string element)
    {
        CacheSetAddResponse response = await client.SetAddAsync(cacheName, setName, element);
        Assert.True(response is CacheSetAddResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetAddResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task SetAddFetch_ElementIsString_HappyPath()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidString();

        CacheSetAddResponse respose = await client.SetAddAsync(cacheName, setName, element);
        Assert.True(respose is CacheSetAddResponse.Success, $"Unexpected response: {respose}");

        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Hit, $"Unexpected response: {fetchResponse}");

        var set = ((CacheSetFetchResponse.Hit)fetchResponse).ValueSetString;
        Assert.Single(set);
        Assert.Contains(element, set);
    }

    [Fact]
    public async Task SetAddFetch_ElementIsString_noRefreshTtlOnUpdates()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidString();

        CacheSetAddResponse response = await client.SetAddAsync(cacheName, setName, element, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(5)).WithNoRefreshTtlOnUpdates());
        Assert.True(response is CacheSetAddResponse.Success, $"Unexpected response: {response}");
        await Task.Delay(100);

        response = await client.SetAddAsync(cacheName, setName, element, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(10)).WithNoRefreshTtlOnUpdates());
        Assert.True(response is CacheSetAddResponse.Success, $"Unexpected response: {response}");
        await Task.Delay(4900);

        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Miss, $"Unexpected response: {fetchResponse}");
    }

    [Fact]
    public async Task SetAddFetch_ElementIsString_RefreshTtl()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidString();

        CacheSetAddResponse response = await client.SetAddAsync(cacheName, setName, element, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(2)).WithNoRefreshTtlOnUpdates());
        Assert.True(response is CacheSetAddResponse.Success, $"Unexpected response: {response}");
        response = await client.SetAddAsync(cacheName, setName, element, CollectionTtl.Of(TimeSpan.FromSeconds(10)));
        Assert.True(response is CacheSetAddResponse.Success, $"Unexpected response: {response}");
        await Task.Delay(2000);

        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
        Assert.Single(((CacheSetFetchResponse.Hit)fetchResponse).ValueSetString);
    }

    [Fact]
    public async Task SetAddBatchAsync_NullChecksByteArray_IsError()
    {
        var setName = Utils.NewGuidString();
        var set = new HashSet<byte[]>();
        CacheSetAddBatchResponse response = await client.SetAddBatchAsync(null!, setName, set);
        Assert.True(response is CacheSetAddBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetAddBatchResponse.Error)response).ErrorCode);
        response = await client.SetAddBatchAsync(cacheName, null!, set);
        Assert.True(response is CacheSetAddBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetAddBatchResponse.Error)response).ErrorCode);
        response = await client.SetAddBatchAsync(cacheName, setName, (IEnumerable<byte[]>)null!);
        Assert.True(response is CacheSetAddBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetAddBatchResponse.Error)response).ErrorCode);

        set.Add(null!);
        response = await client.SetAddBatchAsync(cacheName, setName, set);
        Assert.True(response is CacheSetAddBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetAddBatchResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task SetAddBatchAsync_ElementsAreByteArrayEnumerable_HappyPath()
    {
        var setName = Utils.NewGuidString();
        var element1 = Utils.NewGuidByteArray();
        var element2 = Utils.NewGuidByteArray();
        var content = new List<byte[]>() { element1, element2 };

        CacheSetAddBatchResponse response = await client.SetAddBatchAsync(cacheName, setName, content);
        Assert.True(response is CacheSetAddBatchResponse.Success, $"Unexpected response: {response}");

        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Hit, $"Unexpected response: {fetchResponse}");

        var set = ((CacheSetFetchResponse.Hit)fetchResponse).ValueSetByteArray;
        Assert.Equal(2, set!.Count);
        Assert.Contains(element1, set);
        Assert.Contains(element2, set);
    }

    [Fact]
    public async Task SetAddBatchAsync_ElementsAreByteArrayEnumerable_noRefreshTtlOnUpdates()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidByteArray();
        var content = new List<byte[]>() { element };

        CacheSetAddBatchResponse response = await client.SetAddBatchAsync(cacheName, setName, content, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(5)).WithNoRefreshTtlOnUpdates());
        Assert.True(response is CacheSetAddBatchResponse.Success, $"Unexpected response: {response}");
        await Task.Delay(100);

        response = await client.SetAddBatchAsync(cacheName, setName, content, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(10)).WithNoRefreshTtlOnUpdates());
        Assert.True(response is CacheSetAddBatchResponse.Success, $"Unexpected response: {response}");
        await Task.Delay(4900);

        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Miss, $"Unexpected response: {fetchResponse}");
    }

    [Fact]
    public async Task SetAddBatchAsync_ElementsAreByteArrayEnumerable_RefreshTtl()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidByteArray();
        var content = new List<byte[]>() { element };

        CacheSetAddBatchResponse response = await client.SetAddBatchAsync(cacheName, setName, content, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(2)).WithNoRefreshTtlOnUpdates());
        Assert.True(response is CacheSetAddBatchResponse.Success, $"Unexpected response: {response}");
        await client.SetAddBatchAsync(cacheName, setName, content, CollectionTtl.Of(TimeSpan.FromSeconds(10)));
        await Task.Delay(2000);

        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Hit, $"Unexpected response: {fetchResponse}");

        var set = ((CacheSetFetchResponse.Hit)fetchResponse).ValueSetByteArray;
        Assert.Single(set);
        Assert.Contains(element, set);
    }

    [Fact]
    public async Task SetAddBatchAsync_NullChecksString_IsError()
    {
        var setName = Utils.NewGuidString();
        var set = new HashSet<string>();
        CacheSetAddBatchResponse response = await client.SetAddBatchAsync(null!, setName, set);
        Assert.True(response is CacheSetAddBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetAddBatchResponse.Error)response).ErrorCode);
        response = await client.SetAddBatchAsync(cacheName, null!, set);
        Assert.True(response is CacheSetAddBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetAddBatchResponse.Error)response).ErrorCode);
        response = await client.SetAddBatchAsync(cacheName, setName, (IEnumerable<string>)null!);
        Assert.True(response is CacheSetAddBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetAddBatchResponse.Error)response).ErrorCode);

        set.Add(null!);
        response = await client.SetAddBatchAsync(cacheName, setName, set);
        Assert.True(response is CacheSetAddBatchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetAddBatchResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task SetAddBatchAsync_ElementsAreStringEnumerable_HappyPath()
    {
        var setName = Utils.NewGuidString();
        var element1 = Utils.NewGuidString();
        var element2 = Utils.NewGuidString();
        var content = new List<string>() { element1, element2 };

        CacheSetAddBatchResponse response = await client.SetAddBatchAsync(cacheName, setName, content);
        Assert.True(response is CacheSetAddBatchResponse.Success, $"Unexpected response: {response}");

        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Hit, $"Unexpected response: {fetchResponse}");

        var set = ((CacheSetFetchResponse.Hit)fetchResponse).ValueSetString;
        Assert.Equal(2, set!.Count);
        Assert.Contains(element1, set);
        Assert.Contains(element2, set);
    }

    [Fact]
    public async Task SetAddBatchAsync_ElementsAreStringEnumerable_noRefreshTtlOnUpdates()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidString();
        var content = new List<string>() { element };

        CacheSetAddBatchResponse response = await client.SetAddBatchAsync(cacheName, setName, content, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(5)).WithNoRefreshTtlOnUpdates());
        Assert.True(response is CacheSetAddBatchResponse.Success, $"Unexpected response: {response}");
        await Task.Delay(100);

        response = await client.SetAddBatchAsync(cacheName, setName, content, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(10)).WithNoRefreshTtlOnUpdates());
        Assert.True(response is CacheSetAddBatchResponse.Success, $"Unexpected response: {response}");
        await Task.Delay(4900);

        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Miss, $"Unexpected response: {fetchResponse}");
    }

    [Fact]
    public async Task SetAddBatchAsync_ElementsAreStringEnumerable_RefreshTtl()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidString();
        var content = new List<string>() { element };

        CacheSetAddBatchResponse response = await client.SetAddBatchAsync(cacheName, setName, content, ttl: CollectionTtl.Of(TimeSpan.FromSeconds(2)).WithNoRefreshTtlOnUpdates());
        Assert.True(response is CacheSetAddBatchResponse.Success, $"Unexpected response: {response}");
        response = await client.SetAddBatchAsync(cacheName, setName, content, CollectionTtl.Of(TimeSpan.FromSeconds(10)));
        Assert.True(response is CacheSetAddBatchResponse.Success, $"Unexpected response: {response}");
        await Task.Delay(2000);

        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Hit, $"Unexpected response: {fetchResponse}");

        var set = ((CacheSetFetchResponse.Hit)fetchResponse).ValueSetString;
        Assert.Single(set);
        Assert.Contains(element, set);
    }

    [Theory]
    [InlineData(null, "my-set", new byte[] { 0x00 })]
    [InlineData("cache", null, new byte[] { 0x00 })]
    [InlineData("cache", "my-set", null)]
    public async Task SetRemoveElementAsync_NullChecksByteArray_IsError(string cacheName, string setName, byte[] element)
    {
        CacheSetRemoveElementResponse response = await client.SetRemoveElementAsync(cacheName, setName, element);
        Assert.True(response is CacheSetRemoveElementResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task SetRemoveElementAsync_ElementIsByteArray_HappyPath()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidByteArray();

        CacheSetAddResponse response = await client.SetAddAsync(cacheName, setName, element);
        Assert.True(response is CacheSetAddResponse.Success, $"Unexpected response: {response}");

        // Remove element that is not there -- no-op
        CacheSetRemoveElementResponse removeResponse = await client.SetRemoveElementAsync(cacheName, setName, Utils.NewGuidByteArray());
        Assert.True(removeResponse is CacheSetRemoveElementResponse.Success, $"Unexpected response: {removeResponse}");
        // Fetch the whole set and make sure response has element we expect
        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
        var set = ((CacheSetFetchResponse.Hit)fetchResponse).ValueSetByteArray;
        Assert.Single(set);
        Assert.Contains(element, set);

        // Remove element
        removeResponse = await client.SetRemoveElementAsync(cacheName, setName, element);
        Assert.True(removeResponse is CacheSetRemoveElementResponse.Success, $"Unexpected response: {removeResponse}");
        fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Miss, $"Unexpected response: {fetchResponse}");
    }

    [Fact]
    public async Task SetRemoveElementAsync_SetIsMissingElementIsByteArray_Noop()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidString();

        // Pre-condition: set is missing
        Assert.True(await client.SetFetchAsync(cacheName, setName) is CacheSetFetchResponse.Miss);

        // Remove element that is not there -- no-op
        CacheSetRemoveElementResponse removeResponse = await client.SetRemoveElementAsync(cacheName, setName, Utils.NewGuidByteArray());
        Assert.True(removeResponse is CacheSetRemoveElementResponse.Success, $"Unexpected response: {removeResponse}");

        // Post-condition: set is still missing
        Assert.True(await client.SetFetchAsync(cacheName, setName) is CacheSetFetchResponse.Miss);
    }

    [Theory]
    [InlineData(null, "my-set", "my-element")]
    [InlineData("cache", null, "my-element")]
    [InlineData("cache", "my-set", null)]
    public async Task SetRemoveElementAsync_NullChecksString_IsError(string cacheName, string setName, string element)
    {
        CacheSetRemoveElementResponse response = await client.SetRemoveElementAsync(cacheName, setName, element);
        Assert.True(response is CacheSetRemoveElementResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task SetRemoveElementAsync_ElementIsString_HappyPath()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidString();

        CacheSetAddResponse response = await client.SetAddAsync(cacheName, setName, element);
        Assert.True(response is CacheSetAddResponse.Success, $"Unexpected response: {response}");

        // Remove element that is not there -- no-op
        CacheSetRemoveElementResponse removeResponse = await client.SetRemoveElementAsync(cacheName, setName, Utils.NewGuidString());
        Assert.True(removeResponse is CacheSetRemoveElementResponse.Success, $"Unexpected response: {removeResponse}");
        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
        var set = ((CacheSetFetchResponse.Hit)fetchResponse).ValueSetString;
        Assert.Single(set);
        Assert.Contains(element, set);

        // Remove element
        removeResponse = await client.SetRemoveElementAsync(cacheName, setName, element);
        Assert.True(removeResponse is CacheSetRemoveElementResponse.Success, $"Unexpected response: {removeResponse}");
        fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Miss, $"Unexpected response: {fetchResponse}");
    }

    [Fact]
    public async Task SetRemoveElementAsync_SetIsMissingElementIsString_Noop()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidString();

        // Pre-condition: set is missing
        Assert.True(await client.SetFetchAsync(cacheName, setName) is CacheSetFetchResponse.Miss);

        // Remove element that is not there -- no-op
        CacheSetRemoveElementResponse response = await client.SetRemoveElementAsync(cacheName, setName, Utils.NewGuidString());
        Assert.True(response is CacheSetRemoveElementResponse.Success, $"Unexpected response: {response}");

        // Post-condition: set is still missing
        Assert.True(await client.SetFetchAsync(cacheName, setName) is CacheSetFetchResponse.Miss);
    }

    [Fact]
    public async Task SetRemoveElementsAsync_NullChecksElementsAreByteArray_IsError()
    {
        var setName = Utils.NewGuidString();
        var testData = new byte[][][] { new byte[][] { Utils.NewGuidByteArray(), Utils.NewGuidByteArray() }, new byte[][] { Utils.NewGuidByteArray(), null! } };

        CacheSetRemoveElementsResponse response = await client.SetRemoveElementsAsync(null!, setName, testData[0]);
        Assert.True(response is CacheSetRemoveElementsResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementsResponse.Error)response).ErrorCode);
        response = await client.SetRemoveElementsAsync(cacheName, null!, testData[0]);
        Assert.True(response is CacheSetRemoveElementsResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementsResponse.Error)response).ErrorCode);
        response = await client.SetRemoveElementsAsync(cacheName, setName, (byte[][])null!);
        Assert.True(response is CacheSetRemoveElementsResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementsResponse.Error)response).ErrorCode);
        response = await client.SetRemoveElementsAsync(cacheName, setName, testData[1]);
        Assert.True(response is CacheSetRemoveElementsResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementsResponse.Error)response).ErrorCode);

        var fieldsList = new List<byte[]>(testData[0]);
        response = await client.SetRemoveElementsAsync(null!, setName, fieldsList);
        Assert.True(response is CacheSetRemoveElementsResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementsResponse.Error)response).ErrorCode);
        response = await client.SetRemoveElementsAsync(cacheName, null!, fieldsList);
        Assert.True(response is CacheSetRemoveElementsResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementsResponse.Error)response).ErrorCode);
        response = await client.SetRemoveElementsAsync(cacheName, setName, (List<byte[]>)null!);
        Assert.True(response is CacheSetRemoveElementsResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementsResponse.Error)response).ErrorCode);
        response = await client.SetRemoveElementsAsync(cacheName, setName, new List<byte[]>(testData[1]));
        Assert.True(response is CacheSetRemoveElementsResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementsResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task SetRemoveElementsAsync_FieldsAreByteArray_HappyPath()
    {
        var setName = Utils.NewGuidString();
        var elements = new byte[][] { Utils.NewGuidByteArray(), Utils.NewGuidByteArray() };
        var otherElement = Utils.NewGuidByteArray();

        // Test enumerable
        CacheSetAddResponse response = await client.SetAddAsync(cacheName, setName, elements[0]);
        Assert.True(response is CacheSetAddResponse.Success, $"Unexpected response: {response}");
        await client.SetAddAsync(cacheName, setName, elements[1]);
        Assert.True(response is CacheSetAddResponse.Success, $"Unexpected response: {response}");
        await client.SetAddAsync(cacheName, setName, otherElement);
        Assert.True(response is CacheSetAddResponse.Success, $"Unexpected response: {response}");

        var elementsList = new List<byte[]>(elements);
        CacheSetRemoveElementsResponse removeResponse = await client.SetRemoveElementsAsync(cacheName, setName, elementsList);
        Assert.True(removeResponse is CacheSetRemoveElementsResponse.Success, $"Unexpected response: {removeResponse}");
        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
        var hitResponse = (CacheSetFetchResponse.Hit)fetchResponse;
        Assert.Single(hitResponse.ValueSetByteArray!);
        Assert.Contains(otherElement, hitResponse.ValueSetByteArray!);
    }

    [Fact]
    public async Task SetRemoveElementsAsync_NullChecksElementsAreString_IsError()
    {
        var setName = Utils.NewGuidString();
        var testData = new string[][] { new string[] { Utils.NewGuidString(), Utils.NewGuidString() }, new string[] { Utils.NewGuidString(), null! } };

        CacheSetRemoveElementsResponse response = await client.SetRemoveElementsAsync(null!, setName, testData[0]);
        Assert.True(response is CacheSetRemoveElementsResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementsResponse.Error)response).ErrorCode);
        response = await client.SetRemoveElementsAsync(cacheName, null!, testData[0]);
        Assert.True(response is CacheSetRemoveElementsResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementsResponse.Error)response).ErrorCode);
        response = await client.SetRemoveElementsAsync(cacheName, setName, (byte[][])null!);
        Assert.True(response is CacheSetRemoveElementsResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementsResponse.Error)response).ErrorCode);
        response = await client.SetRemoveElementsAsync(cacheName, setName, testData[1]);
        Assert.True(response is CacheSetRemoveElementsResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementsResponse.Error)response).ErrorCode);

        var elementsList = new List<string>(testData[0]);
        response = await client.SetRemoveElementsAsync(null!, setName, elementsList);
        Assert.True(response is CacheSetRemoveElementsResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementsResponse.Error)response).ErrorCode);
        response = await client.SetRemoveElementsAsync(cacheName, null!, elementsList);
        Assert.True(response is CacheSetRemoveElementsResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementsResponse.Error)response).ErrorCode);
        response = await client.SetRemoveElementsAsync(cacheName, setName, (List<string>)null!);
        Assert.True(response is CacheSetRemoveElementsResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementsResponse.Error)response).ErrorCode);
        response = await client.SetRemoveElementsAsync(cacheName, setName, new List<string>(testData[1]));
        Assert.True(response is CacheSetRemoveElementsResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementsResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task SetRemoveElementsAsync_FieldsAreString_HappyPath()
    {
        var setName = Utils.NewGuidString();
        var elements = new string[] { Utils.NewGuidString(), Utils.NewGuidString() };
        var otherElement = Utils.NewGuidByteArray();

        // Test enumerable
        CacheSetAddResponse response = await client.SetAddAsync(cacheName, setName, elements[0]);
        Assert.True(response is CacheSetAddResponse.Success, $"Unexpected response: {response}");
        response = await client.SetAddAsync(cacheName, setName, elements[1]);
        Assert.True(response is CacheSetAddResponse.Success, $"Unexpected response: {response}");
        response = await client.SetAddAsync(cacheName, setName, otherElement);
        Assert.True(response is CacheSetAddResponse.Success, $"Unexpected response: {response}");

        var elementsList = new List<string>(elements);
        CacheSetRemoveElementsResponse removeResponse = await client.SetRemoveElementsAsync(cacheName, setName, elementsList);
        Assert.True(removeResponse is CacheSetRemoveElementsResponse.Success, $"Unexpected response: {removeResponse}");
        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Hit, $"Unexpected response: {fetchResponse}");
        var hitResponse = (CacheSetFetchResponse.Hit)fetchResponse;
        Assert.Single(hitResponse.ValueSetByteArray!);
        Assert.Contains(otherElement, hitResponse.ValueSetByteArray!);
    }

    [Theory]
    [InlineData(null, "my-set")]
    [InlineData("cache", null)]
    public async Task SetFetchAsync_NullChecks_IsError(string cacheName, string setName)
    {
        CacheSetFetchResponse response = await client.SetFetchAsync(cacheName, setName);
        Assert.True(response is CacheSetFetchResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetFetchResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task SetFetchAsync_Missing_HappyPath()
    {
        var setName = Utils.NewGuidString();
        CacheSetFetchResponse response = await client.SetFetchAsync(cacheName, setName);
        Assert.True(response is CacheSetFetchResponse.Miss, $"Unexpected response: {response}");
    }

    [Fact]
    public async Task SetFetchAsync_UsesCachedByteArraySet_HappyPath()
    {
        var setName = Utils.NewGuidString();
        CacheSetAddBatchResponse setResponse = await client.SetAddBatchAsync(cacheName, setName, new string[] { Utils.NewGuidString(), Utils.NewGuidString() });
        Assert.True(setResponse is CacheSetAddBatchResponse.Success, $"Unexpected response: {setResponse}");
        CacheSetFetchResponse response = await client.SetFetchAsync(cacheName, setName);
        Assert.True(response is CacheSetFetchResponse.Hit, $"Unexpected response: {response}");
        var hitResponse = (CacheSetFetchResponse.Hit)response;
        var set1 = hitResponse.ValueSetByteArray;
        var set2 = hitResponse.ValueSetByteArray;
        Assert.Same(set1, set2);
    }

    [Fact]
    public async Task SetFetchAsync_UsesCachedStringSet_HappyPath()
    {
        var setName = Utils.NewGuidString();
        CacheSetAddBatchResponse setResponse = await client.SetAddBatchAsync(cacheName, setName, new string[] { Utils.NewGuidString(), Utils.NewGuidString() });
        Assert.True(setResponse is CacheSetAddBatchResponse.Success, $"Unexpected response: {setResponse}");
        CacheSetFetchResponse response = await client.SetFetchAsync(cacheName, setName);
        Assert.True(response is CacheSetFetchResponse.Hit, $"Unexpected response: {response}");
        var hitResponse = (CacheSetFetchResponse.Hit)response;
        var set1 = hitResponse.ValueSetString;
        var set2 = hitResponse.ValueSetString;
        Assert.Same(set1, set2);
    }

    [Theory]
    [InlineData(null, "my-set")]
    [InlineData("my-cache", null)]
    public async Task SetDeleteAsync_NullChecks_IsError(string cacheName, string setName)
    {
        CacheSetDeleteResponse response = await client.SetDeleteAsync(cacheName, setName);
        Assert.True(response is CacheSetDeleteResponse.Error, $"Unexpected response: {response}");
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetDeleteResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task SetDeleteAsync_SetDoesNotExist_Noop()
    {
        var setName = Utils.NewGuidString();
        Assert.True(await client.SetFetchAsync(cacheName, setName) is CacheSetFetchResponse.Miss);
        CacheSetDeleteResponse response = await client.SetDeleteAsync(cacheName, setName);
        Assert.True(response is CacheSetDeleteResponse.Success, $"Unexpected response: {response}");
        Assert.True(await client.SetFetchAsync(cacheName, setName) is CacheSetFetchResponse.Miss);
    }

    [Fact]
    public async Task SetDeleteAsync_SetExists_HappyPath()
    {
        var setName = Utils.NewGuidString();
        CacheSetAddResponse response = await client.SetAddAsync(cacheName, setName, Utils.NewGuidString());
        Assert.True(response is CacheSetAddResponse.Success, $"Unexpected response: {response}");
        response = await client.SetAddAsync(cacheName, setName, Utils.NewGuidString());
        Assert.True(response is CacheSetAddResponse.Success, $"Unexpected response: {response}");
        response = await client.SetAddAsync(cacheName, setName, Utils.NewGuidString());
        Assert.True(response is CacheSetAddResponse.Success, $"Unexpected response: {response}");

        Assert.True(await client.SetFetchAsync(cacheName, setName) is CacheSetFetchResponse.Hit);
        CacheSetDeleteResponse deleteResponse = await client.SetDeleteAsync(cacheName, setName);
        Assert.True(deleteResponse is CacheSetDeleteResponse.Success, $"Unexpected response: {deleteResponse}");
        Assert.True(await client.SetFetchAsync(cacheName, setName) is CacheSetFetchResponse.Miss);
    }
}
