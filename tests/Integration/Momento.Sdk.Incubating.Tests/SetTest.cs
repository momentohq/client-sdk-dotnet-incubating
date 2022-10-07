using System.Xml.Linq;
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
    public async Task SetAddAsync_NullChecksByteArray_ThrowsException(string cacheName, string setName, byte[] element)
    {
        CacheSetAddResponse response = await client.SetAddAsync(cacheName, setName, element, false);
        Assert.True(response is CacheSetAddResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetAddResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task SetAddFetch_ElementIsByteArray_HappyPath()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidByteArray();

        CacheSetAddResponse response = await client.SetAddAsync(cacheName, setName, element, false);
        Assert.True(response is CacheSetAddResponse.Success);

        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Hit);

        var set = ((CacheSetFetchResponse.Hit)fetchResponse).ByteArraySet;
        Assert.Single(set);
        Assert.Contains(element, set);
    }

    [Fact]
    public async Task SetAddFetch_ElementIsByteArray_NoRefreshTtl()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidByteArray();

        CacheSetAddResponse response = await client.SetAddAsync(cacheName, setName, element, false, ttlSeconds: 5);
        Assert.True(response is CacheSetAddResponse.Success);
        await Task.Delay(100);

        response = await client.SetAddAsync(cacheName, setName, element, false, ttlSeconds: 10);
        Assert.True(response is CacheSetAddResponse.Success);
        await Task.Delay(4900);
        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Miss);
    }

    [Fact]
    public async Task SetAddFetch_ElementIsByteArray_RefreshTtl()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidByteArray();

        CacheSetAddResponse response = await client.SetAddAsync(cacheName, setName, element, false, ttlSeconds: 2);
        Assert.True(response is CacheSetAddResponse.Success);
        await client.SetAddAsync(cacheName, setName, element, true, ttlSeconds: 10);
        await Task.Delay(2000);

        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Hit);
        Assert.Single(((CacheSetFetchResponse.Hit)fetchResponse).ByteArraySet);
    }

    [Theory]
    [InlineData(null, "my-set", "my-element")]
    [InlineData("cache", null, "my-element")]
    [InlineData("cache", "my-set", null)]
    public async Task SetAddAsync_NullChecksString_ThrowsException(string cacheName, string setName, string element)
    {
        CacheSetAddResponse response = await client.SetAddAsync(cacheName, setName, element, false);
        Assert.True(response is CacheSetAddResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetAddResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task SetAddFetch_ElementIsString_HappyPath()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidString();

        CacheSetAddResponse respose = await client.SetAddAsync(cacheName, setName, element, false);
        Assert.True(respose is CacheSetAddResponse.Success);

        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Hit);

        var set = ((CacheSetFetchResponse.Hit)fetchResponse).StringSet();
        Assert.Single(set);
        Assert.Contains(element, set);
    }

    [Fact]
    public async Task SetAddFetch_ElementIsString_NoRefreshTtl()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidString();

        CacheSetAddResponse response = await client.SetAddAsync(cacheName, setName, element, false, ttlSeconds: 5);
        Assert.True(response is CacheSetAddResponse.Success);
        await Task.Delay(100);

        response = await client.SetAddAsync(cacheName, setName, element, false, ttlSeconds: 10);
        Assert.True(response is CacheSetAddResponse.Success);
        await Task.Delay(4900);

        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Miss);
    }

    [Fact]
    public async Task SetAddFetch_ElementIsString_RefreshTtl()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidString();

        CacheSetAddResponse response = await client.SetAddAsync(cacheName, setName, element, false, ttlSeconds: 2);
        Assert.True(response is CacheSetAddResponse.Success);
        response = await client.SetAddAsync(cacheName, setName, element, true, ttlSeconds: 10);
        Assert.True(response is CacheSetAddResponse.Success);
        await Task.Delay(2000);

        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Hit);
        Assert.Single(((CacheSetFetchResponse.Hit)fetchResponse).StringSet());
    }

    [Fact]
    public async Task SetAddBatchAsync_NullChecksByteArray_ThrowsException()
    {
        var setName = Utils.NewGuidString();
        var set = new HashSet<byte[]>();
        CacheSetAddBatchResponse response = await client.SetAddBatchAsync(null!, setName, set, false);
        Assert.True(response is CacheSetAddBatchResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetAddBatchResponse.Error)response).ErrorCode);
        response = await client.SetAddBatchAsync(cacheName, null!, set, false);
        Assert.True(response is CacheSetAddBatchResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetAddBatchResponse.Error)response).ErrorCode);
        response = await client.SetAddBatchAsync(cacheName, setName, (IEnumerable<byte[]>)null!, false);
        Assert.True(response is CacheSetAddBatchResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetAddBatchResponse.Error)response).ErrorCode);

        set.Add(null!);
        response = await client.SetAddBatchAsync(cacheName, setName, set, false);
        Assert.True(response is CacheSetAddBatchResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetAddBatchResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task SetAddBatchAsync_ElementsAreByteArrayEnumerable_HappyPath()
    {
        var setName = Utils.NewGuidString();
        var element1 = Utils.NewGuidByteArray();
        var element2 = Utils.NewGuidByteArray();
        var content = new List<byte[]>() { element1, element2 };

        CacheSetAddBatchResponse response = await client.SetAddBatchAsync(cacheName, setName, content, false, 10);
        Assert.True(response is CacheSetAddBatchResponse.Success);

        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Hit);

        var set = ((CacheSetFetchResponse.Hit)fetchResponse).ByteArraySet;
        Assert.Equal(2, set!.Count);
        Assert.Contains(element1, set);
        Assert.Contains(element2, set);
    }

    [Fact]
    public async Task SetAddBatchAsync_ElementsAreByteArrayEnumerable_NoRefreshTtl()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidByteArray();
        var content = new List<byte[]>() { element };

        CacheSetAddBatchResponse response = await client.SetAddBatchAsync(cacheName, setName, content, false, ttlSeconds: 5);
        Assert.True(response is CacheSetAddBatchResponse.Success);
        await Task.Delay(100);

        response = await client.SetAddBatchAsync(cacheName, setName, content, false, ttlSeconds: 10);
        Assert.True(response is CacheSetAddBatchResponse.Success);
        await Task.Delay(4900);

        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Miss);
    }

    [Fact]
    public async Task SetAddBatchAsync_ElementsAreByteArrayEnumerable_RefreshTtl()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidByteArray();
        var content = new List<byte[]>() { element };

        CacheSetAddBatchResponse response = await client.SetAddBatchAsync(cacheName, setName, content, false, ttlSeconds: 2);
        Assert.True(response is CacheSetAddBatchResponse.Success);
        await client.SetAddBatchAsync(cacheName, setName, content, true, ttlSeconds: 10);
        await Task.Delay(2000);

        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Hit);

        var set = ((CacheSetFetchResponse.Hit)fetchResponse).ByteArraySet;
        Assert.Single(set);
        Assert.Contains(element, set);
    }

    [Fact]
    public async Task SetAddBatchAsync_NullChecksString_ThrowsException()
    {
        var setName = Utils.NewGuidString();
        var set = new HashSet<string>();
        CacheSetAddBatchResponse response = await client.SetAddBatchAsync(null!, setName, set, false);
        Assert.True(response is CacheSetAddBatchResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetAddBatchResponse.Error)response).ErrorCode);
        response = await client.SetAddBatchAsync(cacheName, null!, set, false);
        Assert.True(response is CacheSetAddBatchResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetAddBatchResponse.Error)response).ErrorCode);
        response = await client.SetAddBatchAsync(cacheName, setName, (IEnumerable<string>)null!, false);
        Assert.True(response is CacheSetAddBatchResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetAddBatchResponse.Error)response).ErrorCode);

        set.Add(null!);
        response = await client.SetAddBatchAsync(cacheName, setName, set, false);
        Assert.True(response is CacheSetAddBatchResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetAddBatchResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task SetAddBatchAsync_ElementsAreStringEnumerable_HappyPath()
    {
        var setName = Utils.NewGuidString();
        var element1 = Utils.NewGuidString();
        var element2 = Utils.NewGuidString();
        var content = new List<string>() { element1, element2 };

        CacheSetAddBatchResponse response = await client.SetAddBatchAsync(cacheName, setName, content, false, 10);
        Assert.True(response is CacheSetAddBatchResponse.Success);

        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Hit);

        var set = ((CacheSetFetchResponse.Hit)fetchResponse).StringSet();
        Assert.Equal(2, set!.Count);
        Assert.Contains(element1, set);
        Assert.Contains(element2, set);
    }

    [Fact]
    public async Task SetAddBatchAsync_ElementsAreStringEnumerable_NoRefreshTtl()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidString();
        var content = new List<string>() { element };

        CacheSetAddBatchResponse response = await client.SetAddBatchAsync(cacheName, setName, content, false, ttlSeconds: 5);
        Assert.True(response is CacheSetAddBatchResponse.Success);
        await Task.Delay(100);

        response = await client.SetAddBatchAsync(cacheName, setName, content, false, ttlSeconds: 10);
        Assert.True(response is CacheSetAddBatchResponse.Success);
        await Task.Delay(4900);

        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Miss);
    }

    [Fact]
    public async Task SetAddBatchAsync_ElementsAreStringEnumerable_RefreshTtl()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidString();
        var content = new List<string>() { element };

        CacheSetAddBatchResponse response = await client.SetAddBatchAsync(cacheName, setName, content, false, ttlSeconds: 2);
        Assert.True(response is CacheSetAddBatchResponse.Success);
        response = await client.SetAddBatchAsync(cacheName, setName, content, true, ttlSeconds: 10);
        Assert.True(response is CacheSetAddBatchResponse.Success);
        await Task.Delay(2000);

        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Hit);

        var set = ((CacheSetFetchResponse.Hit)fetchResponse).StringSet();
        Assert.Single(set);
        Assert.Contains(element, set);
    }

    [Theory]
    [InlineData(null, "my-set", new byte[] { 0x00 })]
    [InlineData("cache", null, new byte[] { 0x00 })]
    [InlineData("cache", "my-set", null)]
    public async Task SetRemoveElementAsync_NullChecksByteArray_ThrowsException(string cacheName, string setName, byte[] element)
    {
        CacheSetRemoveElementResponse response = await client.SetRemoveElementAsync(cacheName, setName, element);
        Assert.True(response is CacheSetRemoveElementResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task SetRemoveElementAsync_ElementIsByteArray_HappyPath()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidByteArray();

        CacheSetAddResponse response = await client.SetAddAsync(cacheName, setName, element, false);
        Assert.True(response is CacheSetAddResponse.Success);

        // Remove element that is not there -- no-op
        CacheSetRemoveElementResponse removeResponse = await client.SetRemoveElementAsync(cacheName, setName, Utils.NewGuidByteArray());
        Assert.True(removeResponse is CacheSetRemoveElementResponse.Success);
        // Fetch the whole set and make sure response has element we expect 
        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Hit);
        var set = ((CacheSetFetchResponse.Hit)fetchResponse).ByteArraySet;
        Assert.Single(set);
        Assert.Contains(element, set);

        // Remove element
        removeResponse = await client.SetRemoveElementAsync(cacheName, setName, element);
        Assert.True(removeResponse is CacheSetRemoveElementResponse.Success);
        fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Miss);
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
        Assert.True(removeResponse is CacheSetRemoveElementResponse.Success);

        // Post-condition: set is still missing
        Assert.True(await client.SetFetchAsync(cacheName, setName) is CacheSetFetchResponse.Miss);
    }

    [Theory]
    [InlineData(null, "my-set", "my-element")]
    [InlineData("cache", null, "my-element")]
    [InlineData("cache", "my-set", null)]
    public async Task SetRemoveElementAsync_NullChecksString_ThrowsException(string cacheName, string setName, string element)
    {
        CacheSetRemoveElementResponse response = await client.SetRemoveElementAsync(cacheName, setName, element);
        Assert.True(response is CacheSetRemoveElementResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task SetRemoveElementAsync_ElementIsString_HappyPath()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidString();

        CacheSetAddResponse response = await client.SetAddAsync(cacheName, setName, element, false);
        Assert.True(response is CacheSetAddResponse.Success);

        // Remove element that is not there -- no-op
        CacheSetRemoveElementResponse removeResponse = await client.SetRemoveElementAsync(cacheName, setName, Utils.NewGuidString());
        Assert.True(removeResponse is CacheSetRemoveElementResponse.Success);
        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Hit);
        var set = ((CacheSetFetchResponse.Hit)fetchResponse).StringSet();
        Assert.Single(set);
        Assert.Contains(element, set);

        // Remove element
        removeResponse = await client.SetRemoveElementAsync(cacheName, setName, element);
        Assert.True(removeResponse is CacheSetRemoveElementResponse.Success);
        fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Miss);
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
        Assert.True(response is CacheSetRemoveElementResponse.Success);

        // Post-condition: set is still missing
        Assert.True(await client.SetFetchAsync(cacheName, setName) is CacheSetFetchResponse.Miss);
    }

    [Fact]
    public async Task SetRemoveElementsAsync_NullChecksElementsAreByteArray_ThrowsException()
    {
        var setName = Utils.NewGuidString();
        var testData = new byte[][][] { new byte[][] { Utils.NewGuidByteArray(), Utils.NewGuidByteArray() }, new byte[][] { Utils.NewGuidByteArray(), null! } };

        CacheSetRemoveElementsResponse response = await client.SetRemoveElementsAsync(null!, setName, testData[0]);
        Assert.True(response is CacheSetRemoveElementsResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementsResponse.Error)response).ErrorCode);
        response = await client.SetRemoveElementsAsync(cacheName, null!, testData[0]);
        Assert.True(response is CacheSetRemoveElementsResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementsResponse.Error)response).ErrorCode);
        response = await client.SetRemoveElementsAsync(cacheName, setName, (byte[][])null!);
        Assert.True(response is CacheSetRemoveElementsResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementsResponse.Error)response).ErrorCode);
        response = await client.SetRemoveElementsAsync(cacheName, setName, testData[1]);
        Assert.True(response is CacheSetRemoveElementsResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementsResponse.Error)response).ErrorCode);

        var fieldsList = new List<byte[]>(testData[0]);
        response = await client.SetRemoveElementsAsync(null!, setName, fieldsList);
        Assert.True(response is CacheSetRemoveElementsResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementsResponse.Error)response).ErrorCode);
        response = await client.SetRemoveElementsAsync(cacheName, null!, fieldsList);
        Assert.True(response is CacheSetRemoveElementsResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementsResponse.Error)response).ErrorCode);
        response = await client.SetRemoveElementsAsync(cacheName, setName, (List<byte[]>)null!);
        Assert.True(response is CacheSetRemoveElementsResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementsResponse.Error)response).ErrorCode);
        response = await client.SetRemoveElementsAsync(cacheName, setName, new List<byte[]>(testData[1]));
        Assert.True(response is CacheSetRemoveElementsResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementsResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task SetRemoveElementsAsync_FieldsAreByteArray_HappyPath()
    {
        var setName = Utils.NewGuidString();
        var elements = new byte[][] { Utils.NewGuidByteArray(), Utils.NewGuidByteArray() };
        var otherElement = Utils.NewGuidByteArray();

        // Test enumerable
        CacheSetAddResponse response = await client.SetAddAsync(cacheName, setName, elements[0], false);
        Assert.True(response is CacheSetAddResponse.Success);
        await client.SetAddAsync(cacheName, setName, elements[1], false);
        Assert.True(response is CacheSetAddResponse.Success);
        await client.SetAddAsync(cacheName, setName, otherElement, false);
        Assert.True(response is CacheSetAddResponse.Success);

        var elementsList = new List<byte[]>(elements);
        CacheSetRemoveElementsResponse removeResponse = await client.SetRemoveElementsAsync(cacheName, setName, elementsList);
        Assert.True(removeResponse is CacheSetRemoveElementsResponse.Success);
        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Hit);
        var hitResponse = (CacheSetFetchResponse.Hit)fetchResponse;
        Assert.Single(hitResponse.ByteArraySet!);
        Assert.Contains(otherElement, hitResponse.ByteArraySet!);
    }

    [Fact]
    public async Task SetRemoveElementsAsync_NullChecksElementsAreString_ThrowsException()
    {
        var setName = Utils.NewGuidString();
        var testData = new string[][] { new string[] { Utils.NewGuidString(), Utils.NewGuidString() }, new string[] { Utils.NewGuidString(), null! } };

        CacheSetRemoveElementsResponse response = await client.SetRemoveElementsAsync(null!, setName, testData[0]);
        Assert.True(response is CacheSetRemoveElementsResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementsResponse.Error)response).ErrorCode);
        response = await client.SetRemoveElementsAsync(cacheName, null!, testData[0]);
        Assert.True(response is CacheSetRemoveElementsResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementsResponse.Error)response).ErrorCode);
        response = await client.SetRemoveElementsAsync(cacheName, setName, (byte[][])null!);
        Assert.True(response is CacheSetRemoveElementsResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementsResponse.Error)response).ErrorCode);
        response = await client.SetRemoveElementsAsync(cacheName, setName, testData[1]);
        Assert.True(response is CacheSetRemoveElementsResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementsResponse.Error)response).ErrorCode);

        var elementsList = new List<string>(testData[0]);
        response = await client.SetRemoveElementsAsync(null!, setName, elementsList);
        Assert.True(response is CacheSetRemoveElementsResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementsResponse.Error)response).ErrorCode);
        response = await client.SetRemoveElementsAsync(cacheName, null!, elementsList);
        Assert.True(response is CacheSetRemoveElementsResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementsResponse.Error)response).ErrorCode);
        response = await client.SetRemoveElementsAsync(cacheName, setName, (List<string>)null!);
        Assert.True(response is CacheSetRemoveElementsResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementsResponse.Error)response).ErrorCode);
        response = await client.SetRemoveElementsAsync(cacheName, setName, new List<string>(testData[1]));
        Assert.True(response is CacheSetRemoveElementsResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetRemoveElementsResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task SetRemoveElementsAsync_FieldsAreString_HappyPath()
    {
        var setName = Utils.NewGuidString();
        var elements = new string[] { Utils.NewGuidString(), Utils.NewGuidString() };
        var otherElement = Utils.NewGuidByteArray();

        // Test enumerable
        CacheSetAddResponse response = await client.SetAddAsync(cacheName, setName, elements[0], false);
        Assert.True(response is CacheSetAddResponse.Success);
        response = await client.SetAddAsync(cacheName, setName, elements[1], false);
        Assert.True(response is CacheSetAddResponse.Success);
        response = await client.SetAddAsync(cacheName, setName, otherElement, false);
        Assert.True(response is CacheSetAddResponse.Success);

        var elementsList = new List<string>(elements);
        CacheSetRemoveElementsResponse removeResponse = await client.SetRemoveElementsAsync(cacheName, setName, elementsList);
        Assert.True(removeResponse is CacheSetRemoveElementsResponse.Success);
        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True(fetchResponse is CacheSetFetchResponse.Hit);
        var hitResponse = (CacheSetFetchResponse.Hit)fetchResponse;
        Assert.Single(hitResponse.ByteArraySet!);
        Assert.Contains(otherElement, hitResponse.ByteArraySet!);
    }

    [Theory]
    [InlineData(null, "my-set")]
    [InlineData("cache", null)]
    public async Task SetFetchAsync_NullChecks_ThrowsException(string cacheName, string setName)
    {
        CacheSetFetchResponse response = await client.SetFetchAsync(cacheName, setName);
        Assert.True(response is CacheSetFetchResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetFetchResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task SetFetchAsync_Missing_HappyPath()
    {
        var setName = Utils.NewGuidString();
        CacheSetFetchResponse response = await client.SetFetchAsync(cacheName, setName);
        Assert.True(response is CacheSetFetchResponse.Miss);
    }

    [Fact]
    public async Task SetFetchAsync_UsesCachedByteArraySet_HappyPath()
    {
        var setName = Utils.NewGuidString();
        CacheSetAddBatchResponse setResponse = await client.SetAddBatchAsync(cacheName, setName, new string[] { Utils.NewGuidString(), Utils.NewGuidString() }, false);
        Assert.True(setResponse is CacheSetAddBatchResponse.Success);
        CacheSetFetchResponse response = await client.SetFetchAsync(cacheName, setName);
        Assert.True(response is CacheSetFetchResponse.Hit);
        var hitResponse = (CacheSetFetchResponse.Hit)response;
        var set1 = hitResponse.ByteArraySet;
        var set2 = hitResponse.ByteArraySet;
        Assert.Same(set1, set2);
    }

    [Fact]
    public async Task SetFetchAsync_UsesCachedStringSet_HappyPath()
    {
        var setName = Utils.NewGuidString();
        CacheSetAddBatchResponse setResponse = await client.SetAddBatchAsync(cacheName, setName, new string[] { Utils.NewGuidString(), Utils.NewGuidString() }, false);
        Assert.True(setResponse is CacheSetAddBatchResponse.Success);
        CacheSetFetchResponse response = await client.SetFetchAsync(cacheName, setName);
        Assert.True(response is CacheSetFetchResponse.Hit);
        var hitResponse = (CacheSetFetchResponse.Hit)response;
        var set1 = hitResponse.StringSet();
        var set2 = hitResponse.StringSet();
        Assert.Same(set1, set2);
    }

    [Theory]
    [InlineData(null, "my-set")]
    [InlineData("my-cache", null)]
    public async Task SetDeleteAsync_NullChecks_ThrowsException(string cacheName, string setName)
    {
        CacheSetDeleteResponse response = await client.SetDeleteAsync(cacheName, setName);
        Assert.True(response is CacheSetDeleteResponse.Error);
        Assert.Equal(MomentoErrorCode.INVALID_ARGUMENT_ERROR, ((CacheSetDeleteResponse.Error)response).ErrorCode);
    }

    [Fact]
    public async Task SetDeleteAsync_SetDoesNotExist_Noop()
    {
        var setName = Utils.NewGuidString();
        Assert.True(await client.SetFetchAsync(cacheName, setName) is CacheSetFetchResponse.Miss);
        CacheSetDeleteResponse response = await client.SetDeleteAsync(cacheName, setName);
        Assert.True(response is CacheSetDeleteResponse.Success);
        Assert.True(await client.SetFetchAsync(cacheName, setName) is CacheSetFetchResponse.Miss);
    }

    [Fact]
    public async Task SetDeleteAsync_SetExists_HappyPath()
    {
        var setName = Utils.NewGuidString();
        CacheSetAddResponse response = await client.SetAddAsync(cacheName, setName, Utils.NewGuidString(), false);
        Assert.True(response is CacheSetAddResponse.Success);
        response = await client.SetAddAsync(cacheName, setName, Utils.NewGuidString(), false);
        Assert.True(response is CacheSetAddResponse.Success);
        response = await client.SetAddAsync(cacheName, setName, Utils.NewGuidString(), false);
        Assert.True(response is CacheSetAddResponse.Success);

        Assert.True(await client.SetFetchAsync(cacheName, setName) is CacheSetFetchResponse.Hit);
        CacheSetDeleteResponse deleteResponse = await client.SetDeleteAsync(cacheName, setName);
        Assert.True(deleteResponse is CacheSetDeleteResponse.Success);
        Assert.True(await client.SetFetchAsync(cacheName, setName) is CacheSetFetchResponse.Miss);
    }
}
