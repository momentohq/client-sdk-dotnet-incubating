using Momento.Sdk.Responses;
using Momento.Sdk.Incubating.Responses;

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
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.SetAddAsync(cacheName, setName, element, false));
    }

    [Fact]
    public async Task SetAddFetch_ElementIsByteArray_HappyPath()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidByteArray();

        await client.SetAddAsync(cacheName, setName, element, false);

        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        var hitResponse = (CacheSetFetchResponse.Hit)fetchResponse;

        var set = hitResponse.ByteArraySet;
        Assert.Single(set);
        Assert.Contains(element, set);
    }

    [Fact]
    public async Task SetAddFetch_ElementIsByteArray_NoRefreshTtl()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidByteArray();

        await client.SetAddAsync(cacheName, setName, element, false, ttlSeconds: 5);
        await Task.Delay(100);

        await client.SetAddAsync(cacheName, setName, element, false, ttlSeconds: 10);
        await Task.Delay(4900);
        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True((CacheSetFetchResponse.Miss)fetchResponse is CacheSetFetchResponse.Miss);
    }

    [Fact]
    public async Task SetAddFetch_ElementIsByteArray_RefreshTtl()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidByteArray();

        await client.SetAddAsync(cacheName, setName, element, false, ttlSeconds: 2);
        await client.SetAddAsync(cacheName, setName, element, true, ttlSeconds: 10);
        await Task.Delay(2000);

        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        var hitResponse = (CacheSetFetchResponse.Hit)fetchResponse;
        Assert.Single(hitResponse.ByteArraySet);
    }

    [Theory]
    [InlineData(null, "my-set", "my-element")]
    [InlineData("cache", null, "my-element")]
    [InlineData("cache", "my-set", null)]
    public async Task SetAddAsync_NullChecksString_ThrowsException(string cacheName, string setName, string element)
    {
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.SetAddAsync(cacheName, setName, element, false));
    }

    [Fact]
    public async Task SetAddFetch_ElementIsString_HappyPath()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidString();

        await client.SetAddAsync(cacheName, setName, element, false);

        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        var hitResponse = (CacheSetFetchResponse.Hit)fetchResponse;

        var set = hitResponse.StringSet();
        Assert.Single(set);
        Assert.Contains(element, set);
    }

    [Fact]
    public async Task SetAddFetch_ElementIsString_NoRefreshTtl()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidString();

        await client.SetAddAsync(cacheName, setName, element, false, ttlSeconds: 5);
        await Task.Delay(100);

        await client.SetAddAsync(cacheName, setName, element, false, ttlSeconds: 10);
        await Task.Delay(4900);

        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True((CacheSetFetchResponse.Miss)fetchResponse is CacheSetFetchResponse.Miss);
    }

    [Fact]
    public async Task SetAddFetch_ElementIsString_RefreshTtl()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidString();

        await client.SetAddAsync(cacheName, setName, element, false, ttlSeconds: 2);
        await client.SetAddAsync(cacheName, setName, element, true, ttlSeconds: 10);
        await Task.Delay(2000);

        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        var hitResponse = (CacheSetFetchResponse.Hit)fetchResponse;
        Assert.Single(hitResponse.StringSet());
    }

    [Fact]
    public async Task SetAddBatchAsync_NullChecksByteArray_ThrowsException()
    {
        var setName = Utils.NewGuidString();
        var set = new HashSet<byte[]>();
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.SetAddBatchAsync(null!, setName, set, false));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.SetAddBatchAsync(cacheName, null!, set, false));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.SetAddBatchAsync(cacheName, setName, (IEnumerable<byte[]>)null!, false));

        set.Add(null!);
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.SetAddBatchAsync(cacheName, setName, set, false));
    }

    [Fact]
    public async Task SetAddBatchAsync_ElementsAreByteArrayEnumerable_HappyPath()
    {
        var setName = Utils.NewGuidString();
        var element1 = Utils.NewGuidByteArray();
        var element2 = Utils.NewGuidByteArray();
        var content = new List<byte[]>() { element1, element2 };

        await client.SetAddBatchAsync(cacheName, setName, content, false, 10);

        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        var hitResponse = (CacheSetFetchResponse.Hit)fetchResponse;

        var set = hitResponse.ByteArraySet;
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

        await client.SetAddBatchAsync(cacheName, setName, content, false, ttlSeconds: 5);
        await Task.Delay(100);

        await client.SetAddBatchAsync(cacheName, setName, content, false, ttlSeconds: 10);
        await Task.Delay(4900);

        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True((CacheSetFetchResponse.Miss)fetchResponse is CacheSetFetchResponse.Miss);
    }

    [Fact]
    public async Task SetAddBatchAsync_ElementsAreByteArrayEnumerable_RefreshTtl()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidByteArray();
        var content = new List<byte[]>() { element };

        await client.SetAddBatchAsync(cacheName, setName, content, false, ttlSeconds: 2);
        await client.SetAddBatchAsync(cacheName, setName, content, true, ttlSeconds: 10);
        await Task.Delay(2000);

        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        var hitResponse = (CacheSetFetchResponse.Hit)fetchResponse;

        var set = hitResponse.ByteArraySet;
        Assert.Single(set);
        Assert.Contains(element, set);
    }

    [Fact]
    public async Task SetAddBatchAsync_NullChecksString_ThrowsException()
    {
        var setName = Utils.NewGuidString();
        var set = new HashSet<string>();
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.SetAddBatchAsync(null!, setName, set, false));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.SetAddBatchAsync(cacheName, null!, set, false));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.SetAddBatchAsync(cacheName, setName, (IEnumerable<string>)null!, false));

        set.Add(null!);
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.SetAddBatchAsync(cacheName, setName, set, false));
    }

    [Fact]
    public async Task SetAddBatchAsync_ElementsAreStringEnumerable_HappyPath()
    {
        var setName = Utils.NewGuidString();
        var element1 = Utils.NewGuidString();
        var element2 = Utils.NewGuidString();
        var content = new List<string>() { element1, element2 };

        await client.SetAddBatchAsync(cacheName, setName, content, false, 10);

        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        var hitResponse = (CacheSetFetchResponse.Hit)fetchResponse;

        var set = hitResponse.StringSet();
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

        await client.SetAddBatchAsync(cacheName, setName, content, false, ttlSeconds: 5);
        await Task.Delay(100);

        await client.SetAddBatchAsync(cacheName, setName, content, false, ttlSeconds: 10);
        await Task.Delay(4900);

        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True((CacheSetFetchResponse.Miss)fetchResponse is CacheSetFetchResponse.Miss);
    }

    [Fact]
    public async Task SetAddBatchAsync_ElementsAreStringEnumerable_RefreshTtl()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidString();
        var content = new List<string>() { element };

        await client.SetAddBatchAsync(cacheName, setName, content, false, ttlSeconds: 2);
        await client.SetAddBatchAsync(cacheName, setName, content, true, ttlSeconds: 10);
        await Task.Delay(2000);

        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        var hitResponse = (CacheSetFetchResponse.Hit)fetchResponse;

        var set = hitResponse.StringSet();
        Assert.Single(set);
        Assert.Contains(element, set);
    }

    [Theory]
    [InlineData(null, "my-set", new byte[] { 0x00 })]
    [InlineData("cache", null, new byte[] { 0x00 })]
    [InlineData("cache", "my-set", null)]
    public async Task SetRemoveElementAsync_NullChecksByteArray_ThrowsException(string cacheName, string setName, byte[] element)
    {
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.SetRemoveElementAsync(cacheName, setName, element));
    }

    [Fact]
    public async Task SetRemoveElementAsync_ElementIsByteArray_HappyPath()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidByteArray();

        await client.SetAddAsync(cacheName, setName, element, false);

        // Remove element that is not there -- no-op
        await client.SetRemoveElementAsync(cacheName, setName, Utils.NewGuidByteArray());
        // Fetch the whole set and make sure response has element we expect 
        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        var hitResponse = (CacheSetFetchResponse.Hit)fetchResponse;
        var set = hitResponse.ByteArraySet;
        Assert.Single(set);
        Assert.Contains(element, set);

        // Remove element
        await client.SetRemoveElementAsync(cacheName, setName, element);
        fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True((CacheSetFetchResponse.Miss)fetchResponse is CacheSetFetchResponse.Miss);
    }

    [Fact]
    public async Task SetRemoveElementAsync_SetIsMissingElementIsByteArray_Noop()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidString();

        // Pre-condition: set is missing
        Assert.True((CacheSetFetchResponse.Miss)(await client.SetFetchAsync(cacheName, setName)) is CacheSetFetchResponse.Miss);

        // Remove element that is not there -- no-op
        await client.SetRemoveElementAsync(cacheName, setName, Utils.NewGuidByteArray());

        // Post-condition: set is still missing
        Assert.True((CacheSetFetchResponse.Miss)(await client.SetFetchAsync(cacheName, setName)) is CacheSetFetchResponse.Miss);
    }

    [Theory]
    [InlineData(null, "my-set", "my-element")]
    [InlineData("cache", null, "my-element")]
    [InlineData("cache", "my-set", null)]
    public async Task SetRemoveElementAsync_NullChecksString_ThrowsException(string cacheName, string setName, string element)
    {
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.SetRemoveElementAsync(cacheName, setName, element));
    }

    [Fact]
    public async Task SetRemoveElementAsync_ElementIsString_HappyPath()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidString();

        await client.SetAddAsync(cacheName, setName, element, false);

        // Remove element that is not there -- no-op
        await client.SetRemoveElementAsync(cacheName, setName, Utils.NewGuidString());
        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        var hitResponse = (CacheSetFetchResponse.Hit)fetchResponse
        var set = hitResponse.StringSet();
        Assert.Single(set);
        Assert.Contains(element, set);

        // Remove element
        await client.SetRemoveElementAsync(cacheName, setName, element);
        fetchResponse = await client.SetFetchAsync(cacheName, setName);
        Assert.True((CacheSetFetchResponse.Miss)fetchResponse is CacheSetFetchResponse.Miss);
    }

    [Fact]
    public async Task SetRemoveElementAsync_SetIsMissingElementIsString_Noop()
    {
        var setName = Utils.NewGuidString();
        var element = Utils.NewGuidString();

        // Pre-condition: set is missing
        Assert.True((CacheSetFetchResponse.Miss)(await client.SetFetchAsync(cacheName, setName)) is CacheSetFetchResponse.Miss);

        // Remove element that is not there -- no-op
        await client.SetRemoveElementAsync(cacheName, setName, Utils.NewGuidString());

        // Post-condition: set is still missing
        Assert.True((CacheSetFetchResponse.Miss)(await client.SetFetchAsync(cacheName, setName)) is CacheSetFetchResponse.Miss);
    }

    [Fact]
    public async Task SetRemoveElementsAsync_NullChecksElementsAreByteArray_ThrowsException()
    {
        var setName = Utils.NewGuidString();
        var testData = new byte[][][] { new byte[][] { Utils.NewGuidByteArray(), Utils.NewGuidByteArray() }, new byte[][] { Utils.NewGuidByteArray(), null! } };

        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.SetRemoveElementsAsync(null!, setName, testData[0]));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.SetRemoveElementsAsync(cacheName, null!, testData[0]));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.SetRemoveElementsAsync(cacheName, setName, (byte[][])null!));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.SetRemoveElementsAsync(cacheName, setName, testData[1]));

        var fieldsList = new List<byte[]>(testData[0]);
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.SetRemoveElementsAsync(null!, setName, fieldsList));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.SetRemoveElementsAsync(cacheName, null!, fieldsList));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.SetRemoveElementsAsync(cacheName, setName, (List<byte[]>)null!));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.SetRemoveElementsAsync(cacheName, setName, new List<byte[]>(testData[1])));
    }

    [Fact]
    public async Task SetRemoveElementsAsync_FieldsAreByteArray_HappyPath()
    {
        var setName = Utils.NewGuidString();
        var elements = new byte[][] { Utils.NewGuidByteArray(), Utils.NewGuidByteArray() };
        var otherElement = Utils.NewGuidByteArray();

        // Test enumerable
        await client.SetAddAsync(cacheName, setName, elements[0], false);
        await client.SetAddAsync(cacheName, setName, elements[1], false);
        await client.SetAddAsync(cacheName, setName, otherElement, false);

        var elementsList = new List<byte[]>(elements);
        await client.SetRemoveElementsAsync(cacheName, setName, elementsList);
        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        var hitResponse = (CacheSetFetchResponse.Hit)fetchResponse;
        Assert.Single(hitResponse.ByteArraySet!);
        Assert.Contains(otherElement, hitResponse.ByteArraySet!);
    }

    [Fact]
    public async Task SetRemoveElementsAsync_NullChecksElementsAreString_ThrowsException()
    {
        var setName = Utils.NewGuidString();
        var testData = new string[][] { new string[] { Utils.NewGuidString(), Utils.NewGuidString() }, new string[] { Utils.NewGuidString(), null! } };

        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.SetRemoveElementsAsync(null!, setName, testData[0]));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.SetRemoveElementsAsync(cacheName, null!, testData[0]));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.SetRemoveElementsAsync(cacheName, setName, (byte[][])null!));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.SetRemoveElementsAsync(cacheName, setName, testData[1]));

        var elementsList = new List<string>(testData[0]);
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.SetRemoveElementsAsync(null!, setName, elementsList));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.SetRemoveElementsAsync(cacheName, null!, elementsList));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.SetRemoveElementsAsync(cacheName, setName, (List<string>)null!));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.SetRemoveElementsAsync(cacheName, setName, new List<string>(testData[1])));
    }

    [Fact]
    public async Task SetRemoveElementsAsync_FieldsAreString_HappyPath()
    {
        var setName = Utils.NewGuidString();
        var elements = new string[] { Utils.NewGuidString(), Utils.NewGuidString() };
        var otherElement = Utils.NewGuidByteArray();

        // Test enumerable
        await client.SetAddAsync(cacheName, setName, elements[0], false);
        await client.SetAddAsync(cacheName, setName, elements[1], false);
        await client.SetAddAsync(cacheName, setName, otherElement, false);

        var elementsList = new List<string>(elements);
        await client.SetRemoveElementsAsync(cacheName, setName, elementsList);
        CacheSetFetchResponse fetchResponse = await client.SetFetchAsync(cacheName, setName);
        var hitResponse = (CacheSetFetchResponse.Hit)fetchResponse;
        Assert.Single(hitResponse.ByteArraySet!);
        Assert.Contains(otherElement, hitResponse.ByteArraySet!);
    }

    [Theory]
    [InlineData(null, "my-set")]
    [InlineData("cache", null)]
    public async Task SetFetchAsync_NullChecks_ThrowsException(string cacheName, string setName)
    {
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.SetFetchAsync(cacheName, setName));
    }

    [Fact]
    public async Task SetFetchAsync_Missing_HappyPath()
    {
        var setName = Utils.NewGuidString();
        CacheSetFetchResponse response = await client.SetFetchAsync(cacheName, setName);
        Assert.True((CacheSetFetchResponse.Miss)response is CacheSetFetchResponse.Miss);
        var misResponse = (CacheSetFetchResponse.Miss)response;
        Assert.Null(misResponse.ByteArraySet);
        Assert.Null(misResponse.StringSet());
    }

    [Fact]
    public async Task SetFetchAsync_UsesCachedByteArraySet_HappyPath()
    {
        var setName = Utils.NewGuidString();
        await client.SetAddBatchAsync(cacheName, setName, new string[] { Utils.NewGuidString(), Utils.NewGuidString() }, false);
        CacheSetFetchResponse response = await client.SetFetchAsync(cacheName, setName);
        var hitResponse = (CacheSetFetchResponse.Hit)response;
        var set1 = hitResponse.ByteArraySet;
        var set2 = hitResponse.ByteArraySet;
        Assert.Same(set1, set2);
    }

    [Fact]
    public async Task SetFetchAsync_UsesCachedStringSet_HappyPath()
    {
        var setName = Utils.NewGuidString();
        await client.SetAddBatchAsync(cacheName, setName, new string[] { Utils.NewGuidString(), Utils.NewGuidString() }, false);
        CacheSetFetchResponse response = await client.SetFetchAsync(cacheName, setName);
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
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.SetDeleteAsync(cacheName, setName));
    }

    [Fact]
    public async Task SetDeleteAsync_SetDoesNotExist_Noop()
    {
        var setName = Utils.NewGuidString();
        Assert.True((CacheSetFetchResponse.Miss)(await client.SetFetchAsync(cacheName, setName)) is CacheSetFetchResponse.Miss);
        await client.SetDeleteAsync(cacheName, setName);
        Assert.True((CacheSetFetchResponse.Miss)(await client.SetFetchAsync(cacheName, setName)) is CacheSetFetchResponse.Miss);
    }

    [Fact]
    public async Task SetDeleteAsync_SetExists_HappyPath()
    {
        var setName = Utils.NewGuidString();
        await client.SetAddAsync(cacheName, setName, Utils.NewGuidString(), false);
        await client.SetAddAsync(cacheName, setName, Utils.NewGuidString(), false);
        await client.SetAddAsync(cacheName, setName, Utils.NewGuidString(), false);

        Assert.True((CacheSetFetchResponse.Hit)(await client.SetFetchAsync(cacheName, setName)) is CacheSetFetchResponse.Hit);
        await client.SetDeleteAsync(cacheName, setName);
        Assert.True((CacheSetFetchResponse.Miss)(await client.SetFetchAsync(cacheName, setName)) is CacheSetFetchResponse.Miss);
    }
}
