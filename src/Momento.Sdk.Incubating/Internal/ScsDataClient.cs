using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Momento.Protos.CacheClient;
using Momento.Sdk.Config;
using Momento.Sdk.Incubating.Requests;
using Momento.Sdk.Incubating.Responses;
using Momento.Sdk.Internal;
using Momento.Sdk.Internal.ExtensionMethods;

namespace Momento.Sdk.Incubating.Internal;

internal sealed class ScsDataClient : ScsDataClientBase
{
    public ScsDataClient(IConfiguration config, string authToken, string endpoint, TimeSpan defaultTtl)
        : base(config, authToken, endpoint, defaultTtl)
    {
    }

    // NB: we exclude this from the build; once we have server-side support we will re-enable and change appropriately
#if USE_UNARY_BATCH
    public async Task<CacheGetBatchResponse> GetBatchAsync(ISimpleCacheClient simpleCacheClient, string cacheName, IEnumerable<string> keys)
    {
        // Gather the tasks
        var tasks = keys.Select(key => simpleCacheClient.GetAsync(cacheName, key));
        return await SendGetBatchAsync(tasks);
    }

    public async Task<CacheGetBatchResponse> GetBatchAsync(ISimpleCacheClient simpleCacheClient, string cacheName, IEnumerable<byte[]> keys)
    {
        // Gather the tasks
        var tasks = keys.Select(key => simpleCacheClient.GetAsync(cacheName, key));
        return await SendGetBatchAsync(tasks);
    }

    public async Task<CacheGetBatchResponse> SendGetBatchAsync(IEnumerable<Task<CacheGetResponse>> tasks)
    {

        // Run the tasks
        var continuation = Task.WhenAll(tasks);
        try
        {
            await continuation;
        }
        catch (Exception e)
        {
            return new CacheGetBatchResponse.Error(_exceptionMapper.Convert(e));
        }

        // Handle failures
        if (continuation.Status == TaskStatus.Faulted)
        {
            return new CacheGetBatchResponse.Error(
                _exceptionMapper.Convert(continuation.Exception)
            );
        }
        else if (continuation.Status != TaskStatus.RanToCompletion)
        {
            return new CacheGetBatchResponse.Error(
                _exceptionMapper.Convert(
                    new Exception(String.Format("Failure issuing multi-get: {0}", continuation.Status))
                )
            );
        }

        // preserve old behavior of failing on first error
        foreach (CacheGetResponse response in continuation.Result)
        {
            if (response is CacheGetResponse.Error errorResponse)
            {
                return new CacheGetBatchResponse.Error(errorResponse.InnerException);
            }
        }

        // Package results
        return new CacheGetBatchResponse.Success(continuation.Result);
    }

    public async Task<CacheSetBatchResponse> SetBatchAsync(ISimpleCacheClient simpleCacheClient, string cacheName, IEnumerable<KeyValuePair<string, string>> items, TimeSpan? ttl = null)
    {
        // Gather the tasks
        var tasks = items.Select(item => simpleCacheClient.SetAsync(cacheName, item.Key, item.Value, ttl));
        return await SendSetBatchAsync(tasks);
    }

    public async Task<CacheSetBatchResponse> SetBatchAsync(ISimpleCacheClient simpleCacheClient, string cacheName, IEnumerable<KeyValuePair<byte[], byte[]>> items, TimeSpan? ttl = null)
    {
        // Gather the tasks
        var tasks = items.Select(item => simpleCacheClient.SetAsync(cacheName, item.Key, item.Value, ttl));
        return await SendSetBatchAsync(tasks);
    }

    public async Task<CacheSetBatchResponse> SendSetBatchAsync(IEnumerable<Task<CacheSetResponse>> tasks)
    {
        // Run the tasks
        var continuation = Task.WhenAll(tasks);
        try
        {
            await continuation;
        }
        catch (Exception e)
        {
            return new CacheSetBatchResponse.Error(
                _exceptionMapper.Convert(e)
            );
        }

        // Handle failures
        if (continuation.Status == TaskStatus.Faulted)
        {
            return new CacheSetBatchResponse.Error(
                _exceptionMapper.Convert(continuation.Exception)
            );
        }
        else if (continuation.Status != TaskStatus.RanToCompletion)
        {
            return new CacheSetBatchResponse.Error(
                _exceptionMapper.Convert(
                    new Exception(String.Format("Failure issuing multi-set: {0}", continuation.Status))
                )
            );
        }
        return new CacheSetBatchResponse.Success();
    }
#endif

    private _DictionaryFieldValuePair[] ToSingletonFieldValuePair(byte[] field, byte[] value) => new _DictionaryFieldValuePair[] { new _DictionaryFieldValuePair() { Field = field.ToByteString(), Value = value.ToByteString() } };
    private _DictionaryFieldValuePair[] ToSingletonFieldValuePair(string field, string value) => new _DictionaryFieldValuePair[] { new _DictionaryFieldValuePair() { Field = field.ToByteString(), Value = value.ToByteString() } };
    private _DictionaryFieldValuePair[] ToSingletonFieldValuePair(string field, byte[] value) => new _DictionaryFieldValuePair[] { new _DictionaryFieldValuePair() { Field = field.ToByteString(), Value = value.ToByteString() } };

    public async Task<CacheDictionaryFetchResponse> DictionaryFetchAsync(string cacheName, string dictionaryName)
    {
        return await SendDictionaryFetchAsync(cacheName, dictionaryName);
    }

    public async Task<CacheDictionaryGetFieldResponse> DictionaryGetFieldAsync(string cacheName, string dictionaryName, byte[] field)
    {
        return await SendDictionaryGetFieldAsync(cacheName, dictionaryName, field.ToSingletonByteString());
    }

    public async Task<CacheDictionaryGetFieldResponse> DictionaryGetFieldAsync(string cacheName, string dictionaryName, string field)
    {
        return await SendDictionaryGetFieldAsync(cacheName, dictionaryName, field.ToSingletonByteString());
    }

    public async Task<CacheDictionaryGetFieldsResponse> DictionaryGetFieldsAsync(string cacheName, string dictionaryName, IEnumerable<byte[]> fields)
    {
        return await SendDictionaryGetFieldsAsync(cacheName, dictionaryName, fields.ToEnumerableByteString());
    }

    public async Task<CacheDictionaryGetFieldsResponse> DictionaryGetFieldsAsync(string cacheName, string dictionaryName, IEnumerable<string> fields)
    {
        return await SendDictionaryGetFieldsAsync(cacheName, dictionaryName, fields.ToEnumerableByteString());
    }

    public async Task<CacheDictionarySetFieldResponse> DictionarySetFieldAsync(string cacheName, string dictionaryName, byte[] field, byte[] value, CollectionTtl ttl = default(CollectionTtl))
    {
        return await SendDictionarySetFieldAsync(cacheName, dictionaryName, ToSingletonFieldValuePair(field, value), ttl);
    }

    public async Task<CacheDictionarySetFieldResponse> DictionarySetFieldAsync(string cacheName, string dictionaryName, string field, string value, CollectionTtl ttl = default(CollectionTtl))
    {
        return await SendDictionarySetFieldAsync(cacheName, dictionaryName, ToSingletonFieldValuePair(field, value), ttl);
    }

    public async Task<CacheDictionarySetFieldResponse> DictionarySetFieldAsync(string cacheName, string dictionaryName, string field, byte[] value, CollectionTtl ttl = default(CollectionTtl))
    {
        return await SendDictionarySetFieldAsync(cacheName, dictionaryName, ToSingletonFieldValuePair(field, value), ttl);
    }

    public async Task<CacheDictionarySetFieldsResponse> DictionarySetFieldsAsync(string cacheName, string dictionaryName, IEnumerable<KeyValuePair<byte[], byte[]>> items, CollectionTtl ttl = default(CollectionTtl))
    {
        var protoItems = items.Select(kv => new _DictionaryFieldValuePair() { Field = kv.Key.ToByteString(), Value = kv.Value.ToByteString() });
        return await SendDictionarySetFieldsAsync(cacheName, dictionaryName, protoItems, ttl);
    }

    public async Task<CacheDictionarySetFieldsResponse> DictionarySetFieldsAsync(string cacheName, string dictionaryName, IEnumerable<KeyValuePair<string, string>> items, CollectionTtl ttl = default(CollectionTtl))
    {
        var protoItems = items.Select(kv => new _DictionaryFieldValuePair() { Field = kv.Key.ToByteString(), Value = kv.Value.ToByteString() });
        return await SendDictionarySetFieldsAsync(cacheName, dictionaryName, protoItems, ttl);
    }

    public async Task<CacheDictionarySetFieldsResponse> DictionarySetFieldsAsync(string cacheName, string dictionaryName, IEnumerable<KeyValuePair<string, byte[]>> items, CollectionTtl ttl = default(CollectionTtl))
    {
        var protoItems = items.Select(kv => new _DictionaryFieldValuePair() { Field = kv.Key.ToByteString(), Value = kv.Value.ToByteString() });
        return await SendDictionarySetFieldsAsync(cacheName, dictionaryName, protoItems, ttl);
    }

    public async Task<CacheDictionaryIncrementResponse> DictionaryIncrementAsync(string cacheName, string dictionaryName, string field, long amount = 1, CollectionTtl ttl = default(CollectionTtl))
    {
        return await SendDictionaryIncrementAsync(cacheName, dictionaryName, field, amount, ttl);
    }

    public async Task<CacheDictionaryDeleteResponse> DictionaryDeleteAsync(string cacheName, string dictionaryName)
    {
        return await SendDictionaryDeleteAsync(cacheName, dictionaryName);
    }

    public async Task<CacheDictionaryRemoveFieldResponse> DictionaryRemoveFieldAsync(string cacheName, string dictionaryName, byte[] field)
    {
        return await SendDictionaryRemoveFieldAsync(cacheName, dictionaryName, field.ToByteString());
    }

    public async Task<CacheDictionaryRemoveFieldResponse> DictionaryRemoveFieldAsync(string cacheName, string dictionaryName, string field)
    {
        return await SendDictionaryRemoveFieldAsync(cacheName, dictionaryName, field.ToByteString());
    }

    public async Task<CacheDictionaryRemoveFieldsResponse> DictionaryRemoveFieldsAsync(string cacheName, string dictionaryName, IEnumerable<byte[]> fields)
    {
        return await SendDictionaryRemoveFieldsAsync(cacheName, dictionaryName, fields.ToEnumerableByteString());
    }

    public async Task<CacheDictionaryRemoveFieldsResponse> DictionaryRemoveFieldsAsync(string cacheName, string dictionaryName, IEnumerable<string> fields)
    {
        return await SendDictionaryRemoveFieldsAsync(cacheName, dictionaryName, fields.ToEnumerableByteString());
    }

    public async Task<CacheSetAddElementResponse> SetAddElementAsync(string cacheName, string setName, byte[] element, CollectionTtl ttl = default(CollectionTtl))
    {
        return await SendSetAddElementAsync(cacheName, setName, element.ToSingletonByteString(), ttl);
    }

    public async Task<CacheSetAddElementResponse> SetAddElementAsync(string cacheName, string setName, string element, CollectionTtl ttl = default(CollectionTtl))
    {
        return await SendSetAddElementAsync(cacheName, setName, element.ToSingletonByteString(), ttl);
    }

    public async Task<CacheSetAddElementsResponse> SetAddElementsAsync(string cacheName, string setName, IEnumerable<byte[]> elements, CollectionTtl ttl = default(CollectionTtl))
    {
        return await SendSetAddElementsAsync(cacheName, setName, elements.ToEnumerableByteString(), ttl);
    }

    public async Task<CacheSetAddElementsResponse> SetAddElementsAsync(string cacheName, string setName, IEnumerable<string> elements, CollectionTtl ttl = default(CollectionTtl))
    {
        return await SendSetAddElementsAsync(cacheName, setName, elements.ToEnumerableByteString(), ttl);
    }

    public async Task<CacheSetRemoveElementResponse> SetRemoveElementAsync(string cacheName, string setName, byte[] element)
    {
        return await SendSetRemoveElementAsync(cacheName, setName, element.ToSingletonByteString());
    }

    public async Task<CacheSetRemoveElementResponse> SetRemoveElementAsync(string cacheName, string setName, string element)
    {
        return await SendSetRemoveElementAsync(cacheName, setName, element.ToSingletonByteString());
    }

    public async Task<CacheSetRemoveElementsResponse> SetRemoveElementsAsync(string cacheName, string setName, IEnumerable<byte[]> elements)
    {
        return await SendSetRemoveElementsAsync(cacheName, setName, elements.ToEnumerableByteString());
    }

    public async Task<CacheSetRemoveElementsResponse> SetRemoveElementsAsync(string cacheName, string setName, IEnumerable<string> elements)
    {
        return await SendSetRemoveElementsAsync(cacheName, setName, elements.ToEnumerableByteString());
    }

    public async Task<CacheSetFetchResponse> SetFetchAsync(string cacheName, string setName)
    {
        return await SendSetFetchAsync(cacheName, setName);
    }

    public async Task<CacheSetDeleteResponse> SetDeleteAsync(string cacheName, string setName)
    {
        return await SendSetDeleteAsync(cacheName, setName);
    }

    public async Task<CacheListPushFrontResponse> ListPushFrontAsync(string cacheName, string listName, byte[] value, int? truncateBackToSize = null, CollectionTtl ttl = default(CollectionTtl))
    {
        return await SendListPushFrontAsync(cacheName, listName, value.ToByteString(), truncateBackToSize, ttl);
    }

    public async Task<CacheListPushFrontResponse> ListPushFrontAsync(string cacheName, string listName, string value, int? truncateBackToSize = null, CollectionTtl ttl = default(CollectionTtl))
    {
        return await SendListPushFrontAsync(cacheName, listName, value.ToByteString(), truncateBackToSize, ttl);
    }

    public async Task<CacheListPushBackResponse> ListPushBackAsync(string cacheName, string listName, byte[] value, int? truncateFrontToSize = null, CollectionTtl ttl = default(CollectionTtl))
    {
        return await SendListPushBackAsync(cacheName, listName, value.ToByteString(), truncateFrontToSize, ttl);
    }

    public async Task<CacheListPushBackResponse> ListPushBackAsync(string cacheName, string listName, string value, int? truncateFrontToSize = null, CollectionTtl ttl = default(CollectionTtl))
    {
        return await SendListPushBackAsync(cacheName, listName, value.ToByteString(), truncateFrontToSize, ttl);
    }

    public async Task<CacheListPopFrontResponse> ListPopFrontAsync(string cacheName, string listName)
    {
        return await SendListPopFrontAsync(cacheName, listName);
    }

    public async Task<CacheListPopBackResponse> ListPopBackAsync(string cacheName, string listName)
    {
        return await SendListPopBackAsync(cacheName, listName);
    }

    public async Task<CacheListFetchResponse> ListFetchAsync(string cacheName, string listName)
    {
        return await SendListFetchAsync(cacheName, listName);
    }

    public async Task<CacheListRemoveValueResponse> ListRemoveValueAsync(string cacheName, string listName, byte[] value)
    {
        return await SendListRemoveValueAsync(cacheName, listName, value.ToByteString());
    }

    public async Task<CacheListRemoveValueResponse> ListRemoveValueAsync(string cacheName, string listName, string value)
    {
        return await SendListRemoveValueAsync(cacheName, listName, value.ToByteString());
    }

    public async Task<CacheListLengthResponse> ListLengthAsync(string cacheName, string listName)
    {
        return await SendListLengthAsync(cacheName, listName);
    }
    
    public async Task<CacheListDeleteResponse> ListDeleteAsync(string cacheName, string listName)
    {
        return await SendListDeleteAsync(cacheName, listName);
    }


    /***************************************************************************
     * Private "Send" methods"
     **************************************************************************/

    private async Task<CacheDictionaryFetchResponse> SendDictionaryFetchAsync(string cacheName, string dictionaryName)
    {
        _DictionaryFetchRequest request = new() { DictionaryName = dictionaryName.ToByteString() };
        _DictionaryFetchResponse response;
        var metadata = MetadataWithCache(cacheName);

        try
        {
            response = await this.grpcManager.Client.DictionaryFetchAsync(request, new CallOptions(headers: metadata, deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return new CacheDictionaryFetchResponse.Error(_exceptionMapper.Convert(e, metadata));
        }

        if (response.DictionaryCase == _DictionaryFetchResponse.DictionaryOneofCase.Found)
        {
            return new CacheDictionaryFetchResponse.Hit(response);
        }

        return new CacheDictionaryFetchResponse.Miss();
    }


    private async Task<CacheDictionaryGetFieldResponse> SendDictionaryGetFieldAsync(string cacheName, string dictionaryName, IEnumerable<ByteString> fields)
    {
        _DictionaryGetRequest request = new() { DictionaryName = dictionaryName.ToByteString() };
        request.Fields.Add(fields);
        _DictionaryGetResponse response;
        var metadata = MetadataWithCache(cacheName);

        try
        {
            response = await this.grpcManager.Client.DictionaryGetAsync(request, new CallOptions(headers: metadata, deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return new CacheDictionaryGetFieldResponse.Error(_exceptionMapper.Convert(e, metadata));
        }

        if (response.DictionaryCase == _DictionaryGetResponse.DictionaryOneofCase.Missing)
        {
            return new CacheDictionaryGetFieldResponse.Miss();
        }

        if (response.Found.Items.Count == 0)
        {
            var exc = _exceptionMapper.Convert(new Exception("_DictionaryGetResponseResponse contained no data but was found"), metadata);
            return new CacheDictionaryGetFieldResponse.Error(exc);
        }

        if (response.Found.Items[0].Result == ECacheResult.Miss)
        {
            return new CacheDictionaryGetFieldResponse.Miss();
        }

        return new CacheDictionaryGetFieldResponse.Hit(response);
    }

    private async Task<CacheDictionaryGetFieldsResponse> SendDictionaryGetFieldsAsync(string cacheName, string dictionaryName, IEnumerable<ByteString> fields)
    {
        _DictionaryGetRequest request = new() { DictionaryName = dictionaryName.ToByteString() };
        request.Fields.Add(fields);
        _DictionaryGetResponse response;
        var metadata = MetadataWithCache(cacheName);

        try
        {
            response = await this.grpcManager.Client.DictionaryGetAsync(request, new CallOptions(headers: metadata, deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return new CacheDictionaryGetFieldsResponse.Error(_exceptionMapper.Convert(e, metadata));
        }

        if (response.DictionaryCase == _DictionaryGetResponse.DictionaryOneofCase.Found)
        {
            return new CacheDictionaryGetFieldsResponse.Hit(fields, response);
        }

        return new CacheDictionaryGetFieldsResponse.Miss();
    }

    private async Task<CacheDictionarySetFieldResponse> SendDictionarySetFieldAsync(string cacheName, string dictionaryName, IEnumerable<_DictionaryFieldValuePair> items, CollectionTtl ttl)
    {
        _DictionarySetRequest request = new()
        {
            DictionaryName = dictionaryName.ToByteString(),
            RefreshTtl = ttl.RefreshTtl,
            TtlMilliseconds = TtlToMilliseconds(ttl.Ttl)
        };
        request.Items.Add(items);
        var metadata = MetadataWithCache(cacheName);

        try
        {
            await this.grpcManager.Client.DictionarySetAsync(request, new CallOptions(headers: metadata, deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return new CacheDictionarySetFieldResponse.Error(_exceptionMapper.Convert(e, metadata));
        }

        return new CacheDictionarySetFieldResponse.Success();
    }

    private async Task<CacheDictionarySetFieldsResponse> SendDictionarySetFieldsAsync(string cacheName, string dictionaryName, IEnumerable<_DictionaryFieldValuePair> items, CollectionTtl ttl)
    {
        _DictionarySetRequest request = new()
        {
            DictionaryName = dictionaryName.ToByteString(),
            RefreshTtl = ttl.RefreshTtl,
            TtlMilliseconds = TtlToMilliseconds(ttl.Ttl)
        };
        request.Items.Add(items);
        var metadata = MetadataWithCache(cacheName);

        try
        {
            await this.grpcManager.Client.DictionarySetAsync(request, new CallOptions(headers: metadata, deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return new CacheDictionarySetFieldsResponse.Error(_exceptionMapper.Convert(e, metadata));
        }

        return new CacheDictionarySetFieldsResponse.Success();
    }


    private async Task<CacheDictionaryIncrementResponse> SendDictionaryIncrementAsync(string cacheName, string dictionaryName, string field, long amount, CollectionTtl ttl)
    {
        _DictionaryIncrementRequest request = new()
        {
            DictionaryName = dictionaryName.ToByteString(),
            Field = field.ToByteString(),
            Amount = amount,
            RefreshTtl = ttl.RefreshTtl,
            TtlMilliseconds = TtlToMilliseconds(ttl.Ttl)
        };
        _DictionaryIncrementResponse response;
        var metadata = MetadataWithCache(cacheName);

        try
        {
            response = await this.grpcManager.Client.DictionaryIncrementAsync(request, new CallOptions(headers: metadata, deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return new CacheDictionaryIncrementResponse.Error(_exceptionMapper.Convert(e, metadata));
        }

        return new CacheDictionaryIncrementResponse.Success(response);
    }

    private async Task<CacheDictionaryDeleteResponse> SendDictionaryDeleteAsync(string cacheName, string dictionaryName)
    {
        _DictionaryDeleteRequest request = new()
        {
            DictionaryName = dictionaryName.ToByteString(),
            All = new()
        };
        var metadata = MetadataWithCache(cacheName);

        try
        {
            await this.grpcManager.Client.DictionaryDeleteAsync(request, new CallOptions(headers: metadata, deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return new CacheDictionaryDeleteResponse.Error(_exceptionMapper.Convert(e, metadata));
        }

        return new CacheDictionaryDeleteResponse.Success();
    }

    private async Task<CacheDictionaryRemoveFieldResponse> SendDictionaryRemoveFieldAsync(string cacheName, string dictionaryName, ByteString field)
    {
        _DictionaryDeleteRequest request = new()
        {
            DictionaryName = dictionaryName.ToByteString(),
            Some = new()
        };
        request.Some.Fields.Add(field);
        var metadata = MetadataWithCache(cacheName);

        try
        {
            await this.grpcManager.Client.DictionaryDeleteAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return new CacheDictionaryRemoveFieldResponse.Error(_exceptionMapper.Convert(e, metadata));
        }

        return new CacheDictionaryRemoveFieldResponse.Success();
    }

    private async Task<CacheDictionaryRemoveFieldsResponse> SendDictionaryRemoveFieldsAsync(string cacheName, string dictionaryName, IEnumerable<ByteString> fields)
    {
        _DictionaryDeleteRequest request = new()
        {
            DictionaryName = dictionaryName.ToByteString(),
            Some = new()
        };
        request.Some.Fields.Add(fields);
        var metadata = MetadataWithCache(cacheName);

        try
        {
            await this.grpcManager.Client.DictionaryDeleteAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return new CacheDictionaryRemoveFieldsResponse.Error(_exceptionMapper.Convert(e, metadata));
        }

        return new CacheDictionaryRemoveFieldsResponse.Success();
    }

    private async Task<CacheSetAddElementResponse> SendSetAddElementAsync(string cacheName, string setName, IEnumerable<ByteString> elements, CollectionTtl ttl)
    {
        _SetUnionRequest request = new()
        {
            SetName = setName.ToByteString(),
            RefreshTtl = ttl.RefreshTtl,
            TtlMilliseconds = TtlToMilliseconds(ttl.Ttl)
        };
        request.Elements.Add(elements);
        var metadata = MetadataWithCache(cacheName);

        try
        {
            await this.grpcManager.Client.SetUnionAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return new CacheSetAddElementResponse.Error(_exceptionMapper.Convert(e, metadata));
        }

        return new CacheSetAddElementResponse.Success();
    }

    private async Task<CacheSetAddElementsResponse> SendSetAddElementsAsync(string cacheName, string setName, IEnumerable<ByteString> elements, CollectionTtl ttl)
    {
        _SetUnionRequest request = new()
        {
            SetName = setName.ToByteString(),
            RefreshTtl = ttl.RefreshTtl,
            TtlMilliseconds = TtlToMilliseconds(ttl.Ttl)
        };
        request.Elements.Add(elements);
        var metadata = MetadataWithCache(cacheName);

        try
        {
            await this.grpcManager.Client.SetUnionAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return new CacheSetAddElementsResponse.Error(_exceptionMapper.Convert(e, metadata));
        }

        return new CacheSetAddElementsResponse.Success();
    }

    private async Task<CacheSetRemoveElementResponse> SendSetRemoveElementAsync(string cacheName, string setName, IEnumerable<ByteString> elements)
    {
        _SetDifferenceRequest request = new()
        {
            SetName = setName.ToByteString(),
            Subtrahend = new() { Set = new() }
        };
        request.Subtrahend.Set.Elements.Add(elements);
        var metadata = MetadataWithCache(cacheName);

        try
        {
            await this.grpcManager.Client.SetDifferenceAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return new CacheSetRemoveElementResponse.Error(_exceptionMapper.Convert(e, metadata));
        }

        return new CacheSetRemoveElementResponse.Success();
    }

    private async Task<CacheSetRemoveElementsResponse> SendSetRemoveElementsAsync(string cacheName, string setName, IEnumerable<ByteString> elements)
    {
        _SetDifferenceRequest request = new()
        {
            SetName = setName.ToByteString(),
            Subtrahend = new() { Set = new() }
        };
        request.Subtrahend.Set.Elements.Add(elements);
        var metadata = MetadataWithCache(cacheName);

        try
        {
            await this.grpcManager.Client.SetDifferenceAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return new CacheSetRemoveElementsResponse.Error(_exceptionMapper.Convert(e, metadata));
        }

        return new CacheSetRemoveElementsResponse.Success();
    }

    private async Task<CacheSetFetchResponse> SendSetFetchAsync(string cacheName, string setName)
    {
        _SetFetchRequest request = new() { SetName = setName.ToByteString() };
        _SetFetchResponse response;
        var metadata = MetadataWithCache(cacheName);

        try
        {
            response = await this.grpcManager.Client.SetFetchAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return new CacheSetFetchResponse.Error(_exceptionMapper.Convert(e, metadata));
        }
        if (response.SetCase == _SetFetchResponse.SetOneofCase.Found)
        {
            return new CacheSetFetchResponse.Hit(response);
        }

        return new CacheSetFetchResponse.Miss();
    }

    private async Task<CacheSetDeleteResponse> SendSetDeleteAsync(string cacheName, string setName)
    {
        _SetDifferenceRequest request = new()
        {
            SetName = setName.ToByteString(),
            Subtrahend = new() { Identity = new() }
        };
        var metadata = MetadataWithCache(cacheName);

        try
        {
            await this.grpcManager.Client.SetDifferenceAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return new CacheSetDeleteResponse.Error(_exceptionMapper.Convert(e, metadata));
        }

        return new CacheSetDeleteResponse.Success();
    }

    private async Task<CacheListPushFrontResponse> SendListPushFrontAsync(string cacheName, string listName, ByteString value, int? truncateBackToSize, CollectionTtl ttl)
    {
        _ListPushFrontRequest request = new()
        {
            TruncateBackToSize = Convert.ToUInt32(truncateBackToSize.GetValueOrDefault()),
            ListName = listName.ToByteString(),
            Value = value,
            RefreshTtl = ttl.RefreshTtl,
            TtlMilliseconds = TtlToMilliseconds(ttl.Ttl)
        };
        _ListPushFrontResponse response;
        var metadata = MetadataWithCache(cacheName);

        try
        {
            response = await this.grpcManager.Client.ListPushFrontAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return new CacheListPushFrontResponse.Error(_exceptionMapper.Convert(e, metadata));
        }

        return new CacheListPushFrontResponse.Success(response);
    }

    private async Task<CacheListPushBackResponse> SendListPushBackAsync(string cacheName, string listName, ByteString value, int? truncateFrontToSize, CollectionTtl ttl)
    {
        _ListPushBackRequest request = new()
        {
            TruncateFrontToSize = Convert.ToUInt32(truncateFrontToSize.GetValueOrDefault()),
            ListName = listName.ToByteString(),
            Value = value,
            RefreshTtl = ttl.RefreshTtl,
            TtlMilliseconds = TtlToMilliseconds(ttl.Ttl)
        };
        _ListPushBackResponse response;
        var metadata = MetadataWithCache(cacheName);

        try
        {
            response = await this.grpcManager.Client.ListPushBackAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return new CacheListPushBackResponse.Error(_exceptionMapper.Convert(e, metadata));
        }

        return new CacheListPushBackResponse.Success(response);
    }

    private async Task<CacheListPopFrontResponse> SendListPopFrontAsync(string cacheName, string listName)
    {
        _ListPopFrontRequest request = new() { ListName = listName.ToByteString() };
        _ListPopFrontResponse response;
        var metadata = MetadataWithCache(cacheName);

        try
        {
            response = await this.grpcManager.Client.ListPopFrontAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return new CacheListPopFrontResponse.Error(_exceptionMapper.Convert(e, metadata));
        }

        if (response.ListCase == _ListPopFrontResponse.ListOneofCase.Missing)
        {
            return new CacheListPopFrontResponse.Miss();
        }

        return new CacheListPopFrontResponse.Hit(response);
    }

    private async Task<CacheListPopBackResponse> SendListPopBackAsync(string cacheName, string listName)
    {
        _ListPopBackRequest request = new() { ListName = listName.ToByteString() };
        _ListPopBackResponse response;
        var metadata = MetadataWithCache(cacheName);

        try
        {
            response = await this.grpcManager.Client.ListPopBackAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return new CacheListPopBackResponse.Error(_exceptionMapper.Convert(e, metadata));
        }

        if (response.ListCase == _ListPopBackResponse.ListOneofCase.Missing)
        {
            return new CacheListPopBackResponse.Miss();
        }

        return new CacheListPopBackResponse.Hit(response);
    }

    private async Task<CacheListFetchResponse> SendListFetchAsync(string cacheName, string listName)
    {
        _ListFetchRequest request = new() { ListName = listName.ToByteString() };
        _ListFetchResponse response;
        var metadata = MetadataWithCache(cacheName);

        try
        {
            response = await this.grpcManager.Client.ListFetchAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return new CacheListFetchResponse.Error(_exceptionMapper.Convert(e, metadata));
        }

        if (response.ListCase == _ListFetchResponse.ListOneofCase.Found)
        {
            return new CacheListFetchResponse.Hit(response);
        }

        return new CacheListFetchResponse.Miss();
    }

    private async Task<CacheListRemoveValueResponse> SendListRemoveValueAsync(string cacheName, string listName, ByteString value)
    {
        _ListRemoveRequest request = new()
        {
            ListName = listName.ToByteString(),
            AllElementsWithValue = value
        };
        var metadata = MetadataWithCache(cacheName);

        try
        {
            await this.grpcManager.Client.ListRemoveAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return new CacheListRemoveValueResponse.Error(_exceptionMapper.Convert(e, metadata));
        }

        return new CacheListRemoveValueResponse.Success();
    }

    private async Task<CacheListLengthResponse> SendListLengthAsync(string cacheName, string listName)
    {
        _ListLengthRequest request = new()
        {
            ListName = listName.ToByteString(),
        };
        _ListLengthResponse response;
        var metadata = MetadataWithCache(cacheName);

        try
        {
            response = await this.grpcManager.Client.ListLengthAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return new CacheListLengthResponse.Error(_exceptionMapper.Convert(e, metadata));
        }

        return new CacheListLengthResponse.Success(response);
    }

    private async Task<CacheListDeleteResponse> SendListDeleteAsync(string cacheName, string listName)
    {
        _ListEraseRequest request = new()
        {
            ListName = listName.ToByteString(),
            All = new()
        };
        _ListEraseResponse response;
        var metadata = MetadataWithCache(cacheName);

        try
        {
            response = await this.grpcManager.Client.ListEraseAsync(request, new CallOptions(headers: metadata, deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return new CacheListDeleteResponse.Error(_exceptionMapper.Convert(e, metadata));
        }

        return new CacheListDeleteResponse.Success();
    }
}
