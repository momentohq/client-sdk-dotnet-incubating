using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Momento.Protos.CacheClient;
using Momento.Sdk.Config;
using Momento.Sdk.Incubating.Responses;
using Momento.Sdk.Internal;
using Momento.Sdk.Internal.ExtensionMethods;
using Momento.Sdk.Responses;

namespace Momento.Sdk.Incubating.Internal;

internal sealed class ScsDataClient : ScsDataClientBase
{
    public ScsDataClient(IConfiguration config, string authToken, string endpoint, TimeSpan defaultTtl)
        : base(config, authToken, endpoint, defaultTtl)
    {
    }

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

    private _DictionaryFieldValuePair[] ToSingletonFieldValuePair(byte[] field, byte[] value) => new _DictionaryFieldValuePair[] { new _DictionaryFieldValuePair() { Field = field.ToByteString(), Value = value.ToByteString() } };
    private _DictionaryFieldValuePair[] ToSingletonFieldValuePair(string field, string value) => new _DictionaryFieldValuePair[] { new _DictionaryFieldValuePair() { Field = field.ToByteString(), Value = value.ToByteString() } };
    private _DictionaryFieldValuePair[] ToSingletonFieldValuePair(string field, byte[] value) => new _DictionaryFieldValuePair[] { new _DictionaryFieldValuePair() { Field = field.ToByteString(), Value = value.ToByteString() } };

    public async Task<CacheDictionarySetResponse> DictionarySetAsync(string cacheName, string dictionaryName, byte[] field, byte[] value, bool refreshTtl, TimeSpan? ttl = null)
    {
        return await SendDictionarySetAsync(cacheName, dictionaryName, ToSingletonFieldValuePair(field, value), refreshTtl, ttl);
    }

    public async Task<CacheDictionarySetResponse> DictionarySetAsync(string cacheName, string dictionaryName, string field, string value, bool refreshTtl, TimeSpan? ttl = null)
    {
        return await SendDictionarySetAsync(cacheName, dictionaryName, ToSingletonFieldValuePair(field, value), refreshTtl, ttl);
    }

    public async Task<CacheDictionarySetResponse> DictionarySetAsync(string cacheName, string dictionaryName, string field, byte[] value, bool refreshTtl, TimeSpan? ttl = null)
    {
        return await SendDictionarySetAsync(cacheName, dictionaryName, ToSingletonFieldValuePair(field, value), refreshTtl, ttl);
    }

    public async Task<CacheDictionaryGetResponse> DictionaryGetAsync(string cacheName, string dictionaryName, byte[] field)
    {
        return await SendDictionaryGetAsync(cacheName, dictionaryName, field.ToSingletonByteString());
    }

    public async Task<CacheDictionaryGetResponse> DictionaryGetAsync(string cacheName, string dictionaryName, string field)
    {
        return await SendDictionaryGetAsync(cacheName, dictionaryName, field.ToSingletonByteString());
    }

    private async Task<CacheDictionaryGetResponse> SendDictionaryGetAsync(string cacheName, string dictionaryName, IEnumerable<ByteString> fields)
    {
        _DictionaryGetRequest request = new() { DictionaryName = dictionaryName.ToByteString() };
        request.Fields.Add(fields);
        _DictionaryGetResponse response;

        try
        {
            response = await this.grpcManager.Client.DictionaryGetAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            var exc = _exceptionMapper.Convert(e);
            if (exc.TransportDetails != null)
            {
                exc.TransportDetails.Grpc.Metadata = MetadataWithCache(cacheName);
            }
            return new CacheDictionaryGetResponse.Error(exc);
        }

        if (response.DictionaryCase == _DictionaryGetResponse.DictionaryOneofCase.Missing)
        {
            return new CacheDictionaryGetResponse.Miss();
        }

        if (response.Found.Items.Count == 0)
        {
            var exc = _exceptionMapper.Convert(new Exception("_DictionaryGetResponseResponse contained no data but was found"));
            return new CacheDictionaryGetResponse.Error(exc);
        }

        if (response.Found.Items[0].Result == ECacheResult.Miss)
        {
            return new CacheDictionaryGetResponse.Miss();
        }

        return new CacheDictionaryGetResponse.Hit(response);
    }

    public async Task<CacheDictionarySetBatchResponse> DictionarySetBatchAsync(string cacheName, string dictionaryName, IEnumerable<KeyValuePair<byte[], byte[]>> items, bool refreshTtl, TimeSpan? ttl = null)
    {
        var protoItems = items.Select(kv => new _DictionaryFieldValuePair() { Field = kv.Key.ToByteString(), Value = kv.Value.ToByteString() });
        return await SendDictionarySetBatchAsync(cacheName, dictionaryName, protoItems, refreshTtl, ttl);
    }

    public async Task<CacheDictionarySetBatchResponse> DictionarySetBatchAsync(string cacheName, string dictionaryName, IEnumerable<KeyValuePair<string, string>> items, bool refreshTtl, TimeSpan? ttl = null)
    {
        var protoItems = items.Select(kv => new _DictionaryFieldValuePair() { Field = kv.Key.ToByteString(), Value = kv.Value.ToByteString() });
        return await SendDictionarySetBatchAsync(cacheName, dictionaryName, protoItems, refreshTtl, ttl);
    }

    public async Task<CacheDictionarySetBatchResponse> DictionarySetBatchAsync(string cacheName, string dictionaryName, IEnumerable<KeyValuePair<string, byte[]>> items, bool refreshTtl, TimeSpan? ttl = null)
    {
        var protoItems = items.Select(kv => new _DictionaryFieldValuePair() { Field = kv.Key.ToByteString(), Value = kv.Value.ToByteString() });
        return await SendDictionarySetBatchAsync(cacheName, dictionaryName, protoItems, refreshTtl, ttl);
    }

    public async Task<CacheDictionarySetResponse> SendDictionarySetAsync(string cacheName, string dictionaryName, IEnumerable<_DictionaryFieldValuePair> items, bool refreshTtl, TimeSpan? ttl = null)
    {
        _DictionarySetRequest request = new()
        {
            DictionaryName = dictionaryName.ToByteString(),
            RefreshTtl = refreshTtl,
            TtlMilliseconds = TtlToMilliseconds(ttl)
        };
        request.Items.Add(items);

        try
        {
            await this.grpcManager.Client.DictionarySetAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return new CacheDictionarySetResponse.Error(_exceptionMapper.Convert(e));
        }
        return new CacheDictionarySetResponse.Success();
    }

    public async Task<CacheDictionarySetBatchResponse> SendDictionarySetBatchAsync(string cacheName, string dictionaryName, IEnumerable<_DictionaryFieldValuePair> items, bool refreshTtl, TimeSpan? ttl = null)
    {
        _DictionarySetRequest request = new()
        {
            DictionaryName = dictionaryName.ToByteString(),
            RefreshTtl = refreshTtl,
            TtlMilliseconds = TtlToMilliseconds(ttl)
        };
        request.Items.Add(items);

        try
        {
            await this.grpcManager.Client.DictionarySetAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return new CacheDictionarySetBatchResponse.Error(_exceptionMapper.Convert(e));
        }
        return new CacheDictionarySetBatchResponse.Success();
    }

    public async Task<CacheDictionaryIncrementResponse> DictionaryIncrementAsync(string cacheName, string dictionaryName, string field, bool refreshTtl, long amount = 1, TimeSpan? ttl = null)
    {
        _DictionaryIncrementRequest request = new()
        {
            DictionaryName = dictionaryName.ToByteString(),
            Field = field.ToByteString(),
            Amount = amount,
            RefreshTtl = refreshTtl,
            TtlMilliseconds = TtlToMilliseconds(ttl)
        };
        _DictionaryIncrementResponse response;

        try
        {
            response = await this.grpcManager.Client.DictionaryIncrementAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            var exc = _exceptionMapper.Convert(e);
            if (exc.TransportDetails != null)
            {
                exc.TransportDetails.Grpc.Metadata = MetadataWithCache(cacheName);
            }
            return new CacheDictionaryIncrementResponse.Error(exc);
        }
        return new CacheDictionaryIncrementResponse.Success(response);
    }

    public async Task<CacheDictionaryGetBatchResponse> DictionaryGetBatchAsync(string cacheName, string dictionaryName, IEnumerable<byte[]> fields)
    {
        return await SendDictionaryGetBatchAsync(cacheName, dictionaryName, fields.ToEnumerableByteString());
    }

    public async Task<CacheDictionaryGetBatchResponse> DictionaryGetBatchAsync(string cacheName, string dictionaryName, IEnumerable<string> fields)
    {
        return await SendDictionaryGetBatchAsync(cacheName, dictionaryName, fields.ToEnumerableByteString());
    }

    private async Task<CacheDictionaryGetBatchResponse> SendDictionaryGetBatchAsync(string cacheName, string dictionaryName, IEnumerable<ByteString> fields)
    {
        _DictionaryGetRequest request = new() { DictionaryName = dictionaryName.ToByteString() };
        request.Fields.Add(fields);
        _DictionaryGetResponse response;

        try
        {
            response = await this.grpcManager.Client.DictionaryGetAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            var exc = _exceptionMapper.Convert(e);
            if (exc.TransportDetails != null)
            {
                exc.TransportDetails.Grpc.Metadata = MetadataWithCache(cacheName);
            }
            return new CacheDictionaryGetBatchResponse.Error(exc);
        }
        if (response.DictionaryCase == _DictionaryGetResponse.DictionaryOneofCase.Found)
        {
            return new CacheDictionaryGetBatchResponse.Success(response);
        }
        return new CacheDictionaryGetBatchResponse.Success(fields.Count());
    }

    public async Task<CacheDictionaryFetchResponse> DictionaryFetchAsync(string cacheName, string dictionaryName)
    {
        _DictionaryFetchRequest request = new() { DictionaryName = dictionaryName.ToByteString() };
        _DictionaryFetchResponse response;
        try
        {
            response = await this.grpcManager.Client.DictionaryFetchAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            var exc = _exceptionMapper.Convert(e);
            if (exc.TransportDetails != null)
            {
                exc.TransportDetails.Grpc.Metadata = MetadataWithCache(cacheName);
            }
            return new CacheDictionaryFetchResponse.Error(exc);
        }
        if (response.DictionaryCase == _DictionaryFetchResponse.DictionaryOneofCase.Found)
        {
            return new CacheDictionaryFetchResponse.Hit(response);
        }
        return new CacheDictionaryFetchResponse.Miss();
    }

    public async Task<CacheDictionaryDeleteResponse> DictionaryDeleteAsync(string cacheName, string dictionaryName)
    {
        _DictionaryDeleteRequest request = new()
        {
            DictionaryName = dictionaryName.ToByteString(),
            All = new()
        };
        try
        {
            await this.grpcManager.Client.DictionaryDeleteAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            var exc = _exceptionMapper.Convert(e);
            if (exc.TransportDetails != null)
            {
                exc.TransportDetails.Grpc.Metadata = MetadataWithCache(cacheName);
            }
            return new CacheDictionaryDeleteResponse.Error(exc);
        }
        return new CacheDictionaryDeleteResponse.Success();
    }

    public async Task<CacheDictionaryRemoveFieldResponse> DictionaryRemoveFieldAsync(string cacheName, string dictionaryName, byte[] field)
    {
        return await DictionaryRemoveFieldAsync(cacheName, dictionaryName, field.ToByteString());
    }

    public async Task<CacheDictionaryRemoveFieldResponse> DictionaryRemoveFieldAsync(string cacheName, string dictionaryName, string field)
    {
        return await DictionaryRemoveFieldAsync(cacheName, dictionaryName, field.ToByteString());
    }

    public async Task<CacheDictionaryRemoveFieldResponse> DictionaryRemoveFieldAsync(string cacheName, string dictionaryName, ByteString field)
    {
        _DictionaryDeleteRequest request = new()
        {
            DictionaryName = dictionaryName.ToByteString(),
            Some = new()
        };
        request.Some.Fields.Add(field);

        try
        {
            await this.grpcManager.Client.DictionaryDeleteAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            var exc = _exceptionMapper.Convert(e);
            if (exc.TransportDetails != null)
            {
                exc.TransportDetails.Grpc.Metadata = MetadataWithCache(cacheName);
            }
            return new CacheDictionaryRemoveFieldResponse.Error(exc);
        }
        return new CacheDictionaryRemoveFieldResponse.Success();
    }

    public async Task<CacheDictionaryRemoveFieldsResponse> DictionaryRemoveFieldsAsync(string cacheName, string dictionaryName, IEnumerable<byte[]> fields)
    {
        return await DictionaryRemoveFieldsAsync(cacheName, dictionaryName, fields.ToEnumerableByteString());
    }

    public async Task<CacheDictionaryRemoveFieldsResponse> DictionaryRemoveFieldsAsync(string cacheName, string dictionaryName, IEnumerable<string> fields)
    {
        return await DictionaryRemoveFieldsAsync(cacheName, dictionaryName, fields.ToEnumerableByteString());
    }

    public async Task<CacheDictionaryRemoveFieldsResponse> DictionaryRemoveFieldsAsync(string cacheName, string dictionaryName, IEnumerable<ByteString> fields)
    {
        _DictionaryDeleteRequest request = new()
        {
            DictionaryName = dictionaryName.ToByteString(),
            Some = new()
        };
        request.Some.Fields.Add(fields);

        try
        {
            await this.grpcManager.Client.DictionaryDeleteAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            var exc = _exceptionMapper.Convert(e);
            if (exc.TransportDetails != null)
            {
                exc.TransportDetails.Grpc.Metadata = MetadataWithCache(cacheName);
            }
            return new CacheDictionaryRemoveFieldsResponse.Error(exc);
        }
        return new CacheDictionaryRemoveFieldsResponse.Success();
    }

    public async Task<CacheSetAddResponse> SetAddAsync(string cacheName, string setName, byte[] element, bool refreshTtl, TimeSpan? ttl = null)
    {
        return await SendSetAddAsync(cacheName, setName, element.ToSingletonByteString(), refreshTtl, ttl);
    }

    public async Task<CacheSetAddResponse> SetAddAsync(string cacheName, string setName, string element, bool refreshTtl, TimeSpan? ttl = null)
    {
        return await SendSetAddAsync(cacheName, setName, element.ToSingletonByteString(), refreshTtl, ttl);
    }

    public async Task<CacheSetAddBatchResponse> SetAddBatchAsync(string cacheName, string setName, IEnumerable<byte[]> elements, bool refreshTtl, TimeSpan? ttl = null)
    {
        return await SendSetAddBatchAsync(cacheName, setName, elements.ToEnumerableByteString(), refreshTtl, ttl);
    }

    public async Task<CacheSetAddBatchResponse> SetAddBatchAsync(string cacheName, string setName, IEnumerable<string> elements, bool refreshTtl, TimeSpan? ttl = null)
    {
        return await SendSetAddBatchAsync(cacheName, setName, elements.ToEnumerableByteString(), refreshTtl, ttl);
    }

    public async Task<CacheSetAddResponse> SendSetAddAsync(string cacheName, string setName, IEnumerable<ByteString> elements, bool refreshTtl, TimeSpan? ttl = null)
    {
        _SetUnionRequest request = new()
        {
            SetName = setName.ToByteString(),
            RefreshTtl = refreshTtl,
            TtlMilliseconds = TtlToMilliseconds(ttl)
        };
        request.Elements.Add(elements);
        try
        {
            await this.grpcManager.Client.SetUnionAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            var exc = _exceptionMapper.Convert(e);
            if (exc.TransportDetails != null)
            {
                exc.TransportDetails.Grpc.Metadata = MetadataWithCache(cacheName);
            }
            return new CacheSetAddResponse.Error(exc);
        }
        return new CacheSetAddResponse.Success();
    }

    public async Task<CacheSetAddBatchResponse> SendSetAddBatchAsync(string cacheName, string setName, IEnumerable<ByteString> elements, bool refreshTtl, TimeSpan? ttl = null)
    {
        _SetUnionRequest request = new()
        {
            SetName = setName.ToByteString(),
            RefreshTtl = refreshTtl,
            TtlMilliseconds = TtlToMilliseconds(ttl)
        };
        request.Elements.Add(elements);
        try
        {
            await this.grpcManager.Client.SetUnionAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            var exc = _exceptionMapper.Convert(e);
            if (exc.TransportDetails != null)
            {
                exc.TransportDetails.Grpc.Metadata = MetadataWithCache(cacheName);
            }
            return new CacheSetAddBatchResponse.Error(exc);
        }
        return new CacheSetAddBatchResponse.Success();
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

    public async Task<CacheSetRemoveElementResponse> SendSetRemoveElementAsync(string cacheName, string setName, IEnumerable<ByteString> elements)
    {
        _SetDifferenceRequest request = new()
        {
            SetName = setName.ToByteString(),
            Subtrahend = new() { Set = new() }
        };
        request.Subtrahend.Set.Elements.Add(elements);

        try
        {
            await this.grpcManager.Client.SetDifferenceAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            var exc = _exceptionMapper.Convert(e);
            if (exc.TransportDetails != null)
            {
                exc.TransportDetails.Grpc.Metadata = MetadataWithCache(cacheName);
            }
            return new CacheSetRemoveElementResponse.Error(exc);
        }
        return new CacheSetRemoveElementResponse.Success();
    }

    public async Task<CacheSetRemoveElementsResponse> SendSetRemoveElementsAsync(string cacheName, string setName, IEnumerable<ByteString> elements)
    {
        _SetDifferenceRequest request = new()
        {
            SetName = setName.ToByteString(),
            Subtrahend = new() { Set = new() }
        };
        request.Subtrahend.Set.Elements.Add(elements);

        try
        {
            await this.grpcManager.Client.SetDifferenceAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            var exc = _exceptionMapper.Convert(e);
            if (exc.TransportDetails != null)
            {
                exc.TransportDetails.Grpc.Metadata = MetadataWithCache(cacheName);
            }
            return new CacheSetRemoveElementsResponse.Error(exc);
        }
        return new CacheSetRemoveElementsResponse.Success();
    }

    public async Task<CacheSetFetchResponse> SetFetchAsync(string cacheName, string setName)
    {
        _SetFetchRequest request = new() { SetName = setName.ToByteString() };
        _SetFetchResponse response;

        try
        {
            response = await this.grpcManager.Client.SetFetchAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            var exc = _exceptionMapper.Convert(e);
            if (exc.TransportDetails != null)
            {
                exc.TransportDetails.Grpc.Metadata = MetadataWithCache(cacheName);
            }
            return new CacheSetFetchResponse.Error(exc);
        }
        if (response.SetCase == _SetFetchResponse.SetOneofCase.Found)
        {
            return new CacheSetFetchResponse.Hit(response);
        }
        return new CacheSetFetchResponse.Miss();
    }

    public async Task<CacheSetDeleteResponse> SetDeleteAsync(string cacheName, string setName)
    {
        _SetDifferenceRequest request = new()
        {
            SetName = setName.ToByteString(),
            Subtrahend = new() { Identity = new() }
        };

        try
        {
            await this.grpcManager.Client.SetDifferenceAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            var exc = _exceptionMapper.Convert(e);
            if (exc.TransportDetails != null)
            {
                exc.TransportDetails.Grpc.Metadata = MetadataWithCache(cacheName);
            }
            return new CacheSetDeleteResponse.Error(exc);
        }
        return new CacheSetDeleteResponse.Success();
    }

    public async Task<CacheListConcatenateFrontResponse> ListConcatenateFrontAsync(string cacheName, string listName, IEnumerable<byte[]> values, bool refreshTtl, int? truncateBackToSize = null, TimeSpan? ttl = null)
    {
        return await SendListConcatenateFrontAsync(cacheName, listName, values.Select(value => value.ToByteString()), refreshTtl, truncateBackToSize, ttl);
    }
    public async Task<CacheListConcatenateFrontResponse> ListConcatenateFrontAsync(string cacheName, string listName, IEnumerable<string> values, bool refreshTtl, int? truncateBackToSize = null, TimeSpan? ttl = null)
    {
        return await SendListConcatenateFrontAsync(cacheName, listName, values.Select(value => value.ToByteString()), refreshTtl, truncateBackToSize, ttl);
    }

    public async Task<CacheListConcatenateFrontResponse> SendListConcatenateFrontAsync(string cacheName, string listName, IEnumerable<ByteString> values, bool refreshTtl, int? truncateBackToSize = null, TimeSpan? ttl = null)
    {
        _ListConcatenateFrontRequest request = new()
        {
            TruncateBackToSize = Convert.ToUInt32(truncateBackToSize.GetValueOrDefault()),
            ListName = listName.ToByteString(),
            RefreshTtl = refreshTtl,
            TtlMilliseconds = TtlToMilliseconds(ttl)
        };
        request.Values.Add(values);
        _ListConcatenateFrontResponse response;

        try
        {
            response = await this.grpcManager.Client.ListConcatenateFrontAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            var exc = _exceptionMapper.Convert(e);
            if (exc.TransportDetails != null)
            {
                exc.TransportDetails.Grpc.Metadata = MetadataWithCache(cacheName);
            }
            return new CacheListConcatenateFrontResponse.Error(exc);
        }
        return new CacheListConcatenateFrontResponse.Success(response);
    }

    public async Task<CacheListConcatenateBackResponse> ListConcatenateBackAsync(string cacheName, string listName, IEnumerable<byte[]> values, bool refreshTtl, int? truncateFrontToSize = null, TimeSpan? ttl = null)
    {
        return await SendListConcatenateBackAsync(cacheName, listName, values.Select(value => value.ToByteString()), refreshTtl, truncateFrontToSize, ttl);
    }
    public async Task<CacheListConcatenateBackResponse> ListConcatenateBackAsync(string cacheName, string listName, IEnumerable<string> values, bool refreshTtl, int? truncateFrontToSize = null, TimeSpan? ttl = null)
    {
        return await SendListConcatenateBackAsync(cacheName, listName, values.Select(value => value.ToByteString()), refreshTtl, truncateFrontToSize, ttl);
    }

    public async Task<CacheListConcatenateBackResponse> SendListConcatenateBackAsync(string cacheName, string listName, IEnumerable<ByteString> values, bool refreshTtl, int? truncateFrontToSize = null, TimeSpan? ttl = null)
    {
        _ListConcatenateBackRequest request = new()
        {
            TruncateFrontToSize = Convert.ToUInt32(truncateFrontToSize.GetValueOrDefault()),
            ListName = listName.ToByteString(),
            RefreshTtl = refreshTtl,
            TtlMilliseconds = TtlToMilliseconds(ttl)
        };
        request.Values.Add(values);
        _ListConcatenateBackResponse response;

        try
        {
            response = await this.grpcManager.Client.ListConcatenateBackAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            var exc = _exceptionMapper.Convert(e);
            if (exc.TransportDetails != null)
            {
                exc.TransportDetails.Grpc.Metadata = MetadataWithCache(cacheName);
            }
            return new CacheListConcatenateBackResponse.Error(exc);
        }
        return new CacheListConcatenateBackResponse.Success(response);
    }


    public async Task<CacheListPushFrontResponse> ListPushFrontAsync(string cacheName, string listName, byte[] value, bool refreshTtl, int? truncateBackToSize = null, TimeSpan? ttl = null)
    {
        return await SendListPushFrontAsync(cacheName, listName, value.ToByteString(), refreshTtl, truncateBackToSize, ttl);
    }

    public async Task<CacheListPushFrontResponse> ListPushFrontAsync(string cacheName, string listName, string value, bool refreshTtl, int? truncateBackToSize = null, TimeSpan? ttl = null)
    {
        return await SendListPushFrontAsync(cacheName, listName, value.ToByteString(), refreshTtl, truncateBackToSize, ttl);
    }

    public async Task<CacheListPushFrontResponse> SendListPushFrontAsync(string cacheName, string listName, ByteString value, bool refreshTtl, int? truncateBackToSize = null, TimeSpan? ttl = null)
    {
        _ListPushFrontRequest request = new()
        {
            TruncateBackToSize = Convert.ToUInt32(truncateBackToSize.GetValueOrDefault()),
            ListName = listName.ToByteString(),
            Value = value,
            RefreshTtl = refreshTtl,
            TtlMilliseconds = TtlToMilliseconds(ttl)
        };
        _ListPushFrontResponse response;

        try
        {
            response = await this.grpcManager.Client.ListPushFrontAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            var exc = _exceptionMapper.Convert(e);
            if (exc.TransportDetails != null)
            {
                exc.TransportDetails.Grpc.Metadata = MetadataWithCache(cacheName);
            }
            return new CacheListPushFrontResponse.Error(exc);
        }
        return new CacheListPushFrontResponse.Success(response);
    }

    public async Task<CacheListPushBackResponse> ListPushBackAsync(string cacheName, string listName, byte[] value, bool refreshTtl, int? truncateFrontToSize = null, TimeSpan? ttl = null)
    {
        return await SendListPushBackAsync(cacheName, listName, value.ToByteString(), refreshTtl, truncateFrontToSize, ttl);
    }

    public async Task<CacheListPushBackResponse> ListPushBackAsync(string cacheName, string listName, string value, bool refreshTtl, int? truncateFrontToSize = null, TimeSpan? ttl = null)
    {
        return await SendListPushBackAsync(cacheName, listName, value.ToByteString(), refreshTtl, truncateFrontToSize, ttl);
    }

    public async Task<CacheListPushBackResponse> SendListPushBackAsync(string cacheName, string listName, ByteString value, bool refreshTtl, int? truncateFrontToSize = null, TimeSpan? ttl = null)
    {
        _ListPushBackRequest request = new()
        {
            TruncateFrontToSize = Convert.ToUInt32(truncateFrontToSize.GetValueOrDefault()),
            ListName = listName.ToByteString(),
            Value = value,
            RefreshTtl = refreshTtl,
            TtlMilliseconds = TtlToMilliseconds(ttl)
        };
        _ListPushBackResponse response;

        try
        {
            response = await this.grpcManager.Client.ListPushBackAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            var exc = _exceptionMapper.Convert(e);
            if (exc.TransportDetails != null)
            {
                exc.TransportDetails.Grpc.Metadata = MetadataWithCache(cacheName);
            }
            return new CacheListPushBackResponse.Error(exc);
        }
        return new CacheListPushBackResponse.Success(response);
    }

    public async Task<CacheListPopFrontResponse> ListPopFrontAsync(string cacheName, string listName)
    {
        _ListPopFrontRequest request = new() { ListName = listName.ToByteString() };
        _ListPopFrontResponse response;

        try
        {
            response = await this.grpcManager.Client.ListPopFrontAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            var exc = _exceptionMapper.Convert(e);
            if (exc.TransportDetails != null)
            {
                exc.TransportDetails.Grpc.Metadata = MetadataWithCache(cacheName);
            }
            return new CacheListPopFrontResponse.Error(exc);
        }
        if (response.ListCase == _ListPopFrontResponse.ListOneofCase.Missing)
        {
            return new CacheListPopFrontResponse.Miss();
        }
        return new CacheListPopFrontResponse.Hit(response);
    }

    public async Task<CacheListPopBackResponse> ListPopBackAsync(string cacheName, string listName)
    {
        _ListPopBackRequest request = new() { ListName = listName.ToByteString() };
        _ListPopBackResponse response;

        try
        {
            response = await this.grpcManager.Client.ListPopBackAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            var exc = _exceptionMapper.Convert(e);
            if (exc.TransportDetails != null)
            {
                exc.TransportDetails.Grpc.Metadata = MetadataWithCache(cacheName);
            }
            return new CacheListPopBackResponse.Error(exc);
        }
        if (response.ListCase == _ListPopBackResponse.ListOneofCase.Missing)
        {
            return new CacheListPopBackResponse.Miss();
        }
        return new CacheListPopBackResponse.Hit(response);
    }

    public async Task<CacheListFetchResponse> ListFetchAsync(string cacheName, string listName)
    {
        _ListFetchRequest request = new() { ListName = listName.ToByteString() };
        _ListFetchResponse response;

        try
        {
            response = await this.grpcManager.Client.ListFetchAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            var exc = _exceptionMapper.Convert(e);
            if (exc.TransportDetails != null)
            {
                exc.TransportDetails.Grpc.Metadata = MetadataWithCache(cacheName);
            }
            return new CacheListFetchResponse.Error(exc);
        }
        if (response.ListCase == _ListFetchResponse.ListOneofCase.Found)
        {
            return new CacheListFetchResponse.Hit(response);
        }
        return new CacheListFetchResponse.Miss();
    }

    public async Task<CacheListRemoveValueResponse> ListRemoveValueAsync(string cacheName, string listName, byte[] value)
    {
        return await ListRemoveValueAsync(cacheName, listName, value.ToByteString());
    }

    public async Task<CacheListRemoveValueResponse> ListRemoveValueAsync(string cacheName, string listName, string value)
    {
        return await ListRemoveValueAsync(cacheName, listName, value.ToByteString());
    }

    public async Task<CacheListRemoveValueResponse> ListRemoveValueAsync(string cacheName, string listName, ByteString value)
    {
        _ListRemoveRequest request = new()
        {
            ListName = listName.ToByteString(),
            AllElementsWithValue = value
        };

        try
        {
            await this.grpcManager.Client.ListRemoveAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            var exc = _exceptionMapper.Convert(e);
            if (exc.TransportDetails != null)
            {
                exc.TransportDetails.Grpc.Metadata = MetadataWithCache(cacheName);
            }
            return new CacheListRemoveValueResponse.Error(exc);
        }
        return new CacheListRemoveValueResponse.Success();
    }

    public async Task<CacheListLengthResponse> ListLengthAsync(string cacheName, string listName)
    {
        _ListLengthRequest request = new()
        {
            ListName = listName.ToByteString(),
        };
        _ListLengthResponse response;

        try
        {
            response = await this.grpcManager.Client.ListLengthAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            var exc = _exceptionMapper.Convert(e);
            if (exc.TransportDetails != null)
            {
                exc.TransportDetails.Grpc.Metadata = MetadataWithCache(cacheName);
            }
            return new CacheListLengthResponse.Error(exc);
        }
        return new CacheListLengthResponse.Success(response);
    }

    public async Task<CacheListDeleteResponse> ListDeleteAsync(string cacheName, string listName)
    {
        _ListEraseRequest request = new()
        {
            ListName = listName.ToByteString(),
            All = new()
        };
        _ListEraseResponse response;

        try
        {
            response = await this.grpcManager.Client.ListEraseAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            var exc = _exceptionMapper.Convert(e);
            if (exc.TransportDetails != null)
            {
                exc.TransportDetails.Grpc.Metadata = MetadataWithCache(cacheName);
            }
            return new CacheListDeleteResponse.Error(exc);
        }
        return new CacheListDeleteResponse.Success();
    }
}
