using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Momento.Protos.CacheClient;
using Momento.Sdk.Config;
using Momento.Sdk.Incubating.Requests;
using Momento.Sdk.Incubating.Responses;
using Momento.Sdk.Internal;
using Momento.Sdk.Internal.ExtensionMethods;

namespace Momento.Sdk.Incubating.Internal;

internal sealed class ScsDataClient : ScsDataClientBase
{
    private readonly ILogger _logger;

    public ScsDataClient(IConfiguration config, string authToken, string endpoint, TimeSpan defaultTtl)
        : base(config, authToken, endpoint, defaultTtl)
    {
        this._logger = config.LoggerFactory.CreateLogger<ScsDataClient>();
    }

    private _DictionaryFieldValuePair[] ToSingletonFieldValuePair(byte[] field, byte[] value) => new _DictionaryFieldValuePair[] { new _DictionaryFieldValuePair() { Field = field.ToByteString(), Value = value.ToByteString() } };
    private _DictionaryFieldValuePair[] ToSingletonFieldValuePair(string field, string value) => new _DictionaryFieldValuePair[] { new _DictionaryFieldValuePair() { Field = field.ToByteString(), Value = value.ToByteString() } };
    private _DictionaryFieldValuePair[] ToSingletonFieldValuePair(string field, byte[] value) => new _DictionaryFieldValuePair[] { new _DictionaryFieldValuePair() { Field = field.ToByteString(), Value = value.ToByteString() } };

    public async Task<CacheDictionaryFetchResponse> DictionaryFetchAsync(string cacheName, string dictionaryName)
    {
        return await SendDictionaryFetchAsync(cacheName, dictionaryName);
    }

    public async Task<CacheDictionaryGetFieldResponse> DictionaryGetFieldAsync(string cacheName, string dictionaryName, byte[] field)
    {
        return await SendDictionaryGetFieldAsync(cacheName, dictionaryName, field.ToByteString());
    }

    public async Task<CacheDictionaryGetFieldResponse> DictionaryGetFieldAsync(string cacheName, string dictionaryName, string field)
    {
        return await SendDictionaryGetFieldAsync(cacheName, dictionaryName, field.ToByteString());
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

    public async Task<CacheListConcatenateFrontResponse> ListConcatenateFrontAsync(string cacheName, string listName, IEnumerable<byte[]> values, int? truncateBackToSize = null, CollectionTtl ttl = default(CollectionTtl))
    {
        return await SendListConcatenateFrontAsync(cacheName, listName, values.Select(value => value.ToByteString()), truncateBackToSize, ttl);
    }

    public async Task<CacheListConcatenateFrontResponse> ListConcatenateFrontAsync(string cacheName, string listName, IEnumerable<string> values, int? truncateBackToSize = null, CollectionTtl ttl = default(CollectionTtl))
    {
        return await SendListConcatenateFrontAsync(cacheName, listName, values.Select(value => value.ToByteString()), truncateBackToSize, ttl);
    }

    public async Task<CacheListConcatenateBackResponse> ListConcatenateBackAsync(string cacheName, string listName, IEnumerable<byte[]> values, int? truncateFrontToSize = null, CollectionTtl ttl = default(CollectionTtl))
    {
        return await SendListConcatenateBackAsync(cacheName, listName, values.Select(value => value.ToByteString()), truncateFrontToSize, ttl);
    }

    public async Task<CacheListConcatenateBackResponse> ListConcatenateBackAsync(string cacheName, string listName, IEnumerable<string> values, int? truncateFrontToSize = null, CollectionTtl ttl = default(CollectionTtl))
    {
        return await SendListConcatenateBackAsync(cacheName, listName, values.Select(value => value.ToByteString()), truncateFrontToSize, ttl);
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


    /***************************************************************************
     * Private "Send" methods"
     **************************************************************************/

    const string REQUEST_TYPE_DICTIONARY_FETCH = "DICTIONARY_FETCH";
    private async Task<CacheDictionaryFetchResponse> SendDictionaryFetchAsync(string cacheName, string dictionaryName)
    {
        _DictionaryFetchRequest request = new() { DictionaryName = dictionaryName.ToByteString() };
        _DictionaryFetchResponse response;
        var metadata = MetadataWithCache(cacheName);

        try
        {
            this._logger.LogTraceExecutingCollectionRequest(REQUEST_TYPE_DICTIONARY_FETCH, cacheName, dictionaryName);
            response = await this.grpcManager.Client.DictionaryFetchAsync(request, new CallOptions(headers: metadata, deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return this._logger.LogTraceCollectionRequestError(REQUEST_TYPE_DICTIONARY_FETCH, cacheName, dictionaryName, new CacheDictionaryFetchResponse.Error(_exceptionMapper.Convert(e, metadata)));
        }

        if (response.DictionaryCase == _DictionaryFetchResponse.DictionaryOneofCase.Found)
        {
            return this._logger.LogTraceCollectionRequestSuccess(REQUEST_TYPE_DICTIONARY_FETCH, cacheName, dictionaryName, new CacheDictionaryFetchResponse.Hit(response));
        }

        return this._logger.LogTraceCollectionRequestSuccess(REQUEST_TYPE_DICTIONARY_FETCH, cacheName, dictionaryName, new CacheDictionaryFetchResponse.Miss());
    }


    const string REQUEST_TYPE_DICTIONARY_GET_FIELD = "DICTIONARY_GET_FIELD";
    private async Task<CacheDictionaryGetFieldResponse> SendDictionaryGetFieldAsync(string cacheName, string dictionaryName, ByteString field)
    {
        _DictionaryGetRequest request = new() { DictionaryName = dictionaryName.ToByteString() };
        request.Fields.Add(field);
        _DictionaryGetResponse response;
        var metadata = MetadataWithCache(cacheName);

        try
        {
            this._logger.LogTraceExecutingCollectionRequest(REQUEST_TYPE_DICTIONARY_GET_FIELD, cacheName, dictionaryName, field, null);
            response = await this.grpcManager.Client.DictionaryGetAsync(request, new CallOptions(headers: metadata, deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return this._logger.LogTraceCollectionRequestError(REQUEST_TYPE_DICTIONARY_GET_FIELD, cacheName, dictionaryName, field, null, new CacheDictionaryGetFieldResponse.Error(field, _exceptionMapper.Convert(e, metadata)));
        }

        if (response.DictionaryCase == _DictionaryGetResponse.DictionaryOneofCase.Missing)
        {
            return this._logger.LogTraceCollectionRequestSuccess(REQUEST_TYPE_DICTIONARY_GET_FIELD, cacheName, dictionaryName, field, null, new CacheDictionaryGetFieldResponse.Miss(field));
        }

        if (response.Found.Items.Count == 0)
        {
            var exc = _exceptionMapper.Convert(new Exception("_DictionaryGetResponseResponse contained no data but was found"), metadata);
            return this._logger.LogTraceCollectionRequestError(REQUEST_TYPE_DICTIONARY_GET_FIELD, cacheName, dictionaryName, field, null, new CacheDictionaryGetFieldResponse.Error(field, exc));
        }

        if (response.Found.Items[0].Result == ECacheResult.Miss)
        {
            return this._logger.LogTraceCollectionRequestSuccess(REQUEST_TYPE_DICTIONARY_GET_FIELD, cacheName, dictionaryName, field, null, new CacheDictionaryGetFieldResponse.Miss(field));
        }

        return this._logger.LogTraceCollectionRequestSuccess(REQUEST_TYPE_DICTIONARY_GET_FIELD, cacheName, dictionaryName, field, null, new CacheDictionaryGetFieldResponse.Hit(field, response));
    }

    const string REQUEST_TYPE_DICTIONARY_GET_FIELDS = "DICTIONARY_GET_FIELDS";
    private async Task<CacheDictionaryGetFieldsResponse> SendDictionaryGetFieldsAsync(string cacheName, string dictionaryName, IEnumerable<ByteString> fields)
    {
        _DictionaryGetRequest request = new() { DictionaryName = dictionaryName.ToByteString() };
        request.Fields.Add(fields);
        _DictionaryGetResponse response;
        var metadata = MetadataWithCache(cacheName);

        try
        {
            this._logger.LogTraceExecutingCollectionRequest(REQUEST_TYPE_DICTIONARY_GET_FIELDS, cacheName, dictionaryName, fields, null);
            response = await this.grpcManager.Client.DictionaryGetAsync(request, new CallOptions(headers: metadata, deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return this._logger.LogTraceCollectionRequestError(REQUEST_TYPE_DICTIONARY_GET_FIELDS, cacheName, dictionaryName, fields, null, new CacheDictionaryGetFieldsResponse.Error(_exceptionMapper.Convert(e, metadata)));
        }

        if (response.DictionaryCase == _DictionaryGetResponse.DictionaryOneofCase.Found)
        {
            return this._logger.LogTraceCollectionRequestSuccess(REQUEST_TYPE_DICTIONARY_GET_FIELDS, cacheName, dictionaryName, fields, null, new CacheDictionaryGetFieldsResponse.Hit(fields, response));
        }

        return this._logger.LogTraceCollectionRequestSuccess(REQUEST_TYPE_DICTIONARY_GET_FIELDS, cacheName, dictionaryName, fields, null, new CacheDictionaryGetFieldsResponse.Miss());
    }

    const string REQUEST_TYPE_DICTIONARY_SET_FIELD = "DICTIONARY_SET_FIELD";
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
            this._logger.LogTraceExecutingCollectionRequest(REQUEST_TYPE_DICTIONARY_SET_FIELD, cacheName, dictionaryName, items, ttl);
            await this.grpcManager.Client.DictionarySetAsync(request, new CallOptions(headers: metadata, deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return this._logger.LogTraceCollectionRequestError(REQUEST_TYPE_DICTIONARY_SET_FIELD, cacheName, dictionaryName, items, ttl, new CacheDictionarySetFieldResponse.Error(_exceptionMapper.Convert(e, metadata)));
        }

        return this._logger.LogTraceCollectionRequestSuccess(REQUEST_TYPE_DICTIONARY_SET_FIELD, cacheName, dictionaryName, items, ttl, new CacheDictionarySetFieldResponse.Success());
    }

    const string REQUEST_TYPE_DICTIONARY_SET_FIELDS = "DICTIONARY_SET_FIELDS";
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
            this._logger.LogTraceExecutingCollectionRequest(REQUEST_TYPE_DICTIONARY_SET_FIELDS, cacheName, dictionaryName, items, ttl);
            await this.grpcManager.Client.DictionarySetAsync(request, new CallOptions(headers: metadata, deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return this._logger.LogTraceCollectionRequestError(REQUEST_TYPE_DICTIONARY_SET_FIELDS, cacheName, dictionaryName, items, ttl, new CacheDictionarySetFieldsResponse.Error(_exceptionMapper.Convert(e, metadata)));
        }

        return this._logger.LogTraceCollectionRequestSuccess(REQUEST_TYPE_DICTIONARY_SET_FIELDS, cacheName, dictionaryName, items, ttl, new CacheDictionarySetFieldsResponse.Success());
    }


    const string REQUEST_TYPE_DICTIONARY_INCREMENT = "DICTIONARY_INCREMENT";
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
            this._logger.LogTraceExecutingCollectionRequest(REQUEST_TYPE_DICTIONARY_INCREMENT, cacheName, dictionaryName, field, ttl);
            response = await this.grpcManager.Client.DictionaryIncrementAsync(request, new CallOptions(headers: metadata, deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return this._logger.LogTraceCollectionRequestError(REQUEST_TYPE_DICTIONARY_INCREMENT, cacheName, dictionaryName, field, ttl, new CacheDictionaryIncrementResponse.Error(_exceptionMapper.Convert(e, metadata)));
        }

        return this._logger.LogTraceCollectionRequestSuccess(REQUEST_TYPE_DICTIONARY_INCREMENT, cacheName, dictionaryName, field, ttl, new CacheDictionaryIncrementResponse.Success(response));
    }

    const string REQUEST_TYPE_DICTIONARY_REMOVE_FIELD = "DICTIONARY_REMOVE_FIELD";
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
            this._logger.LogTraceExecutingCollectionRequest(REQUEST_TYPE_DICTIONARY_REMOVE_FIELD, cacheName, dictionaryName, field, null);
            await this.grpcManager.Client.DictionaryDeleteAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return this._logger.LogTraceCollectionRequestError(REQUEST_TYPE_DICTIONARY_REMOVE_FIELD, cacheName, dictionaryName, new CacheDictionaryRemoveFieldResponse.Error(_exceptionMapper.Convert(e, metadata)));
        }

        return this._logger.LogTraceCollectionRequestSuccess(REQUEST_TYPE_DICTIONARY_REMOVE_FIELD, cacheName, dictionaryName, new CacheDictionaryRemoveFieldResponse.Success());
    }

    const string REQUEST_TYPE_DICTIONARY_REMOVE_FIELDS = "DICTIONARY_REMOVE_FIELDS";
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
            this._logger.LogTraceExecutingCollectionRequest(REQUEST_TYPE_DICTIONARY_REMOVE_FIELDS, cacheName, dictionaryName, fields, null);
            await this.grpcManager.Client.DictionaryDeleteAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return this._logger.LogTraceCollectionRequestError(REQUEST_TYPE_DICTIONARY_REMOVE_FIELDS, cacheName, dictionaryName, fields, null, new CacheDictionaryRemoveFieldsResponse.Error(_exceptionMapper.Convert(e, metadata)));
        }

        return this._logger.LogTraceCollectionRequestSuccess(REQUEST_TYPE_DICTIONARY_REMOVE_FIELDS, cacheName, dictionaryName, fields, null, new CacheDictionaryRemoveFieldsResponse.Success());
    }

    const string REQUEST_TYPE_SET_ADD_ELEMENT = "SET_ADD_ELEMENT";
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
            this._logger.LogTraceExecutingCollectionRequest(REQUEST_TYPE_SET_ADD_ELEMENT, cacheName, setName, elements, ttl);
            await this.grpcManager.Client.SetUnionAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return this._logger.LogTraceCollectionRequestError(REQUEST_TYPE_SET_ADD_ELEMENT, cacheName, setName, elements, ttl, new CacheSetAddElementResponse.Error(_exceptionMapper.Convert(e, metadata)));
        }

        return this._logger.LogTraceCollectionRequestSuccess(REQUEST_TYPE_SET_ADD_ELEMENT, cacheName, setName, elements, ttl, new CacheSetAddElementResponse.Success());
    }

    const string REQUEST_TYPE_SET_ADD_ELEMENTS = "SET_ADD_ELEMENTS";
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
            this._logger.LogTraceExecutingCollectionRequest(REQUEST_TYPE_SET_ADD_ELEMENTS, cacheName, setName, elements, ttl);
            await this.grpcManager.Client.SetUnionAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return this._logger.LogTraceCollectionRequestError(REQUEST_TYPE_SET_ADD_ELEMENTS, cacheName, setName, elements, ttl, new CacheSetAddElementsResponse.Error(_exceptionMapper.Convert(e, metadata)));
        }

        return this._logger.LogTraceCollectionRequestSuccess(REQUEST_TYPE_SET_ADD_ELEMENTS, cacheName, setName, elements, ttl, new CacheSetAddElementsResponse.Success());
    }

    const string REQUEST_TYPE_SET_REMOVE_ELEMENT = "SET_REMOVE_ELEMENT";
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
            this._logger.LogTraceExecutingCollectionRequest(REQUEST_TYPE_SET_REMOVE_ELEMENT, cacheName, setName, elements, null);
            await this.grpcManager.Client.SetDifferenceAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return this._logger.LogTraceCollectionRequestError(REQUEST_TYPE_SET_REMOVE_ELEMENT, cacheName, setName, elements, null, new CacheSetRemoveElementResponse.Error(_exceptionMapper.Convert(e, metadata)));
        }

        return this._logger.LogTraceCollectionRequestSuccess(REQUEST_TYPE_SET_REMOVE_ELEMENT, cacheName, setName, elements, null, new CacheSetRemoveElementResponse.Success());
    }

    const string REQUEST_TYPE_SET_REMOVE_ELEMENTS = "SET_REMOVE_ELEMENTS";
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
            this._logger.LogTraceExecutingCollectionRequest(REQUEST_TYPE_SET_REMOVE_ELEMENTS, cacheName, setName, elements, null);
            await this.grpcManager.Client.SetDifferenceAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return this._logger.LogTraceCollectionRequestError(REQUEST_TYPE_SET_REMOVE_ELEMENTS, cacheName, setName, elements, null, new CacheSetRemoveElementsResponse.Error(_exceptionMapper.Convert(e, metadata)));
        }

        return this._logger.LogTraceCollectionRequestSuccess(REQUEST_TYPE_SET_REMOVE_ELEMENTS, cacheName, setName, elements, null, new CacheSetRemoveElementsResponse.Success());
    }

    const string REQUEST_TYPE_SET_FETCH = "SET_FETCH";
    private async Task<CacheSetFetchResponse> SendSetFetchAsync(string cacheName, string setName)
    {
        _SetFetchRequest request = new() { SetName = setName.ToByteString() };
        _SetFetchResponse response;
        var metadata = MetadataWithCache(cacheName);

        try
        {
            this._logger.LogTraceExecutingCollectionRequest(REQUEST_TYPE_SET_FETCH, cacheName, setName);
            response = await this.grpcManager.Client.SetFetchAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return this._logger.LogTraceCollectionRequestError(REQUEST_TYPE_SET_FETCH, cacheName, setName, new CacheSetFetchResponse.Error(_exceptionMapper.Convert(e, metadata)));
        }
        if (response.SetCase == _SetFetchResponse.SetOneofCase.Found)
        {
            return this._logger.LogTraceCollectionRequestSuccess(REQUEST_TYPE_SET_FETCH, cacheName, setName, new CacheSetFetchResponse.Hit(response));
        }

        return this._logger.LogTraceCollectionRequestSuccess(REQUEST_TYPE_SET_FETCH, cacheName, setName, new CacheSetFetchResponse.Miss());
    }

    const string REQUEST_TYPE_LIST_CONCATENATE_FRONT = "LIST_CONCATENATE_FRONT";

    private async Task<CacheListConcatenateFrontResponse> SendListConcatenateFrontAsync(string cacheName, string listName, IEnumerable<ByteString> values, int? truncateBackToSize, CollectionTtl ttl)
    {
        _ListConcatenateFrontRequest request = new()
        {
            TruncateBackToSize = Convert.ToUInt32(truncateBackToSize.GetValueOrDefault()),
            ListName = listName.ToByteString(),
            RefreshTtl = ttl.RefreshTtl,
            TtlMilliseconds = TtlToMilliseconds(ttl.Ttl)
        };
        request.Values.AddRange(values);
        _ListConcatenateFrontResponse response;
        var metadata = MetadataWithCache(cacheName);

        try
        {
            this._logger.LogTraceExecutingCollectionRequest(REQUEST_TYPE_LIST_CONCATENATE_FRONT, cacheName, listName, values, ttl);
            response = await this.grpcManager.Client.ListConcatenateFrontAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return this._logger.LogTraceCollectionRequestError(REQUEST_TYPE_LIST_CONCATENATE_FRONT, cacheName, listName, values, ttl, new CacheListConcatenateFrontResponse.Error(_exceptionMapper.Convert(e, metadata)));
        }
        return this._logger.LogTraceCollectionRequestSuccess(REQUEST_TYPE_LIST_CONCATENATE_FRONT, cacheName, listName, values, ttl, new CacheListConcatenateFrontResponse.Success(response));
    }

    const string REQUEST_TYPE_LIST_CONCATENATE_BACK = "LIST_CONCATENATE_BACK";

    private async Task<CacheListConcatenateBackResponse> SendListConcatenateBackAsync(string cacheName, string listName, IEnumerable<ByteString> values, int? truncateFrontToSize, CollectionTtl ttl)
    {
        _ListConcatenateBackRequest request = new()
        {
            TruncateFrontToSize = Convert.ToUInt32(truncateFrontToSize.GetValueOrDefault()),
            ListName = listName.ToByteString(),
            RefreshTtl = ttl.RefreshTtl,
            TtlMilliseconds = TtlToMilliseconds(ttl.Ttl)
        };
        request.Values.AddRange(values);
        _ListConcatenateBackResponse response;
        var metadata = MetadataWithCache(cacheName);

        try
        {
            this._logger.LogTraceExecutingCollectionRequest(REQUEST_TYPE_LIST_CONCATENATE_BACK, cacheName, listName, values, ttl);
            response = await this.grpcManager.Client.ListConcatenateBackAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return this._logger.LogTraceCollectionRequestError(REQUEST_TYPE_LIST_CONCATENATE_BACK, cacheName, listName, values, ttl, new CacheListConcatenateBackResponse.Error(_exceptionMapper.Convert(e, metadata)));
        }
        return this._logger.LogTraceCollectionRequestSuccess(REQUEST_TYPE_LIST_CONCATENATE_BACK, cacheName, listName, values, ttl, new CacheListConcatenateBackResponse.Success(response));
    }

    const string REQUEST_TYPE_LIST_PUSH_FRONT = "LIST_PUSH_FRONT";
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
            this._logger.LogTraceExecutingCollectionRequest(REQUEST_TYPE_LIST_PUSH_FRONT, cacheName, listName, value, ttl);
            response = await this.grpcManager.Client.ListPushFrontAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return this._logger.LogTraceCollectionRequestError(REQUEST_TYPE_LIST_PUSH_FRONT, cacheName, listName, value, ttl, new CacheListPushFrontResponse.Error(_exceptionMapper.Convert(e, metadata)));
        }

        return this._logger.LogTraceCollectionRequestSuccess(REQUEST_TYPE_LIST_PUSH_FRONT, cacheName, listName, value, ttl, new CacheListPushFrontResponse.Success(response));
    }

    const string REQUEST_TYPE_LIST_PUSH_BACK = "LIST_PUSH_BACK";
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
            this._logger.LogTraceExecutingCollectionRequest(REQUEST_TYPE_LIST_PUSH_BACK, cacheName, listName, value, ttl);
            response = await this.grpcManager.Client.ListPushBackAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return this._logger.LogTraceCollectionRequestError(REQUEST_TYPE_LIST_PUSH_BACK, cacheName, listName, value, ttl, new CacheListPushBackResponse.Error(_exceptionMapper.Convert(e, metadata)));
        }

        return this._logger.LogTraceCollectionRequestSuccess(REQUEST_TYPE_LIST_PUSH_BACK, cacheName, listName, value, ttl, new CacheListPushBackResponse.Success(response));
    }

    const string REQUEST_TYPE_LIST_POP_FRONT = "LIST_POP_FRONT";
    private async Task<CacheListPopFrontResponse> SendListPopFrontAsync(string cacheName, string listName)
    {
        _ListPopFrontRequest request = new() { ListName = listName.ToByteString() };
        _ListPopFrontResponse response;
        var metadata = MetadataWithCache(cacheName);

        try
        {
            this._logger.LogTraceExecutingCollectionRequest(REQUEST_TYPE_LIST_POP_FRONT, cacheName, listName);
            response = await this.grpcManager.Client.ListPopFrontAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return this._logger.LogTraceCollectionRequestError(REQUEST_TYPE_LIST_POP_FRONT, cacheName, listName, new CacheListPopFrontResponse.Error(_exceptionMapper.Convert(e, metadata)));
        }

        if (response.ListCase == _ListPopFrontResponse.ListOneofCase.Missing)
        {
            return this._logger.LogTraceCollectionRequestSuccess(REQUEST_TYPE_LIST_POP_FRONT, cacheName, listName, new CacheListPopFrontResponse.Miss());
        }

        return this._logger.LogTraceCollectionRequestSuccess(REQUEST_TYPE_LIST_POP_FRONT, cacheName, listName, new CacheListPopFrontResponse.Hit(response));
    }

    const string REQUEST_TYPE_LIST_POP_BACK = "LIST_POP_BACK";
    private async Task<CacheListPopBackResponse> SendListPopBackAsync(string cacheName, string listName)
    {
        _ListPopBackRequest request = new() { ListName = listName.ToByteString() };
        _ListPopBackResponse response;
        var metadata = MetadataWithCache(cacheName);

        try
        {
            this._logger.LogTraceExecutingCollectionRequest(REQUEST_TYPE_LIST_POP_BACK, cacheName, listName);
            response = await this.grpcManager.Client.ListPopBackAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return this._logger.LogTraceCollectionRequestError(REQUEST_TYPE_LIST_POP_BACK, cacheName, listName, new CacheListPopBackResponse.Error(_exceptionMapper.Convert(e, metadata)));
        }

        if (response.ListCase == _ListPopBackResponse.ListOneofCase.Missing)
        {
            return this._logger.LogTraceCollectionRequestSuccess(REQUEST_TYPE_LIST_POP_BACK, cacheName, listName, new CacheListPopBackResponse.Miss());
        }

        return this._logger.LogTraceCollectionRequestSuccess(REQUEST_TYPE_LIST_POP_BACK, cacheName, listName, new CacheListPopBackResponse.Hit(response));
    }

    const string REQUEST_TYPE_LIST_FETCH = "LIST_FETCH";
    private async Task<CacheListFetchResponse> SendListFetchAsync(string cacheName, string listName)
    {
        _ListFetchRequest request = new() { ListName = listName.ToByteString() };
        _ListFetchResponse response;
        var metadata = MetadataWithCache(cacheName);

        try
        {
            this._logger.LogTraceExecutingCollectionRequest(REQUEST_TYPE_LIST_FETCH, cacheName, listName);
            response = await this.grpcManager.Client.ListFetchAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return this._logger.LogTraceCollectionRequestError(REQUEST_TYPE_LIST_FETCH, cacheName, listName, new CacheListFetchResponse.Error(_exceptionMapper.Convert(e, metadata)));
        }

        if (response.ListCase == _ListFetchResponse.ListOneofCase.Found)
        {
            return this._logger.LogTraceCollectionRequestSuccess(REQUEST_TYPE_LIST_FETCH, cacheName, listName, new CacheListFetchResponse.Hit(response));
        }

        return this._logger.LogTraceCollectionRequestSuccess(REQUEST_TYPE_LIST_FETCH, cacheName, listName, new CacheListFetchResponse.Miss());
    }

    const string REQUEST_TYPE_LIST_REMOVE_VALUE = "LIST_REMOVE_VALUE";
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
            this._logger.LogTraceExecutingCollectionRequest(REQUEST_TYPE_LIST_REMOVE_VALUE, cacheName, listName, value, null);
            await this.grpcManager.Client.ListRemoveAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return this._logger.LogTraceCollectionRequestError(REQUEST_TYPE_LIST_REMOVE_VALUE, cacheName, listName, value, null, new CacheListRemoveValueResponse.Error(_exceptionMapper.Convert(e, metadata)));
        }

        return this._logger.LogTraceCollectionRequestSuccess(REQUEST_TYPE_LIST_REMOVE_VALUE, cacheName, listName, value, null, new CacheListRemoveValueResponse.Success());
    }

    const string REQUEST_TYPE_LIST_LENGTH = "LIST_LENGTH";
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
            this._logger.LogTraceExecutingCollectionRequest(REQUEST_TYPE_LIST_LENGTH, cacheName, listName);
            response = await this.grpcManager.Client.ListLengthAsync(request, new CallOptions(headers: MetadataWithCache(cacheName), deadline: CalculateDeadline()));
        }
        catch (Exception e)
        {
            return this._logger.LogTraceCollectionRequestError(REQUEST_TYPE_LIST_LENGTH, cacheName, listName, new CacheListLengthResponse.Error(_exceptionMapper.Convert(e, metadata)));
        }

        return this._logger.LogTraceCollectionRequestSuccess(REQUEST_TYPE_LIST_LENGTH, cacheName, listName, new CacheListLengthResponse.Success(response));
    }
}
