using System;
using System.Collections.Generic;
using System.Linq;
using Google.Protobuf.Collections;
using Momento.Protos.CacheClient;
using Momento.Sdk.Internal;
using Momento.Sdk.Responses;
using Momento.Sdk.Exceptions;

namespace Momento.Sdk.Incubating.Responses;

public abstract class CacheDictionaryFetchResponse
{
    public class Hit : CacheDictionaryFetchResponse
    {
        protected readonly RepeatedField<_DictionaryFieldValuePair>? items;
        protected readonly Lazy<Dictionary<byte[], byte[]>?> _byteArrayByteArrayDictionary;
        protected readonly Lazy<Dictionary<string, string>?> _stringStringDictionary;
        protected readonly Lazy<Dictionary<string, byte[]>?> _stringByteArrayDictionary;

        public Hit(_DictionaryFetchResponse response)
        {
            items = response.Found.Items;
            _byteArrayByteArrayDictionary = new(() =>
            {
                if (items == null)
                {
                    return null;
                }
                return new Dictionary<byte[], byte[]>(
                    items.Select(kv => new KeyValuePair<byte[], byte[]>(kv.Field.ToByteArray(), kv.Value.ToByteArray())),
                    Utils.ByteArrayComparer);
            });

            _stringStringDictionary = new(() =>
            {
                if (items == null)
                {
                    return null;
                }
                return new Dictionary<string, string>(
                    items.Select(kv => new KeyValuePair<string, string>(kv.Field.ToStringUtf8(), kv.Value.ToStringUtf8())));
            });
            _stringByteArrayDictionary = new(() =>
            {
                if (items == null)
                {
                    return null;
                }
                return new Dictionary<string, byte[]>(
                    items.Select(kv => new KeyValuePair<string, byte[]>(kv.Field.ToStringUtf8(), kv.Value.ToByteArray())));
            });
        }

        public Dictionary<byte[], byte[]>? ByteArrayByteArrayDictionary { get => _byteArrayByteArrayDictionary.Value; }

        public Dictionary<string, string>? StringStringDictionary() => _stringStringDictionary.Value;

        public Dictionary<string, byte[]>? StringByteArrayDictionary() => _stringByteArrayDictionary.Value;
    }

    public class Miss : CacheDictionaryFetchResponse
    {
        protected readonly Lazy<Dictionary<byte[], byte[]>?> _byteArrayByteArrayDictionary;
        protected readonly Lazy<Dictionary<string, string>?> _stringStringDictionary;
        protected readonly Lazy<Dictionary<string, byte[]>?> _stringByteArrayDictionary;
        public Miss()
        {
            _byteArrayByteArrayDictionary = new(() =>
            {
                return null;
            });

            _stringStringDictionary = new(() =>
            {
                return null;
            });
            _stringByteArrayDictionary = new(() =>
            {
                return null;
            });
        }

        public Dictionary<byte[], byte[]>? ByteArrayByteArrayDictionary { get => _byteArrayByteArrayDictionary.Value; }

        public Dictionary<string, string>? StringStringDictionary() => _stringStringDictionary.Value;

        public Dictionary<string, byte[]>? StringByteArrayDictionary() => _stringByteArrayDictionary.Value;
    }
}

public class Error : CacheDictionaryFetchResponse
{
    private readonly SdkException _error;
    public Error(SdkException error)
    {
        _error = error;
    }

    public SdkException Exception
    {
        get => _error;
    }

    public MomentoErrorCode ErrorCode
    {
        get => _error.ErrorCode;
    }

    public string Message
    {
        get => $"{_error.MessageWrapper}: {_error.Message}";
    }

}
}
