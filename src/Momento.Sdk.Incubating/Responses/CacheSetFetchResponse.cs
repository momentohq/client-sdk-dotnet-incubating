using System;
using System.Collections.Generic;
using System.Linq;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Momento.Protos.CacheClient;
using Momento.Sdk.Exceptions;
using Momento.Sdk.Internal;
using Momento.Sdk.Responses;

namespace Momento.Sdk.Incubating.Responses;

public abstract class CacheSetFetchResponse
{
    public class Hit : CacheSetFetchResponse
    {
        protected readonly RepeatedField<ByteString> elements;
        protected readonly Lazy<HashSet<byte[]>> _byteArraySet;
        protected readonly Lazy<HashSet<string>> _stringSet;

        public Hit(_SetFetchResponse response)
        {
            elements = response.Found.Elements;
            _byteArraySet = new(() =>
            {

                return new HashSet<byte[]>(
                    elements.Select(element => element.ToByteArray()),
                    Utils.ByteArrayComparer);
            });

            _stringSet = new(() =>
            {

                return new HashSet<string>(elements.Select(element => element.ToStringUtf8()));
            });
        }

        public HashSet<byte[]> ValueByteArraySet { get => _byteArraySet.Value; }

        public HashSet<string> ValueStringSet { get => _stringSet.Value; }
    }

    public class Miss : CacheSetFetchResponse
    {

    }

    public class Error : CacheSetFetchResponse
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

        public override string ToString()
        {
            return base.ToString() + ": " + Message;
        }
    }
}
