using System;
using System.Collections.Generic;
using System.Linq;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Momento.Protos.CacheClient;
using Momento.Sdk.Exceptions;
using Momento.Sdk.Responses;

namespace Momento.Sdk.Incubating.Responses;

public abstract class CacheListFetchResponse
{
    public class Hit : CacheListFetchResponse
    {
        protected readonly RepeatedField<ByteString> values;
        protected readonly Lazy<List<byte[]>> _byteArrayList;
        protected readonly Lazy<List<string>> _stringList;

        public Hit(_ListFetchResponse response)
        {
            values = response.Found.Values;
            _byteArrayList = new(() =>
            {
                return new List<byte[]>(values.Select(v => v.ToByteArray()));
            });

            _stringList = new(() =>
            {
                return new List<string>(values.Select(v => v.ToStringUtf8()));
            });
        }

        public List<byte[]> ByteArrayList { get => _byteArrayList.Value; }

        public List<string> StringList() => _stringList.Value;
    }

    public class Miss : CacheListFetchResponse
    {

    }

    public class Error : CacheListFetchResponse
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
