﻿using System.Collections.Generic;
using System.Linq;
using Google.Protobuf.Collections;
using Momento.Protos.CacheClient;
using Momento.Sdk.Exceptions;
using Momento.Sdk.Responses;
using static Momento.Protos.CacheClient._DictionaryGetResponse.Types;

namespace Momento.Sdk.Incubating.Responses;

public abstract class CacheDictionaryGetBatchResponse
{
    public class Success : CacheDictionaryGetBatchResponse
    {
        public List<CacheDictionaryGetResponse> Responses { get; private set; }

        public Success(_DictionaryGetResponse responses)
        {
            var responsesList = new List<CacheDictionaryGetResponse>();
            foreach (_DictionaryGetResponsePart response in responses.Found.Items)
            {
                if (response.Result == ECacheResult.Hit)
                {
                    responsesList.Add(new CacheDictionaryGetResponse.Hit(response.CacheBody));
                }
                if (response.Result == ECacheResult.Miss)
                {
                    responsesList.Add(new CacheDictionaryGetResponse.Miss());
                }
            }
            this.Responses = responsesList;
        }

        public Success(int numRequested)
        {
            Responses = (List<CacheDictionaryGetResponse>)Enumerable.Range(1, numRequested).Select(_ => new CacheDictionaryGetResponse.Miss());
        }

        public IEnumerable<string?> Strings()
        {
            var ret = new List<string?>();
            foreach (CacheDictionaryGetResponse response in Responses)
            {
                if (response is CacheDictionaryGetResponse.Hit hitResponse)
                {
                    ret.Add(hitResponse.String());
                }
                else if (response is CacheDictionaryGetResponse.Miss missResponse)
                {
                    ret.Add(null);
                }
            }
            return ret.ToArray();
        }

        public IEnumerable<byte[]?> ByteArrays
        {
            get
            {
                var ret = new List<byte[]?>();
                foreach (CacheDictionaryGetResponse response in Responses)
                {
                    if (response is CacheDictionaryGetResponse.Hit hitResponse)
                    {
                        ret.Add(hitResponse.ByteArray);
                    }
                    else if (response is CacheDictionaryGetResponse.Miss missResponse)
                    {
                        ret.Add(null);
                    }
                }
                return ret.ToArray();
            }
        }
    }

    public class Error : CacheDictionaryGetBatchResponse
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
