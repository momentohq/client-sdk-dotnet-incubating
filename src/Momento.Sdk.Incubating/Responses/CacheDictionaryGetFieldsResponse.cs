﻿using System;
using System.Collections.Generic;
using System.Linq;
using Google.Protobuf.Collections;
using Momento.Protos.CacheClient;
using Momento.Sdk.Exceptions;
using Momento.Sdk.Responses;
using static Momento.Protos.CacheClient._DictionaryGetResponse.Types;

namespace Momento.Sdk.Incubating.Responses;

public abstract class CacheDictionaryGetFieldsResponse
{
    public class Hit : CacheDictionaryGetFieldsResponse
    {
        public List<CacheDictionaryGetFieldResponse> Responses { get; private set; }

        public Hit(_DictionaryGetResponse responses)
        {
            var responsesList = new List<CacheDictionaryGetFieldResponse>();
            foreach (_DictionaryGetResponsePart response in responses.Found.Items)
            {
                if (response.Result == ECacheResult.Hit)
                {
                    responsesList.Add(new CacheDictionaryGetFieldResponse.Hit(response.CacheBody));
                }
                else if (response.Result == ECacheResult.Miss)
                {
                    responsesList.Add(new CacheDictionaryGetFieldResponse.Miss());
                }
                else
                {
                    responsesList.Add(new CacheDictionaryGetFieldResponse.Error(new UnknownException(response.Result.ToString())));
                }
            }
            this.Responses = responsesList;
        }
    }

    public class Miss : CacheDictionaryGetFieldsResponse
    {

    }

    public class Error : CacheDictionaryGetFieldsResponse
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
