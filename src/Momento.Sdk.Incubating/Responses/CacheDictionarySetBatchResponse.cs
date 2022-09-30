﻿using Momento.Sdk.Exceptions;

namespace Momento.Sdk.Incubating.Responses;

public abstract class CacheDictionarySetBatchResponse
{
    public class Success : CacheDictionarySetBatchResponse
    {
    }
    public class Error : CacheDictionarySetBatchResponse
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
