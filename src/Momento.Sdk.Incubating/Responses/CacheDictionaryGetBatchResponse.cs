using System.Collections.Generic;
using System.Linq;
using Momento.Protos.CacheClient;
using Momento.Sdk.Exceptions;
using Momento.Sdk.Responses;


namespace Momento.Sdk.Incubating.Responses;

public abstract class CacheDictionaryGetBatchResponse
{
    public class Success : CacheDictionaryGetBatchResponse
    {
        public List<CacheDictionaryGetResponse> Responses { get; }

        public Success(IEnumerable<CacheDictionaryGetResponse> responses)
        {
            this.Responses = new(responses);
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
